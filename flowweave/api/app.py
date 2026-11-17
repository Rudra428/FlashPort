"""FastAPI application for bidirectional FlowWeave transfer management, raw TCP peer transfers, monitoring, and experiments."""

from __future__ import annotations

import asyncio
import os
import socket
import tempfile
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from flowweave.config import FlowWeaveConfig
from flowweave.coordinator import FlowWeaveCoordinator
from flowweave.demo import ensure_sample_files
from flowweave.experiments.runner import run_scenario_comparison
from flowweave.metrics import summarize_experiment
from flowweave.models import PolicyName, TransferDirection, TransferOutcome, TransferState
from flowweave.simulator.scenarios import get_all_scenarios
from flowweave.transfer.security import safe_resolve_under, sanitize_filename
from flowweave.transfer.tcp_protocol import recv_frame_async, send_frame_async
from flowweave.transfer.tcp_server import FlowWeaveTCPServer, detect_all_local_ips
from flowweave.transfer.test_server import LocalFileServer


def _detect_lan_ip() -> str:
    """Best-effort local LAN IPv4 lookup without transmitting any packets."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            return str(s.getsockname()[0])
        finally:
            s.close()
    except OSError:
        return "127.0.0.1"


class CreateDownloadRequest(BaseModel):
    url: str
    filename: str | None = None
    expected_size_bytes: int = 0
    candidate_links: list[str] | None = None
    priority_weight: float = Field(default=1.0, gt=0.0)
    expected_sha256: str | None = None


class CreateUploadRequest(BaseModel):
    sample_filename: str | None = None
    file_path: str | None = None
    filename: str | None = None
    server_url: str | None = None
    transport: Literal["http", "tcp"] = "tcp"
    priority_weight: float = Field(default=1.0, gt=0.0)


class SendStagedRequest(BaseModel):
    host: str = "127.0.0.1"
    port: int = Field(default=9000, ge=1, le=65535)
    staged_ids: list[str] | None = None
    policy: PolicyName | None = None
    link_cap_mbps: float | None = Field(default=None, gt=0.1, le=500.0)


class CompareStagedRequest(BaseModel):
    host: str = "127.0.0.1"
    port: int = Field(default=9000, ge=1, le=65535)
    staged_ids: list[str] | None = None
    policies: list[PolicyName] = Field(default_factory=lambda: [PolicyName.SRPT, PolicyName.FLOWWEAVE])
    link_cap_mbps: float = Field(default=2.0, gt=0.1, le=500.0)


class RemoteDownloadBatchRequest(BaseModel):
    host: str = "127.0.0.1"
    port: int = Field(default=9000, ge=1, le=65535)
    filenames: list[str]
    policy: PolicyName | None = None
    link_cap_mbps: float | None = Field(default=None, gt=0.1, le=500.0)


class CompareRemoteDownloadsRequest(BaseModel):
    host: str = "127.0.0.1"
    port: int = Field(default=9000, ge=1, le=65535)
    filenames: list[str]
    policies: list[PolicyName] = Field(default_factory=lambda: [PolicyName.SRPT, PolicyName.FLOWWEAVE])
    link_cap_mbps: float = Field(default=2.0, gt=0.1, le=500.0)


class UpdateSchedulerRequest(BaseModel):
    policy: PolicyName
    min_fair_share_fraction: float | None = Field(default=None, ge=0.0, le=0.9)
    srpt_exponent: float | None = Field(default=None, ge=0.25, le=4.0)
    total_link_capacity_bps: float | None = Field(default=None, gt=0.0)
    upload_link_capacity_bps: float | None = Field(default=None, gt=0.0)


class RunExperimentRequest(BaseModel):
    scenario: str = "mixed_shared_bottleneck"
    seed: int = 42


class StartDemoRequest(BaseModel):
    mode: Literal["downloads", "uploads", "mixed"] = "mixed"
    transport: Literal["http", "tcp"] = "tcp"
    downlink_mbps: float = Field(default=3.0, gt=0.1, le=500.0)
    uplink_mbps: float = Field(default=2.0, gt=0.1, le=500.0)
    total_link_capacity_mbps: float | None = Field(default=None, gt=0.1, le=500.0)
    include_noranges_file: bool = True


class UpdateReceiverPortRequest(BaseModel):
    port: int = Field(..., ge=1, le=65535)


def create_app(config: FlowWeaveConfig | None = None, *, tcp_port: int | None = None) -> FastAPI:
    """Application factory so tests and production servers can inject isolated configs."""
    cfg = config or FlowWeaveConfig()
    resolved_tcp_port = (
        tcp_port if tcp_port is not None else int(os.environ.get("FLOWWEAVE_TCP_PORT", "9000"))
    )
    coordinator = FlowWeaveCoordinator(cfg)
    demo_server = LocalFileServer(upload_dir=cfg.transfer.upload_dir)

    # Keep internal staging/shared scratch out of the user's Downloads folder so ~/Downloads
    # only receives clean verified files.
    scratch_root = Path(tempfile.gettempdir()) / "flowweave_runtime"
    shared_dir = scratch_root / "shared"
    shared_dir.mkdir(parents=True, exist_ok=True)
    tcp_server = FlowWeaveTCPServer(
        host="0.0.0.0",
        port=resolved_tcp_port,
        serve_dir=shared_dir,
        upload_dir=cfg.transfer.upload_dir,
    )

    staged_files: dict[str, dict[str, Any]] = {}
    recorded_runs: list[dict[str, Any]] = []
    compare_status: dict[str, Any] = {
        "running": False,
        "current_policy": None,
        "completed_policies": [],
        "total_policies": 0,
        "error": None,
    }
    background_tasks: set[asyncio.Task] = set()

    def _track_task(coro) -> asyncio.Task:
        task = asyncio.create_task(coro)
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)
        return task

    def _ensure_demo_samples() -> list[Path]:
        paths = ensure_sample_files(cfg.transfer.sample_dir)
        for sp in paths:
            if sp.name not in demo_server.files:
                demo_server.add_file_from_disk(sp)
        if "legacy_noranges_1mb.bin" not in demo_server.files:
            demo_server.add_deterministic_file("legacy_noranges_1mb.bin", 1024 * 1024, seed_byte=66)
        return paths

    async def _record_batch_on_completion(
        tids: list[str],
        *,
        policy: str,
        direction: str,
        target: str,
        t0: float,
    ) -> dict[str, Any]:
        """Wait for a set of transfers to finish and record their real measured FCT metrics."""
        while True:
            snaps = [coordinator.transfer_manager.get_snapshot(tid) for tid in tids]
            active_left = [
                s
                for s in snaps
                if s is not None
                and s.state in (TransferState.PENDING, TransferState.ACTIVE, TransferState.PAUSED)
            ]
            if not active_left:
                break
            await asyncio.sleep(0.1)

        outcomes: list[TransferOutcome] = []
        for tid in tids:
            s = coordinator.transfer_manager.get_snapshot(tid)
            if s is None:
                continue
            arr = max(0.0, s.created_at - t0)
            st_t = max(0.0, (s.started_at or s.created_at) - t0)
            end_t = max(st_t + 0.001, (s.completed_at or time.monotonic()) - t0)
            fct = max(0.001, end_t - arr)
            outcomes.append(
                TransferOutcome(
                    transfer_id=s.transfer_id,
                    filename=s.filename,
                    size_bytes=s.total_bytes,
                    arrival_time=arr,
                    start_time=st_t,
                    completion_time=end_t,
                    fct_s=fct,
                    avg_goodput_bps=s.total_bytes / fct if s.total_bytes > 0 else 0.0,
                    direction=s.direction,
                    retries=s.measured.retries,
                    state=s.state,
                )
            )

        summary = summarize_experiment(
            scenario=f"live_tcp_{direction}_{target}",
            policy=policy,
            seed=0,
            outcomes=outcomes,
            scheduler_overhead_ms=[coordinator.last_decision.compute_time_ms if coordinator.last_decision else 0.0],
        )
        summary.is_simulated = False
        record = summary.to_dict()
        record["run_id"] = uuid.uuid4().hex[:8]
        record["timestamp"] = time.time()
        record["direction"] = direction
        record["target"] = target
        if summary.completed_transfers > 0:
            recorded_runs.append(record)
        return record

    async def _verify_tcp_peer(host: str, port: int) -> None:
        """Fast 3s handshake check so unreachable hosts/firewalls report clear errors immediately."""
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=3.0)
            try:
                await send_frame_async(writer, {"op": "LIST"})
                resp, _ = await asyncio.wait_for(recv_frame_async(reader), timeout=3.0)
                if not resp.get("ok"):
                    raise RuntimeError(resp.get("error", "Remote peer rejected FWV1 handshake"))
            finally:
                writer.close()
                await writer.wait_closed()
        except TimeoutError:
            raise HTTPException(
                status_code=502,
                detail=(
                    f"TCP connection to {host}:{port} timed out. Ensure FlowWeave receiver is running "
                    f"on {host}:{port} (`python scripts/receiver.py --port {port}`), Windows Firewall "
                    f"allows inbound TCP {port}, and Wi-Fi client isolation is not blocking peer traffic."
                ),
            )
        except OSError as exc:
            raise HTTPException(
                status_code=502,
                detail=(
                    f"Cannot reach FlowWeave TCP peer at {host}:{port} ({exc}). "
                    f"Start `python scripts/receiver.py --port {port}` or `python -m flowweave tcp-server --port {port}` on that host."
                ),
            )
        except Exception as exc:
            raise HTTPException(
                status_code=502,
                detail=(
                    f"Host {host}:{port} did not speak FlowWeave TCP protocol ({exc}). "
                    f"Make sure you run FlowWeave's TCP server (`scripts/receiver.py` or `flowweave tcp-server`) on port {port}."
                ),
            )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await demo_server.start()
        await tcp_server.start()
        await coordinator.start()
        try:
            yield
        finally:
            for t in list(background_tasks):
                t.cancel()
            await coordinator.stop()
            await tcp_server.stop()
            await demo_server.stop()

    app = FastAPI(
        title="FlowWeave API",
        description="Cooperative, congestion-aware bidirectional TCP & HTTP file-transfer scheduling system",
        version="0.3.0",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.state.coordinator = coordinator
    app.state.demo_server = demo_server
    app.state.tcp_server = tcp_server

    @app.get("/api/health")
    async def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "policy": coordinator.scheduler.name.value,
            "demo_server_port": demo_server.bound_port,
            "tcp_server_port": tcp_server.bound_port,
            "lan_ip": _detect_lan_ip(),
        }

    @app.get("/api/host-info")
    async def host_info() -> dict[str, Any]:
        return {
            "lan_ip": _detect_lan_ip(),
            "tcp_port": tcp_server.bound_port,
            "policy": coordinator.scheduler.name.value,
            "downlink_mbps": round(cfg.transfer.total_link_capacity_bps / (1024 * 1024), 2),
            "uplink_mbps": round(cfg.transfer.upload_link_capacity_bps / (1024 * 1024), 2),
            "compare_status": compare_status,
        }

    @app.get("/api/receiver")
    async def get_receiver_status() -> dict[str, Any]:
        """Integrated live TCP receiver status across all interfaces (0.0.0.0) and listening ports."""
        return {
            "listening": tcp_server.is_listening,
            "bind_host": tcp_server.host,
            "lan_ip": _detect_lan_ip(),
            "local_ips": detect_all_local_ips(),
            "tcp_port": tcp_server.bound_port,
            "listening_ports": tcp_server.listening_ports,
            "save_dir": str(cfg.transfer.upload_dir.resolve()),
            "incoming": tcp_server.get_incoming_transfers(),
            "received_files": tcp_server.list_received_files(),
        }

    @app.post("/api/receiver/port")
    async def update_receiver_port(req: UpdateReceiverPortRequest) -> dict[str, Any]:
        """Rebind the primary TCP receiver socket onto a new port right from the UI."""
        try:
            bound = await tcp_server.restart_on_port(req.port)
        except (ValueError, OSError) as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {
            "tcp_port": bound,
            "listening_ports": tcp_server.listening_ports,
            "lan_ip": _detect_lan_ip(),
            "listening": tcp_server.is_listening,
        }

    @app.post("/api/receiver/ports", status_code=201)
    async def add_receiver_port(req: UpdateReceiverPortRequest) -> dict[str, Any]:
        """Listen on an additional TCP socket port simultaneously on all interfaces (0.0.0.0)."""
        try:
            bound = await tcp_server.add_port(req.port)
        except (ValueError, OSError) as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {
            "added_port": bound,
            "listening_ports": tcp_server.listening_ports,
        }

    @app.delete("/api/receiver/ports/{port}")
    async def remove_receiver_port(port: int) -> dict[str, Any]:
        if len(tcp_server.listening_ports) <= 1:
            raise HTTPException(status_code=400, detail="Keep at least one active receiver port")
        removed = await tcp_server.remove_port(port)
        if not removed:
            raise HTTPException(status_code=404, detail=f"Port {port} is not listening")
        return {"removed_port": port, "listening_ports": tcp_server.listening_ports}

    @app.get("/api/receiver/download/{filename}")
    async def download_received_file(filename: str) -> FileResponse:
        """Download a received file from this host's receiver storage to the browser."""
        try:
            path = tcp_server.resolve_received_file(filename)
        except (ValueError, FileNotFoundError) as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        return FileResponse(path=str(path), filename=path.name)

    @app.delete("/api/receiver/files/{filename}")
    async def delete_received_file(filename: str) -> dict[str, Any]:
        try:
            deleted = tcp_server.delete_received_file(filename)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        if not deleted:
            raise HTTPException(status_code=404, detail=f"File not found: {filename}")
        return {"deleted": filename}

    @app.get("/api/tcp/remote-files")
    async def list_remote_tcp_files(
        host: str = Query(..., min_length=1),
        port: int = Query(default=9000, ge=1, le=65535),
    ) -> dict[str, Any]:
        """Connect to another FlowWeave host via raw TCP socket and list its available files."""
        t0 = time.monotonic()
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=4.0)
            try:
                await send_frame_async(writer, {"op": "LIST"})
                resp, _ = await asyncio.wait_for(recv_frame_async(reader), timeout=4.0)
            finally:
                writer.close()
                await writer.wait_closed()
        except Exception as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Could not connect to FlowWeave TCP server at {host}:{port} ({exc})",
            )

        rtt_ms = (time.monotonic() - t0) * 1000.0
        if not resp.get("ok"):
            raise HTTPException(status_code=502, detail=resp.get("error", "Remote TCP LIST failed"))
        return {
            "host": host,
            "port": port,
            "rtt_ms": round(rtt_ms, 2),
            "files": resp.get("files", []),
        }

    @app.get("/api/transfers")
    async def list_transfers() -> dict[str, Any]:
        live = [s.to_dict() for s in coordinator.transfer_manager.list_snapshots()]
        return {"transfers": live}

    @app.post("/api/transfers/clear")
    async def clear_finished_transfers() -> dict[str, Any]:
        removed = coordinator.transfer_manager.clear_finished()
        return {"cleared": removed}

    @app.post("/api/transfers/stage-browser", status_code=201)
    async def stage_browser_file(request: Request) -> dict[str, Any]:
        """Stage a real file selected in the browser so it can be sent or compared across policies."""
        raw_name = request.headers.get("X-Filename", "upload.bin")
        try:
            safe_name = sanitize_filename(raw_name, fallback="upload.bin")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        body = await request.body()
        if not body:
            raise HTTPException(status_code=400, detail="Empty file payload")

        stage_dir = scratch_root / "staged_queue"
        stage_dir.mkdir(parents=True, exist_ok=True)
        staged_id = uuid.uuid4().hex[:10]
        dest = stage_dir / f"{staged_id}_{safe_name}"
        dest.write_bytes(body)

        entry = {
            "staged_id": staged_id,
            "filename": safe_name,
            "size_bytes": len(body),
            "path": str(dest),
        }
        staged_files[staged_id] = entry
        return {"file": {k: v for k, v in entry.items() if k != "path"}}

    @app.get("/api/transfers/staged")
    async def list_staged_files() -> dict[str, Any]:
        return {
            "files": [
                {k: v for k, v in entry.items() if k != "path"}
                for entry in staged_files.values()
                if Path(entry["path"]).is_file()
            ]
        }

    @app.delete("/api/transfers/staged/{staged_id}")
    async def remove_staged_file(staged_id: str) -> dict[str, Any]:
        entry = staged_files.pop(staged_id, None)
        if entry is None:
            raise HTTPException(status_code=404, detail="Staged file not found")
        Path(entry["path"]).unlink(missing_ok=True)
        return {"removed": staged_id}

    @app.post("/api/transfers/send-staged", status_code=201)
    async def send_staged_files(req: SendStagedRequest) -> dict[str, Any]:
        """Send all selected staged files simultaneously via raw TCP sockets to `host:port`."""
        ids = req.staged_ids or list(staged_files.keys())
        items = [staged_files[i] for i in ids if i in staged_files and Path(staged_files[i]["path"]).is_file()]
        if not items:
            raise HTTPException(status_code=400, detail="No files selected to send")
        await _verify_tcp_peer(req.host, req.port)

        if req.policy is not None or req.link_cap_mbps is not None:
            coordinator.set_policy(
                req.policy or coordinator.scheduler.name,
                upload_link_capacity_bps=(req.link_cap_mbps * 1024 * 1024) if req.link_cap_mbps else None,
            )

        target_url = f"tcp://{req.host}:{req.port}"
        t0 = time.monotonic()
        launched = []
        tids = []
        for entry in items:
            snap = await coordinator.transfer_manager.submit_upload(
                entry["path"],
                target_url,
                filename=entry["filename"],
            )
            launched.append(snap.to_dict())
            tids.append(snap.transfer_id)

        _track_task(
            _record_batch_on_completion(
                tids,
                policy=coordinator.scheduler.name.value,
                direction="upload",
                target=f"{req.host}:{req.port}",
                t0=t0,
            )
        )
        return {"launched": len(launched), "target": target_url, "transfers": launched}

    @app.post("/api/transfers/compare-staged", status_code=202)
    async def compare_staged_policies(req: CompareStagedRequest) -> dict[str, Any]:
        """Run the exact same staged files over raw TCP sockets across policies (e.g. SRPT vs FlowWeave)."""
        if compare_status["running"]:
            raise HTTPException(status_code=409, detail="A live policy comparison is already running")
        ids = req.staged_ids or list(staged_files.keys())
        items = [staged_files[i] for i in ids if i in staged_files and Path(staged_files[i]["path"]).is_file()]
        if not items:
            raise HTTPException(status_code=400, detail="Select at least one file first")
        await _verify_tcp_peer(req.host, req.port)

        async def _runner() -> None:
            compare_status.update(
                {
                    "running": True,
                    "current_policy": None,
                    "completed_policies": [],
                    "total_policies": len(req.policies),
                    "error": None,
                }
            )
            target_url = f"tcp://{req.host}:{req.port}"
            try:
                for pol in req.policies:
                    compare_status["current_policy"] = pol.value
                    coordinator.transfer_manager.clear_finished()
                    coordinator.set_policy(
                        pol,
                        upload_link_capacity_bps=req.link_cap_mbps * 1024 * 1024,
                    )
                    t0 = time.monotonic()
                    tids = []
                    for entry in items:
                        snap = await coordinator.transfer_manager.submit_upload(
                            entry["path"],
                            target_url,
                            filename=entry["filename"],
                        )
                        tids.append(snap.transfer_id)
                    await _record_batch_on_completion(
                        tids,
                        policy=pol.value,
                        direction="upload",
                        target=f"{req.host}:{req.port}",
                        t0=t0,
                    )
                    compare_status["completed_policies"].append(pol.value)
                    await asyncio.sleep(0.25)
            except Exception as exc:
                compare_status["error"] = str(exc)
            finally:
                compare_status["running"] = False
                compare_status["current_policy"] = None

        _track_task(_runner())
        return {"started": True, "policies": [p.value for p in req.policies], "num_files": len(items)}

    @app.post("/api/transfers/download-remote", status_code=201)
    async def download_remote_batch(req: RemoteDownloadBatchRequest) -> dict[str, Any]:
        """Download multiple files simultaneously from a remote FlowWeave TCP host."""
        if not req.filenames:
            raise HTTPException(status_code=400, detail="Select at least one remote file")
        if req.policy is not None or req.link_cap_mbps is not None:
            coordinator.set_policy(
                req.policy or coordinator.scheduler.name,
                total_link_capacity_bps=(req.link_cap_mbps * 1024 * 1024) if req.link_cap_mbps else None,
            )

        t0 = time.monotonic()
        launched = []
        tids = []
        for fname in req.filenames:
            url = f"tcp://{req.host}:{req.port}/{fname}"
            snap = await coordinator.transfer_manager.submit_download(url, filename=fname)
            launched.append(snap.to_dict())
            tids.append(snap.transfer_id)

        _track_task(
            _record_batch_on_completion(
                tids,
                policy=coordinator.scheduler.name.value,
                direction="download",
                target=f"{req.host}:{req.port}",
                t0=t0,
            )
        )
        return {"launched": len(launched), "transfers": launched}

    @app.post("/api/transfers/compare-remote", status_code=202)
    async def compare_remote_downloads(req: CompareRemoteDownloadsRequest) -> dict[str, Any]:
        """Compare policies (e.g. SRPT vs FlowWeave) downloading the same remote files over raw TCP."""
        if compare_status["running"]:
            raise HTTPException(status_code=409, detail="A live policy comparison is already running")
        if not req.filenames:
            raise HTTPException(status_code=400, detail="Select at least one remote file")

        async def _runner() -> None:
            compare_status.update(
                {
                    "running": True,
                    "current_policy": None,
                    "completed_policies": [],
                    "total_policies": len(req.policies),
                    "error": None,
                }
            )
            try:
                for pol in req.policies:
                    compare_status["current_policy"] = pol.value
                    coordinator.transfer_manager.clear_finished()
                    coordinator.set_policy(
                        pol,
                        total_link_capacity_bps=req.link_cap_mbps * 1024 * 1024,
                    )
                    t0 = time.monotonic()
                    tids = []
                    for fname in req.filenames:
                        url = f"tcp://{req.host}:{req.port}/{fname}"
                        snap = await coordinator.transfer_manager.submit_download(url, filename=fname)
                        tids.append(snap.transfer_id)
                    await _record_batch_on_completion(
                        tids,
                        policy=pol.value,
                        direction="download",
                        target=f"{req.host}:{req.port}",
                        t0=t0,
                    )
                    compare_status["completed_policies"].append(pol.value)
                    await asyncio.sleep(0.25)
            except Exception as exc:
                compare_status["error"] = str(exc)
            finally:
                compare_status["running"] = False
                compare_status["current_policy"] = None

        _track_task(_runner())
        return {"started": True, "policies": [p.value for p in req.policies], "num_files": len(req.filenames)}

    @app.get("/api/runs")
    async def list_recorded_runs() -> dict[str, Any]:
        return {"runs": recorded_runs, "compare_status": compare_status}

    @app.post("/api/runs/clear")
    async def clear_recorded_runs() -> dict[str, Any]:
        recorded_runs.clear()
        return {"cleared": True}

    @app.post("/api/transfers", status_code=201)
    @app.post("/api/transfers/download", status_code=201)
    async def create_download(req: CreateDownloadRequest) -> dict[str, Any]:
        snap = await coordinator.transfer_manager.submit_download(
            req.url,
            filename=req.filename,
            expected_size_bytes=req.expected_size_bytes,
            candidate_links=req.candidate_links,
            priority_weight=req.priority_weight,
            expected_sha256=req.expected_sha256,
        )
        return {"transfer": snap.to_dict()}

    @app.post("/api/transfers/upload", status_code=201)
    async def create_upload(req: CreateUploadRequest) -> dict[str, Any]:
        try:
            if req.sample_filename:
                _ensure_demo_samples()
                src_path = safe_resolve_under(cfg.transfer.sample_dir, req.sample_filename)
            elif req.file_path:
                src_path = Path(req.file_path).resolve()
            else:
                raise HTTPException(
                    status_code=400, detail="Provide either sample_filename or file_path"
                )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        if not src_path.is_file():
            raise HTTPException(status_code=404, detail=f"Source file not found: {src_path.name}")

        default_target = f"tcp://127.0.0.1:{tcp_server.bound_port}" if req.transport == "tcp" else demo_server.base_url
        target_server = req.server_url or default_target
        snap = await coordinator.transfer_manager.submit_upload(
            src_path,
            target_server,
            filename=req.filename or src_path.name,
            priority_weight=req.priority_weight,
        )
        return {"transfer": snap.to_dict()}

    @app.post("/api/transfers/upload-browser", status_code=201)
    async def upload_from_browser(request: Request) -> dict[str, Any]:
        """Accept raw file bytes from browser and transmit via raw TCP socket to target host."""
        raw_name = request.headers.get("X-Filename", "browser_upload.bin")
        target_host = request.headers.get("X-Target-Host", "127.0.0.1")
        target_port_raw = request.headers.get("X-Target-Port")
        target_port = int(target_port_raw) if target_port_raw and target_port_raw.isdigit() else tcp_server.bound_port
        try:
            safe_name = sanitize_filename(raw_name, fallback="browser_upload.bin")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        body = await request.body()
        if not body:
            raise HTTPException(status_code=400, detail="Empty upload payload")

        inbox_dir = scratch_root / "browser_inbox"
        inbox_dir.mkdir(parents=True, exist_ok=True)
        staged = inbox_dir / f"{uuid.uuid4().hex[:8]}_{safe_name}"
        staged.write_bytes(body)

        snap = await coordinator.transfer_manager.submit_upload(
            staged,
            f"tcp://{target_host}:{target_port}",
            filename=safe_name,
        )
        return {"transfer": snap.to_dict()}

    @app.get("/api/transfers/{transfer_id}")
    async def get_transfer(transfer_id: str) -> dict[str, Any]:
        snap = coordinator.transfer_manager.get_snapshot(transfer_id)
        if snap is None:
            raise HTTPException(status_code=404, detail=f"Transfer {transfer_id} not found")
        return {"transfer": snap.to_dict()}

    @app.post("/api/transfers/{transfer_id}/pause")
    async def pause_transfer(transfer_id: str) -> dict[str, Any]:
        ok = await coordinator.transfer_manager.pause(transfer_id)
        if not ok:
            raise HTTPException(status_code=400, detail=f"Transfer {transfer_id} cannot be paused")
        return {"transfer_id": transfer_id, "paused": True}

    @app.post("/api/transfers/{transfer_id}/resume")
    async def resume_transfer(transfer_id: str) -> dict[str, Any]:
        ok = await coordinator.transfer_manager.resume(transfer_id)
        if not ok:
            raise HTTPException(status_code=400, detail=f"Transfer {transfer_id} cannot be resumed")
        return {"transfer_id": transfer_id, "resumed": True}

    @app.post("/api/transfers/{transfer_id}/cancel")
    async def cancel_transfer(transfer_id: str) -> dict[str, Any]:
        cancelled = await coordinator.transfer_manager.cancel(transfer_id)
        if not cancelled:
            raise HTTPException(status_code=404, detail=f"Transfer {transfer_id} not found")
        return {"transfer_id": transfer_id, "cancelled": True}

    @app.get("/api/demo/files")
    async def list_demo_files() -> dict[str, Any]:
        _ensure_demo_samples()
        items = []
        for name, sf in demo_server.files.items():
            allow_ranges = name != "legacy_noranges_1mb.bin"
            disk_file = cfg.transfer.sample_dir / name
            items.append(
                {
                    "filename": name,
                    "size_bytes": len(sf.data),
                    "sha256": sf.sha256,
                    "download_url": demo_server.url_for(name, allow_ranges=allow_ranges),
                    "tcp_url": f"tcp://127.0.0.1:{tcp_server.bound_port}/{name}" if disk_file.is_file() else None,
                    "supports_ranges": allow_ranges,
                    "can_upload_sample": disk_file.is_file(),
                }
            )
        return {
            "files": items,
            "server_base_url": demo_server.base_url,
            "tcp_server_address": f"tcp://127.0.0.1:{tcp_server.bound_port}",
        }

    @app.post("/api/demo/start", status_code=201)
    async def start_demo_batch(req: StartDemoRequest) -> dict[str, Any]:
        _ensure_demo_samples()
        dl_mbps = req.total_link_capacity_mbps if req.total_link_capacity_mbps is not None else req.downlink_mbps
        dl_bps = dl_mbps * 1024 * 1024
        ul_bps = req.uplink_mbps * 1024 * 1024
        coordinator.set_policy(
            coordinator.scheduler.name,
            total_link_capacity_bps=dl_bps,
            upload_link_capacity_bps=ul_bps,
        )

        launched: list[dict[str, Any]] = []
        download_names = [
            ("patch_512kb.bin", True),
            ("dataset_1mb.bin", True),
            ("archive_2mb.bin", True),
            ("checkpoint_4mb.bin", True),
            ("video_6mb.bin", True),
        ]
        if req.include_noranges_file:
            download_names.append(("legacy_noranges_1mb.bin", False))

        upload_names = [
            "patch_512kb.bin",
            "dataset_1mb.bin",
            "archive_2mb.bin",
            "checkpoint_4mb.bin",
        ]

        if req.mode in ("downloads", "mixed"):
            subset = download_names if req.mode == "downloads" else download_names[:4]
            for name, allow_ranges in subset:
                sf = demo_server.files[name]
                dl_url = demo_server.url_for(name, allow_ranges=allow_ranges)
                snap = await coordinator.transfer_manager.submit_download(
                    dl_url,
                    filename=name,
                    expected_size_bytes=len(sf.data),
                    expected_sha256=sf.sha256,
                )
                launched.append(snap.to_dict())

        if req.mode in ("uploads", "mixed"):
            subset_u = upload_names if req.mode == "uploads" else upload_names[:3]
            up_target = f"tcp://127.0.0.1:{tcp_server.bound_port}" if req.transport == "tcp" else demo_server.base_url
            for name in subset_u:
                src = cfg.transfer.sample_dir / name
                if src.is_file():
                    snap = await coordinator.transfer_manager.submit_upload(
                        src,
                        up_target,
                        filename=f"up_{name}",
                    )
                    launched.append(snap.to_dict())

        return {
            "mode": req.mode,
            "transport": req.transport,
            "launched": len(launched),
            "downlink_capacity_bps": dl_bps,
            "uplink_capacity_bps": ul_bps,
            "transfers": launched,
        }

    @app.get("/api/monitor")
    async def get_monitor() -> dict[str, Any]:
        status = coordinator.get_monitor_status()
        status["compare_status"] = compare_status
        return status

    @app.get("/api/scheduler")
    async def get_scheduler() -> dict[str, Any]:
        cfg_s = coordinator.config.scheduler
        return {
            "policy": coordinator.scheduler.name.value,
            "available_policies": [p.value for p in PolicyName],
            "min_fair_share_fraction": cfg_s.min_fair_share_fraction,
            "srpt_exponent": cfg_s.srpt_exponent,
            "starvation_aging_per_s": cfg_s.starvation_aging_per_s,
            "congestion_headroom": cfg_s.congestion_headroom,
            "total_link_capacity_bps": coordinator.config.transfer.total_link_capacity_bps,
            "upload_link_capacity_bps": coordinator.config.transfer.upload_link_capacity_bps,
        }

    @app.post("/api/scheduler")
    async def update_scheduler(req: UpdateSchedulerRequest) -> dict[str, Any]:
        updated = coordinator.set_policy(
            req.policy,
            min_fair_share_fraction=req.min_fair_share_fraction,
            srpt_exponent=req.srpt_exponent,
            total_link_capacity_bps=req.total_link_capacity_bps,
            upload_link_capacity_bps=req.upload_link_capacity_bps,
        )
        return {
            "policy": updated.policy.value,
            "min_fair_share_fraction": updated.min_fair_share_fraction,
            "srpt_exponent": updated.srpt_exponent,
            "total_link_capacity_bps": coordinator.config.transfer.total_link_capacity_bps,
            "upload_link_capacity_bps": coordinator.config.transfer.upload_link_capacity_bps,
        }

    @app.get("/api/experiments/scenarios")
    async def list_scenarios() -> dict[str, Any]:
        scenarios = get_all_scenarios()
        return {
            "scenarios": [
                {
                    "name": sc.name,
                    "description": sc.description,
                    "num_transfers": len(sc.transfers),
                    "num_links": len(sc.links),
                }
                for sc in scenarios.values()
            ]
        }

    @app.post("/api/experiments/run")
    async def run_experiment(req: RunExperimentRequest) -> dict[str, Any]:
        scenarios = get_all_scenarios(seed=req.seed)
        spec = scenarios.get(req.scenario)
        if spec is None:
            raise HTTPException(
                status_code=404,
                detail=f"Unknown scenario '{req.scenario}'. Available: {list(scenarios.keys())}",
            )
        bundle = run_scenario_comparison(spec)
        for summary in bundle.summaries.values():
            coordinator.storage.save_experiment(summary)
        return bundle.to_dict()

    @app.get("/api/experiments/results")
    async def get_experiment_results(limit: int = 40) -> dict[str, Any]:
        return {"results": coordinator.storage.list_experiments(limit=limit)}

    dist_dir = Path(__file__).resolve().parents[2] / "frontend" / "dist"
    if dist_dir.is_dir():
        app.mount("/", StaticFiles(directory=str(dist_dir), html=True), name="frontend")

    return app


app = create_app()
