"""FastAPI application for bidirectional FlowWeave transfer management, monitoring, scheduling, and experiments."""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from flowweave.config import FlowWeaveConfig
from flowweave.coordinator import FlowWeaveCoordinator
from flowweave.demo import ensure_sample_files
from flowweave.experiments.runner import run_scenario_comparison
from flowweave.models import PolicyName
from flowweave.simulator.scenarios import get_all_scenarios
from flowweave.transfer.security import safe_resolve_under, sanitize_filename
from flowweave.transfer.test_server import LocalFileServer


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
    priority_weight: float = Field(default=1.0, gt=0.0)


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
    downlink_mbps: float = Field(default=3.0, gt=0.1, le=500.0)
    uplink_mbps: float = Field(default=2.0, gt=0.1, le=500.0)
    total_link_capacity_mbps: float | None = Field(default=None, gt=0.1, le=500.0)
    include_noranges_file: bool = True


def create_app(config: FlowWeaveConfig | None = None) -> FastAPI:
    """Application factory so tests and production servers can inject isolated configs."""
    cfg = config or FlowWeaveConfig()
    sample_paths = ensure_sample_files(cfg.transfer.sample_dir)
    coordinator = FlowWeaveCoordinator(cfg)
    demo_server = LocalFileServer(upload_dir=cfg.transfer.upload_dir)

    for sp in sample_paths:
        demo_server.add_file_from_disk(sp)
    # Extra non-Range fallback test file
    demo_server.add_deterministic_file("legacy_noranges_1mb.bin", 1024 * 1024, seed_byte=66)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await demo_server.start()
        await coordinator.start()
        try:
            yield
        finally:
            await coordinator.stop()
            await demo_server.stop()

    app = FastAPI(
        title="FlowWeave API",
        description="Cooperative, congestion-aware bidirectional file-transfer scheduling system",
        version="0.2.0",
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

    @app.get("/api/health")
    async def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "policy": coordinator.scheduler.name.value,
            "demo_server_port": demo_server.bound_port,
            "sample_files": [p.name for p in sample_paths],
        }

    @app.get("/api/transfers")
    async def list_transfers() -> dict[str, Any]:
        live = [s.to_dict() for s in coordinator.transfer_manager.list_snapshots()]
        return {"transfers": live}

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
        """Start a real chunked parallel upload of a sample file or local path."""
        try:
            if req.sample_filename:
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

        target_server = req.server_url or demo_server.base_url
        snap = await coordinator.transfer_manager.submit_upload(
            src_path,
            target_server,
            filename=req.filename or src_path.name,
            priority_weight=req.priority_weight,
        )
        return {"transfer": snap.to_dict()}

    @app.post("/api/transfers/upload-browser", status_code=201)
    async def upload_from_browser(request: Request) -> dict[str, Any]:
        """Accept raw file bytes from the browser UI and schedule via the chunked upload engine."""
        raw_name = request.headers.get("X-Filename", "browser_upload.bin")
        try:
            safe_name = sanitize_filename(raw_name, fallback="browser_upload.bin")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        body = await request.body()
        if not body:
            raise HTTPException(status_code=400, detail="Empty upload payload")

        inbox_dir = cfg.transfer.upload_dir / ".browser_inbox"
        inbox_dir.mkdir(parents=True, exist_ok=True)
        staged = inbox_dir / f"{uuid.uuid4().hex[:8]}_{safe_name}"
        staged.write_bytes(body)

        snap = await coordinator.transfer_manager.submit_upload(
            staged,
            demo_server.base_url,
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
        """List local sample files available for instant offline download or upload testing."""
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
                    "supports_ranges": allow_ranges,
                    "can_upload_sample": disk_file.is_file(),
                }
            )
        return {"files": items, "server_base_url": demo_server.base_url}

    @app.post("/api/demo/start", status_code=201)
    async def start_demo_batch(req: StartDemoRequest) -> dict[str, Any]:
        """Launch concurrent downloads, concurrent uploads, or a mixed bidirectional workload."""
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
                snap = await coordinator.transfer_manager.submit_download(
                    demo_server.url_for(name, allow_ranges=allow_ranges),
                    filename=name,
                    expected_size_bytes=len(sf.data),
                    expected_sha256=sf.sha256,
                )
                launched.append(snap.to_dict())

        if req.mode in ("uploads", "mixed"):
            subset_u = upload_names if req.mode == "uploads" else upload_names[:3]
            for name in subset_u:
                src = cfg.transfer.sample_dir / name
                if src.is_file():
                    snap = await coordinator.transfer_manager.submit_upload(
                        src,
                        demo_server.base_url,
                        filename=f"up_{name}",
                    )
                    launched.append(snap.to_dict())

        return {
            "mode": req.mode,
            "launched": len(launched),
            "downlink_capacity_bps": dl_bps,
            "uplink_capacity_bps": ul_bps,
            "transfers": launched,
        }

    @app.get("/api/monitor")
    async def get_monitor() -> dict[str, Any]:
        return coordinator.get_monitor_status()

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
