from __future__ import annotations

import asyncio
import hashlib
import math
import os
import re
import secrets
import time
import uuid
from collections import deque
from dataclasses import dataclass
from pathlib import Path, PureWindowsPath
from typing import AsyncIterator
from urllib.parse import quote

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from flashport.config import SchedulerConfig
from flashport.core.models import TransferState
from flashport.core.transfer_manager import TransferManager
from flashport.network.estimator import NetworkEstimator
from flashport.network.monitor import NetworkMonitor
from flashport.scheduler import (
    EqualShareScheduler,
    FlashportScheduler,
    IndependentScheduler,
    Scheduler,
    SRPTScheduler,
)
from flashport.transfer.rate_limiter import RateLimiter

STREAM_CHUNK_SIZE = 16_384
_RANGE_RE = re.compile(r"bytes=(\d*)-(\d*)\Z")


class RegisterTransferRequest(BaseModel):
    filename: str = Field(min_length=1, max_length=255)
    client_id: str = Field(default="local-client", min_length=1, max_length=128)


class ProgressRequest(BaseModel):
    downloaded_bytes: int = Field(ge=0)
    goodput_bps: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    http_request_latency_ms: float | None = Field(default=None, ge=0, allow_inf_nan=False)


class ResumeChunk(BaseModel):
    start: int = Field(ge=0)
    end: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class ResumeRequest(BaseModel):
    chunks: list[ResumeChunk] = Field(max_length=10_000)


class SchedulerSelection(BaseModel):
    policy: str
    configured_capacity_bps: float | None = Field(
        default=None,
        gt=0,
        le=1_000_000_000_000,
        allow_inf_nan=False,
    )


class RegisterReceiverRequest(BaseModel):
    name: str = Field(min_length=1, max_length=80)


class CreateDeliveryRequest(BaseModel):
    upload_id: str = Field(min_length=1, max_length=64)
    receiver_ids: list[str] = Field(min_length=1, max_length=100)


@dataclass(frozen=True)
class FileSnapshot:
    path: Path
    size: int
    modified_ns: int
    device: int
    inode: int
    sha256: str

    @classmethod
    def from_stat(cls, path: Path, stat: os.stat_result, sha256: str) -> FileSnapshot:
        return cls(path, stat.st_size, stat.st_mtime_ns, stat.st_dev, stat.st_ino, sha256)

    def matches(self, stat: os.stat_result) -> bool:
        return (
            stat.st_size == self.size
            and stat.st_mtime_ns == self.modified_ns
            and stat.st_dev == self.device
            and stat.st_ino == self.inode
        )


@dataclass
class Receiver:
    receiver_id: str
    name: str
    token: str
    last_seen: float


@dataclass(frozen=True)
class UploadedFile:
    upload_id: str
    filename: str
    snapshot: FileSnapshot


@dataclass(frozen=True)
class DeliveryRecipient:
    receiver_id: str
    receiver_name: str
    transfer_id: str


@dataclass(frozen=True)
class Delivery:
    delivery_id: str
    filename: str
    size: int
    recipients: tuple[DeliveryRecipient, ...]


class TransferServer:
    """Safe, bounded-memory HTTP file service with scheduler-backed rate allocation."""

    RECEIVER_TIMEOUT_SECONDS = 20
    MAX_UPLOAD_SIZE = 1024 * 1024 * 1024

    def __init__(
        self,
        shared_directory: str | Path,
        capacity_bps: float = 20_000_000.0,
        scheduler_name: str = "flashport",
        allocation_interval: float = 0.25,
    ) -> None:
        if not math.isfinite(capacity_bps) or capacity_bps <= 0:
            raise ValueError("capacity_bps must be finite and positive")
        if not math.isfinite(allocation_interval) or allocation_interval <= 0:
            raise ValueError("allocation_interval must be finite and positive")
        self.root = Path(shared_directory).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.upload_root = (self.root / ".flashport-uploads").resolve()
        if not self.upload_root.is_relative_to(self.root):
            raise ValueError("upload directory must be inside the shared directory")
        self.upload_root.mkdir(exist_ok=True)
        self.capacity_bps = float(capacity_bps)
        self.allocation_interval = allocation_interval
        self.manager = TransferManager()
        self.monitor = NetworkMonitor(self.capacity_bps)
        self.estimator = NetworkEstimator()
        self.scheduler_config = SchedulerConfig(total_capacity=self.capacity_bps)
        self._schedulers: dict[str, type[Scheduler]] = {
            "independent": IndependentScheduler,
            "equal_share": EqualShareScheduler,
            "srpt": SRPTScheduler,
            "flashport": FlashportScheduler,
        }
        if scheduler_name not in self._schedulers:
            raise ValueError(f"unknown scheduler: {scheduler_name}")
        self.scheduler_name = scheduler_name
        self.scheduler = self._schedulers[scheduler_name](self.scheduler_config)
        self._tokens: dict[str, str] = {}
        self._snapshots: dict[str, FileSnapshot] = {}
        self._served_ranges: dict[str, list[tuple[int, int]]] = {}
        self._limiters: dict[str, RateLimiter] = {}
        self._progress_times: dict[str, float] = {}
        self._receivers: dict[str, Receiver] = {}
        self._uploads: dict[str, UploadedFile] = {}
        self._deliveries: dict[str, Delivery] = {}
        self._allocation_history: deque[dict[str, object]] = deque(maxlen=200)
        self._scheduler_task: asyncio.Task[None] | None = None
        self.router = APIRouter()
        self._register_routes()

    async def start(self) -> None:
        if self._scheduler_task is None or self._scheduler_task.done():
            self._scheduler_task = asyncio.create_task(self._scheduler_loop())

    async def stop(self) -> None:
        task = self._scheduler_task
        self._scheduler_task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    def _register_routes(self) -> None:
        self.router.add_api_route("/uploads", self.upload_file, methods=["POST"])
        self.router.add_api_route("/receivers", self.list_receivers, methods=["GET"])
        self.router.add_api_route("/receivers", self.register_receiver, methods=["POST"])
        self.router.add_api_route(
            "/receivers/{receiver_id}/heartbeat",
            self.receiver_heartbeat,
            methods=["PUT"],
        )
        self.router.add_api_route(
            "/receivers/{receiver_id}",
            self.unregister_receiver,
            methods=["DELETE"],
        )
        self.router.add_api_route(
            "/receivers/{receiver_id}/deliveries",
            self.receiver_deliveries,
            methods=["GET"],
        )
        self.router.add_api_route("/deliveries", self.create_delivery, methods=["POST"])
        self.router.add_api_route(
            "/deliveries/{delivery_id}",
            self.delivery_status,
            methods=["GET"],
        )
        self.router.add_api_route("/files", self.list_files, methods=["GET"])
        self.router.add_api_route("/files/{filename}/metadata", self.file_metadata, methods=["GET"])
        self.router.add_api_route("/transfers", self.register_transfer, methods=["POST"])
        self.router.add_api_route("/transfers", self.list_transfers, methods=["GET"])
        self.router.add_api_route("/transfers/{transfer_id}/content", self.transfer_content, methods=["GET"])
        self.router.add_api_route("/transfers/{transfer_id}/status", self.transfer_status, methods=["GET"])
        self.router.add_api_route("/transfers/{transfer_id}/progress", self.update_progress, methods=["PUT"])
        self.router.add_api_route("/transfers/{transfer_id}/resume", self.resume_transfer, methods=["PUT"])
        self.router.add_api_route("/transfers/{transfer_id}/complete", self.complete_transfer, methods=["POST"])
        self.router.add_api_route("/transfers/{transfer_id}/cancel", self.cancel_transfer, methods=["POST"])
        self.router.add_api_route("/scheduler", self.scheduler_status, methods=["GET"])
        self.router.add_api_route("/scheduler", self.select_scheduler, methods=["POST"])
        self.router.add_api_route("/network/metrics", self.network_metrics, methods=["GET"])

    @staticmethod
    def _validate_filename(filename: str) -> None:
        stem = filename.split(".", 1)[0].upper() if filename else ""
        reserved_windows_names = {
            "CON", "PRN", "AUX", "NUL",
            *(f"COM{index}" for index in range(1, 10)),
            *(f"LPT{index}" for index in range(1, 10)),
        }
        if (
            not filename
            or filename in {".", ".."}
            or "/" in filename
            or "\\" in filename
            or "\x00" in filename
            or ":" in filename
            or any(ord(char) < 32 or ord(char) == 127 for char in filename)
            or any(char in '<>"|?*' for char in filename)
            or filename.endswith((" ", "."))
            or stem in reserved_windows_names
            or len(filename) > 255
            or PureWindowsPath(filename).name != filename
        ):
            raise HTTPException(status_code=400, detail="Invalid file name.")

    def _resolve_file(self, filename: str) -> Path:
        self._validate_filename(filename)
        candidate = self.root / filename
        try:
            resolved = candidate.resolve(strict=True)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail="File not found.") from exc
        except OSError as exc:
            raise HTTPException(status_code=400, detail="Invalid file path.") from exc
        if not resolved.is_relative_to(self.root) or not resolved.is_file():
            raise HTTPException(status_code=404, detail="File not found.")
        return resolved

    async def upload_file(
        self,
        request: Request,
        filename: str = Query(min_length=1, max_length=255),
    ) -> dict[str, object]:
        self._validate_filename(filename)
        content_length = request.headers.get("content-length")
        if content_length is not None:
            try:
                if int(content_length) > self.MAX_UPLOAD_SIZE:
                    raise HTTPException(status_code=413, detail="Files must be 1 GB or smaller.")
            except ValueError as exc:
                raise HTTPException(status_code=400, detail="Invalid Content-Length header.") from exc

        upload_id = str(uuid.uuid4())
        temporary_path = self.upload_root / f"{upload_id}.partial"
        final_path = self.upload_root / f"{upload_id}.upload"
        digest = hashlib.sha256()
        size = 0
        completed = False
        try:
            with temporary_path.open("xb") as handle:
                async for block in request.stream():
                    size += len(block)
                    if size > self.MAX_UPLOAD_SIZE:
                        raise HTTPException(status_code=413, detail="Files must be 1 GB or smaller.")
                    digest.update(block)
                    await asyncio.to_thread(handle.write, block)
            temporary_path.replace(final_path)
            stat = final_path.stat()
            snapshot = FileSnapshot.from_stat(final_path, stat, digest.hexdigest())
            self._uploads[upload_id] = UploadedFile(upload_id, filename, snapshot)
            completed = True
            return {
                "upload_id": upload_id,
                "filename": filename,
                "size": size,
                "sha256": snapshot.sha256,
            }
        finally:
            if not completed:
                temporary_path.unlink(missing_ok=True)
                final_path.unlink(missing_ok=True)

    async def register_receiver(self, payload: RegisterReceiverRequest) -> dict[str, object]:
        name = payload.name.strip()
        if not name:
            raise HTTPException(status_code=422, detail="Receiver name cannot be blank.")
        receiver_id = str(uuid.uuid4())
        token = secrets.token_urlsafe(32)
        self._receivers[receiver_id] = Receiver(
            receiver_id=receiver_id,
            name=name,
            token=token,
            last_seen=time.monotonic(),
        )
        return {"receiver_id": receiver_id, "token": token, "name": name}

    async def list_receivers(self) -> list[dict[str, object]]:
        now = time.monotonic()
        return [
            {"receiver_id": receiver.receiver_id, "name": receiver.name, "online": True}
            for receiver in self._receivers.values()
            if now - receiver.last_seen <= self.RECEIVER_TIMEOUT_SECONDS
        ]

    def _authorized_receiver(self, receiver_id: str, token: str | None) -> Receiver:
        receiver = self._receivers.get(receiver_id)
        if receiver is None:
            raise HTTPException(status_code=404, detail="Receiver not found.")
        if token is None or not secrets.compare_digest(receiver.token, token):
            raise HTTPException(status_code=401, detail="Invalid receiver token.")
        return receiver

    async def receiver_heartbeat(
        self,
        receiver_id: str,
        token: str | None = Header(default=None, alias="X-FlashPort-Receiver-Token"),
    ) -> dict[str, object]:
        receiver = self._authorized_receiver(receiver_id, token)
        receiver.last_seen = time.monotonic()
        return {"receiver_id": receiver.receiver_id, "name": receiver.name, "online": True}

    async def unregister_receiver(
        self,
        receiver_id: str,
        token: str | None = Header(default=None, alias="X-FlashPort-Receiver-Token"),
    ) -> dict[str, object]:
        receiver = self._authorized_receiver(receiver_id, token)
        del self._receivers[receiver_id]
        for delivery in self._deliveries.values():
            for recipient in delivery.recipients:
                if recipient.receiver_id != receiver_id:
                    continue
                transfer = self.manager.get(recipient.transfer_id)
                if transfer is not None and transfer.state != "completed":
                    transfer.state = "cancelled"
                    await self._limiters[transfer.transfer_id].close()
        return {"receiver_id": receiver.receiver_id, "online": False}

    async def receiver_deliveries(
        self,
        receiver_id: str,
        token: str | None = Header(default=None, alias="X-FlashPort-Receiver-Token"),
    ) -> list[dict[str, object]]:
        receiver = self._authorized_receiver(receiver_id, token)
        receiver.last_seen = time.monotonic()
        jobs: list[dict[str, object]] = []
        for delivery in self._deliveries.values():
            for recipient in delivery.recipients:
                if recipient.receiver_id != receiver_id:
                    continue
                transfer = self.manager.get(recipient.transfer_id)
                if transfer is None or transfer.state in {"completed", "cancelled"}:
                    continue
                jobs.append(
                    {
                        "delivery_id": delivery.delivery_id,
                        "transfer_id": transfer.transfer_id,
                        "token": self._tokens[transfer.transfer_id],
                        "filename": delivery.filename,
                        "size": delivery.size,
                        "sha256": self._snapshots[transfer.transfer_id].sha256,
                        "state": transfer.state,
                    }
                )
        return jobs

    def _create_transfer(
        self,
        filename: str,
        client_id: str,
        snapshot: FileSnapshot,
    ) -> dict[str, object]:
        transfer_id = str(uuid.uuid4())
        token = secrets.token_urlsafe(32)
        transfer = TransferState(
            transfer_id=transfer_id,
            client_id=client_id,
            filename=filename,
            size=snapshot.size,
            active_connections=0,
            start_time=time.time(),
            state="queued",
        )
        self.manager.register(transfer)
        self._tokens[transfer_id] = token
        self._snapshots[transfer_id] = snapshot
        self._served_ranges[transfer_id] = []
        self._limiters[transfer_id] = RateLimiter(self.capacity_bps)
        self._progress_times[transfer_id] = time.monotonic()
        return {
            "transfer_id": transfer_id,
            "token": token,
            "filename": filename,
            "size": snapshot.size,
            "sha256": snapshot.sha256,
            "chunk_size": 1024 * 1024,
            "configured_capacity_bps": self.capacity_bps,
            "selected_scheduler": self.scheduler_name,
        }

    async def create_delivery(self, payload: CreateDeliveryRequest) -> dict[str, object]:
        if len(set(payload.receiver_ids)) != len(payload.receiver_ids):
            raise HTTPException(status_code=422, detail="Receiver IDs must be unique.")
        uploaded = self._uploads.get(payload.upload_id)
        if uploaded is None:
            raise HTTPException(status_code=404, detail="Uploaded file not found.")
        now = time.monotonic()
        receivers: list[Receiver] = []
        for receiver_id in payload.receiver_ids:
            receiver = self._receivers.get(receiver_id)
            if receiver is None or now - receiver.last_seen > self.RECEIVER_TIMEOUT_SECONDS:
                raise HTTPException(status_code=409, detail="One or more selected receivers are offline.")
            receivers.append(receiver)

        recipients: list[DeliveryRecipient] = []
        for receiver in receivers:
            transfer = self._create_transfer(
                uploaded.filename,
                receiver.receiver_id,
                uploaded.snapshot,
            )
            recipients.append(
                DeliveryRecipient(
                    receiver_id=receiver.receiver_id,
                    receiver_name=receiver.name,
                    transfer_id=str(transfer["transfer_id"]),
                )
            )
        delivery_id = str(uuid.uuid4())
        delivery = Delivery(
            delivery_id=delivery_id,
            filename=uploaded.filename,
            size=uploaded.snapshot.size,
            recipients=tuple(recipients),
        )
        self._deliveries[delivery_id] = delivery
        return {
            "delivery_id": delivery_id,
            "filename": delivery.filename,
            "size": delivery.size,
            "receivers": [
                {"receiver_id": item.receiver_id, "name": item.receiver_name}
                for item in delivery.recipients
            ],
        }

    async def delivery_status(self, delivery_id: str) -> dict[str, object]:
        delivery = self._deliveries.get(delivery_id)
        if delivery is None:
            raise HTTPException(status_code=404, detail="Delivery not found.")
        receivers: list[dict[str, object]] = []
        for recipient in delivery.recipients:
            transfer = self.manager.get(recipient.transfer_id)
            if transfer is None:
                continue
            receivers.append(
                {
                    "receiver_id": recipient.receiver_id,
                    "name": recipient.receiver_name,
                    "state": transfer.state,
                    "downloaded": transfer.downloaded,
                    "size": transfer.size,
                    "progress": transfer.downloaded / transfer.size * 100 if transfer.size else (
                        100.0 if transfer.state == "completed" else 0.0
                    ),
                }
            )
        return {
            "delivery_id": delivery.delivery_id,
            "filename": delivery.filename,
            "size": delivery.size,
            "receivers": receivers,
        }

    @staticmethod
    def _read_file_snapshot(path: Path) -> FileSnapshot:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            before = os.fstat(handle.fileno())
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
            after = os.fstat(handle.fileno())
        before_identity = (before.st_size, before.st_mtime_ns, before.st_dev, before.st_ino)
        after_identity = (after.st_size, after.st_mtime_ns, after.st_dev, after.st_ino)
        if before_identity != after_identity:
            raise RuntimeError("shared file changed while its metadata was being read")
        return FileSnapshot.from_stat(path, after, digest.hexdigest())

    @staticmethod
    def _stat_identity(path: Path) -> tuple[int, int, int, int]:
        stat = path.stat()
        return stat.st_size, stat.st_mtime_ns, stat.st_dev, stat.st_ino

    async def list_files(self) -> list[dict[str, object]]:
        items: list[dict[str, object]] = []
        for candidate in sorted(self.root.iterdir(), key=lambda item: item.name.casefold()):
            try:
                resolved = self._resolve_file(candidate.name)
            except HTTPException:
                continue
            stat = resolved.stat()
            items.append(
                {
                    "filename": candidate.name,
                    "size": stat.st_size,
                    "modified_time": stat.st_mtime,
                }
            )
        return items

    async def file_metadata(self, filename: str) -> dict[str, object]:
        path = self._resolve_file(filename)
        try:
            snapshot = await asyncio.to_thread(self._read_file_snapshot, path)
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {
            "filename": filename,
            "size": snapshot.size,
            "modified_time": snapshot.modified_ns / 1_000_000_000,
            "sha256": snapshot.sha256,
        }

    async def register_transfer(self, payload: RegisterTransferRequest) -> dict[str, object]:
        path = self._resolve_file(payload.filename)
        try:
            snapshot = await asyncio.to_thread(self._read_file_snapshot, path)
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return self._create_transfer(payload.filename, payload.client_id, snapshot)

    def _authorized_transfer(self, transfer_id: str, token: str | None) -> TransferState:
        transfer = self.manager.get(transfer_id)
        expected = self._tokens.get(transfer_id)
        if transfer is None or expected is None:
            raise HTTPException(status_code=404, detail="Transfer not found.")
        if token is None or not secrets.compare_digest(expected, token):
            raise HTTPException(status_code=401, detail="Invalid transfer token.")
        return transfer

    def _check_snapshot(self, transfer_id: str, handle) -> FileSnapshot:
        snapshot = self._snapshots[transfer_id]
        if not snapshot.matches(os.fstat(handle.fileno())):
            raise HTTPException(status_code=409, detail="Shared file changed during transfer; register again.")
        return snapshot

    async def transfer_content(
        self,
        transfer_id: str,
        request: Request,
        range_header: str | None = Header(default=None, alias="Range"),
        token: str | None = Header(default=None, alias="X-FlashPort-Token"),
    ) -> StreamingResponse:
        transfer = self._authorized_transfer(transfer_id, token)
        if transfer.state in {"completed", "cancelled"}:
            raise HTTPException(status_code=409, detail=f"Transfer is {transfer.state}.")
        if transfer.size and transfer.remaining == 0:
            raise HTTPException(status_code=409, detail="No bytes remain in this transfer.")
        snapshot = self._snapshots[transfer_id]
        try:
            current_stat = await asyncio.to_thread(snapshot.path.stat)
        except OSError as exc:
            raise HTTPException(status_code=409, detail="Shared file is no longer available.") from exc
        if not snapshot.matches(current_stat):
            raise HTTPException(status_code=409, detail="Shared file changed during transfer; register again.")
        start, end, status_code = self._parse_range(range_header, snapshot.size)
        content_length = max(0, end - start + 1)
        headers = {
            "Accept-Ranges": "bytes",
            "Content-Length": str(content_length),
            "Content-Disposition": f"attachment; filename*=UTF-8''{quote(transfer.filename, safe='')}",
            "X-Content-Type-Options": "nosniff",
            "Cache-Control": "no-store",
        }
        if status_code == 206:
            headers["Content-Range"] = f"bytes {start}-{end}/{snapshot.size}"

        async def body() -> AsyncIterator[bytes]:
            transfer.active_connections += 1
            if transfer.start_time is None:
                transfer.start_time = time.time()
            transfer.state = "active"
            handle = None
            try:
                handle = await asyncio.to_thread(snapshot.path.open, "rb")
                self._check_snapshot(transfer_id, handle)
                await asyncio.to_thread(handle.seek, start)
                remaining = content_length
                limiter = self._limiters[transfer_id]
                while remaining > 0:
                    if transfer.state == "cancelled":
                        raise asyncio.CancelledError
                    block = await asyncio.to_thread(handle.read, min(STREAM_CHUNK_SIZE, remaining))
                    if not block:
                        raise HTTPException(status_code=409, detail="File ended before the requested range.")
                    await limiter.consume(len(block))
                    transfer.bytes_sent += len(block)
                    self.monitor.record_server_bytes(transfer_id, len(block))
                    yield block
                    block_start = start + content_length - remaining
                    self._record_served_range(transfer_id, block_start, block_start + len(block) - 1)
                    remaining -= len(block)
            except asyncio.CancelledError:
                self.monitor.record_connection_failure()
                raise
            except (ConnectionError, OSError):
                self.monitor.record_connection_failure()
                raise
            finally:
                if handle is not None:
                    await asyncio.to_thread(handle.close)
                transfer.active_connections = max(0, transfer.active_connections - 1)
                if transfer.active_connections == 0 and transfer.state == "active":
                    transfer.state = "paused"

        return StreamingResponse(
            body(),
            status_code=status_code,
            headers=headers,
            media_type="application/octet-stream",
        )

    def _record_served_range(self, transfer_id: str, start: int, end: int) -> None:
        ranges = sorted([*self._served_ranges[transfer_id], (start, end)])
        merged: list[tuple[int, int]] = []
        for current_start, current_end in ranges:
            if merged and current_start <= merged[-1][1] + 1:
                previous_start, previous_end = merged[-1]
                merged[-1] = (previous_start, max(previous_end, current_end))
            else:
                merged.append((current_start, current_end))
        self._served_ranges[transfer_id] = merged
        transfer = self.manager.get(transfer_id)
        if transfer is not None:
            transfer.served_bytes = sum(right - left + 1 for left, right in merged)
            self.monitor.record_unique_served(transfer_id, transfer.served_bytes)

    @staticmethod
    def _parse_range(range_header: str | None, size: int) -> tuple[int, int, int]:
        if range_header is None:
            return (0, size - 1, 200) if size else (0, -1, 200)
        match = _RANGE_RE.fullmatch(range_header.strip())
        if match is None or "," in range_header or size == 0:
            raise HTTPException(
                status_code=416,
                detail="Only one valid byte range is supported.",
                headers={"Content-Range": f"bytes */{size}"},
            )
        first, last = match.groups()
        if not first and not last:
            start = end = -1
        elif not first:
            suffix_length = int(last)
            start = max(0, size - suffix_length)
            end = size - 1
        else:
            start = int(first)
            end = min(int(last), size - 1) if last else size - 1
        if start < 0 or start >= size or end < start:
            raise HTTPException(
                status_code=416,
                detail="Requested byte range is not satisfiable.",
                headers={"Content-Range": f"bytes */{size}"},
            )
        return start, end, 206

    async def update_progress(
        self,
        transfer_id: str,
        payload: ProgressRequest,
        token: str | None = Header(default=None, alias="X-FlashPort-Token"),
    ) -> dict[str, object]:
        transfer = self._authorized_transfer(transfer_id, token)
        if transfer.state in {"completed", "cancelled"}:
            raise HTTPException(status_code=409, detail=f"Transfer is {transfer.state}.")
        if payload.downloaded_bytes < transfer.downloaded or payload.downloaded_bytes > transfer.size:
            raise HTTPException(status_code=409, detail="Progress must be monotonic and within file size.")
        now = time.monotonic()
        elapsed = max(now - self._progress_times.get(transfer_id, now), 1e-6)
        delta = payload.downloaded_bytes - transfer.downloaded
        if delta:
            transfer.service_history.append(delta / elapsed)
            if len(transfer.service_history) > self.scheduler_config.service_window:
                del transfer.service_history[:-self.scheduler_config.service_window]
        transfer.downloaded = payload.downloaded_bytes
        transfer.goodput = payload.goodput_bps or (delta / elapsed if delta else transfer.goodput)
        transfer.http_request_latency_ms = payload.http_request_latency_ms
        self._progress_times[transfer_id] = now
        self.monitor.record_client_progress(
            transfer_id,
            payload.downloaded_bytes,
            payload.goodput_bps,
            payload.http_request_latency_ms,
            active=transfer.state == "active",
        )
        if payload.http_request_latency_ms is not None:
            self.estimator.record_request_latency(payload.http_request_latency_ms)
        return transfer.to_snapshot()

    @staticmethod
    def _range_hash(snapshot: FileSnapshot, start: int, end: int) -> str:
        digest = hashlib.sha256()
        with snapshot.path.open("rb") as handle:
            if not snapshot.matches(os.fstat(handle.fileno())):
                raise RuntimeError("shared file changed before validating a resumed range")
            handle.seek(start)
            remaining = end - start + 1
            while remaining:
                block = handle.read(min(STREAM_CHUNK_SIZE, remaining))
                if not block:
                    raise RuntimeError("shared file changed while validating a resumed range")
                digest.update(block)
                remaining -= len(block)
            if not snapshot.matches(os.fstat(handle.fileno())):
                raise RuntimeError("shared file changed while validating a resumed range")
        return digest.hexdigest()

    async def resume_transfer(
        self,
        transfer_id: str,
        payload: ResumeRequest,
        token: str | None = Header(default=None, alias="X-FlashPort-Token"),
    ) -> dict[str, object]:
        transfer = self._authorized_transfer(transfer_id, token)
        if transfer.state in {"completed", "cancelled"}:
            raise HTTPException(status_code=409, detail=f"Transfer is {transfer.state}.")
        snapshot = self._snapshots[transfer_id]
        validated: list[tuple[int, int]] = []
        for chunk in payload.chunks:
            if chunk.start > chunk.end or chunk.end >= snapshot.size:
                raise HTTPException(status_code=422, detail="A resumed byte range is outside the file.")
            try:
                actual_hash = await asyncio.to_thread(
                    self._range_hash,
                    snapshot,
                    chunk.start,
                    chunk.end,
                )
            except RuntimeError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            if not secrets.compare_digest(actual_hash, chunk.sha256):
                raise HTTPException(status_code=422, detail="A resumed chunk failed SHA-256 validation.")
            validated.append((chunk.start, chunk.end))
        for start, end in validated:
            self._record_served_range(transfer_id, start, end)
        return {
            "transfer_id": transfer_id,
            "validated_ranges": len(validated),
            "served_bytes": transfer.served_bytes,
        }

    async def complete_transfer(
        self,
        transfer_id: str,
        payload: dict[str, str],
        token: str | None = Header(default=None, alias="X-FlashPort-Token"),
    ) -> dict[str, object]:
        transfer = self._authorized_transfer(transfer_id, token)
        if transfer.state == "cancelled":
            raise HTTPException(status_code=409, detail="Cancelled transfers cannot be completed.")
        expected = self._snapshots[transfer_id].sha256
        if transfer.state == "completed":
            if payload.get("sha256") == expected:
                return transfer.to_snapshot()
            raise HTTPException(status_code=422, detail="SHA-256 verification failed.")
        if transfer.downloaded != transfer.size:
            raise HTTPException(status_code=409, detail="Cannot complete before all bytes are reported.")
        if transfer.served_bytes != transfer.size:
            raise HTTPException(status_code=409, detail="Server has not completed serving every byte range.")
        if payload.get("sha256") != expected:
            raise HTTPException(status_code=422, detail="SHA-256 verification failed.")
        transfer.mark_completed(time.time())
        flow_duration = max(0.0, transfer.completion_time - (transfer.start_time or transfer.completion_time))
        transfer.goodput = transfer.size / flow_duration if transfer.size and flow_duration > 0 else 0.0
        self.monitor.record_completion(
            transfer.size,
            transfer.start_time or transfer.completion_time,
            transfer.completion_time,
        )
        self.monitor.record_client_progress(
            transfer_id,
            transfer.size,
            transfer.goodput,
            transfer.http_request_latency_ms,
            active=False,
        )
        await self._limiters[transfer_id].set_rate(0.0)
        return transfer.to_snapshot()

    async def cancel_transfer(
        self,
        transfer_id: str,
        token: str | None = Header(default=None, alias="X-FlashPort-Token"),
    ) -> dict[str, object]:
        transfer = self._authorized_transfer(transfer_id, token)
        if transfer.state != "completed":
            transfer.state = "cancelled"
            await self._limiters[transfer_id].close()
        return transfer.to_snapshot()

    async def list_transfers(self) -> list[dict[str, object]]:
        return [transfer.to_snapshot() for transfer in self.manager.all()]

    async def transfer_status(
        self,
        transfer_id: str,
        token: str | None = Header(default=None, alias="X-FlashPort-Token"),
    ) -> dict[str, object]:
        transfer = self._authorized_transfer(transfer_id, token)
        snapshot = self._snapshots[transfer_id]
        try:
            current_stat = await asyncio.to_thread(snapshot.path.stat)
        except OSError as exc:
            raise HTTPException(status_code=409, detail="Shared file is no longer available.") from exc
        if not snapshot.matches(current_stat):
            raise HTTPException(status_code=409, detail="Shared file changed; register a new transfer.")
        return {
            **transfer.to_snapshot(),
            "sha256": snapshot.sha256,
        }

    async def select_scheduler(self, payload: SchedulerSelection) -> dict[str, object]:
        if payload.policy not in self._schedulers:
            raise HTTPException(
                status_code=422,
                detail=f"policy must be one of {', '.join(sorted(self._schedulers))}.",
            )
        if payload.configured_capacity_bps is not None:
            self.capacity_bps = payload.configured_capacity_bps
            self.monitor.configured_capacity_bps = self.capacity_bps
            self.scheduler_config = SchedulerConfig(total_capacity=self.capacity_bps)
            for limiter in self._limiters.values():
                limiter.capacity = self.capacity_bps
        self.scheduler_name = payload.policy
        self.scheduler = self._schedulers[self.scheduler_name](self.scheduler_config)
        return await self.scheduler_status()

    async def scheduler_status(self) -> dict[str, object]:
        active = [
            transfer
            for transfer in self.manager.active()
            if transfer.state == "active"
            and transfer.active_connections > 0
            and transfer.remaining > 0
        ]
        return {
            "measurement_type": "actual_application",
            "selected_policy": self.scheduler_name,
            "configured_capacity_bps": self.capacity_bps,
            "active_transfer_count": len(active),
            "allocations": {
                transfer.transfer_id: transfer.last_rate
                for transfer in active
            },
            "allocation_history": list(self._allocation_history),
        }

    async def network_metrics(self) -> dict[str, object]:
        return {
            **self.monitor.snapshot(),
            **self.estimator.snapshot(),
        }

    async def _scheduler_loop(self) -> None:
        try:
            while True:
                active = [
                    transfer
                    for transfer in self.manager.active()
                    if transfer.state == "active"
                    and transfer.active_connections > 0
                    and transfer.remaining > 0
                ]
                allocations = self.scheduler.allocate(active, self.capacity_bps)
                if not isinstance(allocations, dict):
                    raise RuntimeError("scheduler must return a transfer-to-rate mapping")
                active_ids = {transfer.transfer_id for transfer in active}
                if set(allocations) - active_ids:
                    raise RuntimeError("scheduler allocated a rate to an inactive transfer")
                if any(not isinstance(rate, (int, float)) or not math.isfinite(rate) or rate < 0 for rate in allocations.values()):
                    raise RuntimeError("scheduler produced a non-finite or negative rate")
                total_allocated = sum(allocations.values())
                if total_allocated > self.capacity_bps + max(1.0, self.capacity_bps) * 1e-10:
                    raise RuntimeError("scheduler allocation exceeds configured capacity")
                decision_time = time.time()
                for transfer in active:
                    rate = allocations.get(transfer.transfer_id, 0.0)
                    if not math.isfinite(rate) or rate < 0:
                        raise RuntimeError("scheduler produced an invalid rate")
                    transfer.last_rate = rate
                    await self._limiters[transfer.transfer_id].set_rate(rate)
                if active:
                    self._allocation_history.append(
                        {
                            "timestamp": decision_time,
                            "policy": self.scheduler_name,
                            "total_capacity_bps": self.capacity_bps,
                            "total_allocated_bps": total_allocated,
                            "allocations": dict(allocations),
                        }
                    )
                await asyncio.sleep(self.allocation_interval)
        except asyncio.CancelledError:
            raise

