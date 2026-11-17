"""Unified bidirectional TransferManager coordinating concurrent HTTP uploads and downloads."""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

import aiohttp

from flowweave.config import TransferConfig
from flowweave.models import (
    ChunkRecord,
    ChunkState,
    ScheduleDecision,
    TransferDirection,
    TransferSnapshot,
    TransferState,
)
from flowweave.monitor.monitor import NetworkMonitor
from flowweave.storage import Storage
from flowweave.straggler.detector import ActiveChunkView
from flowweave.transfer.engine import HTTPDownloadEngine
from flowweave.transfer.rate_limiter import AsyncRateLimiter
from flowweave.transfer.security import sanitize_filename
from flowweave.transfer.tcp_client import TCPDownloadEngine, TCPUploadEngine
from flowweave.transfer.upload_client import HTTPUploadEngine


@dataclass
class _ManagedTransfer:
    transfer_id: str
    direction: TransferDirection
    url: str
    filename: str
    source: str
    destination: str
    dest_path: Path
    candidate_links: list[str]
    assigned_link: str
    priority_weight: float
    expected_sha256: str | None
    created_at: float
    limiter: AsyncRateLimiter
    pause_event: asyncio.Event = field(default_factory=asyncio.Event)
    source_file_path: Path | None = None
    state: TransferState = TransferState.PENDING
    total_bytes: int = 0
    transferred_bytes: int = 0
    total_chunks: int = 0
    chunks: list[ChunkRecord] = field(default_factory=list)
    started_at: float | None = None
    completed_at: float | None = None
    sha256: str | None = None
    error: str | None = None
    task: asyncio.Task | None = None
    pending_bytes: int = 0
    pending_retries: int = 0
    last_rtt_ms: float | None = None

    def __post_init__(self) -> None:
        self.pause_event.set()  # Unpaused initially


class TransferManager:
    """Manages concurrent async uploads and downloads under one unified scheduling model."""

    def __init__(
        self,
        *,
        config: TransferConfig | None = None,
        monitor: NetworkMonitor | None = None,
        storage: Storage | None = None,
    ) -> None:
        self.config = config or TransferConfig()
        self.monitor = monitor or NetworkMonitor()
        self.storage = storage
        self.download_engine = HTTPDownloadEngine(self.config)
        self.upload_engine = HTTPUploadEngine(self.config)
        self.tcp_download_engine = TCPDownloadEngine(self.config)
        self.tcp_upload_engine = TCPUploadEngine(self.config)
        self._transfers: dict[str, _ManagedTransfer] = {}
        self._session: aiohttp.ClientSession | None = None

    async def start(self) -> None:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(limit=64, force_close=False)
            self._session = aiohttp.ClientSession(connector=connector)

    async def shutdown(self) -> None:
        tasks = [t.task for t in self._transfers.values() if t.task and not t.task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None

    async def submit(
        self,
        url: str,
        *,
        filename: str | None = None,
        expected_size_bytes: int = 0,
        candidate_links: list[str] | None = None,
        priority_weight: float = 1.0,
        expected_sha256: str | None = None,
    ) -> TransferSnapshot:
        """Backwards-compatible alias for `submit_download`."""
        return await self.submit_download(
            url,
            filename=filename,
            expected_size_bytes=expected_size_bytes,
            candidate_links=candidate_links,
            priority_weight=priority_weight,
            expected_sha256=expected_sha256,
        )

    async def submit_download(
        self,
        url: str,
        *,
        filename: str | None = None,
        expected_size_bytes: int = 0,
        candidate_links: list[str] | None = None,
        priority_weight: float = 1.0,
        expected_sha256: str | None = None,
    ) -> TransferSnapshot:
        """Queue and start a concurrent HTTP file download."""
        await self.start()
        tid = f"dl-{uuid.uuid4().hex[:8]}"
        inferred = filename or Path(urlparse(url).path).name or f"{tid}.bin"
        safe_name = sanitize_filename(inferred, fallback=f"{tid}.bin")
        dest_path = self.config.download_dir / f"{tid}_{safe_name}"
        links = candidate_links or ["downlink"]
        now = time.monotonic()

        mt = _ManagedTransfer(
            transfer_id=tid,
            direction=TransferDirection.DOWNLOAD,
            url=url,
            filename=safe_name,
            source=url,
            destination=str(dest_path),
            dest_path=dest_path,
            candidate_links=list(links),
            assigned_link=links[0],
            priority_weight=priority_weight,
            expected_sha256=expected_sha256,
            created_at=now,
            total_bytes=expected_size_bytes,
            limiter=AsyncRateLimiter(self.config.total_link_capacity_bps),
        )
        self._transfers[tid] = mt
        self.monitor.register_transfer(
            tid, max(expected_size_bytes, 1), now, direction=TransferDirection.DOWNLOAD
        )
        mt.task = asyncio.create_task(self._run_download(mt))
        snap = self._to_snapshot(mt)
        if self.storage is not None:
            self.storage.upsert_transfer(snap)
        return snap

    async def submit_upload(
        self,
        file_path: str | Path,
        server_base_url: str,
        *,
        filename: str | None = None,
        candidate_links: list[str] | None = None,
        priority_weight: float = 1.0,
    ) -> TransferSnapshot:
        """Queue and start a concurrent chunked HTTP file upload."""
        await self.start()
        src_path = Path(file_path)
        if not src_path.is_file():
            raise FileNotFoundError(f"Upload file not found: {src_path}")

        tid = f"ul-{uuid.uuid4().hex[:8]}"
        safe_name = sanitize_filename(filename or src_path.name, fallback=f"{tid}.bin")
        total_bytes = src_path.stat().st_size
        links = candidate_links or ["uplink"]
        now = time.monotonic()

        mt = _ManagedTransfer(
            transfer_id=tid,
            direction=TransferDirection.UPLOAD,
            url=f"{server_base_url.rstrip('/')}/upload",
            filename=safe_name,
            source=str(src_path),
            destination=f"{server_base_url.rstrip('/')}/upload/{safe_name}",
            dest_path=self.config.upload_dir / f"{tid}_{safe_name}",
            source_file_path=src_path,
            candidate_links=list(links),
            assigned_link=links[0],
            priority_weight=priority_weight,
            expected_sha256=None,
            created_at=now,
            total_bytes=total_bytes,
            limiter=AsyncRateLimiter(self.config.upload_link_capacity_bps),
        )
        self._transfers[tid] = mt
        self.monitor.register_transfer(
            tid, max(total_bytes, 1), now, direction=TransferDirection.UPLOAD
        )
        mt.task = asyncio.create_task(self._run_upload(mt, server_base_url))
        snap = self._to_snapshot(mt)
        if self.storage is not None:
            self.storage.upsert_transfer(snap)
        return snap

    async def pause(self, transfer_id: str) -> bool:
        mt = self._transfers.get(transfer_id)
        if mt is None or mt.state != TransferState.ACTIVE:
            return False
        mt.pause_event.clear()
        mt.state = TransferState.PAUSED
        if self.storage is not None:
            self.storage.upsert_transfer(self._to_snapshot(mt))
        return True

    async def resume(self, transfer_id: str) -> bool:
        mt = self._transfers.get(transfer_id)
        if mt is None or mt.state != TransferState.PAUSED:
            return False
        mt.state = TransferState.ACTIVE
        mt.pause_event.set()
        if self.storage is not None:
            self.storage.upsert_transfer(self._to_snapshot(mt))
        return True

    async def cancel(self, transfer_id: str) -> bool:
        mt = self._transfers.get(transfer_id)
        if mt is None:
            return False
        mt.pause_event.set()  # Unblock paused workers so cancellation propagates immediately
        if mt.task is not None and not mt.task.done():
            mt.task.cancel()
            try:
                await mt.task
            except asyncio.CancelledError:
                pass
        if mt.state in (TransferState.PENDING, TransferState.ACTIVE, TransferState.PAUSED):
            mt.state = TransferState.CANCELLED
            mt.completed_at = time.monotonic()
            if self.storage is not None:
                self.storage.upsert_transfer(self._to_snapshot(mt))
        return True

    def clear_finished(self) -> int:
        """Remove completed, failed, and cancelled transfers from active view."""
        done_ids = [
            tid
            for tid, mt in self._transfers.items()
            if mt.state in (TransferState.COMPLETED, TransferState.FAILED, TransferState.CANCELLED)
        ]
        for tid in done_ids:
            self._transfers.pop(tid, None)
            self.monitor.remove_transfer(tid)
        return len(done_ids)

    async def wait_all(self, timeout: float | None = None) -> None:
        tasks = [t.task for t in self._transfers.values() if t.task is not None]
        if tasks:
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=timeout)

    def flush_metrics(self, now: float, dt: float) -> dict[str, float]:
        """Push accumulated upload/download progress into NetworkMonitor and update links."""
        link_bytes: dict[str, int] = {}
        link_counts: dict[str, int] = {}

        for mt in self._transfers.values():
            if mt.state == TransferState.ACTIVE:
                link_counts[mt.assigned_link] = link_counts.get(mt.assigned_link, 0) + 1
            if mt.state != TransferState.ACTIVE and mt.pending_bytes == 0 and mt.pending_retries == 0:
                continue

            useful = mt.pending_bytes
            retries = mt.pending_retries
            rtt = mt.last_rtt_ms
            mt.pending_bytes = 0
            mt.pending_retries = 0

            link_bytes[mt.assigned_link] = link_bytes.get(mt.assigned_link, 0) + useful
            rem = max(0, mt.total_bytes - mt.transferred_bytes) if mt.total_bytes > 0 else 0
            link_cap = (
                self.config.upload_link_capacity_bps
                if mt.direction == TransferDirection.UPLOAD
                else self.config.total_link_capacity_bps
            )
            self.monitor.record_progress(
                mt.transfer_id,
                now=now,
                dt=dt,
                useful_bytes=useful,
                remaining_bytes=rem,
                rtt_ms=rtt,
                retries_delta=retries,
                link_capacity_bps=link_cap,
            )
            if self.storage is not None:
                self.storage.upsert_transfer(self._to_snapshot(mt))

        link_rates = {lid: b / dt if dt > 0 else 0.0 for lid, b in link_bytes.items()}
        for lid in list(self.monitor.get_links().keys()):
            self.monitor.update_link(
                lid,
                measured_throughput_bps=link_rates.get(lid, 0.0),
                active_transfers=link_counts.get(lid, 0),
                requested_bps=sum(
                    t.limiter.rate_bps
                    for t in self._transfers.values()
                    if t.state == TransferState.ACTIVE and t.assigned_link == lid
                ),
            )
        return link_rates

    def apply_decision(self, decision: ScheduleDecision) -> None:
        """Apply validated per-transfer rate allocations and link assignments."""
        for tid, rate_bps in decision.allocations_bps.items():
            mt = self._transfers.get(tid)
            if mt is None or mt.state != TransferState.ACTIVE:
                continue
            mt.limiter.set_rate(rate_bps)
            if tid in decision.link_assignments:
                mt.assigned_link = decision.link_assignments[tid]

    def active_chunk_views(self, now: float) -> list[ActiveChunkView]:
        views: list[ActiveChunkView] = []
        for mt in self._transfers.values():
            if mt.state != TransferState.ACTIVE:
                continue
            in_flight = [c for c in mt.chunks if c.state == ChunkState.IN_FLIGHT and c.started_at is not None]
            per_chunk_alloc = mt.limiter.rate_bps / max(1, len(in_flight))
            for ch in in_flight:
                age = max(0.05, now - ch.started_at)  # type: ignore[operator]
                views.append(
                    ActiveChunkView(
                        chunk_id=ch.chunk_id,
                        transfer_id=mt.transfer_id,
                        link_id=mt.assigned_link,
                        candidate_links=mt.candidate_links,
                        total_bytes=ch.total_bytes,
                        remaining_bytes=ch.remaining_bytes,
                        started_at=ch.started_at,  # type: ignore[arg-type]
                        observed_rate_bps=ch.downloaded_bytes / age,
                        allocated_rate_bps=per_chunk_alloc,
                    )
                )
        return views

    def list_snapshots(self) -> list[TransferSnapshot]:
        return [self._to_snapshot(mt) for mt in self._transfers.values()]

    def get_snapshot(self, transfer_id: str) -> TransferSnapshot | None:
        mt = self._transfers.get(transfer_id)
        return self._to_snapshot(mt) if mt else None

    async def _run_download(self, mt: _ManagedTransfer) -> None:
        assert self._session is not None
        mt.state = TransferState.ACTIVE
        mt.started_at = time.monotonic()

        def on_metadata(total_bytes: int, total_chunks: int) -> None:
            mt.total_bytes = total_bytes
            mt.total_chunks = total_chunks

        def on_progress(delta_bytes: int, rtt_ms: float | None, retries: int) -> None:
            mt.transferred_bytes += delta_bytes
            mt.pending_bytes += delta_bytes
            mt.pending_retries += retries
            if rtt_ms is not None:
                mt.last_rtt_ms = rtt_ms

        try:
            if mt.url.startswith("tcp://"):
                res = await self.tcp_download_engine.download(
                    transfer_id=mt.transfer_id,
                    url=mt.url,
                    dest_path=mt.dest_path,
                    rate_limiter=mt.limiter,
                    expected_sha256=mt.expected_sha256,
                    pause_event=mt.pause_event,
                    on_metadata=on_metadata,
                    on_progress=on_progress,
                    chunk_records=mt.chunks,
                )
            else:
                res = await self.download_engine.download(
                    session=self._session,
                    transfer_id=mt.transfer_id,
                    url=mt.url,
                    dest_path=mt.dest_path,
                    rate_limiter=mt.limiter,
                    expected_sha256=mt.expected_sha256,
                    pause_event=mt.pause_event,
                    on_metadata=on_metadata,
                    on_progress=on_progress,
                    chunk_records=mt.chunks,
                )
            mt.total_bytes = res.total_bytes
            mt.transferred_bytes = res.total_bytes
            mt.total_chunks = res.total_chunks
            mt.sha256 = res.sha256
            mt.state = TransferState.COMPLETED
            mt.completed_at = time.monotonic()
        except asyncio.CancelledError:
            mt.state = TransferState.CANCELLED
            mt.completed_at = time.monotonic()
            raise
        except Exception as exc:
            mt.state = TransferState.FAILED
            mt.error = str(exc)
            mt.completed_at = time.monotonic()
        finally:
            if self.storage is not None:
                self.storage.upsert_transfer(self._to_snapshot(mt))

    async def _run_upload(self, mt: _ManagedTransfer, server_base_url: str) -> None:
        assert self._session is not None and mt.source_file_path is not None
        mt.state = TransferState.ACTIVE
        mt.started_at = time.monotonic()

        def on_metadata(total_bytes: int, total_chunks: int) -> None:
            mt.total_bytes = total_bytes
            mt.total_chunks = total_chunks

        def on_progress(delta_bytes: int, rtt_ms: float | None, retries: int) -> None:
            mt.transferred_bytes += delta_bytes
            mt.pending_bytes += delta_bytes
            mt.pending_retries += retries
            if rtt_ms is not None:
                mt.last_rtt_ms = rtt_ms

        try:
            if server_base_url.startswith("tcp://"):
                res = await self.tcp_upload_engine.upload(
                    transfer_id=mt.transfer_id,
                    file_path=mt.source_file_path,
                    server_url=server_base_url,
                    rate_limiter=mt.limiter,
                    filename=mt.filename,
                    pause_event=mt.pause_event,
                    on_metadata=on_metadata,
                    on_progress=on_progress,
                    chunk_records=mt.chunks,
                )
            else:
                res = await self.upload_engine.upload(
                    session=self._session,
                    transfer_id=mt.transfer_id,
                    file_path=mt.source_file_path,
                    server_base_url=server_base_url,
                    rate_limiter=mt.limiter,
                    filename=mt.filename,
                    pause_event=mt.pause_event,
                    on_metadata=on_metadata,
                    on_progress=on_progress,
                    chunk_records=mt.chunks,
                )
            mt.total_bytes = res.total_bytes
            mt.transferred_bytes = res.total_bytes
            mt.total_chunks = res.total_chunks
            mt.sha256 = res.sha256
            mt.dest_path = Path(res.dest_path)
            mt.destination = res.dest_path
            mt.state = TransferState.COMPLETED
            mt.completed_at = time.monotonic()
        except asyncio.CancelledError:
            mt.state = TransferState.CANCELLED
            mt.completed_at = time.monotonic()
            raise
        except Exception as exc:
            mt.state = TransferState.FAILED
            mt.error = str(exc)
            mt.completed_at = time.monotonic()
        finally:
            if self.storage is not None:
                self.storage.upsert_transfer(self._to_snapshot(mt))

    def _to_snapshot(self, mt: _ManagedTransfer) -> TransferSnapshot:
        measured, estimated = self.monitor.get_transfer_metrics(mt.transfer_id)
        remaining = max(0, mt.total_bytes - mt.transferred_bytes) if mt.total_bytes > 0 else 0
        if mt.state == TransferState.COMPLETED:
            remaining = 0
        active_chunks = sum(1 for c in mt.chunks if c.state == ChunkState.IN_FLIGHT)
        completed_chunks = sum(1 for c in mt.chunks if c.state == ChunkState.COMPLETED)

        return TransferSnapshot(
            transfer_id=mt.transfer_id,
            direction=mt.direction,
            filename=mt.filename,
            url=mt.url,
            source=mt.source,
            destination=mt.destination,
            total_bytes=mt.total_bytes,
            remaining_bytes=remaining,
            state=mt.state,
            candidate_links=list(mt.candidate_links),
            assigned_link=mt.assigned_link,
            route=[mt.assigned_link],
            created_at=mt.created_at,
            started_at=mt.started_at,
            completed_at=mt.completed_at,
            allocated_bps=mt.limiter.rate_bps if mt.state == TransferState.ACTIVE else 0.0,
            active_chunks=active_chunks,
            active_workers=active_chunks,
            completed_chunks=completed_chunks,
            total_chunks=mt.total_chunks,
            priority_weight=mt.priority_weight,
            measured=measured,
            estimated=estimated,
            dest_path=str(mt.dest_path) if mt.state == TransferState.COMPLETED else None,
            sha256=mt.sha256,
            error=mt.error,
        )
