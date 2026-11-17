"""Asynchronous chunked HTTP upload client with parallel workers, retries, pause/resume, and SHA-256."""

from __future__ import annotations

import asyncio
import hashlib
import math
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import aiohttp

from flowweave.config import TransferConfig
from flowweave.models import ChunkRecord, ChunkState
from flowweave.transfer.engine import compute_sha256
from flowweave.transfer.rate_limiter import AsyncRateLimiter

UploadProgressCallback = Callable[[int, float | None, int], None]


@dataclass
class UploadResult:
    session_id: str
    dest_path: str
    total_bytes: int
    sha256: str
    total_chunks: int
    retries: int
    resumed_chunks: int


class HTTPUploadEngine:
    """Uploads a local file in parallel paced chunks to a FlowWeave upload server."""

    def __init__(self, config: TransferConfig | None = None) -> None:
        self.config = config or TransferConfig()

    async def upload(
        self,
        *,
        session: aiohttp.ClientSession,
        transfer_id: str,
        file_path: Path,
        server_base_url: str,
        rate_limiter: AsyncRateLimiter,
        filename: str | None = None,
        pause_event: asyncio.Event | None = None,
        on_metadata: Callable[[int, int], None] | None = None,
        on_progress: UploadProgressCallback | None = None,
        chunk_records: list[ChunkRecord] | None = None,
    ) -> UploadResult:
        """Chunk, transmit in parallel with rate pacing, and atomically finalize a file upload."""
        path = Path(file_path)
        if not path.is_file():
            raise FileNotFoundError(f"Upload source file does not exist: {path}")

        total_bytes = path.stat().st_size
        chunk_size = self.config.chunk_size_bytes
        expected_sha = await asyncio.to_thread(compute_sha256, path)
        send_name = filename or path.name
        base = server_base_url.rstrip("/")

        timeout = aiohttp.ClientTimeout(total=self.config.request_timeout_s)
        init_payload = {
            "filename": send_name,
            "total_bytes": total_bytes,
            "chunk_size": chunk_size,
            "transfer_id": transfer_id,
            "expected_sha256": expected_sha,
        }
        async with session.post(f"{base}/upload/init", json=init_payload, timeout=timeout) as resp:
            if resp.status >= 400:
                detail = await resp.text()
                raise RuntimeError(f"Upload init failed ({resp.status}): {detail}")
            init_data = await resp.json()

        session_id: str = init_data["session_id"]
        existing_chunks = set(init_data.get("existing_chunks", []))
        total_chunks = max(1, math.ceil(total_bytes / chunk_size)) if total_bytes > 0 else 1

        chunks: list[ChunkRecord] = []
        for idx in range(total_chunks):
            start_b = idx * chunk_size
            end_b = min(total_bytes - 1, start_b + chunk_size - 1) if total_bytes > 0 else -1
            rec = ChunkRecord(
                chunk_id=f"{transfer_id}-u{idx}",
                transfer_id=transfer_id,
                index=idx,
                start_byte=start_b,
                end_byte=max(start_b, end_b) if total_bytes > 0 else -1,
            )
            if idx in existing_chunks:
                rec.state = ChunkState.COMPLETED
                rec.downloaded_bytes = (end_b - start_b + 1) if total_bytes > 0 else 0
            chunks.append(rec)

        if chunk_records is not None:
            chunk_records.clear()
            chunk_records.extend(chunks)
        if on_metadata is not None:
            on_metadata(total_bytes, total_chunks)

        # Credit already-uploaded chunks on resumption
        resumed_bytes = sum(c.downloaded_bytes for c in chunks if c.state == ChunkState.COMPLETED)
        if resumed_bytes > 0 and on_progress is not None:
            on_progress(resumed_bytes, None, 0)

        queue: asyncio.Queue[ChunkRecord] = asyncio.Queue()
        for ch in chunks:
            if ch.state != ChunkState.COMPLETED:
                queue.put_nowait(ch)

        retries_total = 0

        async def worker() -> None:
            nonlocal retries_total
            while not queue.empty():
                if pause_event is not None:
                    await pause_event.wait()
                try:
                    ch = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    retries = await self._send_chunk(
                        session=session,
                        base_url=base,
                        session_id=session_id,
                        transfer_id=transfer_id,
                        file_path=path,
                        total_bytes=total_bytes,
                        chunk=ch,
                        rate_limiter=rate_limiter,
                        pause_event=pause_event,
                        on_progress=on_progress,
                    )
                    retries_total += retries
                finally:
                    queue.task_done()

        worker_count = min(self.config.max_chunks_per_transfer, max(1, queue.qsize()))
        workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
        try:
            await asyncio.gather(*workers)
        except Exception:
            for w in workers:
                w.cancel()
            raise

        # Finalize and verify SHA-256 atomically on the server.
        if pause_event is not None:
            await pause_event.wait()
        async with session.post(
            f"{base}/upload/{session_id}/complete",
            json={"transfer_id": transfer_id},
            timeout=timeout,
        ) as resp:
            if resp.status >= 400:
                detail = await resp.text()
                raise RuntimeError(f"Upload finalization failed ({resp.status}): {detail}")
            fin = await resp.json()

        if fin.get("sha256", "").lower() != expected_sha.lower():
            raise ValueError(
                f"Server SHA-256 mismatch: expected {expected_sha}, got {fin.get('sha256')}"
            )

        return UploadResult(
            session_id=session_id,
            dest_path=fin["dest_path"],
            total_bytes=total_bytes,
            sha256=expected_sha,
            total_chunks=total_chunks,
            retries=retries_total,
            resumed_chunks=len(existing_chunks),
        )

    async def _send_chunk(
        self,
        *,
        session: aiohttp.ClientSession,
        base_url: str,
        session_id: str,
        transfer_id: str,
        file_path: Path,
        total_bytes: int,
        chunk: ChunkRecord,
        rate_limiter: AsyncRateLimiter,
        pause_event: asyncio.Event | None,
        on_progress: UploadProgressCallback | None,
    ) -> int:
        timeout = aiohttp.ClientTimeout(total=self.config.request_timeout_s)
        attempts = 0
        last_exc: Exception | None = None

        chunk_len = (chunk.end_byte - chunk.start_byte + 1) if total_bytes > 0 else 0

        def _read_slice() -> bytes:
            if chunk_len <= 0:
                return b""
            with file_path.open("rb") as fh:
                fh.seek(chunk.start_byte)
                return fh.read(chunk_len)

        raw_data = await asyncio.to_thread(_read_slice)
        chunk_sha = hashlib.sha256(raw_data).hexdigest()

        async def _paced_body():
            if not raw_data:
                return
            step = 32_768
            for pos in range(0, len(raw_data), step):
                if pause_event is not None:
                    await pause_event.wait()
                piece = raw_data[pos : pos + step]
                await rate_limiter.acquire(len(piece))
                yield piece

        while attempts <= self.config.max_retries:
            if pause_event is not None:
                await pause_event.wait()
            chunk.state = ChunkState.IN_FLIGHT
            chunk.started_at = time.monotonic()
            chunk.downloaded_bytes = 0

            headers = {
                "X-Transfer-Id": transfer_id,
                "X-Chunk-Start": str(chunk.start_byte),
                "X-Chunk-End": str(chunk.end_byte if total_bytes > 0 else -1),
                "X-Chunk-SHA256": chunk_sha,
                "Content-Length": str(len(raw_data)),
            }
            t0 = time.monotonic()
            try:
                async with session.put(
                    f"{base_url}/upload/{session_id}/chunk/{chunk.index}",
                    data=_paced_body(),
                    headers=headers,
                    timeout=timeout,
                ) as resp:
                    if resp.status >= 400:
                        text = await resp.text()
                        raise aiohttp.ClientResponseError(
                            resp.request_info,
                            resp.history,
                            status=resp.status,
                            message=f"HTTP {resp.status}: {text}",
                        )
                    rtt_ms = (time.monotonic() - t0) * 1000.0
                    chunk.downloaded_bytes = len(raw_data)
                    chunk.state = ChunkState.COMPLETED
                    chunk.completed_at = time.monotonic()
                    if on_progress is not None:
                        on_progress(len(raw_data), rtt_ms, 0)
                    return attempts
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                last_exc = exc
                attempts += 1
                chunk.retries = attempts
                chunk.state = ChunkState.FAILED
                if on_progress is not None:
                    on_progress(0, None, 1)
                if attempts > self.config.max_retries:
                    break
                await asyncio.sleep(self.config.retry_backoff_s * (2 ** (attempts - 1)))

        raise RuntimeError(f"Upload chunk {chunk.chunk_id} failed after {attempts} attempts: {last_exc}")
