"""Asynchronous HTTP download engine with Range chunking, retries, reassembly, and SHA-256 checks."""

from __future__ import annotations

import asyncio
import hashlib
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import aiohttp

from flowweave.config import TransferConfig
from flowweave.models import ChunkRecord, ChunkState
from flowweave.transfer.rate_limiter import AsyncRateLimiter

ProgressCallback = Callable[[int, float | None, int], None]


@dataclass
class RemoteMetadata:
    content_length: int | None
    supports_ranges: bool
    expected_sha256: str | None = None


@dataclass
class DownloadResult:
    dest_path: Path
    total_bytes: int
    sha256: str
    used_ranges: bool
    total_chunks: int
    retries: int


def compute_sha256(file_path: Path) -> str:
    """Compute SHA-256 hex digest of a file using streaming reads."""
    hasher = hashlib.sha256()
    with file_path.open("rb") as fh:
        while True:
            buf = fh.read(65_536)
            if not buf:
                break
            hasher.update(buf)
    return hasher.hexdigest()


async def probe_remote(
    session: aiohttp.ClientSession,
    url: str,
    timeout_s: float = 15.0,
) -> RemoteMetadata:
    """Inspect remote file size, HTTP Range support, and optional SHA-256 header."""
    timeout = aiohttp.ClientTimeout(total=timeout_s)
    try:
        async with session.head(url, timeout=timeout, allow_redirects=True) as resp:
            if resp.status < 400:
                cl_hdr = resp.headers.get("Content-Length")
                content_length = int(cl_hdr) if cl_hdr and cl_hdr.isdigit() else None
                accept_ranges = resp.headers.get("Accept-Ranges", "").lower() == "bytes"
                sha = resp.headers.get("X-Content-SHA256")
                return RemoteMetadata(
                    content_length=content_length,
                    supports_ranges=accept_ranges and content_length is not None and content_length > 0,
                    expected_sha256=sha,
                )
    except aiohttp.ClientError:
        pass

    # Fallback probe with a 1-byte Range GET for servers that reject HEAD.
    try:
        async with session.get(
            url,
            headers={"Range": "bytes=0-0"},
            timeout=timeout,
            allow_redirects=True,
        ) as resp:
            sha = resp.headers.get("X-Content-SHA256")
            if resp.status == 206:
                cr = resp.headers.get("Content-Range", "")
                total = None
                if "/" in cr:
                    tail = cr.rsplit("/", 1)[-1]
                    if tail.isdigit():
                        total = int(tail)
                return RemoteMetadata(
                    content_length=total,
                    supports_ranges=total is not None and total > 0,
                    expected_sha256=sha,
                )
            cl_hdr = resp.headers.get("Content-Length")
            content_length = int(cl_hdr) if cl_hdr and cl_hdr.isdigit() else None
            return RemoteMetadata(
                content_length=content_length,
                supports_ranges=False,
                expected_sha256=sha,
            )
    except aiohttp.ClientError:
        return RemoteMetadata(content_length=None, supports_ranges=False)


class HTTPDownloadEngine:
    """Downloads a single file using parallel HTTP Range chunks or single-stream fallback."""

    def __init__(self, config: TransferConfig | None = None) -> None:
        self.config = config or TransferConfig()

    async def download(
        self,
        *,
        session: aiohttp.ClientSession,
        transfer_id: str,
        url: str,
        dest_path: Path,
        rate_limiter: AsyncRateLimiter,
        expected_sha256: str | None = None,
        pause_event: asyncio.Event | None = None,
        on_metadata: Callable[[int, int], None] | None = None,
        on_progress: ProgressCallback | None = None,
        chunk_records: list[ChunkRecord] | None = None,
    ) -> DownloadResult:
        """Download `url` to `dest_path` with chunking, retries, pause/resume, and integrity check."""
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        meta = await probe_remote(session, url, timeout_s=self.config.request_timeout_s)
        target_sha = expected_sha256 or meta.expected_sha256

        use_ranges = (
            meta.supports_ranges
            and meta.content_length is not None
            and meta.content_length > self.config.chunk_size_bytes
        )

        if use_ranges and meta.content_length is not None:
            result = await self._download_ranged(
                session=session,
                transfer_id=transfer_id,
                url=url,
                dest_path=dest_path,
                total_bytes=meta.content_length,
                rate_limiter=rate_limiter,
                pause_event=pause_event,
                on_metadata=on_metadata,
                on_progress=on_progress,
                chunk_records=chunk_records,
            )
        else:
            result = await self._download_single_stream(
                session=session,
                transfer_id=transfer_id,
                url=url,
                dest_path=dest_path,
                known_size=meta.content_length,
                rate_limiter=rate_limiter,
                pause_event=pause_event,
                on_metadata=on_metadata,
                on_progress=on_progress,
                chunk_records=chunk_records,
            )

        if target_sha and result.sha256.lower() != target_sha.lower():
            dest_path.unlink(missing_ok=True)
            raise ValueError(
                f"SHA-256 mismatch for {dest_path.name}: expected {target_sha}, got {result.sha256}"
            )
        return result

    async def _download_ranged(
        self,
        *,
        session: aiohttp.ClientSession,
        transfer_id: str,
        url: str,
        dest_path: Path,
        total_bytes: int,
        rate_limiter: AsyncRateLimiter,
        pause_event: asyncio.Event | None,
        on_metadata: Callable[[int, int], None] | None,
        on_progress: ProgressCallback | None,
        chunk_records: list[ChunkRecord] | None,
    ) -> DownloadResult:
        chunk_size = self.config.chunk_size_bytes
        chunks: list[ChunkRecord] = []
        offset = 0
        idx = 0
        while offset < total_bytes:
            end = min(total_bytes - 1, offset + chunk_size - 1)
            chunks.append(
                ChunkRecord(
                    chunk_id=f"{transfer_id}-c{idx}",
                    transfer_id=transfer_id,
                    index=idx,
                    start_byte=offset,
                    end_byte=end,
                )
            )
            offset = end + 1
            idx += 1

        if chunk_records is not None:
            chunk_records.clear()
            chunk_records.extend(chunks)
        if on_metadata is not None:
            on_metadata(total_bytes, len(chunks))

        part_paths = [dest_path.with_name(f"{dest_path.name}.part.{c.index}") for c in chunks]
        queue: asyncio.Queue[ChunkRecord] = asyncio.Queue()
        for ch in chunks:
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
                part_file = part_paths[ch.index]
                try:
                    retries = await self._fetch_chunk(
                        session=session,
                        url=url,
                        chunk=ch,
                        part_file=part_file,
                        rate_limiter=rate_limiter,
                        pause_event=pause_event,
                        on_progress=on_progress,
                    )
                    retries_total += retries
                finally:
                    queue.task_done()

        worker_count = min(self.config.max_chunks_per_transfer, len(chunks))
        workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
        try:
            await asyncio.gather(*workers)
        except Exception:
            for w in workers:
                w.cancel()
            for p in part_paths:
                p.unlink(missing_ok=True)
            raise

        # Reassemble parts sequentially while streaming SHA-256 digest.
        hasher = hashlib.sha256()
        written = 0
        tmp_out = dest_path.with_name(f"{dest_path.name}.assembling")
        try:
            with tmp_out.open("wb") as out_fh:
                for part in part_paths:
                    with part.open("rb") as in_fh:
                        while True:
                            buf = in_fh.read(65_536)
                            if not buf:
                                break
                            out_fh.write(buf)
                            hasher.update(buf)
                            written += len(buf)
            tmp_out.replace(dest_path)
        finally:
            tmp_out.unlink(missing_ok=True)
            for part in part_paths:
                part.unlink(missing_ok=True)

        if written != total_bytes:
            dest_path.unlink(missing_ok=True)
            raise IOError(f"Reassembled size {written} != expected {total_bytes}")

        return DownloadResult(
            dest_path=dest_path,
            total_bytes=written,
            sha256=hasher.hexdigest(),
            used_ranges=True,
            total_chunks=len(chunks),
            retries=retries_total,
        )

    async def _fetch_chunk(
        self,
        *,
        session: aiohttp.ClientSession,
        url: str,
        chunk: ChunkRecord,
        part_file: Path,
        rate_limiter: AsyncRateLimiter,
        pause_event: asyncio.Event | None,
        on_progress: ProgressCallback | None,
    ) -> int:
        timeout = aiohttp.ClientTimeout(total=self.config.request_timeout_s)
        attempts = 0
        last_exc: Exception | None = None

        while attempts <= self.config.max_retries:
            if pause_event is not None:
                await pause_event.wait()
            chunk.state = ChunkState.IN_FLIGHT
            chunk.started_at = time.monotonic()
            chunk.downloaded_bytes = 0
            headers = {"Range": f"bytes={chunk.start_byte}-{chunk.end_byte}"}
            t0 = time.monotonic()
            try:
                async with session.get(url, headers=headers, timeout=timeout) as resp:
                    if resp.status not in (200, 206):
                        raise aiohttp.ClientResponseError(
                            resp.request_info,
                            resp.history,
                            status=resp.status,
                            message=f"Unexpected status {resp.status}",
                        )
                    rtt_ms = (time.monotonic() - t0) * 1000.0
                    first = True
                    with part_file.open("wb") as fh:
                        async for block in resp.content.iter_chunked(32_768):
                            if not block:
                                continue
                            if pause_event is not None:
                                await pause_event.wait()
                            await rate_limiter.acquire(len(block))
                            fh.write(block)
                            chunk.downloaded_bytes += len(block)
                            if on_progress is not None:
                                on_progress(len(block), rtt_ms if first else None, 0)
                            first = False

                    if chunk.downloaded_bytes != chunk.total_bytes:
                        raise IOError(
                            f"Chunk {chunk.chunk_id} truncated: {chunk.downloaded_bytes}/{chunk.total_bytes}"
                        )
                    chunk.state = ChunkState.COMPLETED
                    chunk.completed_at = time.monotonic()
                    return attempts
            except asyncio.CancelledError:
                part_file.unlink(missing_ok=True)
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

        part_file.unlink(missing_ok=True)
        raise RuntimeError(f"Chunk {chunk.chunk_id} failed after {attempts} attempts: {last_exc}")

    async def _download_single_stream(
        self,
        *,
        session: aiohttp.ClientSession,
        transfer_id: str,
        url: str,
        dest_path: Path,
        known_size: int | None,
        rate_limiter: AsyncRateLimiter,
        pause_event: asyncio.Event | None,
        on_metadata: Callable[[int, int], None] | None,
        on_progress: ProgressCallback | None,
        chunk_records: list[ChunkRecord] | None,
    ) -> DownloadResult:
        """Fallback path when remote server does not support HTTP Range requests."""
        ch = ChunkRecord(
            chunk_id=f"{transfer_id}-stream",
            transfer_id=transfer_id,
            index=0,
            start_byte=0,
            end_byte=max(0, (known_size or 1) - 1),
        )
        if chunk_records is not None:
            chunk_records.clear()
            chunk_records.append(ch)
        if on_metadata is not None and known_size is not None:
            on_metadata(known_size, 1)

        timeout = aiohttp.ClientTimeout(total=self.config.request_timeout_s)
        attempts = 0
        last_exc: Exception | None = None
        tmp_path = dest_path.with_name(f"{dest_path.name}.stream")

        while attempts <= self.config.max_retries:
            ch.state = ChunkState.IN_FLIGHT
            ch.started_at = time.monotonic()
            ch.downloaded_bytes = 0
            hasher = hashlib.sha256()
            t0 = time.monotonic()
            try:
                async with session.get(url, timeout=timeout) as resp:
                    if resp.status >= 400:
                        raise aiohttp.ClientResponseError(
                            resp.request_info,
                            resp.history,
                            status=resp.status,
                            message=f"HTTP {resp.status}",
                        )
                    rtt_ms = (time.monotonic() - t0) * 1000.0
                    if known_size is None:
                        cl = resp.headers.get("Content-Length")
                        if cl and cl.isdigit():
                            known_size = int(cl)
                            ch.end_byte = max(0, known_size - 1)
                            if on_metadata is not None:
                                on_metadata(known_size, 1)

                    first = True
                    with tmp_path.open("wb") as fh:
                        async for block in resp.content.iter_chunked(32_768):
                            if not block:
                                continue
                            if pause_event is not None:
                                await pause_event.wait()
                            await rate_limiter.acquire(len(block))
                            fh.write(block)
                            hasher.update(block)
                            ch.downloaded_bytes += len(block)
                            if on_progress is not None:
                                on_progress(len(block), rtt_ms if first else None, 0)
                            first = False

                    tmp_path.replace(dest_path)
                    ch.state = ChunkState.COMPLETED
                    ch.completed_at = time.monotonic()
                    return DownloadResult(
                        dest_path=dest_path,
                        total_bytes=ch.downloaded_bytes,
                        sha256=hasher.hexdigest(),
                        used_ranges=False,
                        total_chunks=1,
                        retries=attempts,
                    )
            except asyncio.CancelledError:
                tmp_path.unlink(missing_ok=True)
                raise
            except Exception as exc:
                last_exc = exc
                attempts += 1
                ch.retries = attempts
                ch.state = ChunkState.FAILED
                if on_progress is not None:
                    on_progress(0, None, 1)
                if attempts > self.config.max_retries:
                    break
                await asyncio.sleep(self.config.retry_backoff_s * (2 ** (attempts - 1)))

        tmp_path.unlink(missing_ok=True)
        raise RuntimeError(f"Download {url} failed after {attempts} attempts: {last_exc}")
