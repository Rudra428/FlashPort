"""Raw TCP socket download and upload engines paced by FlowWeave's cooperative scheduler."""

from __future__ import annotations

import asyncio
import hashlib
import math
import socket
import time
from pathlib import Path
from typing import Callable
from urllib.parse import urlparse

from flowweave.config import TransferConfig
from flowweave.models import ChunkRecord, ChunkState
from flowweave.transfer.engine import DownloadResult, compute_sha256
from flowweave.transfer.rate_limiter import AsyncRateLimiter
from flowweave.transfer.tcp_protocol import recv_frame_async, send_frame_async
from flowweave.transfer.upload_client import UploadResult

ProgressCallback = Callable[[int, float | None, int], None]


def parse_tcp_url(url: str) -> tuple[str, int, str]:
    """Parse `tcp://host:port/filename` into (host, port, filename)."""
    parsed = urlparse(url)
    if parsed.scheme != "tcp" or not parsed.hostname or not parsed.port:
        raise ValueError(f"Invalid FlowWeave TCP URL (expected tcp://host:port/file): {url!r}")
    filename = parsed.path.lstrip("/")
    return parsed.hostname, int(parsed.port), filename


async def _open_tcp_connection(host: str, port: int, timeout_s: float) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout_s)
    raw_sock = writer.get_extra_info("socket")
    if raw_sock is not None:
        try:
            raw_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass
    return reader, writer


class TCPDownloadEngine:
    """Downloads a file in parallel chunks over raw TCP sockets with rate pacing & SHA-256 verification."""

    def __init__(self, config: TransferConfig | None = None) -> None:
        self.config = config or TransferConfig()

    async def download(
        self,
        *,
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
        host, port, remote_name = parse_tcp_url(url)
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        # 1. Query file size & SHA-256 over a TCP control connection.
        reader, writer = await _open_tcp_connection(host, port, self.config.request_timeout_s)
        try:
            await send_frame_async(writer, {"op": "STAT", "filename": remote_name})
            meta, _ = await recv_frame_async(reader)
        finally:
            writer.close()
            await writer.wait_closed()

        if not meta.get("ok"):
            raise RuntimeError(f"TCP STAT failed: {meta.get('error')}")

        total_bytes = int(meta["size_bytes"])
        target_sha = expected_sha256 or meta.get("sha256")
        chunk_size = self.config.chunk_size_bytes
        total_chunks = max(1, math.ceil(total_bytes / chunk_size)) if total_bytes > 0 else 1

        chunks: list[ChunkRecord] = []
        for idx in range(total_chunks):
            start_b = idx * chunk_size
            end_b = min(total_bytes - 1, start_b + chunk_size - 1) if total_bytes > 0 else -1
            chunks.append(
                ChunkRecord(
                    chunk_id=f"{transfer_id}-tc{idx}",
                    transfer_id=transfer_id,
                    index=idx,
                    start_byte=start_b,
                    end_byte=max(start_b, end_b) if total_bytes > 0 else -1,
                )
            )

        if chunk_records is not None:
            chunk_records.clear()
            chunk_records.extend(chunks)
        if on_metadata is not None:
            on_metadata(total_bytes, total_chunks)

        part_paths = [dest_path.with_name(f"{dest_path.name}.part.{c.index}") for c in chunks]
        queue: asyncio.Queue[ChunkRecord] = asyncio.Queue()
        for ch in chunks:
            queue.put_nowait(ch)

        retries_total = 0

        async def worker() -> None:
            nonlocal retries_total
            w_reader: asyncio.StreamReader | None = None
            w_writer: asyncio.StreamWriter | None = None
            try:
                while not queue.empty():
                    if pause_event is not None:
                        await pause_event.wait()
                    try:
                        ch = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return

                    attempts = 0
                    part_file = part_paths[ch.index]
                    chunk_len = (ch.end_byte - ch.start_byte + 1) if total_bytes > 0 else 0
                    try:
                        while attempts <= self.config.max_retries:
                            if pause_event is not None:
                                await pause_event.wait()
                            ch.state = ChunkState.IN_FLIGHT
                            ch.started_at = time.monotonic()
                            ch.downloaded_bytes = 0
                            t0 = time.monotonic()
                            try:
                                if w_reader is None or w_writer is None:
                                    w_reader, w_writer = await _open_tcp_connection(
                                        host, port, self.config.request_timeout_s
                                    )
                                await send_frame_async(
                                    w_writer,
                                    {
                                        "op": "GET_CHUNK",
                                        "filename": remote_name,
                                        "offset": ch.start_byte,
                                        "length": chunk_len,
                                    },
                                )
                                resp, data = await recv_frame_async(w_reader)
                                if not resp.get("ok"):
                                    raise RuntimeError(resp.get("error", "GET_CHUNK failed"))
                                rtt_ms = (time.monotonic() - t0) * 1000.0
                                if len(data) != chunk_len:
                                    raise IOError(f"Short TCP chunk: {len(data)}/{chunk_len}")
                                if resp.get("chunk_sha256") and hashlib.sha256(data).hexdigest() != resp["chunk_sha256"]:
                                    raise IOError("TCP chunk SHA-256 mismatch")

                                # Pace delivery through FlowWeave's scheduler rate limiter in 32 KiB slices.
                                first = True
                                with part_file.open("wb") as fh:
                                    for pos in range(0, len(data), 32_768):
                                        if pause_event is not None:
                                            await pause_event.wait()
                                        piece = data[pos : pos + 32_768]
                                        await rate_limiter.acquire(len(piece))
                                        fh.write(piece)
                                        ch.downloaded_bytes += len(piece)
                                        if on_progress is not None:
                                            on_progress(len(piece), rtt_ms if first else None, 0)
                                        first = False
                                ch.state = ChunkState.COMPLETED
                                ch.completed_at = time.monotonic()
                                retries_total += attempts
                                break
                            except asyncio.CancelledError:
                                part_file.unlink(missing_ok=True)
                                raise
                            except Exception:
                                attempts += 1
                                ch.retries = attempts
                                ch.state = ChunkState.FAILED
                                if w_writer is not None:
                                    w_writer.close()
                                    w_reader = None
                                    w_writer = None
                                if on_progress is not None:
                                    on_progress(0, None, 1)
                                if attempts > self.config.max_retries:
                                    raise
                                await asyncio.sleep(self.config.retry_backoff_s * (2 ** (attempts - 1)))
                    finally:
                        queue.task_done()
            finally:
                if w_writer is not None:
                    w_writer.close()
                    try:
                        await w_writer.wait_closed()
                    except Exception:
                        pass

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

        # Reassemble parts and verify full-file SHA-256.
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

        actual_sha = hasher.hexdigest()
        if target_sha and actual_sha.lower() != target_sha.lower():
            dest_path.unlink(missing_ok=True)
            raise ValueError(f"TCP download SHA-256 mismatch: expected {target_sha}, got {actual_sha}")

        return DownloadResult(
            dest_path=dest_path,
            total_bytes=written,
            sha256=actual_sha,
            used_ranges=True,
            total_chunks=total_chunks,
            retries=retries_total,
        )


class TCPUploadEngine:
    """Uploads a local file in parallel chunks over raw TCP sockets with pacing & SHA-256 checks."""

    def __init__(self, config: TransferConfig | None = None) -> None:
        self.config = config or TransferConfig()

    async def upload(
        self,
        *,
        transfer_id: str,
        file_path: Path,
        server_url: str,
        rate_limiter: AsyncRateLimiter,
        filename: str | None = None,
        pause_event: asyncio.Event | None = None,
        on_metadata: Callable[[int, int], None] | None = None,
        on_progress: ProgressCallback | None = None,
        chunk_records: list[ChunkRecord] | None = None,
    ) -> UploadResult:
        host, port, _ = parse_tcp_url(server_url)
        path = Path(file_path)
        if not path.is_file():
            raise FileNotFoundError(f"Upload file not found: {path}")

        total_bytes = path.stat().st_size
        chunk_size = self.config.chunk_size_bytes
        expected_sha = await asyncio.to_thread(compute_sha256, path)
        send_name = filename or path.name

        # 1. Initialize or resume upload session over TCP.
        reader, writer = await _open_tcp_connection(host, port, self.config.request_timeout_s)
        try:
            await send_frame_async(
                writer,
                {
                    "op": "PUT_INIT",
                    "filename": send_name,
                    "total_bytes": total_bytes,
                    "chunk_size": chunk_size,
                    "transfer_id": transfer_id,
                    "expected_sha256": expected_sha,
                },
            )
            init_resp, _ = await recv_frame_async(reader)
        finally:
            writer.close()
            await writer.wait_closed()

        if not init_resp.get("ok"):
            raise RuntimeError(f"TCP PUT_INIT failed: {init_resp.get('error')}")

        session_id = str(init_resp["session_id"])
        existing_chunks = set(init_resp.get("existing_chunks", []))
        total_chunks = max(1, math.ceil(total_bytes / chunk_size)) if total_bytes > 0 else 1

        chunks: list[ChunkRecord] = []
        for idx in range(total_chunks):
            start_b = idx * chunk_size
            end_b = min(total_bytes - 1, start_b + chunk_size - 1) if total_bytes > 0 else -1
            rec = ChunkRecord(
                chunk_id=f"{transfer_id}-tu{idx}",
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

        queue: asyncio.Queue[ChunkRecord] = asyncio.Queue()
        for ch in chunks:
            if ch.state != ChunkState.COMPLETED:
                queue.put_nowait(ch)

        retries_total = 0

        async def worker() -> None:
            nonlocal retries_total
            w_reader: asyncio.StreamReader | None = None
            w_writer: asyncio.StreamWriter | None = None
            try:
                while not queue.empty():
                    if pause_event is not None:
                        await pause_event.wait()
                    try:
                        ch = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return

                    chunk_len = (ch.end_byte - ch.start_byte + 1) if total_bytes > 0 else 0

                    def _read_slice() -> bytes:
                        if chunk_len <= 0:
                            return b""
                        with path.open("rb") as fh:
                            fh.seek(ch.start_byte)
                            return fh.read(chunk_len)

                    raw_data = await asyncio.to_thread(_read_slice)
                    chunk_sha = hashlib.sha256(raw_data).hexdigest()

                    attempts = 0
                    try:
                        while attempts <= self.config.max_retries:
                            if pause_event is not None:
                                await pause_event.wait()
                            ch.state = ChunkState.IN_FLIGHT
                            ch.started_at = time.monotonic()
                            t0 = time.monotonic()
                            try:
                                # Pace transmission via FlowWeave's token bucket before sending frame.
                                for pos in range(0, len(raw_data), 32_768):
                                    if pause_event is not None:
                                        await pause_event.wait()
                                    await rate_limiter.acquire(min(32_768, len(raw_data) - pos))

                                if w_reader is None or w_writer is None:
                                    w_reader, w_writer = await _open_tcp_connection(
                                        host, port, self.config.request_timeout_s
                                    )
                                await send_frame_async(
                                    w_writer,
                                    {
                                        "op": "PUT_CHUNK",
                                        "session_id": session_id,
                                        "transfer_id": transfer_id,
                                        "index": ch.index,
                                        "start_byte": ch.start_byte,
                                        "end_byte": ch.end_byte if total_bytes > 0 else -1,
                                        "chunk_sha256": chunk_sha,
                                    },
                                    raw_data,
                                )
                                resp, _ = await recv_frame_async(w_reader)
                                if not resp.get("ok"):
                                    raise RuntimeError(resp.get("error", "PUT_CHUNK failed"))
                                rtt_ms = (time.monotonic() - t0) * 1000.0
                                ch.downloaded_bytes = len(raw_data)
                                ch.state = ChunkState.COMPLETED
                                ch.completed_at = time.monotonic()
                                if on_progress is not None:
                                    on_progress(len(raw_data), rtt_ms, 0)
                                retries_total += attempts
                                break
                            except asyncio.CancelledError:
                                raise
                            except Exception:
                                attempts += 1
                                ch.retries = attempts
                                ch.state = ChunkState.FAILED
                                if w_writer is not None:
                                    w_writer.close()
                                    w_reader = None
                                    w_writer = None
                                if on_progress is not None:
                                    on_progress(0, None, 1)
                                if attempts > self.config.max_retries:
                                    raise
                                await asyncio.sleep(self.config.retry_backoff_s * (2 ** (attempts - 1)))
                    finally:
                        queue.task_done()
            finally:
                if w_writer is not None:
                    w_writer.close()
                    try:
                        await w_writer.wait_closed()
                    except Exception:
                        pass

        worker_count = min(self.config.max_chunks_per_transfer, max(1, queue.qsize()))
        workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
        try:
            await asyncio.gather(*workers)
        except Exception:
            for w in workers:
                w.cancel()
            raise

        # 3. Atomically finalize & verify SHA-256 on TCP server.
        reader, writer = await _open_tcp_connection(host, port, self.config.request_timeout_s)
        try:
            await send_frame_async(
                writer,
                {"op": "PUT_COMPLETE", "session_id": session_id, "transfer_id": transfer_id},
            )
            fin, _ = await recv_frame_async(reader)
        finally:
            writer.close()
            await writer.wait_closed()

        if not fin.get("ok"):
            raise RuntimeError(f"TCP PUT_COMPLETE failed: {fin.get('error')}")

        return UploadResult(
            session_id=session_id,
            dest_path=str(fin["dest_path"]),
            total_bytes=total_bytes,
            sha256=expected_sha,
            total_chunks=total_chunks,
            retries=retries_total,
            resumed_chunks=len(existing_chunks),
        )
