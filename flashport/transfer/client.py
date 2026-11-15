from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import os
import time
import uuid
from collections.abc import Awaitable, Callable, Iterable
from pathlib import Path, PureWindowsPath
from typing import Any
from urllib.parse import quote

import aiohttp

from flashport.transfer.chunk import Chunk
from flashport.transfer.integrity import sha256_file
from flashport.transfer.worker import (
    ChunkWorker,
    RetryableChunkError,
    TransferProtocolError,
)

ProgressCallback = Callable[[dict[str, Any]], Awaitable[None] | None]


class TransferError(RuntimeError):
    pass


class IntegrityError(TransferError):
    pass


class TransferClient:
    """Concurrent, resumable HTTP range client for a FlashPort server."""

    def __init__(
        self,
        base_url: str,
        chunk_size: int = 1024 * 1024,
        max_connections: int = 4,
        retries: int = 3,
        client_id: str | None = None,
    ) -> None:
        if not base_url.startswith(("http://", "https://")):
            raise ValueError("base_url must use http:// or https://")
        if (
            chunk_size <= 0
            or chunk_size > 64 * 1024 * 1024
            or max_connections <= 0
            or max_connections > 256
            or retries < 0
            or retries > 10
        ):
            raise ValueError("chunk_size must be 1..64 MiB, connections 1..256, retries 0..10")
        self.base_url = base_url.rstrip("/")
        self.chunk_size = chunk_size
        self.max_connections = max_connections
        self.retries = retries
        self.client_id = client_id or f"client-{uuid.uuid4()}"
        self._session: aiohttp.ClientSession | None = None
        self._request_semaphore = asyncio.Semaphore(max_connections)

    async def __aenter__(self) -> TransferClient:
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=None, connect=15, sock_read=120)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def list_files(self) -> list[dict[str, Any]]:
        session = await self._ensure_session()
        async with session.get(f"{self.base_url}/api/files") as response:
            response.raise_for_status()
            return await response.json()

    async def file_metadata(self, filename: str) -> dict[str, Any]:
        safe_name = self._validate_filename(filename)
        session = await self._ensure_session()
        url_name = quote(safe_name, safe="")
        async with session.get(f"{self.base_url}/api/files/{url_name}/metadata") as response:
            response.raise_for_status()
            return await response.json()

    async def download_file(
        self,
        filename: str,
        destination_directory: str | Path = "downloads",
        *,
        overwrite: bool = False,
        resume: bool = True,
        progress_callback: ProgressCallback | None = None,
    ) -> Path:
        safe_name = self._validate_filename(filename)
        destination = Path(destination_directory).expanduser().resolve()
        destination.mkdir(parents=True, exist_ok=True)
        target = destination / safe_name
        part = destination / f".{safe_name}.flashport.part"
        manifest_path = destination / f".{safe_name}.flashport.json"
        self._reject_symlink(target)
        self._reject_symlink(part)
        self._reject_symlink(manifest_path)
        if target.exists() and not overwrite:
            raise FileExistsError(f"Destination already exists: {target}")

        session = await self._ensure_session()
        manifest = self._read_manifest(manifest_path) if resume else None
        transfer_info = await self._reuse_session(session, safe_name, manifest)
        reused_session = transfer_info is not None
        if transfer_info is None:
            transfer_info = await self._register_transfer(session, safe_name)
        transfer_id = transfer_info["transfer_id"]
        token = transfer_info["token"]
        size = int(transfer_info["size"])
        expected_hash = str(transfer_info["sha256"])
        chunk_size = self.chunk_size
        chunks = Chunk.partition(size, chunk_size)
        if not self._manifest_matches(manifest, safe_name, size, expected_hash, chunk_size):
            manifest = {
                "filename": safe_name,
                "size": size,
                "sha256": expected_hash,
                "chunk_size": chunk_size,
                "completed_chunks": [],
                "chunk_hashes": {},
            }
            await asyncio.to_thread(self._prepare_part, part, size)
            await asyncio.to_thread(self._write_manifest, manifest_path, manifest)
        elif not part.is_file() or part.stat().st_size != size:
            manifest["completed_chunks"] = []
            manifest["chunk_hashes"] = {}
            await asyncio.to_thread(self._prepare_part, part, size)
            await asyncio.to_thread(self._write_manifest, manifest_path, manifest)

        recorded_chunks = list(manifest.get("completed_chunks", []))
        chunk_hashes = manifest.get("chunk_hashes", {})
        original_chunk_hashes = dict(chunk_hashes)
        completed: set[int] = set()
        for index in recorded_chunks:
            if type(index) is not int or not 0 <= index < len(chunks):
                continue
            chunk = chunks[index]
            local_bytes = await asyncio.to_thread(
                self._read_at,
                part,
                chunk.offset,
                chunk.length,
            )
            actual_chunk_hash = hashlib.sha256(local_bytes).hexdigest()
            if actual_chunk_hash == chunk_hashes.get(str(index)):
                completed.add(index)
            else:
                chunk_hashes.pop(str(index), None)
        manifest["completed_chunks"] = sorted(completed)
        manifest["chunk_hashes"] = chunk_hashes
        if manifest["completed_chunks"] != recorded_chunks or chunk_hashes != original_chunk_hashes:
            await asyncio.to_thread(self._write_manifest, manifest_path, manifest)
        completed_bytes = sum(chunks[index].length for index in completed)
        if reused_session and int(transfer_info["downloaded"]) != completed_bytes:
            transfer_info = await self._register_transfer(session, safe_name)
            transfer_id = transfer_info["transfer_id"]
            token = transfer_info["token"]
            reused_session = False
        manifest["transfer_id"] = transfer_id
        manifest["token"] = token
        await asyncio.to_thread(self._write_manifest, manifest_path, manifest)
        last_progress_at = time.monotonic()
        progress_lock = asyncio.Lock()
        token_header = {"X-FlashPort-Token": token}
        remote_completed = reused_session and transfer_info["state"] == "completed"
        if completed and not reused_session:
            resume_payload = {
                "chunks": [
                    {
                        "start": chunks[index].offset,
                        "end": chunks[index].end - 1,
                        "sha256": chunk_hashes[str(index)],
                    }
                    for index in sorted(completed)
                ]
            }
            async with session.put(
                f"{self.base_url}/api/transfers/{transfer_id}/resume",
                headers=token_header,
                json=resume_payload,
            ) as response:
                response.raise_for_status()

        async def report_progress(request_latency_ms: float | None = None) -> None:
            nonlocal last_progress_at
            async with progress_lock:
                bytes_done = sum(chunks[index].length for index in completed)
                now = time.monotonic()
                elapsed = max(now - last_progress_at, 1e-6)
                delta = max(0, bytes_done - getattr(report_progress, "last_bytes", 0))
                rate = delta / elapsed
                report_progress.last_bytes = bytes_done
                last_progress_at = now
                async with session.put(
                    f"{self.base_url}/api/transfers/{transfer_id}/progress",
                    headers=token_header,
                    json={
                        "downloaded_bytes": bytes_done,
                        "goodput_bps": rate,
                        "http_request_latency_ms": request_latency_ms,
                    },
                ) as response:
                    response.raise_for_status()
                if progress_callback is not None:
                    value = progress_callback(
                        {
                            "transfer_id": transfer_id,
                            "filename": safe_name,
                            "downloaded_bytes": bytes_done,
                            "total_bytes": size,
                            "percent": 100.0 if size == 0 else bytes_done * 100.0 / size,
                            "goodput_bps": rate,
                        }
                    )
                    if inspect.isawaitable(value):
                        await value

        report_progress.last_bytes = completed_bytes
        if completed_bytes and not remote_completed:
            await report_progress()

        async def download_chunk(partition: Chunk) -> None:
            nonlocal completed
            index = partition.index
            if index in completed:
                return
            chunk = ChunkWorker(
                transfer_id=transfer_id,
                index=index,
                start=partition.offset,
                end=partition.end - 1,
                retries=self.retries,
            )
            async with self._request_semaphore:
                async def fetch() -> tuple[bytes, float]:
                    request_started = time.monotonic()
                    async with session.get(
                        f"{self.base_url}/api/transfers/{transfer_id}/content",
                        headers={
                            **token_header,
                            "Range": partition.range_header,
                        },
                    ) as response:
                        if response.status != 206:
                            if response.status >= 500:
                                raise RetryableChunkError(f"Server returned {response.status}.")
                            raise TransferProtocolError(
                                f"Server returned {response.status} instead of the requested byte range."
                            )
                        expected_range = f"bytes {partition.offset}-{partition.end - 1}/{size}"
                        if response.headers.get("Content-Range") != expected_range:
                            raise TransferProtocolError("Server returned an unexpected Content-Range.")
                        latency_ms = (time.monotonic() - request_started) * 1000.0
                        content = await response.read()
                    if len(content) != partition.length:
                        raise TransferProtocolError("Chunk response length did not match the requested range.")
                    return content, latency_ms

                content, latency_ms = await chunk.run(fetch)
                await asyncio.to_thread(self._write_at, part, partition.offset, content)
                async with progress_lock:
                    completed.add(index)
                    manifest["completed_chunks"] = sorted(completed)
                    manifest.setdefault("chunk_hashes", {})[str(index)] = hashlib.sha256(content).hexdigest()
                    await asyncio.to_thread(self._write_manifest, manifest_path, manifest)
                await report_progress(latency_ms)

        tasks = [
            asyncio.create_task(download_chunk(partition))
            for partition in chunks
            if partition.index not in completed
        ]
        try:
            if tasks:
                await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self._cancel_remote(transfer_id, token)
            raise
        except Exception:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

        actual_hash = await asyncio.to_thread(sha256_file, part)
        if actual_hash != expected_hash:
            part.unlink(missing_ok=True)
            manifest_path.unlink(missing_ok=True)
            await self._cancel_remote(transfer_id, token)
            raise IntegrityError(
                f"SHA-256 mismatch for {safe_name}: expected {expected_hash}, got {actual_hash}"
            )
        if not chunks and not remote_completed:
            await report_progress()
        if not remote_completed:
            async with session.post(
                f"{self.base_url}/api/transfers/{transfer_id}/complete",
                headers=token_header,
                json={"sha256": actual_hash},
            ) as response:
                response.raise_for_status()
        os.replace(part, target)
        manifest_path.unlink(missing_ok=True)
        return target

    async def _register_transfer(
        self,
        session: aiohttp.ClientSession,
        filename: str,
    ) -> dict[str, Any]:
        async with session.post(
            f"{self.base_url}/api/transfers",
            json={"filename": filename, "client_id": self.client_id},
        ) as response:
            response.raise_for_status()
            return await response.json()

    async def _reuse_session(
        self,
        session: aiohttp.ClientSession,
        filename: str,
        manifest: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        if not manifest or not isinstance(manifest.get("transfer_id"), str):
            return None
        try:
            transfer_id = str(uuid.UUID(manifest["transfer_id"]))
        except ValueError:
            return None
        token = manifest.get("token")
        if not isinstance(token, str) or not token.isascii() or not token:
            return None
        async with session.get(
            f"{self.base_url}/api/transfers/{quote(transfer_id, safe='')}/status",
            headers={"X-FlashPort-Token": token},
        ) as response:
            if response.status in {401, 404, 409}:
                return None
            response.raise_for_status()
            status = await response.json()
        if (
            status.get("filename") != filename
            or status.get("size") != manifest.get("size")
            or status.get("sha256") != manifest.get("sha256")
            or manifest.get("chunk_size") != self.chunk_size
            or status.get("state") not in {"queued", "paused", "completed"}
        ):
            return None
        return {
            **status,
            "transfer_id": transfer_id,
            "token": token,
        }

    async def download_many(
        self,
        filenames: Iterable[str],
        destination_directory: str | Path = "downloads",
        *,
        overwrite: bool = False,
        resume: bool = True,
        progress_callback: ProgressCallback | None = None,
    ) -> list[Path]:
        names = [self._validate_filename(name) for name in filenames]
        async def download(name: str) -> Path:
            return await self.download_file(
                name,
                destination_directory,
                overwrite=overwrite,
                resume=resume,
                progress_callback=progress_callback,
            )

        if len(set(names)) != len(names):
            raise ValueError("filenames must be unique within download_many")
        return list(await asyncio.gather(*(download(name) for name in names)))

    async def _cancel_remote(self, transfer_id: str, token: str) -> None:
        session = await self._ensure_session()
        try:
            async with session.post(
                f"{self.base_url}/api/transfers/{transfer_id}/cancel",
                headers={"X-FlashPort-Token": token},
            ) as response:
                if response.status not in {200, 404, 409}:
                    response.raise_for_status()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return

    @staticmethod
    def _validate_filename(filename: str) -> str:
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
            raise ValueError("filename must be a single safe file name")
        return filename

    @staticmethod
    def _reject_symlink(path: Path) -> None:
        if path.is_symlink():
            raise ValueError(f"Refusing to use symlink destination: {path}")

    @staticmethod
    def _read_manifest(path: Path) -> dict[str, Any] | None:
        try:
            with path.open(encoding="utf-8") as handle:
                data = json.load(handle)
        except FileNotFoundError:
            return None
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None
        return data if isinstance(data, dict) else None

    @staticmethod
    def _manifest_matches(
        manifest: dict[str, Any] | None,
        filename: str,
        size: int,
        sha256: str,
        chunk_size: int,
    ) -> bool:
        return bool(
            manifest
            and manifest.get("filename") == filename
            and manifest.get("size") == size
            and manifest.get("sha256") == sha256
            and manifest.get("chunk_size") == chunk_size
            and isinstance(manifest.get("completed_chunks"), list)
            and isinstance(manifest.get("chunk_hashes"), dict)
        )

    @staticmethod
    def _prepare_part(path: Path, size: int) -> None:
        with path.open("wb") as handle:
            handle.truncate(size)

    @staticmethod
    def _write_at(path: Path, offset: int, content: bytes) -> None:
        with path.open("r+b") as handle:
            handle.seek(offset)
            written = handle.write(content)
            if written != len(content):
                raise OSError("partial chunk write")
            handle.flush()

    @staticmethod
    def _read_at(path: Path, offset: int, length: int) -> bytes:
        with path.open("rb") as handle:
            handle.seek(offset)
            return handle.read(length)

    @staticmethod
    def _write_manifest(path: Path, data: dict[str, Any]) -> None:
        temporary = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
        try:
            with temporary.open("x", encoding="utf-8") as handle:
                json.dump(data, handle, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
