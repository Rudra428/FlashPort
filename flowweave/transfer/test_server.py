"""Local HTTP Range download + chunked upload server for end-to-end bidirectional testing and demos."""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass
from pathlib import Path

from aiohttp import web

from flowweave.transfer.upload_server import ChunkedUploadStore


@dataclass
class ServedFile:
    name: str
    data: bytes
    sha256: str


class LocalFileServer:
    """Lightweight aiohttp server supporting HTTP Range downloads, no-Range fallback, and chunked uploads."""

    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        upload_dir: str | Path = "uploads",
        artificial_delay_s: float = 0.0,
    ) -> None:
        self.host = host
        self.port = port
        self.artificial_delay_s = artificial_delay_s
        self.files: dict[str, ServedFile] = {}
        self.upload_store = ChunkedUploadStore(upload_dir)
        # Optional fault injection set of (session_or_any, chunk_index) that fail once with 503
        self.fail_once_upload_chunks: set[int] = set()
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self.bound_port: int = port

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.bound_port}"

    def add_deterministic_file(self, name: str, size_bytes: int, seed_byte: int = 42) -> ServedFile:
        """Create an in-memory deterministic binary payload for repeatable integrity checks."""
        pattern = bytes((seed_byte + i * 31) % 256 for i in range(4096))
        repeats, rem = divmod(size_bytes, len(pattern))
        data = pattern * repeats + pattern[:rem]
        sf = ServedFile(name=name, data=data, sha256=hashlib.sha256(data).hexdigest())
        self.files[name] = sf
        return sf

    def add_file_from_disk(self, path: Path) -> ServedFile:
        """Load a real sample file from disk so it can be downloaded via HTTP Range requests."""
        data = path.read_bytes()
        sf = ServedFile(name=path.name, data=data, sha256=hashlib.sha256(data).hexdigest())
        self.files[path.name] = sf
        return sf

    def url_for(self, name: str, *, allow_ranges: bool = True) -> str:
        prefix = "files" if allow_ranges else "noranges"
        return f"{self.base_url}/{prefix}/{name}"

    async def start(self) -> None:
        app = web.Application(client_max_size=64 * 1024 * 1024)
        # Download routes
        app.router.add_route("HEAD", "/files/{name}", self._handle_ranged_head)
        app.router.add_route("GET", "/files/{name}", self._handle_ranged_get)
        app.router.add_route("HEAD", "/noranges/{name}", self._handle_noranges_head)
        app.router.add_route("GET", "/noranges/{name}", self._handle_noranges_get)
        # Chunked resumable upload routes
        app.router.add_route("POST", "/upload/init", self._handle_upload_init)
        app.router.add_route("PUT", "/upload/{session_id}/chunk/{index}", self._handle_upload_chunk)
        app.router.add_route("GET", "/upload/{session_id}/status", self._handle_upload_status)
        app.router.add_route("POST", "/upload/{session_id}/complete", self._handle_upload_complete)
        app.router.add_route("DELETE", "/upload/{session_id}", self._handle_upload_abort)

        self._runner = web.AppRunner(app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self.host, self.port)
        await self._site.start()
        sockets = self._site._server.sockets  # type: ignore[union-attr]
        self.bound_port = sockets[0].getsockname()[1]

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
            self._site = None

    async def _handle_ranged_head(self, request: web.Request) -> web.StreamResponse:
        sf = self.files.get(request.match_info["name"])
        if sf is None:
            raise web.HTTPNotFound()
        return web.Response(
            headers={
                "Content-Length": str(len(sf.data)),
                "Accept-Ranges": "bytes",
                "X-Content-SHA256": sf.sha256,
            }
        )

    async def _handle_ranged_get(self, request: web.Request) -> web.StreamResponse:
        sf = self.files.get(request.match_info["name"])
        if sf is None:
            raise web.HTTPNotFound()
        if self.artificial_delay_s > 0:
            await asyncio.sleep(self.artificial_delay_s)

        range_hdr = request.headers.get("Range")
        total = len(sf.data)
        if range_hdr and range_hdr.startswith("bytes="):
            spec = range_hdr.replace("bytes=", "", 1).split(",", 1)[0]
            start_s, end_s = spec.split("-", 1)
            start = int(start_s) if start_s else 0
            end = int(end_s) if end_s else total - 1
            end = min(end, total - 1)
            if start > end or start >= total:
                raise web.HTTPRequestRangeNotSatisfiable()
            payload = sf.data[start : end + 1]
            return web.Response(
                status=206,
                body=payload,
                headers={
                    "Content-Length": str(len(payload)),
                    "Content-Range": f"bytes {start}-{end}/{total}",
                    "Accept-Ranges": "bytes",
                    "X-Content-SHA256": sf.sha256,
                },
            )

        return web.Response(
            body=sf.data,
            headers={
                "Content-Length": str(total),
                "Accept-Ranges": "bytes",
                "X-Content-SHA256": sf.sha256,
            },
        )

    async def _handle_noranges_head(self, request: web.Request) -> web.StreamResponse:
        sf = self.files.get(request.match_info["name"])
        if sf is None:
            raise web.HTTPNotFound()
        return web.Response(
            headers={
                "Content-Length": str(len(sf.data)),
                "Accept-Ranges": "none",
                "X-Content-SHA256": sf.sha256,
            }
        )

    async def _handle_noranges_get(self, request: web.Request) -> web.StreamResponse:
        sf = self.files.get(request.match_info["name"])
        if sf is None:
            raise web.HTTPNotFound()
        if self.artificial_delay_s > 0:
            await asyncio.sleep(self.artificial_delay_s)
        return web.Response(
            body=sf.data,
            headers={
                "Content-Length": str(len(sf.data)),
                "Accept-Ranges": "none",
                "X-Content-SHA256": sf.sha256,
            },
        )

    async def _handle_upload_init(self, request: web.Request) -> web.Response:
        try:
            body = await request.json()
            res = self.upload_store.init_session(
                filename=body["filename"],
                total_bytes=int(body["total_bytes"]),
                chunk_size=int(body["chunk_size"]),
                transfer_id=body.get("transfer_id"),
                expected_sha256=body.get("expected_sha256"),
            )
            return web.json_response(res, status=201)
        except (KeyError, ValueError) as exc:
            raise web.HTTPBadRequest(text=str(exc))

    async def _handle_upload_chunk(self, request: web.Request) -> web.Response:
        if self.artificial_delay_s > 0:
            await asyncio.sleep(self.artificial_delay_s)
        sid = request.match_info["session_id"]
        try:
            idx = int(request.match_info["index"])
        except ValueError:
            raise web.HTTPBadRequest(text="Invalid chunk index")

        if idx in self.fail_once_upload_chunks:
            self.fail_once_upload_chunks.remove(idx)
            raise web.HTTPServiceUnavailable(text="Simulated transient chunk failure")

        tid = request.headers.get("X-Transfer-Id", "")
        start_hdr = request.headers.get("X-Chunk-Start")
        end_hdr = request.headers.get("X-Chunk-End")
        chunk_sha = request.headers.get("X-Chunk-SHA256")
        payload = await request.read()
        try:
            res = self.upload_store.store_chunk(
                session_id=sid,
                transfer_id=tid,
                index=idx,
                payload=payload,
                start_byte=int(start_hdr) if start_hdr is not None else None,
                end_byte=int(end_hdr) if end_hdr is not None else None,
                chunk_sha256=chunk_sha,
            )
            return web.json_response(res)
        except KeyError as exc:
            raise web.HTTPNotFound(text=str(exc))
        except PermissionError as exc:
            raise web.HTTPForbidden(text=str(exc))
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc))

    async def _handle_upload_status(self, request: web.Request) -> web.Response:
        sid = request.match_info["session_id"]
        try:
            return web.json_response(self.upload_store.get_status(sid))
        except KeyError as exc:
            raise web.HTTPNotFound(text=str(exc))

    async def _handle_upload_complete(self, request: web.Request) -> web.Response:
        sid = request.match_info["session_id"]
        try:
            body = await request.json()
            tid = body["transfer_id"]
            res = await asyncio.to_thread(
                self.upload_store.finalize_session,
                session_id=sid,
                transfer_id=tid,
            )
            return web.json_response(res)
        except KeyError as exc:
            raise web.HTTPNotFound(text=str(exc))
        except PermissionError as exc:
            raise web.HTTPForbidden(text=str(exc))
        except (ValueError, IOError) as exc:
            raise web.HTTPBadRequest(text=str(exc))

    async def _handle_upload_abort(self, request: web.Request) -> web.Response:
        sid = request.match_info["session_id"]
        removed = self.upload_store.abort_session(sid)
        return web.json_response({"session_id": sid, "aborted": removed})
