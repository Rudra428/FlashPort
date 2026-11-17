"""Multi-port, multi-client raw TCP socket server for FlowWeave chunked downloads and uploads.

Listens on all IPv4 interfaces (`0.0.0.0`) across one or more TCP ports simultaneously,
saves received files directly into the receiver's Downloads folder with SHA-256 verification,
and tracks live incoming streams for the Receiver dashboard tab.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import socket
import time
from pathlib import Path
from typing import Any

from flowweave.transfer.engine import compute_sha256
from flowweave.transfer.security import safe_resolve_under
from flowweave.transfer.tcp_protocol import recv_frame_async, send_frame_async
from flowweave.transfer.upload_server import ChunkedUploadStore

logger = logging.getLogger("flowweave.tcp_server")


def detect_all_local_ips() -> list[str]:
    """Return all reachable local IPv4 addresses on this machine (Wi-Fi, Ethernet, Hotspot, loopback)."""
    ips: list[str] = []
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            primary = str(s.getsockname()[0])
            if primary not in ips:
                ips.append(primary)
        finally:
            s.close()
    except OSError:
        pass

    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ip = str(info[4][0])
            if ip not in ips and not ip.startswith("169.254."):
                ips.append(ip)
    except OSError:
        pass

    if "127.0.0.1" not in ips:
        ips.append("127.0.0.1")
    return ips


class FlowWeaveTCPServer:
    """Raw TCP socket peer server listening on all interfaces (`0.0.0.0`) and one or more ports."""

    def __init__(
        self,
        *,
        host: str = "0.0.0.0",
        port: int = 0,
        serve_dir: str | Path = "sample_files",
        upload_dir: str | Path = "uploads",
    ) -> None:
        self.host = host
        self.port = port
        self.serve_dir = Path(serve_dir)
        self.serve_dir.mkdir(parents=True, exist_ok=True)
        self.upload_store = ChunkedUploadStore(upload_dir)
        self._servers: dict[int, asyncio.AbstractServer] = {}
        self.bound_port: int = port
        self._sha_cache: dict[Path, tuple[int, str]] = {}
        self._incoming: dict[str, dict[str, Any]] = {}
        # Track files explicitly received via FlowWeave so we never expose unrelated personal Downloads
        self._received_files: dict[str, dict[str, Any]] = {}

    @property
    def connect_host(self) -> str:
        return "127.0.0.1" if self.host == "0.0.0.0" else self.host

    @property
    def address(self) -> str:
        return f"tcp://{self.connect_host}:{self.bound_port}"

    @property
    def is_listening(self) -> bool:
        return any(srv.is_serving() for srv in self._servers.values())

    @property
    def listening_ports(self) -> list[int]:
        return sorted(self._servers.keys())

    def url_for(self, filename: str) -> str:
        return f"tcp://{self.connect_host}:{self.bound_port}/{filename}"

    async def start(self) -> None:
        """Bind primary IPv4 TCP listening socket on `self.host:self.port`."""
        bound = await self.add_port(self.port, allow_ephemeral_fallback=True)
        self.bound_port = bound

    async def add_port(self, port: int, *, allow_ephemeral_fallback: bool = False) -> int:
        """Start listening on an additional TCP port on all interfaces (`self.host`)."""
        if port != 0 and port in self._servers:
            return port
        if port < 0 or port > 65535:
            raise ValueError(f"Invalid TCP port: {port}")

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # On POSIX, SO_REUSEADDR allows rebinding TIME_WAIT sockets cleanly.
        # On Windows, SO_REUSEADDR allows multiple processes to bind the same port simultaneously
        # and silently hijack incoming packets, so avoid it on Windows.
        if os.name != "nt":
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((self.host, port))
        except OSError as exc:
            if allow_ephemeral_fallback and port != 0:
                sock.bind((self.host, 0))
            else:
                sock.close()
                raise OSError(f"Port {port} is unavailable: {exc}") from exc

        sock.listen(128)
        sock.setblocking(False)
        actual_port = int(sock.getsockname()[1])

        async def _handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            await self._handle_client(reader, writer, local_port=actual_port)

        server = await asyncio.start_server(_handler, sock=sock)
        self._servers[actual_port] = server
        if self.bound_port == 0:
            self.bound_port = actual_port
        logger.info("FlowWeave TCP receiver listening on %s:%d", self.host, actual_port)
        return actual_port

    async def remove_port(self, port: int) -> bool:
        srv = self._servers.pop(port, None)
        if srv is None:
            return False
        srv.close()
        await srv.wait_closed()
        if self.bound_port == port:
            self.bound_port = next(iter(self._servers.keys()), 0)
        return True

    async def stop(self) -> None:
        for srv in list(self._servers.values()):
            srv.close()
            await srv.wait_closed()
        self._servers.clear()

    async def restart_on_port(self, new_port: int) -> int:
        """Rebind the primary TCP receiver onto `new_port`."""
        if new_port < 1 or new_port > 65535:
            raise ValueError(f"Invalid TCP port: {new_port}")
        if new_port in self._servers:
            self.bound_port = new_port
            return new_port
        old_ports = list(self._servers.keys())
        await self.stop()
        try:
            bound = await self.add_port(new_port, allow_ephemeral_fallback=False)
            self.port = bound
            self.bound_port = bound
            return bound
        except OSError:
            for p in old_ports:
                await self.add_port(p, allow_ephemeral_fallback=True)
            raise

    async def serve_forever(self) -> None:
        if not self._servers:
            await self.start()
        await asyncio.gather(*(srv.serve_forever() for srv in self._servers.values()))

    def get_incoming_transfers(self) -> list[dict[str, Any]]:
        """Return active and recently completed incoming transfers received over TCP."""
        items = list(self._incoming.values())
        items.sort(key=lambda x: x["started_at"], reverse=True)
        return items[:40]

    def list_received_files(self) -> list[dict[str, Any]]:
        """List files received by FlowWeave (saved in Downloads) + explicitly shared files."""
        files: list[dict[str, Any]] = []
        seen: set[str] = set()

        # 1. Files received via FlowWeave in this session (live verified in Downloads)
        for name, info in sorted(
            self._received_files.items(), key=lambda kv: kv[1]["received_at"], reverse=True
        ):
            p = Path(info["dest_path"])
            if p.is_file() and name not in seen:
                seen.add(name)
                files.append(
                    {
                        "filename": name,
                        "size_bytes": p.stat().st_size,
                        "dest_path": str(p),
                        "sha256": info.get("sha256"),
                        "peer_ip": info.get("peer_ip"),
                        "received_at": info["received_at"],
                    }
                )

        # 2. Explicitly shared files in serve_dir (e.g. unit tests or user-shared folder)
        if self.serve_dir.is_dir():
            for p in sorted(self.serve_dir.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True):
                if p.is_file() and not p.name.startswith(".") and p.name not in seen:
                    seen.add(p.name)
                    st = p.stat()
                    files.append(
                        {
                            "filename": p.name,
                            "size_bytes": st.st_size,
                            "dest_path": str(p),
                            "sha256": None,
                            "peer_ip": "local",
                            "received_at": st.st_mtime,
                        }
                    )
        return files

    def resolve_received_file(self, filename: str) -> Path:
        return self._resolve_readable_file(filename)

    def delete_received_file(self, filename: str) -> bool:
        info = self._received_files.pop(filename, None)
        if info is not None:
            p = Path(info["dest_path"])
            if p.is_file():
                p.unlink()
                return True
        candidate = safe_resolve_under(self.serve_dir, filename)
        if candidate.is_file():
            candidate.unlink()
            return True
        return False

    def _resolve_readable_file(self, filename: str) -> Path:
        info = self._received_files.get(filename)
        if info is not None:
            p = Path(info["dest_path"])
            if p.is_file():
                return p
        candidate = safe_resolve_under(self.serve_dir, filename)
        if candidate.is_file():
            return candidate
        # Backwards-compat: also check upload_dir if a file was uploaded prior to tracking
        up_candidate = safe_resolve_under(self.upload_store.upload_dir, filename)
        if up_candidate.is_file():
            return up_candidate
        raise FileNotFoundError(f"File not found: {filename}")

    def _file_sha256(self, path: Path) -> str:
        size = path.stat().st_size
        cached = self._sha_cache.get(path)
        if cached is not None and cached[0] == size:
            return cached[1]
        digest = compute_sha256(path)
        self._sha_cache[path] = (size, digest)
        return digest

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        local_port: int,
    ) -> None:
        peer = writer.get_extra_info("peername")
        peer_ip = str(peer[0]) if isinstance(peer, tuple) and peer else "unknown"
        raw_sock = writer.get_extra_info("socket")
        if raw_sock is not None:
            try:
                raw_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                raw_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            except OSError:
                pass

        try:
            while True:
                try:
                    header, payload = await recv_frame_async(reader)
                except (asyncio.IncompleteReadError, ConnectionResetError, EOFError):
                    break

                op = str(header.get("op", "")).upper()
                try:
                    resp_hdr, resp_payload = await self._dispatch(
                        op, header, payload, peer_ip, local_port
                    )
                except KeyError as exc:
                    resp_hdr, resp_payload = {"ok": False, "error": str(exc), "code": 404}, b""
                except PermissionError as exc:
                    resp_hdr, resp_payload = {"ok": False, "error": str(exc), "code": 403}, b""
                except (ValueError, FileNotFoundError, IOError) as exc:
                    resp_hdr, resp_payload = {"ok": False, "error": str(exc), "code": 400}, b""

                await send_frame_async(writer, resp_hdr, resp_payload)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def _dispatch(
        self,
        op: str,
        header: dict,
        payload: bytes,
        peer_ip: str,
        local_port: int,
    ) -> tuple[dict, bytes]:
        if op == "LIST":
            files = [
                {"filename": f["filename"], "size_bytes": f["size_bytes"]}
                for f in self.list_received_files()
            ]
            return {"ok": True, "files": files}, b""

        if op == "STAT":
            path = self._resolve_readable_file(str(header["filename"]))
            sha = await asyncio.to_thread(self._file_sha256, path)
            return {
                "ok": True,
                "filename": path.name,
                "size_bytes": path.stat().st_size,
                "sha256": sha,
            }, b""

        if op == "GET_CHUNK":
            path = self._resolve_readable_file(str(header["filename"]))
            offset = int(header["offset"])
            length = int(header["length"])
            file_size = path.stat().st_size
            if offset < 0 or length < 0 or offset > file_size:
                raise ValueError(f"Invalid chunk range offset={offset}, length={length}")

            def _read() -> bytes:
                with path.open("rb") as fh:
                    fh.seek(offset)
                    return fh.read(min(length, file_size - offset))

            data = await asyncio.to_thread(_read)
            return {
                "ok": True,
                "offset": offset,
                "length": len(data),
                "chunk_sha256": hashlib.sha256(data).hexdigest(),
            }, data

        if op == "PUT_INIT":
            res = self.upload_store.init_session(
                filename=str(header["filename"]),
                total_bytes=int(header["total_bytes"]),
                chunk_size=int(header["chunk_size"]),
                transfer_id=header.get("transfer_id"),
                expected_sha256=header.get("expected_sha256"),
            )
            sid = res["session_id"]
            self._incoming[sid] = {
                "session_id": sid,
                "transfer_id": res["transfer_id"],
                "filename": res["filename"],
                "peer_ip": peer_ip,
                "local_port": local_port,
                "total_bytes": int(header["total_bytes"]),
                "received_bytes": 0,
                "received_chunks": len(res.get("existing_chunks", [])),
                "expected_chunks": res["expected_chunks"],
                "progress": 0.0,
                "status": "receiving",
                "started_at": time.time(),
                "completed_at": None,
                "saved_as": None,
                "dest_path": None,
                "sha256": None,
            }
            if len(self._incoming) > 50:
                oldest = sorted(self._incoming.keys(), key=lambda k: self._incoming[k]["started_at"])[0]
                self._incoming.pop(oldest, None)
            return {"ok": True, **res}, b""

        if op == "PUT_CHUNK":
            sid = str(header["session_id"])
            res = await asyncio.to_thread(
                self.upload_store.store_chunk,
                session_id=sid,
                transfer_id=str(header["transfer_id"]),
                index=int(header["index"]),
                payload=payload,
                start_byte=header.get("start_byte"),
                end_byte=header.get("end_byte"),
                chunk_sha256=header.get("chunk_sha256"),
            )
            entry = self._incoming.get(sid)
            if entry is not None:
                entry["received_bytes"] += len(payload)
                entry["received_chunks"] = res["received_chunks"]
                total = max(1, entry["total_bytes"])
                entry["progress"] = min(1.0, entry["received_bytes"] / total)
            return {"ok": True, **res}, b""

        if op == "PUT_COMPLETE":
            sid = str(header["session_id"])
            res = await asyncio.to_thread(
                self.upload_store.finalize_session,
                session_id=sid,
                transfer_id=str(header["transfer_id"]),
            )
            now = time.time()
            entry = self._incoming.get(sid)
            if entry is not None:
                entry["received_bytes"] = res["total_bytes"]
                entry["progress"] = 1.0
                entry["status"] = "saved"
                entry["completed_at"] = now
                entry["saved_as"] = res["filename"]
                entry["dest_path"] = res["dest_path"]
                entry["sha256"] = res["sha256"]
            self._received_files[res["filename"]] = {
                "filename": res["filename"],
                "dest_path": res["dest_path"],
                "sha256": res["sha256"],
                "peer_ip": peer_ip,
                "received_at": now,
            }
            return {"ok": True, **res}, b""

        raise ValueError(f"Unknown TCP op: {op!r}")
