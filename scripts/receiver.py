#!/usr/bin/env python3
"""Standalone zero-dependency FlowWeave TCP receiver/peer (Python 3 standard library only).

Copy this single file to any other laptop (Windows, macOS, or Linux) and run:
    python receiver.py --port 5000

No pip install required. Speaks FlowWeave's FWV1 binary-framed TCP socket protocol so
FlowWeave can upload multiple files simultaneously to this host, browse files in
`./received_files`, and download them back while comparing SRPT vs FlowWeave.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import re
import shutil
import socket
import struct
import time
import uuid
from pathlib import Path

MAGIC = b"FWV1"
PREFIX_FMT = "!4sI"
PREFIX_SIZE = struct.calcsize(PREFIX_FMT)
MAX_HEADER = 64 * 1024
MAX_PAYLOAD = 32 * 1024 * 1024
SAFE_RE = re.compile(r"[^A-Za-z0-9._-]")


def detect_lan_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            return str(s.getsockname()[0])
        finally:
            s.close()
    except OSError:
        return "127.0.0.1"


def sanitize_name(raw: str) -> str:
    if not raw or "\x00" in raw or "/" in raw or "\\" in raw or ".." in raw:
        raise ValueError(f"Unsafe filename: {raw!r}")
    return SAFE_RE.sub("_", raw).strip(".") or "file.bin"


def encode_frame(header: dict, payload: bytes = b"") -> bytes:
    meta = dict(header)
    meta["payload_len"] = len(payload)
    hb = json.dumps(meta, separators=(",", ":")).encode("utf-8")
    return struct.pack(PREFIX_FMT, MAGIC, len(hb)) + hb + payload


async def recv_frame(reader: asyncio.StreamReader) -> tuple[dict, bytes]:
    prefix = await reader.readexactly(PREFIX_SIZE)
    magic, hlen = struct.unpack(PREFIX_FMT, prefix)
    if magic != MAGIC or hlen > MAX_HEADER:
        raise ValueError("Invalid FWV1 frame header")
    hdr = json.loads((await reader.readexactly(hlen)).decode("utf-8"))
    plen = int(hdr.get("payload_len", 0))
    if plen < 0 or plen > MAX_PAYLOAD:
        raise ValueError("Invalid payload length")
    payload = await reader.readexactly(plen) if plen > 0 else b""
    return hdr, payload


class StandaloneReceiver:
    def __init__(self, save_dir: Path) -> None:
        self.save_dir = save_dir
        self.staging_root = save_dir / ".staging"
        self.save_dir.mkdir(parents=True, exist_ok=True)
        self.staging_root.mkdir(parents=True, exist_ok=True)
        self.sessions: dict[str, dict] = {}

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = writer.get_extra_info("peername")
        raw_sock = writer.get_extra_info("socket")
        if raw_sock is not None:
            try:
                raw_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass
        try:
            while True:
                try:
                    hdr, payload = await recv_frame(reader)
                except (asyncio.IncompleteReadError, ConnectionResetError, EOFError):
                    break
                op = str(hdr.get("op", "")).upper()
                try:
                    resp, body = await self._dispatch(op, hdr, payload, peer)
                except Exception as exc:
                    resp, body = {"ok": False, "error": str(exc)}, b""
                writer.write(encode_frame(resp, body))
                await writer.drain()
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def _dispatch(self, op: str, hdr: dict, payload: bytes, peer) -> tuple[dict, bytes]:
        if op == "LIST":
            files = [
                {"filename": p.name, "size_bytes": p.stat().st_size}
                for p in sorted(self.save_dir.iterdir())
                if p.is_file() and not p.name.startswith(".")
            ]
            return {"ok": True, "files": files}, b""

        if op == "STAT":
            name = sanitize_name(str(hdr["filename"]))
            p = self.save_dir / name
            if not p.is_file():
                raise FileNotFoundError(f"Not found: {name}")
            sha = hashlib.sha256(p.read_bytes()).hexdigest()
            return {"ok": True, "filename": p.name, "size_bytes": p.stat().st_size, "sha256": sha}, b""

        if op == "GET_CHUNK":
            name = sanitize_name(str(hdr["filename"]))
            p = self.save_dir / name
            offset = int(hdr["offset"])
            length = int(hdr["length"])
            with p.open("rb") as fh:
                fh.seek(offset)
                data = fh.read(length)
            return {
                "ok": True,
                "offset": offset,
                "length": len(data),
                "chunk_sha256": hashlib.sha256(data).hexdigest(),
            }, data

        if op == "PUT_INIT":
            name = sanitize_name(str(hdr["filename"]))
            total = int(hdr["total_bytes"])
            chunk_size = int(hdr["chunk_size"])
            tid = str(hdr.get("transfer_id") or uuid.uuid4().hex[:8])
            sid = uuid.uuid4().hex[:12]
            expected_chunks = max(1, math.ceil(total / chunk_size)) if total > 0 else 1
            sdir = self.staging_root / sid
            sdir.mkdir(parents=True, exist_ok=True)
            self.sessions[sid] = {
                "transfer_id": tid,
                "filename": name,
                "total_bytes": total,
                "chunk_size": chunk_size,
                "expected_chunks": expected_chunks,
                "expected_sha256": hdr.get("expected_sha256"),
                "staging_dir": sdir,
                "received": set(),
            }
            print(f"[RECV] Started upload from {peer}: {name} ({total:,} bytes, {expected_chunks} chunks)")
            return {
                "ok": True,
                "session_id": sid,
                "transfer_id": tid,
                "filename": name,
                "expected_chunks": expected_chunks,
                "existing_chunks": [],
            }, b""

        if op == "PUT_CHUNK":
            sid = str(hdr["session_id"])
            sess = self.sessions[sid]
            idx = int(hdr["index"])
            if hdr.get("chunk_sha256") and hashlib.sha256(payload).hexdigest() != hdr["chunk_sha256"]:
                raise ValueError(f"Chunk {idx} SHA-256 mismatch")
            part = sess["staging_dir"] / f"chunk_{idx:06d}.part"
            part.write_bytes(payload)
            sess["received"].add(idx)
            return {"ok": True, "session_id": sid, "chunk_index": idx, "bytes_written": len(payload)}, b""

        if op == "PUT_COMPLETE":
            sid = str(hdr["session_id"])
            sess = self.sessions[sid]
            out_path = self.save_dir / f"{sess['transfer_id']}_{sess['filename']}"
            tmp_path = sess["staging_dir"] / "final.tmp"
            hasher = hashlib.sha256()
            written = 0
            with tmp_path.open("wb") as out_fh:
                for idx in range(sess["expected_chunks"]):
                    data = (sess["staging_dir"] / f"chunk_{idx:06d}.part").read_bytes()
                    out_fh.write(data)
                    hasher.update(data)
                    written += len(data)
            sha = hasher.hexdigest()
            if sess["expected_sha256"] and sha.lower() != str(sess["expected_sha256"]).lower():
                tmp_path.unlink(missing_ok=True)
                raise ValueError("Full file SHA-256 mismatch")
            tmp_path.replace(out_path)
            shutil.rmtree(sess["staging_dir"], ignore_errors=True)
            print(f"[DONE] Saved {out_path.name} ({written:,} bytes, sha256={sha[:12]}...)")
            return {
                "ok": True,
                "session_id": sid,
                "dest_path": str(out_path),
                "total_bytes": written,
                "sha256": sha,
            }, b""

        raise ValueError(f"Unknown op: {op}")


async def run_server(host: str, port: int, save_dir: Path) -> None:
    receiver = StandaloneReceiver(save_dir)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen(128)
    sock.setblocking(False)
    server = await asyncio.start_server(receiver.handle_client, sock=sock)
    lan_ip = detect_lan_ip()
    print("=" * 60)
    print(f"FlowWeave Standalone TCP Receiver listening on {lan_ip}:{port}")
    print(f"Saving received files to: {save_dir.resolve()}")
    print("If another laptop times out, allow inbound TCP in Windows Firewall:")
    print(f'  New-NetFirewallRule -DisplayName "FlowWeave TCP {port}" -Direction Inbound -Protocol TCP -LocalPort {port} -Action Allow')
    print("=" * 60)
    async with server:
        await server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description="Standalone FlowWeave TCP socket receiver")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--dir", default="received_files")
    args = parser.parse_args()
    asyncio.run(run_server(args.host, args.port, Path(args.dir)))


if __name__ == "__main__":
    main()
