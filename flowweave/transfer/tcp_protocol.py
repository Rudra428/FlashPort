"""Simple length-prefixed binary framing protocol over raw TCP sockets.

TCP is a byte stream, not message-oriented, so every FlowWeave socket message is framed as:
    [4-byte magic b"FWV1"][4-byte big-endian header_len][JSON header][payload_len raw bytes]
"""

from __future__ import annotations

import asyncio
import json
import socket
import struct
from typing import Any

MAGIC = b"FWV1"
PREFIX_FMT = "!4sI"
PREFIX_SIZE = struct.calcsize(PREFIX_FMT)
MAX_HEADER_BYTES = 64 * 1024
MAX_PAYLOAD_BYTES = 32 * 1024 * 1024


def encode_frame(header: dict[str, Any], payload: bytes = b"") -> bytes:
    """Serialize a metadata header and raw binary payload into a TCP wire frame."""
    meta = dict(header)
    meta["payload_len"] = len(payload)
    header_bytes = json.dumps(meta, separators=(",", ":")).encode("utf-8")
    if len(header_bytes) > MAX_HEADER_BYTES:
        raise ValueError("Frame header exceeds maximum size")
    prefix = struct.pack(PREFIX_FMT, MAGIC, len(header_bytes))
    return prefix + header_bytes + payload


def recv_exact_sync(sock: socket.socket, n: int) -> bytes:
    """Read exactly `n` bytes from a blocking TCP socket (handles partial TCP reads)."""
    chunks: list[bytes] = []
    remaining = n
    while remaining > 0:
        data = sock.recv(remaining)
        if not data:
            raise ConnectionError(f"TCP socket closed early ({n - remaining}/{n} bytes read)")
        chunks.append(data)
        remaining -= len(data)
    return b"".join(chunks)


def send_frame_sync(sock: socket.socket, header: dict[str, Any], payload: bytes = b"") -> None:
    """Send a complete framed message over a blocking TCP socket."""
    sock.sendall(encode_frame(header, payload))


def recv_frame_sync(sock: socket.socket) -> tuple[dict[str, Any], bytes]:
    """Receive one complete framed message from a blocking TCP socket."""
    prefix = recv_exact_sync(sock, PREFIX_SIZE)
    magic, header_len = struct.unpack(PREFIX_FMT, prefix)
    if magic != MAGIC:
        raise ValueError(f"Invalid TCP protocol magic: {magic!r}")
    if header_len > MAX_HEADER_BYTES:
        raise ValueError(f"Header too large: {header_len}")

    header = json.loads(recv_exact_sync(sock, header_len).decode("utf-8"))
    payload_len = int(header.get("payload_len", 0))
    if payload_len < 0 or payload_len > MAX_PAYLOAD_BYTES:
        raise ValueError(f"Invalid payload_len: {payload_len}")
    payload = recv_exact_sync(sock, payload_len) if payload_len > 0 else b""
    return header, payload


async def send_frame_async(
    writer: asyncio.StreamWriter,
    header: dict[str, Any],
    payload: bytes = b"",
) -> None:
    """Send a complete framed message over an asyncio TCP StreamWriter."""
    writer.write(encode_frame(header, payload))
    await writer.drain()


async def recv_frame_async(reader: asyncio.StreamReader) -> tuple[dict[str, Any], bytes]:
    """Receive one complete framed message from an asyncio TCP StreamReader."""
    prefix = await reader.readexactly(PREFIX_SIZE)
    magic, header_len = struct.unpack(PREFIX_FMT, prefix)
    if magic != MAGIC:
        raise ValueError(f"Invalid TCP protocol magic: {magic!r}")
    if header_len > MAX_HEADER_BYTES:
        raise ValueError(f"Header too large: {header_len}")

    header_bytes = await reader.readexactly(header_len)
    header = json.loads(header_bytes.decode("utf-8"))
    payload_len = int(header.get("payload_len", 0))
    if payload_len < 0 or payload_len > MAX_PAYLOAD_BYTES:
        raise ValueError(f"Invalid payload_len: {payload_len}")
    payload = await reader.readexactly(payload_len) if payload_len > 0 else b""
    return header, payload
