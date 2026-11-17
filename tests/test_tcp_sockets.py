"""Tests for FlowWeave's raw TCP socket framing protocol, server, client, and coordinator integration."""

from __future__ import annotations

import socket
import threading
from pathlib import Path

import pytest

from flowweave.config import FlowWeaveConfig, SchedulerConfig, TransferConfig
from flowweave.coordinator import FlowWeaveCoordinator
from flowweave.models import PolicyName, TransferDirection, TransferState
from flowweave.transfer import (
    AsyncRateLimiter,
    FlowWeaveTCPServer,
    TCPDownloadEngine,
    TCPUploadEngine,
    compute_sha256,
    recv_frame_sync,
    send_frame_sync,
)


def test_raw_tcp_socket_binary_framing_roundtrip() -> None:
    """Verify length-prefixed FWV1 binary framing over standard Python blocking TCP sockets."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    port = srv.getsockname()[1]

    received: dict[str, object] = {}

    def _server_thread() -> None:
        conn, _ = srv.accept()
        try:
            hdr, body = recv_frame_sync(conn)
            received["hdr"] = hdr
            received["body"] = body
            send_frame_sync(conn, {"ok": True, "echo_len": len(body)}, b"ACK")
        finally:
            conn.close()
            srv.close()

    t = threading.Thread(target=_server_thread, daemon=True)
    t.start()

    client = socket.create_connection(("127.0.0.1", port), timeout=5.0)
    try:
        payload = bytes((i * 13) % 256 for i in range(100_000))
        send_frame_sync(client, {"op": "PING", "seq": 1}, payload)
        resp_hdr, resp_body = recv_frame_sync(client)
    finally:
        client.close()
    t.join(timeout=5.0)

    assert received["body"] == payload
    assert resp_hdr["ok"] is True
    assert resp_hdr["echo_len"] == len(payload)
    assert resp_body == b"ACK"


@pytest.mark.asyncio
async def test_tcp_socket_parallel_download_and_upload(tmp_path) -> None:
    serve_dir = tmp_path / "serve"
    upload_dir = tmp_path / "uploads"
    dl_dir = tmp_path / "downloads"
    serve_dir.mkdir()

    src_dl = serve_dir / "tcp_sample.bin"
    raw_dl = bytes((i * 29) % 256 for i in range(600 * 1024))
    src_dl.write_bytes(raw_dl)
    sha_dl = compute_sha256(src_dl)

    src_ul = tmp_path / "local_upload.bin"
    raw_ul = bytes((i * 7 + 5) % 256 for i in range(520 * 1024))
    src_ul.write_bytes(raw_ul)
    sha_ul = compute_sha256(src_ul)

    server = FlowWeaveTCPServer(serve_dir=serve_dir, upload_dir=upload_dir)
    await server.start()
    try:
        cfg = TransferConfig(chunk_size_bytes=128 * 1024, max_chunks_per_transfer=4)
        dl_engine = TCPDownloadEngine(cfg)
        ul_engine = TCPUploadEngine(cfg)

        dl_res = await dl_engine.download(
            transfer_id="tcp-dl-1",
            url=server.url_for("tcp_sample.bin"),
            dest_path=dl_dir / "tcp_sample.bin",
            rate_limiter=AsyncRateLimiter(20 * 1024 * 1024),
        )
        assert dl_res.total_bytes == len(raw_dl)
        assert dl_res.total_chunks == 5
        assert dl_res.sha256 == sha_dl
        assert compute_sha256(dl_res.dest_path) == sha_dl

        ul_res = await ul_engine.upload(
            transfer_id="tcp-ul-1",
            file_path=src_ul,
            server_url=server.address,
            rate_limiter=AsyncRateLimiter(20 * 1024 * 1024),
        )
        assert ul_res.total_bytes == len(raw_ul)
        assert ul_res.total_chunks == 5
        assert ul_res.sha256 == sha_ul
        assert compute_sha256(Path(ul_res.dest_path)) == sha_ul
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_coordinator_schedules_bidirectional_tcp_sockets(tmp_path) -> None:
    serve_dir = tmp_path / "serve"
    serve_dir.mkdir()
    f1 = serve_dir / "a_small.bin"
    f2 = serve_dir / "b_large.bin"
    f1.write_bytes(b"A" * (256 * 1024))
    f2.write_bytes(b"B" * (512 * 1024))

    server = FlowWeaveTCPServer(serve_dir=serve_dir, upload_dir=tmp_path / "uploads")
    await server.start()

    cfg = FlowWeaveConfig(
        db_path=tmp_path / "tcp_coord.db",
        control_interval_s=0.05,
        scheduler=SchedulerConfig(policy=PolicyName.FLOWWEAVE),
        transfer=TransferConfig(
            download_dir=tmp_path / "dl",
            upload_dir=tmp_path / "uploads",
            chunk_size_bytes=128 * 1024,
            max_chunks_per_transfer=2,
            total_link_capacity_bps=4 * 1024 * 1024,
            upload_link_capacity_bps=4 * 1024 * 1024,
        ),
    )
    coord = FlowWeaveCoordinator(cfg)
    await coord.start()
    try:
        t_dl = await coord.transfer_manager.submit_download(server.url_for("a_small.bin"))
        t_ul = await coord.transfer_manager.submit_upload(f2, server.address)

        await coord.transfer_manager.wait_all(timeout=15.0)
        s_dl = coord.transfer_manager.get_snapshot(t_dl.transfer_id)
        s_ul = coord.transfer_manager.get_snapshot(t_ul.transfer_id)
        assert s_dl is not None and s_dl.state == TransferState.COMPLETED
        assert s_ul is not None and s_ul.state == TransferState.COMPLETED
        assert s_dl.direction == TransferDirection.DOWNLOAD
        assert s_ul.direction == TransferDirection.UPLOAD
        assert s_dl.sha256 == compute_sha256(f1)
        assert s_ul.sha256 == compute_sha256(f2)
    finally:
        await coord.stop()
        await server.stop()
