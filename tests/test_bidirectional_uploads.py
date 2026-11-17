"""Tests for chunked uploads, resumability, retries, security validation, pause/resume, and mixed workloads."""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

import aiohttp
import pytest

from flowweave.config import FlowWeaveConfig, SchedulerConfig, TransferConfig
from flowweave.coordinator import FlowWeaveCoordinator
from flowweave.models import PolicyName, TransferDirection, TransferState
from flowweave.transfer import (
    AsyncRateLimiter,
    ChunkedUploadStore,
    HTTPUploadEngine,
    LocalFileServer,
    compute_sha256,
    safe_resolve_under,
    sanitize_filename,
)


def test_security_rejects_path_traversal_and_bad_chunks(tmp_path) -> None:
    for bad in ("../etc/passwd", "sub/file.bin", "sub\\file.bin", "bad\x00name.bin", ""):
        with pytest.raises(ValueError):
            sanitize_filename(bad)

    assert sanitize_filename("report 2026.bin") == "report_2026.bin"
    resolved = safe_resolve_under(tmp_path, "safe_file.bin")
    assert resolved.parent == tmp_path.resolve()

    store = ChunkedUploadStore(tmp_path / "uploads")
    init = store.init_session(filename="a.bin", total_bytes=1024, chunk_size=512, transfer_id="owner-1")
    sid = init["session_id"]

    # Ownership mismatch rejected
    with pytest.raises(PermissionError):
        store.store_chunk(
            session_id=sid,
            transfer_id="intruder",
            index=0,
            payload=b"x" * 512,
        )

    # Wrong chunk boundary / length rejected
    with pytest.raises(ValueError):
        store.store_chunk(
            session_id=sid,
            transfer_id="owner-1",
            index=0,
            payload=b"short",
        )

    # Abandoned session cleanup removes staging folder
    store._sessions[sid].updated_at = time.time() - 7200
    removed = store.cleanup_abandoned(max_age_s=60.0)
    assert removed == 1
    assert not (tmp_path / "uploads" / ".staging" / sid).exists()


@pytest.mark.asyncio
async def test_chunked_upload_with_retry_and_sha256(tmp_path) -> None:
    src = tmp_path / "source_payload.bin"
    raw = bytes((i * 17) % 256 for i in range(600 * 1024))
    src.write_bytes(raw)
    expected_sha = compute_sha256(src)

    server = LocalFileServer(upload_dir=tmp_path / "srv_uploads")
    # Force chunk 1 to fail once with HTTP 503 to verify real retry recovery.
    server.fail_once_upload_chunks.add(1)
    await server.start()
    try:
        engine = HTTPUploadEngine(
            TransferConfig(
                chunk_size_bytes=128 * 1024,
                max_chunks_per_transfer=3,
                retry_backoff_s=0.05,
            )
        )
        async with aiohttp.ClientSession() as session:
            res = await engine.upload(
                session=session,
                transfer_id="ul-retry-1",
                file_path=src,
                server_base_url=server.base_url,
                rate_limiter=AsyncRateLimiter(20 * 1024 * 1024),
            )
        assert res.total_bytes == len(raw)
        assert res.total_chunks == 5
        assert res.retries >= 1
        assert res.sha256 == expected_sha
        final_file = Path(res.dest_path)
        assert final_file.is_file()
        assert compute_sha256(final_file) == expected_sha
        # Staging folder cleaned up after atomic commit
        assert not (tmp_path / "srv_uploads" / ".staging" / res.session_id).exists()
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_resumable_upload_skips_already_received_chunks(tmp_path) -> None:
    src = tmp_path / "resume_me.bin"
    raw = b"A" * (128 * 1024) + b"B" * (128 * 1024) + b"C" * (64 * 1024)
    src.write_bytes(raw)
    expected_sha = compute_sha256(src)

    server = LocalFileServer(upload_dir=tmp_path / "srv_uploads")
    # Pre-stage chunk 0 under the same transfer_id
    init = server.upload_store.init_session(
        filename="resume_me.bin",
        total_bytes=len(raw),
        chunk_size=128 * 1024,
        transfer_id="ul-resume-42",
        expected_sha256=expected_sha,
    )
    server.upload_store.store_chunk(
        session_id=init["session_id"],
        transfer_id="ul-resume-42",
        index=0,
        payload=raw[: 128 * 1024],
    )

    await server.start()
    try:
        engine = HTTPUploadEngine(TransferConfig(chunk_size_bytes=128 * 1024, max_chunks_per_transfer=2))
        async with aiohttp.ClientSession() as session:
            res = await engine.upload(
                session=session,
                transfer_id="ul-resume-42",
                file_path=src,
                server_base_url=server.base_url,
                rate_limiter=AsyncRateLimiter(20 * 1024 * 1024),
            )
        assert res.resumed_chunks == 1
        assert res.sha256 == expected_sha
        assert Path(res.dest_path).read_bytes() == raw
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_mixed_bidirectional_uploads_downloads_and_pause_resume(tmp_path) -> None:
    src_up1 = tmp_path / "up_small.bin"
    src_up2 = tmp_path / "up_large.bin"
    src_up1.write_bytes(b"U1" * (128 * 1024))
    src_up2.write_bytes(b"U2" * (384 * 1024))

    server = LocalFileServer(upload_dir=tmp_path / "srv_uploads")
    dl1 = server.add_deterministic_file("dl_small.bin", 256 * 1024, seed_byte=7)
    dl2 = server.add_deterministic_file("dl_large.bin", 768 * 1024, seed_byte=9)
    await server.start()

    cfg = FlowWeaveConfig(
        db_path=tmp_path / "bi.db",
        control_interval_s=0.05,
        scheduler=SchedulerConfig(policy=PolicyName.FLOWWEAVE),
        transfer=TransferConfig(
            download_dir=tmp_path / "dl",
            upload_dir=tmp_path / "srv_uploads",
            chunk_size_bytes=128 * 1024,
            max_chunks_per_transfer=2,
            total_link_capacity_bps=2 * 1024 * 1024,
            upload_link_capacity_bps=2 * 1024 * 1024,
        ),
    )
    coord = FlowWeaveCoordinator(cfg)
    await coord.start()
    try:
        t_dl1 = await coord.transfer_manager.submit_download(
            server.url_for("dl_small.bin"), filename="dl_small.bin", expected_sha256=dl1.sha256
        )
        t_dl2 = await coord.transfer_manager.submit_download(
            server.url_for("dl_large.bin"), filename="dl_large.bin", expected_sha256=dl2.sha256
        )
        t_ul1 = await coord.transfer_manager.submit_upload(src_up1, server.base_url)
        t_ul2 = await coord.transfer_manager.submit_upload(src_up2, server.base_url)

        # Pause and resume one active download mid-flight
        await asyncio.sleep(0.05)
        if coord.transfer_manager.get_snapshot(t_dl2.transfer_id).state == TransferState.ACTIVE:  # type: ignore[union-attr]
            assert await coord.transfer_manager.pause(t_dl2.transfer_id) is True
            assert coord.transfer_manager.get_snapshot(t_dl2.transfer_id).state == TransferState.PAUSED  # type: ignore[union-attr]
            await asyncio.sleep(0.08)
            assert await coord.transfer_manager.resume(t_dl2.transfer_id) is True

        await coord.transfer_manager.wait_all(timeout=20.0)

        snaps = {s.transfer_id: s for s in coord.transfer_manager.list_snapshots()}
        for tid in (t_dl1.transfer_id, t_dl2.transfer_id, t_ul1.transfer_id, t_ul2.transfer_id):
            assert snaps[tid].state == TransferState.COMPLETED

        assert snaps[t_dl1.transfer_id].direction == TransferDirection.DOWNLOAD
        assert snaps[t_ul1.transfer_id].direction == TransferDirection.UPLOAD
        assert snaps[t_ul1.transfer_id].sha256 == compute_sha256(src_up1)
        assert snaps[t_ul2.transfer_id].sha256 == compute_sha256(src_up2)
        assert snaps[t_dl1.transfer_id].sha256 == dl1.sha256
        assert snaps[t_dl2.transfer_id].sha256 == dl2.sha256

        status = coord.get_monitor_status()
        assert status["is_simulated"] is False
        assert len(status["links"]) == 2
        assert len(status["decision_log"]) >= 1
    finally:
        await coord.stop()
        await server.stop()
