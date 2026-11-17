"""End-to-end tests for real HTTP Range downloads, fallback, integrity checks, and coordinator."""

from __future__ import annotations

import aiohttp
import pytest

from flowweave.config import FlowWeaveConfig, SchedulerConfig, TransferConfig
from flowweave.coordinator import FlowWeaveCoordinator
from flowweave.models import PolicyName, TransferState
from flowweave.transfer import AsyncRateLimiter, HTTPDownloadEngine, LocalFileServer, compute_sha256


@pytest.mark.asyncio
async def test_ranged_parallel_download_and_sha256(tmp_path) -> None:
    server = LocalFileServer()
    sf = server.add_deterministic_file("bundle.bin", 600 * 1024, seed_byte=17)
    await server.start()
    try:
        engine = HTTPDownloadEngine(
            TransferConfig(
                download_dir=tmp_path,
                chunk_size_bytes=128 * 1024,
                max_chunks_per_transfer=4,
            )
        )
        limiter = AsyncRateLimiter(20 * 1024 * 1024)
        dest = tmp_path / "bundle.bin"
        async with aiohttp.ClientSession() as session:
            res = await engine.download(
                session=session,
                transfer_id="tx-range",
                url=server.url_for("bundle.bin", allow_ranges=True),
                dest_path=dest,
                rate_limiter=limiter,
                expected_sha256=sf.sha256,
            )
        assert res.used_ranges is True
        assert res.total_chunks == 5
        assert res.total_bytes == len(sf.data)
        assert res.sha256 == sf.sha256
        assert compute_sha256(dest) == sf.sha256
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_fallback_when_ranges_unsupported(tmp_path) -> None:
    server = LocalFileServer()
    sf = server.add_deterministic_file("legacy.bin", 300 * 1024, seed_byte=99)
    await server.start()
    try:
        engine = HTTPDownloadEngine(
            TransferConfig(download_dir=tmp_path, chunk_size_bytes=64 * 1024)
        )
        limiter = AsyncRateLimiter(20 * 1024 * 1024)
        dest = tmp_path / "legacy.bin"
        async with aiohttp.ClientSession() as session:
            res = await engine.download(
                session=session,
                transfer_id="tx-fallback",
                url=server.url_for("legacy.bin", allow_ranges=False),
                dest_path=dest,
                rate_limiter=limiter,
                expected_sha256=sf.sha256,
            )
        assert res.used_ranges is False
        assert res.total_chunks == 1
        assert res.sha256 == sf.sha256
        assert dest.read_bytes() == sf.data
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_integrity_verification_rejects_bad_sha256(tmp_path) -> None:
    server = LocalFileServer()
    server.add_deterministic_file("corrupt.bin", 128 * 1024, seed_byte=5)
    await server.start()
    try:
        engine = HTTPDownloadEngine(TransferConfig(download_dir=tmp_path, chunk_size_bytes=64 * 1024))
        dest = tmp_path / "corrupt.bin"
        async with aiohttp.ClientSession() as session:
            with pytest.raises(ValueError, match="SHA-256 mismatch"):
                await engine.download(
                    session=session,
                    transfer_id="tx-bad",
                    url=server.url_for("corrupt.bin"),
                    dest_path=dest,
                    rate_limiter=AsyncRateLimiter(20 * 1024 * 1024),
                    expected_sha256="0" * 64,
                )
        assert not dest.exists()
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_coordinator_runs_concurrent_downloads_and_switches_policy(tmp_path) -> None:
    server = LocalFileServer()
    f_small = server.add_deterministic_file("small.bin", 192 * 1024, seed_byte=11)
    f_large = server.add_deterministic_file("large.bin", 768 * 1024, seed_byte=22)
    await server.start()

    cfg = FlowWeaveConfig(
        db_path=tmp_path / "coord.db",
        control_interval_s=0.05,
        scheduler=SchedulerConfig(policy=PolicyName.FLOWWEAVE),
        transfer=TransferConfig(
            download_dir=tmp_path / "dl",
            chunk_size_bytes=128 * 1024,
            max_chunks_per_transfer=3,
            total_link_capacity_bps=4 * 1024 * 1024,
        ),
    )
    coord = FlowWeaveCoordinator(cfg)
    await coord.start()
    try:
        t1 = await coord.transfer_manager.submit(
            server.url_for("small.bin"),
            filename="small.bin",
            expected_size_bytes=len(f_small.data),
            expected_sha256=f_small.sha256,
        )
        t2 = await coord.transfer_manager.submit(
            server.url_for("large.bin"),
            filename="large.bin",
            expected_size_bytes=len(f_large.data),
            expected_sha256=f_large.sha256,
        )

        # Switch policy live mid-flight to verify hot reconfiguration works cleanly.
        coord.set_policy(PolicyName.MAX_MIN)
        assert coord.scheduler.name == PolicyName.MAX_MIN
        coord.set_policy(PolicyName.FLOWWEAVE)

        await coord.transfer_manager.wait_all(timeout=15.0)
        s1 = coord.transfer_manager.get_snapshot(t1.transfer_id)
        s2 = coord.transfer_manager.get_snapshot(t2.transfer_id)
        assert s1 is not None and s1.state == TransferState.COMPLETED
        assert s2 is not None and s2.state == TransferState.COMPLETED
        assert s1.sha256 == f_small.sha256
        assert s2.sha256 == f_large.sha256

        # SQLite persisted both transfers.
        stored = coord.storage.list_transfers()
        assert len(stored) == 2
        status = coord.get_monitor_status()
        assert status["policy"] == "flowweave"
        assert len(status["links"]) >= 1
    finally:
        await coord.stop()
        await server.stop()
