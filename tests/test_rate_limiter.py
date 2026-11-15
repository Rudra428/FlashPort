import asyncio
import time

import pytest

from flashport.transfer.rate_limiter import RateLimiter
from flashport.transfer.worker import ChunkWorker, RetryableChunkError


@pytest.mark.asyncio
async def test_concurrent_chunks_share_one_transfer_token_bucket() -> None:
    limiter = RateLimiter(capacity=100.0, burst_bytes=10)
    await limiter.set_rate(100.0)
    started = time.monotonic()
    await asyncio.gather(limiter.consume(10), limiter.consume(10))
    elapsed = time.monotonic() - started

    assert elapsed >= 0.17
    assert limiter.rate == 100.0


@pytest.mark.asyncio
async def test_zero_rate_wait_is_released_when_rate_changes() -> None:
    limiter = RateLimiter(capacity=100.0, burst_bytes=10)
    waiter = asyncio.create_task(limiter.consume(1))
    await asyncio.sleep(0.02)
    await limiter.set_rate(100.0)
    await asyncio.wait_for(waiter, timeout=1.0)


@pytest.mark.asyncio
async def test_closing_limiter_cancels_waiting_consumers() -> None:
    limiter = RateLimiter(capacity=100.0, burst_bytes=10)
    waiter = asyncio.create_task(limiter.consume(1))
    await asyncio.sleep(0.01)
    await limiter.close()

    with pytest.raises(asyncio.CancelledError):
        await waiter


@pytest.mark.asyncio
async def test_chunk_worker_retries_transient_failures() -> None:
    worker = ChunkWorker("transfer", index=0, start=0, end=9, retries=2)
    attempts = 0

    async def operation() -> bytes:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RetryableChunkError("temporary server failure")
        return b"complete!"

    assert await worker.run(operation) == b"complete!"
    assert attempts == 3
