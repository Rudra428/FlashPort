from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

import aiohttp

T = TypeVar("T")


class TransferProtocolError(RuntimeError):
    pass


class RetryableChunkError(TransferProtocolError):
    pass


class ChunkWorker:
    """Runs one range operation with bounded retries and exponential backoff."""

    def __init__(
        self,
        transfer_id: str,
        index: int,
        start: int,
        end: int,
        retries: int = 3,
    ) -> None:
        if not transfer_id or index < 0 or start < 0 or end < start or retries < 0:
            raise ValueError("invalid chunk worker parameters")
        self.transfer_id = transfer_id
        self.index = index
        self.start = start
        self.end = end
        self.retries = retries

    async def run(self, operation: Callable[[], Awaitable[T]]) -> T:
        for attempt in range(self.retries + 1):
            try:
                return await operation()
            except (RetryableChunkError, aiohttp.ClientError, asyncio.TimeoutError):
                if attempt >= self.retries:
                    raise
                await asyncio.sleep(min(0.25 * (2**attempt), 3.0))
        raise RuntimeError("chunk retry loop exited unexpectedly")
