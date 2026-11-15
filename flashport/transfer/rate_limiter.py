from __future__ import annotations

import asyncio
import math
import time


class RateLimiter:
    """A transfer-scoped token bucket shared by all concurrent range responses."""

    def __init__(self, capacity: float, burst_bytes: int = 16_384) -> None:
        if not math.isfinite(capacity) or capacity < 0:
            raise ValueError("capacity must be finite and non-negative")
        if burst_bytes <= 0:
            raise ValueError("burst_bytes must be positive")
        self.capacity = float(capacity)
        self.burst_bytes = burst_bytes
        self._rate = 0.0
        self._tokens = 0.0
        self._updated_at = time.monotonic()
        self._lock = asyncio.Lock()
        self._changed = asyncio.Event()
        self._closed = False

    @property
    def rate(self) -> float:
        return self._rate

    async def set_rate(self, rate: float) -> None:
        if not math.isfinite(rate) or rate < 0:
            raise ValueError("rate must be finite and non-negative")
        async with self._lock:
            if self._closed:
                return
            self._refill(time.monotonic())
            self._rate = min(float(rate), self.capacity)
            self._changed.set()

    async def close(self) -> None:
        async with self._lock:
            self._closed = True
            self._changed.set()

    async def consume(self, byte_count: int) -> None:
        if byte_count < 0:
            raise ValueError("byte_count must be non-negative")
        if byte_count == 0:
            return
        if byte_count > self.burst_bytes:
            raise ValueError("byte_count exceeds token bucket burst size")

        while True:
            delay: float | None = None
            async with self._lock:
                if self._closed:
                    raise asyncio.CancelledError
                self._refill(time.monotonic())
                if self._rate > 0 and self._tokens >= byte_count:
                    self._tokens -= byte_count
                    return
                if self._rate > 0:
                    delay = (byte_count - self._tokens) / self._rate
                self._changed.clear()

            try:
                if delay is None:
                    await self._changed.wait()
                else:
                    await asyncio.wait_for(self._changed.wait(), timeout=delay)
            except TimeoutError:
                pass

    def _refill(self, now: float) -> None:
        elapsed = max(0.0, now - self._updated_at)
        self._tokens = min(self.burst_bytes, self._tokens + elapsed * self._rate)
        self._updated_at = now
