"""Dynamic async token-bucket rate limiter controlled by the FlowWeave scheduler."""

from __future__ import annotations

import asyncio
import time


class AsyncRateLimiter:
    """Paces async byte streams according to a live rate_bps that can change every tick."""

    def __init__(self, rate_bps: float, burst_seconds: float = 0.25) -> None:
        self._rate_bps = max(1024.0, rate_bps)
        self._burst_seconds = burst_seconds
        self._tokens = self._rate_bps * burst_seconds
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    @property
    def rate_bps(self) -> float:
        return self._rate_bps

    def set_rate(self, rate_bps: float) -> None:
        """Update target bytes/sec rate dynamically without cancelling in-flight requests."""
        self._rate_bps = max(1024.0, rate_bps)
        max_tokens = self._rate_bps * self._burst_seconds
        if self._tokens > max_tokens:
            self._tokens = max_tokens

    async def acquire(self, num_bytes: int) -> None:
        """Wait until `num_bytes` tokens are available under the current rate limit."""
        if num_bytes <= 0:
            return
        remaining = float(num_bytes)
        while remaining > 0:
            async with self._lock:
                now = time.monotonic()
                elapsed = max(0.0, now - self._last_refill)
                self._last_refill = now
                max_tokens = max(float(num_bytes), self._rate_bps * self._burst_seconds)
                self._tokens = min(max_tokens, self._tokens + elapsed * self._rate_bps)

                if self._tokens >= remaining:
                    self._tokens -= remaining
                    return

                if self._tokens > 0:
                    remaining -= self._tokens
                    self._tokens = 0.0

                # Sleep in short bounded intervals so scheduler rate increases take effect quickly.
                wait_s = min(0.15, remaining / max(self._rate_bps, 1024.0))
            await asyncio.sleep(max(0.005, wait_s))
