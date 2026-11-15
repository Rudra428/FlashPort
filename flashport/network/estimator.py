from __future__ import annotations

from collections import deque
from statistics import median


class NetworkEstimator:
    """Estimates HTTP request latency from client-reported measurements."""

    def __init__(self, window: int = 100) -> None:
        if window <= 0:
            raise ValueError("window must be positive")
        self._latencies_ms: deque[float] = deque(maxlen=window)

    def record_request_latency(self, latency_ms: float) -> None:
        if latency_ms < 0:
            raise ValueError("latency must be non-negative")
        self._latencies_ms.append(float(latency_ms))

    def estimate_rtt(self, baseline: float = 0.0) -> float:
        """Return median HTTP request latency (not a transport-layer RTT measurement)."""
        if not self._latencies_ms:
            return float(baseline)
        return float(median(self._latencies_ms))

    def snapshot(self) -> dict[str, float | int | None]:
        return {
            "http_request_latency_median_ms": self.estimate_rtt() if self._latencies_ms else None,
            "sample_count": len(self._latencies_ms),
        }
