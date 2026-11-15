from __future__ import annotations

from collections import deque
from statistics import median
from time import monotonic, time

from flashport.metrics.fairness import jain_fairness_index


class NetworkMonitor:
    """Stores actual server/client-reported transfer measurements, not simulations."""

    def __init__(self, configured_capacity_bps: float) -> None:
        self.configured_capacity_bps = float(configured_capacity_bps)
        self.bytes_sent = 0
        self._server_bytes_by_transfer: dict[str, int] = {}
        self._unique_served_by_transfer: dict[str, int] = {}
        self.client_downloaded_bytes = 0
        self.connection_failures = 0
        self._progress: dict[str, int] = {}
        self._goodput: dict[str, float] = {}
        self._completed: list[dict[str, float | int]] = []
        self._latencies: deque[float] = deque(maxlen=500)
        self._samples: deque[dict[str, float]] = deque(maxlen=500)
        self._last_sample_at = monotonic()

    def record_server_bytes(self, transfer_id: str, byte_count: int) -> None:
        value = max(0, byte_count)
        self.bytes_sent += value
        self._server_bytes_by_transfer[transfer_id] = (
            self._server_bytes_by_transfer.get(transfer_id, 0) + value
        )

    def record_unique_served(self, transfer_id: str, unique_bytes: int) -> None:
        self._unique_served_by_transfer[transfer_id] = max(0, unique_bytes)

    def record_client_progress(
        self,
        transfer_id: str,
        downloaded_bytes: int,
        goodput_bps: float | None = None,
        request_latency_ms: float | None = None,
        active: bool = True,
    ) -> None:
        previous = self._progress.get(transfer_id, 0)
        current = max(previous, downloaded_bytes)
        self._progress[transfer_id] = current
        self.client_downloaded_bytes += current - previous
        if goodput_bps is not None and goodput_bps >= 0:
            if active:
                self._goodput[transfer_id] = goodput_bps
            else:
                self._goodput.pop(transfer_id, None)
        if request_latency_ms is not None and request_latency_ms >= 0:
            self._latencies.append(request_latency_ms)
        now = time()
        monotonic_now = monotonic()
        elapsed = max(monotonic_now - self._last_sample_at, 1e-6)
        delta = current - previous
        self._samples.append(
            {
                "timestamp": now,
                "goodput_bps": delta / elapsed,
                "configured_capacity_bps": self.configured_capacity_bps,
            }
        )
        self._last_sample_at = monotonic_now

    def record_connection_failure(self) -> None:
        self.connection_failures += 1

    def record_completion(
        self,
        size: int,
        start_time: float,
        completion_time: float,
    ) -> None:
        duration = max(0.0, completion_time - start_time)
        if size <= 0:
            return
        self._completed.append(
            {
                "size": size,
                "start_time": start_time,
                "completion_time": completion_time,
                "flow_completion_time": duration,
                "goodput_bps": size / duration if duration > 0 else 0.0,
            }
        )

    @staticmethod
    def _p95(values: list[float]) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        rank = 0.95 * (len(ordered) - 1)
        lower = int(rank)
        upper = min(lower + 1, len(ordered) - 1)
        return ordered[lower] + (ordered[upper] - ordered[lower]) * (rank - lower)

    def snapshot(self) -> dict[str, object]:
        flow_times = [float(item["flow_completion_time"]) for item in self._completed]
        completed_bytes = sum(int(item["size"]) for item in self._completed)
        makespan = (
            max(float(item["completion_time"]) for item in self._completed)
            - min(float(item["start_time"]) for item in self._completed)
            if self._completed
            else 0.0
        )
        completed_rates = [float(item["goodput_bps"]) for item in self._completed]
        completed_goodput = completed_bytes / makespan if makespan > 0 else 0.0
        active_goodput = sum(self._goodput.values())
        return {
            "measurement_type": "actual_application",
            "configured_capacity_bps": self.configured_capacity_bps,
            "bytes_sent_by_server": self.bytes_sent,
            "redundant_or_unconfirmed_range_bytes": max(
                0,
                sum(self._server_bytes_by_transfer.values())
                - sum(self._unique_served_by_transfer.values()),
            ),
            "client_reported_downloaded_bytes": self.client_downloaded_bytes,
            "active_client_goodput_bps": active_goodput,
            "completed_aggregate_goodput_bps": completed_goodput,
            "completed_transfer_count": len(self._completed),
            "mean_flow_completion_time_s": (
                sum(flow_times) / len(flow_times) if flow_times else None
            ),
            "median_flow_completion_time_s": median(flow_times) if flow_times else None,
            "p95_flow_completion_time_s": self._p95(flow_times),
            "makespan_s": makespan,
            "jain_fairness_index": jain_fairness_index(completed_rates),
            "measured_capacity_utilization": (
                active_goodput / self.configured_capacity_bps
                if self.configured_capacity_bps > 0
                else 0.0
            ),
            "active_client_goodput_bps": dict(self._goodput),
            "http_request_latency_ms": list(self._latencies),
            "connection_failures": self.connection_failures,
            "packet_loss_available": False,
            "tcp_retransmissions_available": False,
            "transport_rtt_available": False,
            "samples": list(self._samples),
        }
