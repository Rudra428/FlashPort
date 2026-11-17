"""Network monitor collecting measured rolling-window statistics and derived estimates."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from flowweave.config import MonitorConfig
from flowweave.models import EstimatedMetrics, LinkState, MeasuredMetrics, TransferDirection


@dataclass
class _TransferTrack:
    started_at: float
    total_bytes: int
    direction: TransferDirection = TransferDirection.DOWNLOAD
    measured: MeasuredMetrics = field(default_factory=MeasuredMetrics)
    estimated: EstimatedMetrics = field(default_factory=EstimatedMetrics)
    recent_goodputs: deque[float] = field(default_factory=lambda: deque(maxlen=6))


class NetworkMonitor:
    """Tracks per-transfer and per-link metrics, separating measurements from estimates."""

    def __init__(self, config: MonitorConfig | None = None) -> None:
        self.config = config or MonitorConfig()
        self._transfers: dict[str, _TransferTrack] = {}
        self._links: dict[str, LinkState] = {}

    def register_link(self, link: LinkState) -> None:
        self._links[link.link_id] = link

    def update_link(
        self,
        link_id: str,
        *,
        capacity_bps: float | None = None,
        latency_ms: float | None = None,
        loss_rate: float | None = None,
        measured_throughput_bps: float | None = None,
        active_transfers: int | None = None,
        requested_bps: float | None = None,
    ) -> LinkState:
        link = self._links.get(link_id)
        if link is None:
            link = LinkState(link_id=link_id, capacity_bps=capacity_bps or 1_000_000.0)
            self._links[link_id] = link

        if capacity_bps is not None:
            link.capacity_bps = capacity_bps
        if latency_ms is not None:
            link.latency_ms = latency_ms
        if loss_rate is not None:
            link.loss_rate = loss_rate
        if measured_throughput_bps is not None:
            link.measured_throughput_bps = measured_throughput_bps
        if active_transfers is not None:
            link.active_transfers = active_transfers

        if requested_bps is not None and link.capacity_bps > 0:
            link.congestion_ratio = requested_bps / link.capacity_bps
        elif link.capacity_bps > 0:
            link.congestion_ratio = link.measured_throughput_bps / link.capacity_bps

        effective_cap = link.capacity_bps * max(0.05, 1.0 - link.loss_rate)
        link.estimated_available_bps = max(0.0, effective_cap - link.measured_throughput_bps)
        return link

    def register_transfer(
        self,
        transfer_id: str,
        total_bytes: int,
        now: float,
        direction: TransferDirection = TransferDirection.DOWNLOAD,
    ) -> None:
        self._transfers[transfer_id] = _TransferTrack(
            started_at=now,
            total_bytes=total_bytes,
            direction=direction,
            recent_goodputs=deque(maxlen=max(1, self.config.rolling_window_samples)),
        )

    def record_progress(
        self,
        transfer_id: str,
        *,
        now: float,
        dt: float,
        useful_bytes: int,
        raw_bytes: int | None = None,
        redundant_bytes: int = 0,
        remaining_bytes: int,
        rtt_ms: float | None = None,
        retries_delta: int = 0,
        link_capacity_bps: float = 0.0,
    ) -> tuple[MeasuredMetrics, EstimatedMetrics]:
        """Update directly measured rolling-window counters and refresh smoothed estimates."""
        track = self._transfers.get(transfer_id)
        if track is None:
            track = _TransferTrack(
                started_at=now,
                total_bytes=max(remaining_bytes + useful_bytes, 1),
                recent_goodputs=deque(maxlen=max(1, self.config.rolling_window_samples)),
            )
            self._transfers[transfer_id] = track

        m = track.measured
        e = track.estimated
        raw = useful_bytes if raw_bytes is None else raw_bytes

        # 1. Directly measured values (instant, rolling window, cumulative average)
        m.bytes_acked += useful_bytes
        m.raw_bytes_received += raw
        m.redundant_bytes += redundant_bytes
        m.retries += retries_delta
        m.sample_count += 1
        m.last_updated = now

        if dt > 0:
            m.instant_goodput_bps = useful_bytes / dt
            track.recent_goodputs.append(m.instant_goodput_bps)
            m.rolling_goodput_bps = sum(track.recent_goodputs) / len(track.recent_goodputs)
        elapsed = max(now - track.started_at, dt, 1e-6)
        m.avg_goodput_bps = m.bytes_acked / elapsed
        if rtt_ms is not None:
            m.last_rtt_ms = rtt_ms

        # 2. Derived estimates (EWMA smoothed)
        alpha = self.config.ewma_alpha
        if m.sample_count == 1 or e.ewma_goodput_bps <= 0.0:
            e.ewma_goodput_bps = m.instant_goodput_bps
        else:
            e.ewma_goodput_bps = alpha * m.instant_goodput_bps + (1.0 - alpha) * e.ewma_goodput_bps

        if rtt_ms is not None:
            r_alpha = self.config.rtt_alpha
            if e.ewma_rtt_ms <= 0.0:
                e.ewma_rtt_ms = rtt_ms
            else:
                e.ewma_rtt_ms = r_alpha * rtt_ms + (1.0 - r_alpha) * e.ewma_rtt_ms

        if link_capacity_bps > 0:
            e.estimated_bottleneck_bps = link_capacity_bps

        e.confidence = min(1.0, m.sample_count / max(1, self.config.min_samples_for_confidence))

        # Blend rolling/EWMA goodput with bottleneck prior during cold start
        rate_est = e.ewma_goodput_bps
        if rate_est <= 1.0 and link_capacity_bps > 0:
            rate_est = link_capacity_bps
        elif e.confidence < 1.0 and link_capacity_bps > 0:
            rate_est = e.confidence * rate_est + (1.0 - e.confidence) * min(
                link_capacity_bps, max(rate_est, 1.0)
            )

        if remaining_bytes <= 0:
            e.predicted_remaining_time_s = 0.0
        elif rate_est > 0:
            e.predicted_remaining_time_s = remaining_bytes / rate_est
        else:
            e.predicted_remaining_time_s = float("inf")

        return m, e

    def get_transfer_metrics(self, transfer_id: str) -> tuple[MeasuredMetrics, EstimatedMetrics]:
        track = self._transfers.get(transfer_id)
        if track is None:
            return MeasuredMetrics(), EstimatedMetrics()
        return track.measured, track.estimated

    def get_links(self) -> dict[str, LinkState]:
        return dict(self._links)

    def remove_transfer(self, transfer_id: str) -> None:
        self._transfers.pop(transfer_id, None)
