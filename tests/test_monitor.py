"""Tests for NetworkMonitor (measured vs estimated separation and link tracking)."""

from __future__ import annotations

import pytest

from flowweave.models import LinkState
from flowweave.monitor import NetworkMonitor


def test_measured_vs_estimated_metrics() -> None:
    mon = NetworkMonitor()
    mon.register_transfer("t1", total_bytes=1_000_000, now=0.0)

    # First tick: 100 KB in 0.5s -> 200 KB/s measured
    m1, e1 = mon.record_progress(
        "t1",
        now=0.5,
        dt=0.5,
        useful_bytes=100_000,
        remaining_bytes=900_000,
        rtt_ms=20.0,
        link_capacity_bps=1_000_000.0,
    )
    assert m1.instant_goodput_bps == pytest.approx(200_000.0)
    assert m1.avg_goodput_bps == pytest.approx(200_000.0)
    assert m1.bytes_acked == 100_000
    assert e1.confidence < 1.0  # cold start

    # Second tick drops sharply: 20 KB in 0.5s -> 40 KB/s instant, but EWMA is smoothed
    m2, e2 = mon.record_progress(
        "t1",
        now=1.0,
        dt=0.5,
        useful_bytes=20_000,
        remaining_bytes=880_000,
        rtt_ms=60.0,
        link_capacity_bps=1_000_000.0,
    )
    assert m2.instant_goodput_bps == pytest.approx(40_000.0)
    assert m2.avg_goodput_bps == pytest.approx(120_000.0)
    assert 40_000.0 < e2.ewma_goodput_bps < 200_000.0
    assert m2.last_rtt_ms == pytest.approx(60.0)
    assert 20.0 < e2.ewma_rtt_ms < 60.0


def test_link_congestion_and_utilization() -> None:
    mon = NetworkMonitor()
    mon.register_link(LinkState("wan", capacity_bps=1_000_000.0, latency_ms=20.0, loss_rate=0.0))

    link = mon.update_link("wan", measured_throughput_bps=500_000.0, requested_bps=500_000.0, active_transfers=2)
    assert link.utilization == pytest.approx(0.5)
    assert not link.is_congested

    link = mon.update_link("wan", measured_throughput_bps=1_000_000.0, requested_bps=2_500_000.0, active_transfers=4)
    assert link.utilization == pytest.approx(1.0)
    assert link.is_congested
