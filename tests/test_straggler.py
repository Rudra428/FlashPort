"""Tests for straggler detection and cost-benefit speculative replication."""

from __future__ import annotations

from flowweave.config import StragglerConfig
from flowweave.models import LinkState
from flowweave.straggler import ActiveChunkView, StragglerDetector


def test_replicates_straggler_when_idle_alternate_link_exists() -> None:
    detector = StragglerDetector(StragglerConfig(min_chunk_age_s=0.5, min_net_saving_s=0.2))
    links = {
        "mirror-a": LinkState("mirror-a", capacity_bps=4_000_000.0, latency_ms=20.0, active_transfers=2),
        "mirror-b": LinkState("mirror-b", capacity_bps=4_000_000.0, latency_ms=25.0, active_transfers=0),
    }
    chunks = [
        ActiveChunkView("c0", "t1", "mirror-a", ["mirror-a", "mirror-b"], 1_000_000, 900_000, 0.0, 120_000.0),
        ActiveChunkView("c1", "t1", "mirror-a", ["mirror-a", "mirror-b"], 1_000_000, 200_000, 0.0, 1_800_000.0),
    ]
    decisions = detector.evaluate(now=1.0, active_chunks=chunks, links=links, total_useful_bytes=20_000_000)
    assert len(decisions) == 1
    assert decisions[0].chunk_id == "c0"
    assert decisions[0].should_replicate is True
    assert decisions[0].replica_link == "mirror-b"
    assert decisions[0].expected_saving_s > decisions[0].expected_cost_s


def test_rejects_replication_when_alternate_link_heavily_congested() -> None:
    detector = StragglerDetector(StragglerConfig(min_chunk_age_s=0.5, min_net_saving_s=0.3))
    links = {
        "mirror-a": LinkState("mirror-a", capacity_bps=4_000_000.0, latency_ms=20.0, active_transfers=1),
        "mirror-b": LinkState(
            "mirror-b",
            capacity_bps=1_000_000.0,
            latency_ms=80.0,
            measured_throughput_bps=980_000.0,
            active_transfers=12,
        ),
    }
    chunks = [
        ActiveChunkView("c0", "t1", "mirror-a", ["mirror-a", "mirror-b"], 1_000_000, 800_000, 0.0, 200_000.0),
        ActiveChunkView("c1", "t1", "mirror-a", ["mirror-a", "mirror-b"], 1_000_000, 200_000, 0.0, 1_800_000.0),
    ]
    decisions = detector.evaluate(now=1.0, active_chunks=chunks, links=links, total_useful_bytes=20_000_000)
    assert len(decisions) == 1
    assert decisions[0].should_replicate is False
    assert decisions[0].reason == "cost_exceeds_benefit"


def test_respects_redundant_byte_budget() -> None:
    detector = StragglerDetector(StragglerConfig(max_redundant_ratio=0.05))
    links = {
        "mirror-a": LinkState("mirror-a", capacity_bps=4_000_000.0, active_transfers=1),
        "mirror-b": LinkState("mirror-b", capacity_bps=4_000_000.0, active_transfers=0),
    }
    chunks = [
        ActiveChunkView("c0", "t1", "mirror-a", ["mirror-a", "mirror-b"], 1_000_000, 900_000, 0.0, 50_000.0),
        ActiveChunkView("c1", "t1", "mirror-a", ["mirror-a", "mirror-b"], 1_000_000, 100_000, 0.0, 2_000_000.0),
    ]
    decisions = detector.evaluate(
        now=1.0,
        active_chunks=chunks,
        links=links,
        total_useful_bytes=5_000_000,
        total_redundant_bytes=400_000,  # already at 8% > 5% cap
    )
    assert len(decisions) == 1
    assert decisions[0].should_replicate is False
    assert decisions[0].reason == "redundant_budget_exhausted"
