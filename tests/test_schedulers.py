"""Tests for all four scheduling policies and capacity invariants."""

from __future__ import annotations

import pytest

from flowweave.config import SchedulerConfig
from flowweave.models import LinkState, PolicyName, SchedulerInput, TransferSnapshot, TransferState
from flowweave.scheduler import create_scheduler


def _make_transfer(
    tid: str,
    remaining_bytes: int,
    links: list[str],
    created_at: float = 0.0,
) -> TransferSnapshot:
    return TransferSnapshot(
        transfer_id=tid,
        filename=f"{tid}.bin",
        url=f"http://example/{tid}.bin",
        total_bytes=remaining_bytes,
        remaining_bytes=remaining_bytes,
        state=TransferState.ACTIVE,
        candidate_links=links,
        assigned_link=links[0],
        created_at=created_at,
        started_at=created_at,
    )


def test_max_min_fair_equal_shares() -> None:
    links = {"wan": LinkState("wan", capacity_bps=10_000_000.0)}
    transfers = [
        _make_transfer("small", 500_000, ["wan"]),
        _make_transfer("medium", 5_000_000, ["wan"]),
        _make_transfer("large", 25_000_000, ["wan"]),
    ]
    dec = create_scheduler(PolicyName.MAX_MIN).decide(SchedulerInput(now=1.0, transfers=transfers, links=links))
    assert sum(dec.allocations_bps.values()) == pytest.approx(10_000_000.0)
    for tid in ("small", "medium", "large"):
        assert dec.allocations_bps[tid] == pytest.approx(10_000_000.0 / 3.0)


def test_srpt_prioritizes_shortest_remaining() -> None:
    links = {"wan": LinkState("wan", capacity_bps=10_000_000.0)}
    transfers = [
        _make_transfer("large", 30_000_000, ["wan"]),
        _make_transfer("small", 2_000_000, ["wan"]),
    ]
    dec = create_scheduler(PolicyName.SRPT, horizon_s=0.25).decide(
        SchedulerInput(now=1.0, transfers=transfers, links=links)
    )
    assert dec.allocations_bps["small"] > dec.allocations_bps["large"]
    assert sum(dec.allocations_bps.values()) <= 10_000_000.0 + 1e-3


def test_flowweave_prioritizes_short_while_protecting_large_from_starvation() -> None:
    links = {"wan": LinkState("wan", capacity_bps=10_000_000.0)}
    transfers = [
        _make_transfer("small", 1_000_000, ["wan"]),
        _make_transfer("medium", 6_000_000, ["wan"]),
        _make_transfer("large", 30_000_000, ["wan"]),
    ]
    cfg = SchedulerConfig(min_fair_share_fraction=0.15)
    dec = create_scheduler(PolicyName.FLOWWEAVE, config=cfg, horizon_s=0.25).decide(
        SchedulerInput(now=1.0, transfers=transfers, links=links)
    )
    assert dec.allocations_bps["small"] > dec.allocations_bps["medium"] > dec.allocations_bps["large"]
    # Fairness floor prevents complete starvation of the largest transfer.
    assert dec.allocations_bps["large"] >= (10_000_000.0 * 0.15 / 3.0) - 1.0
    assert sum(dec.allocations_bps.values()) <= 10_000_000.0 + 1e-3


def test_flowweave_offloads_across_multiple_links() -> None:
    links = {
        "primary": LinkState("primary", capacity_bps=8_000_000.0, latency_ms=20.0),
        "secondary": LinkState("secondary", capacity_bps=6_000_000.0, latency_ms=25.0),
    }
    transfers = [
        _make_transfer(f"t{i}", (i + 1) * 2_000_000, ["primary", "secondary"])
        for i in range(6)
    ]
    dec = create_scheduler(PolicyName.FLOWWEAVE).decide(SchedulerInput(now=0.5, transfers=transfers, links=links))
    used_links = set(dec.link_assignments.values())
    assert used_links == {"primary", "secondary"}


def test_independent_oversubscribes_shared_bottleneck() -> None:
    links = {"wan": LinkState("wan", capacity_bps=5_000_000.0)}
    transfers = [_make_transfer(f"t{i}", 2_000_000, ["wan"]) for i in range(4)]
    dec = create_scheduler(PolicyName.INDEPENDENT).decide(SchedulerInput(now=0.0, transfers=transfers, links=links))
    # Uncoordinated downloaders each greedily ask for the full link capacity.
    assert sum(dec.allocations_bps.values()) == pytest.approx(20_000_000.0)
