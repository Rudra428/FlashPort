"""Comprehensive tests for the convex optimization bandwidth-allocation engine and FlowWeaveScheduler."""

from __future__ import annotations

import math

import pytest

from flowweave.config import SchedulerConfig
from flowweave.models import (
    LinkDirection,
    LinkState,
    PolicyName,
    SchedulerInput,
    TransferDirection,
    TransferSnapshot,
    TransferState,
)
from flowweave.scheduler import (
    create_scheduler,
    optimize_allocations,
    solve_single_link_analytical,
)
from flowweave.simulator import NetworkSimulator
from flowweave.simulator.scenarios import (
    bidirectional_mixed,
    congested_lossy_links,
    dynamic_bandwidth_drop,
    partially_shared_paths,
)


def _snap(
    tid: str,
    rem: int,
    *,
    links: list[str] | None = None,
    route: list[str] | None = None,
    state: TransferState = TransferState.ACTIVE,
    direction: TransferDirection = TransferDirection.DOWNLOAD,
) -> TransferSnapshot:
    cands = links or (route[:1] if route else ["l1"])
    return TransferSnapshot(
        transfer_id=tid,
        filename=f"{tid}.bin",
        url=f"sim://{tid}",
        total_bytes=max(rem, 1),
        remaining_bytes=rem,
        state=state,
        direction=direction,
        candidate_links=cands,
        assigned_link=cands[0],
        route=route or [cands[0]],
        candidate_routes=[route] if route else [[lid] for lid in cands],
        created_at=0.0,
        started_at=0.0,
    )


def _assert_all_valid(res, links: dict[str, LinkState], *, allow_striping: bool = False) -> None:
    """Verify non-negativity, finiteness, and strict effective-capacity constraints on every link."""
    for tid, rate in res.per_transfer_bps.items():
        assert math.isfinite(rate) and rate >= 0.0, f"Invalid rate for {tid}: {rate}"

    link_totals: dict[str, float] = {lid: 0.0 for lid in links}
    for (tid, lid), rate in res.per_transfer_link_bps.items():
        assert math.isfinite(rate) and rate >= 0.0
        link_totals[lid] += rate

    for lid, total in link_totals.items():
        cap_eff = res.effective_capacities_bps[lid]
        assert total <= cap_eff * (1.0 + 1e-5) + 1.0, (
            f"Link {lid} exceeded C_eff: {total:.2f} > {cap_eff:.2f}"
        )


def test_analytical_single_link_matches_sqrt_formula() -> None:
    rem = {"t1": 1_000_000.0, "t2": 4_000_000.0, "t3": 9_000_000.0}
    cap = 12_000_000.0
    allocs, status = solve_single_link_analytical(rem, cap)
    assert status == "optimal_analytical"
    # sqrt ratios: 1 : 2 : 3 -> sum = 6 -> 2 MB/s, 4 MB/s, 6 MB/s
    assert allocs["t1"] == pytest.approx(2_000_000.0, rel=1e-6)
    assert allocs["t2"] == pytest.approx(4_000_000.0, rel=1e-6)
    assert allocs["t3"] == pytest.approx(6_000_000.0, rel=1e-6)


def test_cvxpy_single_link_matches_analytical_oracle_with_and_without_fairness() -> None:
    links = {"shared": LinkState("shared", capacity_bps=10_000_000.0, loss_rate=0.0)}
    transfers = [
        _snap("t1", 500_000, links=["shared"]),
        _snap("t2", 4_500_000, links=["shared"]),
        _snap("t3", 20_000_000, links=["shared"]),
    ]

    for fair in (None, 0.45):
        res_analytic = optimize_allocations(
            transfers, links, fairness_fraction=fair, force_cvxpy=False
        )
        res_cvxpy = optimize_allocations(
            transfers, links, fairness_fraction=fair, force_cvxpy=True
        )
        _assert_all_valid(res_analytic, links)
        _assert_all_valid(res_cvxpy, links)
        assert res_analytic.solver_name == "analytical_kkt"
        assert res_cvxpy.solver_name in ("CLARABEL", "SCS")
        for t in transfers:
            assert res_cvxpy.per_transfer_bps[t.transfer_id] == pytest.approx(
                res_analytic.per_transfer_bps[t.transfer_id], rel=1e-3
            )
        if fair is not None:
            floor = fair * 10_000_000.0 / 3.0
            for t in transfers:
                assert res_cvxpy.per_transfer_bps[t.transfer_id] >= floor - 10.0


def test_independent_links_zero_interference() -> None:
    links = {
        "l1": LinkState("l1", capacity_bps=8_000_000.0, loss_rate=0.0),
        "l2": LinkState("l2", capacity_bps=5_000_000.0, loss_rate=0.10),  # C_eff = 4.5 MB/s * 0.92
    }
    transfers = [
        _snap("t1", 4_000_000, links=["l1"]),
        _snap("t2", 6_000_000, links=["l2"]),
    ]
    res = optimize_allocations(transfers, links, congestion_headroom=0.92)
    _assert_all_valid(res, links)
    assert res.per_transfer_bps["t1"] == pytest.approx(8_000_000.0, rel=1e-6)
    # l2 has loss=0.10 (>= 0.03 -> congested), so C_eff = 5M * 0.90 * 0.92 = 4.14M
    assert res.per_transfer_bps["t2"] == pytest.approx(5_000_000.0 * 0.90 * 0.92, rel=1e-6)


def test_multihop_shared_links_cvxpy() -> None:
    links = {
        "a1": LinkState("a1", capacity_bps=10_000_000.0, loss_rate=0.0),
        "a2": LinkState("a2", capacity_bps=10_000_000.0, loss_rate=0.0),
        "core": LinkState("core", capacity_bps=6_000_000.0, loss_rate=0.0),
        "isolated": LinkState("isolated", capacity_bps=7_000_000.0, loss_rate=0.0),
    }
    transfers = [
        _snap("t1", 1_000_000, route=["a1", "core"]),
        _snap("t2", 9_000_000, route=["a2", "core"]),
        _snap("t3", 5_000_000, route=["isolated"]),
    ]
    res = optimize_allocations(transfers, links, fairness_fraction=None)
    _assert_all_valid(res, links)
    assert res.mode == "multihop_shared"
    # t1 & t2 share core (6 MB/s) in sqrt(1):sqrt(9) = 1:3 ratio -> 1.5 MB/s and 4.5 MB/s
    assert res.per_transfer_bps["t1"] == pytest.approx(1_500_000.0, rel=1e-3)
    assert res.per_transfer_bps["t2"] == pytest.approx(4_500_000.0, rel=1e-3)
    # t3 uses isolated link fully without interference
    assert res.per_transfer_bps["t3"] == pytest.approx(7_000_000.0, rel=1e-3)


def test_multilink_striping_cvxpy_respects_permitted_links() -> None:
    links = {
        "l1": LinkState("l1", capacity_bps=10_000_000.0, loss_rate=0.0),
        "l2": LinkState("l2", capacity_bps=6_000_000.0, loss_rate=0.0),
    }
    # t1 can stripe across both l1 & l2; t2 is restricted to l1 only.
    transfers = [
        _snap("t1", 8_000_000, links=["l1", "l2"]),
        _snap("t2", 8_000_000, links=["l1"]),
    ]
    res = optimize_allocations(transfers, links, allow_striping=True, fairness_fraction=0.1)
    _assert_all_valid(res, links, allow_striping=True)
    assert res.mode == "multilink_striping"
    # t2 must NEVER be allocated bandwidth on forbidden link l2
    assert res.per_transfer_link_bps.get(("t2", "l2"), 0.0) == 0.0
    # Aggregate rate across both links should saturate total 16 MB/s (8 MB/s each by symmetry!)
    assert res.per_transfer_bps["t1"] == pytest.approx(8_000_000.0, rel=1e-2)
    assert res.per_transfer_bps["t2"] == pytest.approx(8_000_000.0, rel=1e-2)
    # t1 genuinely stripes: uses l2 (6 MB/s) + part of l1 (2 MB/s)
    assert res.per_transfer_link_bps[("t1", "l2")] == pytest.approx(6_000_000.0, rel=1e-2)


def test_zero_remaining_inactive_and_no_feasible_link() -> None:
    links = {
        "ok": LinkState("ok", capacity_bps=5_000_000.0, loss_rate=0.0),
        "down": LinkState("down", capacity_bps=0.0, loss_rate=0.0),
    }
    transfers = [
        _snap("active", 2_000_000, links=["ok"]),
        _snap("zero_rem", 0, links=["ok"]),
        _snap("paused", 2_000_000, links=["ok"], state=TransferState.PAUSED),
        _snap("done", 2_000_000, links=["ok"], state=TransferState.COMPLETED),
        _snap("dead_link", 2_000_000, links=["down"]),
    ]
    res = optimize_allocations(transfers, links)
    _assert_all_valid(res, links)
    assert res.per_transfer_bps["active"] == pytest.approx(5_000_000.0)
    assert res.per_transfer_bps["zero_rem"] == 0.0
    assert res.per_transfer_bps["paused"] == 0.0
    assert res.per_transfer_bps["done"] == 0.0
    assert res.per_transfer_bps["dead_link"] == 0.0
    assert any("dead_link" in w for w in res.warnings)


def test_infeasible_fairness_falls_back_safely_without_violating_capacity() -> None:
    links = {"wan": LinkState("wan", capacity_bps=10_000_000.0, loss_rate=0.0)}
    transfers = [_snap("t1", 1_000_000, links=["wan"]), _snap("t2", 9_000_000, links=["wan"])]
    res = optimize_allocations(transfers, links, fairness_fraction=1.5)
    _assert_all_valid(res, links)
    assert res.status == "infeasible_fairness_fallback"
    assert sum(res.per_transfer_bps.values()) <= 10_000_000.0 + 1.0


def test_solver_failure_triggers_capacity_safe_fallback() -> None:
    links = {
        "a1": LinkState("a1", capacity_bps=10_000_000.0, loss_rate=0.0),
        "core": LinkState("core", capacity_bps=6_000_000.0, loss_rate=0.0),
    }
    transfers = [
        _snap("t1", 2_000_000, route=["a1", "core"]),
        _snap("t2", 8_000_000, route=["a1", "core"]),
    ]
    res = optimize_allocations(
        transfers,
        links,
        force_cvxpy=True,
        solver_override="NONEXISTENT_SOLVER",
    )
    _assert_all_valid(res, links)
    assert res.status == "solver_fallback"
    assert sum(res.per_transfer_bps.values()) <= 6_000_000.0 + 1.0


def test_separate_full_duplex_upload_and_download_capacities() -> None:
    links = {
        "downlink": LinkState("downlink", capacity_bps=10_000_000.0, direction=LinkDirection.DOWNLINK),
        "uplink": LinkState("uplink", capacity_bps=4_000_000.0, direction=LinkDirection.UPLINK),
    }
    transfers = [
        _snap("dl1", 1_000_000, links=["downlink"], direction=TransferDirection.DOWNLOAD),
        _snap("dl2", 4_000_000, links=["downlink"], direction=TransferDirection.DOWNLOAD),
        _snap("ul1", 1_000_000, links=["uplink"], direction=TransferDirection.UPLOAD),
        _snap("ul2", 9_000_000, links=["uplink"], direction=TransferDirection.UPLOAD),
    ]
    sched = create_scheduler(PolicyName.FLOWWEAVE, config=SchedulerConfig(min_fair_share_fraction=0.0))
    dec = sched.decide(SchedulerInput(now=0.0, transfers=transfers, links=links))
    # Downlink (10M) and uplink (4M) are optimized independently without conflating full-duplex capacities.
    assert dec.allocations_bps["dl1"] + dec.allocations_bps["dl2"] == pytest.approx(10_000_000.0, rel=1e-4)
    assert dec.allocations_bps["ul1"] + dec.allocations_bps["ul2"] == pytest.approx(4_000_000.0, rel=1e-4)
    assert dec.allocations_bps["ul1"] == pytest.approx(1_000_000.0, rel=1e-4)
    assert dec.allocations_bps["ul2"] == pytest.approx(3_000_000.0, rel=1e-4)


@pytest.mark.parametrize(
    "scenario_fn",
    [partially_shared_paths, bidirectional_mixed, dynamic_bandwidth_drop, congested_lossy_links],
)
def test_flowweave_convex_in_simulator_scenarios(scenario_fn) -> None:
    sc = scenario_fn(seed=42)
    res = NetworkSimulator(sc, PolicyName.FLOWWEAVE).run()
    assert res.capacity_violations == 0
    assert res.summary.completed_transfers == len(sc.transfers)
    assert res.summary.sum_fct_s > 0
