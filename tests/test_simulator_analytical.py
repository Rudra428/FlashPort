"""Analytical verification and directed-graph constraint tests for the SimPy simulator."""

from __future__ import annotations

import json

import pytest

from flowweave.models import (
    LinkDirection,
    PolicyName,
    ScheduleDecision,
    SchedulerInput,
    TransferDirection,
    TransferState,
)
from flowweave.scheduler.base import Scheduler
from flowweave.simulator import BandwidthEvent, LinkSpec, NetworkSimulator, ScenarioSpec, TransferSpec
from flowweave.simulator.scenarios import (
    bidirectional_mixed,
    independent_paths,
    partially_shared_paths,
)

MB = 1024 * 1024


class _AdversarialScheduler(Scheduler):
    """Returns negative rates and 50x over-capacity rates to verify simulator enforcement."""

    name = PolicyName.INDEPENDENT

    def _schedule(self, state: SchedulerInput) -> ScheduleDecision:
        allocs = {}
        routes = {}
        links = {}
        for idx, t in enumerate(state.transfers):
            route = t.route or [t.assigned_link]
            routes[t.transfer_id] = route
            links[t.transfer_id] = route[0]
            if idx == 0:
                allocs[t.transfer_id] = 50.0 * state.links[route[0]].capacity_bps
            else:
                allocs[t.transfer_id] = -10_000.0
        return ScheduleDecision(
            policy=self.name,
            timestamp=state.now,
            allocations_bps=allocs,
            link_assignments=links,
            route_assignments=routes,
        )


def test_single_transfer_analytical_completion_time() -> None:
    """100 MB over a lossless, zero-latency 10 MB/s link must finish in ~10.0s."""
    scenario = ScenarioSpec(
        name="analytical_single",
        description="Single 100 MB transfer over 10 MB/s link.",
        links=[LinkSpec("l1", capacity_bps=10 * MB, latency_ms=0.0, loss_rate=0.0)],
        transfers=[
            TransferSpec("t1", "100mb.bin", 100 * MB, arrival_time_s=0.0, route=["l1"], chunk_size_bytes=MB)
        ],
        tick_s=0.1,
        seed=1,
    )
    res = NetworkSimulator(scenario, PolicyName.MAX_MIN).run()
    assert res.capacity_violations == 0
    assert res.summary.completed_transfers == 1
    assert res.summary.outcomes[0].fct_s == pytest.approx(10.0, abs=0.2)


def test_three_transfers_sharing_bottleneck() -> None:
    """Three 10 MB files on a 10 MB/s bottleneck under max-min finish around 3.0s each."""
    scenario = ScenarioSpec(
        name="three_shared",
        description="Three 10 MB transfers on one 10 MB/s link.",
        links=[LinkSpec("shared", capacity_bps=10 * MB, latency_ms=0.0, loss_rate=0.0)],
        transfers=[
            TransferSpec(f"t{i}", f"f{i}.bin", 10 * MB, arrival_time_s=0.0, route=["shared"])
            for i in range(3)
        ],
        tick_s=0.1,
        seed=2,
    )
    res = NetworkSimulator(scenario, PolicyName.MAX_MIN).run()
    assert res.capacity_violations == 0
    assert res.summary.makespan_s == pytest.approx(3.0, abs=0.2)
    for o in res.summary.outcomes:
        assert o.fct_s == pytest.approx(3.0, abs=0.2)


def test_independent_links_zero_interference() -> None:
    """Transfers on separate links finish at their exact individual link capacities."""
    res = NetworkSimulator(independent_paths(seed=7), PolicyName.FLOWWEAVE).run()
    assert res.capacity_violations == 0
    by_id = {o.transfer_id: o for o in res.summary.outcomes}
    # 20 MB / 10 MB/s = 2.0s; 12 MB / 6 MB/s = 2.0s; 8 MB / 4 MB/s = 2.0s
    for tid in ("tx-1", "tx-2", "tx-3"):
        assert by_id[tid].fct_s == pytest.approx(2.0, abs=0.25)


def test_partially_shared_multihop_routes() -> None:
    """Shared core bottleneck is respected while isolated path finishes independently at 2.0s."""
    for policy in PolicyName:
        res = NetworkSimulator(partially_shared_paths(seed=42), policy).run()
        assert res.capacity_violations == 0
        assert res.summary.completed_transfers == 4
        by_id = {o.transfer_id: o for o in res.summary.outcomes}
        # tx-4 is 12 MB on isolated 6 MB/s direct-link -> ~2.0s regardless of policy!
        assert by_id["tx-4"].fct_s == pytest.approx(2.0, abs=0.25)
        # No negative remaining bytes or allocations anywhere in timeline
        for pt in res.timeline:
            assert all(v >= 0 for v in pt["remaining_bytes"].values())
            assert all(v >= 0.0 for v in pt["allocated_bps"].values())
            assert all(0.0 <= u <= 1.001 for u in pt["link_utilization"].values())


def test_dynamic_bandwidth_analytical() -> None:
    """75 MB transfer at 10 MB/s for 5s (50 MB) then 5 MB/s (remaining 25 MB -> 5s) = 10.0s."""
    scenario = ScenarioSpec(
        name="dynamic_analytical",
        description="Capacity halves at t=5s.",
        links=[
            LinkSpec(
                "dyn",
                capacity_bps=10 * MB,
                latency_ms=0.0,
                loss_rate=0.0,
                events=[BandwidthEvent(time_s=5.0, capacity_bps=5 * MB)],
            )
        ],
        transfers=[TransferSpec("t1", "75mb.bin", 75 * MB, arrival_time_s=0.0, route=["dyn"])],
        tick_s=0.1,
        seed=3,
    )
    res = NetworkSimulator(scenario, PolicyName.MAX_MIN).run()
    assert res.capacity_violations == 0
    assert res.summary.outcomes[0].fct_s == pytest.approx(10.0, abs=0.25)


def test_bidirectional_mixed_workload_and_reproducibility() -> None:
    """Simultaneous uploads and downloads respect separate uplink/downlink + shared core limits."""
    sc = bidirectional_mixed(seed=99)
    r1 = NetworkSimulator(sc, PolicyName.FLOWWEAVE).run()
    r2 = NetworkSimulator(sc, PolicyName.FLOWWEAVE).run()
    assert r1.capacity_violations == 0
    assert r1.summary.completed_transfers == 6
    assert r1.summary.sum_fct_s == pytest.approx(r2.summary.sum_fct_s)
    dirs = {o.direction for o in r1.summary.outcomes}
    assert dirs == {TransferDirection.UPLOAD, TransferDirection.DOWNLOAD}


def test_completed_transfers_get_zero_bandwidth_and_adversarial_clamped(tmp_path) -> None:
    """Adversarial negative/over-capacity allocations are strictly clamped, and completed flows get 0."""
    scenario = ScenarioSpec(
        name="clamp_test",
        description="Staggered arrival + adversarial scheduler.",
        links=[
            LinkSpec("core", capacity_bps=5 * MB, latency_ms=0.0, loss_rate=0.0, direction=LinkDirection.SHARED)
        ],
        transfers=[
            TransferSpec("fast", "fast.bin", 5 * MB, arrival_time_s=0.0, route=["core"]),
            TransferSpec("late", "late.bin", 5 * MB, arrival_time_s=0.5, route=["core"]),
        ],
        tick_s=0.1,
        seed=5,
        max_sim_time_s=20.0,
    )
    res = NetworkSimulator(scenario, _AdversarialScheduler()).run()
    assert res.capacity_violations == 0
    # fast finishes cleanly while late got clamped from negative to 0 and never went negative
    fast_outcome = next(o for o in res.summary.outcomes if o.transfer_id == "fast")
    assert fast_outcome.state == TransferState.COMPLETED
    for pt in res.timeline:
        if pt["time_s"] > fast_outcome.completion_time + 0.15:
            assert pt["allocated_bps"]["fast"] == 0.0
        assert all(v >= 0.0 for v in pt["allocated_bps"].values())
        assert all(v >= 0 for v in pt["remaining_bytes"].values())

    # Verify JSON, CSV, and Matplotlib plot export
    json_p = res.save_json(tmp_path / "sim.json")
    csv_p = res.save_csv(tmp_path / "sim.csv")
    png_p = res.plot(tmp_path / "sim.png")
    assert json.loads(json_p.read_text(encoding="utf-8"))["is_simulated"] is True
    assert csv_p.exists() and csv_p.stat().st_size > 20
    assert png_p.exists() and png_p.stat().st_size > 1000
