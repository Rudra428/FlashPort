"""SimPy-based discrete-event fluid-flow network simulator for FlowWeave.

Note: This is a fluid-flow bandwidth-sharing simulator (not a packet-level TCP simulator).
All simulated throughput numbers are explicitly tagged as simulated goodput rather than
real measured OS/network throughput.
"""

from __future__ import annotations

import csv
import json
import math
import random
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import simpy

from flowweave.config import MonitorConfig, SchedulerConfig, StragglerConfig
from flowweave.metrics import summarize_experiment
from flowweave.models import (
    ChunkState,
    ExperimentSummary,
    LinkState,
    PolicyName,
    SchedulerInput,
    TransferOutcome,
    TransferSnapshot,
    TransferState,
)
from flowweave.monitor.monitor import NetworkMonitor
from flowweave.scheduler import Scheduler, create_scheduler
from flowweave.scheduler.base import validate_and_enforce_allocations
from flowweave.simulator.topology import BandwidthEvent, ScenarioSpec, TransferSpec
from flowweave.straggler.detector import ActiveChunkView, StragglerDetector


@dataclass
class _SimChunk:
    chunk_id: str
    logical_index: int
    transfer_id: str
    link_id: str
    route: list[str]
    total_bytes: int
    remaining_bytes: float
    started_at: float
    startup_remaining_s: float
    slowdown_factor: float = 1.0
    state: ChunkState = ChunkState.IN_FLIGHT
    is_replica: bool = False
    replica_of: str | None = None
    has_replica: bool = False
    transmitted_bytes: float = 0.0
    last_rate_bps: float = 0.0


@dataclass
class _SimTransfer:
    spec: TransferSpec
    state: TransferState = TransferState.PENDING
    assigned_link: str = ""
    assigned_route: list[str] = field(default_factory=list)
    allocated_bps: float = 0.0
    started_at: float | None = None
    completed_at: float | None = None
    total_chunks: int = 0
    next_chunk_index: int = 0
    completed_logical_chunks: set[int] = field(default_factory=set)
    active_chunks: list[_SimChunk] = field(default_factory=list)
    connected_links: set[str] = field(default_factory=set)
    remaining_bytes_total: int = 0
    redundant_bytes: float = 0.0
    retries: int = 0


@dataclass
class SimulationResult:
    """Detailed output of one simulation run including per-step telemetry and invariants."""

    summary: ExperimentSummary
    timeline: list[dict[str, Any]]
    capacity_violations: int
    replications_triggered: int
    replications_won: int

    def save_json(self, path: str | Path) -> Path:
        out = Path(path)
        out.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "is_simulated": True,
            "summary": self.summary.to_dict(),
            "capacity_violations": self.capacity_violations,
            "timeline": self.timeline,
        }
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return out

    def save_csv(self, path: str | Path) -> Path:
        out = Path(path)
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(
                [
                    "time_s",
                    "active_transfers",
                    "simulated_goodput_bps",
                    "scheduler_ms",
                    "link_utilization_json",
                ]
            )
            for pt in self.timeline:
                writer.writerow(
                    [
                        pt["time_s"],
                        pt["active_transfers"],
                        pt["aggregate_goodput_bps"],
                        pt.get("scheduler_ms", 0.0),
                        json.dumps(pt.get("link_utilization", {})),
                    ]
                )
        return out

    def plot(self, output_path: str | Path) -> Path:
        """Generate a 3-panel Matplotlib figure: remaining bytes, allocations, and link utilization."""
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)

        times = [pt["time_s"] for pt in self.timeline]
        transfer_ids = [o.transfer_id for o in self.summary.outcomes]
        link_ids = sorted({lid for pt in self.timeline for lid in pt.get("link_utilization", {})})

        fig, axes = plt.subplots(3, 1, figsize=(10, 9), sharex=True)

        for tid in transfer_ids:
            rem_mb = [pt.get("remaining_bytes", {}).get(tid, 0) / (1024 * 1024) for pt in self.timeline]
            axes[0].plot(times, rem_mb, label=tid)
        axes[0].set_ylabel("Remaining (MB)")
        axes[0].set_title(
            f"FlowWeave Fluid-Flow Simulation: {self.summary.scenario} [{self.summary.policy}]"
        )
        axes[0].grid(True, linestyle="--", alpha=0.35)
        if len(transfer_ids) <= 10:
            axes[0].legend(ncol=3, fontsize=8)

        for tid in transfer_ids:
            alloc_mb = [pt.get("allocated_bps", {}).get(tid, 0.0) / (1024 * 1024) for pt in self.timeline]
            axes[1].plot(times, alloc_mb, label=tid)
        axes[1].set_ylabel("Allocated (MB/s)")
        axes[1].grid(True, linestyle="--", alpha=0.35)

        for lid in link_ids:
            util = [pt.get("link_utilization", {}).get(lid, 0.0) * 100.0 for pt in self.timeline]
            axes[2].plot(times, util, label=lid, linewidth=2.0)
        axes[2].set_ylabel("Link Util (%)")
        axes[2].set_xlabel("Simulation Time (s)")
        axes[2].set_ylim(-5, 108)
        axes[2].grid(True, linestyle="--", alpha=0.35)
        axes[2].legend(ncol=3, fontsize=8)

        fig.tight_layout()
        fig.savefig(out, dpi=140)
        plt.close(fig)
        return out


class NetworkSimulator:
    """Discrete-event directed-graph simulator enforcing shared-link capacity constraints."""

    def __init__(
        self,
        scenario: ScenarioSpec,
        scheduler: Scheduler | PolicyName | str,
        *,
        scheduler_config: SchedulerConfig | None = None,
        straggler_config: StragglerConfig | None = None,
        monitor_config: MonitorConfig | None = None,
    ) -> None:
        self.scenario = scenario
        if isinstance(scheduler, Scheduler):
            self.scheduler = scheduler
        else:
            self.scheduler = create_scheduler(
                scheduler,
                config=scheduler_config,
                horizon_s=max(scenario.tick_s, 0.05),
            )
        self.monitor = NetworkMonitor(monitor_config)
        self.straggler_detector = StragglerDetector(straggler_config)

    def run(self) -> SimulationResult:
        """Run the scenario to completion (or max_sim_time_s) and return metrics + telemetry."""
        env = simpy.Environment()
        rng = random.Random(self.scenario.seed)

        # Pre-draw deterministic per-chunk slowdowns so every policy sees identical impairments.
        chunk_slowdowns: dict[tuple[str, int], float] = {}
        for tspec in self.scenario.transfers:
            n_chunks = max(1, math.ceil(tspec.size_bytes / max(1, tspec.chunk_size_bytes)))
            for idx in range(n_chunks):
                frac = idx / max(1, n_chunks)
                factor = 1.0
                if tspec.impairment_factor < 1.0 and frac >= tspec.impairment_start_fraction:
                    factor = tspec.impairment_factor
                elif (
                    self.scenario.straggler_chunk_prob > 0
                    and rng.random() < self.scenario.straggler_chunk_prob
                ):
                    factor = self.scenario.straggler_slowdown_factor
                chunk_slowdowns[(tspec.transfer_id, idx)] = factor

        links: dict[str, LinkState] = {}
        for lspec in self.scenario.links:
            lstate = LinkState(
                link_id=lspec.link_id,
                capacity_bps=lspec.capacity_bps,
                latency_ms=lspec.latency_ms,
                loss_rate=lspec.loss_rate,
                is_shared=lspec.is_shared,
                direction=lspec.direction,
                source_node=lspec.source_node,
                target_node=lspec.target_node,
                estimated_available_bps=lspec.capacity_bps,
            )
            links[lspec.link_id] = lstate
            self.monitor.register_link(lstate)
            for ev in lspec.events:
                env.process(self._apply_link_event(env, links[lspec.link_id], ev))

        transfers: dict[str, _SimTransfer] = {}
        for tspec in self.scenario.transfers:
            n_chunks = max(1, math.ceil(tspec.size_bytes / max(1, tspec.chunk_size_bytes)))
            init_route = list(tspec.route) if tspec.route else [next(iter(links))]
            st = _SimTransfer(
                spec=tspec,
                state=TransferState.PENDING,
                assigned_link=init_route[0],
                assigned_route=init_route,
                total_chunks=n_chunks,
                remaining_bytes_total=tspec.size_bytes,
            )
            transfers[tspec.transfer_id] = st
            env.process(self._arrive_transfer(env, st))

        scheduler_overheads: list[float] = []
        timeline: list[dict[str, Any]] = []
        stats = {
            "capacity_violations": 0,
            "replications_triggered": 0,
            "replications_won": 0,
        }

        env.process(
            self._tick_loop(
                env=env,
                links=links,
                transfers=transfers,
                chunk_slowdowns=chunk_slowdowns,
                rng=rng,
                scheduler_overheads=scheduler_overheads,
                timeline=timeline,
                stats=stats,
            )
        )

        env.run(until=self.scenario.max_sim_time_s)

        outcomes: list[TransferOutcome] = []
        for tspec in self.scenario.transfers:
            st = transfers[tspec.transfer_id]
            start_t = st.started_at if st.started_at is not None else tspec.arrival_time_s
            end_t = st.completed_at if st.completed_at is not None else env.now
            fct = max(0.0, end_t - tspec.arrival_time_s)
            useful_done = max(0, tspec.size_bytes - st.remaining_bytes_total)
            avg_gp = useful_done / fct if fct > 0 else 0.0
            outcomes.append(
                TransferOutcome(
                    transfer_id=tspec.transfer_id,
                    filename=tspec.filename,
                    size_bytes=tspec.size_bytes,
                    arrival_time=tspec.arrival_time_s,
                    start_time=start_t,
                    completion_time=end_t,
                    fct_s=fct,
                    avg_goodput_bps=avg_gp,
                    direction=tspec.direction,
                    redundant_bytes=int(round(st.redundant_bytes)),
                    retries=st.retries,
                    state=st.state,
                )
            )

        summary = summarize_experiment(
            scenario=self.scenario.name,
            policy=self.scheduler.name.value,
            seed=self.scenario.seed,
            outcomes=outcomes,
            scheduler_overhead_ms=scheduler_overheads,
            replications_triggered=stats["replications_triggered"],
            replications_won=stats["replications_won"],
        )
        return SimulationResult(
            summary=summary,
            timeline=timeline,
            capacity_violations=stats["capacity_violations"],
            replications_triggered=stats["replications_triggered"],
            replications_won=stats["replications_won"],
        )

    @staticmethod
    def _apply_link_event(env: simpy.Environment, link: LinkState, event: BandwidthEvent):
        if event.time_s > env.now:
            yield env.timeout(event.time_s - env.now)
        if event.capacity_bps is not None:
            link.capacity_bps = event.capacity_bps
        if event.latency_ms is not None:
            link.latency_ms = event.latency_ms
        if event.loss_rate is not None:
            link.loss_rate = event.loss_rate

    def _arrive_transfer(self, env: simpy.Environment, st: _SimTransfer):
        if st.spec.arrival_time_s > env.now:
            yield env.timeout(st.spec.arrival_time_s - env.now)
        st.state = TransferState.ACTIVE
        st.started_at = env.now
        self.monitor.register_transfer(
            st.spec.transfer_id,
            st.spec.size_bytes,
            env.now,
            direction=st.spec.direction,
        )

    def _tick_loop(
        self,
        *,
        env: simpy.Environment,
        links: dict[str, LinkState],
        transfers: dict[str, _SimTransfer],
        chunk_slowdowns: dict[tuple[str, int], float],
        rng: random.Random,
        scheduler_overheads: list[float],
        timeline: list[dict[str, Any]],
        stats: dict[str, int],
    ):
        dt = self.scenario.tick_s
        while True:
            if all(t.state == TransferState.COMPLETED for t in transfers.values()):
                break

            active_list = [t for t in transfers.values() if t.state == TransferState.ACTIVE]
            if not active_list:
                yield env.timeout(dt)
                continue

            # 1. Request allocations from the configured scheduler.
            snapshots = [self._to_snapshot(t) for t in active_list]
            decision = self.scheduler.decide(
                SchedulerInput(now=env.now, transfers=snapshots, links=dict(links))
            )
            scheduler_overheads.append(decision.compute_time_ms)

            # 2. Apply route assignments, clamp negative/inactive rates, and spawn chunks.
            active_ids = {t.spec.transfer_id for t in active_list}
            raw_requested: dict[str, float] = {}
            routes_by_tid: dict[str, list[str]] = {}
            for st in active_list:
                tid = st.spec.transfer_id
                if tid in decision.route_assignments and decision.route_assignments[tid]:
                    st.assigned_route = list(decision.route_assignments[tid])
                    st.assigned_link = st.assigned_route[0]
                elif tid in decision.link_assignments:
                    st.assigned_link = decision.link_assignments[tid]
                    st.assigned_route = [st.assigned_link]
                routes_by_tid[tid] = list(st.assigned_route)
                raw_requested[tid] = max(0.0, float(decision.allocations_bps.get(tid, 0.0)))
                self._ensure_chunks_spawned(st, links, env.now, chunk_slowdowns)

            # Reset completed transfers strictly to 0 allocated_bps.
            for st in transfers.values():
                if st.state != TransferState.ACTIVE:
                    st.allocated_bps = 0.0

            # 3. Optional straggler detection & speculative replication (FlowWeave only).
            if (
                self.scenario.enable_straggler_replication
                and self.scheduler.name == PolicyName.FLOWWEAVE
            ):
                for st in active_list:
                    st.allocated_bps = raw_requested.get(st.spec.transfer_id, 0.0)
                self._run_straggler_check(env.now, active_list, links, stats)

            # 4. Collect per-chunk requested rates across all hops of each chunk's route,
            #    compute per-link oversubscription ratios, and enforce sum_{i: j in P_i} b_i <= C_j.
            chunk_requested: dict[str, float] = {}
            chunk_routes: dict[str, list[str]] = {}
            chunk_map: dict[str, tuple[_SimTransfer, _SimChunk]] = {}
            link_raw_demand: dict[str, float] = defaultdict(float)

            for st in active_list:
                tid = st.spec.transfer_id
                in_flight = [c for c in st.active_chunks if c.state == ChunkState.IN_FLIGHT]
                if not in_flight or raw_requested[tid] <= 0:
                    continue
                per_chunk_req = raw_requested[tid] / len(in_flight)
                for ch in in_flight:
                    req = per_chunk_req * ch.slowdown_factor
                    chunk_requested[ch.chunk_id] = req
                    chunk_routes[ch.chunk_id] = ch.route
                    chunk_map[ch.chunk_id] = (st, ch)
                    for lid in ch.route:
                        link_raw_demand[lid] += req

            # Enforce strict multi-hop link capacities across all active chunks simultaneously.
            chunk_enforced = validate_and_enforce_allocations(
                allocations=chunk_requested,
                routes=chunk_routes,
                links=links,
                active_ids=set(chunk_requested.keys()),
            )

            # Compute per-link oversubscription congestion factor.
            link_oversub_waste: dict[str, float] = {}
            for lid, link in links.items():
                ratio = link_raw_demand.get(lid, 0.0) / link.capacity_bps if link.capacity_bps > 0 else 0.0
                if ratio > 1.05:
                    link_oversub_waste[lid] = min(0.28, 0.085 * math.log(ratio))
                else:
                    link_oversub_waste[lid] = 0.0

            # Record effective allocated rate per transfer (sum of enforced chunk rates).
            for st in active_list:
                st.allocated_bps = sum(
                    chunk_enforced.get(c.chunk_id, 0.0)
                    for c in st.active_chunks
                    if c.state == ChunkState.IN_FLIGHT
                )

            useful_delta: dict[str, int] = {tid: 0 for tid in active_ids}
            raw_delta: dict[str, int] = {tid: 0 for tid in active_ids}
            redundant_delta: dict[str, int] = {tid: 0 for tid in active_ids}
            retries_delta: dict[str, int] = {tid: 0 for tid in active_ids}
            wire_bytes_per_link: dict[str, float] = defaultdict(float)
            active_tids_per_link: dict[str, set[str]] = defaultdict(set)
            agg_goodput_tick = 0.0

            for cid, wire_rate in chunk_enforced.items():
                st, ch = chunk_map[cid]
                tid = st.spec.transfer_id
                for lid in ch.route:
                    active_tids_per_link[lid].add(tid)

                if wire_rate <= 0.0:
                    ch.last_rate_bps = 0.0
                    continue

                avail_dt = dt
                if ch.startup_remaining_s > 0:
                    consumed = min(ch.startup_remaining_s, avail_dt)
                    ch.startup_remaining_s -= consumed
                    avail_dt -= consumed
                if avail_dt <= 0:
                    ch.last_rate_bps = 0.0
                    continue

                # Compound loss and oversubscription tax along the chunk's directed route.
                route_goodput_factor = 1.0
                max_loss = 0.0
                for lid in ch.route:
                    loss_tax = min(0.85, max(0.0, links[lid].loss_rate))
                    max_loss = max(max_loss, loss_tax)
                    route_goodput_factor *= (1.0 - loss_tax) * (1.0 - link_oversub_waste[lid])
                route_goodput_factor = max(0.05, route_goodput_factor)

                if max_loss > 0.01 and rng.random() < max_loss * 0.5:
                    retries_delta[tid] += 1
                    st.retries += 1

                wire_bytes_budget = wire_rate * avail_dt
                useful_bytes_budget = wire_bytes_budget * route_goodput_factor

                current = ch
                useful_spent = 0.0
                while useful_bytes_budget > 1e-6:
                    advanced = min(current.remaining_bytes, useful_bytes_budget)
                    actual_wire = advanced / route_goodput_factor
                    waste_bytes = max(0.0, actual_wire - advanced)

                    for lid in current.route:
                        wire_bytes_per_link[lid] += actual_wire

                    useful_spent += advanced
                    current.remaining_bytes = max(0.0, current.remaining_bytes - advanced)
                    current.transmitted_bytes += actual_wire
                    useful_bytes_budget -= advanced

                    raw_delta[tid] += int(round(actual_wire))
                    if waste_bytes > 0:
                        st.redundant_bytes += waste_bytes
                        redundant_delta[tid] += int(round(waste_bytes))
                    if not current.is_replica:
                        useful_delta[tid] += int(round(advanced))
                        agg_goodput_tick += advanced / dt

                    if (
                        current.remaining_bytes > 1e-6
                        or current.is_replica
                        or current.slowdown_factor < 1.0
                        or st.next_chunk_index >= st.total_chunks
                        or st.assigned_route != current.route
                    ):
                        break

                    next_idx = st.next_chunk_index
                    next_slow = chunk_slowdowns.get((tid, next_idx), 1.0)
                    if next_slow < 1.0:
                        break
                    st.next_chunk_index += 1
                    start_b = next_idx * st.spec.chunk_size_bytes
                    chunk_bytes = min(st.spec.chunk_size_bytes, st.spec.size_bytes - start_b)
                    if chunk_bytes <= 0:
                        st.completed_logical_chunks.add(next_idx)
                        break
                    current = _SimChunk(
                        chunk_id=f"{tid}-c{next_idx}",
                        logical_index=next_idx,
                        transfer_id=tid,
                        link_id=current.route[0],
                        route=list(current.route),
                        total_bytes=chunk_bytes,
                        remaining_bytes=float(chunk_bytes),
                        started_at=env.now,
                        startup_remaining_s=0.0,
                        slowdown_factor=next_slow,
                    )
                    st.active_chunks.append(current)

                ch.last_rate_bps = useful_spent / dt if dt > 0 else 0.0

            # Verify strict physical link capacity invariant on EVERY link in the graph.
            link_util_snapshot: dict[str, float] = {}
            for lid, link in links.items():
                w_bytes = wire_bytes_per_link.get(lid, 0.0)
                if w_bytes > link.capacity_bps * dt * 1.0001 + 1.0:
                    stats["capacity_violations"] += 1
                updated = self.monitor.update_link(
                    lid,
                    capacity_bps=link.capacity_bps,
                    latency_ms=link.latency_ms,
                    loss_rate=link.loss_rate,
                    measured_throughput_bps=w_bytes / dt if dt > 0 else 0.0,
                    active_transfers=len(active_tids_per_link.get(lid, ())),
                    requested_bps=link_raw_demand.get(lid, 0.0),
                )
                link_util_snapshot[lid] = round(updated.utilization, 4)

            # 5. Advance simulation clock, resolve completed chunks & transfers, record telemetry.
            yield env.timeout(dt)

            for st in active_list:
                self._resolve_completed_chunks(st, stats, useful_delta, redundant_delta)
                self._recompute_remaining(st)

                tid = st.spec.transfer_id
                route_rtt = sum(links[lid].latency_ms for lid in st.assigned_route if lid in links)
                route_cap = min(
                    (links[lid].capacity_bps for lid in st.assigned_route if lid in links),
                    default=0.0,
                )
                self.monitor.record_progress(
                    tid,
                    now=env.now,
                    dt=dt,
                    useful_bytes=useful_delta[tid],
                    raw_bytes=raw_delta[tid],
                    redundant_bytes=redundant_delta[tid],
                    remaining_bytes=st.remaining_bytes_total,
                    rtt_ms=route_rtt,
                    retries_delta=retries_delta[tid],
                    link_capacity_bps=route_cap,
                )

                if st.remaining_bytes_total <= 0 and len(st.completed_logical_chunks) >= st.total_chunks:
                    st.state = TransferState.COMPLETED
                    st.completed_at = env.now
                    st.remaining_bytes_total = 0
                    st.allocated_bps = 0.0

            timeline.append(
                {
                    "time_s": round(env.now, 3),
                    "active_transfers": float(
                        sum(1 for t in transfers.values() if t.state == TransferState.ACTIVE)
                    ),
                    "aggregate_goodput_bps": round(agg_goodput_tick, 2),
                    "scheduler_ms": round(decision.compute_time_ms, 4),
                    "remaining_bytes": {
                        tid: st.remaining_bytes_total for tid, st in transfers.items()
                    },
                    "allocated_bps": {
                        tid: round(st.allocated_bps, 2) for tid, st in transfers.items()
                    },
                    "link_utilization": link_util_snapshot,
                }
            )

    def _ensure_chunks_spawned(
        self,
        st: _SimTransfer,
        links: dict[str, LinkState],
        now: float,
        chunk_slowdowns: dict[tuple[str, int], float],
    ) -> None:
        primary_in_flight = [
            c for c in st.active_chunks if c.state == ChunkState.IN_FLIGHT and not c.is_replica
        ]
        while (
            len(primary_in_flight) < st.spec.max_concurrent_chunks
            and st.next_chunk_index < st.total_chunks
        ):
            idx = st.next_chunk_index
            st.next_chunk_index += 1
            start_b = idx * st.spec.chunk_size_bytes
            chunk_bytes = min(st.spec.chunk_size_bytes, st.spec.size_bytes - start_b)
            if chunk_bytes <= 0:
                st.completed_logical_chunks.add(idx)
                continue
            route = list(st.assigned_route)
            route_key = "->".join(route)
            route_rtt_s = sum(links[lid].latency_ms for lid in route if lid in links) / 1000.0
            startup = route_rtt_s if route_key not in st.connected_links else 0.0
            st.connected_links.add(route_key)
            ch = _SimChunk(
                chunk_id=f"{st.spec.transfer_id}-c{idx}",
                logical_index=idx,
                transfer_id=st.spec.transfer_id,
                link_id=route[0],
                route=route,
                total_bytes=chunk_bytes,
                remaining_bytes=float(chunk_bytes),
                started_at=now,
                startup_remaining_s=startup,
                slowdown_factor=chunk_slowdowns.get((st.spec.transfer_id, idx), 1.0),
            )
            st.active_chunks.append(ch)
            primary_in_flight.append(ch)

    def _run_straggler_check(
        self,
        now: float,
        active_list: list[_SimTransfer],
        links: dict[str, LinkState],
        stats: dict[str, int],
    ) -> None:
        views: list[ActiveChunkView] = []
        chunk_lookup: dict[str, tuple[_SimTransfer, _SimChunk]] = {}
        total_useful = 0
        total_redundant = 0

        for st in active_list:
            total_useful += st.spec.size_bytes - st.remaining_bytes_total
            total_redundant += int(st.redundant_bytes)
            in_flight_primary = [
                c for c in st.active_chunks if c.state == ChunkState.IN_FLIGHT and not c.is_replica
            ]
            per_chunk_alloc = st.allocated_bps / max(1, len(in_flight_primary))
            for ch in in_flight_primary:
                if ch.route != st.assigned_route or per_chunk_alloc <= 0:
                    continue
                views.append(
                    ActiveChunkView(
                        chunk_id=ch.chunk_id,
                        transfer_id=st.spec.transfer_id,
                        link_id=ch.link_id,
                        candidate_links=st.spec.candidate_links,
                        total_bytes=ch.total_bytes,
                        remaining_bytes=int(math.ceil(ch.remaining_bytes)),
                        started_at=ch.started_at,
                        observed_rate_bps=ch.last_rate_bps,
                        allocated_rate_bps=per_chunk_alloc,
                        already_replicated=ch.has_replica,
                    )
                )
                chunk_lookup[ch.chunk_id] = (st, ch)

        decisions = self.straggler_detector.evaluate(
            now=now,
            active_chunks=views,
            links=links,
            total_useful_bytes=total_useful,
            total_redundant_bytes=total_redundant,
        )
        for dec in decisions:
            if not dec.should_replicate or dec.replica_link is None:
                continue
            st, orig = chunk_lookup[dec.chunk_id]
            orig.has_replica = True
            alt_link = links[dec.replica_link]
            replica = _SimChunk(
                chunk_id=f"{orig.chunk_id}-rep",
                logical_index=orig.logical_index,
                transfer_id=st.spec.transfer_id,
                link_id=dec.replica_link,
                route=[dec.replica_link],
                total_bytes=orig.total_bytes,
                remaining_bytes=float(orig.total_bytes),
                started_at=now,
                startup_remaining_s=alt_link.latency_ms / 1000.0,
                slowdown_factor=1.0,
                is_replica=True,
                replica_of=orig.chunk_id,
            )
            st.active_chunks.append(replica)
            stats["replications_triggered"] += 1

    @staticmethod
    def _resolve_completed_chunks(
        st: _SimTransfer,
        stats: dict[str, int],
        useful_delta: dict[str, int],
        redundant_delta: dict[str, int],
    ) -> None:
        newly_completed_indices: set[int] = set()
        for ch in st.active_chunks:
            if ch.state != ChunkState.IN_FLIGHT:
                continue
            if ch.remaining_bytes <= 1e-6:
                ch.remaining_bytes = 0.0
                ch.state = ChunkState.COMPLETED
                if ch.logical_index not in st.completed_logical_chunks:
                    st.completed_logical_chunks.add(ch.logical_index)
                    newly_completed_indices.add(ch.logical_index)
                    if ch.is_replica:
                        stats["replications_won"] += 1
                        useful_delta[st.spec.transfer_id] += ch.total_bytes

        if newly_completed_indices:
            for ch in st.active_chunks:
                if ch.logical_index in newly_completed_indices and ch.state == ChunkState.IN_FLIGHT:
                    ch.state = ChunkState.FAILED
                    st.redundant_bytes += ch.transmitted_bytes
                    redundant_delta[st.spec.transfer_id] += int(round(ch.transmitted_bytes))

        st.active_chunks = [c for c in st.active_chunks if c.state == ChunkState.IN_FLIGHT]

    @staticmethod
    def _recompute_remaining(st: _SimTransfer) -> None:
        unspawned_bytes = 0
        for idx in range(st.next_chunk_index, st.total_chunks):
            start_b = idx * st.spec.chunk_size_bytes
            unspawned_bytes += max(0, min(st.spec.chunk_size_bytes, st.spec.size_bytes - start_b))

        in_flight_min: dict[int, float] = {}
        for ch in st.active_chunks:
            if ch.state != ChunkState.IN_FLIGHT or ch.logical_index in st.completed_logical_chunks:
                continue
            prev = in_flight_min.get(ch.logical_index)
            if prev is None or ch.remaining_bytes < prev:
                in_flight_min[ch.logical_index] = ch.remaining_bytes

        st.remaining_bytes_total = max(
            0, int(math.ceil(unspawned_bytes + sum(in_flight_min.values())))
        )

    def _to_snapshot(self, st: _SimTransfer) -> TransferSnapshot:
        measured, estimated = self.monitor.get_transfer_metrics(st.spec.transfer_id)
        return TransferSnapshot(
            transfer_id=st.spec.transfer_id,
            filename=st.spec.filename,
            url=f"sim://{st.spec.filename}",
            total_bytes=st.spec.size_bytes,
            remaining_bytes=st.remaining_bytes_total,
            state=st.state,
            direction=st.spec.direction,
            candidate_links=list(st.spec.candidate_links),
            assigned_link=st.assigned_link,
            route=list(st.assigned_route),
            candidate_routes=[list(r) for r in st.spec.candidate_routes],
            created_at=st.spec.arrival_time_s,
            started_at=st.started_at,
            completed_at=st.completed_at,
            allocated_bps=st.allocated_bps,
            active_chunks=len(st.active_chunks),
            active_workers=len(st.active_chunks),
            completed_chunks=len(st.completed_logical_chunks),
            total_chunks=st.total_chunks,
            priority_weight=st.spec.priority_weight,
            measured=measured,
            estimated=estimated,
        )
