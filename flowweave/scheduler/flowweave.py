"""Policy 4: FlowWeave cooperative placement + convex bandwidth-allocation scheduler.

Pipeline on every scheduling tick:
1. Estimate effective link capacities C_eff = C * (1 - p) * h.
2. Run the cooperative marginal batch-delay route/link placement heuristic (discrete step).
3. Solve the continuous convex bandwidth-allocation problem min sum(R_i / b_i) subject to
   shared-link effective capacity constraints and optional minimum fairness floor
   (analytical KKT closed-form on independent single-hop links; CVXPY + CLARABEL on
   coupled multi-hop routes).
4. Validate allocations against link capacities and attach full optimization telemetry.
"""

from __future__ import annotations

from collections import defaultdict

from flowweave.models import LinkState, PolicyName, ScheduleDecision, SchedulerInput, TransferSnapshot
from flowweave.scheduler.base import (
    Scheduler,
    active_transfers,
    effective_capacity,
    route_latency_ms,
    valid_routes_for,
)
from flowweave.scheduler.optimizer import optimize_allocations


class FlowWeaveScheduler(Scheduler):
    """Cooperative placement paired with convex sum-FCT bandwidth optimization."""

    name = PolicyName.FLOWWEAVE

    def __init__(self, config=None, horizon_s: float = 0.25) -> None:
        super().__init__(config)
        self.horizon_s = max(horizon_s, 0.05)

    def _schedule(self, state: SchedulerInput) -> ScheduleDecision:
        active = active_transfers(state)
        if not active:
            return ScheduleDecision(
                policy=self.name,
                timestamp=state.now,
                allocations_bps={},
                link_assignments={},
                route_assignments={},
                notes={
                    "optimization_status": "empty_no_active_transfers",
                    "solver_name": "none",
                    "solve_time_ms": 0.0,
                    "fairness_fraction": self.config.min_fair_share_fraction,
                    "predicted_completion_times_s": {},
                    "effective_capacities_bps": {},
                    "link_utilization": {},
                },
            )

        # Step 1: Effective link capacities C_eff = C * (1 - p) * h.
        usable_cap: dict[str, float] = {
            lid: effective_capacity(
                link,
                congestion_headroom=self.config.congestion_headroom,
            )
            for lid, link in state.links.items()
        }

        # Step 2: Cooperative discrete route/link placement minimizing marginal batch sum(T_i).
        routes, assignments = self._assign_routes(active, state.links, usable_cap)

        # Step 3: Continuous convex sum-FCT bandwidth allocation on the placed routes.
        fair_frac = (
            self.config.min_fair_share_fraction
            if self.config.min_fair_share_fraction > 0.0
            else None
        )
        opt = optimize_allocations(
            active,
            state.links,
            routes=routes,
            fairness_fraction=fair_frac,
            congestion_headroom=self.config.congestion_headroom,
            allow_striping=False,
        )

        pred_clean = {
            tid: (round(val, 4) if val != float("inf") else None)
            for tid, val in opt.predicted_completion_times_s.items()
        }
        congested_links = [lid for lid, link in state.links.items() if link.is_congested]

        rationale = [
            (
                f"Convex allocation ({opt.solver_name}, {opt.status}, {opt.solve_time_ms:.2f} ms) "
                f"across {len(active)} transfers; predicted ΣT_i = {opt.objective_value_s:.2f}s"
                if opt.objective_value_s is not None
                else f"Convex allocation ({opt.solver_name}, {opt.status})"
            )
        ]
        if congested_links:
            rationale.append(f"Applied congestion headroom backoff on: {', '.join(congested_links)}")
        if opt.warnings:
            rationale.extend(opt.warnings)

        return ScheduleDecision(
            policy=self.name,
            timestamp=state.now,
            allocations_bps={tid: opt.per_transfer_bps.get(tid, 0.0) for tid in routes},
            link_assignments=assignments,
            route_assignments=routes,
            rationale=rationale,
            notes={
                "optimization_status": opt.status,
                "solver_name": opt.solver_name,
                "solve_time_ms": round(opt.solve_time_ms, 4),
                "objective_value_s": (
                    round(opt.objective_value_s, 4) if opt.objective_value_s is not None else None
                ),
                "fairness_fraction": fair_frac,
                "mode": opt.mode,
                "predicted_completion_times_s": pred_clean,
                "effective_capacities_bps": {
                    lid: round(cap, 2) for lid, cap in opt.effective_capacities_bps.items()
                },
                "link_utilization": {
                    lid: round(u, 4) for lid, u in opt.link_utilization.items()
                },
                "congested_links": congested_links,
                "active_transfers": len(active),
                "uploads": sum(1 for t in active if t.direction.value == "upload"),
                "downloads": sum(1 for t in active if t.direction.value == "download"),
                "warnings": list(opt.warnings),
            },
        )

    def _assign_routes(
        self,
        active: list[TransferSnapshot],
        links: dict[str, LinkState],
        usable_cap: dict[str, float],
    ) -> tuple[dict[str, list[str]], dict[str, str]]:
        routes: dict[str, list[str]] = {}
        assignments: dict[str, str] = {}
        link_work_bytes: dict[str, list[int]] = defaultdict(list)

        # Largest-first balances makespan across links and populates behind_count accurately.
        ordered = sorted(active, key=lambda t: (-t.remaining_bytes, t.transfer_id))
        for transfer in ordered:
            candidate_routes = valid_routes_for(transfer, links)
            if not candidate_routes:
                continue
            if len(candidate_routes) == 1:
                chosen = candidate_routes[0]
            else:
                chosen = min(
                    candidate_routes,
                    key=lambda r: self._marginal_route_cost(
                        transfer.remaining_bytes, r, link_work_bytes, links, usable_cap
                    ),
                )
            routes[transfer.transfer_id] = chosen
            assignments[transfer.transfer_id] = chosen[0]
            for lid in chosen:
                link_work_bytes[lid].append(transfer.remaining_bytes)

        return routes, assignments

    @staticmethod
    def _marginal_route_cost(
        rem_bytes: int,
        route: list[str],
        link_work_bytes: dict[str, list[int]],
        links: dict[str, LinkState],
        usable_cap: dict[str, float],
    ) -> float:
        """Marginal increase in sum(T_i) from placing `rem_bytes` onto `route`."""
        hop_costs = []
        for lid in route:
            existing = link_work_bytes.get(lid, ())
            cap = usable_cap[lid]
            ahead = sum(w for w in existing if w <= rem_bytes)
            behind_count = sum(1 for w in existing if w > rem_bytes)
            hop_costs.append((ahead + rem_bytes) / cap + behind_count * (rem_bytes / cap))
        rtt_penalty = route_latency_ms(route, links) / 1000.0
        loss_penalty = sum(links[lid].loss_rate for lid in route) * 2.0
        return max(hop_costs) + rtt_penalty + loss_penalty
