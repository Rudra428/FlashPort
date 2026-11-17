"""Policy 4: FlowWeave cooperative, congestion-aware bidirectional scheduler.

Combines three well-understood scheduling principles across uploads, downloads, and
multi-hop directed link graphs:
1. Cooperative route placement: assigns multi-path transfers largest-first onto the route
   minimizing marginal increase in total batch completion time sum(T_i), naturally
   offloading shared bottlenecks onto underutilized links.
2. Congestion-aware capacity budgeting: scales link budgets by congestion_headroom only
   when real congestion or elevated packet loss is observed, and respects separate upload
   (uplink), download (downlink), and shared link constraints independently.
3. Fairness-guarded urgency-weighted SRPT: reserves a max-min fairness floor on every link
   so large uploads and downloads never starve, then distributes remaining cooperative
   capacity in urgency order (remaining work, aging anti-starvation, priority weight),
   subtracting grants across all hops of each transfer's route and logging rationale.
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


class FlowWeaveScheduler(Scheduler):
    """Cooperative, congestion-aware scheduler minimizing sum(T_i) with fairness guards."""

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
            )

        usable_cap: dict[str, float] = {
            lid: effective_capacity(
                link,
                congestion_headroom=self.config.congestion_headroom,
            )
            for lid, link in state.links.items()
        }
        wire_budget: dict[str, float] = {
            lid: link.capacity_bps
            * (self.config.congestion_headroom if link.is_congested else 1.0)
            for lid, link in state.links.items()
        }

        # Step 1: Cooperative route selection minimizing marginal batch sum(T_i).
        routes, assignments = self._assign_routes(active, state.links, usable_cap)

        # Step 2: Check whether all routes are single-hop (fast exact per-link path)
        #         or multi-hop directed graph (general graph path).
        is_multihop = any(len(r) > 1 for r in routes.values())
        if not is_multihop:
            allocations = self._allocate_single_hop(active, routes, state.links, wire_budget, state.now)
        else:
            allocations = self._allocate_multihop(active, routes, state.links, wire_budget, state.now)

        congested_links = [lid for lid, link in state.links.items() if link.is_congested]
        rationale = self._build_rationale(active, allocations, congested_links)

        return ScheduleDecision(
            policy=self.name,
            timestamp=state.now,
            allocations_bps=allocations,
            link_assignments=assignments,
            route_assignments=routes,
            rationale=rationale,
            notes={
                "congested_links": congested_links,
                "active_transfers": len(active),
                "uploads": sum(1 for t in active if t.direction.value == "upload"),
                "downloads": sum(1 for t in active if t.direction.value == "download"),
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

    def _urgency_score(self, transfer: TransferSnapshot, now: float) -> float:
        work_floor = max(float(transfer.remaining_bytes), 65_536.0)
        wait_s = max(0.0, now - (transfer.started_at if transfer.started_at is not None else transfer.created_at))
        aging = 1.0 + self.config.starvation_aging_per_s * wait_s
        return (transfer.priority_weight * aging) / (work_floor ** self.config.srpt_exponent)

    def _allocate_single_hop(
        self,
        active: list[TransferSnapshot],
        routes: dict[str, list[str]],
        links: dict[str, LinkState],
        wire_budget: dict[str, float],
        now: float,
    ) -> dict[str, float]:
        per_link: dict[str, list[TransferSnapshot]] = defaultdict(list)
        for t in active:
            if t.transfer_id in routes:
                per_link[routes[t.transfer_id][0]].append(t)

        allocations: dict[str, float] = {}
        for link_id, group in per_link.items():
            allocations.update(
                self._allocate_on_link(group, links[link_id], wire_budget[link_id], now)
            )
        return allocations

    def _allocate_on_link(
        self,
        group: list[TransferSnapshot],
        link: LinkState,
        cap_bps: float,
        now: float,
    ) -> dict[str, float]:
        n = len(group)
        if n == 1:
            return {group[0].transfer_id: cap_bps}

        floor_budget = cap_bps * min(max(self.config.min_fair_share_fraction, 0.0), 0.9)
        base_share = max(min(self.config.min_rate_bps, cap_bps / n), floor_budget / n)
        allocs: dict[str, float] = {}
        used = 0.0
        for t in group:
            max_useful = max(self.config.min_rate_bps, t.remaining_bytes / self.horizon_s)
            grant = min(base_share, max_useful)
            allocs[t.transfer_id] = grant
            used += grant

        pool = max(0.0, cap_bps - used)
        if pool <= 1e-6:
            return allocs

        scores = {t.transfer_id: self._urgency_score(t, now) for t in group}
        ordered = sorted(group, key=lambda t: (-scores[t.transfer_id], t.remaining_bytes, t.transfer_id))

        waterfall_budget = pool * 0.70
        proportional_budget = pool - waterfall_budget

        for idx, t in enumerate(ordered):
            if waterfall_budget <= 1e-6:
                break
            max_useful = max(
                0.0,
                ((t.remaining_bytes / self.horizon_s) * 1.15) - allocs[t.transfer_id],
            )
            grant = waterfall_budget if idx == len(ordered) - 1 else min(waterfall_budget, max_useful)
            allocs[t.transfer_id] += grant
            waterfall_budget -= grant

        proportional_budget += waterfall_budget
        if proportional_budget > 1e-6:
            total_score = sum(scores.values())
            for t in ordered:
                share = (
                    proportional_budget * (scores[t.transfer_id] / total_score)
                    if total_score > 0
                    else proportional_budget / n
                )
                allocs[t.transfer_id] += share

        total_alloc = sum(allocs.values())
        if total_alloc > link.capacity_bps and total_alloc > 0:
            scale = link.capacity_bps / total_alloc
            for tid in allocs:
                allocs[tid] *= scale
        return allocs

    def _allocate_multihop(
        self,
        active: list[TransferSnapshot],
        routes: dict[str, list[str]],
        links: dict[str, LinkState],
        wire_budget: dict[str, float],
        now: float,
    ) -> dict[str, float]:
        """Graph-wide fairness floor + urgency-weighted SRPT respecting every shared hop."""
        eligible = [t for t in active if t.transfer_id in routes]
        rem_cap = dict(wire_budget)
        allocs: dict[str, float] = {t.transfer_id: 0.0 for t in eligible}

        # 1. Fairness floor across every link
        link_flow_counts: dict[str, int] = defaultdict(int)
        for t in eligible:
            for lid in routes[t.transfer_id]:
                link_flow_counts[lid] += 1

        fair_frac = min(max(self.config.min_fair_share_fraction, 0.0), 0.9)
        for t in eligible:
            route = routes[t.transfer_id]
            floor_rate = min(
                (wire_budget[lid] * fair_frac) / max(1, link_flow_counts[lid]) for lid in route
            )
            grant = min(floor_rate, min(rem_cap[lid] for lid in route))
            allocs[t.transfer_id] += grant
            for lid in route:
                rem_cap[lid] = max(0.0, rem_cap[lid] - grant)

        # 2. Urgency-ordered waterfall over remaining graph capacity
        scores = {t.transfer_id: self._urgency_score(t, now) for t in eligible}
        ordered = sorted(eligible, key=lambda t: (-scores[t.transfer_id], t.remaining_bytes, t.transfer_id))

        for idx, t in enumerate(ordered):
            route = routes[t.transfer_id]
            avail = min(rem_cap[lid] for lid in route)
            if avail <= 1e-6:
                continue
            needed = max(0.0, (t.remaining_bytes / self.horizon_s) * 1.15 - allocs[t.transfer_id])
            grant = avail if idx == len(ordered) - 1 else min(avail, needed)
            allocs[t.transfer_id] += grant
            for lid in route:
                rem_cap[lid] = max(0.0, rem_cap[lid] - grant)

        # 3. Spill any leftover capacity proportionally so independent/fast links stay saturated
        for t in ordered:
            route = routes[t.transfer_id]
            avail = min(rem_cap[lid] for lid in route)
            if avail <= 1e-6:
                continue
            allocs[t.transfer_id] += avail
            for lid in route:
                rem_cap[lid] = max(0.0, rem_cap[lid] - avail)

        return allocs

    @staticmethod
    def _build_rationale(
        active: list[TransferSnapshot],
        allocations: dict[str, float],
        congested_links: list[str],
    ) -> list[str]:
        reasons: list[str] = []
        ranked = sorted(active, key=lambda t: -allocations.get(t.transfer_id, 0.0))
        if ranked:
            top = ranked[0]
            rate_mb = allocations.get(top.transfer_id, 0.0) / (1024 * 1024)
            reasons.append(
                f"Prioritized {top.direction.value} '{top.filename}' ({top.remaining_bytes / 1024:.0f} KB left) at {rate_mb:.2f} MB/s"
            )
        if len(ranked) > 1:
            low = ranked[-1]
            low_mb = allocations.get(low.transfer_id, 0.0) / (1024 * 1024)
            reasons.append(
                f"Protected '{low.filename}' with fairness floor at {low_mb:.2f} MB/s to prevent starvation"
            )
        if congested_links:
            reasons.append(f"Applied congestion headroom backoff on: {', '.join(congested_links)}")
        return reasons
