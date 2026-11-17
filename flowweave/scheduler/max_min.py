"""Policy 2: Progressive max-min fair scheduling over directed multi-hop link graphs."""

from __future__ import annotations

from collections import defaultdict

from flowweave.models import PolicyName, ScheduleDecision, SchedulerInput
from flowweave.scheduler.base import Scheduler, active_transfers, route_capacity, valid_routes_for


class MaxMinFairScheduler(Scheduler):
    """Allocates max-min fair rates across single-hop and multi-hop shared links.

    For multi-route transfers, picks the route offering the highest projected per-flow
    bottleneck share, then runs exact progressive water-filling over the directed link
    graph so every shared link respects sum_{i: j in P_i} b_i <= C_j.
    """

    name = PolicyName.MAX_MIN

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

        assignments: dict[str, str] = {}
        routes: dict[str, list[str]] = {}
        link_counts: dict[str, int] = defaultdict(int)

        # Select candidate route with highest projected fair share at its tightest hop.
        for transfer in active:
            candidate_routes = valid_routes_for(transfer, state.links)
            if not candidate_routes:
                continue
            best_route = max(
                candidate_routes,
                key=lambda r: min(
                    state.links[lid].capacity_bps / (link_counts.get(lid, 0) + 1)
                    for lid in r
                ),
            )
            routes[transfer.transfer_id] = best_route
            assignments[transfer.transfer_id] = best_route[0]
            for lid in best_route:
                link_counts[lid] += 1

        # Progressive water-filling across the directed graph of links.
        rem_cap: dict[str, float] = {lid: link.capacity_bps for lid, link in state.links.items()}
        unfrozen: set[str] = set(routes.keys())
        allocations: dict[str, float] = {}

        while unfrozen:
            flows_on_link: dict[str, list[str]] = defaultdict(list)
            for tid in unfrozen:
                for lid in routes[tid]:
                    flows_on_link[lid].append(tid)

            bottleneck_link = min(
                flows_on_link.keys(),
                key=lambda lid: rem_cap[lid] / len(flows_on_link[lid]),
            )
            share = max(0.0, rem_cap[bottleneck_link] / len(flows_on_link[bottleneck_link]))

            # Freeze every flow traversing the tightest bottleneck link at its fair share.
            to_freeze = list(flows_on_link[bottleneck_link])
            for tid in to_freeze:
                allocations[tid] = min(share, route_capacity(routes[tid], state.links))
                for lid in routes[tid]:
                    rem_cap[lid] = max(0.0, rem_cap[lid] - allocations[tid])
                unfrozen.remove(tid)

        return ScheduleDecision(
            policy=self.name,
            timestamp=state.now,
            allocations_bps=allocations,
            link_assignments=assignments,
            route_assignments=routes,
            rationale=[f"Max-min water-filling across {len(allocations)} active transfers"],
        )
