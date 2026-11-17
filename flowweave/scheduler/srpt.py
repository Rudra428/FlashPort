"""Policy 3: Shortest Predicted Remaining Completion Time (SRPT) over multi-hop routes."""

from __future__ import annotations

from collections import defaultdict

from flowweave.models import PolicyName, ScheduleDecision, SchedulerInput
from flowweave.scheduler.base import (
    Scheduler,
    active_transfers,
    estimate_route_remaining_time,
    route_capacity,
    valid_routes_for,
)


class SRPTScheduler(Scheduler):
    """Prioritizes transfers with the shortest predicted remaining completion time.

    Supports both single-link and multi-hop directed routes, enforcing remaining link
    capacity along every hop of each prioritized transfer's route.
    """

    name = PolicyName.SRPT

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

        assignments: dict[str, str] = {}
        routes: dict[str, list[str]] = {}
        link_queued_bytes: dict[str, int] = defaultdict(int)

        # Assign multi-route transfers to the candidate route with earliest queued completion.
        for transfer in sorted(active, key=lambda t: (t.remaining_bytes, t.transfer_id)):
            candidate_routes = valid_routes_for(transfer, state.links)
            if not candidate_routes:
                continue
            best_route = min(
                candidate_routes,
                key=lambda r: max(
                    (link_queued_bytes.get(lid, 0) + transfer.remaining_bytes)
                    / max(route_capacity([lid], state.links, effective=True), 1.0)
                    for lid in r
                ),
            )
            routes[transfer.transfer_id] = best_route
            assignments[transfer.transfer_id] = best_route[0]
            for lid in best_route:
                link_queued_bytes[lid] += transfer.remaining_bytes

        # Allocate bandwidth strictly in ascending order of predicted remaining time.
        rem_cap: dict[str, float] = {lid: link.capacity_bps for lid, link in state.links.items()}
        ordered = sorted(
            [t for t in active if t.transfer_id in routes],
            key=lambda t: (
                estimate_route_remaining_time(t, routes[t.transfer_id], state.links),
                t.remaining_bytes,
                t.transfer_id,
            ),
        )

        # Count how many flows share each link so the last remaining flow gets full leftover cap.
        flows_remaining_on_link: dict[str, int] = defaultdict(int)
        for t in ordered:
            for lid in routes[t.transfer_id]:
                flows_remaining_on_link[lid] += 1

        allocations: dict[str, float] = {}
        for transfer in ordered:
            route = routes[transfer.transfer_id]
            avail = min(rem_cap[lid] for lid in route)
            if avail <= 0.0:
                allocations[transfer.transfer_id] = 0.0
            else:
                is_last_on_bottleneck = all(flows_remaining_on_link[lid] == 1 for lid in route)
                if is_last_on_bottleneck:
                    grant = avail
                else:
                    # Allocate enough to finish within this control tick (+15% margin).
                    needed_rate = (transfer.remaining_bytes / self.horizon_s) * 1.15
                    grant = min(avail, needed_rate)
                allocations[transfer.transfer_id] = grant
                for lid in route:
                    rem_cap[lid] = max(0.0, rem_cap[lid] - grant)
            for lid in route:
                flows_remaining_on_link[lid] -= 1

        top = ordered[0]
        return ScheduleDecision(
            policy=self.name,
            timestamp=state.now,
            allocations_bps=allocations,
            link_assignments=assignments,
            route_assignments=routes,
            rationale=[
                f"SRPT prioritized {top.filename} ({top.remaining_bytes / 1024:.0f} KB remaining)"
            ],
        )
