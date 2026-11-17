"""Policy 1: Independent transfer baseline (greedy, uncoordinated transfers)."""

from __future__ import annotations

from flowweave.models import PolicyName, ScheduleDecision, SchedulerInput
from flowweave.scheduler.base import Scheduler, active_transfers, route_capacity, valid_routes_for


class IndependentScheduler(Scheduler):
    """Models independent transfers that ignore competing flows and shared bottlenecks.

    Each transfer selects its primary candidate route and greedily demands the full
    bottleneck capacity of that route without coordinating with competing transfers.
    """

    name = PolicyName.INDEPENDENT

    def _schedule(self, state: SchedulerInput) -> ScheduleDecision:
        active = active_transfers(state)
        allocations: dict[str, float] = {}
        assignments: dict[str, str] = {}
        routes: dict[str, list[str]] = {}

        for transfer in active:
            candidate_routes = valid_routes_for(transfer, state.links)
            if not candidate_routes:
                continue
            chosen = candidate_routes[0]
            routes[transfer.transfer_id] = chosen
            assignments[transfer.transfer_id] = chosen[0]
            allocations[transfer.transfer_id] = route_capacity(chosen, state.links)

        return ScheduleDecision(
            policy=self.name,
            timestamp=state.now,
            allocations_bps=allocations,
            link_assignments=assignments,
            route_assignments=routes,
            rationale=[f"Independent greedy demand across {len(allocations)} active transfers"],
        )
