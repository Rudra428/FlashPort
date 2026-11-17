"""Common scheduler interface, multi-hop route helpers, and link-capacity validator."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from collections import defaultdict

from flowweave.config import SchedulerConfig
from flowweave.models import (
    LinkState,
    PolicyName,
    ScheduleDecision,
    SchedulerInput,
    TransferSnapshot,
    TransferState,
)


class Scheduler(ABC):
    """Base interface implemented by every FlowWeave scheduling policy."""

    name: PolicyName

    def __init__(self, config: SchedulerConfig | None = None) -> None:
        self.config = config or SchedulerConfig()

    def decide(self, state: SchedulerInput) -> ScheduleDecision:
        """Execute scheduling logic and measure wall-clock compute overhead."""
        t0 = time.perf_counter()
        decision = self._schedule(state)
        decision.compute_time_ms = (time.perf_counter() - t0) * 1000.0
        # Ensure route_assignments is always populated from link_assignments when omitted.
        if not decision.route_assignments and decision.link_assignments:
            decision.route_assignments = {
                tid: [lid] for tid, lid in decision.link_assignments.items()
            }
        return decision

    @abstractmethod
    def _schedule(self, state: SchedulerInput) -> ScheduleDecision:
        """Compute per-transfer rate allocations and route/link assignments."""


def active_transfers(state: SchedulerInput) -> list[TransferSnapshot]:
    """Return transfers currently eligible for bandwidth allocation (excludes PAUSED/COMPLETED)."""
    return [
        t
        for t in state.transfers
        if t.state in (TransferState.ACTIVE, TransferState.PENDING) and t.remaining_bytes > 0
    ]


def valid_routes_for(transfer: TransferSnapshot, links: dict[str, LinkState]) -> list[list[str]]:
    """Return all candidate directed routes whose constituent links exist in the graph."""
    routes: list[list[str]] = []
    for route in transfer.candidate_routes:
        if route and all(lid in links for lid in route):
            routes.append(list(route))
    if routes:
        return routes
    if transfer.route and all(lid in links for lid in transfer.route):
        return [list(transfer.route)]
    for lid in transfer.candidate_links:
        if lid in links:
            routes.append([lid])
    if routes:
        return routes
    if transfer.assigned_link in links:
        return [[transfer.assigned_link]]
    first = next(iter(links.keys()), None)
    return [[first]] if first else []


def valid_links_for(transfer: TransferSnapshot, links: dict[str, LinkState]) -> list[str]:
    """Return single-hop candidate link IDs present in the current topology."""
    return [r[0] for r in valid_routes_for(transfer, links)]


def effective_capacity(link: LinkState, *, congestion_headroom: float = 1.0) -> float:
    """Usable goodput capacity accounting for packet loss and optional congestion headroom."""
    loss_factor = max(0.05, 1.0 - link.loss_rate)
    headroom = congestion_headroom if link.is_congested else 1.0
    return max(1.0, link.capacity_bps * loss_factor * headroom)


def route_capacity(
    route: list[str],
    links: dict[str, LinkState],
    *,
    effective: bool = False,
    congestion_headroom: float = 1.0,
) -> float:
    """Bottleneck capacity along a multi-hop directed route P_i."""
    if not route:
        return 0.0
    if effective:
        return min(
            effective_capacity(links[lid], congestion_headroom=congestion_headroom)
            for lid in route
        )
    return min(links[lid].capacity_bps for lid in route)


def route_latency_ms(route: list[str], links: dict[str, LinkState]) -> float:
    return sum(links[lid].latency_ms for lid in route)


def estimate_remaining_time(transfer: TransferSnapshot, link: LinkState) -> float:
    """Predict remaining completion time if `transfer` is scheduled on `link`."""
    return estimate_route_remaining_time(transfer, [link.link_id], {link.link_id: link})


def estimate_route_remaining_time(
    transfer: TransferSnapshot,
    route: list[str],
    links: dict[str, LinkState],
) -> float:
    """Predict remaining completion time along a multi-hop route P_i.

    Uses the route's achievable effective capacity rather than a starved flow's historical
    goodput, preventing priority inversion when a paused flow's EWMA decays toward zero.
    """
    if transfer.remaining_bytes <= 0:
        return 0.0
    cap = route_capacity(route, links, effective=True)
    est_rate = transfer.estimated.ewma_goodput_bps
    if (
        transfer.allocated_bps > 0
        and transfer.estimated.confidence >= 0.75
        and est_rate > 0.1 * transfer.allocated_bps
    ):
        rate = min(cap, max(est_rate, cap * 0.5))
    else:
        rate = cap
    return transfer.remaining_bytes / max(rate, 1.0) + (route_latency_ms(route, links) / 1000.0)


def validate_and_enforce_allocations(
    allocations: dict[str, float],
    routes: dict[str, list[str]],
    links: dict[str, LinkState],
    active_ids: set[str],
) -> dict[str, float]:
    """Enforce non-negativity, inactive-zero, and sum_{i: j in P_i} b_i <= C_j on every link."""
    enforced: dict[str, float] = {}
    for tid, rate in allocations.items():
        if tid not in active_ids or tid not in routes or not routes[tid]:
            enforced[tid] = 0.0
        else:
            enforced[tid] = max(0.0, float(rate))

    # Iteratively scale down flows on any oversubscribed link until all links satisfy C_j.
    for _ in range(len(links) + 2):
        link_flows: dict[str, list[str]] = defaultdict(list)
        for tid, rate in enforced.items():
            if rate <= 0.0:
                continue
            for lid in routes.get(tid, ()):
                if lid in links:
                    link_flows[lid].append(tid)

        worst_link: str | None = None
        worst_scale = 1.0
        for lid, tids in link_flows.items():
            total = sum(enforced[tid] for tid in tids)
            cap = max(0.0, links[lid].capacity_bps)
            if total > cap * (1.0 + 1e-9) and total > 0:
                scale = cap / total
                if scale < worst_scale:
                    worst_scale = scale
                    worst_link = lid

        if worst_link is None:
            break
        for tid in link_flows[worst_link]:
            enforced[tid] *= worst_scale

    return enforced
