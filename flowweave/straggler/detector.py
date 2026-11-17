"""Cost-benefit straggler detection and chunk replication decision logic."""

from __future__ import annotations

from dataclasses import dataclass
from statistics import median

from flowweave.config import StragglerConfig
from flowweave.models import LinkState, StragglerDecision
from flowweave.scheduler.base import effective_capacity


@dataclass
class ActiveChunkView:
    """Snapshot of an in-flight chunk inspected by the straggler detector."""

    chunk_id: str
    transfer_id: str
    link_id: str
    candidate_links: list[str]
    total_bytes: int
    remaining_bytes: int
    started_at: float
    observed_rate_bps: float
    allocated_rate_bps: float = 0.0
    already_replicated: bool = False


class StragglerDetector:
    """Identifies slow chunks and decides whether speculative replication is net-beneficial."""

    def __init__(self, config: StragglerConfig | None = None) -> None:
        self.config = config or StragglerConfig()

    def evaluate(
        self,
        *,
        now: float,
        active_chunks: list[ActiveChunkView],
        links: dict[str, LinkState],
        total_useful_bytes: int = 0,
        total_redundant_bytes: int = 0,
    ) -> list[StragglerDecision]:
        """Evaluate all in-flight chunks and return straggler replication decisions."""
        if not self.config.enabled or not active_chunks:
            return []

        # Compute sibling chunk median rate per transfer (only compare within the same transfer,
        # because different transfers intentionally receive different scheduler allocations).
        rates_by_transfer: dict[str, list[float]] = {}
        for ch in active_chunks:
            if ch.observed_rate_bps > 0:
                rates_by_transfer.setdefault(ch.transfer_id, []).append(ch.observed_rate_bps)

        decisions: list[StragglerDecision] = []

        # Track cumulative redundant bytes if multiple chunks replicate in the same tick.
        projected_redundant = total_redundant_bytes
        budget_denom = max(total_useful_bytes, sum(c.total_bytes for c in active_chunks), 1)

        for chunk in active_chunks:
            age = now - chunk.started_at
            if chunk.already_replicated or chunk.remaining_bytes <= 0 or age < self.config.min_chunk_age_s:
                continue

            peer_rates = rates_by_transfer.get(chunk.transfer_id, [])
            sibling_median = median(peer_rates) if len(peer_rates) >= 2 else 0.0

            # Expected rate is what the scheduler allocated to this chunk (or sibling median
            # if allocation wasn't provided). An intentionally paced large flow is NOT a straggler.
            orig_link = links.get(chunk.link_id)
            loss_factor = max(0.1, 1.0 - orig_link.loss_rate) if orig_link else 1.0
            if chunk.allocated_rate_bps > 0:
                expected_rate = chunk.allocated_rate_bps * loss_factor
                if sibling_median > 0:
                    expected_rate = max(expected_rate, sibling_median)
            elif sibling_median > 0:
                expected_rate = sibling_median
            elif orig_link is not None:
                expected_rate = effective_capacity(orig_link) / max(1, orig_link.active_transfers)
            else:
                continue

            if expected_rate <= 0:
                continue

            is_slow = chunk.observed_rate_bps < expected_rate * self.config.slow_ratio_threshold
            if not is_slow:
                continue

            decision = self._decide_replication(
                chunk=chunk,
                links=links,
                projected_redundant=projected_redundant,
                budget_denom=budget_denom,
            )
            decisions.append(decision)
            if decision.should_replicate:
                projected_redundant += chunk.remaining_bytes

        return decisions

    def _decide_replication(
        self,
        *,
        chunk: ActiveChunkView,
        links: dict[str, LinkState],
        projected_redundant: int,
        budget_denom: int,
    ) -> StragglerDecision:
        alternates = [
            lid for lid in chunk.candidate_links if lid != chunk.link_id and lid in links
        ]
        if not alternates:
            return StragglerDecision(
                transfer_id=chunk.transfer_id,
                chunk_id=chunk.chunk_id,
                original_link=chunk.link_id,
                replica_link=None,
                should_replicate=False,
                expected_saving_s=0.0,
                expected_cost_s=0.0,
                reason="no_alternate_link",
            )

        # Enforce global redundant traffic budget before evaluating paths.
        if (projected_redundant + chunk.remaining_bytes) / budget_denom > self.config.max_redundant_ratio:
            return StragglerDecision(
                transfer_id=chunk.transfer_id,
                chunk_id=chunk.chunk_id,
                original_link=chunk.link_id,
                replica_link=None,
                should_replicate=False,
                expected_saving_s=0.0,
                expected_cost_s=0.0,
                reason="redundant_budget_exhausted",
            )

        orig_rate = max(chunk.observed_rate_bps, 1024.0)
        t_orig = chunk.remaining_bytes / orig_rate

        best_link: str | None = None
        best_net = float("-inf")
        best_saving = 0.0
        best_cost = 0.0

        for lid in alternates:
            alt = links[lid]
            alt_eff_cap = effective_capacity(alt)
            # Estimated rate available to the replica if added to alt link
            est_alt_rate = alt_eff_cap / max(1, alt.active_transfers + 1)
            t_replica = (alt.latency_ms / 1000.0) + (chunk.remaining_bytes / max(est_alt_rate, 1.0))
            saving = t_orig - t_replica

            # Congestion externality: delaying co-located flows currently using alt link
            colocated = alt.active_transfers
            cost = (
                self.config.congestion_cost_weight
                * colocated
                * (chunk.remaining_bytes / max(alt_eff_cap, 1.0))
                * max(0.25, alt.utilization)
            )
            net = saving - cost
            if net > best_net:
                best_net = net
                best_link = lid
                best_saving = saving
                best_cost = cost

        if best_link is not None and best_net >= self.config.min_net_saving_s:
            return StragglerDecision(
                transfer_id=chunk.transfer_id,
                chunk_id=chunk.chunk_id,
                original_link=chunk.link_id,
                replica_link=best_link,
                should_replicate=True,
                expected_saving_s=best_saving,
                expected_cost_s=best_cost,
                reason="net_positive_replication",
            )

        return StragglerDecision(
            transfer_id=chunk.transfer_id,
            chunk_id=chunk.chunk_id,
            original_link=chunk.link_id,
            replica_link=best_link,
            should_replicate=False,
            expected_saving_s=best_saving,
            expected_cost_s=best_cost,
            reason="cost_exceeds_benefit",
        )
