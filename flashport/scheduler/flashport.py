from __future__ import annotations

from collections.abc import Iterable
from statistics import median

from flashport.config import SchedulerConfig
from flashport.core.models import TransferState
from flashport.scheduler.allocator import allocate_by_weights, clamp, compute_service_deficit
from flashport.scheduler.base import BaselineScheduler


class FlashportScheduler(BaselineScheduler):
    """Experimental workload-aware weighted fair-share scheduler.

    This heuristic is intentionally simple and bounded. It combines size awareness,
    recent service deficit, and fairness into a priority score. It is not a mathematically
    optimal policy; it is a reproducible prototype for experimentation and comparison.
    """

    name = "flashport"

    def __init__(self, config: SchedulerConfig | None = None) -> None:
        super().__init__(config)

    def allocate(
        self,
        transfers: Iterable[TransferState],
        total_capacity: float | None = None,
    ) -> dict[str, float]:
        active = self._active(transfers)
        if not active:
            return {}

        capacity = self._capacity(total_capacity)
        if capacity <= 0:
            return {transfer.transfer_id: 0.0 for transfer in active}

        median_remaining = median(float(transfer.remaining) for transfer in active)

        weights: dict[str, float] = {}
        for transfer in active:
            size_priority = clamp(
                (median_remaining / max(float(transfer.remaining), self.config.chunk_size)) ** 0.5,
                self.config.min_size_weight,
                self.config.max_size_weight,
            )
            expected_service = max(
                capacity / len(active) * self.config.fair_share_bias,
                self.config.min_rate,
            )
            samples = transfer.service_history[-self.config.service_window :]
            delivered = (
                sum(samples) / len(samples)
                if samples
                else max(transfer.goodput, 0.0)
            )
            deficit_factor = clamp(
                compute_service_deficit(delivered, expected_service),
                1.0,
                self.config.max_deficit_multiplier,
            )
            priority = clamp(size_priority * deficit_factor, self.config.min_priority, self.config.max_priority)
            weights[transfer.transfer_id] = priority

        minimums = {
            transfer.transfer_id: self.config.min_rate
            for transfer in active
        }
        allocations = allocate_by_weights(weights, capacity, minimums)
        return {
            transfer.transfer_id: max(0.0, allocations.get(transfer.transfer_id, 0.0))
            for transfer in active
        }
