from __future__ import annotations

from collections.abc import Iterable

from flashport.config import SchedulerConfig
from flashport.core.models import TransferState
from flashport.scheduler.base import BaselineScheduler


class SRPTScheduler(BaselineScheduler):
    """Shortest Remaining Size Priority baseline.

    Assumptions: each transfer's remaining bytes are the best available proxy for job size,
    and observed completion time under a single bottleneck is improved by prioritizing smaller
    transfers. Limitations: ignores fairness and can repeatedly starve large transfers.
    """

    name = "srpt"

    def __init__(self, config: SchedulerConfig | None = None) -> None:
        super().__init__(config)

    def allocate(
        self,
        transfers: Iterable[TransferState],
        total_capacity: float | None = None,
    ) -> dict[str, float]:
        active = sorted(self._active(transfers), key=lambda transfer: transfer.remaining)
        if not active:
            return {}

        capacity = self._capacity(total_capacity)
        # Strict preemptive SRPT serves the smallest remaining flow first.
        # Ties retain the stable input order.
        allocations = {transfer.transfer_id: 0.0 for transfer in active}
        allocations[active[0].transfer_id] = capacity
        return allocations
