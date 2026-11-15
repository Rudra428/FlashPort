from __future__ import annotations

from collections.abc import Iterable

from flashport.config import SchedulerConfig
from flashport.core.models import TransferState
from flashport.scheduler.base import BaselineScheduler


class IndependentScheduler(BaselineScheduler):
    """Approximate conventional baseline without cooperative application rate control.

    A single bottleneck cannot simulate independent TCP congestion control directly. For this
    abstract comparison, assume long-lived flows with similar RTTs converge to equal shares.
    Under that assumption this baseline intentionally matches EqualShareScheduler; it is not
    evidence that real uncoordinated transfers always behave this way.
    """

    name = "independent"

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
        per_transfer = capacity / len(active)
        return {transfer.transfer_id: max(0.0, per_transfer) for transfer in active}
