from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable
import math

from flashport.config import SchedulerConfig
from flashport.core.models import TransferState


class Scheduler(ABC):
    """Abstract scheduler interface used throughout the simulation and experiments."""

    name = "base"

    def __init__(self, config: SchedulerConfig | None = None) -> None:
        self.config = config or SchedulerConfig()

    @abstractmethod
    def allocate(
        self,
        transfers: Iterable[TransferState],
        total_capacity: float | None = None,
    ) -> dict[str, float]:
        """Return a mapping of transfer_id -> allocated throughput in bytes/s."""


class BaselineScheduler(Scheduler):
    """Base class with common helpers for the allocation logic."""

    def _active(self, transfers: Iterable[TransferState]) -> list[TransferState]:
        return [transfer for transfer in transfers if transfer.state != "completed"]

    def _capacity(self, total_capacity: float | None) -> float:
        capacity = self.config.total_capacity if total_capacity is None else total_capacity
        if not math.isfinite(capacity) or capacity < 0:
            raise ValueError("total_capacity must be finite and non-negative")
        return capacity

    def _normalize(self, weights: dict[str, float], total_capacity: float) -> dict[str, float]:
        total_weight = sum(weights.values()) or 1.0
        allocations = {}
        for transfer_id, weight in weights.items():
            allocations[transfer_id] = total_capacity * (weight / total_weight)
        return allocations
