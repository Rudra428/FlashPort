from __future__ import annotations

from dataclasses import dataclass, field
import math
from typing import Callable


@dataclass(frozen=True)
class SchedulerConfig:
    """Configuration parameters for the cooperative scheduling heuristics."""

    total_capacity: float = 100.0
    chunk_size: int = 1_048_576
    min_rate: float = 1.0
    allocation_interval: float = 1.0
    service_window: int = 5
    min_size_weight: float = 0.5
    max_size_weight: float = 2.0
    max_priority: float = 4.0
    min_priority: float = 0.5
    fair_share_bias: float = 1.0
    max_deficit_multiplier: float = 2.0

    def __post_init__(self) -> None:
        numeric_values = (
            self.total_capacity,
            self.min_rate,
            self.allocation_interval,
            self.min_size_weight,
            self.max_size_weight,
            self.max_priority,
            self.min_priority,
            self.fair_share_bias,
            self.max_deficit_multiplier,
        )
        if any(not math.isfinite(value) for value in numeric_values):
            raise ValueError("scheduler configuration values must be finite")
        if self.total_capacity < 0 or self.min_rate < 0 or self.allocation_interval <= 0:
            raise ValueError("scheduler capacities and rates must be non-negative")
        if self.chunk_size <= 0 or self.service_window <= 0:
            raise ValueError("chunk_size and service_window must be positive")
        if self.min_priority <= 0 or self.max_priority < self.min_priority:
            raise ValueError("priority bounds must be positive and ordered")
        if self.min_size_weight <= 0 or self.max_size_weight < self.min_size_weight:
            raise ValueError("size-weight bounds must be positive and ordered")
        if self.fair_share_bias <= 0 or self.max_deficit_multiplier < 1:
            raise ValueError("fair-share bias must be positive and deficit cap at least one")


@dataclass(frozen=True)
class SimulationConfig:
    """Configuration for discrete-time network simulations."""

    total_capacity: float = 100.0
    time_step: float = 1.0
    max_time: float = 120.0
    seed: int = 42
    capacity_profile: Callable[[float], float] | None = None
    capacity_profile_fn: Callable[[float], float] | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if not math.isfinite(self.total_capacity) or self.total_capacity < 0:
            raise ValueError("total_capacity must be finite and non-negative")
        if not math.isfinite(self.time_step) or self.time_step <= 0:
            raise ValueError("time_step must be finite and positive")
        if not math.isfinite(self.max_time) or self.max_time < 0:
            raise ValueError("max_time must be finite and non-negative")

    def capacity_at(self, current_time: float) -> float:
        if self.capacity_profile_fn is not None:
            capacity = float(self.capacity_profile_fn(current_time))
        elif self.capacity_profile is not None:
            capacity = float(self.capacity_profile(current_time))
        else:
            capacity = float(self.total_capacity)
        if not math.isfinite(capacity) or capacity < 0:
            raise ValueError("capacity profile must return a finite non-negative value")
        return capacity
