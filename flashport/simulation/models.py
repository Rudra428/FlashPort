from __future__ import annotations

from dataclasses import dataclass
import math


@dataclass(frozen=True)
class TransferRequest:
    transfer_id: str
    filename: str
    size: int
    arrival_time: float
    client_id: str = "client-0"

    def __post_init__(self) -> None:
        if not self.transfer_id:
            raise ValueError("transfer_id must not be empty")
        if not isinstance(self.size, int) or isinstance(self.size, bool) or self.size < 0:
            raise ValueError("transfer size must be non-negative")
        if not math.isfinite(self.arrival_time) or self.arrival_time < 0:
            raise ValueError("arrival_time must be finite and non-negative")


@dataclass(frozen=True)
class TransferResult:
    transfer_id: str
    filename: str
    size: int
    arrival_time: float
    completion_time: float | None
    flow_completion_time: float | None
    goodput: float
    bytes_transferred: int = 0


@dataclass(frozen=True)
class SimulationSummary:
    by_policy: dict[str, list[TransferResult]]
    total_workload: int
    total_capacity: float


@dataclass(frozen=True)
class SimulationStats:
    """Run-level accounting and scheduler timing collected by the simulator."""

    bytes_transferred: int
    available_capacity_bytes: float
    network_utilization: float
    scheduler_time_seconds: float
    scheduler_calls: int
    iterations: int
