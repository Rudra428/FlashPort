from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class TransferState:
    """Represents one logical file transfer in the transfer manager."""

    transfer_id: str
    client_id: str
    filename: str
    size: int
    downloaded: int = 0
    goodput: float = 0.0
    rtt: float | None = None
    active_connections: int = 1
    start_time: float | None = None
    completion_time: float | None = None
    state: str = "queued"
    completed_chunks: set[str] = field(default_factory=set)
    last_rate: float = 0.0
    bytes_sent: int = 0
    served_bytes: int = 0
    http_request_latency_ms: float | None = None
    service_history: list[float] = field(default_factory=list)

    @property
    def remaining(self) -> int:
        return max(self.size - self.downloaded, 0)

    def mark_started(self, current_time: float | None = None) -> None:
        if self.start_time is None:
            self.start_time = current_time if current_time is not None else 0.0
        self.state = "active"

    def mark_completed(self, current_time: float | None = None) -> None:
        self.downloaded = self.size
        self.state = "completed"
        self.completion_time = current_time if current_time is not None else self.start_time

    def update_download(self, bytes_received: float) -> None:
        self.downloaded = int(min(self.size, self.downloaded + int(bytes_received)))
        if self.downloaded >= self.size:
            self.state = "completed"

    def to_snapshot(self) -> dict[str, Any]:
        return {
            "transfer_id": self.transfer_id,
            "client_id": self.client_id,
            "filename": self.filename,
            "size": self.size,
            "downloaded": self.downloaded,
            "remaining": self.remaining,
            "goodput": self.goodput,
            "rtt": self.rtt,
            "active_connections": self.active_connections,
            "allocated_rate": self.last_rate,
            "bytes_sent": self.bytes_sent,
            "served_bytes": self.served_bytes,
            "redundant_bytes": max(0, self.bytes_sent - self.served_bytes),
            "state": self.state,
            "start_time": self.start_time,
            "completion_time": self.completion_time,
            "http_request_latency_ms": self.http_request_latency_ms,
        }


@dataclass(frozen=True)
class SchedulerUpdate:
    """Allocation decision produced by a scheduler for a set of active transfers."""

    allocations: dict[str, float]
    total_capacity: float
    active_transfer_ids: tuple[str, ...]
