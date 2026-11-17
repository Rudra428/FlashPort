"""Core domain models shared across simulator, schedulers, monitor, transfer engine, and API."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class PolicyName(str, Enum):
    INDEPENDENT = "independent"
    MAX_MIN = "max_min"
    SRPT = "srpt"
    FLOWWEAVE = "flowweave"


class TransferDirection(str, Enum):
    DOWNLOAD = "download"
    UPLOAD = "upload"


class LinkDirection(str, Enum):
    DOWNLINK = "downlink"
    UPLINK = "uplink"
    SHARED = "shared"


class TransferState(str, Enum):
    PENDING = "pending"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ChunkState(str, Enum):
    PENDING = "pending"
    IN_FLIGHT = "in_flight"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class LinkState:
    """Directed graph link with capacity, latency, loss, direction, and measured load."""

    link_id: str
    capacity_bps: float
    latency_ms: float = 10.0
    loss_rate: float = 0.0
    is_shared: bool = True
    direction: LinkDirection = LinkDirection.SHARED
    source_node: str = "client"
    target_node: str = "server"
    measured_throughput_bps: float = 0.0
    estimated_available_bps: float = 0.0
    active_transfers: int = 0
    congestion_ratio: float = 0.0  # > 1.0 means oversubscribed

    @property
    def utilization(self) -> float:
        if self.capacity_bps <= 0:
            return 0.0
        return min(1.0, self.measured_throughput_bps / self.capacity_bps)

    @property
    def is_congested(self) -> bool:
        return self.congestion_ratio > 1.02 or self.loss_rate >= 0.03

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["direction"] = self.direction.value
        data["utilization"] = self.utilization
        data["is_congested"] = self.is_congested
        return data


@dataclass
class MeasuredMetrics:
    """Directly observed transfer statistics (no smoothing or prediction)."""

    bytes_acked: int = 0
    raw_bytes_received: int = 0
    redundant_bytes: int = 0
    instant_goodput_bps: float = 0.0
    rolling_goodput_bps: float = 0.0
    avg_goodput_bps: float = 0.0
    last_rtt_ms: float = 0.0
    retries: int = 0
    sample_count: int = 0
    last_updated: float = 0.0


@dataclass
class EstimatedMetrics:
    """Derived/smoothed estimates used by the scheduler and straggler detector."""

    ewma_goodput_bps: float = 0.0
    ewma_rtt_ms: float = 0.0
    predicted_remaining_time_s: float = float("inf")
    estimated_bottleneck_bps: float = 0.0
    confidence: float = 0.0  # 0.0 (cold start) .. 1.0 (converged)


@dataclass
class ChunkRecord:
    """Tracks a single byte-range chunk of a download or upload."""

    chunk_id: str
    transfer_id: str
    index: int
    start_byte: int
    end_byte: int  # inclusive
    link_id: str = "default"
    state: ChunkState = ChunkState.PENDING
    downloaded_bytes: int = 0
    started_at: float | None = None
    completed_at: float | None = None
    retries: int = 0
    is_replica: bool = False
    replica_of: str | None = None

    @property
    def total_bytes(self) -> int:
        return max(0, self.end_byte - self.start_byte + 1)

    @property
    def remaining_bytes(self) -> int:
        return max(0, self.total_bytes - self.downloaded_bytes)


@dataclass
class TransferSnapshot:
    """Unified point-in-time transfer state for both uploads and downloads."""

    transfer_id: str
    filename: str
    url: str
    total_bytes: int
    remaining_bytes: int
    state: TransferState
    candidate_links: list[str]
    assigned_link: str
    created_at: float
    direction: TransferDirection = TransferDirection.DOWNLOAD
    source: str = ""
    destination: str = ""
    route: list[str] = field(default_factory=list)
    candidate_routes: list[list[str]] = field(default_factory=list)
    started_at: float | None = None
    completed_at: float | None = None
    allocated_bps: float = 0.0
    active_chunks: int = 0
    active_workers: int = 0
    completed_chunks: int = 0
    total_chunks: int = 0
    priority_weight: float = 1.0
    measured: MeasuredMetrics = field(default_factory=MeasuredMetrics)
    estimated: EstimatedMetrics = field(default_factory=EstimatedMetrics)
    dest_path: str | None = None
    sha256: str | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        if not self.route and self.assigned_link:
            self.route = [self.assigned_link]
        if not self.candidate_routes:
            if self.candidate_links:
                self.candidate_routes = [[lid] for lid in self.candidate_links]
            elif self.route:
                self.candidate_routes = [list(self.route)]
        if not self.source:
            self.source = self.url
        if not self.destination and self.dest_path:
            self.destination = self.dest_path

    @property
    def bytes_transferred(self) -> int:
        if self.total_bytes <= 0:
            return self.measured.bytes_acked
        return max(0, self.total_bytes - self.remaining_bytes)

    @property
    def progress(self) -> float:
        if self.total_bytes <= 0:
            return 1.0 if self.state == TransferState.COMPLETED else 0.0
        return min(1.0, max(0.0, self.bytes_transferred / self.total_bytes))

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["state"] = self.state.value
        data["direction"] = self.direction.value
        data["bytes_transferred"] = self.bytes_transferred
        data["progress"] = self.progress
        if data["estimated"]["predicted_remaining_time_s"] == float("inf"):
            data["estimated"]["predicted_remaining_time_s"] = None
        return data


@dataclass
class SchedulerInput:
    """Complete network and transfer state given to a scheduler on each decision tick."""

    now: float
    transfers: list[TransferSnapshot]
    links: dict[str, LinkState]


@dataclass
class ScheduleDecision:
    """Output of a scheduling tick."""

    policy: PolicyName
    timestamp: float
    allocations_bps: dict[str, float]
    link_assignments: dict[str, str]
    route_assignments: dict[str, list[str]] = field(default_factory=dict)
    compute_time_ms: float = 0.0
    rationale: list[str] = field(default_factory=list)
    notes: dict[str, Any] = field(default_factory=dict)


@dataclass
class StragglerDecision:
    """Decision on whether to replicate a slow chunk onto another link."""

    transfer_id: str
    chunk_id: str
    original_link: str
    replica_link: str | None
    should_replicate: bool
    expected_saving_s: float
    expected_cost_s: float
    reason: str


@dataclass
class TransferOutcome:
    """Final metrics for one completed (or failed) transfer."""

    transfer_id: str
    filename: str
    size_bytes: int
    arrival_time: float
    start_time: float
    completion_time: float
    fct_s: float
    avg_goodput_bps: float
    direction: TransferDirection = TransferDirection.DOWNLOAD
    redundant_bytes: int = 0
    retries: int = 0
    state: TransferState = TransferState.COMPLETED

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["state"] = self.state.value
        data["direction"] = self.direction.value
        return data


@dataclass
class ExperimentSummary:
    """Aggregate evaluation metrics for one scenario + policy run."""

    scenario: str
    policy: str
    seed: int
    num_transfers: int
    completed_transfers: int
    avg_fct_s: float
    sum_fct_s: float
    p95_fct_s: float
    makespan_s: float
    aggregate_goodput_bps: float
    jains_fairness: float
    redundant_bytes: int
    redundant_ratio: float
    scheduler_overhead_ms_total: float
    scheduler_overhead_ms_avg: float
    replications_triggered: int = 0
    replications_won: int = 0
    is_simulated: bool = True
    outcomes: list[TransferOutcome] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "policy": self.policy,
            "seed": self.seed,
            "is_simulated": self.is_simulated,
            "num_transfers": self.num_transfers,
            "completed_transfers": self.completed_transfers,
            "avg_fct_s": round(self.avg_fct_s, 4),
            "sum_fct_s": round(self.sum_fct_s, 4),
            "p95_fct_s": round(self.p95_fct_s, 4),
            "makespan_s": round(self.makespan_s, 4),
            "aggregate_goodput_bps": round(self.aggregate_goodput_bps, 2),
            "jains_fairness": round(self.jains_fairness, 4),
            "redundant_bytes": self.redundant_bytes,
            "redundant_ratio": round(self.redundant_ratio, 5),
            "scheduler_overhead_ms_total": round(self.scheduler_overhead_ms_total, 4),
            "scheduler_overhead_ms_avg": round(self.scheduler_overhead_ms_avg, 4),
            "replications_triggered": self.replications_triggered,
            "replications_won": self.replications_won,
            "outcomes": [o.to_dict() for o in self.outcomes],
        }
