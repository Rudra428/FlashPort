"""Directed-graph network topology and scenario definitions for the SimPy fluid-flow simulator."""

from __future__ import annotations

from dataclasses import dataclass, field

from flowweave.models import LinkDirection, TransferDirection


@dataclass
class BandwidthEvent:
    """Scheduled change in link capacity, latency, or packet loss at a given simulation time."""

    time_s: float
    capacity_bps: float | None = None
    latency_ms: float | None = None
    loss_rate: float | None = None


@dataclass
class LinkSpec:
    """Directed edge in the network graph with static and time-varying characteristics."""

    link_id: str
    capacity_bps: float
    latency_ms: float = 15.0
    loss_rate: float = 0.0
    is_shared: bool = True
    direction: LinkDirection = LinkDirection.SHARED
    source_node: str = "client"
    target_node: str = "server"
    events: list[BandwidthEvent] = field(default_factory=list)


@dataclass
class TransferSpec:
    """One upload or download workload entry inside a simulation scenario."""

    transfer_id: str
    filename: str
    size_bytes: int
    arrival_time_s: float = 0.0
    direction: TransferDirection = TransferDirection.DOWNLOAD
    candidate_links: list[str] = field(default_factory=lambda: ["bottleneck"])
    route: list[str] = field(default_factory=list)
    candidate_routes: list[list[str]] = field(default_factory=list)
    chunk_size_bytes: int = 512 * 1024
    max_concurrent_chunks: int = 2
    priority_weight: float = 1.0
    # Optional deterministic per-transfer impairment factor (0 < factor <= 1) applied to
    # remaining chunks in straggler scenarios.
    impairment_factor: float = 1.0
    impairment_start_fraction: float = 0.0

    def __post_init__(self) -> None:
        if self.route and not self.candidate_routes:
            self.candidate_routes = [list(self.route)]
        elif self.candidate_routes and not self.route:
            self.route = list(self.candidate_routes[0])
        elif not self.route and self.candidate_links:
            self.route = [self.candidate_links[0]]
            self.candidate_routes = [[lid] for lid in self.candidate_links]
        if self.route and not self.candidate_links:
            self.candidate_links = [r[0] for r in self.candidate_routes]


@dataclass
class ScenarioSpec:
    """Complete reproducible simulation scenario (fluid-flow model, not packet-level TCP)."""

    name: str
    description: str
    links: list[LinkSpec]
    transfers: list[TransferSpec]
    tick_s: float = 0.1
    seed: int = 42
    straggler_chunk_prob: float = 0.0
    straggler_slowdown_factor: float = 0.18
    enable_straggler_replication: bool = True
    max_sim_time_s: float = 600.0
