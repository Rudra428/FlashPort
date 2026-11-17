"""Configuration dataclasses for FlowWeave components."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from flowweave.models import PolicyName


@dataclass
class SchedulerConfig:
    policy: PolicyName = PolicyName.FLOWWEAVE
    # Fraction of each shared bottleneck reserved as a max-min fairness floor
    # so large flows make steady progress and never starve under SRPT pressure.
    min_fair_share_fraction: float = 0.15
    # Priority exponent applied to inverse remaining work (1.0 = linear SRPT bias,
    # higher values concentrate bandwidth more aggressively on short flows).
    srpt_exponent: float = 1.25
    # Aging rate (per second) that lifts priority of long-waiting flows.
    starvation_aging_per_s: float = 0.04
    # Target utilization headroom when loss or congestion is detected.
    congestion_headroom: float = 0.92
    # Minimum per-transfer rate floor (bytes/sec) when active, preventing stalled sockets.
    min_rate_bps: float = 16_384.0


@dataclass
class StragglerConfig:
    enabled: bool = True
    # A chunk is flagged as a straggler if its observed rate is below this fraction
    # of its allocated rate or sibling median.
    slow_ratio_threshold: float = 0.45
    # Minimum chunk age (seconds) before considering it a straggler (avoids cold-start noise).
    min_chunk_age_s: float = 0.5
    # Minimum net time saved (seconds) required to justify launching a replica.
    min_net_saving_s: float = 0.25
    # Cap on total redundant bytes as a fraction of total workload bytes.
    max_redundant_ratio: float = 0.10
    # Weight on external delay imposed on co-located flows on the alternate link.
    congestion_cost_weight: float = 1.15
    # Disabled by default in the real HTTP engine unless explicitly opted in.
    allow_real_replication: bool = False


@dataclass
class MonitorConfig:
    ewma_alpha: float = 0.35
    rtt_alpha: float = 0.25
    rolling_window_samples: int = 6
    min_samples_for_confidence: int = 4


@dataclass
class TransferConfig:
    download_dir: Path = field(default_factory=lambda: Path("downloads"))
    upload_dir: Path = field(default_factory=lambda: Path("uploads"))
    sample_dir: Path = field(default_factory=lambda: Path("sample_files"))
    chunk_size_bytes: int = 256 * 1024  # 256 KiB
    max_chunks_per_transfer: int = 4
    max_retries: int = 3
    retry_backoff_s: float = 0.4
    request_timeout_s: float = 30.0
    # Separate full-duplex default link capacities for downloads and uploads.
    total_link_capacity_bps: float = 10 * 1024 * 1024  # 10 MB/s downlink cap
    upload_link_capacity_bps: float = 6 * 1024 * 1024  # 6 MB/s uplink cap


@dataclass
class FlowWeaveConfig:
    db_path: Path = field(default_factory=lambda: Path("flowweave.db"))
    control_interval_s: float = 0.25
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    straggler: StragglerConfig = field(default_factory=StragglerConfig)
    monitor: MonitorConfig = field(default_factory=MonitorConfig)
    transfer: TransferConfig = field(default_factory=TransferConfig)
