"""Reproducible benchmark scenarios spanning single, multi-hop, dynamic, and bidirectional topologies."""

from __future__ import annotations

import random

import numpy as np

from flowweave.models import LinkDirection, TransferDirection
from flowweave.simulator.topology import BandwidthEvent, LinkSpec, ScenarioSpec, TransferSpec

MB = 1024 * 1024


def mixed_sizes_shared_bottleneck(seed: int = 42) -> ScenarioSpec:
    """Heavy-tailed mix of small, medium, and large files sharing one 10 MB/s bottleneck."""
    sizes_mb = [0.5, 0.75, 1.0, 1.5, 2.0, 4.0, 8.0, 16.0, 24.0, 32.0]
    transfers = [
        TransferSpec(
            transfer_id=f"tx-{i+1}",
            filename=f"file_{i+1}_{sz}MB.bin",
            size_bytes=int(sz * MB),
            arrival_time_s=0.0 if i < 6 else 0.5 * (i - 5),
            candidate_links=["shared-wan"],
            chunk_size_bytes=256 * 1024,
        )
        for i, sz in enumerate(sizes_mb)
    ]
    return ScenarioSpec(
        name="mixed_shared_bottleneck",
        description="10 mixed-size files (0.5–32 MB) contending on a single 10 MB/s shared bottleneck.",
        links=[
            LinkSpec(
                "shared-wan",
                capacity_bps=10 * MB,
                latency_ms=25.0,
                loss_rate=0.005,
                direction=LinkDirection.DOWNLINK,
                source_node="cdn",
                target_node="client",
            )
        ],
        transfers=transfers,
        tick_s=0.1,
        seed=seed,
    )


def independent_paths(seed: int = 42) -> ScenarioSpec:
    """Transfers traverse completely separate links with zero bandwidth contention."""
    links = [
        LinkSpec("path-a", capacity_bps=10 * MB, latency_ms=10.0, loss_rate=0.0, is_shared=False, source_node="s1", target_node="c1"),
        LinkSpec("path-b", capacity_bps=6 * MB, latency_ms=15.0, loss_rate=0.0, is_shared=False, source_node="s2", target_node="c2"),
        LinkSpec("path-c", capacity_bps=4 * MB, latency_ms=20.0, loss_rate=0.0, is_shared=False, source_node="s3", target_node="c3"),
    ]
    transfers = [
        TransferSpec("tx-1", "indep_a_20MB.bin", 20 * MB, 0.0, candidate_links=["path-a"]),
        TransferSpec("tx-2", "indep_b_12MB.bin", 12 * MB, 0.0, candidate_links=["path-b"]),
        TransferSpec("tx-3", "indep_c_8MB.bin", 8 * MB, 0.0, candidate_links=["path-c"]),
    ]
    return ScenarioSpec(
        name="independent_paths",
        description="3 transfers on completely independent links verifying zero cross-flow interference.",
        links=links,
        transfers=transfers,
        tick_s=0.1,
        seed=seed,
    )


def partially_shared_paths(seed: int = 42) -> ScenarioSpec:
    """Directed graph with multi-hop routes where some paths share only a single core bottleneck."""
    links = [
        LinkSpec("access-1", capacity_bps=12 * MB, latency_ms=5.0, loss_rate=0.0, is_shared=False, source_node="h1", target_node="r1"),
        LinkSpec("access-2", capacity_bps=12 * MB, latency_ms=5.0, loss_rate=0.0, is_shared=False, source_node="h2", target_node="r1"),
        LinkSpec("access-3", capacity_bps=12 * MB, latency_ms=5.0, loss_rate=0.0, is_shared=False, source_node="h3", target_node="r1"),
        LinkSpec("core-bottleneck", capacity_bps=8 * MB, latency_ms=20.0, loss_rate=0.005, is_shared=True, source_node="r1", target_node="r2"),
        LinkSpec("direct-link", capacity_bps=6 * MB, latency_ms=15.0, loss_rate=0.0, is_shared=False, source_node="h4", target_node="r2"),
    ]
    transfers = [
        TransferSpec("tx-1", "shared_small_2MB.bin", 2 * MB, 0.0, route=["access-1", "core-bottleneck"], chunk_size_bytes=256 * 1024),
        TransferSpec("tx-2", "shared_med_6MB.bin", 6 * MB, 0.0, route=["access-2", "core-bottleneck"], chunk_size_bytes=256 * 1024),
        TransferSpec("tx-3", "shared_large_18MB.bin", 18 * MB, 0.0, route=["access-3", "core-bottleneck"], chunk_size_bytes=256 * 1024),
        TransferSpec("tx-4", "isolated_12MB.bin", 12 * MB, 0.0, route=["direct-link"], chunk_size_bytes=256 * 1024),
    ]
    return ScenarioSpec(
        name="partially_shared_paths",
        description="Multi-hop routes sharing only `core-bottleneck` (8 MB/s) alongside an isolated 6 MB/s path.",
        links=links,
        transfers=transfers,
        tick_s=0.1,
        seed=seed,
    )


def bidirectional_mixed(seed: int = 42) -> ScenarioSpec:
    """Simultaneous uploads and downloads over full-duplex uplink/downlink + shared core link."""
    links = [
        LinkSpec("wan-downlink", capacity_bps=10 * MB, latency_ms=20.0, loss_rate=0.005, direction=LinkDirection.DOWNLINK, source_node="isp", target_node="client"),
        LinkSpec("wan-uplink", capacity_bps=6 * MB, latency_ms=20.0, loss_rate=0.005, direction=LinkDirection.UPLINK, source_node="client", target_node="isp"),
        LinkSpec("isp-shared", capacity_bps=12 * MB, latency_ms=10.0, loss_rate=0.0, direction=LinkDirection.SHARED, source_node="isp", target_node="dc"),
    ]
    transfers = [
        # Concurrent downloads
        TransferSpec("dl-1", "dl_patch_1MB.bin", 1 * MB, 0.0, direction=TransferDirection.DOWNLOAD, route=["wan-downlink", "isp-shared"], chunk_size_bytes=256 * 1024),
        TransferSpec("dl-2", "dl_model_4MB.bin", 4 * MB, 0.0, direction=TransferDirection.DOWNLOAD, route=["wan-downlink", "isp-shared"], chunk_size_bytes=256 * 1024),
        TransferSpec("dl-3", "dl_archive_16MB.bin", 16 * MB, 0.2, direction=TransferDirection.DOWNLOAD, route=["wan-downlink", "isp-shared"], chunk_size_bytes=256 * 1024),
        # Concurrent uploads
        TransferSpec("ul-1", "ul_report_512KB.bin", int(0.5 * MB), 0.0, direction=TransferDirection.UPLOAD, route=["wan-uplink", "isp-shared"], chunk_size_bytes=256 * 1024),
        TransferSpec("ul-2", "ul_dataset_3MB.bin", 3 * MB, 0.0, direction=TransferDirection.UPLOAD, route=["wan-uplink", "isp-shared"], chunk_size_bytes=256 * 1024),
        TransferSpec("ul-3", "ul_backup_10MB.bin", 10 * MB, 0.3, direction=TransferDirection.UPLOAD, route=["wan-uplink", "isp-shared"], chunk_size_bytes=256 * 1024),
    ]
    return ScenarioSpec(
        name="bidirectional_mixed",
        description="3 simultaneous downloads + 3 uploads over separate full-duplex downlink (10 MB/s), uplink (6 MB/s), and shared ISP core (12 MB/s).",
        links=links,
        transfers=transfers,
        tick_s=0.1,
        seed=seed,
    )


def multi_link_offload(seed: int = 42) -> ScenarioSpec:
    """Primary shared bottleneck plus secondary independent links for cooperative offloading."""
    links = [
        LinkSpec("primary-shared", capacity_bps=8 * MB, latency_ms=20.0, loss_rate=0.01, is_shared=True),
        LinkSpec("secondary-a", capacity_bps=6 * MB, latency_ms=30.0, loss_rate=0.005, is_shared=False),
        LinkSpec("secondary-b", capacity_bps=4 * MB, latency_ms=35.0, loss_rate=0.005, is_shared=False),
    ]
    sizes_mb = [1.0, 1.5, 2.0, 3.0, 5.0, 8.0, 12.0, 18.0, 20.0, 25.0]
    transfers = []
    for i, sz in enumerate(sizes_mb):
        if i % 3 == 0:
            cands = ["primary-shared", "secondary-a"]
        elif i % 3 == 1:
            cands = ["primary-shared", "secondary-b"]
        else:
            cands = ["primary-shared", "secondary-a", "secondary-b"]
        transfers.append(
            TransferSpec(
                transfer_id=f"tx-{i+1}",
                filename=f"multi_{i+1}_{sz}MB.bin",
                size_bytes=int(sz * MB),
                arrival_time_s=0.0,
                candidate_links=cands,
                chunk_size_bytes=256 * 1024,
            )
        )
    return ScenarioSpec(
        name="multi_link_offload",
        description="10 transfers across a shared primary bottleneck and two secondary links.",
        links=links,
        transfers=transfers,
        tick_s=0.1,
        seed=seed,
    )


def dynamic_bandwidth_drop(seed: int = 42) -> ScenarioSpec:
    """Changing network conditions: bottleneck drops 65% mid-transfer with loss spike, then recovers."""
    links = [
        LinkSpec(
            "wan-primary",
            capacity_bps=12 * MB,
            latency_ms=20.0,
            loss_rate=0.005,
            events=[
                BandwidthEvent(time_s=2.0, capacity_bps=4 * MB, latency_ms=75.0, loss_rate=0.06),
                BandwidthEvent(time_s=7.0, capacity_bps=10 * MB, latency_ms=25.0, loss_rate=0.01),
            ],
        ),
        LinkSpec(
            "wan-backup",
            capacity_bps=5 * MB,
            latency_ms=40.0,
            loss_rate=0.005,
            events=[
                BandwidthEvent(time_s=4.0, capacity_bps=7 * MB, latency_ms=30.0, loss_rate=0.005),
            ],
        ),
    ]
    sizes_mb = [1.0, 2.0, 3.0, 5.0, 8.0, 12.0, 15.0, 20.0]
    transfers = [
        TransferSpec(
            transfer_id=f"tx-{i+1}",
            filename=f"dyn_{i+1}_{sz}MB.bin",
            size_bytes=int(sz * MB),
            arrival_time_s=0.25 * i,
            candidate_links=["wan-primary", "wan-backup"],
            chunk_size_bytes=256 * 1024,
        )
        for i, sz in enumerate(sizes_mb)
    ]
    return ScenarioSpec(
        name="dynamic_bandwidth_drop",
        description="Primary link suffers a severe bandwidth drop and 6% loss spike at t=2s before recovering at t=7s.",
        links=links,
        transfers=transfers,
        tick_s=0.1,
        seed=seed,
    )


def straggler_recovery(seed: int = 42) -> ScenarioSpec:
    """Multi-path transfers with configurable chunk impairment testing cost-benefit replication."""
    links = [
        LinkSpec("mirror-a", capacity_bps=8 * MB, latency_ms=20.0, loss_rate=0.01),
        LinkSpec("mirror-b", capacity_bps=8 * MB, latency_ms=25.0, loss_rate=0.005),
    ]
    sizes_mb = [2.0, 3.0, 4.0, 6.0, 8.0, 10.0]
    transfers = [
        TransferSpec(
            transfer_id=f"tx-{i+1}",
            filename=f"strag_{i+1}_{sz}MB.bin",
            size_bytes=int(sz * MB),
            arrival_time_s=0.0,
            candidate_links=["mirror-a", "mirror-b"],
            chunk_size_bytes=512 * 1024,
            max_concurrent_chunks=2,
        )
        for i, sz in enumerate(sizes_mb)
    ]
    return ScenarioSpec(
        name="straggler_recovery",
        description="6 dual-mirror transfers with 25% chunk straggler slowdown testing cost-benefit replication.",
        links=links,
        transfers=transfers,
        tick_s=0.1,
        seed=seed,
        straggler_chunk_prob=0.25,
        straggler_slowdown_factor=0.12,
        enable_straggler_replication=True,
    )


def high_concurrency_scale(num_transfers: int = 25, seed: int = 42) -> ScenarioSpec:
    """Scalability benchmark with log-normal file sizes and staggered arrivals."""
    rng = random.Random(seed)
    raw_sizes = [rng.lognormvariate(1.1, 1.15) for _ in range(num_transfers)]
    sizes_mb = np.clip(np.asarray(raw_sizes, dtype=np.float64), 0.25, 40.0)
    arrivals = np.sort(
        np.asarray([rng.uniform(0.0, 4.0) for _ in range(num_transfers)], dtype=np.float64)
    )
    transfers = [
        TransferSpec(
            transfer_id=f"tx-{i+1}",
            filename=f"scale_{i+1}.bin",
            size_bytes=int(round(float(sz) * MB)),
            arrival_time_s=round(float(arr), 2),
            candidate_links=["core-shared"] if i % 4 != 0 else ["core-shared", "edge-alt"],
            chunk_size_bytes=512 * 1024,
        )
        for i, (sz, arr) in enumerate(zip(sizes_mb, arrivals))
    ]
    return ScenarioSpec(
        name=f"high_concurrency_{num_transfers}",
        description=f"{num_transfers} concurrent log-normal transfers with staggered arrivals on shared + edge links.",
        links=[
            LinkSpec("core-shared", capacity_bps=16 * MB, latency_ms=25.0, loss_rate=0.01),
            LinkSpec("edge-alt", capacity_bps=6 * MB, latency_ms=35.0, loss_rate=0.005),
        ],
        transfers=transfers,
        tick_s=0.1,
        seed=seed,
    )


def uniform_sizes_fairness(seed: int = 42) -> ScenarioSpec:
    """Identical-sized files on a shared bottleneck to inspect fairness and zero-starvation behavior."""
    transfers = [
        TransferSpec(
            transfer_id=f"tx-{i+1}",
            filename=f"uniform_{i+1}_8MB.bin",
            size_bytes=8 * MB,
            arrival_time_s=0.0,
            candidate_links=["shared-link"],
            chunk_size_bytes=256 * 1024,
        )
        for i in range(8)
    ]
    return ScenarioSpec(
        name="uniform_sizes_fairness",
        description="8 identical 8 MB files starting simultaneously on a 10 MB/s shared bottleneck.",
        links=[LinkSpec("shared-link", capacity_bps=10 * MB, latency_ms=15.0, loss_rate=0.0)],
        transfers=transfers,
        tick_s=0.1,
        seed=seed,
    )


def get_all_scenarios(seed: int = 42) -> dict[str, ScenarioSpec]:
    """Return the full catalog of built-in evaluation scenarios."""
    specs = [
        mixed_sizes_shared_bottleneck(seed),
        independent_paths(seed),
        partially_shared_paths(seed),
        bidirectional_mixed(seed),
        multi_link_offload(seed),
        dynamic_bandwidth_drop(seed),
        straggler_recovery(seed),
        uniform_sizes_fairness(seed),
        high_concurrency_scale(25, seed),
    ]
    return {s.name: s for s in specs}
