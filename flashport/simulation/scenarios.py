from __future__ import annotations

import random
from dataclasses import dataclass

from flashport.config import SimulationConfig
from flashport.simulation.models import TransferRequest

BENCHMARK_CAPACITY = 12_500_000.0  # 100 Mbps expressed in bytes/second.
BENCHMARK_TIME_STEP = 0.5


@dataclass(frozen=True)
class BenchmarkCase:
    """A reproducible workload/configuration pair for policy comparisons."""

    name: str
    category: str
    seed: int
    description: str
    workload: tuple[TransferRequest, ...]
    config: SimulationConfig


def _requests(sizes: list[int], arrivals: list[float] | None = None) -> tuple[TransferRequest, ...]:
    release_times = arrivals or [0.0] * len(sizes)
    if len(release_times) != len(sizes):
        raise ValueError("sizes and arrivals must have the same length")
    return tuple(
        TransferRequest(
            transfer_id=f"flow-{index:04d}",
            filename=f"file-{index:04d}.bin",
            size=size,
            arrival_time=release_times[index],
            client_id=f"client-{index:04d}",
        )
        for index, size in enumerate(sizes)
    )


def benchmark_cases(random_seeds: tuple[int, ...] = (7, 21, 42, 84, 126)) -> list[BenchmarkCase]:
    """Build the complete deterministic milestone 2.5 experiment matrix.

    Workloads use byte counts and rates in bytes/second. Large jobs are simulated as
    integer metadata only; no file data is allocated.
    """
    cases: list[BenchmarkCase] = []

    def add(
        name: str,
        category: str,
        seed: int,
        description: str,
        workload: tuple[TransferRequest, ...],
        capacity: float = BENCHMARK_CAPACITY,
        profile=None,
        max_time: float = 10_000.0,
    ) -> None:
        cases.append(
            BenchmarkCase(
                name=name,
                category=category,
                seed=seed,
                description=description,
                workload=workload,
                config=SimulationConfig(
                    total_capacity=capacity,
                    time_step=BENCHMARK_TIME_STEP,
                    max_time=max_time,
                    seed=seed,
                    capacity_profile=profile,
                ),
            )
        )

    add(
        "equal_10",
        "equal-sized",
        100,
        "10 simultaneous 10 MB files",
        _requests([10_000_000] * 10),
    )
    add(
        "unequal_sizes",
        "unequal-sized",
        200,
        "Simultaneous 10, 50, 100, 500 and 1,000 MB files",
        _requests([10_000_000, 50_000_000, 100_000_000, 500_000_000, 1_000_000_000]),
        max_time=1_000.0,
    )

    for count in (10, 50, 100, 500):
        add(
            f"concurrency_{count}",
            "high-concurrency",
            300 + count,
            f"{count} simultaneous 1 MB files",
            _requests([1_000_000] * count),
            max_time=1_000.0,
        )

    mixed_rng = random.Random(404)
    mixed_sizes = [mixed_rng.choice([250_000, 1_000_000, 5_000_000, 20_000_000, 50_000_000]) for _ in range(30)]
    mixed_arrivals = [index * 0.5 for index in range(len(mixed_sizes))]
    add(
        "mixed_staggered",
        "mixed-workload",
        404,
        "30 mixed-size files arriving every 0.5 seconds",
        _requests(mixed_sizes, mixed_arrivals),
    )

    def dynamic_capacity(current_time: float) -> float:
        if current_time < 4.0:
            return BENCHMARK_CAPACITY
        if current_time < 10.0:
            return BENCHMARK_CAPACITY / 2.0
        return BENCHMARK_CAPACITY

    add(
        "dynamic_bandwidth",
        "dynamic-bandwidth",
        505,
        "600 MB total: 100 Mbps, then 50 Mbps from 4–10 seconds, then 100 Mbps",
        _requests([100_000_000] * 6),
        profile=dynamic_capacity,
    )
    add(
        "heavy_contention",
        "heavy-contention",
        606,
        "500 simultaneous 500 KB files sharing a 10 Mbps bottleneck",
        _requests([500_000] * 500),
        capacity=1_250_000.0,
        max_time=1_000.0,
    )

    for seed in random_seeds:
        rng = random.Random(seed)
        sizes = [rng.randint(1_000_000, 20_000_000) for _ in range(40)]
        arrivals = [rng.uniform(0.0, 10.0) for _ in sizes]
        add(
            f"random_seed_{seed}",
            "randomized",
            seed,
            "40 randomized files (1–20 MB) with deterministic staggered arrivals",
            _requests(sizes, arrivals),
        )

    return cases

def make_workload(
    file_sizes: list[int] | None = None,
    arrival_times: list[float] | None = None,
    seed: int = 42,
) -> list[TransferRequest]:
    """Return a deterministic synthetic workload for simulation experiments."""

    rng = random.Random(seed)
    sizes = file_sizes or [10_000, 25_000, 70_000, 150_000, 300_000]
    arrivals = arrival_times or [0.0, 1.0, 2.0, 5.0, 8.0]

    return [
        TransferRequest(
            transfer_id=f"flow-{index}",
            filename=f"file-{index}.bin",
            size=sizes[index % len(sizes)],
            arrival_time=arrivals[index % len(arrivals)],
            client_id=f"client-{index}",
        )
        for index in range(max(len(sizes), len(arrivals)))
    ]


def mixed_size_workload(seed: int = 5) -> list[TransferRequest]:
    rng = random.Random(seed)
    sizes = [rng.randint(5_000, 200_000) for _ in range(12)]
    arrivals = [i * 0.5 for i in range(len(sizes))]
    return make_workload(file_sizes=sizes, arrival_times=arrivals, seed=seed)
