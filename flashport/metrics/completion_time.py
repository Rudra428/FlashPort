from __future__ import annotations

import math
from statistics import mean, median

from flashport.simulation.models import TransferResult


def flow_completion_times(results: list[TransferResult]) -> list[float]:
    values = [result.flow_completion_time for result in results if result.flow_completion_time is not None]
    return [float(value) for value in values]


def mean_flow_completion_time(results: list[TransferResult]) -> float:
    values = flow_completion_times(results)
    return float(mean(values)) if values else 0.0


def median_flow_completion_time(results: list[TransferResult]) -> float:
    values = flow_completion_times(results)
    return float(median(values)) if values else 0.0


def percentile_flow_completion_time(results: list[TransferResult], percentile: float = 95.0) -> float:
    values = sorted(flow_completion_times(results))
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    rank = max(0.0, min(1.0, percentile / 100.0)) * (len(values) - 1)
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return float(values[lower])
    fraction = rank - lower
    return float(values[lower] + (values[upper] - values[lower]) * fraction)


def total_workload_completion_time(results: list[TransferResult]) -> float:
    completed = [result for result in results if result.completion_time is not None]
    if not completed:
        return 0.0
    first_arrival = min(result.arrival_time for result in results) if results else 0.0
    return max(0.0, float(max(result.completion_time for result in completed) - first_arrival))


def aggregate_goodput(results: list[TransferResult]) -> float:
    total_bytes = sum(result.size for result in results if result.completion_time is not None)
    total_runtime = total_workload_completion_time(results)
    if total_runtime <= 0:
        return 0.0
    return total_bytes / total_runtime


def long_flow_threshold(results: list[TransferResult], factor: float = 2.0) -> float:
    if factor <= 0:
        raise ValueError("factor must be positive")
    values = flow_completion_times(results)
    return (median(values) * factor) if values else 0.0


def long_flow_completion_count(results: list[TransferResult], factor: float = 2.0) -> int:
    threshold = long_flow_threshold(results, factor)
    if threshold <= 0:
        return 0
    return sum(
        result.flow_completion_time is not None and result.flow_completion_time > threshold
        for result in results
    )
