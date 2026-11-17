"""Evaluation metrics for transfer workloads and scheduling policy comparisons."""

from __future__ import annotations

import numpy as np

from flowweave.models import ExperimentSummary, TransferOutcome, TransferState


def jains_fairness_index(values: list[float]) -> float:
    """Compute Jain's fairness index: (sum x_i)^2 / (n * sum x_i^2) in [1/n, 1]."""
    arr = np.asarray([v for v in values if v >= 0.0], dtype=np.float64)
    if arr.size == 0:
        return 1.0
    denom = arr.size * float(np.sum(arr * arr))
    if denom <= 0.0:
        return 1.0
    total = float(np.sum(arr))
    return float((total * total) / denom)


def summarize_experiment(
    *,
    scenario: str,
    policy: str,
    seed: int,
    outcomes: list[TransferOutcome],
    scheduler_overhead_ms: list[float],
    replications_triggered: int = 0,
    replications_won: int = 0,
) -> ExperimentSummary:
    """Aggregate per-transfer outcomes into a complete ExperimentSummary."""
    completed = [o for o in outcomes if o.state == TransferState.COMPLETED]
    fcts = [o.fct_s for o in completed]

    if fcts:
        fct_arr = np.asarray(fcts, dtype=np.float64)
        avg_fct = float(np.mean(fct_arr))
        sum_fct = float(np.sum(fct_arr))
        p95_fct = float(np.percentile(fct_arr, 95))
        earliest_arrival = min(o.arrival_time for o in completed)
        latest_completion = max(o.completion_time for o in completed)
        makespan = max(0.0, latest_completion - earliest_arrival)
        total_useful_bytes = sum(o.size_bytes for o in completed)
        agg_goodput = total_useful_bytes / makespan if makespan > 0 else 0.0
        goodputs = [o.avg_goodput_bps for o in completed]
        fairness = jains_fairness_index(goodputs)
    else:
        avg_fct = 0.0
        sum_fct = 0.0
        p95_fct = 0.0
        makespan = 0.0
        total_useful_bytes = 0
        agg_goodput = 0.0
        fairness = 0.0

    redundant_bytes = sum(o.redundant_bytes for o in outcomes)
    denom_bytes = max(1, total_useful_bytes + redundant_bytes)
    redundant_ratio = redundant_bytes / denom_bytes

    overhead_total = float(sum(scheduler_overhead_ms))
    overhead_avg = overhead_total / len(scheduler_overhead_ms) if scheduler_overhead_ms else 0.0

    return ExperimentSummary(
        scenario=scenario,
        policy=policy,
        seed=seed,
        num_transfers=len(outcomes),
        completed_transfers=len(completed),
        avg_fct_s=avg_fct,
        sum_fct_s=sum_fct,
        p95_fct_s=p95_fct,
        makespan_s=makespan,
        aggregate_goodput_bps=agg_goodput,
        jains_fairness=fairness,
        redundant_bytes=redundant_bytes,
        redundant_ratio=redundant_ratio,
        scheduler_overhead_ms_total=overhead_total,
        scheduler_overhead_ms_avg=overhead_avg,
        replications_triggered=replications_triggered,
        replications_won=replications_won,
        outcomes=outcomes,
    )
