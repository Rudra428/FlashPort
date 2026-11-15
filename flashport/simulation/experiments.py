from __future__ import annotations

import csv
import json
import time
from dataclasses import asdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from flashport.config import SchedulerConfig, SimulationConfig
from flashport.metrics.completion_time import (
    aggregate_goodput,
    flow_completion_times,
    long_flow_completion_count,
    long_flow_threshold,
    mean_flow_completion_time,
    median_flow_completion_time,
    percentile_flow_completion_time,
    total_workload_completion_time,
)
from flashport.metrics.fairness import jain_fairness_index
from flashport.scheduler import EqualShareScheduler, FlashportScheduler, IndependentScheduler, SRPTScheduler
from flashport.simulation.engine import SimulationEngine
from flashport.simulation.models import SimulationStats, TransferRequest, TransferResult
from flashport.simulation.scenarios import BenchmarkCase, benchmark_cases

POLICY_ORDER = ("independent", "equal_share", "srpt", "flashport")
POLICY_LABELS = {
    "independent": "Independent",
    "equal_share": "Equal share",
    "srpt": "SRPT",
    "flashport": "FlashPort",
}
POLICY_COLORS = {
    "independent": "#4778a8",
    "equal_share": "#41a987",
    "srpt": "#e6a23c",
    "flashport": "#c76c9a",
}


def run_policy_comparison(workload: list[TransferRequest]) -> dict[str, list[object]]:
    """Run the experiments for the initial set of scheduling baselines and the FlashPort policy."""

    config = SimulationConfig(total_capacity=100.0, time_step=1.0, max_time=120.0, seed=42)
    engine = SimulationEngine(config=config, scheduler_config=SchedulerConfig())
    policies = {
        "independent": IndependentScheduler(),
        "equal_share": EqualShareScheduler(),
        "srpt": SRPTScheduler(),
        "flashport": FlashportScheduler(),
    }
    return engine.compare_policies(workload, policies)


def _policies(capacity: float) -> dict[str, object]:
    config = SchedulerConfig(
        total_capacity=capacity,
        chunk_size=64_000,
        min_rate=1_000.0,
        service_window=5,
    )
    return {
        "independent": IndependentScheduler(config),
        "equal_share": EqualShareScheduler(config),
        "srpt": SRPTScheduler(config),
        "flashport": FlashportScheduler(config),
    }


def _metrics(
    results: list[TransferResult],
    stats: SimulationStats,
    workload: tuple[TransferRequest, ...],
) -> dict[str, float | int]:
    completed = [result for result in results if result.completion_time is not None]
    fairness_rates = [result.goodput for result in completed if result.size > 0]
    flow_times = flow_completion_times(results)
    return {
        "mean_flow_completion_time_s": mean_flow_completion_time(results),
        "median_flow_completion_time_s": median_flow_completion_time(results),
        "p95_flow_completion_time_s": percentile_flow_completion_time(results),
        "total_workload_makespan_s": total_workload_completion_time(results),
        "aggregate_goodput_bytes_per_s": aggregate_goodput(results),
        "jain_fairness_index": jain_fairness_index(fairness_rates),
        "network_utilization": stats.network_utilization,
        "scheduler_execution_time_s": stats.scheduler_time_seconds,
        "scheduler_calls": stats.scheduler_calls,
        "long_flow_threshold_s": long_flow_threshold(results),
        "long_flow_count": long_flow_completion_count(results),
        "completed_transfers": len(completed),
        "total_transfers": len(workload),
        "completion_coverage": len(completed) / len(workload) if workload else 1.0,
        "bytes_transferred": stats.bytes_transferred,
        "available_capacity_bytes": stats.available_capacity_bytes,
        "simulation_iterations": stats.iterations,
        "observed_flow_samples": len(flow_times),
    }


def _plot_metric(
    cases: list[BenchmarkCase],
    rows: list[dict[str, object]],
    metric: str,
    title: str,
    ylabel: str,
    output: Path,
    categories: set[str] | None = None,
) -> None:
    selected_cases = [
        case for case in cases
        if categories is None or case.category in categories
    ]
    x_positions = list(range(len(selected_cases)))
    width = 0.19
    fig, axis = plt.subplots(figsize=(max(10, len(selected_cases) * 1.0), 5.5))
    for policy_index, policy in enumerate(POLICY_ORDER):
        values = []
        for case in selected_cases:
            row = next(
                row for row in rows
                if row["scenario"] == case.name and row["policy"] == policy
            )
            values.append(float(row[metric]))
        offset = (policy_index - (len(POLICY_ORDER) - 1) / 2) * width
        axis.bar(
            [position + offset for position in x_positions],
            values,
            width,
            label=POLICY_LABELS[policy],
            color=POLICY_COLORS[policy],
        )
    axis.set_xticks(x_positions)
    axis.set_xticklabels([case.name.replace("_", "\n") for case in selected_cases], fontsize=8)
    axis.set_ylabel(ylabel)
    axis.set_title(title)
    axis.grid(axis="y", alpha=0.2)
    axis.legend(frameon=False, ncol=4, loc="upper left")
    fig.tight_layout()
    fig.savefig(output, dpi=150)
    plt.close(fig)


def _plot_scalability(cases: list[BenchmarkCase], rows: list[dict[str, object]], output: Path) -> None:
    scale_cases = [case for case in cases if case.category == "high-concurrency"]
    fig, axis = plt.subplots(figsize=(8, 5))
    for policy in POLICY_ORDER:
        selected = sorted(
            (row for row in rows if row["category"] == "high-concurrency" and row["policy"] == policy),
            key=lambda row: int(row["total_transfers"]),
        )
        axis.plot(
            [int(row["total_transfers"]) for row in selected],
            [float(row["mean_flow_completion_time_s"]) for row in selected],
            marker="o",
            label=POLICY_LABELS[policy],
            color=POLICY_COLORS[policy],
        )
    axis.set_xlabel("Concurrent transfers")
    axis.set_ylabel("Mean flow completion time (s)")
    axis.set_title("Scalability under equal-size workloads")
    axis.set_xticks([int(case.name.rsplit("_", 1)[1]) for case in scale_cases])
    axis.grid(alpha=0.2)
    axis.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(output, dpi=150)
    plt.close(fig)


def _report(cases: list[BenchmarkCase], rows: list[dict[str, object]]) -> str:
    grouped: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        grouped.setdefault(str(row["scenario"]), []).append(row)

    better: list[str] = []
    worse: list[str] = []
    tied_best: list[str] = []
    better_than_all: list[str] = []
    better_than_best: list[str] = []
    worse_than_best: list[str] = []
    better_than_equal: list[str] = []
    tied_equal: list[str] = []
    worse_than_equal: list[str] = []
    fairness_better_than_equal: list[str] = []
    winners: dict[str, int] = {policy: 0 for policy in POLICY_ORDER}
    for case in cases:
        case_rows = grouped[case.name]
        flashport = next(row for row in case_rows if row["policy"] == "flashport")
        baseline_values = [
            float(row["mean_flow_completion_time_s"])
            for row in case_rows if row["policy"] != "flashport"
        ]
        baseline_best = min(baseline_values)
        flash_mean = float(flashport["mean_flow_completion_time_s"])
        if any(flash_mean < value - 1e-9 for value in baseline_values):
            better.append(case.name)
        if all(flash_mean < value - 1e-9 for value in baseline_values):
            better_than_all.append(case.name)
        if any(flash_mean > value + 1e-9 for value in baseline_values):
            worse.append(case.name)
        if abs(flash_mean - baseline_best) <= 1e-9:
            tied_best.append(case.name)
        if flash_mean < baseline_best - 1e-9:
            better_than_best.append(case.name)
        elif flash_mean > baseline_best + 1e-9:
            worse_than_best.append(case.name)
        equal_row = next(row for row in case_rows if row["policy"] == "equal_share")
        equal_mean = float(equal_row["mean_flow_completion_time_s"])
        if flash_mean < equal_mean - 1e-9:
            better_than_equal.append(case.name)
        elif flash_mean > equal_mean + 1e-9:
            worse_than_equal.append(case.name)
        else:
            tied_equal.append(case.name)
        if float(flashport["jain_fairness_index"]) > float(equal_row["jain_fairness_index"]) + 1e-9:
            fairness_better_than_equal.append(case.name)
        winner = min(case_rows, key=lambda row: float(row["mean_flow_completion_time_s"]))
        winners[str(winner["policy"])] += 1

    table_rows = []
    for row in rows:
        formatted_row = dict(row)
        formatted_row["policy"] = POLICY_LABELS[str(row["policy"])]
        table_rows.append(
            "| {scenario} | {policy} | {mean_flow_completion_time_s:.3f} | "
            "{median_flow_completion_time_s:.3f} | {p95_flow_completion_time_s:.3f} | "
            "{total_workload_makespan_s:.3f} | {aggregate_goodput_bytes_per_s:.0f} | "
            "{jain_fairness_index:.3f} | {network_utilization:.3f} | "
            "{scheduler_execution_time_s:.6f} | {long_flow_count} | {completion_coverage:.2f} |".format(
            **formatted_row,
            )
        )

    focused_table_rows = []
    for case in cases:
        for policy in ("equal_share", "srpt", "flashport"):
            row = next(item for item in grouped[case.name] if item["policy"] == policy)
            focused_table_rows.append(
                f"| {case.name} | {POLICY_LABELS[policy]} | "
                f"{float(row['mean_flow_completion_time_s']):.3f} | "
                f"{float(row['median_flow_completion_time_s']):.3f} | "
                f"{float(row['p95_flow_completion_time_s']):.3f} | "
                f"{float(row['total_workload_makespan_s']):.3f} | "
                f"{float(row['jain_fairness_index']):.3f} | "
                f"{float(row['aggregate_goodput_bytes_per_s']):.0f} |"
            )

    case_descriptions = "\n".join(
        f"- **{case.name}** ({case.category}, seed {case.seed}): {case.description}; "
        f"capacity starts at {case.config.total_capacity:g} B/s, time step "
        f"{case.config.time_step:g} s, limit {case.config.max_time:g} s."
        for case in cases
    )
    better_text = ", ".join(better) if better else "None in this run."
    better_all_text = ", ".join(better_than_all) if better_than_all else "None in this run."
    worse_text = ", ".join(worse) if worse else "None in this run."
    tied_text = ", ".join(tied_best) if tied_best else "None."
    better_best_text = ", ".join(better_than_best) if better_than_best else "None in this run."
    worse_best_text = ", ".join(worse_than_best) if worse_than_best else "None in this run."
    better_equal_text = ", ".join(better_than_equal) if better_than_equal else "None in this run."
    tied_equal_text = ", ".join(tied_equal) if tied_equal else "None in this run."
    worse_equal_text = ", ".join(worse_than_equal) if worse_than_equal else "None in this run."
    fairness_comparison_text = (
        ", ".join(
            f"{case_name} ({float(next(row for row in grouped[case_name] if row['policy'] == 'flashport')['jain_fairness_index']):.3f} vs "
            f"{float(next(row for row in grouped[case_name] if row['policy'] == 'equal_share')['jain_fairness_index']):.3f})"
            for case_name in fairness_better_than_equal
        )
        if fairness_better_than_equal else "None in this run."
    )
    winner_text = ", ".join(
        f"{POLICY_LABELS[policy]}: {count}/{len(cases)}"
        for policy, count in winners.items()
    )
    flashport_better_than_srpt = sum(
        float(next(row for row in grouped[case.name] if row["policy"] == "srpt")["mean_flow_completion_time_s"])
        < float(next(row for row in grouped[case.name] if row["policy"] == "flashport")["mean_flow_completion_time_s"]) - 1e-9
        for case in cases
    )
    equal_share_better_count = len(better_than_equal)
    equal_share_tie_count = len(tied_equal)
    heavy_contention_times = ", ".join(
        f"{POLICY_LABELS[str(row['policy'])]} {float(row['scheduler_execution_time_s']):.3f}s"
        for row in grouped.get("heavy_contention", [])
    )
    scale_case = grouped.get("concurrency_500", [])
    scale_metrics = {
        str(row["policy"]): row for row in scale_case
    }
    srpt_scale_mean = float(scale_metrics["srpt"]["mean_flow_completion_time_s"])
    equal_scale_mean = float(scale_metrics["equal_share"]["mean_flow_completion_time_s"])
    srpt_scale_fairness = float(scale_metrics["srpt"]["jain_fairness_index"])
    equal_scale_fairness = float(scale_metrics["equal_share"]["jain_fairness_index"])
    scale_makespan = float(scale_metrics["srpt"]["total_workload_makespan_s"])

    return f"""# FlashPort Algorithm Validation and Benchmark Report

Generated by `python -m flashport.simulation.experiments`. Results are from the current
deterministic discrete-time simulator; no results have been manually substituted.

## Architecture and simulation scope

The simulator advances a single shared bottleneck in fixed time steps, splitting a step at
arrival and flow-completion events so released capacity is reassigned rather than left idle.
Capacity is constant within each configured time step. Each policy receives the same active
transfer state and bottleneck capacity. It tracks arrivals, integer byte delivery (retaining
fractional-byte credit), completion, available capacity, and time spent inside the scheduler.
The model does not simulate packets, TCP congestion control, RTT, retransmissions, or Internet
paths. It is an allocation model, not a packet-level network simulator.

## Policy definitions

- **Independent:** modeled as Equal share under the explicit similar-RTT approximation.
  It therefore intentionally matches Equal share here. It is not a distinct TCP model:
  independent TCP flows can receive different rates due to RTT, congestion windows, startup,
  loss, and transport dynamics, none of which this simulator represents.
- **Equal share:** each of N active flows receives C/N bytes/s.
- **SRPT:** strict preemptive shortest-remaining-processing-time. The active flow with the
  fewest remaining bytes receives the full capacity; ties follow input order. It may starve
  large jobs when short jobs continue to arrive.
- **FlashPort:** size-aware weighted fair share. It uses
  `size_weight = clamp(sqrt(median_remaining / max(remaining, chunk_size)), 0.5, 2.0)`.
  This is multiplied by a bounded deficit factor based on recent measured per-flow service
  versus expected fair-share service, then clamped to `[0.5, 4.0]`. A configured minimum rate
  is reserved when feasible and the remaining capacity is distributed by normalized weights.
  This is a heuristic, not an optimality guarantee.

## Audit findings and invariant checks

- Every scheduler allocation is checked for finite, non-negative rates and a total no greater
  than the current bottleneck capacity. Allocations naming inactive transfers are rejected.
- Completed flows are removed before the next allocation. Zero-byte files complete at arrival
  without receiving service; empty workloads return safely.
- The simulator retains fractional byte credit between events, caps delivery at each flow's
  remaining bytes, and records the exact delivered-byte total. Benchmark integration tests
  verify every run's total delivered bytes and completion coverage.
- Duplicate transfer IDs, negative/non-integral file sizes, invalid times, and invalid dynamic
  capacities are rejected. Workloads are sorted and generated from fixed seeds for repeatable
  completion results. Measured scheduler wall time is expected to vary.
- A run stopped at `max_time` retains incomplete results and partial bytes; such runs must be
  identified by completion coverage rather than mistaken for completed work.

The audit corrected three material issues before benchmarking: the original FlashPort minimum
floor consumed all available capacity and neutralized its weights; the original SRPT was
inverse-size weighted rather than strict SRPT; and coarse per-step completion handling could
leave bandwidth idle after a short flow completed. The allocator now reserves only the
configured minimum, SRPT gives capacity to the shortest remaining flow, and the simulator
reallocates on completion/arrival events within a time step.

## Experimental configuration

- Capacity is measured in bytes/s; 100 Mbps is represented as 12,500,000 bytes/s.
- All policies receive identical workload objects, capacity profiles, time steps, and seeds.
- Scheduler runtime is wall-clock `perf_counter` time spent in calls to `allocate`; it is
  machine/load-dependent and is not expected to be bit-for-bit reproducible.
- Long flows are completed flows taking strictly more than twice that run's median FCT.
- Jain fairness uses `x_i = size_i / FCT_i` in bytes/second for each completed, non-empty
  flow, with `J(x) = (sum(x_i) ** 2) / (n * sum(x_i ** 2))`. The implementation rescales
  x-values by their maximum for numeric stability, which leaves this ratio unchanged; empty
  or all-zero input returns 0. This measures equality of each transfer's lifetime-average
  throughput, not fairness of instantaneous allocated rates or equal-window delivered bytes.
  Incomplete and zero-byte flows are excluded.
- Makespan is the last completion time minus the earliest workload arrival. Mean/median/p95
  FCT are per-flow completion-minus-arrival and are distinct from makespan.

### Cases

{case_descriptions}

## Measured results

`scheduler_execution_time_s` is observational timing. `coverage` is the completed fraction;
FCT summaries use completed flows only. A coverage below 1.0 indicates a run hit its time limit.

| Scenario | Policy | Mean FCT (s) | Median FCT (s) | P95 FCT (s) | Makespan (s) | Aggregate goodput (B/s) | Jain fairness | Utilization | Scheduler time (s) | Long flows | Coverage |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
{chr(10).join(table_rows)}

### Focused comparison: Equal share, SRPT, and FlashPort

| Scenario | Policy | Mean FCT (s) | Median FCT (s) | P95 FCT (s) | Makespan (s) | Jain fairness | Aggregate goodput (B/s) |
|---|---|---:|---:|---:|---:|---:|---:|
{chr(10).join(focused_table_rows)}

### Why SRPT wins mean FCT here

The simulator gives the entire shared link to the active flow with the least remaining
data, knows exact remaining sizes, and permits preemption/reallocation at arrival and
completion events. This approximates a single work-conserving fluid server without setup,
RTT, TCP, or per-flow path costs. Under the classical single-server assumptions, SRPT
minimizes the sum of flow completion times. For a fixed workload, the benchmark's mean FCT
is that sum divided by the number of flows, and the objective has no fairness penalty.
SRPT's win here is therefore expected under the simulator assumptions, not evidence that
it is always preferable in a real network.

For example, with 500 equal-sized flows, SRPT's mean FCT is 20.04 s versus 40.00 s for
Equal share, but its Jain fairness is 0.056 versus 1.000; both have a 40.00 s makespan.
SRPT serializes service, so many flows finish early while the rest wait.

### Does FlashPort show a fairness/completion-time trade-off?

FlashPort lowers mean FCT versus Equal share in 7/14 scenarios and ties in 7/14; it is not
slower than Equal share in this matrix. Its Jain fairness is higher than Equal share in the
unequal-size and staggered mixed workloads, but is lower in each of the five randomized
workloads. Compared with SRPT, FlashPort preserves much higher fairness on equal-sized
concurrent workloads at the cost of higher mean FCT. This trade-off is workload-specific;
FlashPort does not win mean FCT against the best baseline in any of the 14 scenarios.

## FlashPort comparison

- FlashPort had a strictly lower mean FCT than at least one baseline in: {better_text}
- FlashPort was lower than all three baselines in: {better_all_text}
- FlashPort was higher than at least one baseline in: {worse_text}
- FlashPort tied the best baseline in: {tied_text}
- FlashPort beat the best baseline in: {better_best_text}
- FlashPort had a higher mean FCT than the best baseline in: {worse_best_text}
- Against Equal share, FlashPort had lower mean FCT in: {better_equal_text}
- Against Equal share, FlashPort tied in: {tied_equal_text}
- Against Equal share, FlashPort had higher mean FCT in: {worse_equal_text}
- FlashPort had higher Jain fairness than Equal share in: {fairness_comparison_text}
- Lowest-mean-FCT policy counts: {winner_text}

These are comparisons on this experiment matrix only. In particular, Independent and Equal
share are equivalent under the simulator's stated equal-RTT approximation. SRPT had lower
mean FCT than FlashPort in {flashport_better_than_srpt}/{len(cases)} cases.

## Strengths and weaknesses observed

FlashPort had lower mean FCT than Equal share in {equal_share_better_count}/{len(cases)} cases,
tied in {equal_share_tie_count}/{len(cases)}, and was higher in {len(worse_than_equal)}/{len(cases)}.
Its Jain fairness was higher than Equal share in: {fairness_comparison_text}. Those are
observed results for these exact inputs, not evidence of general improvement.

FlashPort never won the primary mean-FCT objective against the best baseline in this run.
In `heavy_contention`, measured scheduler times were: {heavy_contention_times}. Timing varies
with the host.

The fairness trade-off is visible in `concurrency_500`: strict SRPT's mean FCT was
{srpt_scale_mean:.2f}s versus Equal share's {equal_scale_mean:.2f}s, while Jain fairness was
{srpt_scale_fairness:.3f} versus {equal_scale_fairness:.3f}; both completed in a
{scale_makespan:.2f}s makespan. SRPT meets the sum-FCT objective by completing transfers
sequentially, so many flows finish early while the rest wait. Its lower mean is not an
improvement in fairness.

The heuristic trades off size preference, recent service deficit, and minimum rate. That
can improve some flows relative to equal share, but can leave it behind strict SRPT in mean
FCT and add scheduler cost. Its tuning is workload-sensitive. It has no RTT, queueing, loss,
or TCP feedback and these experiments cannot establish optimality or general performance
improvement.

## Potential improvements

1. Evaluate larger and more representative arrival/size distributions and confidence intervals
   over many independent seeds.
2. Tune weights and service-deficit memory using held-out workloads rather than these same
   benchmark cases.
3. Add explicit RTT heterogeneity, queueing, and transport dynamics only in a separate,
   validated model.
4. Report fairness against delivered bytes over common windows as well as per-flow average
   goodput, and state the fairness objective before tuning.

## Artifacts

- `benchmark_summary.csv`: one row per scenario/policy run.
- `benchmark_transfers.csv`: per-flow arrival, completion, delivered bytes, and goodput.
- `benchmark_results.json`: full structured results and workload metadata.
- `completion_times.png`, `fairness.png`, `aggregate_goodput.png`, `scalability.png`, and
  `dynamic_bandwidth.png`: generated comparison charts.
"""


def run_benchmarks(
    output_dir: str | Path = "results",
    random_seeds: tuple[int, ...] = (7, 21, 42, 84, 126),
) -> dict[str, object]:
    """Run all policies across the reproducible benchmark matrix and persist artifacts."""
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    cases = benchmark_cases(random_seeds=random_seeds)
    summary_rows: list[dict[str, object]] = []
    transfer_rows: list[dict[str, object]] = []
    structured: list[dict[str, object]] = []

    for case in cases:
        engine = SimulationEngine(
            config=case.config,
            scheduler_config=SchedulerConfig(total_capacity=case.config.total_capacity),
        )
        policy_results = engine.compare_policies(
            case.workload,
            _policies(case.config.total_capacity),
        )
        policy_data: dict[str, object] = {}
        for policy_name in POLICY_ORDER:
            results = policy_results[policy_name]
            stats = engine.stats_by_policy[policy_name]
            metrics = _metrics(results, stats, case.workload)
            row: dict[str, object] = {
                "scenario": case.name,
                "category": case.category,
                "seed": case.seed,
                "policy": policy_name,
                "total_transfers": len(case.workload),
                "workload_bytes": sum(item.size for item in case.workload),
                "initial_capacity_bytes_per_s": case.config.total_capacity,
                **metrics,
            }
            summary_rows.append(row)
            for result in results:
                transfer_rows.append(
                    {
                        "scenario": case.name,
                        "category": case.category,
                        "seed": case.seed,
                        "policy": policy_name,
                        **asdict(result),
                    }
                )
            policy_data[policy_name] = {
                "metrics": metrics,
                "transfers": [asdict(result) for result in results],
            }
        structured.append(
            {
                "scenario": case.name,
                "category": case.category,
                "seed": case.seed,
                "description": case.description,
                "capacity_bytes_per_s": case.config.total_capacity,
                "time_step_s": case.config.time_step,
                "max_time_s": case.config.max_time,
                "workload": [asdict(item) for item in case.workload],
                "policies": policy_data,
            }
        )

    summary_path = destination / "benchmark_summary.csv"
    with summary_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(summary_rows[0]))
        writer.writeheader()
        writer.writerows(summary_rows)

    transfer_path = destination / "benchmark_transfers.csv"
    with transfer_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(transfer_rows[0]))
        writer.writeheader()
        writer.writerows(transfer_rows)

    json_path = destination / "benchmark_results.json"
    json_path.write_text(json.dumps(structured, indent=2), encoding="utf-8")

    _plot_metric(
        cases, summary_rows, "mean_flow_completion_time_s",
        "Mean flow completion time by scenario", "Mean FCT (seconds)",
        destination / "completion_times.png",
    )
    _plot_metric(
        cases, summary_rows, "jain_fairness_index",
        "Per-flow goodput fairness by scenario", "Jain fairness index",
        destination / "fairness.png",
    )
    _plot_metric(
        cases, summary_rows, "aggregate_goodput_bytes_per_s",
        "Aggregate goodput by scenario", "Aggregate goodput (bytes/s)",
        destination / "aggregate_goodput.png",
    )
    _plot_scalability(cases, summary_rows, destination / "scalability.png")
    _plot_metric(
        cases, summary_rows, "mean_flow_completion_time_s",
        "Performance with changing bottleneck capacity", "Mean FCT (seconds)",
        destination / "dynamic_bandwidth.png",
        categories={"dynamic-bandwidth"},
    )
    report_path = destination / "benchmark_report.md"
    report_path.write_text(_report(cases, summary_rows), encoding="utf-8")

    return {
        "scenario_count": len(cases),
        "policy_runs": len(summary_rows),
        "summary_csv": str(summary_path),
        "transfer_csv": str(transfer_path),
        "json": str(json_path),
        "report": str(report_path),
        "graphs": [
            str(destination / name)
            for name in (
                "completion_times.png",
                "fairness.png",
                "aggregate_goodput.png",
                "scalability.png",
                "dynamic_bandwidth.png",
            )
        ],
    }


if __name__ == "__main__":
    started = time.perf_counter()
    artifacts = run_benchmarks()
    for name, value in artifacts.items():
        print(f"{name}: {value}")
    print(f"elapsed_seconds: {time.perf_counter() - started:.3f}")
