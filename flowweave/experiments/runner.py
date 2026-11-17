"""Reproducible multi-policy evaluation runner and chart generator."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from flowweave.models import ExperimentSummary, PolicyName
from flowweave.simulator import NetworkSimulator
from flowweave.simulator.scenarios import get_all_scenarios, high_concurrency_scale
from flowweave.simulator.topology import ScenarioSpec

ALL_POLICIES: list[PolicyName] = [
    PolicyName.INDEPENDENT,
    PolicyName.MAX_MIN,
    PolicyName.SRPT,
    PolicyName.FLOWWEAVE,
]


def _mean_link_utilization(timeline: list[dict[str, Any]]) -> float:
    vals = [
        u
        for pt in timeline
        for u in pt.get("link_utilization", {}).values()
        if isinstance(u, (int, float))
    ]
    return float(sum(vals) / len(vals)) if vals else 0.0


@dataclass
class ComparisonBundle:
    scenario: str
    description: str
    seed: int
    summaries: dict[str, ExperimentSummary]
    timelines: dict[str, list[dict[str, float]]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "description": self.description,
            "seed": self.seed,
            "policies": {p: s.to_dict() for p, s in self.summaries.items()},
            "timelines": self.timelines,
        }


def run_scenario_comparison(
    scenario: ScenarioSpec,
    policies: list[PolicyName] | None = None,
) -> ComparisonBundle:
    """Run all requested policies under the exact same scenario and seed."""
    selected = policies or ALL_POLICIES
    summaries: dict[str, ExperimentSummary] = {}
    timelines: dict[str, list[dict[str, float]]] = {}

    for policy in selected:
        res = NetworkSimulator(scenario, policy).run()
        if res.capacity_violations > 0:
            raise AssertionError(
                f"Capacity invariant violated ({res.capacity_violations}) in "
                f"{scenario.name} under {policy.value}"
            )
        summaries[policy.value] = res.summary
        timelines[policy.value] = res.timeline

    return ComparisonBundle(
        scenario=scenario.name,
        description=scenario.description,
        seed=scenario.seed,
        summaries=summaries,
        timelines=timelines,
    )


def run_full_benchmark(
    output_dir: str | Path = "results",
    seed: int = 42,
    generate_charts: bool = True,
) -> list[ComparisonBundle]:
    """Execute the full scenario matrix + scalability sweep and persist reproducible outputs."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    scenarios = list(get_all_scenarios(seed=seed).values())
    bundles = [run_scenario_comparison(sc) for sc in scenarios]

    # Additional scalability sweep across 5, 10, 20, 40 concurrent transfers.
    scale_counts = [5, 10, 20, 40]
    scale_bundles = [
        run_scenario_comparison(high_concurrency_scale(n, seed=seed))
        for n in scale_counts
    ]

    # Save structured JSON (all scenarios + scale sweep).
    payload = {
        "seed": seed,
        "scenarios": [b.to_dict() for b in bundles],
        "scalability": [b.to_dict() for b in scale_bundles],
    }
    (out / "experiment_results.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")

    # Save flat CSV summary table.
    csv_path = out / "experiment_summary.csv"
    fieldnames = [
        "scenario",
        "policy",
        "seed",
        "num_transfers",
        "avg_fct_s",
        "sum_fct_s",
        "p95_fct_s",
        "makespan_s",
        "aggregate_goodput_mbps",
        "jains_fairness",
        "mean_link_utilization",
        "redundant_bytes",
        "redundant_ratio",
        "scheduler_overhead_ms_avg",
        "replications_triggered",
        "replications_won",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for bundle in bundles + scale_bundles:
            for p_name, s in bundle.summaries.items():
                writer.writerow(
                    {
                        "scenario": s.scenario,
                        "policy": s.policy,
                        "seed": s.seed,
                        "num_transfers": s.num_transfers,
                        "avg_fct_s": round(s.avg_fct_s, 4),
                        "sum_fct_s": round(s.sum_fct_s, 4),
                        "p95_fct_s": round(s.p95_fct_s, 4),
                        "makespan_s": round(s.makespan_s, 4),
                        "aggregate_goodput_mbps": round(s.aggregate_goodput_bps / (1024 * 1024), 4),
                        "jains_fairness": round(s.jains_fairness, 4),
                        "mean_link_utilization": round(_mean_link_utilization(bundle.timelines.get(p_name, [])), 4),
                        "redundant_bytes": s.redundant_bytes,
                        "redundant_ratio": round(s.redundant_ratio, 5),
                        "scheduler_overhead_ms_avg": round(s.scheduler_overhead_ms_avg, 4),
                        "replications_triggered": s.replications_triggered,
                        "replications_won": s.replications_won,
                    }
                )

    # Save Markdown summary report directly from measured outputs.
    _write_markdown_report(out / "experiment_report.md", bundles, scale_bundles, seed)

    if generate_charts:
        _generate_charts(out, bundles, scale_bundles)

    return bundles


def _write_markdown_report(
    path: Path,
    bundles: list[ComparisonBundle],
    scale_bundles: list[ComparisonBundle],
    seed: int,
) -> None:
    lines = [
        "# FlowWeave Experiment Report",
        "",
        f"Reproducible simulation benchmark (`seed={seed}`). All {len(ALL_POLICIES)} scheduling policies were evaluated under identical topologies, file sizes, arrival schedules, and deterministic link impairment streams.",
        "",
    ]
    for bundle in bundles:
        lines.append(f"## Scenario: `{bundle.scenario}`")
        lines.append("")
        lines.append(bundle.description)
        lines.append("")
        lines.append(
            "| Policy | Sum FCT (s) | Avg FCT (s) | P95 FCT (s) | Makespan (s) | Goodput (MB/s) | Jain Fairness | Link Util | Redundant KB | Sched Avg (ms) |"
        )
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        for p in [pol.value for pol in ALL_POLICIES]:
            s = bundle.summaries[p]
            util_pct = _mean_link_utilization(bundle.timelines.get(p, [])) * 100.0
            lines.append(
                f"| `{p}` | {s.sum_fct_s:.2f} | {s.avg_fct_s:.2f} | {s.p95_fct_s:.2f} | "
                f"{s.makespan_s:.2f} | {s.aggregate_goodput_bps / (1024 * 1024):.2f} | "
                f"{s.jains_fairness:.3f} | {util_pct:.1f}% | {s.redundant_bytes / 1024:.1f} | {s.scheduler_overhead_ms_avg:.4f} |"
            )
        lines.append("")

    lines.append("## Scalability Sweep (Concurrent Transfers)")
    lines.append("")
    lines.append("| Transfers | Independent | Max-Min | SRPT | FlowWeave | FlowWeave Sched Avg (ms) |")
    lines.append("|---:|---:|---:|---:|---:|---:|")
    for bundle in scale_bundles:
        n = bundle.summaries["flowweave"].num_transfers
        lines.append(
            f"| {n} | {bundle.summaries['independent'].sum_fct_s:.2f} | "
            f"{bundle.summaries['max_min'].sum_fct_s:.2f} | "
            f"{bundle.summaries['srpt'].sum_fct_s:.2f} | "
            f"{bundle.summaries['flowweave'].sum_fct_s:.2f} | "
            f"{bundle.summaries['flowweave'].scheduler_overhead_ms_avg:.4f} |"
        )
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def _generate_charts(
    out: Path,
    bundles: list[ComparisonBundle],
    scale_bundles: list[ComparisonBundle],
) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    policy_order = [p.value for p in ALL_POLICIES]
    colors = {
        "independent": "#94a3b8",
        "max_min": "#3b82f6",
        "srpt": "#f59e0b",
        "flowweave": "#10b981",
    }
    labels = {
        "independent": "Independent",
        "max_min": "Max-Min Fair",
        "srpt": "SRPT",
        "flowweave": "FlowWeave (Convex)",
    }

    # 1. Sum of Completion Times across scenarios
    fig, ax = plt.subplots(figsize=(12, 5))
    x = np.arange(len(bundles))
    width = 0.2
    for idx, pol in enumerate(policy_order):
        vals = [b.summaries[pol].sum_fct_s for b in bundles]
        ax.bar(x + (idx - 1.5) * width, vals, width, label=labels[pol], color=colors[pol])
    ax.set_ylabel("Sum of Completion Times Σ T_i (s) — lower is better")
    ax.set_title("FlowWeave vs Baselines: Total Completion Time Sum Across Scenarios")
    ax.set_xticks(x)
    ax.set_xticklabels([b.scenario for b in bundles], rotation=18, ha="right")
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out / "sum_fct_comparison.png", dpi=140)
    plt.close(fig)

    # 2. Fairness vs Average FCT across scenarios
    fig, axes = plt.subplots(1, 2, figsize=(12.5, 4.8))
    for idx, pol in enumerate(policy_order):
        avg_fcts = [b.summaries[pol].avg_fct_s for b in bundles]
        fairness = [b.summaries[pol].jains_fairness for b in bundles]
        axes[0].bar(x + (idx - 1.5) * width, avg_fcts, width, label=labels[pol], color=colors[pol])
        axes[1].bar(x + (idx - 1.5) * width, fairness, width, label=labels[pol], color=colors[pol])
    axes[0].set_title("Average Flow Completion Time (s)")
    axes[0].set_xticks(x)
    axes[0].set_xticklabels([b.scenario for b in bundles], rotation=22, ha="right")
    axes[0].grid(axis="y", linestyle="--", alpha=0.4)
    axes[1].set_title("Jain's Fairness Index (closer to 1 is fairer)")
    axes[1].set_ylim(0.0, 1.1)
    axes[1].set_xticks(x)
    axes[1].set_xticklabels([b.scenario for b in bundles], rotation=22, ha="right")
    axes[1].grid(axis="y", linestyle="--", alpha=0.4)
    axes[1].legend(loc="lower right")
    fig.tight_layout()
    fig.savefig(out / "fairness_and_avg_fct.png", dpi=140)
    plt.close(fig)

    # 3. Dynamic bandwidth drop timeline
    dyn = next((b for b in bundles if b.scenario == "dynamic_bandwidth_drop"), None)
    if dyn is not None:
        fig, ax = plt.subplots(figsize=(10, 4.5))
        for pol in policy_order:
            tl = dyn.timelines[pol]
            ts = [pt["time_s"] for pt in tl]
            gp = [pt["aggregate_goodput_bps"] / (1024 * 1024) for pt in tl]
            ax.plot(ts, gp, label=labels[pol], color=colors[pol], linewidth=2.0)
        ax.axvline(2.0, color="#ef4444", linestyle="--", alpha=0.7, label="Primary drop (t=2s)")
        ax.axvline(7.0, color="#6366f1", linestyle="--", alpha=0.7, label="Recovery (t=7s)")
        ax.set_xlabel("Simulation Time (s)")
        ax.set_ylabel("Aggregate Goodput (MB/s)")
        ax.set_title("Dynamic Network Conditions: Aggregate Goodput Over Time")
        ax.grid(True, linestyle="--", alpha=0.35)
        ax.legend()
        fig.tight_layout()
        fig.savefig(out / "dynamic_goodput.png", dpi=140)
        plt.close(fig)

    # 4. Scalability sweep
    fig, ax = plt.subplots(figsize=(9, 4.5))
    counts = [b.summaries["flowweave"].num_transfers for b in scale_bundles]
    for pol in policy_order:
        vals = [b.summaries[pol].avg_fct_s for b in scale_bundles]
        ax.plot(counts, vals, marker="o", label=labels[pol], color=colors[pol], linewidth=2.2)
    ax.set_xlabel("Concurrent Transfers (N)")
    ax.set_ylabel("Average Completion Time (s)")
    ax.set_title("Scalability: Average FCT vs Number of Concurrent Transfers")
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out / "scalability.png", dpi=140)
    plt.close(fig)
