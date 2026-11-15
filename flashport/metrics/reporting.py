from __future__ import annotations

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except Exception:  # pragma: no cover - optional dependency handling
    plt = None


def compare_results(results_by_policy: dict[str, list[object]], output_path: str = "results/summary.png") -> None:
    """Create a simple bar chart comparing mean flow completion times across policies."""

    if plt is None:
        raise RuntimeError("matplotlib is required to generate the experiment summary plot")

    labels = list(results_by_policy)
    values = []
    for policy_results in results_by_policy.values():
        if not policy_results:
            values.append(0.0)
            continue
        flow_times = [float(r.flow_completion_time) for r in policy_results if r.flow_completion_time is not None]
        values.append(sum(flow_times) / len(flow_times) if flow_times else 0.0)

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(labels, values, color=["steelblue", "forestgreen", "tomato", "darkorchid"][: len(labels)])
    ax.set_ylabel("Mean flow completion time")
    ax.set_title("FlashPort scheduling comparison")
    fig.tight_layout()
    fig.savefig(output_path)
    plt.close(fig)
