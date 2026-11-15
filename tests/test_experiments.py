import csv
import json

from flashport.simulation.experiments import run_benchmarks
from flashport.simulation.scenarios import benchmark_cases


def test_benchmark_cases_are_reproducible_and_cover_required_scenarios() -> None:
    first = benchmark_cases(random_seeds=(7,))
    second = benchmark_cases(random_seeds=(7,))

    assert [
        (case.name, case.seed, case.workload)
        for case in first
    ] == [
        (case.name, case.seed, case.workload)
        for case in second
    ]
    names = {case.name for case in first}
    assert {
        "equal_10",
        "unequal_sizes",
        "concurrency_10",
        "concurrency_50",
        "concurrency_100",
        "concurrency_500",
        "mixed_staggered",
        "dynamic_bandwidth",
        "heavy_contention",
        "random_seed_7",
    } <= names
    unequal = next(case for case in first if case.name == "unequal_sizes")
    assert [transfer.size for transfer in unequal.workload] == [
        10_000_000,
        50_000_000,
        100_000_000,
        500_000_000,
        1_000_000_000,
    ]


def test_benchmark_run_writes_actual_results_and_visualizations(tmp_path) -> None:
    artifacts = run_benchmarks(tmp_path, random_seeds=(7,))

    assert artifacts["scenario_count"] == 10
    assert artifacts["policy_runs"] == 40
    for path in (
        "benchmark_summary.csv",
        "benchmark_transfers.csv",
        "benchmark_results.json",
        "benchmark_report.md",
        "completion_times.png",
        "fairness.png",
        "aggregate_goodput.png",
        "scalability.png",
        "dynamic_bandwidth.png",
    ):
        assert (tmp_path / path).is_file()
        assert (tmp_path / path).stat().st_size > 0

    with (tmp_path / "benchmark_summary.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 40
    assert all(float(row["completion_coverage"]) == 1.0 for row in rows)

    json_results = json.loads((tmp_path / "benchmark_results.json").read_text(encoding="utf-8"))
    assert len(json_results) == 10
    for case in json_results:
        assert set(case["policies"]) == {"independent", "equal_share", "srpt", "flashport"}
        expected_bytes = sum(transfer["size"] for transfer in case["workload"])
        for policy in case["policies"].values():
            assert sum(item["bytes_transferred"] for item in policy["transfers"]) == expected_bytes
            assert all(item["completion_time"] is not None for item in policy["transfers"])

    report = (tmp_path / "benchmark_report.md").read_text(encoding="utf-8")
    assert "Measured results" in report
    assert "no results have been manually substituted" in report
    assert "Focused comparison: Equal share, SRPT, and FlashPort" in report
    assert "`x_i = size_i / FCT_i`" in report
    assert "Why SRPT wins mean FCT here" in report
    assert "Does FlashPort show a fairness/completion-time trade-off?" in report
