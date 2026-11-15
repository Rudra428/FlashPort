import pytest

from flashport.metrics.completion_time import (
    aggregate_goodput,
    long_flow_completion_count,
    mean_flow_completion_time,
    percentile_flow_completion_time,
    total_workload_completion_time,
)
from flashport.metrics.fairness import jain_fairness_index
from flashport.simulation.models import TransferResult


def result(flow: float, start: float = 0.0, size: int = 100) -> TransferResult:
    return TransferResult(
        transfer_id=str(flow),
        filename="file",
        size=size,
        arrival_time=start,
        completion_time=start + flow,
        flow_completion_time=flow,
        goodput=size / flow if flow else 0.0,
        bytes_transferred=size,
    )


def test_jain_fairness_known_values_and_invalid_inputs() -> None:
    assert jain_fairness_index([5.0, 5.0]) == pytest.approx(1.0)
    assert jain_fairness_index([1.0, 0.0]) == pytest.approx(0.5)
    assert jain_fairness_index([1e308, 1e308]) == 1.0
    assert jain_fairness_index([]) == 0.0
    with pytest.raises(ValueError):
        jain_fairness_index([-1.0, 1.0])


def test_completion_metrics_distinguish_flow_time_from_makespan() -> None:
    results = [
        result(2.0, start=0.0),
        result(3.0, start=10.0),
        result(10.0, start=0.0),
    ]
    assert mean_flow_completion_time(results) == pytest.approx(5.0)
    assert total_workload_completion_time(results) == pytest.approx(13.0)
    assert aggregate_goodput(results) == pytest.approx(300 / 13)
    assert percentile_flow_completion_time(results, 95.0) == pytest.approx(9.3)
    assert long_flow_completion_count(results) == 1
