from flashport.config import SchedulerConfig, SimulationConfig
from flashport.core.models import TransferState
from flashport.metrics.completion_time import total_workload_completion_time
from flashport.scheduler.equal_share import EqualShareScheduler
from flashport.scheduler.base import Scheduler
from flashport.simulation.engine import SimulationEngine
from flashport.simulation.models import TransferRequest
import pytest


def test_simulation_completes_transfers() -> None:
    config = SimulationConfig(total_capacity=100.0, time_step=1.0, max_time=50.0, seed=123)
    engine = SimulationEngine(config=config, scheduler_config=SchedulerConfig())
    workload = [
        TransferRequest("flow-1", "small.txt", 40, 0.0, "client-1"),
        TransferRequest("flow-2", "large.txt", 80, 0.0, "client-2"),
    ]
    results = engine.simulate(workload, EqualShareScheduler())
    assert len(results) == 2
    assert all(result.flow_completion_time is not None for result in results)
    assert all(result.completion_time is not None for result in results)


def test_simulation_applies_bandwidth_over_each_time_step() -> None:
    config = SimulationConfig(total_capacity=100_000.0, time_step=0.1, max_time=1.0)
    engine = SimulationEngine(config=config)
    workload = [TransferRequest("flow-1", "small.bin", 10_000, 0.0)]

    result = engine.simulate(workload, EqualShareScheduler())[0]

    assert result.flow_completion_time == 0.1
    assert result.goodput == 100_000.0
    assert result.bytes_transferred == result.size
    assert engine.last_stats is not None
    assert engine.last_stats.bytes_transferred == result.size
    assert engine.last_stats.network_utilization <= 1.0


def test_zero_size_transfers_complete_at_arrival_without_capacity() -> None:
    engine = SimulationEngine(
        SimulationConfig(total_capacity=0.0, time_step=1.0, max_time=5.0)
    )
    results = engine.simulate(
        [TransferRequest("empty", "empty.bin", 0, 3.0)],
        EqualShareScheduler(),
    )
    assert results[0].completion_time == 3.0
    assert results[0].flow_completion_time == 0.0
    assert results[0].bytes_transferred == 0
    assert engine.last_stats is not None
    assert engine.last_stats.scheduler_calls == 0


def test_empty_workload_is_safe_and_deterministic() -> None:
    engine = SimulationEngine(SimulationConfig())
    assert engine.simulate([], EqualShareScheduler()) == []
    assert engine.last_stats is not None
    assert engine.last_stats.network_utilization == 0.0


def test_duplicate_ids_and_invalid_requests_are_rejected() -> None:
    engine = SimulationEngine(SimulationConfig())
    transfer = TransferRequest("same", "x", 1, 0.0)
    with pytest.raises(ValueError, match="duplicate"):
        engine.simulate([transfer, transfer], EqualShareScheduler())
    with pytest.raises(ValueError, match="size"):
        TransferRequest("negative", "x", -1, 0.0)
    with pytest.raises(ValueError, match="size"):
        TransferRequest("fractional", "x", 1.5, 0.0)


def test_dynamic_capacity_is_accounted_and_completions_are_repeatable() -> None:
    config = SimulationConfig(
        total_capacity=100.0,
        time_step=1.0,
        max_time=10.0,
        capacity_profile=lambda current: 100.0 if current < 1.0 else 50.0,
    )
    engine = SimulationEngine(config)
    workload = [TransferRequest("flow", "file", 250, 0.0)]

    first = engine.simulate(workload, EqualShareScheduler())
    stats_first = engine.last_stats
    second = engine.simulate(workload, EqualShareScheduler())
    stats_second = engine.last_stats

    assert first == second
    assert first[0].bytes_transferred == 250
    assert first[0].completion_time == 4.0
    assert total_workload_completion_time(first) == 4.0
    assert stats_first is not None and stats_second is not None
    assert stats_first.available_capacity_bytes == 250.0
    assert stats_first.network_utilization == pytest.approx(1.0)
    assert stats_second.bytes_transferred == stats_first.bytes_transferred


def test_scheduler_cannot_allocate_over_capacity() -> None:
    class InvalidScheduler(Scheduler):
        def allocate(self, transfers, total_capacity=None):
            return {transfer.transfer_id: 2 * total_capacity for transfer in transfers}

    engine = SimulationEngine(SimulationConfig(total_capacity=100.0, time_step=1.0))
    with pytest.raises(ValueError, match="exceed"):
        engine.simulate(
            [TransferRequest("flow", "file", 1000, 0.0)],
            InvalidScheduler(),
        )


def test_completed_flow_is_not_scheduled_again() -> None:
    class RecordingScheduler(Scheduler):
        def __init__(self):
            super().__init__()
            self.calls = []

        def allocate(self, transfers, total_capacity=None):
            active = list(transfers)
            self.calls.append([transfer.transfer_id for transfer in active])
            return {transfer.transfer_id: total_capacity for transfer in active}

    scheduler = RecordingScheduler()
    engine = SimulationEngine(SimulationConfig(total_capacity=100.0, time_step=1.0))
    engine.simulate(
        [
            TransferRequest("first", "first.bin", 100, 0.0),
            TransferRequest("later", "later.bin", 100, 2.0),
        ],
        scheduler,
    )
    assert scheduler.calls == [["first"], ["later"]]
