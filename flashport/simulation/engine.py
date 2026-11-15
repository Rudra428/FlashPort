from __future__ import annotations

from dataclasses import dataclass, field
from math import isfinite
from typing import Iterable
from time import perf_counter

from flashport.config import SchedulerConfig, SimulationConfig
from flashport.core.models import TransferState
from flashport.scheduler.base import Scheduler
from flashport.simulation.models import (
    SimulationStats,
    SimulationSummary,
    TransferRequest,
    TransferResult,
)


@dataclass
class SimulationEngine:
    """Discrete-time simulator for multiple parallel transfers sharing one bottleneck."""

    config: SimulationConfig
    scheduler_config: SchedulerConfig | None = None
    last_stats: SimulationStats | None = field(default=None, init=False)
    stats_by_policy: dict[str, SimulationStats] = field(default_factory=dict, init=False)

    def simulate(self, workload: Iterable[TransferRequest], policy: Scheduler) -> list[TransferResult]:
        self.last_stats = None
        transfer_map: dict[str, TransferState] = {}
        results: list[TransferResult] = []
        completed_ids: set[str] = set()
        bytes_transferred = 0
        available_capacity_bytes = 0.0
        scheduler_time_seconds = 0.0
        scheduler_calls = 0
        iterations = 0

        # Pre-sort arrivals to keep the simulation deterministic.
        ordered_workload = sorted(workload, key=lambda item: (item.arrival_time, item.transfer_id))
        for item in ordered_workload:
            if item.transfer_id in transfer_map:
                raise ValueError(f"duplicate transfer_id: {item.transfer_id}")
            transfer_map[item.transfer_id] = TransferState(
                transfer_id=item.transfer_id,
                client_id=item.client_id,
                filename=item.filename,
                size=item.size,
                downloaded=0,
                start_time=item.arrival_time,
                goodput=0.0,
            )
            if item.size == 0:
                transfer_map[item.transfer_id].mark_completed(item.arrival_time)
                completed_ids.add(item.transfer_id)
                results.append(
                    TransferResult(
                        transfer_id=item.transfer_id,
                        filename=item.filename,
                        size=0,
                        arrival_time=item.arrival_time,
                        completion_time=item.arrival_time,
                        flow_completion_time=0.0,
                        goodput=0.0,
                        bytes_transferred=0,
                    )
                )
        fractional_bytes = {transfer_id: 0.0 for transfer_id in transfer_map}

        step_index = 0
        while len(completed_ids) < len(transfer_map):
            current_time = round(step_index * self.config.time_step, 12)
            if current_time >= self.config.max_time:
                break
            interval = min(self.config.time_step, self.config.max_time - current_time)
            interval_end = round(current_time + interval, 12)
            capacity = self.config.capacity_at(current_time)
            sub_time = current_time
            tick_bytes: dict[str, int] = {}
            tick_active_time: dict[str, float] = {}
            while sub_time < interval_end - 1e-12:
                active = [
                    transfer
                    for transfer in transfer_map.values()
                    if transfer.transfer_id not in completed_ids
                    and transfer.remaining > 0
                    and transfer.start_time is not None
                    and transfer.start_time <= sub_time + 1e-12
                ]
                next_arrivals = [
                    transfer.start_time
                    for transfer in transfer_map.values()
                    if transfer.transfer_id not in completed_ids
                    and transfer.remaining > 0
                    and transfer.start_time is not None
                    and transfer.start_time > sub_time + 1e-12
                    and transfer.start_time < interval_end - 1e-12
                ]
                next_arrival = min(next_arrivals) if next_arrivals else None

                if not active:
                    next_event = next_arrival if next_arrival is not None else interval_end
                    duration = next_event - sub_time
                    available_capacity_bytes += capacity * duration
                    sub_time = next_event
                    continue

                for transfer in active:
                    transfer.mark_started(sub_time)
                scheduler_started = perf_counter()
                allocations = policy.allocate(active, capacity)
                scheduler_time_seconds += perf_counter() - scheduler_started
                scheduler_calls += 1
                if not isinstance(allocations, dict):
                    raise TypeError("scheduler.allocate must return a transfer_id-to-rate dict")
                active_ids = {transfer.transfer_id for transfer in active}
                if set(allocations) - active_ids:
                    raise ValueError("scheduler allocated bandwidth to an inactive transfer")
                if any(not isfinite(rate) or rate < 0 for rate in allocations.values()):
                    raise ValueError("scheduler allocations must be finite and non-negative")
                allocated_capacity = sum(allocations.values())
                if allocated_capacity > capacity + max(1.0, capacity) * 1e-10:
                    raise ValueError("scheduler allocations exceed the configured capacity")

                event_durations = [interval_end - sub_time]
                if next_arrival is not None:
                    event_durations.append(next_arrival - sub_time)
                for transfer in active:
                    rate = allocations.get(transfer.transfer_id, 0.0)
                    if rate > 0:
                        outstanding = transfer.remaining - fractional_bytes[transfer.transfer_id]
                        event_durations.append(max(0.0, outstanding / rate))
                duration = min(event_durations)
                if duration <= 1e-12:
                    raise RuntimeError("simulation made no progress during an allocation interval")

                available_capacity_bytes += capacity * duration
                completion_time = round(sub_time + duration, 12)
                for transfer in active:
                    transfer_id = transfer.transfer_id
                    rate = allocations.get(transfer_id, 0.0)
                    transfer.last_rate = rate
                    tick_active_time[transfer_id] = tick_active_time.get(transfer_id, 0.0) + duration
                    accrued = rate * duration + fractional_bytes[transfer_id]
                    if accrued >= transfer.remaining - 1e-7:
                        bytes_sent = transfer.remaining
                        fractional_bytes[transfer_id] = 0.0
                    else:
                        bytes_sent = int(accrued)
                        fractional_bytes[transfer_id] = accrued - bytes_sent
                    transfer.downloaded += bytes_sent
                    tick_bytes[transfer_id] = tick_bytes.get(transfer_id, 0) + bytes_sent
                    bytes_transferred += bytes_sent
                    if transfer.downloaded >= transfer.size:
                        transfer.mark_completed(completion_time)
                        completed_ids.add(transfer_id)
                        flow_duration = completion_time - transfer.start_time
                        results.append(
                            TransferResult(
                                transfer_id=transfer_id,
                                filename=transfer.filename,
                                size=transfer.size,
                                arrival_time=transfer.start_time,
                                completion_time=completion_time,
                                flow_completion_time=flow_duration,
                                goodput=transfer.size / flow_duration if flow_duration > 0 else 0.0,
                                bytes_transferred=transfer.downloaded,
                            )
                        )
                if completion_time <= sub_time:
                    raise RuntimeError("simulation event time did not advance")
                sub_time = completion_time

            service_window = getattr(getattr(policy, "config", None), "service_window", 5)
            for transfer_id, active_duration in tick_active_time.items():
                transfer = transfer_map[transfer_id]
                transfer.goodput = tick_bytes.get(transfer_id, 0) / active_duration
                transfer.service_history.append(transfer.goodput)
                if len(transfer.service_history) > service_window:
                    del transfer.service_history[:-service_window]
            iterations += 1
            step_index += 1

        for transfer in transfer_map.values():
            if transfer.transfer_id not in completed_ids:
                results.append(
                    TransferResult(
                        transfer_id=transfer.transfer_id,
                        filename=transfer.filename,
                        size=transfer.size,
                        arrival_time=transfer.start_time if transfer.start_time is not None else 0.0,
                        completion_time=None,
                        flow_completion_time=None,
                        goodput=transfer.goodput,
                        bytes_transferred=transfer.downloaded,
                    )
                )

        utilization = (
            bytes_transferred / available_capacity_bytes
            if available_capacity_bytes > 0
            else 0.0
        )
        utilization = min(1.0, utilization)
        self.last_stats = SimulationStats(
            bytes_transferred=bytes_transferred,
            available_capacity_bytes=available_capacity_bytes,
            network_utilization=utilization,
            scheduler_time_seconds=scheduler_time_seconds,
            scheduler_calls=scheduler_calls,
            iterations=iterations,
        )
        return sorted(results, key=lambda item: item.transfer_id)

    def compare_policies(self, workload: Iterable[TransferRequest], policies: dict[str, Scheduler]) -> dict[str, list[TransferResult]]:
        requests = list(workload)
        comparison: dict[str, list[TransferResult]] = {}
        self.stats_by_policy = {}
        for name, policy in policies.items():
            comparison[name] = self.simulate(requests, policy)
            if self.last_stats is not None:
                self.stats_by_policy[name] = self.last_stats
        return comparison

    def summarize(self, results_by_policy: dict[str, list[TransferResult]]) -> dict[str, SimulationSummary]:
        return {
            name: SimulationSummary(
                by_policy={name: values},
                total_workload=len(values),
                total_capacity=self.config.total_capacity,
            )
            for name, values in results_by_policy.items()
        }
