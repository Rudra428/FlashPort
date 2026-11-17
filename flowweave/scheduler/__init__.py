"""Scheduling engine factory and policy exports."""

from __future__ import annotations

from flowweave.config import SchedulerConfig
from flowweave.models import PolicyName
from flowweave.scheduler.base import Scheduler
from flowweave.scheduler.flowweave import FlowWeaveScheduler
from flowweave.scheduler.independent import IndependentScheduler
from flowweave.scheduler.max_min import MaxMinFairScheduler
from flowweave.scheduler.optimizer import (
    OptimizationResult,
    TransferLinkAllocation,
    optimize_allocations,
    solve_single_link_analytical,
)
from flowweave.scheduler.srpt import SRPTScheduler


def create_scheduler(
    policy: PolicyName | str,
    config: SchedulerConfig | None = None,
    horizon_s: float = 0.25,
) -> Scheduler:
    """Instantiate a scheduler by PolicyName."""
    policy_enum = PolicyName(policy)
    cfg = config or SchedulerConfig(policy=policy_enum)
    if policy_enum == PolicyName.INDEPENDENT:
        return IndependentScheduler(cfg)
    if policy_enum == PolicyName.MAX_MIN:
        return MaxMinFairScheduler(cfg)
    if policy_enum == PolicyName.SRPT:
        return SRPTScheduler(cfg, horizon_s=horizon_s)
    if policy_enum == PolicyName.FLOWWEAVE:
        return FlowWeaveScheduler(cfg, horizon_s=horizon_s)
    raise ValueError(f"Unsupported scheduling policy: {policy}")


__all__ = [
    "Scheduler",
    "IndependentScheduler",
    "MaxMinFairScheduler",
    "SRPTScheduler",
    "FlowWeaveScheduler",
    "OptimizationResult",
    "TransferLinkAllocation",
    "optimize_allocations",
    "solve_single_link_analytical",
    "create_scheduler",
]
