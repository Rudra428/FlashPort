"""Tests for the SimPy network simulator, capacity invariants, dynamic events, and policies."""

from __future__ import annotations

import pytest

from flowweave.models import PolicyName
from flowweave.simulator import NetworkSimulator
from flowweave.simulator.scenarios import (
    dynamic_bandwidth_drop,
    mixed_sizes_shared_bottleneck,
    multi_link_offload,
    straggler_recovery,
)


@pytest.mark.parametrize(
    "policy",
    [PolicyName.INDEPENDENT, PolicyName.MAX_MIN, PolicyName.SRPT, PolicyName.FLOWWEAVE],
)
def test_capacity_constraints_respected_for_all_policies(policy: PolicyName) -> None:
    scenario = mixed_sizes_shared_bottleneck(seed=42)
    result = NetworkSimulator(scenario, policy).run()
    assert result.capacity_violations == 0
    assert result.summary.completed_transfers == len(scenario.transfers)
    assert result.summary.sum_fct_s > 0


def test_flowweave_improves_sum_fct_over_independent_and_max_min() -> None:
    scenario = mixed_sizes_shared_bottleneck(seed=42)
    ind = NetworkSimulator(scenario, PolicyName.INDEPENDENT).run().summary
    mm = NetworkSimulator(scenario, PolicyName.MAX_MIN).run().summary
    fw = NetworkSimulator(scenario, PolicyName.FLOWWEAVE).run().summary

    assert fw.sum_fct_s < mm.sum_fct_s
    assert fw.sum_fct_s < ind.sum_fct_s
    assert fw.avg_fct_s < mm.avg_fct_s
    assert fw.redundant_bytes <= ind.redundant_bytes


def test_flowweave_multi_link_offload_outperforms_independent() -> None:
    scenario = multi_link_offload(seed=42)
    ind = NetworkSimulator(scenario, PolicyName.INDEPENDENT).run().summary
    fw = NetworkSimulator(scenario, PolicyName.FLOWWEAVE).run().summary

    assert fw.sum_fct_s < ind.sum_fct_s
    assert fw.makespan_s < ind.makespan_s
    assert fw.aggregate_goodput_bps > ind.aggregate_goodput_bps


def test_dynamic_bandwidth_drop_completes_cleanly() -> None:
    scenario = dynamic_bandwidth_drop(seed=42)
    for policy in PolicyName:
        res = NetworkSimulator(scenario, policy).run()
        assert res.capacity_violations == 0
        assert res.summary.completed_transfers == len(scenario.transfers)


def test_straggler_replication_triggers_in_simulation() -> None:
    scenario = straggler_recovery(seed=42)
    res_fw = NetworkSimulator(scenario, PolicyName.FLOWWEAVE).run()
    assert res_fw.capacity_violations == 0
    assert res_fw.summary.completed_transfers == len(scenario.transfers)
    assert res_fw.replications_triggered > 0
    assert res_fw.replications_won > 0
    # Redundant traffic stays bounded by straggler config budget.
    assert res_fw.summary.redundant_ratio < 0.20
