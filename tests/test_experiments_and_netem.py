"""Tests for the experiment runner and Linux tc / iperf3 utilities."""

from __future__ import annotations

import json

import pytest

from flowweave.experiments.netem import (
    NetworkProfile,
    apply_tc_profile,
    build_iperf3_command,
    build_tc_apply_commands,
    build_tc_clear_command,
    generate_linux_experiment_script,
    parse_iperf3_json,
)
from flowweave.experiments.runner import run_scenario_comparison
from flowweave.simulator.scenarios import mixed_sizes_shared_bottleneck


def test_run_scenario_comparison_returns_all_four_policies() -> None:
    bundle = run_scenario_comparison(mixed_sizes_shared_bottleneck(seed=123))
    assert set(bundle.summaries.keys()) == {"independent", "max_min", "srpt", "flowweave"}
    for policy, summary in bundle.summaries.items():
        assert summary.completed_transfers == summary.num_transfers
        assert summary.avg_fct_s > 0
        assert 0.0 < summary.jains_fairness <= 1.0


def test_tc_and_iperf3_command_builders_and_parser(tmp_path) -> None:
    profile = NetworkProfile(name="wan-test", interface="eth0", rate_mbit=50.0, delay_ms=30.0, loss_pct=1.2)
    cmds = build_tc_apply_commands(profile)
    assert len(cmds) == 3
    assert "50.00mbit" in cmds[1]
    assert "30.0ms" in cmds[2]
    assert "1.20%" in cmds[2]

    clear = build_tc_clear_command("eth0")
    assert clear == ["tc", "qdisc", "del", "dev", "eth0", "root"]

    dry = apply_tc_profile(profile, dry_run=True)
    assert len(dry) == 3

    iperf_cmd = build_iperf3_command("10.0.0.1", port=5201, duration_s=10, parallel=4)
    assert "-J" in iperf_cmd and "10.0.0.1" in iperf_cmd

    sample_iperf = json.dumps(
        {
            "end": {
                "sum_sent": {"bits_per_second": 80_000_000.0, "retransmits": 7, "seconds": 5.0},
                "sum_received": {"bits_per_second": 78_400_000.0, "seconds": 5.0},
                "streams": [{"sender": {"mean_rtt": 24_500}}],
            }
        }
    )
    parsed = parse_iperf3_json(sample_iperf)
    assert parsed.bits_per_second == pytest.approx(78_400_000.0)
    assert parsed.bytes_per_second == pytest.approx(9_800_000.0)
    assert parsed.retransmits == 7
    assert parsed.mean_rtt_ms == pytest.approx(24.5)

    script = generate_linux_experiment_script(tmp_path / "tc.sh")
    assert script.exists()
    content = script.read_text(encoding="utf-8")
    assert "tc qdisc add" in content and "iperf3" in content
