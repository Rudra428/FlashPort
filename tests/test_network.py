import sys

import pytest

from flashport.network.estimator import NetworkEstimator
from flashport.network.monitor import NetworkMonitor
from flashport.network.traffic_control import TcProfile, build_tc_commands


def test_network_metrics_distinguish_configured_capacity_and_actual_completions() -> None:
    monitor = NetworkMonitor(configured_capacity_bps=1_000.0)
    monitor.record_server_bytes("a", 1_200)
    monitor.record_unique_served("a", 1_000)
    monitor.record_client_progress("a", 1_000, goodput_bps=500.0, request_latency_ms=12.0)
    monitor.record_completion(0, 1.0, 1.0)  # zero-byte flows do not enter fairness
    monitor.record_completion(1_000, 10.0, 12.0)

    snapshot = monitor.snapshot()

    assert snapshot["measurement_type"] == "actual_application"
    assert snapshot["configured_capacity_bps"] == 1_000.0
    assert snapshot["bytes_sent_by_server"] == 1_200
    assert snapshot["redundant_or_unconfirmed_range_bytes"] == 200
    assert snapshot["completed_aggregate_goodput_bps"] == 500.0
    assert snapshot["mean_flow_completion_time_s"] == 2.0
    assert snapshot["jain_fairness_index"] == 1.0
    assert snapshot["packet_loss_available"] is False
    assert snapshot["tcp_retransmissions_available"] is False


def test_network_estimator_labels_http_latency_not_transport_rtt() -> None:
    estimator = NetworkEstimator(window=3)
    estimator.record_request_latency(20.0)
    estimator.record_request_latency(12.0)
    estimator.record_request_latency(16.0)
    estimator.record_request_latency(40.0)

    assert estimator.estimate_rtt() == 16.0
    assert estimator.snapshot()["sample_count"] == 3


def test_tc_helper_only_builds_commands_and_validates_profiles() -> None:
    profile = TcProfile(bandwidth_mbit=20, delay_ms=15, packet_loss_percent=0.1)
    if sys.platform == "linux":
        apply, remove = build_tc_commands(profile, "veth-flash0")
        assert apply[:6] == ["sudo", "tc", "qdisc", "replace", "dev", "veth-flash0"]
        assert remove[-1] == "root"
    else:
        with pytest.raises(RuntimeError, match="Linux"):
            build_tc_commands(profile, "veth-flash0")
    with pytest.raises(ValueError):
        TcProfile(bandwidth_mbit=0)
    with pytest.raises(ValueError):
        TcProfile(bandwidth_mbit=10, packet_loss_percent=101)
