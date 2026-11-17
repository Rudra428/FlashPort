"""Linux traffic control (`tc` netem/tbf) and `iperf3` experiment harness."""

from __future__ import annotations

import json
import platform
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class NetworkProfile:
    """Traffic-shaping profile applied on Linux via `tc`."""

    name: str
    interface: str = "lo"
    rate_mbit: float = 80.0
    delay_ms: float = 25.0
    jitter_ms: float = 3.0
    loss_pct: float = 0.5
    burst_kb: int = 64


@dataclass
class Iperf3Measurement:
    """Parsed summary from `iperf3 -J` JSON output."""

    bits_per_second: float
    bytes_per_second: float
    retransmits: int
    mean_rtt_ms: float | None
    duration_s: float


def build_tc_apply_commands(profile: NetworkProfile) -> list[list[str]]:
    """Return the exact Linux `tc` commands to enforce bandwidth, delay, and loss."""
    return [
        ["tc", "qdisc", "del", "dev", profile.interface, "root"],
        [
            "tc",
            "qdisc",
            "add",
            "dev",
            profile.interface,
            "root",
            "handle",
            "1:",
            "tbf",
            "rate",
            f"{profile.rate_mbit:.2f}mbit",
            "burst",
            f"{profile.burst_kb}kbit",
            "latency",
            "50ms",
        ],
        [
            "tc",
            "qdisc",
            "add",
            "dev",
            profile.interface,
            "parent",
            "1:1",
            "handle",
            "10:",
            "netem",
            "delay",
            f"{profile.delay_ms:.1f}ms",
            f"{profile.jitter_ms:.1f}ms",
            "loss",
            f"{profile.loss_pct:.2f}%",
        ],
    ]


def build_tc_clear_command(interface: str = "lo") -> list[str]:
    """Return the command to remove root `tc` shaping rules."""
    return ["tc", "qdisc", "del", "dev", interface, "root"]


def build_iperf3_command(
    host: str = "127.0.0.1",
    port: int = 5201,
    duration_s: int = 5,
    parallel: int = 1,
) -> list[str]:
    """Build a JSON-emitting `iperf3` client command."""
    return [
        "iperf3",
        "-c",
        host,
        "-p",
        str(port),
        "-t",
        str(duration_s),
        "-P",
        str(parallel),
        "-J",
    ]


def parse_iperf3_json(raw_json: str) -> Iperf3Measurement:
    """Extract throughput, retransmits, and RTT from `iperf3 -J` output."""
    data: dict[str, Any] = json.loads(raw_json)
    end = data.get("end", {})
    sent = end.get("sum_sent", {})
    received = end.get("sum_received", sent)
    bps = float(received.get("bits_per_second", sent.get("bits_per_second", 0.0)))
    retransmits = int(sent.get("retransmits", 0))
    duration = float(received.get("seconds", sent.get("seconds", 0.0)))

    rtts: list[float] = []
    for stream in end.get("streams", []):
        sender = stream.get("sender", {})
        if "mean_rtt" in sender:
            # iperf3 reports mean_rtt in microseconds
            rtts.append(float(sender["mean_rtt"]) / 1000.0)
    mean_rtt_ms = sum(rtts) / len(rtts) if rtts else None

    return Iperf3Measurement(
        bits_per_second=bps,
        bytes_per_second=bps / 8.0,
        retransmits=retransmits,
        mean_rtt_ms=mean_rtt_ms,
        duration_s=duration,
    )


def apply_tc_profile(profile: NetworkProfile, *, dry_run: bool = False) -> list[str]:
    """Apply a `tc` profile on Linux (or return shell-formatted commands when dry_run=True)."""
    cmds = build_tc_apply_commands(profile)
    formatted = [" ".join(c) for c in cmds]
    if dry_run:
        return formatted

    if platform.system() != "Linux":
        raise RuntimeError(
            "Linux `tc` traffic shaping requires a Linux host with CAP_NET_ADMIN. "
            "Use dry_run=True or scripts/run_tc_experiment.sh on Linux."
        )
    if shutil.which("tc") is None:
        raise RuntimeError("`tc` binary not found in PATH (install iproute2).")

    # First command clears any existing qdisc; ignore failure if none existed.
    subprocess.run(cmds[0], check=False, capture_output=True)
    for cmd in cmds[1:]:
        subprocess.run(cmd, check=True, capture_output=True)
    return formatted


def generate_linux_experiment_script(output_path: str | Path = "scripts/run_tc_experiment.sh") -> Path:
    """Write a reproducible Linux bash script combining `tc`, `iperf3`, and FlowWeave."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    script = """#!/usr/bin/env bash
# Reproducible Linux network experiment for FlowWeave using `tc` (tbf + netem) and `iperf3`.
# Requires: Linux, sudo/CAP_NET_ADMIN, iproute2 (`tc`), iperf3, and Python 3.11+ with FlowWeave installed.
set -euo pipefail

IFACE="${1:-lo}"
RATE_MBIT="${2:-80}"
DELAY_MS="${3:-25}"
LOSS_PCT="${4:-0.5}"
RESULTS_DIR="${5:-results/linux_tc}"
mkdir -p "$RESULTS_DIR"

cleanup() {
  sudo tc qdisc del dev "$IFACE" root 2>/dev/null || true
  if [[ -n "${IPERF_PID:-}" ]]; then
    kill "$IPERF_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

echo "[1/4] Applying tc shaping on $IFACE: ${RATE_MBIT}mbit, ${DELAY_MS}ms delay, ${LOSS_PCT}% loss"
sudo tc qdisc del dev "$IFACE" root 2>/dev/null || true
sudo tc qdisc add dev "$IFACE" root handle 1: tbf rate "${RATE_MBIT}mbit" burst 64kbit latency 50ms
sudo tc qdisc add dev "$IFACE" parent 1:1 handle 10: netem delay "${DELAY_MS}ms" 3ms loss "${LOSS_PCT}%"

echo "[2/4] Measuring shaped bottleneck capacity with iperf3"
iperf3 -s -D -p 5201 --pidfile "$RESULTS_DIR/iperf3.pid"
IPERF_PID="$(cat "$RESULTS_DIR/iperf3.pid")"
sleep 1
iperf3 -c 127.0.0.1 -p 5201 -t 5 -P 4 -J > "$RESULTS_DIR/iperf3_baseline.json"

echo "[3/4] Running FlowWeave live transfer benchmark across all 4 policies"
python3 -m flowweave live-benchmark --rate-mbit "$RATE_MBIT" --out "$RESULTS_DIR/live_comparison.json"

echo "[4/4] Done. Results written to $RESULTS_DIR"
"""
    path.write_text(script, encoding="utf-8", newline="\n")
    return path
