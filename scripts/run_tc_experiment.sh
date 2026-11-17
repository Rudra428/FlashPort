#!/usr/bin/env bash
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
