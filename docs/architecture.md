# FlowWeave Architecture

FlowWeave separates simulation, scheduling, telemetry, real HTTP transport, and presentation into independent modules that share one core set of domain models (`flowweave/models.py`).

## Module Map

```text
flowweave/
├── models.py          # Shared domain models (LinkState, TransferSnapshot, MeasuredMetrics, EstimatedMetrics, ...)
├── config.py          # Typed configuration dataclasses
├── metrics.py         # FCT statistics, makespan, aggregate goodput, Jain's fairness index
├── storage.py         # Thread-safe SQLite persistence (transfers, monitor history, experiment runs)
├── coordinator.py     # Live periodic control loop tying monitor -> scheduler -> transfer manager -> storage
├── cli.py             # CLI entrypoints (`serve`, `benchmark`, `live-benchmark`, `netem-script`)
├── simulator/         # SimPy discrete-event network simulator & benchmark scenarios
│   ├── topology.py    # LinkSpec, BandwidthEvent, TransferSpec, ScenarioSpec
│   ├── engine.py      # Shared-link capacity enforcement, loss/latency, chunk pipelining, replication
│   └── scenarios.py   # 6 reproducible benchmark topologies + scalability sweep
├── scheduler/         # Pluggable scheduling policies behind one common Scheduler interface
│   ├── base.py        # Scheduler ABC, effective capacity, remaining-time estimator
│   ├── independent.py # Policy 1: Uncoordinated greedy baseline
│   ├── max_min.py     # Policy 2: Max-min fair water-filling
│   ├── srpt.py        # Policy 3: Shortest Predicted Remaining Completion Time
│   └── flowweave.py   # Policy 4: Cooperative, congestion-aware fairness-guarded scheduler
├── monitor/           # Transfer & link telemetry (strictly separates measured vs estimated)
│   └── monitor.py
├── straggler/         # Cost-benefit slow-chunk detection and speculative replication policy
│   └── detector.py
├── transfer/          # Real async HTTP download engine (asyncio + aiohttp)
│   ├── rate_limiter.py # Live-adjustable async token bucket
│   ├── engine.py      # HTTP Range parallel chunks, retries, reassembly, SHA-256, single-stream fallback
│   ├── manager.py     # Multi-transfer orchestration and rate application
│   └── test_server.py # Embedded aiohttp HTTP Range server (plus /noranges fallback route)
├── experiments/       # Reproducible multi-policy evaluation & Linux tc/iperf3 harness
│   ├── runner.py      # Runs all 4 policies with identical seeds, outputs JSON/CSV/Markdown/PNGs
│   └── netem.py       # Linux `tc` (tbf + netem) and `iperf3 -J` integration + bash generator
└── api/
    └── app.py         # FastAPI REST backend + static frontend serving
```

## Key Architectural Decisions

### 1. Explicit Separation of Measured vs Estimated Metrics
`NetworkMonitor` maintains two separate dataclasses per transfer:
- `MeasuredMetrics`: raw, directly observed counters (`bytes_acked`, `raw_bytes_received`, `redundant_bytes`, `instant_goodput_bps`, `avg_goodput_bps`, `last_rtt_ms`, `retries`).
- `EstimatedMetrics`: EWMA-smoothed predictions (`ewma_goodput_bps`, `ewma_rtt_ms`, `predicted_remaining_time_s`, `estimated_bottleneck_bps`, `confidence`).

This prevents downstream components from mistaking noisy cold-start or starved EWMA values for ground truth—a critical safeguard that avoided SRPT priority inversion when paused flows decay toward zero observed goodput.

### 2. Shared Scheduler Interface Across Simulation and Live Downloads
Both `NetworkSimulator` (SimPy discrete-event ticks) and `FlowWeaveCoordinator` (real `asyncio` control loop) construct the exact same `SchedulerInput(now, transfers, links)` and consume the same `ScheduleDecision(allocations_bps, link_assignments)`. A policy tested in simulation runs unmodified against real HTTP downloads.

### 3. Live Token-Bucket Pacing Without Connection Churn
Rather than tearing down and reopening HTTP sockets whenever priorities change, each managed transfer owns a dynamic `AsyncRateLimiter`. On each control tick (`control_interval_s = 0.25s`), `TransferManager.apply_decision()` updates `limiter.set_rate(bps)` in place. Rate increases take effect within milliseconds while persistent TCP connections remain warm.

### 4. Conservative Straggler Handling in Real Transfers
Speculative chunk replication is fully active in the simulator (where alternate mirrors and controlled straggler events are modeled directly) and governed by a cost-benefit gate plus a redundant-byte cap. In the live HTTP engine, `StragglerConfig.allow_real_replication` defaults to `False`: stragglers are detected and surfaced in `/api/monitor` without generating speculative duplicate WAN traffic unless explicitly enabled.

### 5. SQLite Persistence
SQLite stores transfer records, rolling monitor samples (bounded to the latest 300 ticks), and experiment comparison summaries with zero external database setup.
