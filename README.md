# FlowWeave — Cooperative Bidirectional File-Transfer Scheduling System

**FlowWeave** is a cooperative, congestion-aware, bidirectional (upload + download) file-transfer scheduling system built in Python (`FastAPI`, `asyncio`, `aiohttp`, `SimPy`, `NumPy`, `SQLite`) and React (`TypeScript`, `Vite`, `Chart.js`).

Instead of letting simultaneous uploads and downloads compete blindly for shared bandwidth, FlowWeave monitors active transfers across both directions, separates directly observed measurements from smoothed estimates, and coordinates link placement and bandwidth allocation to minimize aggregate flow completion time:

$$\min \sum_{i=1}^{N} T_i$$

while protecting fairness, tail completion time ($P_{95}$), aggregate goodput, and bounded redundant transmissions.

---

## Quick Start

### 1. Install Python Backend Dependencies

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux / macOS:
source .venv/bin/activate

pip install -r requirements.txt
```

### 2. Build Frontend Dashboard (Already Built in `frontend/dist/`)

```bash
cd frontend
npm install
npm run build
cd ..
```

### 3. Start FlowWeave Locally

```bash
# Single command serves BOTH the FastAPI REST API and the built React Dashboard + embedded HTTP Range/Upload demo server:
python -m flowweave serve --host 127.0.0.1 --port 8080
```

Open:
- **Dashboard UI**: `http://127.0.0.1:8080`
- **Interactive API Docs (Swagger / OpenAPI)**: `http://127.0.0.1:8080/docs`

*(Optional: for hot-reload frontend development, run `npm --prefix frontend run dev` alongside FastAPI on port `8000`.)*

---

## Demonstration Walkthrough (No External Internet Required)

Sample binary files are provisioned automatically in `sample_files/` (`patch_512kb.bin`, `dataset_1mb.bin`, `archive_2mb.bin`, `checkpoint_4mb.bin`, `video_6mb.bin`, `demo_notes.txt`), backed by an embedded local `aiohttp` HTTP Range + Chunked Upload server:

1. Open **Overview** → adjust Downlink/Uplink caps (e.g., `3 MB/s` down, `2 MB/s` up) → click **Run Mixed Upload + Download Demo** to watch 4 concurrent downloads + 3 concurrent uploads execute simultaneously.
2. Open **All Transfers** to inspect unified bidirectional progress, worker counts, measured speeds, and test live **Pause / Resume / Cancel**.
3. Open **Uploads** to select real files from your laptop (`<input type="file">`) or sample files; each upload is split into parallel chunks, paced by the scheduler, reassembled, SHA-256 verified, and atomically committed to `uploads/`.
4. Open **Downloads** to test HTTP Range parallel downloads or the `/noranges/` single-stream fallback file.
5. Open **Network Monitor** to inspect separate Upload vs Download throughput charts, link utilization, congestion status, measured vs estimated EWMA rates, and straggler alerts.
6. Open **Scheduler** to switch policies live (`Independent`, `Max-Min Fair`, `SRPT`, `FlowWeave`) and inspect the live **Coordinator Decision Log**.
7. Open **Experiments** to run reproducible 4-policy SimPy comparisons across 9 scenarios (including `bidirectional_mixed`, `partially_shared_paths`, `independent_paths`, `dynamic_bandwidth_drop`, and `straggler_recovery`) with Chart.js visualizations clearly labeled as simulated fluid-flow results.

---

## Running Tests & Benchmarks

```bash
# Run complete test suite (39 tests: unit, analytical simulator, bidirectional uploads/downloads, API)
pytest -v

# Run reproducible 9-scenario + scalability simulation benchmark and generate PNG charts
python -m flowweave benchmark --out results --seed 42

# Run live HTTP Range 4-policy benchmark against local server
python -m flowweave live-benchmark --rate-mbit 24 --out results/live_comparison.json

# Generate Linux tc (tbf+netem) + iperf3 validation script
python -m flowweave netem-script --out scripts/run_tc_experiment.sh
```

---

## REST API Summary

| Method & Endpoint | Description |
|---|---|
| `GET /api/health` | Service status, active policy, demo server port, sample files |
| `GET /api/transfers` | List all active & completed uploads and downloads |
| `POST /api/transfers/download` | Start an HTTP download (Range chunking + single-stream fallback) |
| `POST /api/transfers/upload` | Start a chunked parallel upload of a local/sample file |
| `POST /api/transfers/upload-browser` | Upload a browser-selected file through the chunked FlowWeave engine |
| `GET /api/transfers/{id}` | Inspect one transfer's measured & estimated telemetry |
| `POST /api/transfers/{id}/pause` | Pause an active upload or download |
| `POST /api/transfers/{id}/resume` | Resume a paused upload or download |
| `POST /api/transfers/{id}/cancel` | Cancel an active/paused transfer |
| `GET /api/monitor` | Live bidirectional throughput, links, congestion, stragglers, decision log |
| `GET /api/scheduler` & `POST /api/scheduler` | View or hot-switch scheduling policy and link capacities |
| `GET /api/demo/files` & `POST /api/demo/start` | List sample files and launch `downloads`, `uploads`, or `mixed` demo batches |
| `GET /api/experiments/scenarios` | List all 9 built-in simulation scenarios |
| `POST /api/experiments/run` & `GET /api/experiments/results` | Execute 4-policy simulation comparison and retrieve SQLite history |

---

## Project Structure

- `flowweave/models.py`, `config.py`, `metrics.py`, `storage.py`, `demo.py` — Unified bidirectional domain models, metrics, SQLite persistence, and sample file provisioning.
- `flowweave/simulator/` — SimPy directed-graph fluid-flow simulator enforcing $\sum_{i: j \in P_i} b_i(t) \le C_j$ across single-hop, multi-hop, full-duplex, and shared links, plus 9 reproducible scenarios and Matplotlib plotting.
- `flowweave/scheduler/` — Common `Scheduler` interface with 4 policies (`independent`, `max_min`, `srpt`, `flowweave`) + multi-hop capacity validator.
- `flowweave/monitor/` — Rolling-window + EWMA network monitor separating measured statistics from derived estimates.
- `flowweave/straggler/` — Cost-benefit straggler detector and replication planner.
- `flowweave/transfer/` — Real `aiohttp` Range download engine, chunked resumable upload client & server (`ChunkedUploadStore` with path-traversal protection and atomic finalization), live `AsyncRateLimiter`, unified `TransferManager`, and local demo server.
- `flowweave/coordinator.py` — Periodic `asyncio` control loop with allocation validation and rationale logging.
- `flowweave/api/app.py` — FastAPI REST backend serving both `/api/*` and the built React dashboard.
- `frontend/` — 7-section React + TypeScript + Vite + Chart.js dashboard.
- `docs/` — [architecture.md](docs/architecture.md), [algorithm.md](docs/algorithm.md).
- `results/` — [experiment_report.md](results/experiment_report.md), CSV/JSON outputs, and Matplotlib comparison & per-scenario telemetry charts.

---

## Completed vs Known Limitations

### Completed & Verified
- Bidirectional real HTTP transfers (parallel Range downloads with single-stream fallback; parallel chunked resumable uploads with atomic commit, ownership/boundary/path-traversal validation, retries, SHA-256 verification, and Pause/Resume/Cancel).
- Directed-graph multi-hop SimPy simulator with strict per-link capacity enforcement, analytical tests, full-duplex uplink/downlink modeling, dynamic capacity/loss events, and JSON/CSV/Matplotlib export.
- All 4 scheduling policies (`independent`, `max_min`, `srpt`, `flowweave`) with live policy switching and coordinator rationale logs.
- Cost-benefit straggler detection and simulation-level speculative replication.
- Full 7-section React + Chart.js dashboard backed exclusively by real FastAPI responses.
- 39 automated pytest tests passing + 9-scenario reproducible benchmark suite.

### Known Limitations
- **Fluid-flow simulator (not packet-level TCP)**: The SimPy simulator models bandwidth sharing, RTT startup, packet loss goodput tax, and oversubscription penalties at fluid-flow tick granularity (`0.1s`); it does not emulate TCP CUBIC/BBR congestion windows, packet reordering, or router AQM queues.
- **Real-world bottleneck enforcement requires OS shaping**: On local loopback without Linux `tc` (`scripts/run_tc_experiment.sh`), the OS kernel imposes no physical bottleneck on `independent` flows, whereas coordinated policies (`max_min`, `srpt`, `flowweave`) obey their configured token-bucket limits.
- **Speculative replication in live HTTP engine**: Enabled and evaluated in simulation, but disabled by default (`allow_real_replication = False`) in the live HTTP engine to prevent unintended duplicate WAN traffic when single-origin URLs are used.
- **Upload server persistence**: Upload session metadata is held in memory within `ChunkedUploadStore` (staging files are on disk); restarting the server mid-upload requires starting a fresh upload session.
- **Prior-art honesty**: FlowWeave combines established techniques (SRPT, max-min water-filling, marginal batch-delay routing, starvation aging, and cost-benefit hedged replication) into a unified prototype and does not claim algorithmic novelty.
