# FlashPort

**FlashPort — Intelligent, Cooperative Scheduling for Concurrent File Transfers** is a
Computer Networks demonstration project. It combines an existing reproducible scheduler
simulator with a real, local HTTP file server and resumable asynchronous client.

## Problem and motivation

When several files share a limited outgoing link, equal sharing can delay short files,
while aggressively prioritizing short files can make large files wait. FlashPort exposes
four policies and measures their trade-offs instead of assuming one policy is universally
best. Its application scheduler shapes the server's own outgoing byte stream; it does not
control TCP internals, routers, or Internet congestion.

## Features

- Independent, Equal Share, SRPT, and FlashPort scheduling policies.
- An asyncio/FastAPI server for safe file listing, metadata, single HTTP byte ranges,
  transfer status, cancellation, and per-transfer scheduling.
- Browser drag-and-drop/upload, live receiver registration, multi-recipient delivery, and
  per-receiver progress with SHA-256 verification.
- A token bucket per logical transfer, shared across every concurrent range request for
  that transfer; allocations across active transfers are bounded by configured capacity.
- An aiohttp client supporting concurrent files/chunks, resume manifests, retries, safe
  partial-file writes, and final SHA-256 verification.
- A focused file-sharing dashboard; simulation and benchmark APIs remain available separately.
- Client-reported goodput/progress, server stream-byte counters, HTTP request-latency
  samples, verified completion-time metrics, and explicit unavailable transport metrics.
- Reproducible simulation scenarios, benchmark CSV/JSON, report, and charts.
- Optional Linux `tc/netem` command generation. FlashPort does not execute privileged
  commands or alter host network settings.

There are no machine-learning components in the inspected repository. No existing ML
policy was removed.

## Architecture

```text
Client (aiohttp; concurrent byte ranges; resume + SHA-256)
                         |
                  HTTP / Range
                         v
FastAPI API -> safe shared-file resolver -> transfer manager
                                              |
                                  selected scheduler (250 ms)
                                              |
                             one token bucket per transfer ID
                                              |
                                  async bounded file streaming

Dashboard -> actual transfer/status/network endpoints
          -> simulation endpoint + stored benchmark CSV/JSON/graphs
```

The simulation engine and live transfer manager are separate. See
[docs/architecture.md](docs/architecture.md), [docs/network_model.md](docs/network_model.md),
and [docs/algorithm.md](docs/algorithm.md) for diagrams and details.

## Technology

- Python 3.11+
- FastAPI and Uvicorn
- aiohttp and Python asyncio
- pytest / pytest-asyncio
- matplotlib, NumPy, and pandas for experiment/report tooling

## Installation

From the project directory, on Windows PowerShell:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

On Linux/macOS:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Generate deterministic demonstration files (1 MB, 8 MB, and empty):

```bash
python scripts/create_demo_files.py
```

The server serves direct files from `shared/` by default. Change the directory with
`--shared-dir` or the `FLASHPORT_SHARED_DIR` environment variable.

## Start the server and dashboard

```bash
python -m flashport.cli serve --host 127.0.0.1 --port 8000 `
  --shared-dir shared --capacity-bps 20000000 --scheduler flashport
```

The backtick above is PowerShell line continuation. Run it on one line if preferred.
Open [http://127.0.0.1:8000](http://127.0.0.1:8000); interactive API documentation is
at [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs).

For a direct Uvicorn launch:

```bash
python -m uvicorn flashport.api.main:app --host 127.0.0.1 --port 8000
```

Use one server worker: the transfer registry and shared-capacity token buckets are
in-memory and process-local. The default binds only to localhost. Do not expose this
unauthenticated academic demo to an untrusted network; file listing and scheduler selection
are intentionally local-demo controls.

## Share files from the browser

Open the dashboard on each receiver device, enter a device name under **Receive on this
device**, and choose **Register device**. Keep that page open while receiving. On the
sender's page, drop or browse for a file, select one or more online receivers, and choose
**Send file**. The browser uploads the file to the server, which queues an actual
scheduler-limited transfer for each selected receiver. Receivers download and verify the
file's SHA-256; the sender's Transfers panel shows individual progress and completion.
Browser uploads are limited to 1 GB and are stored under `shared/.flashport-uploads/`.

The default server address is reachable only from the same computer. For testing on a
trusted local network, start the server with `--host 0.0.0.0`, then open
`http://<server-private-IP>:8000` on each device (allow the port through the host firewall
if needed). This demo has no user authentication or TLS, so do not expose it to untrusted
networks or the public Internet.

## Start clients and demonstrate concurrent transfers

List the server's files:

```bash
python -m flashport.cli list --server http://127.0.0.1:8000
```

Download two different files concurrently from one client:

```bash
python -m flashport.cli download --server http://127.0.0.1:8000 `
  --destination downloads\client-a --chunk-size 262144 --connections 4 `
  demo-1mb.bin demo-8mb.bin
```

In another terminal, run a second client at the same time:

```bash
python -m flashport.cli download --server http://127.0.0.1:8000 `
  --destination downloads\client-b --connections 3 demo-8mb.bin
```

Each client reports progress; completed downloads are renamed from their `.part` file only
after the whole-file SHA-256 matches. To change the client behavior, try `--chunk-size`,
`--connections`, `--retries`, `--overwrite`, or `--no-resume`. The client concurrency
setting limits range requests for that client instance. The server applies the policy
allocation to each logical transfer, not each range connection.

If a client is interrupted, rerun the same download with the same destination and chunk
size. The client resumes its verified chunks and reuses the logical server transfer while
the server process is alive. After a server restart, the client validates local chunk hashes
against the shared source before resuming under a new server-side transfer ID.

## Scheduler selection and monitoring

Choose a default policy on server startup with `--scheduler` (`independent`, `equal_share`,
`srpt`, or `flashport`). While running, change the policy and configured application capacity
with `POST /api/scheduler`. The API also supports:

```text
GET  /api/scheduler
POST /api/scheduler
GET  /api/transfers
GET  /api/transfers/{transfer_id}/status   (requires X-FlashPort-Token)
GET  /api/network/metrics
```

Allocation values are configured sending rates. Actual goodput is measured by client
progress reports and may differ because of TCP, host scheduling, disk I/O, and network
conditions. HTTP request latency includes application/server delay and is not a pure RTT.
Packet loss and TCP retransmission counters are not available in this app-only monitor.

## Simulation and reproducible benchmarks

Run the interactive fixed workload comparison:

```bash
python -m flashport.simulation.experiments
```

This writes scenario/policy CSV, per-transfer CSV, JSON, a Markdown report, and five charts
under `results/`. It does not transfer files. The `/api/simulation/comparison` endpoint and
benchmark artifact routes remain available; the main dashboard is focused on actual file
sharing rather than charts or analytics.

The latest stored benchmark report is [results/benchmark_report.md](results/benchmark_report.md).
It records that SRPT had lower mean FCT than FlashPort in 14/14 scenarios, while FlashPort
beat or tied Equal Share in all 14 (7 wins, 7 ties). Interpret those results only under the
documented single-bottleneck simulator assumptions; actual live runs are not benchmark
claims.

## Tests

```bash
python -m pytest -q
```

Tests cover the existing simulation and policies plus receiver registration, browser upload
and multi-receiver delivery, HTTP paths/ranges, transfer tokens, complete-byte-range checks,
concurrent clients/chunks, resumable downloads, integrity, rate-budget sharing, cancellation,
empty files, and API/dashboard integration.

## HTTP API overview

- `GET /health`
- `POST /api/uploads?filename=...` (raw file body; maximum 1 GB)
- `GET|POST /api/receivers`; receiver heartbeat, unregister, and queued-delivery routes
- `POST /api/deliveries`; `GET /api/deliveries/{id}` for per-receiver progress
- `GET /api/files`
- `GET /api/files/{filename}/metadata`
- `POST /api/transfers`
- `GET /api/transfers`
- `GET /api/transfers/{id}/content` with one `Range: bytes=start-end` and transfer token
- `PUT /api/transfers/{id}/resume`, `/progress`
- `GET /api/transfers/{id}/status`, `POST /complete`, `/cancel`
- `GET|POST /api/scheduler`
- `GET /api/network/metrics`
- `GET /api/simulation/comparison`
- `GET /api/benchmarks`

Unsupported or malformed ranges return 416; missing files return 404; path traversal and
invalid inputs return explicit 4xx responses. Transfer IDs use random bearer tokens for
range/progress/status control. Resume manifests are stored beside local `.part` files and
contain that token; protect the destination directory accordingly.

## Limitations

- One Uvicorn worker/process; live state is in memory and is reset on server restart.
- The default shared directory is served only at its top level. This is a local demo, not
  a hardened public file-hosting service; it has no user-account authentication/TLS setup.
- A single byte range is supported per HTTP response. Multipart ranges are rejected.
- Application token buckets shape bytes yielded by the server; they do not guarantee
  physical-link capacity or control TCP/network behavior.
- Actual latency is HTTP request latency, not raw network RTT. Packet loss and retransmits
  need OS-level tools and are not currently collected.
- `tc/netem` support only generates reviewed Linux commands. Applying or removing them
  requires the user's explicit manual approval and appropriate privileges.
- The Independent policy is an Equal Share approximation in the simulator, not a TCP
  implementation. FlashPort is a heuristic, not proven optimal or novel.

See [docs/limitations.md](docs/limitations.md), [docs/experiments.md](docs/experiments.md),
[docs/viva_questions.md](docs/viva_questions.md), and
[docs/project_presentation.md](docs/project_presentation.md) for viva/presentation material.
