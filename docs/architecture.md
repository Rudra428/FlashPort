# FlashPort architecture

FlashPort keeps its deterministic simulator independent from its real HTTP transfer path.
They share the same scheduling policy interface but do not share measurements or state.

```text
 Sender browser -> raw upload -> private upload store -> delivery queue
      |                                                |
      |                                      receiver registration/jobs
      |                                                |
      +-------------------- FastAPI -------------------+
                               |
                          TransferServer <--- CLI HTTP Range client
                               |
                   TransferManager (in memory)
                               |
               Scheduler (Independent / Equal / SRPT / FlashPort)
                               |
               per-transfer shared token bucket
                               |
                    bounded async file stream
                               |
                      receiver browser
                 progress + SHA-256 verification

 FastAPI also keeps simulation/comparison and benchmark APIs independent
 from live file delivery; saved experiment artifacts remain under results/.
```

## Components and boundaries

### FastAPI application

`flashport.api.main.create_app` builds the service and its lifespan. The transfer manager,
scheduler loop, monitor, receiver registry, upload registry, delivery status, and rate buckets
are process-local. Run one Uvicorn worker to keep a single shared capacity budget. The main
browser page focuses on sending and receiving files; simulation and benchmark APIs remain
separate and do not supply simulated data to the sharing UI.

### Safe file service

`TransferServer` serves direct files under the configured shared directory and accepts raw
browser uploads of up to 1 GB into a private `.flashport-uploads/` subdirectory. Upload
filenames are validated, stored under generated names, and never appear in the ordinary file
listing. File content is read in bounded blocks; full files are never loaded into server
memory. Registration computes a streaming SHA-256 and captures a file identity so a source
modified during a transfer is rejected.

One transfer registration produces a random transfer ID and bearer token. A client requests
one byte range per response. Successful served intervals are merged, so retries/overlaps do
not inflate coverage. The server will not mark a transfer complete unless all file bytes
have been served, the client reports the full byte count, and its submitted final hash equals
the registered source hash.

The browser receiver registers a name and heartbeats while its page remains open. A sender
selects online receiver IDs; the server creates one normal scheduler-managed transfer per
receiver. Each receiver polls for jobs, downloads the actual bytes using its transfer token,
reports progress, checks SHA-256, and saves the result through a browser download. The sender
polls a delivery record that aggregates each receiver's transfer state.

### Transfer manager and scheduler

The manager holds filename, client ID, total/downloaded/remaining bytes, served bytes,
active connections, lifecycle timestamps, throughput observations, and allocation. The
scheduler re-evaluates active logical transfers periodically and on each loop tick. It
checks nonnegative finite allocations and enforces the configured capacity before updating
rate limiters. Completed, paused, and queued transfers do not receive rates.

All range connections belonging to one transfer share exactly one token bucket. A second
logical transfer gets its own bucket at the policy's allocated rate. Since the rates are
allocated as a bounded sum, one multi-range client cannot multiply its share by opening more
connections. See [network_model.md](network_model.md) for rate units and measurement caveats.

### Client

`TransferClient` uses aiohttp with a global per-client range-request semaphore. It divides a
file into configurable non-overlapping chunks, writes each response at the correct offset,
and atomically records successful chunk hashes in a local resume manifest. A restart can
reuse a live server transfer token; after a server restart, the client asks the new server
to validate saved chunk hashes against the source before treating them as served. The final
whole-file digest is checked before the `.part` file is renamed to its destination.
Browser receivers use the same token-authorized content and progress/completion endpoints,
then verify the response and initiate a local browser download.

### Monitoring

The network monitor distinguishes server stream bytes from client-reported stored progress.
Actual completed-flow completion times, makespan, aggregate goodput, and Jain fairness are
computed from verified completions. Client-reported HTTP response-header latency is
described as application request latency, not raw TCP RTT. Packet loss and retransmission
statistics are explicitly marked unavailable. These measurements remain available through
the API; the minimal file-sharing page does not display charts or analytics.

### Simulation and reports

`SimulationEngine`, scenario generation, metrics, and the 14-scenario benchmark suite remain
standalone. `/api/simulation/comparison` returns `"measurement_type": "simulation"`.
Benchmark CSV/JSON/report/graphs are stored under `results/`; actual transfer metrics are
served from `/api/network/metrics` with `"measurement_type": "actual_application"`.

No machine-learning or additional scheduler policy was found in the repository inventory.
