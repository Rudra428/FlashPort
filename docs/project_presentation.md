# FlashPort project presentation outline

Suggested 8–10 minute presentation for a Computer Networks project. Keep actual-transfer
measurements separate from simulator results and avoid claiming an unmeasured speedup.

## Slide 1 — Title and objective

**FlashPort — Intelligent, Cooperative Scheduling for Concurrent File Transfers**

- Problem: concurrent downloads share a constrained server egress link.
- Goal: expose scheduling choices and measure their trade-offs.

## Slide 2 — Motivation and network concepts

- Flow versus connection: one logical file transfer can use several HTTP range connections.
- Fairness versus completion time.
- Application-layer scheduling versus TCP congestion control.

## Slide 3 — Architecture

Show the diagram in [architecture.md](architecture.md):

```text
Sender browser -> upload / delivery API -> Transfer Manager -> Scheduler
Receiver browser -> token-authorized file stream <- shared per-transfer token bucket
CLI client ------------------------------^
Simulation Engine -> policies -> metrics -> benchmark outputs (separate API)
Dashboard -> focused file sharing and per-receiver delivery progress
```

## Slide 4 — Scheduling policies

- Independent: Equal Share approximation in the simulator.
- Equal Share: `r_i = C/N`.
- SRPT: allocate current service to the least remaining file.
- FlashPort: bounded size weight plus recent-service deficit, then weighted allocation.

Do not claim FlashPort is globally optimal. The heuristic is experimental.

## Slide 5 — Safe transfer implementation

- Server exposes only files in a configured shared directory.
- One logical transfer ID, multiple bounded HTTP byte ranges.
- Shared token bucket prevents additional chunks multiplying one transfer's allocated rate.
- Client writes ranges to a partial file, resumes using chunk hashes, then checks whole-file
  SHA-256 before finalizing.

## Slide 6 — Simulation results

Display the generated completion-time/fairness charts and the 14-scenario report.

- SRPT had lower mean FCT than FlashPort in 14/14 simulation scenarios.
- FlashPort beat Equal Share in 7/14 and tied in 7/14.
- In the 500 equal-size concurrency case, SRPT mean FCT was 20.04 s versus 40.00 s for
  Equal Share, while Jain fairness was 0.056 versus 1.000 and both makespans were 40.00 s.

Explain this as an idealized single-bottleneck result, not a real-network claim.

## Slide 7 — Live file-sharing demonstration

1. Start the server with `python -m flashport.cli serve --host 127.0.0.1 --port 8000`.
2. Open `http://127.0.0.1:8000` in the sender and receiver browsers.
3. Register a receiver device; on the sender page, drop a file, select one or more
   registered receivers, and choose **Send file**.
4. Show the real per-receiver progress and the receiver's verified downloaded file.
5. Optionally demonstrate the existing CLI client's resumable transfer:
   `python -m flashport.cli download --server http://127.0.0.1:8000 --destination downloads\client-a demo-8mb.bin`.
6. Confirm final files exist and compare SHA-256:
   - PowerShell: `Get-FileHash downloads\client-a\demo-8mb.bin -Algorithm SHA256`
   - Linux: `sha256sum downloads/client-a/demo-8mb.bin`
7. Present benchmark charts from the saved report separately; they are simulator results,
   not measurements from this browser transfer.

## Slide 8 — Measurements and limitations

- Configured application rate is not actual physical-link capacity.
- Client goodput is client-reported; server stream bytes can include retries.
- HTTP request latency is not pure RTT; packet loss/retransmission data are unavailable.
- One server process/worker; no public-network authentication/TLS setup.
- `tc/netem` commands are generated for manual review only and were not executed here.

## Slide 9 — Conclusion and future work

- Demonstrated modular simulation plus a working local client/server transfer workflow.
- Preserved honest baseline comparisons; FlashPort has workload-dependent results.
- Future: authorization/TLS, durable server state, OS-level telemetry, and approved Linux
  network experiments.
