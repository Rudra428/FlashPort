# FlashPort viva questions and answers

## 1. What problem does FlashPort address?

Several downloads can compete for one server's outgoing link. FlashPort lets a server
coordinate its own application-level sending rates rather than treating every range
connection as an unrelated transfer.

## 2. What is a flow?

A flow is one logical file download, identified by a transfer ID. It may use several
concurrent HTTP Range requests, but all those requests share the same scheduled rate.

## 3. What is flow completion time?

The time from a transfer's registration/arrival until the server confirms full byte
coverage and the client's SHA-256. It is measured per file, not across the whole workload.

## 4. What is makespan?

The interval from the first transfer registration to the last verified completion in a
completed workload. It is different from mean or median per-flow completion time.

## 5. What does Equal Share do?

With `N` active logical transfers and configured capacity `C`, it assigns `C/N` bytes per
second to each transfer.

## 6. What does SRPT do?

Shortest Remaining Processing Time gives the available application rate to the active
transfer with the least remaining data and reallocates at subsequent scheduler updates.
Knowing exact remaining file sizes is an idealized assumption.

## 7. How does FlashPort form its priority?

It computes a bounded size weight from the square root of the median remaining size divided
by the transfer's remaining bytes (with a chunk-size floor). It multiplies this by a bounded
service-deficit factor derived from recent service versus an expected fair-share service
level, then allocates weighted rates with configured minimums where capacity permits.

## 8. Is FlashPort mathematically optimal?

No. It is a bounded heuristic. The benchmark report demonstrates workload-specific outcomes,
not universal superiority or optimality.

## 9. Why does SRPT often reduce mean FCT in simulation?

The simulator is a single work-conserving server with known job sizes and preemption. Under
the classical assumptions, SRPT minimizes the sum of completion times, which is mean FCT
times a fixed flow count. The objective does not penalize unfairness.

## 10. What is Jain's Fairness Index?

For nonnegative rates `x_i`, it is
`J = (sum(x_i)^2) / (n * sum(x_i^2))`. It equals 1 for equal positive values and approaches
`1/n` when one of `n` flows gets all the rate.

## 11. What values are passed to Jain's formula in the report?

Each included value is a completed non-empty transfer's lifetime-average goodput:
`x_i = file_size_i / flow_completion_time_i`. The benchmark excludes incomplete and empty
transfers. It is not instantaneous rate fairness.

## 12. Why do Independent and Equal Share match in the simulator?

The simulator does not model individual TCP congestion control. Independent is represented
as equal share under a similar-RTT approximation. Real TCP flows can differ in RTT, startup,
congestion window, loss, and other factors.

## 13. How does the server handle a large file?

It reads and yields small blocks from a file stream. It does not read the whole file into
memory. The client stores each requested range at its offset in a pre-sized `.part` file.

## 14. How do parallel chunks avoid multiplying a transfer's bandwidth?

The server stores one token bucket for the logical transfer ID. Every range handler uses the
same bucket. The bucket rate comes from the scheduler, not from the number of HTTP requests.

## 15. What is a token bucket?

It accumulates tokens at configured rate `r`, up to a burst limit `B`. Sending `k` bytes
consumes `k` tokens. If tokens are insufficient, the stream waits. A small bounded burst
means a short sample may differ from the long-term rate.

## 16. How does resume work?

The client saves completed chunk indexes and their SHA-256 hashes beside its partial file.
On resume it checks each saved local chunk; the server either reuses the same in-memory
transfer session or validates the saved hashes against its unchanged source before accepting
the ranges as already served.

## 17. How is full-file integrity checked?

The client hashes the reconstructed file with SHA-256 and compares it with the hash returned
by the server at registration. It renames the partial file only after validation succeeds.

## 18. Does FlashPort control TCP or the Internet router?

No. It shapes bytes yielded by the server application. TCP, the host, and the path can change
the actual goodput. The configured rate is not a guarantee of physical-link throughput.

## 19. What does the actual network monitor measure?

It records client-reported progress/goodput, server stream bytes, completion timing, and
HTTP response-header latency. This latency includes server delay and is not pure RTT. Packet
loss and TCP retransmission counters are marked unavailable.

## 20. Can `tc/netem` be used?

The project can generate Linux command lines for a reviewed test interface. It never runs
them. Applying or removing a qdisc requires explicit manual approval and suitable
privileges; the current Windows environment cannot validate this experiment.

## 21. What were the benchmark findings?

On the current 14-scenario simulation matrix, SRPT beat FlashPort on mean FCT in all 14
scenarios. FlashPort beat Equal Share in 7 and tied it in 7. Its fairness results varied by
workload. See `results/benchmark_report.md`; do not turn these simulation results into a
claim about real-network speed.

## 22. What is not production-ready?

The service lacks user accounts, TLS setup, persistent server state, public-network
authorization/quotas, OS transport counters, and multi-worker coordination. It is intended
for a loopback/local academic demonstration.
