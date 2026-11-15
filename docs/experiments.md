# Experiment notes

Run the deterministic benchmark suite from the repository root with:

```bash
python -m flashport.simulation.experiments
```

The runner compares Independent, Equal share, SRPT, and FlashPort against identical workload
and capacity inputs. It writes JSON and CSV results plus charts and a Markdown report under
`results/`.

## Scenario design

- 10 equal-size, simultaneous files.
- Simultaneous 10, 50, 100, 500 MB and 1 GB files.
- Equal-size concurrency levels of 10, 50, 100, and 500.
- Mixed file sizes with staggered arrivals.
- 100 Mbps capacity reduced to 50 Mbps during active transfers and later restored.
- 500 flows sharing a 10 Mbps bottleneck.
- 40-flow randomized workloads on seeds 7, 21, 42, 84, and 126.
- Fixed 0.5-second simulation intervals with within-interval reallocation at flow completion
  and arrival events. Dynamic link capacity is piecewise constant per interval.

## Metrics to record

- mean, median, and 95th-percentile flow completion time
- workload makespan, measured from first arrival to last completion
- aggregate goodput and network utilization
- Jain fairness over completed, non-empty flows' lifetime-average goodput
- scheduler execution time, measured around calls to the scheduling policy
- count of completed flows with FCT strictly above twice the run median
- completion coverage and exact transferred-byte accounting

## Interpretation and limitations

For each completed, non-empty transfer, the fairness input is its average goodput over its
flow lifetime: `x_i = file_size_i / (completion_time_i - arrival_time_i)`. Jain's index is
`J = (sum(x_i) ** 2) / (n * sum(x_i ** 2))`, where `n` is the number of included transfers.
The implementation rescales inputs by their maximum before applying the formula for numeric
stability; the result is mathematically unchanged. Empty/all-zero input returns 0. This is
not fairness of instantaneous allocated bandwidth or delivered bytes over a common time
window; incomplete and zero-byte transfers are excluded.

The current 14-scenario result set shows SRPT with a lower mean FCT than FlashPort in all
14 scenarios. FlashPort lowers mean FCT versus Equal share in 7 scenarios and ties in 7;
it is never slower. FlashPort's Jain score is higher than Equal share in the unequal-size
and staggered mixed scenarios, but lower in each of the five randomized scenarios. These
results show a workload-dependent fairness/completion-time trade-off, not a universal win.
SRPT's mean-FCT advantage is expected in this simulator: it is a preemptive, work-conserving
single-server model with known flow sizes, and the mean FCT objective rewards early
completion without a fairness penalty. This does not establish its advantage in a real
network or for other objectives.

Independent scheduling is represented as equal share under a stated equal-RTT approximation,
so those two policies intentionally produce identical results. This is a modeling
convenience, not a measurement of uncoordinated TCP flows. Distinguishing them requires
transport behavior such as RTT, congestion windows, startup, and loss. FlashPort remains a
configurable heuristic; the results do not establish optimality or novelty. Scheduler
runtime varies with machine load.

The simulator does not model TCP, packets, RTT variation, loss, retransmissions, or multiple
bottlenecks. See [../results/benchmark_report.md](../results/benchmark_report.md) for the
measured results from the current run. Real network tests with `tc`, `netem`, and `iperf3`
remain future work and require an explicitly approved Linux environment.

## Live transfer measurements

The real server/client and a live dashboard path are now implemented separately from the
simulator. The client uses concurrent byte ranges, resumable chunk manifests, and
whole-file SHA-256 validation; the server applies the selected policy through shared
per-logical-transfer token buckets. Live API values are explicitly labeled
`actual_application`; simulator and generated benchmark values are labeled `simulation`.

These are application-level test results, not packet-level or router measurements. HTTP
request latency is not pure RTT, and packet loss/retransmission data is not collected. The
optional `tc/netem` helper only produces commands for user review; executing controlled
Linux network tests remains future work and requires explicit approval in a suitable
environment.
