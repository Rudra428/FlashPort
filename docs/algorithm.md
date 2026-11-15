# Scheduling algorithm notes

## Baselines

### Independent transfers

The independent policy is approximated as equal share in this simulator under a similar-RTT
assumption. It is not a distinct TCP congestion-control model: RTT, congestion windows,
startup behavior, and loss are not simulated. Consequently, its results intentionally match
Equal share and should not be interpreted as a general model of uncoordinated downloads.

### Equal share

The equal-share policy assigns each active transfer the same throughput:

rate_i = C / N

This is simple and fair under a static bottleneck but not workload-aware.

### Shortest remaining processing time (SRPT)

The SRPT baseline gives all available capacity to the active flow with the least remaining
bytes and reallocates at flow completion or arrival events. This is strict preemptive SRPT
under the simulator assumptions. It can reduce mean completion time, but can create starvation
for large transfers when short jobs continue to arrive and can be unfair in mixed-size workloads.

## FlashPort heuristic

The FlashPort policy starts from a bounded, workload-aware heuristic rather than a mathematically proven optimal controller.

For each active transfer i:

- Let R_i be its remaining bytes.
- Let M be the median remaining size among active transfers.
- Define a size-weight as:

  size_weight_i = clamp(sqrt(M / max(R_i, chunk_size)), 0.5, 2.0)

- Compute a bounded service-deficit adjustment from measured goodput samples over a recent
  observation window, compared with expected equal-share service.
- Update the priority as:

  weight_i = clamp(size_weight_i * deficit_factor_i, 0.5, 4.0)

- Reserve the configurable minimum service floor where feasible, then allocate remaining
  capacity proportionally to normalized weights.

This is a configurable heuristic, not a mathematically proven optimal controller. Its
measured outcome depends on the workload and the selected fairness/completion objectives.

## What the benchmarks demonstrate

In the current 14-scenario benchmark matrix, SRPT has lower mean flow completion time (FCT)
than FlashPort in every scenario. FlashPort has lower mean FCT than Equal share in 7
scenarios and ties it in 7, with no scenario slower than Equal share. Its measured Jain
fairness is higher than Equal share for the unequal-size and staggered mixed workloads, but
lower for all five randomized workloads. Thus the fairness/completion-time trade-off is
workload-dependent; these results do not establish that FlashPort is uniformly fairer or
faster.

SRPT's mean-FCT result follows from the simulator's idealized model: known remaining sizes,
one work-conserving bottleneck, and preemption/reallocation without transport overhead.
The mean-FCT objective rewards minimizing the sum of completion times and includes no
fairness penalty. In the 500 equal-sized flow scenario, for example, SRPT's mean FCT is
20.04 s versus 40.00 s for Equal share, while Jain fairness is 0.056 versus 1.000 and both
makepans are 40.00 s. This illustrates how earlier finishes for some flows can coexist with
long waits for others.

## Fairness metric used in the benchmark

For each completed, non-empty transfer, the benchmark uses its lifetime-average goodput
`x_i = file_size_i / FCT_i` in bytes/second. It computes Jain's index as
`J = (sum(x_i) ** 2) / (n * sum(x_i ** 2))`, where `n` is the number of included flows.
Inputs are scaled by their maximum before evaluation for numerical stability; this does not
change the index. Empty/all-zero input yields 0. The value represents equality of
per-transfer lifetime-average goodput—not instantaneous allocated rates, equal-window
delivered bytes, or fairness including incomplete/zero-byte transfers.

## Use on the live server

The same policy interface is called for active live transfers at a configurable interval
(250 ms by default). A live `TransferState` includes file size, bytes confirmed by the client,
remaining bytes, recent progress-derived service samples, active range-connection count, and
the last allocation. A paused or completed transfer is excluded from allocation.

The policy returns configured bytes/second per transfer. The service validates finite,
nonnegative rates and checks that their sum does not exceed the configured application
capacity. It then sets one token bucket per logical transfer ID. Concurrent HTTP ranges for
that transfer share that bucket, so opening extra connections cannot multiply its scheduled
rate. The scheduler does not control TCP congestion control, a router, or physical-link
capacity; observed client goodput can differ from its allocation.

The benchmark suite and simulation engine remain separate from this live loop. Simulator
results are not relabeled as actual transfer measurements.

## Benchmark audit corrections

The previous FlashPort allocator set every minimum to equal share, which consumed the entire
capacity budget and made its weights ineffective. The floor is now the configured minimum
rate only. The previous SRPT implementation used inverse-size proportional shares; it now
implements strict preemptive SRPT. The simulator also reallocates capacity inside a time step
when a transfer completes, preventing coarse-step idle capacity from biasing SRPT results.
See the generated benchmark report for tests, assumptions, and observed outcomes.
