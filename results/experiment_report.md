# FlowWeave Experiment Report

Reproducible simulation benchmark (`seed=42`). All 4 scheduling policies were evaluated under identical topologies, file sizes, arrival schedules, and deterministic link impairment streams.

## Scenario: `mixed_shared_bottleneck`

10 mixed-size files (0.5–32 MB) contending on a single 10 MB/s shared bottleneck.

| Policy | Sum FCT (s) | Avg FCT (s) | P95 FCT (s) | Makespan (s) | Goodput (MB/s) | Jain Fairness | Link Util | Redundant KB | Sched Avg (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `independent` | 31.60 | 3.16 | 7.97 | 10.20 | 8.80 | 0.862 | 97.1% | 9498.4 | 0.0081 |
| `max_min` | 26.80 | 2.68 | 6.97 | 9.20 | 9.76 | 0.870 | 98.0% | 461.8 | 0.0399 |
| `srpt` | 20.70 | 2.07 | 6.58 | 9.30 | 9.65 | 0.906 | 97.0% | 461.8 | 0.0323 |
| `flowweave` | 37.50 | 3.75 | 7.45 | 9.20 | 9.76 | 0.665 | 98.0% | 461.8 | 0.0889 |

## Scenario: `independent_paths`

3 transfers on completely independent links verifying zero cross-flow interference.

| Policy | Sum FCT (s) | Avg FCT (s) | P95 FCT (s) | Makespan (s) | Goodput (MB/s) | Jain Fairness | Link Util | Redundant KB | Sched Avg (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `independent` | 6.30 | 2.10 | 2.10 | 2.10 | 19.05 | 0.877 | 95.2% | 0.0 | 0.0079 |
| `max_min` | 6.30 | 2.10 | 2.10 | 2.10 | 19.05 | 0.877 | 95.2% | 0.0 | 0.0202 |
| `srpt` | 6.30 | 2.10 | 2.10 | 2.10 | 19.05 | 0.877 | 95.2% | 0.0 | 0.0299 |
| `flowweave` | 6.30 | 2.10 | 2.10 | 2.10 | 19.05 | 0.877 | 95.2% | 0.0 | 0.0649 |

## Scenario: `partially_shared_paths`

Multi-hop routes sharing only `core-bottleneck` (8 MB/s) alongside an isolated 6 MB/s path.

| Policy | Sum FCT (s) | Avg FCT (s) | P95 FCT (s) | Makespan (s) | Goodput (MB/s) | Jain Fairness | Link Util | Redundant KB | Sched Avg (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `independent` | 8.60 | 2.15 | 3.38 | 3.60 | 10.56 | 0.887 | 42.7% | 1312.9 | 0.0083 |
| `max_min` | 8.10 | 2.03 | 3.21 | 3.40 | 11.18 | 0.909 | 43.8% | 133.8 | 0.0187 |
| `srpt` | 7.00 | 1.75 | 3.21 | 3.40 | 11.18 | 0.998 | 43.8% | 133.8 | 0.0355 |
| `flowweave` | 10.50 | 2.63 | 3.33 | 3.40 | 11.18 | 0.747 | 43.8% | 133.8 | 250.4265 |

## Scenario: `bidirectional_mixed`

3 simultaneous downloads + 3 uploads over separate full-duplex downlink (10 MB/s), uplink (6 MB/s), and shared ISP core (12 MB/s).

| Policy | Sum FCT (s) | Avg FCT (s) | P95 FCT (s) | Makespan (s) | Goodput (MB/s) | Jain Fairness | Link Util | Redundant KB | Sched Avg (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `independent` | 10.10 | 1.68 | 3.08 | 3.40 | 10.15 | 0.808 | 77.3% | 3261.1 | 0.0125 |
| `max_min` | 8.40 | 1.40 | 2.83 | 3.20 | 10.78 | 0.907 | 75.6% | 177.5 | 0.0206 |
| `srpt` | 7.60 | 1.27 | 2.78 | 3.10 | 11.13 | 0.909 | 78.1% | 177.5 | 0.0392 |
| `flowweave` | 11.90 | 1.98 | 2.75 | 3.00 | 11.50 | 0.621 | 80.7% | 177.5 | 13.6544 |

## Scenario: `multi_link_offload`

10 transfers across a shared primary bottleneck and two secondary links.

| Policy | Sum FCT (s) | Avg FCT (s) | P95 FCT (s) | Makespan (s) | Goodput (MB/s) | Jain Fairness | Link Util | Redundant KB | Sched Avg (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `independent` | 75.30 | 7.53 | 13.78 | 14.10 | 6.77 | 0.880 | 32.8% | 16029.2 | 0.0133 |
| `max_min` | 33.50 | 3.35 | 6.70 | 7.20 | 13.26 | 0.910 | 69.5% | 952.3 | 0.0294 |
| `srpt` | 32.00 | 3.20 | 7.68 | 8.00 | 11.94 | 0.948 | 58.5% | 1230.1 | 0.0422 |
| `flowweave` | 45.30 | 4.53 | 6.16 | 6.30 | 15.16 | 0.668 | 82.1% | 873.9 | 0.1264 |

## Scenario: `dynamic_bandwidth_drop`

Primary link suffers a severe bandwidth drop and 6% loss spike at t=2s before recovering at t=7s.

| Policy | Sum FCT (s) | Avg FCT (s) | P95 FCT (s) | Makespan (s) | Goodput (MB/s) | Jain Fairness | Link Util | Redundant KB | Sched Avg (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `independent` | 33.40 | 4.17 | 8.69 | 10.60 | 6.23 | 0.746 | 48.0% | 7525.9 | 0.0095 |
| `max_min` | 18.80 | 2.35 | 5.31 | 7.50 | 8.80 | 0.889 | 72.8% | 1340.3 | 0.0187 |
| `srpt` | 16.70 | 2.09 | 5.66 | 8.20 | 8.05 | 0.913 | 57.9% | 900.3 | 0.0277 |
| `flowweave` | 26.90 | 3.36 | 5.90 | 7.70 | 8.57 | 0.767 | 68.3% | 1314.0 | 0.0731 |

## Scenario: `congested_lossy_links`

8 transfers over a 12%-loss primary link (10 MB/s) and cleaner 6 MB/s secondary link.

| Policy | Sum FCT (s) | Avg FCT (s) | P95 FCT (s) | Makespan (s) | Goodput (MB/s) | Jain Fairness | Link Util | Redundant KB | Sched Avg (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `independent` | 32.70 | 4.09 | 7.13 | 8.80 | 7.61 | 0.943 | 48.3% | 18489.3 | 0.0093 |
| `max_min` | 15.40 | 1.92 | 3.62 | 5.40 | 12.41 | 0.931 | 81.9% | 6347.6 | 0.0188 |
| `srpt` | 15.00 | 1.87 | 4.07 | 6.20 | 10.81 | 0.983 | 68.5% | 7208.1 | 0.0275 |
| `flowweave` | 23.00 | 2.87 | 4.76 | 6.10 | 10.98 | 0.818 | 73.5% | 6029.2 | 0.0825 |

## Scenario: `straggler_recovery`

6 dual-mirror transfers with 25% chunk straggler slowdown testing cost-benefit replication.

| Policy | Sum FCT (s) | Avg FCT (s) | P95 FCT (s) | Makespan (s) | Goodput (MB/s) | Jain Fairness | Link Util | Redundant KB | Sched Avg (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `independent` | 25.10 | 4.18 | 5.15 | 5.20 | 6.35 | 0.895 | 42.3% | 2283.7 | 0.0127 |
| `max_min` | 34.60 | 5.77 | 7.17 | 7.30 | 4.52 | 0.904 | 28.5% | 263.4 | 0.0281 |
| `srpt` | 42.10 | 7.02 | 7.57 | 7.60 | 4.34 | 0.802 | 27.3% | 250.4 | 0.0587 |
| `flowweave` | 24.30 | 4.05 | 4.87 | 5.00 | 6.60 | 0.859 | 45.0% | 3058.3 | 0.1332 |

## Scenario: `uniform_sizes_fairness`

8 identical 8 MB files starting simultaneously on a 10 MB/s shared bottleneck.

| Policy | Sum FCT (s) | Avg FCT (s) | P95 FCT (s) | Makespan (s) | Goodput (MB/s) | Jain Fairness | Link Util | Redundant KB | Sched Avg (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `independent` | 62.40 | 7.80 | 7.80 | 7.80 | 8.21 | 1.000 | 99.7% | 14070.7 | 0.0139 |
| `max_min` | 52.00 | 6.50 | 6.50 | 6.50 | 9.85 | 1.000 | 98.5% | 0.0 | 0.0279 |
| `srpt` | 32.10 | 4.01 | 6.32 | 6.60 | 9.70 | 0.599 | 97.0% | 0.0 | 0.0359 |
| `flowweave` | 52.00 | 6.50 | 6.50 | 6.50 | 9.85 | 1.000 | 98.5% | 0.0 | 0.0814 |

## Scenario: `high_concurrency_25`

25 concurrent log-normal transfers with staggered arrivals on shared + edge links.

| Policy | Sum FCT (s) | Avg FCT (s) | P95 FCT (s) | Makespan (s) | Goodput (MB/s) | Jain Fairness | Link Util | Redundant KB | Sched Avg (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `independent` | 133.30 | 5.33 | 13.88 | 16.47 | 12.54 | 0.432 | 48.7% | 43620.7 | 0.0169 |
| `max_min` | 81.70 | 3.27 | 8.89 | 10.97 | 18.83 | 0.635 | 87.1% | 1964.3 | 0.0329 |
| `srpt` | 62.10 | 2.48 | 10.07 | 12.77 | 16.18 | 0.838 | 63.2% | 2062.7 | 0.0495 |
| `flowweave` | 136.60 | 5.46 | 11.12 | 13.07 | 15.81 | 0.450 | 57.2% | 2067.9 | 0.1224 |

## Scalability Sweep (Concurrent Transfers)

| Transfers | Independent | Max-Min | SRPT | FlowWeave | FlowWeave Sched Avg (ms) |
|---:|---:|---:|---:|---:|---:|
| 5 | 2.33 | 2.33 | 2.13 | 2.53 | 0.0577 |
| 10 | 9.24 | 8.34 | 7.44 | 9.34 | 0.0721 |
| 20 | 91.75 | 58.15 | 45.15 | 98.65 | 0.0897 |
| 40 | 319.00 | 175.00 | 121.30 | 297.70 | 0.1572 |
