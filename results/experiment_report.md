# FlowWeave Experiment Report

Reproducible simulation benchmark (`seed=42`). All four scheduling policies were evaluated under identical topologies, file sizes, arrival schedules, and deterministic link impairment streams.

## Scenario: `mixed_shared_bottleneck`

10 mixed-size files (0.5–32 MB) contending on a single 10 MB/s shared bottleneck.

| Policy | Sum FCT (s) | Avg FCT (s) | P95 FCT (s) | Makespan (s) | Goodput (MB/s) | Jain Fairness | Redundant KB | Sched Avg (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `independent` | 31.60 | 3.16 | 7.97 | 10.20 | 8.80 | 0.862 | 9498.4 | 0.0119 |
| `max_min` | 26.80 | 2.68 | 6.97 | 9.20 | 9.76 | 0.870 | 461.8 | 0.0394 |
| `srpt` | 20.70 | 2.07 | 6.58 | 9.30 | 9.65 | 0.906 | 461.8 | 0.0488 |
| `flowweave` | 21.00 | 2.10 | 6.46 | 9.50 | 9.45 | 0.918 | 461.8 | 0.0503 |

## Scenario: `independent_paths`

3 transfers on completely independent links verifying zero cross-flow interference.

| Policy | Sum FCT (s) | Avg FCT (s) | P95 FCT (s) | Makespan (s) | Goodput (MB/s) | Jain Fairness | Redundant KB | Sched Avg (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `independent` | 6.30 | 2.10 | 2.10 | 2.10 | 19.05 | 0.877 | 0.0 | 0.0146 |
| `max_min` | 6.30 | 2.10 | 2.10 | 2.10 | 19.05 | 0.877 | 0.0 | 0.0315 |
| `srpt` | 6.30 | 2.10 | 2.10 | 2.10 | 19.05 | 0.877 | 0.0 | 0.0609 |
| `flowweave` | 6.30 | 2.10 | 2.10 | 2.10 | 19.05 | 0.877 | 0.0 | 0.0490 |

## Scenario: `partially_shared_paths`

Multi-hop routes sharing only `core-bottleneck` (8 MB/s) alongside an isolated 6 MB/s path.

| Policy | Sum FCT (s) | Avg FCT (s) | P95 FCT (s) | Makespan (s) | Goodput (MB/s) | Jain Fairness | Redundant KB | Sched Avg (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `independent` | 8.60 | 2.15 | 3.38 | 3.60 | 10.56 | 0.887 | 1312.9 | 0.0163 |
| `max_min` | 8.10 | 2.03 | 3.21 | 3.40 | 11.18 | 0.909 | 133.8 | 0.0394 |
| `srpt` | 7.00 | 1.75 | 3.21 | 3.40 | 11.18 | 0.998 | 133.8 | 0.0598 |
| `flowweave` | 7.10 | 1.78 | 3.21 | 3.40 | 11.18 | 0.997 | 133.8 | 0.0713 |

## Scenario: `bidirectional_mixed`

3 simultaneous downloads + 3 uploads over separate full-duplex downlink (10 MB/s), uplink (6 MB/s), and shared ISP core (12 MB/s).

| Policy | Sum FCT (s) | Avg FCT (s) | P95 FCT (s) | Makespan (s) | Goodput (MB/s) | Jain Fairness | Redundant KB | Sched Avg (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `independent` | 10.10 | 1.68 | 3.08 | 3.40 | 10.15 | 0.808 | 3261.1 | 0.0160 |
| `max_min` | 8.40 | 1.40 | 2.83 | 3.20 | 10.78 | 0.907 | 177.5 | 0.0359 |
| `srpt` | 7.60 | 1.27 | 2.78 | 3.10 | 11.13 | 0.909 | 177.5 | 0.0375 |
| `flowweave` | 7.20 | 1.20 | 2.73 | 3.10 | 11.13 | 0.948 | 177.5 | 0.0534 |

## Scenario: `multi_link_offload`

10 transfers across a shared primary bottleneck and two secondary links.

| Policy | Sum FCT (s) | Avg FCT (s) | P95 FCT (s) | Makespan (s) | Goodput (MB/s) | Jain Fairness | Redundant KB | Sched Avg (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `independent` | 75.30 | 7.53 | 13.78 | 14.10 | 6.77 | 0.880 | 16029.2 | 0.0234 |
| `max_min` | 33.50 | 3.35 | 6.70 | 7.20 | 13.26 | 0.910 | 952.3 | 0.0533 |
| `srpt` | 32.00 | 3.20 | 7.68 | 8.00 | 11.94 | 0.948 | 1230.1 | 0.0506 |
| `flowweave` | 26.60 | 2.66 | 6.16 | 6.70 | 14.25 | 0.992 | 1444.0 | 0.0659 |

## Scenario: `dynamic_bandwidth_drop`

Primary link suffers a severe bandwidth drop and 6% loss spike at t=2s before recovering at t=7s.

| Policy | Sum FCT (s) | Avg FCT (s) | P95 FCT (s) | Makespan (s) | Goodput (MB/s) | Jain Fairness | Redundant KB | Sched Avg (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `independent` | 33.40 | 4.17 | 8.69 | 10.60 | 6.23 | 0.746 | 7525.9 | 0.0109 |
| `max_min` | 18.80 | 2.35 | 5.31 | 7.50 | 8.80 | 0.889 | 1340.3 | 0.0225 |
| `srpt` | 16.70 | 2.09 | 5.66 | 8.20 | 8.05 | 0.913 | 900.3 | 0.0315 |
| `flowweave` | 18.50 | 2.31 | 5.70 | 8.20 | 8.05 | 0.884 | 1213.4 | 0.0485 |

## Scenario: `straggler_recovery`

6 dual-mirror transfers with 25% chunk straggler slowdown testing cost-benefit replication.

| Policy | Sum FCT (s) | Avg FCT (s) | P95 FCT (s) | Makespan (s) | Goodput (MB/s) | Jain Fairness | Redundant KB | Sched Avg (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `independent` | 25.10 | 4.18 | 5.15 | 5.20 | 6.35 | 0.895 | 2283.7 | 0.0156 |
| `max_min` | 34.60 | 5.77 | 7.17 | 7.30 | 4.52 | 0.904 | 263.4 | 0.0341 |
| `srpt` | 42.10 | 7.02 | 7.57 | 7.60 | 4.34 | 0.802 | 250.4 | 0.0919 |
| `flowweave` | 23.20 | 3.87 | 6.50 | 6.70 | 4.93 | 0.951 | 3958.8 | 0.0630 |

## Scenario: `uniform_sizes_fairness`

8 identical 8 MB files starting simultaneously on a 10 MB/s shared bottleneck.

| Policy | Sum FCT (s) | Avg FCT (s) | P95 FCT (s) | Makespan (s) | Goodput (MB/s) | Jain Fairness | Redundant KB | Sched Avg (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `independent` | 62.40 | 7.80 | 7.80 | 7.80 | 8.21 | 1.000 | 14070.7 | 0.0212 |
| `max_min` | 52.00 | 6.50 | 6.50 | 6.50 | 9.85 | 1.000 | 0.0 | 0.0465 |
| `srpt` | 32.10 | 4.01 | 6.32 | 6.60 | 9.70 | 0.599 | 0.0 | 0.0430 |
| `flowweave` | 35.50 | 4.44 | 6.66 | 6.80 | 9.41 | 0.697 | 0.0 | 0.0559 |

## Scenario: `high_concurrency_25`

25 concurrent log-normal transfers with staggered arrivals on shared + edge links.

| Policy | Sum FCT (s) | Avg FCT (s) | P95 FCT (s) | Makespan (s) | Goodput (MB/s) | Jain Fairness | Redundant KB | Sched Avg (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `independent` | 133.30 | 5.33 | 13.88 | 16.47 | 12.54 | 0.432 | 43620.7 | 0.0292 |
| `max_min` | 81.70 | 3.27 | 8.89 | 10.97 | 18.83 | 0.635 | 1964.3 | 0.0385 |
| `srpt` | 62.10 | 2.48 | 10.07 | 12.77 | 16.18 | 0.838 | 2062.7 | 0.0480 |
| `flowweave` | 60.20 | 2.41 | 9.56 | 13.57 | 15.22 | 0.838 | 3161.3 | 0.0753 |

## Scalability Sweep (Concurrent Transfers)

| Transfers | Independent Sum FCT (s) | Max-Min Sum FCT (s) | SRPT Sum FCT (s) | FlowWeave Sum FCT (s) | FlowWeave Sched Avg (ms) |
|---:|---:|---:|---:|---:|---:|
| 5 | 2.33 | 2.33 | 2.13 | 2.13 | 0.0460 |
| 10 | 9.24 | 8.34 | 7.44 | 7.84 | 0.0559 |
| 20 | 91.75 | 58.15 | 45.15 | 46.05 | 0.0638 |
| 40 | 319.00 | 175.00 | 121.30 | 118.30 | 0.0751 |
