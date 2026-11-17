# Scheduling Algorithms and Prior-Art Positioning

## Objective

Given $N$ concurrent file transfers sharing a network graph of links $\mathcal{L}$ with capacities $C_\ell(t)$, latencies $d_\ell(t)$, and packet loss rates $p_\ell(t)$, FlowWeave aims to minimize total flow completion time:

$$
\min \sum_{i=1}^{N} T_i
$$

while keeping tail completion time ($P_{95}$), aggregate goodput, Jain's fairness index, and redundant bytes well-behaved across dynamic network conditions.

## Prior-Art Honesty

**FlowWeave does not claim algorithmic novelty.** Its scheduling and straggler mechanisms combine established ideas from queuing theory, networking, and distributed systems into a clean, inspectable prototype:

- **SRPT optimality for $\sum T_i$** on a single bottleneck: Schrage (1968), Bansal & Harchol-Balter (2001).
- **Max-min fair water-filling** across shared network links: Bertsekas & Gallager (*Data Networks*).
- **Batch/cooperative placement** considering delay imposed on co-located transfers: Smith's rule / weighted shortest processing time and coflow/batch-aware scheduling literature (e.g., Varys / Chowdhury et al., SIGCOMM 2014).
- **Cost-benefit speculative replication** for tail latency / stragglers: Mantri (Ananthanarayanan et al., OSDI 2010) and hedged requests (*The Tail at Scale*, Dean & Barroso, CACM 2013).

## Implemented Policies

### 1. Independent (`independent`)
Models ordinary uncoordinated downloaders. Each transfer selects its primary link and demands full link capacity without coordinating with peer transfers. When aggregate demand exceeds a shared link's capacity ($D_\ell > C_\ell$), the simulator enforces physical link capacity strictly and applies a realistic logarithmic congestion/retransmission penalty ($\text{waste} = \min(0.28, 0.085 \ln(D_\ell / C_\ell))$), capturing packet drops and wasted bytes caused by self-inflicted contention.

### 2. Max-Min Fair (`max_min`)
Assigns multi-link transfers to the candidate link maximizing projected per-flow fair share, then divides each link's capacity equally ($C_\ell / n_\ell$) across all active transfers on that link. Eliminates self-inflicted oversubscription waste and maximizes fairness, but delays small files behind large ones on contended links.

### 3. Shortest Predicted Remaining Completion Time (`srpt`)
Places multi-link transfers onto the candidate link with the earliest projected completion time given already-queued bytes, then allocates each link's bandwidth strictly in ascending order of predicted remaining completion time. To avoid the classic EWMA starvation-inversion pathology (where a paused flow's observed rate decays toward zero and makes its estimated remaining time blow up), remaining time is estimated using the link's achievable effective capacity when scheduled.

### 4. FlowWeave Cooperative, Convex-Optimized Scheduler (`flowweave`)
Separates discrete link/route placement from continuous convex bandwidth allocation:

1. **Congestion-aware effective link capacity**: Computes $C_\ell^{\text{eff}} = C_\ell (1 - p_\ell) h_\ell$, applying `congestion_headroom` backoff $h_\ell = 0.92$ only when a link exhibits real congestion (`congestion_ratio > 1.02` or `loss_rate >= 3%`).
2. **Cooperative multi-link placement** (discrete, non-convex): Iterates transfers largest-first and assigns each transfer to the candidate route $P_i$ minimizing marginal increase in batch completion time:
   $$
   \text{cost}(i, \ell) = \frac{\text{ahead}_i + \text{rem}_i}{C_\ell^{\text{eff}}} + \text{behind}_i \cdot \frac{\text{rem}_i}{C_\ell^{\text{eff}}} + \frac{d_\ell}{1000} + 2 p_\ell
   $$
   where $\text{ahead}_i$ is bytes of smaller jobs already on $\ell$ and $\text{behind}_i$ is the count of larger jobs on $\ell$ delayed by inserting transfer $i$.
3. **Continuous convex bandwidth allocation** (`flowweave/scheduler/optimizer.py`):
   - **Single shared link** (exact KKT closed-form fast path, ~0.05–0.15 ms):
     $$\min_{b_i > 0} \sum_{i=1}^n \frac{R_i}{b_i} \quad \text{s.t.} \quad \sum_{i=1}^n b_i \le C^{\text{eff}}, \quad b_i \ge \frac{f \cdot C^{\text{eff}}}{n}$$
     Unconstrained analytical optimum: $b_i^* = C^{\text{eff}} \frac{\sqrt{R_i}}{\sum_j \sqrt{R_j}}$; with minimum fairness floor $b_i \ge b^{\min}$, exact KKT water-filling $b_i^* = \max(b^{\min}, \sqrt{R_i / \lambda})$.
   - **Multi-hop shared routes** (CVXPY + `CLARABEL` conic solver with pure-SciPy canonicalization):
     $$\min_{b_i > 0} \sum_{i=1}^n R_i \cdot \text{inv\_pos}(b_i) \quad \text{s.t.} \quad \sum_{i: \ell \in P_i} b_i \le C_\ell^{\text{eff}} \;\;\forall \ell, \quad b_i \ge b_i^{\min}$$
   - **Genuine multi-link striping** (`allow_striping=True` in `optimize_allocations`):
     $$\min_{b_{i,\ell} \ge 0} \sum_{i=1}^n R_i \cdot \text{inv\_pos}\!\left(\sum_{\ell \in L_i} b_{i,\ell}\right) \quad \text{s.t.} \quad \sum_{i: \ell \in L_i} b_{i,\ell} \le C_\ell^{\text{eff}} \;\;\forall \ell$$
   - **Explicit feasibility & fallback**: If `fairness_fraction` is infeasible ($\sum b_i^{\min} > C_\ell^{\text{eff}}$) or a numerical solver fails, the optimizer never violates link capacities; it reports explicit fallback status (`infeasible_fairness_fallback` or `solver_fallback`) and falls back to capacity-enforced max-min water-filling.

#### Key Mathematical Finding: Static Snapshot Convex Objective vs Dynamic Preemptive $\sum T_i$
Benchmarking all 4 policies (`seed=42`, see `results/experiment_report.md`) reveals an important theoretical distinction:

- Because the static snapshot problem $\min \sum R_i / b_i$ assumes constant rates until completion (ignoring future reallocation after short flows finish), its KKT solution allocates $b_i^* \propto \mathbf{+\sqrt{R_i}}$—giving **more** instantaneous bandwidth to **larger** files.
- **Resulting strength**: `flowweave` achieves the **best makespan, lowest P95 tail, highest aggregate goodput, and highest link utilization** across multi-link, bidirectional, and straggler workloads (`multi_link_offload`: `6.30s` makespan, `6.16s` P95, `15.16 MB/s` goodput, `82.1%` link util; `bidirectional_mixed`: `3.00s` makespan, `2.75s` P95, `11.50 MB/s`; `straggler_recovery`: `24.30s` sum FCT, `4.87s` P95, `5.00s` makespan, `6.60 MB/s`).
- **Resulting limitation**: Under heavy-tailed file sizes on a single shared bottleneck (`mixed_shared_bottleneck`), static $\sqrt{R_i}$ allocation delays small files behind large ones compared to dynamic preemptive prioritization (`srpt`: `20.70s` vs `flowweave`: `37.50s` sum FCT, while matching best makespan `9.20s` and `9.76 MB/s` goodput). Minimizing dynamic preemptive $\sum T_i$ requires prioritizing *smaller* remaining sizes (SRPT/Smith's rule), whereas static $\min \sum R_i / b_i$ is optimal only when rates cannot be reallocated mid-workload.

## Cost-Benefit Straggler Replication

A chunk is flagged as a straggler only after `min_chunk_age_s` (0.5s) if its observed rate falls below `slow_ratio_threshold` (45%) of its **allocated per-chunk rate** (preventing false positives on intentionally paced large flows). Before spawning a replica on an alternate candidate link $\ell_{\text{alt}}$:

- Estimated saving: $\Delta t_{\text{save}} = \frac{B_{\text{rem}}}{r_{\text{slow}}} - \left(\frac{d_{\ell_{\text{alt}}}}{1000} + \frac{B_{\text{rem}}}{r_{\text{alt}}}\right)$
- Congestion externality on $k$ co-located flows sharing $\ell_{\text{alt}}$: $\Delta t_{\text{cost}} = w_{\text{cost}} \cdot k \cdot \frac{B_{\text{rem}}}{C_{\ell_{\text{alt}}}^{\text{eff}}} \cdot \text{util}_{\ell_{\text{alt}}}$
- Global redundant budget check: $\frac{\text{redundant} + B_{\text{rem}}}{\text{total\_bytes}} \le 10\%$

Replication triggers only if $\Delta t_{\text{save}} - \Delta t_{\text{cost}} \ge 0.25\text{s}$ and the budget permits. Whichever copy finishes first completes the logical chunk; the losing copy is cancelled immediately and its transmitted bytes are accounted as `redundant_bytes`.

## Observed Tradeoffs (From Reproducible Benchmarks)

Measured at `seed=42` across all 4 policies (`independent`, `max_min`, `srpt`, `flowweave`; see `results/experiment_report.md`):

- **Multi-link offloading (`multi_link_offload`)**: FlowWeave achieves the best P95 (`6.16s`), best makespan (`6.30s` vs `7.20s` `max_min`, `8.00s` `srpt`, `14.10s` `independent`), highest goodput (`15.16 MB/s`), highest link utilization (`82.1%`), lowest redundant bytes (`873.9 KB`), and cuts sum FCT by 39.8% vs `independent` (`45.30s` vs `75.30s`).
- **Bidirectional mixed (`bidirectional_mixed`)**: FlowWeave achieves the best P95 (`2.75s`), best makespan (`3.00s`), highest goodput (`11.50 MB/s`), and highest link utilization (`80.7%`) over separate full-duplex uplink/downlink + shared ISP core links.
- **Straggler recovery (`straggler_recovery`)**: Cost-benefit replication cuts FlowWeave sum FCT to `24.30s` (best of all 4 policies, vs `34.60s` `max_min` and `42.10s` `srpt`), with best P95 (`4.87s`), best makespan (`5.00s`), and highest goodput (`6.60 MB/s`).
- **Single shared bottleneck (`mixed_shared_bottleneck`)**: FlowWeave matches the best makespan (`9.20s`), highest goodput (`9.76 MB/s`), and highest link utilization (`98.0%`) with 95% less redundant overhead than `independent`, while `srpt` achieves the lowest dynamic sum FCT (`20.70s`) by prioritizing short files first.
- **Uniform identical sizes (`uniform_sizes_fairness`)**: With equal remaining bytes, $\sqrt{R_i}$ reduces to exact equal sharing—FlowWeave matches `max_min` with `1.000` Jain fairness, `6.50s` makespan, `9.85 MB/s` goodput, and `0.0 KB` redundant traffic.
