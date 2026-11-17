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

### 4. FlowWeave Cooperative, Congestion-Aware Scheduler (`flowweave`)
Executes three steps on every scheduling tick:

1. **Congestion-aware link budgeting**: Computes effective link capacity $C_\ell^{\text{eff}} = C_\ell (1 - p_\ell) h_\ell$, applying headroom backoff $h_\ell = 0.92$ only when a link exhibits real congestion (`congestion_ratio > 1.02` or `loss_rate >= 3%`).
2. **Cooperative multi-link placement**: Iterates transfers largest-first (minimizing makespan imbalance across links) and assigns each transfer to the candidate link minimizing marginal increase in batch completion time:
   $$
   \text{cost}(i, \ell) = \frac{\text{ahead}_i + \text{rem}_i}{C_\ell^{\text{eff}}} + \text{behind}_i \cdot \frac{\text{rem}_i}{C_\ell^{\text{eff}}} + \frac{d_\ell}{1000} + 2 p_\ell
   $$
   where $\text{ahead}_i$ is bytes of smaller jobs already on $\ell$ and $\text{behind}_i$ is the count of larger jobs on $\ell$ delayed by inserting transfer $i$.
3. **Fairness-guarded weighted SRPT**: On each link, reserves a fairness floor (`min_fair_share_fraction`, default 15%) shared equally among active transfers so large files never starve, then distributes the remaining 85% pool via a priority waterfall (70% strict priority capped at per-tick demand so leftover bandwidth immediately spills to the next flow, plus 30% score-proportional blending with gentle starvation aging).

## Cost-Benefit Straggler Replication

A chunk is flagged as a straggler only after `min_chunk_age_s` (0.5s) if its observed rate falls below `slow_ratio_threshold` (45%) of its **allocated per-chunk rate** (preventing false positives on intentionally paced large flows). Before spawning a replica on an alternate candidate link $\ell_{\text{alt}}$:

- Estimated saving: $\Delta t_{\text{save}} = \frac{B_{\text{rem}}}{r_{\text{slow}}} - \left(\frac{d_{\ell_{\text{alt}}}}{1000} + \frac{B_{\text{rem}}}{r_{\text{alt}}}\right)$
- Congestion externality on $k$ co-located flows sharing $\ell_{\text{alt}}$: $\Delta t_{\text{cost}} = w_{\text{cost}} \cdot k \cdot \frac{B_{\text{rem}}}{C_{\ell_{\text{alt}}}^{\text{eff}}} \cdot \text{util}_{\ell_{\text{alt}}}$
- Global redundant budget check: $\frac{\text{redundant} + B_{\text{rem}}}{\text{total\_bytes}} \le 10\%$

Replication triggers only if $\Delta t_{\text{save}} - \Delta t_{\text{cost}} \ge 0.25\text{s}$ and the budget permits. Whichever copy finishes first completes the logical chunk; the losing copy is cancelled immediately and its transmitted bytes are accounted as `redundant_bytes`.

## Observed Tradeoffs (From Reproducible Benchmarks)

Measured at `seed=42` (see `results/experiment_report.md`):

- **Single shared bottleneck (`mixed_shared_bottleneck`)**: FlowWeave achieves `21.00s` sum FCT (33.5% better than `independent`'s `31.60s`, 21.6% better than `max_min`'s `26.80s`, within 1.4% of theoretical single-link `srpt`'s `20.70s`) while achieving the best P95 (`6.46s`) and highest Jain fairness (`0.918`).
- **Multi-link offloading (`multi_link_offload`)**: FlowWeave outperforms all three baselines on every metric: `26.60s` sum FCT (-64.7% vs `independent`, -20.6% vs `max_min`, -16.9% vs `srpt`), `6.70s` makespan, `14.25 MB/s` goodput, and `0.992` Jain fairness.
- **Straggler recovery (`straggler_recovery`)**: Cost-benefit replication cuts sum FCT to `23.20s` vs `34.60s` (`max_min`) and `42.10s` (`srpt`), where unhedged policies stall on degraded chunks.
- **Uniform identical sizes (`uniform_sizes_fairness`)**: Highlights the real fairness/FCT tradeoff—`max_min` achieves `1.000` Jain fairness (`52.00s` sum FCT), strict `srpt` drops sum FCT to `32.10s` at `0.599` fairness, and FlowWeave sits in between (`35.50s` sum FCT, `0.697` fairness).
- **Dynamic bandwidth drop (`dynamic_bandwidth_drop`)**: Strict `srpt` slightly edges FlowWeave on sum FCT (`16.70s` vs `18.50s`) because FlowWeave backs off by `congestion_headroom = 0.92` during the 6% loss spike, trading ~1.8s of sum FCT for conservative loss avoidance.
