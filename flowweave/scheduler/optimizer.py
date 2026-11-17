"""Convex optimization engine for single-link, multi-hop, and multi-link bandwidth allocation.

Mathematical formulation:
- Single shared link (effective capacity C_eff, remaining bytes R_i > 0):
      min  sum_i (R_i / b_i)
      s.t. sum_i b_i <= C_eff,   b_i >= b_min_i > 0
  Unconstrained KKT closed-form: b_i* = C_eff * sqrt(R_i) / sum_j sqrt(R_j).
  With lower bounds b_i >= b_min_i: exact KKT water-filling b_i* = max(b_min_i, sqrt(R_i / lambda)).

- Multi-hop shared routes (fixed route P_i per transfer i):
      min  sum_i R_i * inv_pos(b_i)
      s.t. sum_{i: l in P_i} b_i <= C_eff_l   for all links l
           b_i >= b_min_i

- Genuine multi-link striping (transfer i may split data across permitted links L_i):
      min  sum_i R_i * inv_pos(sum_{l in L_i} b_{i,l})
      s.t. sum_{i: l in L_i} b_{i,l} <= C_eff_l   for all links l
           b_{i,l} >= 0,  sum_{l in L_i} b_{i,l} >= b_min_i

Note: Discrete link/route placement is non-convex and remains handled by the cooperative
placement heuristic. This module solves only the continuous bandwidth allocation subproblem.
"""

from __future__ import annotations

import logging
import math
import time
from collections import defaultdict
from dataclasses import dataclass, field

import numpy as np

from flowweave.models import LinkState, TransferSnapshot, TransferState
from flowweave.scheduler.base import (
    effective_capacity,
    validate_and_enforce_allocations,
    valid_routes_for,
)

logger = logging.getLogger("flowweave.optimizer")

_SCALE_BYTES = 1_000_000.0  # Scale bytes -> MB for well-conditioned conic solves


@dataclass
class TransferLinkAllocation:
    """Per-(transfer, link) bandwidth allocation produced by the optimizer."""

    transfer_id: str
    link_id: str
    allocated_bps: float
    predicted_completion_time_s: float


@dataclass
class OptimizationResult:
    """Complete output of one convex bandwidth optimization solve."""

    allocations: list[TransferLinkAllocation]
    per_transfer_bps: dict[str, float]
    per_transfer_link_bps: dict[tuple[str, str], float]
    predicted_completion_times_s: dict[str, float]
    link_utilization: dict[str, float]
    effective_capacities_bps: dict[str, float]
    status: str
    solver_name: str
    solve_time_ms: float
    objective_value_s: float | None
    fairness_fraction: float | None
    mode: str
    warnings: list[str] = field(default_factory=list)


def solve_single_link_analytical(
    remaining_bytes: dict[str, float],
    capacity_bps: float,
    *,
    min_floor_bps: float = 0.0,
) -> tuple[dict[str, float], str]:
    """Exact KKT closed-form solver for min sum(R_i / b_i) on one link of capacity C.

    Returns (allocations_bps, status). Raises ValueError if min_floor_bps is infeasible
    (i.e. n * min_floor_bps > capacity_bps).
    """
    active = {tid: float(rem) for tid, rem in remaining_bytes.items() if rem > 0.0}
    if not active or capacity_bps <= 0.0:
        return {tid: 0.0 for tid in remaining_bytes}, "trivial_empty"

    n = len(active)
    floor = max(0.0, float(min_floor_bps))
    if floor * n > capacity_bps * (1.0 + 1e-9):
        raise ValueError(
            f"Infeasible fairness floor: {n} transfers x {floor:.1f} B/s > capacity {capacity_bps:.1f} B/s"
        )

    if floor <= 0.0:
        sqrts = {tid: math.sqrt(rem) for tid, rem in active.items()}
        denom = sum(sqrts.values())
        allocs = {tid: capacity_bps * (s / denom) for tid, s in sqrts.items()}
        for tid in remaining_bytes:
            allocs.setdefault(tid, 0.0)
        return allocs, "optimal_analytical"

    # Exact KKT water-filling with uniform lower bound b_i >= floor:
    # Freeze transfers whose unconstrained sqrt(R_i) share falls below `floor`.
    frozen: dict[str, float] = {}
    unfrozen = dict(active)
    rem_cap = float(capacity_bps)

    while unfrozen:
        sqrts = {tid: math.sqrt(rem) for tid, rem in unfrozen.items()}
        denom = sum(sqrts.values())
        newly_frozen = [
            tid for tid, s in sqrts.items() if rem_cap * (s / denom) < floor - 1e-9
        ]
        if not newly_frozen:
            for tid, s in sqrts.items():
                frozen[tid] = rem_cap * (s / denom)
            break
        for tid in newly_frozen:
            frozen[tid] = floor
            rem_cap = max(0.0, rem_cap - floor)
            unfrozen.pop(tid)

    for tid in remaining_bytes:
        frozen.setdefault(tid, 0.0)
    return frozen, "optimal_analytical"


def _effective_link_caps(
    links: dict[str, LinkState],
    congestion_headroom: float,
) -> dict[str, float]:
    """Compute C_eff = C * (1 - p) * h using FlowWeave's existing effective_capacity model."""
    caps: dict[str, float] = {}
    for lid, link in links.items():
        if link.capacity_bps <= 0.0:
            caps[lid] = 0.0
        else:
            caps[lid] = effective_capacity(link, congestion_headroom=congestion_headroom)
    return caps


def _fallback_waterfill_routes(
    eligible_ids: list[str],
    routes: dict[str, list[str]],
    eff_caps: dict[str, float],
) -> dict[str, float]:
    """Capacity-safe max-min water-filling fallback when solver fails or fairness is infeasible."""
    rem_cap = dict(eff_caps)
    unfrozen = set(eligible_ids)
    allocs: dict[str, float] = {tid: 0.0 for tid in eligible_ids}

    while unfrozen:
        flows_on_link: dict[str, list[str]] = defaultdict(list)
        for tid in unfrozen:
            for lid in routes[tid]:
                flows_on_link[lid].append(tid)
        if not flows_on_link:
            break
        bottleneck = min(
            flows_on_link.keys(),
            key=lambda lid: rem_cap[lid] / len(flows_on_link[lid]),
        )
        share = max(0.0, rem_cap[bottleneck] / len(flows_on_link[bottleneck]))
        for tid in list(flows_on_link[bottleneck]):
            allocs[tid] = share
            for lid in routes[tid]:
                rem_cap[lid] = max(0.0, rem_cap[lid] - share)
            unfrozen.remove(tid)

    return allocs


def _solve_cvxpy_fixed_routes(
    eligible: list[TransferSnapshot],
    routes: dict[str, list[str]],
    eff_caps: dict[str, float],
    min_floors: dict[str, float],
    solver_override: str | None = None,
) -> tuple[dict[str, float], str, str]:
    """Solve convex sum-FCT problem over fixed (possibly multi-hop) shared routes using CVXPY."""
    import cvxpy as cp

    n = len(eligible)
    tids = [t.transfer_id for t in eligible]
    rem_mb = np.asarray([t.remaining_bytes / _SCALE_BYTES for t in eligible], dtype=np.float64)

    b = cp.Variable(n, pos=True)
    objective = cp.Minimize(cp.sum(cp.multiply(rem_mb, cp.inv_pos(b))))

    link_to_indices: dict[str, list[int]] = defaultdict(list)
    for idx, tid in enumerate(tids):
        for lid in routes[tid]:
            link_to_indices[lid].append(idx)

    constraints = []
    for lid, idxs in link_to_indices.items():
        cap_mb = eff_caps[lid] / _SCALE_BYTES
        constraints.append(cp.sum(b[idxs]) <= cap_mb)

    for idx, tid in enumerate(tids):
        floor_mb = min_floors.get(tid, 0.0) / _SCALE_BYTES
        if floor_mb > 0.0:
            constraints.append(b[idx] >= floor_mb)

    problem = cp.Problem(objective, constraints)
    if not problem.is_dcp():
        raise ValueError("Fixed-route sum-FCT formulation failed DCP convexity check")

    solver_name = _solve_problem(problem, solver_override=solver_override)
    if problem.status not in (cp.OPTIMAL, cp.OPTIMAL_INACCURATE) or b.value is None:
        raise RuntimeError(f"CVXPY status: {problem.status}")

    vals = np.asarray(b.value, dtype=np.float64)
    if not np.all(np.isfinite(vals)):
        raise RuntimeError("CVXPY returned non-finite allocation values")

    allocs = {tid: max(0.0, float(vals[idx]) * _SCALE_BYTES) for idx, tid in enumerate(tids)}
    return allocs, f"optimal_cvxpy({problem.status})", solver_name


def _solve_cvxpy_striping(
    eligible: list[TransferSnapshot],
    permitted_links: dict[str, list[str]],
    eff_caps: dict[str, float],
    min_floors: dict[str, float],
    solver_override: str | None = None,
) -> tuple[dict[tuple[str, str], float], str, str]:
    """Solve general multi-link striping convex problem: min sum_i R_i / sum_{l in P_i} b_{i,l}."""
    import cvxpy as cp

    # Build sparse variable index over valid (transfer, link) pairs only.
    pairs: list[tuple[str, str]] = []
    transfer_pair_idxs: dict[str, list[int]] = defaultdict(list)
    link_pair_idxs: dict[str, list[int]] = defaultdict(list)

    for t in eligible:
        tid = t.transfer_id
        for lid in permitted_links[tid]:
            p_idx = len(pairs)
            pairs.append((tid, lid))
            transfer_pair_idxs[tid].append(p_idx)
            link_pair_idxs[lid].append(p_idx)

    x = cp.Variable(len(pairs), nonneg=True)
    terms = []
    constraints = []

    for t in eligible:
        tid = t.transfer_id
        idxs = transfer_pair_idxs[tid]
        total_rate = cp.sum(x[idxs])
        terms.append((t.remaining_bytes / _SCALE_BYTES) * cp.inv_pos(total_rate))
        floor_mb = min_floors.get(tid, 0.0) / _SCALE_BYTES
        if floor_mb > 0.0:
            constraints.append(total_rate >= floor_mb)

    for lid, idxs in link_pair_idxs.items():
        cap_mb = eff_caps[lid] / _SCALE_BYTES
        constraints.append(cp.sum(x[idxs]) <= cap_mb)

    problem = cp.Problem(cp.Minimize(cp.sum(terms)), constraints)
    if not problem.is_dcp():
        raise ValueError("Multi-link striping formulation failed DCP convexity check")

    solver_name = _solve_problem(problem, solver_override=solver_override)
    if problem.status not in (cp.OPTIMAL, cp.OPTIMAL_INACCURATE) or x.value is None:
        raise RuntimeError(f"CVXPY striping status: {problem.status}")

    vals = np.asarray(x.value, dtype=np.float64)
    if not np.all(np.isfinite(vals)):
        raise RuntimeError("CVXPY returned non-finite striping allocations")

    pair_allocs: dict[tuple[str, str], float] = {}
    for idx, pair in enumerate(pairs):
        v = float(vals[idx]) * _SCALE_BYTES
        # Snap solver numerical dust (< 1 B/s) cleanly to 0
        pair_allocs[pair] = 0.0 if v < 1.0 else v

    return pair_allocs, f"optimal_cvxpy({problem.status})", solver_name


def _solve_problem(problem, *, solver_override: str | None = None) -> str:
    """Invoke CLARABEL (preferred conic solver) with pure-SciPy canonicalization backend."""
    import cvxpy as cp

    # Use SciPy canonicalization backend so environments without C++ _cvxcore work seamlessly.
    canon = getattr(cp, "SCIPY_CANON_BACKEND", None)
    candidates = [solver_override] if solver_override else ["CLARABEL", "SCS"]
    last_exc: Exception | None = None

    for name in candidates:
        if not name:
            continue
        kwargs: dict = {"solver": name}
        if canon is not None:
            kwargs["canon_backend"] = canon
        if name == "CLARABEL":
            kwargs.update({"tol_gap_abs": 1e-8, "tol_gap_rel": 1e-8, "tol_feas": 1e-8, "max_iter": 100})
        elif name == "SCS":
            kwargs.update({"eps": 1e-6, "max_iters": 2500})
        try:
            problem.solve(**kwargs)
            return name
        except Exception as exc:
            last_exc = exc
            continue

    raise RuntimeError(f"All CVXPY solvers failed: {last_exc}")


def _enforce_effective_link_caps_striping(
    pair_allocs: dict[tuple[str, str], float],
    eff_caps: dict[str, float],
) -> dict[tuple[str, str], float]:
    """Guarantee sum_{i} b_{i,l} <= C_eff_l and b_{i,l} >= 0 for striping allocations."""
    clamped = {pair: max(0.0, float(v)) if math.isfinite(v) else 0.0 for pair, v in pair_allocs.items()}
    by_link: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for pair in clamped:
        by_link[pair[1]].append(pair)

    for lid, pairs in by_link.items():
        cap = max(0.0, eff_caps.get(lid, 0.0))
        total = sum(clamped[p] for p in pairs)
        if total > cap * (1.0 + 1e-9) and total > 0.0:
            scale = cap / total
            for p in pairs:
                clamped[p] *= scale
    return clamped


def optimize_allocations(
    transfers: list[TransferSnapshot],
    network_links: dict[str, LinkState],
    *,
    routes: dict[str, list[str]] | None = None,
    fairness_fraction: float | None = None,
    congestion_headroom: float = 0.92,
    allow_striping: bool = False,
    force_cvxpy: bool = False,
    solver_override: str | None = None,
) -> OptimizationResult:
    """Compute convex-optimal bandwidth allocations minimizing sum(R_i / b_i).

    - When `allow_striping=False` (default, matching FlowWeave's single-route placement):
      uses exact analytical KKT on independent single-hop links (fast path), and CVXPY
      on coupled multi-hop routes (or whenever `force_cvxpy=True`).
    - When `allow_striping=True`: solves the general multi-link striping convex problem
      where each transfer can split its data simultaneously across its permitted links.
    """
    t0 = time.perf_counter()
    warnings: list[str] = []
    eff_caps = _effective_link_caps(network_links, congestion_headroom)

    # Filter to active transfers with positive remaining bytes and at least one usable link.
    eligible: list[TransferSnapshot] = []
    resolved_routes: dict[str, list[str]] = {}
    permitted_links: dict[str, list[str]] = {}

    for t in transfers:
        if t.state not in (TransferState.ACTIVE, TransferState.PENDING) or t.remaining_bytes <= 0:
            continue
        if allow_striping:
            cands = [
                lid
                for lid in dict.fromkeys(t.candidate_links or t.route or [t.assigned_link])
                if lid in network_links and eff_caps.get(lid, 0.0) > 0.0
            ]
            if not cands:
                warnings.append(f"Transfer {t.transfer_id} has no feasible link")
                continue
            permitted_links[t.transfer_id] = cands
            resolved_routes[t.transfer_id] = [cands[0]]
            eligible.append(t)
        else:
            if routes is not None and t.transfer_id in routes:
                route = [lid for lid in routes[t.transfer_id] if lid in network_links]
            else:
                cand_routes = valid_routes_for(t, network_links)
                route = cand_routes[0] if cand_routes else []
            if not route or any(eff_caps.get(lid, 0.0) <= 0.0 for lid in route):
                warnings.append(f"Transfer {t.transfer_id} has no feasible route")
                continue
            resolved_routes[t.transfer_id] = route
            eligible.append(t)

    if not eligible:
        return OptimizationResult(
            allocations=[],
            per_transfer_bps={t.transfer_id: 0.0 for t in transfers},
            per_transfer_link_bps={},
            predicted_completion_times_s={},
            link_utilization={lid: 0.0 for lid in network_links},
            effective_capacities_bps=eff_caps,
            status="empty_no_active_transfers",
            solver_name="none",
            solve_time_ms=(time.perf_counter() - t0) * 1000.0,
            objective_value_s=0.0,
            fairness_fraction=fairness_fraction,
            mode="multilink_striping" if allow_striping else "single_link",
            warnings=warnings,
        )

    # Validate fairness fraction and compute per-transfer minimum bandwidth floors.
    min_floors: dict[str, float] = {}
    fairness_infeasible = False
    if fairness_fraction is not None and fairness_fraction > 0.0:
        if fairness_fraction > 1.0:
            fairness_infeasible = True
            warnings.append(f"Infeasible fairness_fraction={fairness_fraction:.3f} (> 1.0)")
        else:
            if allow_striping:
                link_users: dict[str, int] = defaultdict(int)
                for tid, lids in permitted_links.items():
                    for lid in lids:
                        link_users[lid] += 1
                for t in eligible:
                    tid = t.transfer_id
                    # Fair floor from transfer's best permitted link
                    best_share = max(eff_caps[lid] / link_users[lid] for lid in permitted_links[tid])
                    min_floors[tid] = fairness_fraction * best_share
                # Check total floor demand vs total available capacity across all used links
                used_links = {lid for lids in permitted_links.values() for lid in lids}
                if sum(min_floors.values()) > sum(eff_caps[lid] for lid in used_links) * (1.0 + 1e-9):
                    fairness_infeasible = True
                    warnings.append("Infeasible fairness floors exceed aggregate link capacity")
            else:
                link_users = defaultdict(int)
                for tid, route in resolved_routes.items():
                    for lid in route:
                        link_users[lid] += 1
                for t in eligible:
                    tid = t.transfer_id
                    route = resolved_routes[tid]
                    bottleneck_fair = min(eff_caps[lid] / link_users[lid] for lid in route)
                    min_floors[tid] = fairness_fraction * bottleneck_fair
                # Verify per-link feasibility: sum_{i: l in P_i} b_min_i <= C_eff_l
                for lid, count in link_users.items():
                    demand = sum(
                        min_floors[tid] for tid, route in resolved_routes.items() if lid in route
                    )
                    if demand > eff_caps[lid] * (1.0 + 1e-9):
                        fairness_infeasible = True
                        warnings.append(
                            f"Infeasible fairness floor on link {lid}: {demand:.1f} > {eff_caps[lid]:.1f}"
                        )

    per_transfer_bps: dict[str, float] = {t.transfer_id: 0.0 for t in transfers}
    per_pair_bps: dict[tuple[str, str], float] = {}
    status = "optimal"
    solver_name = "analytical_kkt"
    mode = "multilink_striping" if allow_striping else "single_link"

    if fairness_infeasible:
        # Explicit safe fallback: never violate link capacity when fairness constraints are infeasible.
        if allow_striping:
            fb_routes = {tid: [permitted_links[tid][0]] for tid in permitted_links}
        else:
            fb_routes = resolved_routes
        fb = _fallback_waterfill_routes([t.transfer_id for t in eligible], fb_routes, eff_caps)
        for tid, rate in fb.items():
            per_transfer_bps[tid] = rate
            for lid in fb_routes[tid]:
                per_pair_bps[(tid, lid)] = rate
        status = "infeasible_fairness_fallback"
        solver_name = "fallback_waterfill"
    elif allow_striping:
        mode = "multilink_striping"
        try:
            raw_pairs, status, solver_name = _solve_cvxpy_striping(
                eligible, permitted_links, eff_caps, min_floors, solver_override=solver_override
            )
            per_pair_bps = _enforce_effective_link_caps_striping(raw_pairs, eff_caps)
            for (tid, _lid), rate in per_pair_bps.items():
                per_transfer_bps[tid] = per_transfer_bps.get(tid, 0.0) + rate
        except Exception as exc:
            logger.warning("CVXPY striping solve failed (%s); falling back to waterfill", exc)
            warnings.append(f"Solver fallback triggered: {exc}")
            fb_routes = {tid: [permitted_links[tid][0]] for tid in permitted_links}
            fb = _fallback_waterfill_routes([t.transfer_id for t in eligible], fb_routes, eff_caps)
            for tid, rate in fb.items():
                per_transfer_bps[tid] = rate
                per_pair_bps[(tid, fb_routes[tid][0])] = rate
            status = "solver_fallback"
            solver_name = "fallback_waterfill"
    else:
        is_multihop = any(len(r) > 1 for r in resolved_routes.values())
        if not is_multihop and not force_cvxpy:
            # Independent single-hop links: exact closed-form KKT per link (zero solver overhead).
            mode = "single_link"
            by_link: dict[str, list[TransferSnapshot]] = defaultdict(list)
            for t in eligible:
                by_link[resolved_routes[t.transfer_id][0]].append(t)

            for lid, group in by_link.items():
                cap = eff_caps[lid]
                rem_map = {t.transfer_id: float(t.remaining_bytes) for t in group}
                floor = (fairness_fraction * cap / len(group)) if (fairness_fraction and fairness_fraction > 0) else 0.0
                link_allocs, link_status = solve_single_link_analytical(
                    rem_map, cap, min_floor_bps=floor
                )
                status = link_status
                for tid, rate in link_allocs.items():
                    per_transfer_bps[tid] = rate
                    per_pair_bps[(tid, lid)] = rate
            solver_name = "analytical_kkt"
        else:
            mode = "multihop_shared" if is_multihop else "single_link_cvxpy"
            try:
                solved, status, solver_name = _solve_cvxpy_fixed_routes(
                    eligible, resolved_routes, eff_caps, min_floors, solver_override=solver_override
                )
                # Validate against effective capacities strictly.
                eff_link_objs = {
                    lid: LinkState(link_id=lid, capacity_bps=cap) for lid, cap in eff_caps.items()
                }
                validated = validate_and_enforce_allocations(
                    solved,
                    resolved_routes,
                    eff_link_objs,
                    {t.transfer_id for t in eligible},
                )
                for tid, rate in validated.items():
                    per_transfer_bps[tid] = rate
                    for lid in resolved_routes[tid]:
                        per_pair_bps[(tid, lid)] = rate
            except Exception as exc:
                logger.warning("CVXPY fixed-route solve failed (%s); falling back to waterfill", exc)
                warnings.append(f"Solver fallback triggered: {exc}")
                fb = _fallback_waterfill_routes(
                    [t.transfer_id for t in eligible], resolved_routes, eff_caps
                )
                for tid, rate in fb.items():
                    per_transfer_bps[tid] = rate
                    for lid in resolved_routes[tid]:
                        per_pair_bps[(tid, lid)] = rate
                status = "solver_fallback"
                solver_name = "fallback_waterfill"

    # Compute predicted completion times (R_i / b_i), objective value, and link utilization.
    predicted_t: dict[str, float] = {}
    obj_sum = 0.0
    for t in eligible:
        rate = per_transfer_bps.get(t.transfer_id, 0.0)
        if rate > 0.0:
            pred = t.remaining_bytes / rate
            predicted_t[t.transfer_id] = pred
            obj_sum += pred
        else:
            predicted_t[t.transfer_id] = float("inf")

    alloc_records: list[TransferLinkAllocation] = []
    used_per_link: dict[str, float] = defaultdict(float)
    for (tid, lid), rate in per_pair_bps.items():
        used_per_link[lid] += rate
        if rate > 0.0 or not allow_striping:
            alloc_records.append(
                TransferLinkAllocation(
                    transfer_id=tid,
                    link_id=lid,
                    allocated_bps=rate,
                    predicted_completion_time_s=predicted_t.get(tid, float("inf")),
                )
            )

    link_util: dict[str, float] = {}
    for lid, cap in eff_caps.items():
        link_util[lid] = min(1.0, used_per_link.get(lid, 0.0) / cap) if cap > 0.0 else 0.0

    solve_ms = (time.perf_counter() - t0) * 1000.0
    return OptimizationResult(
        allocations=alloc_records,
        per_transfer_bps=per_transfer_bps,
        per_transfer_link_bps=per_pair_bps,
        predicted_completion_times_s=predicted_t,
        link_utilization=link_util,
        effective_capacities_bps=eff_caps,
        status=status,
        solver_name=solver_name,
        solve_time_ms=solve_ms,
        objective_value_s=obj_sum if math.isfinite(obj_sum) else None,
        fairness_fraction=fairness_fraction,
        mode=mode,
        warnings=warnings,
    )
