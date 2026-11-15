from __future__ import annotations

from collections.abc import Iterable
import math


def clamp(value: float, low: float, high: float) -> float:
    return min(max(value, low), high)


def allocate_by_weights(
    weights: dict[str, float],
    total_capacity: float,
    minimums: dict[str, float] | None = None,
) -> dict[str, float]:
    """Allocate capacity according to normalized weights while respecting minimum rates."""

    if not math.isfinite(total_capacity) or total_capacity < 0:
        raise ValueError("total_capacity must be finite and non-negative")
    if any(not math.isfinite(weight) or weight < 0 for weight in weights.values()):
        raise ValueError("weights must be finite and non-negative")
    minimums = minimums or {}
    if any(
        not math.isfinite(minimums.get(transfer_id, 0.0))
        or minimums.get(transfer_id, 0.0) < 0
        for transfer_id in weights
    ):
        raise ValueError("minimum rates must be finite and non-negative")

    if not weights:
        return {}
    if total_capacity == 0:
        return {transfer_id: 0.0 for transfer_id in weights}

    remaining = float(total_capacity)
    allocations: dict[str, float] = {}
    minimum_total = sum(minimums.get(transfer_id, 0.0) for transfer_id in weights)

    if minimum_total > total_capacity:
        min_cap = total_capacity / minimum_total
        for transfer_id in weights:
            allocations[transfer_id] = minimums.get(transfer_id, 0.0) * min_cap
        return allocations

    for transfer_id in weights:
        minimum_value = minimums.get(transfer_id, 0.0)
        allocations[transfer_id] = minimum_value
        remaining -= minimum_value

    proportional = {transfer_id: weight for transfer_id, weight in weights.items() if weight > 0}
    proportional_total = sum(proportional.values())
    if proportional_total <= 0:
        return {transfer_id: allocations[transfer_id] for transfer_id in weights}

    for transfer_id in proportional:
        share = remaining * (proportional[transfer_id] / proportional_total)
        allocations[transfer_id] += share

    return allocations


def compute_service_deficit(
    delivered: float,
    expected: float,
) -> float:
    """Return a ratio capturing whether a transfer has received service below its target."""

    if not math.isfinite(delivered) or not math.isfinite(expected):
        raise ValueError("service measurements must be finite")
    if delivered < 0 or expected < 0:
        raise ValueError("service measurements must be non-negative")
    deficit = max(0.0, expected - delivered)
    if expected == 0:
        return 1.0
    return 1.0 + (deficit / expected)


def list_active(transfer_iterable: Iterable[object]) -> list[object]:
    return [transfer for transfer in transfer_iterable if getattr(transfer, "state", "") != "completed"]
