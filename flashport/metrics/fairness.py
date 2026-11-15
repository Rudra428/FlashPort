from __future__ import annotations

import math


def jain_fairness_index(values: list[float]) -> float:
    if not values:
        return 0.0
    if any(not math.isfinite(value) or value < 0 for value in values):
        raise ValueError("fairness values must be finite and non-negative")
    scale = max(values)
    if scale == 0:
        return 0.0
    positive = [value / scale for value in values]
    if sum(positive) == 0:
        return 0.0
    numerator = sum(positive) ** 2
    denominator = len(positive) * sum(value * value for value in positive)
    if denominator == 0:
        return 0.0
    return min(1.0, numerator / denominator)
