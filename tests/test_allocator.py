from flashport.scheduler.allocator import allocate_by_weights

import pytest


def test_allocate_by_weights_respects_capacity_constraints() -> None:
    allocations = allocate_by_weights({"a": 2.0, "b": 1.0}, 100.0, {"a": 10.0, "b": 5.0})
    assert abs(sum(allocations.values()) - 100.0) < 1e-6
    assert allocations["a"] >= 10.0
    assert allocations["b"] >= 5.0


def test_allocate_by_weights_handles_zero_weights() -> None:
    allocations = allocate_by_weights({"a": 0.0, "b": 0.0}, 100.0)
    assert sum(allocations.values()) == 0.0


def test_allocator_scales_minimums_when_they_are_infeasible() -> None:
    allocations = allocate_by_weights(
        {"a": 1.0, "b": 2.0},
        10.0,
        {"a": 8.0, "b": 8.0},
    )
    assert sum(allocations.values()) == pytest.approx(10.0)
    assert allocations == pytest.approx({"a": 5.0, "b": 5.0})


@pytest.mark.parametrize("capacity", [-1.0, float("nan"), float("inf")])
def test_allocator_rejects_invalid_capacity(capacity: float) -> None:
    with pytest.raises(ValueError):
        allocate_by_weights({"a": 1.0}, capacity)


def test_allocator_rejects_negative_weight_and_minimum() -> None:
    with pytest.raises(ValueError):
        allocate_by_weights({"a": -1.0}, 10.0)
    with pytest.raises(ValueError):
        allocate_by_weights({"a": 1.0}, 10.0, {"a": -1.0})
