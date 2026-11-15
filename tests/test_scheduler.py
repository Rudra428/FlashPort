from flashport.config import SchedulerConfig
from flashport.core.models import TransferState
from flashport.scheduler.equal_share import EqualShareScheduler
from flashport.scheduler.flashport import FlashportScheduler
from flashport.scheduler.independent import IndependentScheduler
from flashport.scheduler.srpt import SRPTScheduler


def make_transfer(transfer_id: str, size: int) -> TransferState:
    return TransferState(
        transfer_id=transfer_id,
        client_id="client",
        filename=f"{transfer_id}.bin",
        size=size,
        downloaded=0,
        goodput=0.0,
    )


def test_equal_share_allocates_evenly() -> None:
    scheduler = EqualShareScheduler(SchedulerConfig(total_capacity=100.0))
    transfers = [make_transfer("a", 100), make_transfer("b", 100)]
    allocations = scheduler.allocate(transfers, 100.0)
    assert allocations["a"] == 50.0
    assert allocations["b"] == 50.0


def test_srpt_gives_preference_to_smaller_remaining_file() -> None:
    scheduler = SRPTScheduler(SchedulerConfig(total_capacity=100.0))
    transfers = [make_transfer("small", 20), make_transfer("large", 100)]
    allocations = scheduler.allocate(transfers, 100.0)
    assert allocations == {"small": 100.0, "large": 0.0}


def test_flashport_allocations_are_bounded_and_sum_to_capacity() -> None:
    scheduler = FlashportScheduler(SchedulerConfig(total_capacity=100.0, min_rate=1.0, chunk_size=10))
    transfers = [make_transfer("a", 60), make_transfer("b", 250)]
    allocations = scheduler.allocate(transfers, 100.0)
    assert abs(sum(allocations.values()) - 100.0) < 1e-6
    assert all(0.0 <= value <= 100.0 for value in allocations.values())
    assert allocations["a"] != allocations["b"]


def test_flashport_service_deficit_favors_an_underserved_flow() -> None:
    scheduler = FlashportScheduler(
        SchedulerConfig(total_capacity=100.0, min_rate=1.0, chunk_size=1)
    )
    underserved = make_transfer("underserved", 100)
    served = make_transfer("served", 100)
    served.service_history = [50.0] * 5

    allocations = scheduler.allocate([underserved, served], 100.0)

    assert allocations["underserved"] > allocations["served"]


def test_independent_and_equal_share_are_explicitly_equivalent_in_this_model() -> None:
    transfers = [make_transfer("a", 100), make_transfer("b", 200)]
    independent = IndependentScheduler().allocate(transfers, 100.0)
    equal_share = EqualShareScheduler().allocate(transfers, 100.0)
    assert independent == equal_share


def test_every_policy_respects_capacity_and_ignores_completed_transfers() -> None:
    active = [make_transfer("a", 100), make_transfer("b", 200)]
    completed = make_transfer("done", 10)
    completed.mark_completed(1.0)
    policies = [
        IndependentScheduler(),
        EqualShareScheduler(),
        SRPTScheduler(),
        FlashportScheduler(),
    ]

    for policy in policies:
        allocations = policy.allocate([*active, completed], 25.0)
        assert set(allocations) == {"a", "b"}
        assert all(rate >= 0.0 for rate in allocations.values())
        assert sum(allocations.values()) <= 25.0
