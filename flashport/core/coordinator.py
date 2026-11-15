from __future__ import annotations

from .state import TransferRegistry


class TransferCoordinator:
    """Simple coordinator responsible for tracking transfers and their state."""

    def __init__(self) -> None:
        self.registrations = TransferRegistry()

    def register_transfer(self, transfer) -> None:
        self.registrations.add(transfer)

    def active_transfers(self):
        return self.registrations.active()

    def aggregate_goodput(self) -> float:
        return sum(transfer.goodput for transfer in self.registrations.all())
