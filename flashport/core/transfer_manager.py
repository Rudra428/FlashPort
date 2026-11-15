from __future__ import annotations

from collections.abc import Iterable

from .models import TransferState


class TransferManager:
    """In-memory transfer manager for the initial experiments and simulation loop."""

    def __init__(self) -> None:
        self._transfers: dict[str, TransferState] = {}

    def register(self, transfer: TransferState) -> TransferState:
        self._transfers[transfer.transfer_id] = transfer
        return transfer

    def get(self, transfer_id: str) -> TransferState | None:
        return self._transfers.get(transfer_id)

    def active(self) -> list[TransferState]:
        return [t for t in self._transfers.values() if t.state != "completed"]

    def all(self) -> list[TransferState]:
        return list(self._transfers.values())

    def completed(self) -> list[TransferState]:
        return [t for t in self._transfers.values() if t.state == "completed"]

    def __iter__(self) -> Iterable[TransferState]:
        return iter(self._transfers.values())

    def __len__(self) -> int:
        return len(self._transfers)
