from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from .models import TransferState


class TransferRegistry:
    """A lightweight in-memory registry for active logical transfers."""

    def __init__(self) -> None:
        self._transfers: dict[str, TransferState] = {}

    def add(self, transfer: TransferState) -> None:
        self._transfers[transfer.transfer_id] = transfer

    def get(self, transfer_id: str) -> TransferState | None:
        return self._transfers.get(transfer_id)

    def remove(self, transfer_id: str) -> TransferState | None:
        return self._transfers.pop(transfer_id, None)

    def active(self) -> list[TransferState]:
        return [transfer for transfer in self._transfers.values() if transfer.state != "completed"]

    def all(self) -> list[TransferState]:
        return list(self._transfers.values())

    def update(self, transfer_id: str, **updates: Any) -> TransferState | None:
        transfer = self._transfers.get(transfer_id)
        if transfer is None:
            return None
        for key, value in updates.items():
            if hasattr(transfer, key):
                setattr(transfer, key, value)
        return transfer

    def __iter__(self) -> Iterable[TransferState]:
        return iter(self._transfers.values())

    def __len__(self) -> int:
        return len(self._transfers)
