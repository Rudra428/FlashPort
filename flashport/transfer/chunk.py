"""Validated byte-range metadata used by concurrent resumable downloads."""

from __future__ import annotations

from dataclasses import dataclass
import re

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True)
class Chunk:
    index: int
    offset: int
    length: int
    checksum: str | None = None

    def __post_init__(self) -> None:
        if self.index < 0 or self.offset < 0 or self.length <= 0:
            raise ValueError("chunk index/offset must be non-negative and length positive")
        if self.checksum is not None and not _SHA256_RE.fullmatch(self.checksum):
            raise ValueError("chunk checksum must be a lowercase SHA-256 hex digest")

    @property
    def end(self) -> int:
        """Return the exclusive ending byte offset."""
        return self.offset + self.length

    @property
    def range_header(self) -> str:
        return f"bytes={self.offset}-{self.end - 1}"

    @classmethod
    def partition(cls, size: int, chunk_size: int) -> list[Chunk]:
        if size < 0 or chunk_size <= 0:
            raise ValueError("size must be non-negative and chunk_size positive")
        return [
            cls(index, offset, min(chunk_size, size - offset))
            for index, offset in enumerate(range(0, size, chunk_size))
        ]
