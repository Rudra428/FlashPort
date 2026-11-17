"""Path and filename validation helpers preventing path traversal and unsafe filesystem access."""

from __future__ import annotations

import re
from pathlib import Path

_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]")


def sanitize_filename(raw_name: str, fallback: str = "upload.bin") -> str:
    """Validate and normalize a user-supplied filename, rejecting path traversal."""
    if not raw_name or "\x00" in raw_name:
        raise ValueError("Invalid empty or NUL-containing filename")
    if "/" in raw_name or "\\" in raw_name or ".." in raw_name:
        raise ValueError(f"Path traversal or directory separator not allowed in filename: {raw_name!r}")

    cleaned = _SAFE_NAME_RE.sub("_", raw_name).strip(".")
    if not cleaned:
        cleaned = fallback
    if len(cleaned) > 180:
        cleaned = cleaned[:180]
    return cleaned


def safe_resolve_under(base_dir: Path, relative_name: str) -> Path:
    """Resolve a filename strictly inside `base_dir`, raising ValueError on escape."""
    safe_name = sanitize_filename(relative_name)
    root = base_dir.resolve()
    candidate = (root / safe_name).resolve()
    if root not in candidate.parents:
        raise ValueError(f"Resolved path escapes target directory: {relative_name!r}")
    return candidate
