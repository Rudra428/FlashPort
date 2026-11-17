"""Sample file provisioning for FlowWeave's offline bidirectional demonstration mode."""

from __future__ import annotations

from pathlib import Path

SAMPLE_SPECS: list[tuple[str, int, int]] = [
    ("patch_512kb.bin", 512 * 1024, 11),
    ("dataset_1mb.bin", 1024 * 1024, 22),
    ("archive_2mb.bin", 2 * 1024 * 1024, 33),
    ("checkpoint_4mb.bin", 4 * 1024 * 1024, 44),
    ("video_6mb.bin", 6 * 1024 * 1024, 55),
]


def ensure_sample_files(sample_dir: str | Path = "sample_files") -> list[Path]:
    """Create deterministic sample binary files on disk if they don't already exist."""
    root = Path(sample_dir)
    root.mkdir(parents=True, exist_ok=True)
    created: list[Path] = []

    for name, size, seed_byte in SAMPLE_SPECS:
        target = root / name
        if not target.exists() or target.stat().st_size != size:
            pattern = bytes((seed_byte + i * 31) % 256 for i in range(4096))
            repeats, rem = divmod(size, len(pattern))
            target.write_bytes(pattern * repeats + pattern[:rem])
        created.append(target)

    readme_sample = root / "demo_notes.txt"
    if not readme_sample.exists():
        readme_sample.write_text(
            "FlowWeave demonstration sample file.\n"
            "Used for testing real chunked uploads, HTTP Range downloads, and SHA-256 verification.\n",
            encoding="utf-8",
        )
    created.append(readme_sample)
    return created
