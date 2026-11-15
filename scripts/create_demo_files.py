from __future__ import annotations

import argparse
from pathlib import Path


def create_demo_files(directory: Path, force: bool = False) -> list[Path]:
    directory.mkdir(parents=True, exist_ok=True)
    pattern = bytes((index * 37 + 11) % 256 for index in range(64 * 1024))
    outputs = []
    for filename, size in (
        ("demo-1mb.bin", 1 * 1024 * 1024),
        ("demo-8mb.bin", 8 * 1024 * 1024),
        ("demo-empty.bin", 0),
    ):
        path = directory / filename
        if path.exists() and not force:
            outputs.append(path)
            continue
        remaining = size
        with path.open("wb") as handle:
            while remaining:
                block = pattern[: min(len(pattern), remaining)]
                handle.write(block)
                remaining -= len(block)
        outputs.append(path)
    return outputs


def main() -> None:
    parser = argparse.ArgumentParser(description="Create deterministic FlashPort download samples.")
    parser.add_argument("--directory", type=Path, default=Path("shared"))
    parser.add_argument("--force", action="store_true", help="replace existing demo files")
    args = parser.parse_args()
    for path in create_demo_files(args.directory, force=args.force):
        print(f"{path} ({path.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
