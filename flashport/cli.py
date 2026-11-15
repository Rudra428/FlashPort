from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

import uvicorn

from flashport.transfer.client import TransferClient


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="flashport", description="FlashPort transfer server and client")
    commands = parser.add_subparsers(dest="command", required=True)

    serve = commands.add_parser("serve", help="start the file-transfer API and dashboard")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument("--shared-dir", type=Path, default=Path("shared"))
    serve.add_argument("--capacity-bps", type=float, default=20_000_000.0)
    serve.add_argument(
        "--scheduler",
        choices=("independent", "equal_share", "srpt", "flashport"),
        default="flashport",
    )

    for name in ("list", "download"):
        command = commands.add_parser(name, help=f"{name} server files")
        command.add_argument("--server", default="http://127.0.0.1:8000")
    download = commands.choices["download"]
    download.add_argument("filenames", nargs="+")
    download.add_argument("--destination", type=Path, default=Path("downloads"))
    download.add_argument("--chunk-size", type=int, default=1024 * 1024)
    download.add_argument("--connections", type=int, default=4)
    download.add_argument("--retries", type=int, default=3)
    download.add_argument("--overwrite", action="store_true")
    download.add_argument("--no-resume", action="store_true")
    return parser


async def _list_files(base_url: str) -> None:
    async with TransferClient(base_url) as client:
        print(json.dumps(await client.list_files(), indent=2))


async def _download(args: argparse.Namespace) -> None:
    async with TransferClient(
        args.server,
        chunk_size=args.chunk_size,
        max_connections=args.connections,
        retries=args.retries,
    ) as client:
        paths = await client.download_many(
            args.filenames,
            args.destination,
            overwrite=args.overwrite,
            resume=not args.no_resume,
            progress_callback=lambda progress: print(
                f"\r{progress['filename']}: {progress['percent']:.1f}% "
                f"({progress['downloaded_bytes']}/{progress['total_bytes']} bytes)",
                end="",
                flush=True,
            ),
        )
        print("\nVerified downloads:")
        for path in paths:
            print(path)


def main(argv: list[str] | None = None) -> None:
    args = _parser().parse_args(argv)
    if args.command == "serve":
        from flashport.api.main import create_app

        application = create_app(
            shared_directory=args.shared_dir,
            capacity_bps=args.capacity_bps,
            scheduler_name=args.scheduler,
        )
        uvicorn.run(application, host=args.host, port=args.port)
    elif args.command == "list":
        asyncio.run(_list_files(args.server))
    elif args.command == "download":
        asyncio.run(_download(args))


if __name__ == "__main__":
    main()
