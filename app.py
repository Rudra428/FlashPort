#!/usr/bin/env python3
"""Single-command launcher for FlowWeave (serves both the React frontend and FastAPI + raw TCP receiver).

Usage:
    python app.py
    python app.py --port 8080 --tcp-port 5000
    python app.py --open
"""

from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import webbrowser
from pathlib import Path

import uvicorn


def _detect_lan_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            return str(s.getsockname()[0])
        finally:
            s.close()
    except OSError:
        return "127.0.0.1"


def _ensure_frontend_built(root: Path) -> None:
    dist_index = root / "frontend" / "dist" / "index.html"
    if dist_index.is_file():
        return
    frontend_dir = root / "frontend"
    if not (frontend_dir / "package.json").is_file():
        return
    print("Frontend dist/ not found — building React dashboard once...")
    npm_cmd = "npm.cmd" if os.name == "nt" else "npm"
    if not (frontend_dir / "node_modules").is_dir():
        subprocess.run([npm_cmd, "install"], cwd=str(frontend_dir), check=True)
    subprocess.run([npm_cmd, "run", "build"], cwd=str(frontend_dir), check=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run FlowWeave (Frontend Dashboard + FastAPI Backend + Raw TCP Socket Receiver)"
    )
    parser.add_argument("--host", default="0.0.0.0", help="HTTP bind address (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8080, help="HTTP dashboard/API port (default: 8080)")
    parser.add_argument(
        "--tcp-port",
        type=int,
        default=9000,
        help="Raw TCP receiver socket port (default: 9000)",
    )
    parser.add_argument("--open", action="store_true", help="Open the dashboard in your default browser")
    args = parser.parse_args()

    root = Path(__file__).resolve().parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    _ensure_frontend_built(root)
    os.environ["FLOWWEAVE_TCP_PORT"] = str(args.tcp_port)

    lan_ip = _detect_lan_ip()
    downloads_dir = Path.home() / "Downloads"
    print("=" * 66)
    print("  FlowWeave — Bidirectional Cooperative Raw-TCP File Transfer")
    print("=" * 66)
    print(f"  Dashboard (Local):   http://127.0.0.1:{args.port}")
    print(f"  Dashboard (LAN):     http://{lan_ip}:{args.port}")
    print(f"  TCP Receiver:        0.0.0.0:{args.tcp_port}  (LAN: {lan_ip}:{args.tcp_port})")
    print(f"  Downloads Folder:    {downloads_dir}")
    print("-" * 66)
    print("  Run `python app.py` on BOTH laptops — switch between Sender & Receiver tabs.")
    print("=" * 66)

    if args.open:
        webbrowser.open(f"http://127.0.0.1:{args.port}")

    uvicorn.run("flowweave.api.app:app", host=args.host, port=args.port, reload=False)


if __name__ == "__main__":
    main()
