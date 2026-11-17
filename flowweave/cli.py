"""Command-line interface for running the FlowWeave server, benchmarks, and Linux tc harness."""

from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
import time
from pathlib import Path

from flowweave.config import FlowWeaveConfig, SchedulerConfig, TransferConfig
from flowweave.coordinator import FlowWeaveCoordinator
from flowweave.experiments.netem import generate_linux_experiment_script
from flowweave.experiments.runner import ALL_POLICIES, run_full_benchmark
from flowweave.metrics import summarize_experiment
from flowweave.models import PolicyName, TransferOutcome, TransferState
from flowweave.transfer.test_server import LocalFileServer


async def _run_live_http_benchmark(rate_mbit: float, out_path: Path) -> None:
    """Benchmark all 4 policies using real HTTP Range downloads against a local server."""
    cap_bps = (rate_mbit * 1_000_000.0) / 8.0
    server = LocalFileServer()
    workload = [
        ("small_256k.bin", 256 * 1024, 1),
        ("small_512k.bin", 512 * 1024, 2),
        ("med_1m.bin", 1024 * 1024, 3),
        ("large_2m.bin", 2 * 1024 * 1024, 4),
        ("xlarge_4m.bin", 4 * 1024 * 1024, 5),
    ]
    for name, sz, seed_b in workload:
        server.add_deterministic_file(name, sz, seed_byte=seed_b)

    await server.start()
    results = {}
    try:
        for policy in ALL_POLICIES:
            with tempfile.TemporaryDirectory(prefix=f"fw_{policy.value}_") as tmp:
                tmp_dir = Path(tmp)
                cfg = FlowWeaveConfig(
                    db_path=tmp_dir / "bench.db",
                    control_interval_s=0.1,
                    scheduler=SchedulerConfig(policy=policy),
                    transfer=TransferConfig(
                        download_dir=tmp_dir / "dl",
                        chunk_size_bytes=128 * 1024,
                        max_chunks_per_transfer=3,
                        total_link_capacity_bps=cap_bps,
                    ),
                )
                coord = FlowWeaveCoordinator(cfg)
                await coord.start()
                t0 = time.monotonic()
                submitted = []
                for name, sz, _ in workload:
                    sf = server.files[name]
                    snap = await coord.transfer_manager.submit(
                        server.url_for(name),
                        filename=name,
                        expected_size_bytes=sz,
                        expected_sha256=sf.sha256,
                    )
                    submitted.append(snap.transfer_id)
                await coord.transfer_manager.wait_all(timeout=120.0)

                outcomes = []
                for tid in submitted:
                    s = coord.transfer_manager.get_snapshot(tid)
                    assert s is not None
                    start_t = (s.started_at or t0) - t0
                    end_t = (s.completed_at or time.monotonic()) - t0
                    fct = max(0.001, end_t - max(0.0, s.created_at - t0))
                    outcomes.append(
                        TransferOutcome(
                            transfer_id=s.transfer_id,
                            filename=s.filename,
                            size_bytes=s.total_bytes,
                            arrival_time=max(0.0, s.created_at - t0),
                            start_time=max(0.0, start_t),
                            completion_time=end_t,
                            fct_s=fct,
                            avg_goodput_bps=s.total_bytes / fct,
                            retries=s.measured.retries,
                            state=s.state,
                        )
                    )
                summary = summarize_experiment(
                    scenario=f"live_http_{rate_mbit:.0f}mbit",
                    policy=policy.value,
                    seed=0,
                    outcomes=outcomes,
                    scheduler_overhead_ms=[0.0],
                )
                results[policy.value] = summary.to_dict()
                await coord.stop()
    finally:
        await server.stop()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(results, indent=2), encoding="utf-8")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="flowweave", description="FlowWeave CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    p_serve = sub.add_parser("serve", help="Run the FlowWeave FastAPI server")
    p_serve.add_argument("--host", default="127.0.0.1")
    p_serve.add_argument("--port", type=int, default=8000)

    p_bench = sub.add_parser("benchmark", help="Run reproducible simulation benchmark suite")
    p_bench.add_argument("--out", default="results")
    p_bench.add_argument("--seed", type=int, default=42)
    p_bench.add_argument("--no-charts", action="store_true")

    p_live = sub.add_parser("live-benchmark", help="Run real HTTP Range benchmark across all 4 policies")
    p_live.add_argument("--rate-mbit", type=float, default=32.0)
    p_live.add_argument("--out", default="results/live_comparison.json")

    p_netem = sub.add_parser("netem-script", help="Generate Linux tc + iperf3 experiment script")
    p_netem.add_argument("--out", default="scripts/run_tc_experiment.sh")

    args = parser.parse_args(argv)

    if args.command == "serve":
        import uvicorn

        uvicorn.run("flowweave.api.app:app", host=args.host, port=args.port, reload=False)
    elif args.command == "benchmark":
        bundles = run_full_benchmark(
            output_dir=args.out,
            seed=args.seed,
            generate_charts=not args.no_charts,
        )
        print(f"Completed {len(bundles)} scenarios -> {Path(args.out).resolve()}")
    elif args.command == "live-benchmark":
        asyncio.run(_run_live_http_benchmark(args.rate_mbit, Path(args.out)))
        print(f"Live HTTP benchmark saved to {args.out}")
    elif args.command == "netem-script":
        script_path = generate_linux_experiment_script(args.out)
        print(f"Generated Linux tc/iperf3 script at {script_path}")


if __name__ == "__main__":
    main()
