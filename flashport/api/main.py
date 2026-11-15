"""FlashPort API for live transfers, monitoring, simulation, and benchmark artifacts."""

from __future__ import annotations

import csv
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

from flashport.config import SchedulerConfig, SimulationConfig
from flashport.metrics.completion_time import (
    aggregate_goodput,
    long_flow_completion_count,
    mean_flow_completion_time,
    median_flow_completion_time,
    percentile_flow_completion_time,
    total_workload_completion_time,
)
from flashport.metrics.fairness import jain_fairness_index
from flashport.scheduler import (
    EqualShareScheduler,
    FlashportScheduler,
    IndependentScheduler,
    SRPTScheduler,
)
from flashport.simulation.engine import SimulationEngine
from flashport.simulation.scenarios import make_workload
from flashport.transfer.server import TransferServer

PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DASHBOARD = Path(__file__).with_name("static") / "index.html"
_RESULTS = PROJECT_ROOT / "results"
_GRAPH_NAMES = {
    "completion_times.png",
    "fairness.png",
    "aggregate_goodput.png",
    "scalability.png",
    "dynamic_bandwidth.png",
}


def create_app(
    shared_directory: str | Path | None = None,
    capacity_bps: float | None = None,
    scheduler_name: str | None = None,
    allocation_interval: float = 0.25,
) -> FastAPI:
    shared_root = Path(
        shared_directory
        or os.environ.get("FLASHPORT_SHARED_DIR", str(PROJECT_ROOT / "shared"))
    )
    configured_capacity = (
        capacity_bps
        if capacity_bps is not None
        else float(os.environ.get("FLASHPORT_CAPACITY_BPS", "20000000"))
    )
    transfer_server = TransferServer(
        shared_root,
        capacity_bps=configured_capacity,
        scheduler_name=scheduler_name or os.environ.get("FLASHPORT_SCHEDULER", "flashport"),
        allocation_interval=allocation_interval,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        await transfer_server.start()
        try:
            yield
        finally:
            await transfer_server.stop()

    app = FastAPI(title="FlashPort", version="1.0.0", lifespan=lifespan)
    app.state.transfer_server = transfer_server
    app.include_router(transfer_server.router, prefix="/api")

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/", include_in_schema=False)
    async def dashboard() -> FileResponse:
        return FileResponse(_DASHBOARD, media_type="text/html")

    @app.get("/api/simulation/comparison")
    async def simulation_comparison(
        capacity: float = Query(default=100_000.0, gt=0, le=10_000_000_000),
        seed: int = Query(default=42, ge=0),
    ) -> dict[str, object]:
        """Run the deterministic sample workload against all scheduling policies."""
        workload = make_workload(seed=seed)
        config = SimulationConfig(
            total_capacity=capacity,
            time_step=0.1,
            max_time=3600.0,
            seed=seed,
        )
        scheduler_config = SchedulerConfig(total_capacity=capacity)
        engine = SimulationEngine(config=config, scheduler_config=scheduler_config)
        policies = {
            "independent": IndependentScheduler(scheduler_config),
            "equal_share": EqualShareScheduler(scheduler_config),
            "srpt": SRPTScheduler(scheduler_config),
            "flashport": FlashportScheduler(scheduler_config),
        }

        comparison: dict[str, object] = {}
        for name, results in engine.compare_policies(workload, policies).items():
            stats = engine.stats_by_policy[name]
            fairness_rates = [
                result.goodput
                for result in results
                if result.completion_time is not None and result.size > 0
            ]
            comparison[name] = {
                "metrics": {
                    "mean_fct": mean_flow_completion_time(results),
                    "median_fct": median_flow_completion_time(results),
                    "p95_fct": percentile_flow_completion_time(results),
                    "workload_completion": total_workload_completion_time(results),
                    "makespan_s": total_workload_completion_time(results),
                    "aggregate_goodput": aggregate_goodput(results),
                    "jain_fairness": jain_fairness_index(fairness_rates),
                    "network_utilization": stats.network_utilization,
                    "scheduler_time_s": stats.scheduler_time_seconds,
                    "long_flow_count": long_flow_completion_count(results),
                },
                "transfers": [
                    {
                        "transfer_id": result.transfer_id,
                        "filename": result.filename,
                        "size": result.size,
                        "arrival_time": result.arrival_time,
                        "completion_time": result.completion_time,
                        "flow_completion_time": result.flow_completion_time,
                        "goodput": result.goodput,
                        "bytes_transferred": result.bytes_transferred,
                    }
                    for result in results
                ],
            }

        return {
            "measurement_type": "simulation",
            "capacity": capacity,
            "seed": seed,
            "workload_size": len(workload),
            "policies": comparison,
        }

    @app.get("/api/benchmarks")
    async def benchmark_results() -> dict[str, object]:
        summary_path = _RESULTS / "benchmark_summary.csv"
        if not summary_path.is_file():
            raise HTTPException(status_code=404, detail="Benchmark results are not available.")
        with summary_path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        return {"measurement_type": "simulation", "runs": rows}

    @app.get("/api/benchmarks/graphs/{name}", include_in_schema=False)
    async def benchmark_graph(name: str) -> FileResponse:
        if name not in _GRAPH_NAMES:
            raise HTTPException(status_code=404, detail="Graph not found.")
        path = _RESULTS / name
        if not path.is_file():
            raise HTTPException(status_code=404, detail="Graph not found.")
        return FileResponse(path, media_type="image/png")

    return app


app = create_app()
