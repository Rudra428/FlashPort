"""API integration tests covering health, scheduler switching, experiments, and real demo transfers."""

from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient

from flowweave.api.app import create_app
from flowweave.config import FlowWeaveConfig, TransferConfig


def test_api_endpoints_end_to_end(tmp_path) -> None:
    cfg = FlowWeaveConfig(
        db_path=tmp_path / "api.db",
        control_interval_s=0.1,
        transfer=TransferConfig(
            download_dir=tmp_path / "downloads",
            upload_dir=tmp_path / "uploads",
            sample_dir=tmp_path / "samples",
            chunk_size_bytes=128 * 1024,
            total_link_capacity_bps=20 * 1024 * 1024,
            upload_link_capacity_bps=20 * 1024 * 1024,
        ),
    )
    app = create_app(cfg)

    with TestClient(app) as client:
        # 1. Health check & built dashboard check
        r = client.get("/api/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

        root = client.get("/")
        assert root.status_code == 200
        assert "FlowWeave" in root.text

        # 2. Scheduler inspection and live policy switch
        r = client.get("/api/scheduler")
        assert r.status_code == 200
        assert r.json()["policy"] == "flowweave"

        r = client.post(
            "/api/scheduler",
            json={"policy": "max_min", "min_fair_share_fraction": 0.2},
        )
        assert r.status_code == 200
        assert r.json()["policy"] == "max_min"

        r = client.post("/api/scheduler", json={"policy": "flowweave"})
        assert r.status_code == 200

        # 3. Scenarios & experiment execution
        r = client.get("/api/experiments/scenarios")
        assert r.status_code == 200
        scenarios = r.json()["scenarios"]
        assert any(s["name"] == "mixed_shared_bottleneck" for s in scenarios)

        r = client.post(
            "/api/experiments/run",
            json={"scenario": "mixed_shared_bottleneck", "seed": 42},
        )
        assert r.status_code == 200
        exp = r.json()
        assert set(exp["policies"].keys()) == {
            "independent",
            "max_min",
            "srpt",
            "flowweave",
        }
        assert exp["policies"]["flowweave"]["makespan_s"] <= exp["policies"]["independent"]["makespan_s"]
        assert exp["policies"]["flowweave"]["aggregate_goodput_bps"] >= exp["policies"]["independent"]["aggregate_goodput_bps"]

        r = client.get("/api/experiments/results")
        assert r.status_code == 200
        assert len(r.json()["results"]) >= 4

        # 4. Demo files list + launch mixed bidirectional demo workload (uploads + downloads)
        r = client.get("/api/demo/files")
        assert r.status_code == 200
        assert len(r.json()["files"]) >= 5

        r = client.post(
            "/api/demo/start",
            json={"mode": "mixed", "downlink_mbps": 50.0, "uplink_mbps": 50.0},
        )
        assert r.status_code == 201
        assert r.json()["launched"] == 7

        # 5. Browser raw upload endpoint -> scheduled through chunked upload engine
        r = client.post(
            "/api/transfers/upload-browser",
            content=b"hello flowweave" * 1024,
            headers={"X-Filename": "browser_demo.txt"},
        )
        assert r.status_code == 201
        assert r.json()["transfer"]["direction"] == "upload"

        # Inspect monitor telemetry
        r = client.get("/api/monitor")
        assert r.status_code == 200
        mon = r.json()
        assert mon["policy"] == "flowweave"
        assert mon["is_simulated"] is False
        assert len(mon["transfers"]) >= 8
        assert len(mon["links"]) >= 2

        # 6. Peer-to-peer host info, remote TCP browse, staging, and SRPT vs FlowWeave run
        info = client.get("/api/host-info").json()
        tcp_port = info["tcp_port"]
        assert tcp_port > 0

        s1 = client.post(
            "/api/transfers/stage-browser",
            content=b"X" * (128 * 1024),
            headers={"X-Filename": "peer_small.bin"},
        )
        assert s1.status_code == 201
        s2 = client.post(
            "/api/transfers/stage-browser",
            content=b"Y" * (256 * 1024),
            headers={"X-Filename": "peer_large.bin"},
        )
        assert s2.status_code == 201

        send = client.post(
            "/api/transfers/send-staged",
            json={"host": "127.0.0.1", "port": tcp_port, "policy": "flowweave", "link_cap_mbps": 50.0},
        )
        assert send.status_code == 201
        assert send.json()["launched"] == 2

        remote = client.get(f"/api/tcp/remote-files?host=127.0.0.1&port={tcp_port}")
        assert remote.status_code == 200
        assert remote.json()["rtt_ms"] >= 0.0

        # 7. Integrated receiver status & port rebind
        recv = client.get("/api/receiver")
        assert recv.status_code == 200
        assert recv.json()["listening"] is True
        assert recv.json()["tcp_port"] == tcp_port
