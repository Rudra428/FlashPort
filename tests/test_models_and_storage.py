"""Tests for core models, metrics, and SQLite storage."""

from __future__ import annotations

import pytest

from flowweave.metrics import jains_fairness_index, summarize_experiment
from flowweave.models import (
    EstimatedMetrics,
    MeasuredMetrics,
    TransferOutcome,
    TransferSnapshot,
    TransferState,
)
from flowweave.storage import Storage


def test_jains_fairness_bounds() -> None:
    assert jains_fairness_index([10.0, 10.0, 10.0]) == pytest.approx(1.0)
    # One transfer gets everything among 4 -> 1/4 = 0.25
    assert jains_fairness_index([100.0, 0.0, 0.0, 0.0]) == pytest.approx(0.25)
    assert jains_fairness_index([]) == 1.0


def test_summarize_experiment() -> None:
    outcomes = [
        TransferOutcome("t1", "a.bin", 1_000_000, 0.0, 0.0, 2.0, 2.0, 500_000.0, redundant_bytes=0),
        TransferOutcome("t2", "b.bin", 4_000_000, 0.0, 0.0, 8.0, 8.0, 500_000.0, redundant_bytes=50_000),
    ]
    summary = summarize_experiment(
        scenario="demo",
        policy="flowweave",
        seed=42,
        outcomes=outcomes,
        scheduler_overhead_ms=[0.2, 0.4],
    )
    assert summary.avg_fct_s == pytest.approx(5.0)
    assert summary.sum_fct_s == pytest.approx(10.0)
    assert summary.makespan_s == pytest.approx(8.0)
    assert summary.aggregate_goodput_bps == pytest.approx(5_000_000 / 8.0)
    assert summary.jains_fairness == pytest.approx(1.0)
    assert summary.redundant_bytes == 50_000
    assert summary.scheduler_overhead_ms_avg == pytest.approx(0.3)


def test_sqlite_storage_roundtrip(tmp_path) -> None:
    db = Storage(tmp_path / "test.db")
    snap = TransferSnapshot(
        transfer_id="tx-1",
        filename="sample.bin",
        url="http://localhost/sample.bin",
        total_bytes=1024,
        remaining_bytes=512,
        state=TransferState.ACTIVE,
        candidate_links=["link-a"],
        assigned_link="link-a",
        created_at=100.0,
        started_at=100.5,
        allocated_bps=256.0,
        measured=MeasuredMetrics(bytes_acked=512, avg_goodput_bps=128.0),
        estimated=EstimatedMetrics(ewma_goodput_bps=128.0, predicted_remaining_time_s=4.0),
    )
    db.upsert_transfer(snap)
    rows = db.list_transfers()
    assert len(rows) == 1
    assert rows[0]["transfer_id"] == "tx-1"
    assert rows[0]["remaining_bytes"] == 512

    snap.remaining_bytes = 0
    snap.state = TransferState.COMPLETED
    db.upsert_transfer(snap)
    rows = db.list_transfers()
    assert rows[0]["state"] == "completed"
    assert rows[0]["remaining_bytes"] == 0

    db.record_monitor_sample(101.0, "flowweave", {"active": 1})
    samples = db.recent_monitor_samples()
    assert len(samples) == 1
    assert samples[0]["policy"] == "flowweave"

    summary = summarize_experiment(
        scenario="s1",
        policy="flowweave",
        seed=7,
        outcomes=[TransferOutcome("tx-1", "sample.bin", 1024, 0.0, 0.0, 2.0, 2.0, 512.0)],
        scheduler_overhead_ms=[0.1],
    )
    run_id = db.save_experiment(summary)
    assert run_id >= 1
    saved = db.list_experiments()
    assert len(saved) == 1
    assert saved[0]["scenario"] == "s1"
    db.close()


def test_sqlite_migrates_legacy_transfers_table(tmp_path) -> None:
    import sqlite3

    db_file = tmp_path / "legacy.db"
    conn = sqlite3.connect(str(db_file))
    conn.execute(
        """
        CREATE TABLE transfers (
            transfer_id TEXT PRIMARY KEY,
            filename TEXT NOT NULL,
            url TEXT NOT NULL,
            total_bytes INTEGER NOT NULL,
            remaining_bytes INTEGER NOT NULL,
            state TEXT NOT NULL,
            assigned_link TEXT NOT NULL,
            created_at REAL NOT NULL,
            started_at REAL,
            completed_at REAL,
            allocated_bps REAL NOT NULL,
            avg_goodput_bps REAL NOT NULL,
            redundant_bytes INTEGER NOT NULL DEFAULT 0,
            retries INTEGER NOT NULL DEFAULT 0,
            dest_path TEXT,
            sha256 TEXT,
            error TEXT,
            updated_at REAL NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()

    # Opening with Storage must migrate seamlessly and allow upserts with direction.
    db = Storage(db_file)
    snap = TransferSnapshot(
        transfer_id="mig-1",
        filename="a.bin",
        url="http://localhost/a.bin",
        total_bytes=100,
        remaining_bytes=0,
        state=TransferState.COMPLETED,
        candidate_links=["downlink"],
        assigned_link="downlink",
        created_at=1.0,
    )
    db.upsert_transfer(snap)
    rows = db.list_transfers()
    assert len(rows) == 1
    assert rows[0]["direction"] == "download"
    db.close()
