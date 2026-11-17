"""Lightweight SQLite persistence for transfers, monitoring snapshots, and experiments."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from flowweave.models import ExperimentSummary, TransferSnapshot


class Storage:
    """Thread-safe SQLite repository for FlowWeave runtime state and experiment results."""

    def __init__(self, db_path: str | Path = "flowweave.db") -> None:
        self.db_path = Path(db_path)
        self._lock = threading.Lock()
        if str(db_path) != ":memory:":
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS transfers (
                    transfer_id TEXT PRIMARY KEY,
                    direction TEXT NOT NULL DEFAULT 'download',
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
                );

                CREATE TABLE IF NOT EXISTS monitor_samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL NOT NULL,
                    policy TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS experiment_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at REAL NOT NULL,
                    scenario TEXT NOT NULL,
                    policy TEXT NOT NULL,
                    seed INTEGER NOT NULL,
                    avg_fct_s REAL NOT NULL,
                    sum_fct_s REAL NOT NULL,
                    p95_fct_s REAL NOT NULL,
                    makespan_s REAL NOT NULL,
                    aggregate_goodput_bps REAL NOT NULL,
                    jains_fairness REAL NOT NULL,
                    redundant_bytes INTEGER NOT NULL,
                    scheduler_overhead_ms_avg REAL NOT NULL,
                    summary_json TEXT NOT NULL
                );
                """
            )
            # Migrate older flowweave.db schemas cleanly when new columns are introduced.
            cols = {
                row["name"]
                for row in self._conn.execute("PRAGMA table_info(transfers)").fetchall()
            }
            if "direction" not in cols:
                self._conn.execute(
                    "ALTER TABLE transfers ADD COLUMN direction TEXT NOT NULL DEFAULT 'download'"
                )
            self._conn.commit()

    def upsert_transfer(self, snapshot: TransferSnapshot) -> None:
        now = time.time()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO transfers (
                    transfer_id, direction, filename, url, total_bytes, remaining_bytes,
                    state, assigned_link, created_at, started_at, completed_at,
                    allocated_bps, avg_goodput_bps, redundant_bytes, retries,
                    dest_path, sha256, error, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(transfer_id) DO UPDATE SET
                    direction=excluded.direction,
                    total_bytes=excluded.total_bytes,
                    remaining_bytes=excluded.remaining_bytes,
                    state=excluded.state,
                    assigned_link=excluded.assigned_link,
                    started_at=excluded.started_at,
                    completed_at=excluded.completed_at,
                    allocated_bps=excluded.allocated_bps,
                    avg_goodput_bps=excluded.avg_goodput_bps,
                    redundant_bytes=excluded.redundant_bytes,
                    retries=excluded.retries,
                    dest_path=excluded.dest_path,
                    sha256=excluded.sha256,
                    error=excluded.error,
                    updated_at=excluded.updated_at
                """,
                (
                    snapshot.transfer_id,
                    snapshot.direction.value,
                    snapshot.filename,
                    snapshot.url,
                    snapshot.total_bytes,
                    snapshot.remaining_bytes,
                    snapshot.state.value,
                    snapshot.assigned_link,
                    snapshot.created_at,
                    snapshot.started_at,
                    snapshot.completed_at,
                    snapshot.allocated_bps,
                    snapshot.measured.avg_goodput_bps,
                    snapshot.measured.redundant_bytes,
                    snapshot.measured.retries,
                    snapshot.dest_path,
                    snapshot.sha256,
                    snapshot.error,
                    now,
                ),
            )
            self._conn.commit()

    def list_transfers(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM transfers ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def record_monitor_sample(self, timestamp: float, policy: str, payload: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO monitor_samples (timestamp, policy, payload_json) VALUES (?, ?, ?)",
                (timestamp, policy, json.dumps(payload)),
            )
            # Keep bounded history in SQLite for fast dashboard polling
            self._conn.execute(
                """
                DELETE FROM monitor_samples
                WHERE id NOT IN (
                    SELECT id FROM monitor_samples ORDER BY id DESC LIMIT 300
                )
                """
            )
            self._conn.commit()

    def recent_monitor_samples(self, limit: int = 60) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT timestamp, policy, payload_json FROM monitor_samples ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        result = [
            {
                "timestamp": r["timestamp"],
                "policy": r["policy"],
                "data": json.loads(r["payload_json"]),
            }
            for r in reversed(rows)
        ]
        return result

    def save_experiment(self, summary: ExperimentSummary) -> int:
        with self._lock:
            cur = self._conn.execute(
                """
                INSERT INTO experiment_runs (
                    created_at, scenario, policy, seed, avg_fct_s, sum_fct_s,
                    p95_fct_s, makespan_s, aggregate_goodput_bps, jains_fairness,
                    redundant_bytes, scheduler_overhead_ms_avg, summary_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    time.time(),
                    summary.scenario,
                    summary.policy,
                    summary.seed,
                    summary.avg_fct_s,
                    summary.sum_fct_s,
                    summary.p95_fct_s,
                    summary.makespan_s,
                    summary.aggregate_goodput_bps,
                    summary.jains_fairness,
                    summary.redundant_bytes,
                    summary.scheduler_overhead_ms_avg,
                    json.dumps(summary.to_dict()),
                ),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def list_experiments(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, created_at, summary_json FROM experiment_runs ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        out = []
        for r in rows:
            item = json.loads(r["summary_json"])
            item["run_id"] = r["id"]
            item["created_at"] = r["created_at"]
            out.append(item)
        return out

    def close(self) -> None:
        with self._lock:
            self._conn.close()
