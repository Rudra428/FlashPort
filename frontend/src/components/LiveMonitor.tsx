import React, { useState } from "react";
import { Bar, Line } from "react-chartjs-2";
import {
  startDemoBatch,
  submitDownload,
  submitSampleUpload,
  updateScheduler,
  uploadBrowserFile,
} from "../api";
import type { DemoFileInfo, MonitorStatus, PolicyName } from "../types";
import { fmtMB, fmtRate, TransfersTable } from "./TransfersTable";

const POLICIES: Array<{ id: PolicyName; label: string; desc: string }> = [
  { id: "independent", label: "Independent", desc: "Uncoordinated greedy transfers ignoring peer contention." },
  { id: "max_min", label: "Max-Min Fair", desc: "Progressive water-filling giving equal fair shares per bottleneck." },
  { id: "srpt", label: "SRPT", desc: "Strict shortest predicted remaining completion time ordering." },
  {
    id: "flowweave",
    label: "FlowWeave (Cooperative)",
    desc: "Urgency-weighted cooperative scheduling with fairness floor, aging, and congestion backoff.",
  },
];

interface CommonProps {
  status: MonitorStatus | null;
  demoFiles: DemoFileInfo[];
  onRefresh: () => void;
}

export const OverviewPanel: React.FC<CommonProps> = ({ status, onRefresh }) => {
  const [dlMbps, setDlMbps] = useState(3.0);
  const [ulMbps, setUlMbps] = useState(2.0);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const transfers = status?.transfers ?? [];
  const activeCount = transfers.filter((t) => t.state === "active").length;
  const completedCount = transfers.filter((t) => t.state === "completed").length;
  const links = status?.links ?? [];
  const maxUtil = links.length > 0 ? Math.max(...links.map((l) => l.utilization)) : 0;

  async function launchDemo(mode: "downloads" | "uploads" | "mixed") {
    setBusy(true);
    setErr(null);
    try {
      await startDemoBatch(mode, dlMbps, ulMbps);
      onRefresh();
    } catch (e) {
      setErr(String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div>
      {err && <div className="error-banner">{err}</div>}
      <div className="grid-4">
        <div className="card" style={{ marginBottom: 0 }}>
          <div className="kpi-label">Active / Completed</div>
          <div className="kpi-value">
            {activeCount} / {completedCount}
          </div>
          <div className="kpi-sub">Total tracked: {transfers.length}</div>
        </div>
        <div className="card" style={{ marginBottom: 0 }}>
          <div className="kpi-label">Aggregate Goodput (Measured)</div>
          <div className="kpi-value" style={{ color: "#10b981" }}>
            {fmtRate(status?.aggregate_goodput_bps ?? 0)}
          </div>
          <div className="kpi-sub">
            ↓ {fmtRate(status?.download_throughput_bps ?? 0)} · ↑{" "}
            {fmtRate(status?.upload_throughput_bps ?? 0)}
          </div>
        </div>
        <div className="card" style={{ marginBottom: 0 }}>
          <div className="kpi-label">Avg Completion Time</div>
          <div className="kpi-value">
            {status && status.average_fct_s > 0 ? `${status.average_fct_s.toFixed(2)}s` : "—"}
          </div>
          <div className="kpi-sub">Peak Link Util: {(maxUtil * 100).toFixed(0)}%</div>
        </div>
        <div className="card" style={{ marginBottom: 0 }}>
          <div className="kpi-label">Active Policy</div>
          <div className="kpi-value" style={{ color: "#3b82f6" }}>
            {status?.policy ?? "—"}
          </div>
          <div className="kpi-sub">
            Sched overhead: {(status?.last_compute_time_ms ?? 0).toFixed(3)} ms
          </div>
        </div>
      </div>

      <div className="card">
        <h2>
          <span>Demonstration Mode (Offline Local HTTP Range + Chunked Upload Server)</span>
          <span className="badge badge-healthy">Real File Transfers</span>
        </h2>
        <div className="controls-row">
          <label style={{ fontSize: "0.83rem", color: "#94a3b8" }}>
            Downlink Cap (MB/s):{" "}
            <input
              type="number"
              min={0.5}
              max={100}
              step={0.5}
              value={dlMbps}
              style={{ width: 72 }}
              onChange={(e) => setDlMbps(Number(e.target.value))}
            />
          </label>
          <label style={{ fontSize: "0.83rem", color: "#94a3b8" }}>
            Uplink Cap (MB/s):{" "}
            <input
              type="number"
              min={0.5}
              max={100}
              step={0.5}
              value={ulMbps}
              style={{ width: 72 }}
              onChange={(e) => setUlMbps(Number(e.target.value))}
            />
          </label>
          <button className="btn" disabled={busy} onClick={() => launchDemo("mixed")}>
            Run Mixed Upload + Download Demo
          </button>
          <button className="btn-secondary" disabled={busy} onClick={() => launchDemo("downloads")}>
            Concurrent Downloads (6 files)
          </button>
          <button className="btn-secondary" disabled={busy} onClick={() => launchDemo("uploads")}>
            Concurrent Uploads (4 files)
          </button>
        </div>
      </div>

      <div className="card">
        <h2>Recent Bidirectional Activity</h2>
        <TransfersTable
          transfers={transfers.slice(0, 10)}
          onRefresh={onRefresh}
          emptyText="Click 'Run Mixed Upload + Download Demo' above to watch live bidirectional scheduling."
        />
      </div>
    </div>
  );
};

export const UploadsPanel: React.FC<CommonProps> = ({ status, demoFiles, onRefresh }) => {
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const uploads = (status?.transfers ?? []).filter((t) => t.direction === "upload");

  async function handleFileSelect(e: React.ChangeEvent<HTMLInputElement>) {
    const files = e.target.files;
    if (!files || files.length === 0) return;
    setBusy(true);
    setErr(null);
    try {
      for (const f of Array.from(files)) {
        await uploadBrowserFile(f);
      }
      onRefresh();
    } catch (ex) {
      setErr(String(ex));
    } finally {
      setBusy(false);
      e.target.value = "";
    }
  }

  return (
    <div>
      {err && <div className="error-banner">{err}</div>}
      <div className="card">
        <h2>Upload Local Files to FlowWeave Server (Chunked, Parallel, SHA-256 Verified)</h2>
        <div className="controls-row" style={{ marginBottom: 12 }}>
          <input type="file" multiple disabled={busy} onChange={handleFileSelect} />
          <span style={{ fontSize: "0.82rem", color: "#94a3b8" }}>
            Selected files are split into chunks, paced by the active scheduler, reassembled, and SHA-256 verified.
          </span>
        </div>
        <div className="controls-row">
          <span style={{ fontSize: "0.82rem", color: "#94a3b8" }}>Quick sample uploads:</span>
          {demoFiles
            .filter((f) => f.can_upload_sample)
            .map((f) => (
              <button
                key={f.filename}
                className="btn-secondary"
                disabled={busy}
                onClick={async () => {
                  setBusy(true);
                  setErr(null);
                  try {
                    await submitSampleUpload(f.filename);
                    onRefresh();
                  } catch (ex) {
                    setErr(String(ex));
                  } finally {
                    setBusy(false);
                  }
                }}
              >
                ↑ {f.filename} ({fmtMB(f.size_bytes)})
              </button>
            ))}
        </div>
      </div>

      <div className="card">
        <h2>Active & Completed Uploads ({uploads.length})</h2>
        <TransfersTable
          transfers={uploads}
          onRefresh={onRefresh}
          emptyText="No uploads yet. Select local files above or click a sample file."
        />
      </div>
    </div>
  );
};

export const DownloadsPanel: React.FC<CommonProps> = ({ status, demoFiles, onRefresh }) => {
  const [url, setUrl] = useState("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const downloads = (status?.transfers ?? []).filter((t) => t.direction === "download");

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!url.trim()) return;
    setBusy(true);
    setErr(null);
    try {
      await submitDownload(url.trim());
      setUrl("");
      onRefresh();
    } catch (ex) {
      setErr(String(ex));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div>
      {err && <div className="error-banner">{err}</div>}
      <div className="card">
        <h2>Start HTTP Download (Range Chunking + Automatic Single-Stream Fallback)</h2>
        <form onSubmit={handleSubmit} className="controls-row" style={{ marginBottom: 12 }}>
          <input
            type="url"
            placeholder="http://... file URL"
            value={url}
            onChange={(e) => setUrl(e.target.value)}
            style={{ width: 380 }}
          />
          <button className="btn" type="submit" disabled={busy}>
            Start Download
          </button>
        </form>
        <div className="controls-row">
          <span style={{ fontSize: "0.82rem", color: "#94a3b8" }}>Local demo server files:</span>
          {demoFiles.map((f) => (
            <button
              key={f.filename}
              className="btn-secondary"
              disabled={busy}
              onClick={async () => {
                setBusy(true);
                setErr(null);
                try {
                  await submitDownload(f.download_url, f.filename);
                  onRefresh();
                } catch (ex) {
                  setErr(String(ex));
                } finally {
                  setBusy(false);
                }
              }}
            >
              ↓ {f.filename} ({fmtMB(f.size_bytes)}
              {f.supports_ranges ? "" : ", no-Range"})
            </button>
          ))}
        </div>
      </div>

      <div className="card">
        <h2>Active & Completed Downloads ({downloads.length})</h2>
        <TransfersTable
          transfers={downloads}
          onRefresh={onRefresh}
          emptyText="No downloads yet. Enter a URL above or click a local demo server file."
        />
      </div>
    </div>
  );
};

export const NetworkMonitorPanel: React.FC<{ status: MonitorStatus | null }> = ({ status }) => {
  const history = status?.history ?? [];
  const transfers = status?.transfers ?? [];
  const links = status?.links ?? [];

  const throughputData = {
    labels: history.map((_, idx) => `${idx + 1}`),
    datasets: [
      {
        label: "Download Throughput (MB/s)",
        data: history.map((h) =>
          Number(((h.data.download_throughput_bps ?? h.data.aggregate_goodput_bps) / (1024 * 1024)).toFixed(2))
        ),
        borderColor: "#3b82f6",
        backgroundColor: "rgba(59, 130, 246, 0.14)",
        fill: true,
        tension: 0.25,
      },
      {
        label: "Upload Throughput (MB/s)",
        data: history.map((h) =>
          Number(((h.data.upload_throughput_bps ?? 0) / (1024 * 1024)).toFixed(2))
        ),
        borderColor: "#a855f7",
        backgroundColor: "rgba(168, 85, 247, 0.14)",
        fill: true,
        tension: 0.25,
      },
    ],
  };

  const recent = transfers.slice(0, 8);
  const allocData = {
    labels: recent.map((t) => `${t.direction === "upload" ? "↑" : "↓"} ${t.filename}`),
    datasets: [
      {
        label: "Allocated (MB/s)",
        data: recent.map((t) => Number((t.allocated_bps / (1024 * 1024)).toFixed(2))),
        backgroundColor: "#3b82f6",
      },
      {
        label: "Measured Rolling (MB/s)",
        data: recent.map((t) =>
          Number(
            ((t.measured.rolling_goodput_bps || t.measured.instant_goodput_bps) / (1024 * 1024)).toFixed(2)
          )
        ),
        backgroundColor: "#10b981",
      },
      {
        label: "Estimated EWMA (MB/s)",
        data: recent.map((t) => Number((t.estimated.ewma_goodput_bps / (1024 * 1024)).toFixed(2))),
        backgroundColor: "#f59e0b",
      },
    ],
  };

  return (
    <div>
      <div className="card">
        <h2>
          <span>Link Utilization & Congestion Indicators</span>
          <span className="badge badge-healthy">Live Measured Telemetry</span>
        </h2>
        <table>
          <thead>
            <tr>
              <th>Link</th>
              <th>Direction</th>
              <th>Capacity</th>
              <th>Measured Throughput</th>
              <th>Utilization</th>
              <th>Active Flows</th>
              <th>RTT / Loss</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {links.map((l) => (
              <tr key={l.link_id}>
                <td>
                  <strong>{l.link_id}</strong>
                </td>
                <td>{l.direction}</td>
                <td>{fmtRate(l.capacity_bps)}</td>
                <td>{fmtRate(l.measured_throughput_bps)}</td>
                <td>{(l.utilization * 100).toFixed(1)}%</td>
                <td>{l.active_transfers}</td>
                <td>
                  {l.latency_ms.toFixed(1)} ms / {(l.loss_rate * 100).toFixed(1)}%
                </td>
                <td>
                  <span className={`badge ${l.is_congested ? "badge-congested" : "badge-healthy"}`}>
                    {l.is_congested ? "Congested" : "Healthy"}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="grid-2">
        <div className="card" style={{ marginBottom: 0 }}>
          <h2>Upload & Download Throughput Over Time</h2>
          <div className="chart-box">
            <Line
              data={throughputData}
              options={{
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { labels: { color: "#cbd5e1" } } },
                scales: {
                  x: { ticks: { color: "#94a3b8" }, grid: { color: "#1e293b" } },
                  y: { beginAtZero: true, ticks: { color: "#94a3b8" }, grid: { color: "#1e293b" } },
                },
              }}
            />
          </div>
        </div>
        <div className="card" style={{ marginBottom: 0 }}>
          <h2>Per-Transfer Allocation vs Measured vs Estimated</h2>
          <div className="chart-box">
            <Bar
              data={allocData}
              options={{
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { labels: { color: "#cbd5e1" } } },
                scales: {
                  x: { ticks: { color: "#94a3b8" }, grid: { color: "#1e293b" } },
                  y: { beginAtZero: true, ticks: { color: "#94a3b8" }, grid: { color: "#1e293b" } },
                },
              }}
            />
          </div>
        </div>
      </div>

      <div className="card">
        <h2>Detected Stragglers</h2>
        {!status || status.straggler_alerts.length === 0 ? (
          <p style={{ color: "#94a3b8", margin: 0 }}>
            No slow/straggler chunks detected in current live transfers.
          </p>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Transfer</th>
                <th>Chunk</th>
                <th>Link</th>
                <th>Plan</th>
                <th>Saving vs Cost</th>
                <th>Reason</th>
              </tr>
            </thead>
            <tbody>
              {status.straggler_alerts.map((s, i) => (
                <tr key={`${s.chunk_id}-${i}`}>
                  <td>{s.transfer_id}</td>
                  <td>{s.chunk_id}</td>
                  <td>{s.original_link}</td>
                  <td>{s.should_replicate ? `Replicate → ${s.replica_link}` : "Hold"}</td>
                  <td>
                    {s.expected_saving_s.toFixed(2)}s vs {s.expected_cost_s.toFixed(2)}s
                  </td>
                  <td>{s.reason}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
};

export const SchedulerPanel: React.FC<{ status: MonitorStatus | null; onRefresh: () => void }> = ({
  status,
  onRefresh,
}) => {
  const [fairFloor, setFairFloor] = useState(status?.scheduler_config.min_fair_share_fraction ?? 0.15);
  const [dlMbps, setDlMbps] = useState(
    status ? Number((status.downlink_capacity_bps / (1024 * 1024)).toFixed(1)) : 10
  );
  const [ulMbps, setUlMbps] = useState(
    status ? Number((status.uplink_capacity_bps / (1024 * 1024)).toFixed(1)) : 6
  );
  const [err, setErr] = useState<string | null>(null);

  async function applyPolicy(policy: PolicyName) {
    setErr(null);
    try {
      await updateScheduler({
        policy,
        min_fair_share_fraction: fairFloor,
        total_link_capacity_bps: dlMbps * 1024 * 1024,
        upload_link_capacity_bps: ulMbps * 1024 * 1024,
      });
      onRefresh();
    } catch (e) {
      setErr(String(e));
    }
  }

  return (
    <div>
      {err && <div className="error-banner">{err}</div>}
      <div className="card">
        <h2>Active Scheduling Policy & Capacity Controls</h2>
        <div className="policy-pills" style={{ marginBottom: 14 }}>
          {POLICIES.map((p) => (
            <button
              key={p.id}
              className={`pill ${status?.policy === p.id ? "selected" : ""}`}
              onClick={() => applyPolicy(p.id)}
            >
              {p.label}
            </button>
          ))}
        </div>

        <div className="controls-row" style={{ marginBottom: 14 }}>
          <label style={{ fontSize: "0.84rem", color: "#94a3b8" }}>
            Fairness Floor (0–0.9):{" "}
            <input
              type="number"
              min={0}
              max={0.9}
              step={0.05}
              value={fairFloor}
              style={{ width: 72 }}
              onChange={(e) => setFairFloor(Number(e.target.value))}
            />
          </label>
          <label style={{ fontSize: "0.84rem", color: "#94a3b8" }}>
            Downlink Cap (MB/s):{" "}
            <input
              type="number"
              min={0.5}
              max={200}
              step={0.5}
              value={dlMbps}
              style={{ width: 78 }}
              onChange={(e) => setDlMbps(Number(e.target.value))}
            />
          </label>
          <label style={{ fontSize: "0.84rem", color: "#94a3b8" }}>
            Uplink Cap (MB/s):{" "}
            <input
              type="number"
              min={0.5}
              max={200}
              step={0.5}
              value={ulMbps}
              style={{ width: 78 }}
              onChange={(e) => setUlMbps(Number(e.target.value))}
            />
          </label>
          <button
            className="btn"
            onClick={() => applyPolicy(status?.policy ?? "flowweave")}
          >
            Apply Parameters
          </button>
        </div>

        <table>
          <thead>
            <tr>
              <th>Policy</th>
              <th>Behavior</th>
            </tr>
          </thead>
          <tbody>
            {POLICIES.map((p) => (
              <tr key={p.id}>
                <td>
                  <strong>{p.label}</strong>
                </td>
                <td>{p.desc}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="card">
        <h2>Coordinator Scheduling Decision Log</h2>
        {!status || status.decision_log.length === 0 ? (
          <p style={{ color: "#94a3b8", margin: 0 }}>
            Start transfers or switch policies to observe live coordinator rationale logs.
          </p>
        ) : (
          <ul className="log-list">
            {[...status.decision_log].reverse().map((entry, idx) => (
              <li key={idx}>
                <strong style={{ color: "#10b981" }}>[{entry.policy}]</strong>{" "}
                {entry.messages.join(" · ")}
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
};
