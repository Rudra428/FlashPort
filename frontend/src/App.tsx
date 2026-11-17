import React, { useCallback, useEffect, useRef, useState } from "react";
import {
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  Filler,
  Legend,
  LinearScale,
  LineElement,
  PointElement,
  Title,
  Tooltip,
} from "chart.js";
import { Bar, Line } from "react-chartjs-2";
import {
  addReceiverPort,
  cancelTransfer,
  clearFinishedTransfers,
  clearRecordedRuns,
  compareStagedFiles,
  deleteReceivedFile,
  deleteStagedFile,
  fetchHostInfo,
  fetchMonitor,
  fetchReceiverStatus,
  fetchRecordedRuns,
  fetchRemoteFiles,
  fetchStagedFiles,
  pauseTransfer,
  removeReceiverPort,
  resumeTransfer,
  sendStagedFiles,
  stageBrowserFile,
  updateScheduler,
} from "./api";
import { ExperimentLab } from "./components/ExperimentLab";
import type {
  CompareStatus,
  HostInfo,
  MonitorStatus,
  PolicyName,
  ReceiverStatus,
  RecordedRun,
  StagedFile,
} from "./types";

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend,
  Filler
);

const POLICIES: Array<{ id: PolicyName; label: string }> = [
  { id: "flowweave", label: "FlowWeave" },
  { id: "srpt", label: "SRPT" },
  { id: "max_min", label: "Max-Min" },
  { id: "independent", label: "Independent" },
];

const fmtSize = (bytes: number) => {
  if (bytes >= 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${bytes} B`;
};

const fmtRate = (bps: number) => `${(bps / (1024 * 1024)).toFixed(2)} MB/s`;

export const App: React.FC = () => {
  const [tab, setTab] = useState<"sender" | "receiver" | "sim">("sender");
  const [hostInfo, setHostInfo] = useState<HostInfo | null>(null);
  const [receiver, setReceiver] = useState<ReceiverStatus | null>(null);
  const [newPortInput, setNewPortInput] = useState(5000);
  const [status, setStatus] = useState<MonitorStatus | null>(null);
  const [runs, setRuns] = useState<RecordedRun[]>([]);
  const [compareStatus, setCompareStatus] = useState<CompareStatus | null>(null);

  // Sender target receiver IP & port
  const [targetIp, setTargetIp] = useState("127.0.0.1");
  const [targetPort, setTargetPort] = useState(9000);
  const [connectedInfo, setConnectedInfo] = useState<{ host: string; port: number; rtt_ms: number } | null>(
    null
  );

  // Local user-picked files (nothing pre-seeded)
  const [staged, setStaged] = useState<StagedFile[]>([]);
  const [linkCapMbps, setLinkCapMbps] = useState(2.5);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement | null>(null);

  const refresh = useCallback(async () => {
    try {
      const [mon, runData, recv] = await Promise.all([
        fetchMonitor(),
        fetchRecordedRuns(),
        fetchReceiverStatus(),
      ]);
      setStatus(mon);
      setRuns(runData.runs);
      setCompareStatus(runData.compare_status);
      setReceiver(recv);
      setError(null);
    } catch (e) {
      setError(String(e));
    }
  }, []);

  useEffect(() => {
    fetchHostInfo()
      .then((info) => {
        setHostInfo(info);
        setTargetPort(info.tcp_port);
      })
      .catch(() => {});
    fetchStagedFiles().then(setStaged).catch(() => {});
    refresh();
    const timer = window.setInterval(refresh, 650);
    return () => window.clearInterval(timer);
  }, [refresh]);

  async function handleTestConnection() {
    setBusy(true);
    setError(null);
    try {
      const res = await fetchRemoteFiles(targetIp.trim(), targetPort);
      setConnectedInfo({ host: res.host, port: res.port, rtt_ms: res.rtt_ms });
    } catch (e) {
      setConnectedInfo(null);
      setError(String(e));
    } finally {
      setBusy(false);
    }
  }

  async function handlePickFiles(files: FileList | null) {
    if (!files || files.length === 0) return;
    setBusy(true);
    setError(null);
    try {
      const added: StagedFile[] = [];
      for (const f of Array.from(files)) {
        added.push(await stageBrowserFile(f));
      }
      setStaged((prev) => [...prev, ...added]);
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
      if (fileInputRef.current) fileInputRef.current.value = "";
    }
  }

  async function handlePolicySelect(policy: PolicyName) {
    setError(null);
    try {
      const capBps = linkCapMbps * 1024 * 1024;
      await updateScheduler({
        policy,
        total_link_capacity_bps: capBps,
        upload_link_capacity_bps: capBps,
      });
      refresh();
    } catch (e) {
      setError(String(e));
    }
  }

  async function handleSendAll() {
    if (staged.length === 0) return;
    setBusy(true);
    setError(null);
    try {
      await sendStagedFiles({
        host: targetIp.trim(),
        port: targetPort,
        policy: status?.policy ?? "flowweave",
        link_cap_mbps: linkCapMbps,
      });
      refresh();
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  }

  async function handleCompare(policies: PolicyName[]) {
    if (staged.length === 0) return;
    setBusy(true);
    setError(null);
    try {
      await compareStagedFiles({
        host: targetIp.trim(),
        port: targetPort,
        policies,
        link_cap_mbps: linkCapMbps,
      });
      refresh();
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  }

  async function handleAddPort() {
    setBusy(true);
    setError(null);
    try {
      await addReceiverPort(newPortInput);
      refresh();
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  }

  const transfers = status?.transfers ?? [];
  const activeCount = transfers.filter((t) => t.state === "active").length;
  const completedCount = transfers.filter((t) => t.state === "completed").length;
  const history = status?.history ?? [];
  const activeIncoming = (receiver?.incoming ?? []).filter((i) => i.status === "receiving").length;

  const latestByPolicy = new Map<PolicyName, RecordedRun>();
  for (const r of runs) {
    latestByPolicy.set(r.policy, r);
  }
  const comparedPolicies = (
    ["independent", "max_min", "srpt", "flowweave"] as PolicyName[]
  ).filter((p) => latestByPolicy.has(p));

  return (
    <div className="shell">
      <header className="header">
        <div className="brand">
          <div className="brand-mark" />
          <div>
            <h1>FlowWeave</h1>
            <span>Cooperative Raw-TCP File Transfer</span>
          </div>
        </div>

        <div className="header-right">
          <div className="host-pill">
            {receiver?.lan_ip ?? hostInfo?.lan_ip ?? "127.0.0.1"}:
            {(receiver?.listening_ports ?? [hostInfo?.tcp_port ?? 9000]).join(",")}
          </div>

          <div className="nav-switch">
            <button
              className={`nav-btn ${tab === "sender" ? "active" : ""}`}
              onClick={() => setTab("sender")}
            >
              Sender
            </button>
            <button
              className={`nav-btn ${tab === "receiver" ? "active" : ""}`}
              onClick={() => setTab("receiver")}
            >
              Receiver{activeIncoming > 0 ? ` (${activeIncoming})` : ""}
            </button>
            <button
              className={`nav-btn ${tab === "sim" ? "active" : ""}`}
              onClick={() => setTab("sim")}
            >
              Simulator
            </button>
          </div>
        </div>
      </header>

      {error && <div className="banner banner-error">{error}</div>}
      {compareStatus?.running && (
        <div className="banner">
          Running live TCP comparison — currently executing{" "}
          <strong>{compareStatus.current_policy?.toUpperCase()}</strong> (
          {compareStatus.completed_policies.length}/{compareStatus.total_policies} completed)
        </div>
      )}

      {/* ==================== SENDER TAB ==================== */}
      {tab === "sender" && (
        <>
          <div className="card">
            <div className="row" style={{ justifyContent: "space-between" }}>
              <div className="row">
                <label className="field">
                  Target Receiver IP
                  <input
                    type="text"
                    value={targetIp}
                    onChange={(e) => setTargetIp(e.target.value)}
                    placeholder="e.g. 192.170.6.90"
                    style={{ width: 175 }}
                  />
                </label>
                <label className="field">
                  Port
                  <input
                    type="number"
                    value={targetPort}
                    onChange={(e) => setTargetPort(Number(e.target.value))}
                    style={{ width: 92 }}
                  />
                </label>
                <div style={{ paddingTop: 18 }}>
                  <button className="btn-outline" disabled={busy} onClick={handleTestConnection}>
                    Test Connection
                  </button>
                </div>
                {connectedInfo && (
                  <span className="badge badge-accent" style={{ marginTop: 18 }}>
                    Connected · {connectedInfo.rtt_ms} ms
                  </span>
                )}
              </div>

              <div className="row">
                <label className="field">
                  Bandwidth Cap (MB/s)
                  <input
                    type="number"
                    min={0.5}
                    max={500}
                    step={0.5}
                    value={linkCapMbps}
                    onChange={(e) => setLinkCapMbps(Number(e.target.value))}
                    style={{ width: 96 }}
                  />
                </label>
                <div className="field">
                  Scheduling Algorithm
                  <div className="pills">
                    {POLICIES.map((p) => (
                      <button
                        key={p.id}
                        className={`pill ${status?.policy === p.id ? "selected" : ""}`}
                        onClick={() => handlePolicySelect(p.id)}
                      >
                        {p.label}
                      </button>
                    ))}
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* File Selection & Dispatch */}
          <div className="card">
            <div className="card-head">
              <h2 className="card-title">
                Send Multiple Files to {targetIp}:{targetPort}
              </h2>
              <span className="muted">{staged.length} files queued</span>
            </div>

            <div
              className="dropzone"
              onClick={() => fileInputRef.current?.click()}
              onDragOver={(e) => e.preventDefault()}
              onDrop={(e) => {
                e.preventDefault();
                handlePickFiles(e.dataTransfer.files);
              }}
            >
              <input
                ref={fileInputRef}
                type="file"
                multiple
                style={{ display: "none" }}
                onChange={(e) => handlePickFiles(e.target.files)}
              />
              <div style={{ fontWeight: 600, fontSize: "0.9rem" }}>
                Click or drop files to send simultaneously
              </div>
              <div className="muted">
                Received files are saved directly into the receiver&apos;s Downloads folder
              </div>
            </div>

            {staged.length > 0 && (
              <ul className="file-list">
                {staged.map((f) => (
                  <li key={f.staged_id}>
                    <span>
                      <strong>{f.filename}</strong>{" "}
                      <span className="muted">({fmtSize(f.size_bytes)})</span>
                    </span>
                    <button
                      className="btn-xs"
                      onClick={async () => {
                        await deleteStagedFile(f.staged_id);
                        setStaged((prev) => prev.filter((x) => x.staged_id !== f.staged_id));
                      }}
                    >
                      ×
                    </button>
                  </li>
                ))}
              </ul>
            )}

            <div className="row">
              <button
                className="btn"
                disabled={busy || staged.length === 0 || compareStatus?.running}
                onClick={handleSendAll}
              >
                Send Simultaneously
              </button>
              <button
                className="btn-outline"
                disabled={busy || staged.length === 0 || compareStatus?.running}
                onClick={() => handleCompare(["srpt", "flowweave"])}
              >
                Compare SRPT vs FlowWeave
              </button>
              <button
                className="btn-outline"
                disabled={busy || staged.length === 0 || compareStatus?.running}
                onClick={() => handleCompare(["independent", "max_min", "srpt", "flowweave"])}
              >
                Compare All 4 Policies
              </button>
            </div>
          </div>

          {/* KPI Strip */}
          <div className="grid-4">
            <div className="kpi">
              <div className="kpi-label">Active / Completed</div>
              <div className="kpi-val">
                {activeCount} / {completedCount}
              </div>
              <div className="kpi-sub">Parallel TCP socket workers</div>
            </div>
            <div className="kpi">
              <div className="kpi-label">Measured Goodput</div>
              <div className="kpi-val accent">{fmtRate(status?.aggregate_goodput_bps ?? 0)}</div>
              <div className="kpi-sub">
                ↑ {fmtRate(status?.upload_throughput_bps ?? 0)} · ↓{" "}
                {fmtRate(status?.download_throughput_bps ?? 0)}
              </div>
            </div>
            <div className="kpi">
              <div className="kpi-label">Avg Completion Time</div>
              <div className="kpi-val">
                {status && status.average_fct_s > 0 ? `${status.average_fct_s.toFixed(2)}s` : "—"}
              </div>
              <div className="kpi-sub">Measured wall-clock FCT</div>
            </div>
            <div className="kpi">
              <div className="kpi-label">Active Policy</div>
              <div className="kpi-val">{status?.policy ?? "—"}</div>
              <div className="kpi-sub">
                Sched tick: {(status?.last_compute_time_ms ?? 0).toFixed(3)} ms
              </div>
            </div>
          </div>

          {/* Convex Optimization Telemetry (shown when flowweave is active) */}
          {status?.policy === "flowweave" && status.last_optimization && (
            <div className="card">
              <div className="card-head">
                <h2 className="card-title">Convex Bandwidth Optimizer Telemetry</h2>
                <span className="badge badge-accent">
                  {status.last_optimization.solver_name} · {status.last_optimization.optimization_status}
                </span>
              </div>
              <div className="row" style={{ gap: 22, marginBottom: 10 }}>
                <span className="muted">
                  Solve Time: <strong style={{ color: "#34d399" }}>{status.last_optimization.solve_time_ms.toFixed(3)} ms</strong>
                </span>
                <span className="muted">
                  Fairness Floor (f):{" "}
                  <strong>
                    {status.last_optimization.fairness_fraction != null
                      ? status.last_optimization.fairness_fraction.toFixed(2)
                      : "0.00 (pure ΣT_i)"}
                  </strong>
                </span>
                <span className="muted">
                  Predicted ΣT_i (model, not measured):{" "}
                  <strong>
                    {status.last_optimization.objective_value_s != null
                      ? `${status.last_optimization.objective_value_s.toFixed(2)}s`
                      : "—"}
                  </strong>
                </span>
              </div>
              <div className="row" style={{ gap: 10 }}>
                {Object.entries(status.last_optimization.effective_capacities_bps).map(([lid, cap]) => {
                  const util = (status.last_optimization?.link_utilization[lid] ?? 0) * 100;
                  return (
                    <span key={lid} className="host-pill">
                      {lid}: C_eff {fmtRate(cap)} · alloc util {util.toFixed(0)}%
                    </span>
                  );
                })}
              </div>
            </div>
          )}

          {/* Live Throughput + SRPT vs FlowWeave Comparison */}
          <div className="grid-2">
            <div className="card" style={{ marginBottom: 0 }}>
              <div className="card-head">
                <h2 className="card-title">Live TCP Throughput (MB/s)</h2>
                <span className="badge badge-accent">Live Measured</span>
              </div>
              <div className="chart-box">
                <Line
                  data={{
                    labels: history.map((_, i) => `${i + 1}`),
                    datasets: [
                      {
                        label: "Goodput (MB/s)",
                        data: history.map((h) =>
                          Number((h.data.aggregate_goodput_bps / (1024 * 1024)).toFixed(2))
                        ),
                        borderColor: "#10b981",
                        backgroundColor: "rgba(16, 185, 129, 0.14)",
                        fill: true,
                        pointRadius: 0,
                        tension: 0.25,
                      },
                    ],
                  }}
                  options={{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: false } },
                    scales: {
                      x: { ticks: { color: "#71717a", maxTicksLimit: 8 }, grid: { display: false } },
                      y: { beginAtZero: true, ticks: { color: "#9ca3af" }, grid: { color: "#27272a" } },
                    },
                  }}
                />
              </div>
            </div>

            <div className="card" style={{ marginBottom: 0 }}>
              <div className="card-head">
                <h2 className="card-title">Real Measured Times: SRPT vs FlowWeave</h2>
                {runs.length > 0 && (
                  <button
                    className="btn-xs"
                    onClick={async () => {
                      await clearRecordedRuns();
                      refresh();
                    }}
                  >
                    Clear Runs
                  </button>
                )}
              </div>
              {comparedPolicies.length === 0 ? (
                <p className="muted" style={{ marginTop: 44, textAlign: "center" }}>
                  Select multiple files and click <strong>Compare SRPT vs FlowWeave</strong> to
                  measure real completion times side by side.
                </p>
              ) : (
                <div className="chart-box">
                  <Bar
                    data={{
                      labels: comparedPolicies.map((p) => p.toUpperCase()),
                      datasets: [
                        {
                          label: "Sum FCT Σ T_i (s)",
                          data: comparedPolicies.map((p) => latestByPolicy.get(p)!.sum_fct_s),
                          backgroundColor: comparedPolicies.map((p) =>
                            p === "flowweave" ? "#10b981" : "#e4e4e7"
                          ),
                          borderRadius: 6,
                        },
                        {
                          label: "Avg FCT (s)",
                          data: comparedPolicies.map((p) => latestByPolicy.get(p)!.avg_fct_s),
                          backgroundColor: comparedPolicies.map((p) =>
                            p === "flowweave" ? "rgba(16,185,129,0.45)" : "#52525b"
                          ),
                          borderRadius: 6,
                        },
                      ],
                    }}
                    options={{
                      responsive: true,
                      maintainAspectRatio: false,
                      plugins: { legend: { labels: { color: "#d4d4d8" } } },
                      scales: {
                        x: { ticks: { color: "#f4f4f5" }, grid: { display: false } },
                        y: { beginAtZero: true, ticks: { color: "#9ca3af" }, grid: { color: "#27272a" } },
                      },
                    }}
                  />
                </div>
              )}
            </div>
          </div>

          {/* Recorded Runs Table */}
          {runs.length > 0 && (
            <div className="card">
              <div className="card-head">
                <h2 className="card-title">Recorded Real Transfer Runs</h2>
                <span className="muted">Wall-clock TCP measurements</span>
              </div>
              <div style={{ overflowX: "auto" }}>
                <table>
                  <thead>
                    <tr>
                      <th>Policy</th>
                      <th>Target</th>
                      <th>Files</th>
                      <th>Sum FCT Σ T_i</th>
                      <th>Avg FCT</th>
                      <th>P95 FCT</th>
                      <th>Makespan</th>
                      <th>Goodput</th>
                      <th>Jain Fairness</th>
                    </tr>
                  </thead>
                  <tbody>
                    {[...runs].reverse().map((r) => (
                      <tr key={r.run_id}>
                        <td>
                          <strong style={{ color: r.policy === "flowweave" ? "#34d399" : "#f4f4f5" }}>
                            {r.policy.toUpperCase()}
                          </strong>
                        </td>
                        <td className="muted">{r.target}</td>
                        <td>
                          {r.completed_transfers}/{r.num_transfers}
                        </td>
                        <td>
                          <strong>{r.sum_fct_s.toFixed(2)}s</strong>
                        </td>
                        <td>{r.avg_fct_s.toFixed(2)}s</td>
                        <td>{r.p95_fct_s.toFixed(2)}s</td>
                        <td>{r.makespan_s.toFixed(2)}s</td>
                        <td>{fmtRate(r.aggregate_goodput_bps)}</td>
                        <td>{r.jains_fairness.toFixed(3)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Outgoing Transfers Table */}
          <div className="card">
            <div className="card-head">
              <h2 className="card-title">Outgoing Transfers ({transfers.length})</h2>
              {transfers.length > 0 && (
                <button
                  className="btn-xs"
                  onClick={async () => {
                    await clearFinishedTransfers();
                    refresh();
                  }}
                >
                  Clear Finished
                </button>
              )}
            </div>
            {transfers.length === 0 ? (
              <p className="muted" style={{ margin: 0 }}>
                No outgoing transfers yet. Pick files above and click Send Simultaneously.
              </p>
            ) : (
              <div style={{ overflowX: "auto" }}>
                <table>
                  <thead>
                    <tr>
                      <th>File</th>
                      <th>Status</th>
                      <th>Progress</th>
                      <th>Transferred</th>
                      <th>Measured Speed</th>
                      <th>Allocated</th>
                      <th>Controls</th>
                    </tr>
                  </thead>
                  <tbody>
                    {transfers.map((t) => (
                      <tr key={t.transfer_id}>
                        <td>
                          <strong>{t.filename}</strong>
                          <div className="muted" style={{ fontSize: "0.72rem" }}>
                            {t.completed_chunks}/{t.total_chunks || 1} chunks · {t.active_workers} workers
                          </div>
                          {t.error && (
                            <div style={{ fontSize: "0.72rem", color: "#e4e4e7", marginTop: 2 }}>
                              Error: {t.error}
                            </div>
                          )}
                        </td>
                        <td>
                          <span className={`badge ${t.state === "completed" ? "badge-accent" : ""}`}>
                            {t.state}
                          </span>
                        </td>
                        <td>
                          <div className="bar-track">
                            <div className="bar-fill" style={{ width: `${t.progress * 100}%` }} />
                          </div>
                          {(t.progress * 100).toFixed(0)}%
                        </td>
                        <td>
                          {fmtSize(t.bytes_transferred)} / {fmtSize(t.total_bytes)}
                        </td>
                        <td>
                          {fmtRate(t.measured.rolling_goodput_bps || t.measured.instant_goodput_bps)}
                        </td>
                        <td>{fmtRate(t.allocated_bps)}</td>
                        <td>
                          {t.state === "active" && (
                            <button
                              className="btn-xs"
                              onClick={async () => {
                                await pauseTransfer(t.transfer_id);
                                refresh();
                              }}
                            >
                              Pause
                            </button>
                          )}
                          {t.state === "paused" && (
                            <button
                              className="btn-xs"
                              onClick={async () => {
                                await resumeTransfer(t.transfer_id);
                                refresh();
                              }}
                            >
                              Resume
                            </button>
                          )}
                          {(t.state === "active" || t.state === "paused" || t.state === "pending") && (
                            <button
                              className="btn-xs"
                              onClick={async () => {
                                await cancelTransfer(t.transfer_id);
                                refresh();
                              }}
                            >
                              Cancel
                            </button>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </>
      )}

      {/* ==================== RECEIVER TAB ==================== */}
      {tab === "receiver" && (
        <>
          <div className="card">
            <div className="card-head">
              <div>
                <h2 className="card-title">Listening on All Network Interfaces (0.0.0.0)</h2>
                <span className="muted">
                  Incoming files are verified with SHA-256 and saved directly into your Downloads folder
                </span>
              </div>
              <span className="badge badge-accent">
                {receiver?.listening ? "Sockets Active" : "Starting"}
              </span>
            </div>

            <div className="grid-2" style={{ marginBottom: 14 }}>
              <div>
                <div className="kpi-label" style={{ marginBottom: 8 }}>
                  Reachable IP Addresses on This Host
                </div>
                <div className="pills">
                  {(receiver?.local_ips ?? [receiver?.lan_ip ?? "127.0.0.1"]).map((ip) => (
                    <span key={ip} className="host-pill">
                      {ip}
                    </span>
                  ))}
                </div>
              </div>

              <div>
                <div className="kpi-label" style={{ marginBottom: 8 }}>
                  Downloads Save Folder
                </div>
                <div
                  style={{
                    fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace",
                    fontSize: "0.84rem",
                    color: "#34d399",
                    background: "#18181d",
                    border: "1px solid #27272a",
                    padding: "8px 12px",
                    borderRadius: 8,
                  }}
                >
                  {receiver?.save_dir ?? "Downloads"}
                </div>
              </div>
            </div>

            <div className="row" style={{ justifyContent: "space-between", paddingTop: 8, borderTop: "1px solid #27272a" }}>
              <div className="row">
                <span className="kpi-label">Active TCP Ports:</span>
                {(receiver?.listening_ports ?? [9000]).map((p) => (
                  <span key={p} className="pill selected" style={{ cursor: "default" }}>
                    :{p}
                    {(receiver?.listening_ports?.length ?? 1) > 1 && (
                      <button
                        style={{
                          marginLeft: 6,
                          background: "transparent",
                          border: "none",
                          color: "#34d399",
                          cursor: "pointer",
                          fontWeight: 700,
                        }}
                        onClick={async () => {
                          await removeReceiverPort(p);
                          refresh();
                        }}
                      >
                        ×
                      </button>
                    )}
                  </span>
                ))}
              </div>

              <div className="row">
                <input
                  type="number"
                  min={1}
                  max={65535}
                  value={newPortInput}
                  onChange={(e) => setNewPortInput(Number(e.target.value))}
                  style={{ width: 92 }}
                />
                <button className="btn" disabled={busy} onClick={handleAddPort}>
                  + Listen on Port
                </button>
              </div>
            </div>
          </div>

          {/* Live Incoming Streams */}
          <div className="card">
            <div className="card-head">
              <h2 className="card-title">
                Live Incoming Socket Streams ({receiver?.incoming.length ?? 0})
              </h2>
              <span className="muted">Real-time chunk arrival from remote senders</span>
            </div>

            {!receiver || receiver.incoming.length === 0 ? (
              <p className="muted" style={{ margin: 0 }}>
                Listening on 0.0.0.0:{(receiver?.listening_ports ?? [9000]).join(", ")} — no
                incoming transfers yet. On the Sender laptop, enter{" "}
                <strong>{receiver?.lan_ip ?? "this IP"}</strong> and send files.
              </p>
            ) : (
              <div style={{ overflowX: "auto" }}>
                <table>
                  <thead>
                    <tr>
                      <th>File</th>
                      <th>From Sender IP</th>
                      <th>Port</th>
                      <th>Progress</th>
                      <th>Received</th>
                      <th>Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {receiver.incoming.map((inc) => (
                      <tr key={inc.session_id}>
                        <td>
                          <strong>{inc.saved_as || inc.filename}</strong>
                          <div className="muted" style={{ fontSize: "0.72rem" }}>
                            {inc.received_chunks}/{inc.expected_chunks} chunks
                            {inc.sha256 ? ` · SHA-256 ${inc.sha256.slice(0, 12)}...` : ""}
                          </div>
                        </td>
                        <td>{inc.peer_ip}</td>
                        <td className="muted">:{inc.local_port ?? receiver.tcp_port}</td>
                        <td>
                          <div className="bar-track">
                            <div className="bar-fill" style={{ width: `${inc.progress * 100}%` }} />
                          </div>
                          {(inc.progress * 100).toFixed(0)}%
                        </td>
                        <td>
                          {fmtSize(inc.received_bytes)} / {fmtSize(inc.total_bytes)}
                        </td>
                        <td>
                          <span
                            className={`badge ${
                              inc.status === "saved" || inc.status === "verified" ? "badge-accent" : ""
                            }`}
                          >
                            {inc.status}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          {/* Saved Files in Downloads */}
          <div className="card">
            <div className="card-head">
              <h2 className="card-title">
                Received Files in Downloads ({receiver?.received_files.length ?? 0})
              </h2>
              <span className="muted">{receiver?.save_dir}</span>
            </div>

            {!receiver || receiver.received_files.length === 0 ? (
              <p className="muted" style={{ margin: 0 }}>
                Files sent to this receiver will appear here and directly inside your Downloads folder.
              </p>
            ) : (
              <div style={{ overflowX: "auto" }}>
                <table>
                  <thead>
                    <tr>
                      <th>File Name</th>
                      <th>Size</th>
                      <th>From Peer</th>
                      <th>Saved Path</th>
                      <th>Action</th>
                    </tr>
                  </thead>
                  <tbody>
                    {receiver.received_files.map((rf) => (
                      <tr key={rf.filename}>
                        <td>
                          <strong>{rf.filename}</strong>
                        </td>
                        <td>{fmtSize(rf.size_bytes)}</td>
                        <td className="muted">{rf.peer_ip ?? "—"}</td>
                        <td
                          className="muted"
                          style={{
                            fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace",
                            fontSize: "0.75rem",
                          }}
                        >
                          {rf.dest_path ?? rf.filename}
                        </td>
                        <td>
                          <a
                            className="btn-xs"
                            style={{ textDecoration: "none" }}
                            href={`/api/receiver/download/${encodeURIComponent(rf.filename)}`}
                            download={rf.filename}
                          >
                            Open
                          </a>
                          <button
                            className="btn-xs"
                            onClick={async () => {
                              await deleteReceivedFile(rf.filename);
                              refresh();
                            }}
                          >
                            Remove
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </>
      )}

      {/* ==================== SIMULATOR TAB ==================== */}
      {tab === "sim" && <ExperimentLab />}
    </div>
  );
};

export default App;
