import React, { useEffect, useState } from "react";
import { Bar, Line } from "react-chartjs-2";
import { fetchScenarios, runExperiment } from "../api";
import type { ExperimentBundle, PolicyName, ScenarioInfo } from "../types";

const POLICY_ORDER: PolicyName[] = ["independent", "max_min", "srpt", "flowweave"];
const POLICY_LABELS: Record<PolicyName, string> = {
  independent: "Independent",
  max_min: "Max-Min",
  srpt: "SRPT",
  flowweave: "FlowWeave",
};

// Strict duocolour palette (monochrome ink steps + emerald accent for FlowWeave)
const POLICY_COLORS: Record<PolicyName, string> = {
  independent: "#52525b",
  max_min: "#71717a",
  srpt: "#e4e4e7",
  flowweave: "#10b981",
};

export const ExperimentLab: React.FC = () => {
  const [scenarios, setScenarios] = useState<ScenarioInfo[]>([]);
  const [selected, setSelected] = useState("bidirectional_mixed");
  const [seed, setSeed] = useState(42);
  const [bundle, setBundle] = useState<ExperimentBundle | null>(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    fetchScenarios()
      .then((list) => {
        setScenarios(list);
        if (list.length > 0 && !list.some((s) => s.name === selected)) {
          setSelected(list[0].name);
        }
      })
      .catch((e) => setErr(String(e)));
  }, []);

  async function handleRun() {
    setLoading(true);
    setErr(null);
    try {
      setBundle(await runExperiment(selected, seed));
    } catch (e) {
      setErr(String(e));
    } finally {
      setLoading(false);
    }
  }

  return (
    <div>
      {err && <div className="banner banner-error">{err}</div>}
      <div className="card">
        <div className="card-head">
          <h2 className="card-title">Controlled Fluid-Flow Simulator</h2>
          <span className="badge">Simulated Topology Comparison</span>
        </div>
        <div className="row">
          <select value={selected} onChange={(e) => setSelected(e.target.value)}>
            {scenarios.map((sc) => (
              <option key={sc.name} value={sc.name}>
                {sc.name} ({sc.num_transfers} transfers, {sc.num_links} links)
              </option>
            ))}
          </select>
          <input
            type="number"
            value={seed}
            style={{ width: 84 }}
            onChange={(e) => setSeed(Number(e.target.value))}
          />
          <button className="btn" disabled={loading} onClick={handleRun}>
            {loading ? "Running..." : "Run 4-Policy Comparison"}
          </button>
        </div>
        {scenarios.find((s) => s.name === selected) && (
          <p className="muted" style={{ margin: "10px 0 0" }}>
            {scenarios.find((s) => s.name === selected)?.description}
          </p>
        )}
      </div>

      {bundle && (
        <>
          <div className="grid-2">
            <div className="card" style={{ marginBottom: 0 }}>
              <div className="card-head">
                <h2 className="card-title">Total Completion Time Sum (Σ T_i)</h2>
                <span className="muted">Lower is better</span>
              </div>
              <div className="chart-box">
                <Bar
                  data={{
                    labels: POLICY_ORDER.map((p) => POLICY_LABELS[p]),
                    datasets: [
                      {
                        label: "Sum FCT (s)",
                        data: POLICY_ORDER.map((p) => bundle.policies[p].sum_fct_s),
                        backgroundColor: POLICY_ORDER.map((p) => POLICY_COLORS[p]),
                        borderRadius: 6,
                      },
                    ],
                  }}
                  options={{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: false } },
                    scales: {
                      x: { ticks: { color: "#9ca3af" }, grid: { display: false } },
                      y: { beginAtZero: true, ticks: { color: "#9ca3af" }, grid: { color: "#27272a" } },
                    },
                  }}
                />
              </div>
            </div>

            <div className="card" style={{ marginBottom: 0 }}>
              <div className="card-head">
                <h2 className="card-title">Aggregate Goodput Over Time</h2>
                <span className="muted">MB/s</span>
              </div>
              <div className="chart-box">
                <Line
                  data={{
                    labels: (bundle.timelines.flowweave ?? []).map((pt) => `${pt.time_s.toFixed(1)}s`),
                    datasets: POLICY_ORDER.map((p) => ({
                      label: POLICY_LABELS[p],
                      data: (bundle.timelines[p] ?? []).map((pt) =>
                        Number((pt.aggregate_goodput_bps / (1024 * 1024)).toFixed(2))
                      ),
                      borderColor: POLICY_COLORS[p],
                      borderWidth: p === "flowweave" ? 2.5 : 1.6,
                      pointRadius: 0,
                      tension: 0.2,
                    })),
                  }}
                  options={{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { labels: { color: "#d4d4d8" } } },
                    scales: {
                      x: { ticks: { color: "#9ca3af", maxTicksLimit: 10 }, grid: { display: false } },
                      y: { beginAtZero: true, ticks: { color: "#9ca3af" }, grid: { color: "#27272a" } },
                    },
                  }}
                />
              </div>
            </div>
          </div>

          <div className="card">
            <div className="card-head">
              <h2 className="card-title">Simulated Metrics — {bundle.scenario}</h2>
            </div>
            <div style={{ overflowX: "auto" }}>
              <table>
                <thead>
                  <tr>
                    <th>Policy</th>
                    <th>Sum FCT (s)</th>
                    <th>Avg FCT (s)</th>
                    <th>P95 FCT (s)</th>
                    <th>Makespan (s)</th>
                    <th>Goodput (MB/s)</th>
                    <th>Jain Fairness</th>
                    <th>Redundant KB</th>
                    <th>Sched (ms)</th>
                  </tr>
                </thead>
                <tbody>
                  {POLICY_ORDER.map((p) => {
                    const m = bundle.policies[p];
                    return (
                      <tr key={p}>
                        <td>
                          <strong style={{ color: p === "flowweave" ? "#34d399" : "#f4f4f5" }}>
                            {POLICY_LABELS[p]}
                          </strong>
                        </td>
                        <td>{m.sum_fct_s.toFixed(2)}</td>
                        <td>{m.avg_fct_s.toFixed(2)}</td>
                        <td>{m.p95_fct_s.toFixed(2)}</td>
                        <td>{m.makespan_s.toFixed(2)}</td>
                        <td>{(m.aggregate_goodput_bps / (1024 * 1024)).toFixed(2)}</td>
                        <td>{m.jains_fairness.toFixed(3)}</td>
                        <td>{(m.redundant_bytes / 1024).toFixed(1)}</td>
                        <td>{m.scheduler_overhead_ms_avg.toFixed(4)}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </div>
  );
};
