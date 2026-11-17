import React, { useEffect, useState } from "react";
import { Bar, Line } from "react-chartjs-2";
import { fetchScenarios, runExperiment } from "../api";
import type { ExperimentBundle, PolicyName, ScenarioInfo } from "../types";

const POLICY_ORDER: PolicyName[] = ["independent", "max_min", "srpt", "flowweave"];
const POLICY_LABELS: Record<PolicyName, string> = {
  independent: "Independent",
  max_min: "Max-Min Fair",
  srpt: "SRPT",
  flowweave: "FlowWeave",
};
const POLICY_COLORS: Record<PolicyName, string> = {
  independent: "#94a3b8",
  max_min: "#3b82f6",
  srpt: "#f59e0b",
  flowweave: "#10b981",
};

export const ExperimentLab: React.FC = () => {
  const [scenarios, setScenarios] = useState<ScenarioInfo[]>([]);
  const [selected, setSelected] = useState("mixed_shared_bottleneck");
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
      const res = await runExperiment(selected, seed);
      setBundle(res);
    } catch (e) {
      setErr(String(e));
    } finally {
      setLoading(false);
    }
  }

  const fctChartData = bundle
    ? {
        labels: POLICY_ORDER.map((p) => POLICY_LABELS[p]),
        datasets: [
          {
            label: "Sum FCT Σ T_i (s) — lower is better",
            data: POLICY_ORDER.map((p) => bundle.policies[p].sum_fct_s),
            backgroundColor: POLICY_ORDER.map((p) => POLICY_COLORS[p]),
          },
        ],
      }
    : null;

  const fairnessChartData = bundle
    ? {
        labels: POLICY_ORDER.map((p) => POLICY_LABELS[p]),
        datasets: [
          {
            label: "Jain's Fairness Index (0–1)",
            data: POLICY_ORDER.map((p) => bundle.policies[p].jains_fairness),
            backgroundColor: "#3b82f6",
          },
          {
            label: "Avg FCT (s)",
            data: POLICY_ORDER.map((p) => bundle.policies[p].avg_fct_s),
            backgroundColor: "#f59e0b",
          },
        ],
      }
    : null;

  const timelineChartData = bundle
    ? (() => {
        const longest = POLICY_ORDER.reduce((best, p) => {
          const tl = bundle.timelines[p] ?? [];
          return tl.length > best.length ? tl : best;
        }, [] as Array<{ time_s: number; aggregate_goodput_bps: number }>);
        return {
          labels: longest.map((pt) => `${pt.time_s.toFixed(1)}s`),
          datasets: POLICY_ORDER.map((p) => ({
            label: POLICY_LABELS[p],
            data: (bundle.timelines[p] ?? []).map((pt) =>
              Number((pt.aggregate_goodput_bps / (1024 * 1024)).toFixed(2))
            ),
            borderColor: POLICY_COLORS[p],
            backgroundColor: "transparent",
            pointRadius: 0,
            tension: 0.2,
          })),
        };
      })()
    : null;

  return (
    <div>
      {err && <div className="error-banner">{err}</div>}

      <div className="card">
        <h2>
          <span>Simulation Benchmark & Policy Comparison</span>
          <span className="badge badge-sim">Simulated Fluid-Flow Model (Not Live Hardware)</span>
        </h2>
        <div className="controls-row">
          <select value={selected} onChange={(e) => setSelected(e.target.value)}>
            {scenarios.map((sc) => (
              <option key={sc.name} value={sc.name}>
                {sc.name} ({sc.num_transfers} transfers, {sc.num_links} links)
              </option>
            ))}
          </select>
          <label style={{ fontSize: "0.84rem", color: "#94a3b8" }}>
            Seed:{" "}
            <input
              type="number"
              value={seed}
              style={{ width: 80 }}
              onChange={(e) => setSeed(Number(e.target.value))}
            />
          </label>
          <button className="btn" disabled={loading} onClick={handleRun}>
            {loading ? "Running Simulation..." : "Run 4-Policy Comparison"}
          </button>
        </div>
        {scenarios.find((s) => s.name === selected) && (
          <p style={{ color: "#94a3b8", fontSize: "0.86rem", margin: "10px 0 0" }}>
            {scenarios.find((s) => s.name === selected)?.description}
          </p>
        )}
      </div>

      {bundle && (
        <>
          <div className="grid-2">
            <div className="card" style={{ marginBottom: 0 }}>
              <h2>Total Completion-Time Sum (Σ T_i)</h2>
              <div className="chart-box">
                {fctChartData && (
                  <Bar
                    data={fctChartData}
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
                )}
              </div>
            </div>

            <div className="card" style={{ marginBottom: 0 }}>
              <h2>Average FCT vs Jain's Fairness Index</h2>
              <div className="chart-box">
                {fairnessChartData && (
                  <Bar
                    data={fairnessChartData}
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
                )}
              </div>
            </div>
          </div>

          <div className="card">
            <h2>Aggregate Goodput Timeline (MB/s)</h2>
            <div className="chart-box">
              {timelineChartData && (
                <Line
                  data={timelineChartData}
                  options={{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { labels: { color: "#cbd5e1" } } },
                    scales: {
                      x: { ticks: { color: "#94a3b8", maxTicksLimit: 12 }, grid: { color: "#1e293b" } },
                      y: { beginAtZero: true, ticks: { color: "#94a3b8" }, grid: { color: "#1e293b" } },
                    },
                  }}
                />
              )}
            </div>
          </div>

          <div className="card">
            <h2>Full Evaluation Metrics — {bundle.scenario}</h2>
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
                    <th>Sched Avg (ms)</th>
                    <th>Replicas Won</th>
                  </tr>
                </thead>
                <tbody>
                  {POLICY_ORDER.map((p) => {
                    const m = bundle.policies[p];
                    return (
                      <tr key={p}>
                        <td>
                          <strong style={{ color: POLICY_COLORS[p] }}>{POLICY_LABELS[p]}</strong>
                        </td>
                        <td>{m.sum_fct_s.toFixed(2)}</td>
                        <td>{m.avg_fct_s.toFixed(2)}</td>
                        <td>{m.p95_fct_s.toFixed(2)}</td>
                        <td>{m.makespan_s.toFixed(2)}</td>
                        <td>{(m.aggregate_goodput_bps / (1024 * 1024)).toFixed(2)}</td>
                        <td>{m.jains_fairness.toFixed(3)}</td>
                        <td>{(m.redundant_bytes / 1024).toFixed(1)}</td>
                        <td>{m.scheduler_overhead_ms_avg.toFixed(4)}</td>
                        <td>
                          {m.replications_won}/{m.replications_triggered}
                        </td>
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
