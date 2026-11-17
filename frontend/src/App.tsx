import React, { useCallback, useEffect, useState } from "react";
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
import { fetchDemoFiles, fetchMonitor } from "./api";
import { ExperimentLab } from "./components/ExperimentLab";
import {
  DownloadsPanel,
  NetworkMonitorPanel,
  OverviewPanel,
  SchedulerPanel,
  UploadsPanel,
} from "./components/LiveMonitor";
import { TransfersTable } from "./components/TransfersTable";
import type { DemoFileInfo, MonitorStatus } from "./types";

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

type TabId =
  | "overview"
  | "transfers"
  | "uploads"
  | "downloads"
  | "monitor"
  | "scheduler"
  | "experiments";

const TABS: Array<{ id: TabId; label: string }> = [
  { id: "overview", label: "Overview" },
  { id: "transfers", label: "All Transfers" },
  { id: "uploads", label: "Uploads" },
  { id: "downloads", label: "Downloads" },
  { id: "monitor", label: "Network Monitor" },
  { id: "scheduler", label: "Scheduler" },
  { id: "experiments", label: "Experiments" },
];

export const App: React.FC = () => {
  const [tab, setTab] = useState<TabId>("overview");
  const [status, setStatus] = useState<MonitorStatus | null>(null);
  const [demoFiles, setDemoFiles] = useState<DemoFileInfo[]>([]);
  const [apiError, setApiError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const data = await fetchMonitor();
      setStatus(data);
      setApiError(null);
    } catch (e) {
      setApiError(`Backend unreachable (${String(e)}). Start FastAPI via: python -m flowweave serve`);
    }
  }, []);

  useEffect(() => {
    refresh();
    fetchDemoFiles().then(setDemoFiles).catch(() => {});
    const timer = window.setInterval(refresh, 750);
    return () => window.clearInterval(timer);
  }, [refresh]);

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="brand">
          <h1>FlowWeave</h1>
          <p>Cooperative, Congestion-Aware Bidirectional File-Transfer Scheduling</p>
        </div>
        <nav className="tabs">
          {TABS.map((t) => (
            <button
              key={t.id}
              className={`tab-btn ${tab === t.id ? "active" : ""}`}
              onClick={() => setTab(t.id)}
            >
              {t.label}
            </button>
          ))}
        </nav>
      </header>

      {apiError && <div className="error-banner">{apiError}</div>}

      {tab === "overview" && (
        <OverviewPanel status={status} demoFiles={demoFiles} onRefresh={refresh} />
      )}
      {tab === "transfers" && (
        <div className="card">
          <h2>All Transfers — Uploads & Downloads ({status?.transfers.length ?? 0})</h2>
          <TransfersTable
            transfers={status?.transfers ?? []}
            onRefresh={refresh}
            emptyText="No transfers yet. Launch a demo batch from Overview or add an upload/download."
          />
        </div>
      )}
      {tab === "uploads" && (
        <UploadsPanel status={status} demoFiles={demoFiles} onRefresh={refresh} />
      )}
      {tab === "downloads" && (
        <DownloadsPanel status={status} demoFiles={demoFiles} onRefresh={refresh} />
      )}
      {tab === "monitor" && <NetworkMonitorPanel status={status} />}
      {tab === "scheduler" && <SchedulerPanel status={status} onRefresh={refresh} />}
      {tab === "experiments" && <ExperimentLab />}
    </div>
  );
};

export default App;
