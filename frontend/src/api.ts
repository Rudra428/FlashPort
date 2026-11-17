import type {
  CompareStatus,
  ExperimentBundle,
  HostInfo,
  MonitorStatus,
  PolicyName,
  ReceiverStatus,
  RecordedRun,
  RemoteFile,
  ScenarioInfo,
  StagedFile,
} from "./types";

const BASE = "/api";

async function jsonFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!res.ok) {
    let msg = `${res.status} ${res.statusText}`;
    try {
      const body = await res.json();
      if (body?.detail) msg = String(body.detail);
    } catch {
      const text = await res.text();
      if (text) msg = text;
    }
    throw new Error(msg);
  }
  return res.json() as Promise<T>;
}

export function fetchHostInfo(): Promise<HostInfo> {
  return jsonFetch<HostInfo>("/host-info");
}

export function fetchReceiverStatus(): Promise<ReceiverStatus> {
  return jsonFetch<ReceiverStatus>("/receiver");
}

export function updateReceiverPort(port: number): Promise<{ tcp_port: number; listening_ports: number[]; lan_ip: string; listening: boolean }> {
  return jsonFetch("/receiver/port", {
    method: "POST",
    body: JSON.stringify({ port }),
  });
}

export function addReceiverPort(port: number): Promise<{ added_port: number; listening_ports: number[] }> {
  return jsonFetch("/receiver/ports", {
    method: "POST",
    body: JSON.stringify({ port }),
  });
}

export function removeReceiverPort(port: number): Promise<{ removed_port: number; listening_ports: number[] }> {
  return jsonFetch(`/receiver/ports/${port}`, { method: "DELETE" });
}

export function deleteReceivedFile(filename: string) {
  return jsonFetch(`/receiver/files/${encodeURIComponent(filename)}`, { method: "DELETE" });
}

export function fetchMonitor(): Promise<MonitorStatus> {
  return jsonFetch<MonitorStatus>("/monitor");
}

export function fetchRemoteFiles(
  host: string,
  port: number
): Promise<{ host: string; port: number; rtt_ms: number; files: RemoteFile[] }> {
  return jsonFetch(`/tcp/remote-files?host=${encodeURIComponent(host)}&port=${port}`);
}

export async function stageBrowserFile(file: File): Promise<StagedFile> {
  const res = await fetch(`${BASE}/transfers/stage-browser`, {
    method: "POST",
    headers: { "X-Filename": file.name },
    body: file,
  });
  if (!res.ok) {
    throw new Error(await res.text());
  }
  const data = (await res.json()) as { file: StagedFile };
  return data.file;
}

export async function fetchStagedFiles(): Promise<StagedFile[]> {
  const res = await jsonFetch<{ files: StagedFile[] }>("/transfers/staged");
  return res.files;
}

export function deleteStagedFile(stagedId: string) {
  return jsonFetch(`/transfers/staged/${stagedId}`, { method: "DELETE" });
}

export function sendStagedFiles(payload: {
  host: string;
  port: number;
  policy: PolicyName;
  link_cap_mbps: number;
}) {
  return jsonFetch("/transfers/send-staged", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function compareStagedFiles(payload: {
  host: string;
  port: number;
  policies: PolicyName[];
  link_cap_mbps: number;
}) {
  return jsonFetch("/transfers/compare-staged", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function downloadRemoteBatch(payload: {
  host: string;
  port: number;
  filenames: string[];
  policy: PolicyName;
  link_cap_mbps: number;
}) {
  return jsonFetch("/transfers/download-remote", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function compareRemoteDownloads(payload: {
  host: string;
  port: number;
  filenames: string[];
  policies: PolicyName[];
  link_cap_mbps: number;
}) {
  return jsonFetch("/transfers/compare-remote", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function fetchRecordedRuns(): Promise<{ runs: RecordedRun[]; compare_status: CompareStatus }> {
  return jsonFetch("/runs");
}

export function clearRecordedRuns() {
  return jsonFetch("/runs/clear", { method: "POST" });
}

export function clearFinishedTransfers() {
  return jsonFetch("/transfers/clear", { method: "POST" });
}

export function updateScheduler(payload: {
  policy: PolicyName;
  total_link_capacity_bps?: number;
  upload_link_capacity_bps?: number;
}) {
  return jsonFetch("/scheduler", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function pauseTransfer(transferId: string) {
  return jsonFetch(`/transfers/${transferId}/pause`, { method: "POST" });
}

export function resumeTransfer(transferId: string) {
  return jsonFetch(`/transfers/${transferId}/resume`, { method: "POST" });
}

export function cancelTransfer(transferId: string) {
  return jsonFetch(`/transfers/${transferId}/cancel`, { method: "POST" });
}

export async function fetchScenarios(): Promise<ScenarioInfo[]> {
  const res = await jsonFetch<{ scenarios: ScenarioInfo[] }>("/experiments/scenarios");
  return res.scenarios;
}

export function runExperiment(scenario: string, seed: number): Promise<ExperimentBundle> {
  return jsonFetch<ExperimentBundle>("/experiments/run", {
    method: "POST",
    body: JSON.stringify({ scenario, seed }),
  });
}
