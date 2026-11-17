import type {
  DemoFileInfo,
  ExperimentBundle,
  MonitorStatus,
  PolicyName,
  ScenarioInfo,
} from "./types";

const BASE = "/api";

async function jsonFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${text}`);
  }
  return res.json() as Promise<T>;
}

export function fetchMonitor(): Promise<MonitorStatus> {
  return jsonFetch<MonitorStatus>("/monitor");
}

export async function fetchDemoFiles(): Promise<DemoFileInfo[]> {
  const res = await jsonFetch<{ files: DemoFileInfo[] }>("/demo/files");
  return res.files;
}

export function updateScheduler(payload: {
  policy: PolicyName;
  min_fair_share_fraction?: number;
  total_link_capacity_bps?: number;
  upload_link_capacity_bps?: number;
}) {
  return jsonFetch("/scheduler", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function startDemoBatch(
  mode: "downloads" | "uploads" | "mixed",
  downlinkMbps: number,
  uplinkMbps: number
) {
  return jsonFetch("/demo/start", {
    method: "POST",
    body: JSON.stringify({
      mode,
      downlink_mbps: downlinkMbps,
      uplink_mbps: uplinkMbps,
      include_noranges_file: true,
    }),
  });
}

export function submitDownload(url: string, filename?: string) {
  return jsonFetch("/transfers/download", {
    method: "POST",
    body: JSON.stringify({ url, filename: filename || undefined }),
  });
}

export function submitSampleUpload(sampleFilename: string) {
  return jsonFetch("/transfers/upload", {
    method: "POST",
    body: JSON.stringify({ sample_filename: sampleFilename }),
  });
}

export async function uploadBrowserFile(file: File) {
  const res = await fetch(`${BASE}/transfers/upload-browser`, {
    method: "POST",
    headers: { "X-Filename": file.name },
    body: file,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${text}`);
  }
  return res.json();
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
