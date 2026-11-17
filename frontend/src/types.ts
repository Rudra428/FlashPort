export type PolicyName = "independent" | "max_min" | "srpt" | "flowweave";
export type TransferDirection = "download" | "upload";
export type TransferState = "pending" | "active" | "paused" | "completed" | "failed" | "cancelled";

export interface HostInfo {
  lan_ip: string;
  tcp_port: number;
  policy: PolicyName;
  downlink_mbps: number;
  uplink_mbps: number;
  compare_status: CompareStatus;
}

export interface CompareStatus {
  running: boolean;
  current_policy: string | null;
  completed_policies: string[];
  total_policies: number;
  error: string | null;
}

export interface StagedFile {
  staged_id: string;
  filename: string;
  size_bytes: number;
}

export interface RemoteFile {
  filename: string;
  size_bytes: number;
}

export interface IncomingTransfer {
  session_id: string;
  transfer_id: string;
  filename: string;
  peer_ip: string;
  local_port?: number;
  total_bytes: number;
  received_bytes: number;
  received_chunks: number;
  expected_chunks: number;
  progress: number;
  status: "receiving" | "saved" | "verified";
  started_at: number;
  completed_at: number | null;
  saved_as: string | null;
  dest_path?: string | null;
  sha256: string | null;
}

export interface ReceivedFile {
  filename: string;
  size_bytes: number;
  dest_path?: string;
  peer_ip?: string;
  sha256?: string | null;
  received_at?: number;
}

export interface ReceiverStatus {
  listening: boolean;
  bind_host?: string;
  lan_ip: string;
  local_ips?: string[];
  tcp_port: number;
  listening_ports?: number[];
  save_dir: string;
  incoming: IncomingTransfer[];
  received_files: ReceivedFile[];
}

export interface MeasuredMetrics {
  bytes_acked: number;
  instant_goodput_bps: number;
  rolling_goodput_bps: number;
  avg_goodput_bps: number;
  last_rtt_ms: number;
  retries: number;
}

export interface EstimatedMetrics {
  ewma_goodput_bps: number;
  predicted_remaining_time_s: number | null;
}

export interface TransferSnapshot {
  transfer_id: string;
  direction: TransferDirection;
  filename: string;
  total_bytes: number;
  bytes_transferred: number;
  remaining_bytes: number;
  state: TransferState;
  allocated_bps: number;
  active_workers: number;
  completed_chunks: number;
  total_chunks: number;
  progress: number;
  measured: MeasuredMetrics;
  estimated: EstimatedMetrics;
  sha256: string | null;
  error: string | null;
}

export interface LinkState {
  link_id: string;
  direction: "downlink" | "uplink" | "shared";
  capacity_bps: number;
  measured_throughput_bps: number;
  utilization: number;
  active_transfers: number;
  is_congested: boolean;
}

export interface MonitorHistoryPoint {
  timestamp: number;
  policy: PolicyName;
  data: {
    active_transfers: number;
    download_throughput_bps?: number;
    upload_throughput_bps?: number;
    aggregate_goodput_bps: number;
  };
}

export interface OptimizationTelemetry {
  optimization_status: string;
  solver_name: string;
  solve_time_ms: number;
  objective_value_s: number | null;
  fairness_fraction: number | null;
  mode?: string;
  predicted_completion_times_s: Record<string, number | null>;
  effective_capacities_bps: Record<string, number>;
  link_utilization: Record<string, number>;
  warnings?: string[];
}

export interface MonitorStatus {
  is_simulated: boolean;
  policy: PolicyName;
  download_throughput_bps: number;
  upload_throughput_bps: number;
  aggregate_goodput_bps: number;
  average_fct_s: number;
  downlink_capacity_bps: number;
  uplink_capacity_bps: number;
  links: LinkState[];
  transfers: TransferSnapshot[];
  decision_log: Array<{ timestamp: number; policy: PolicyName; messages: string[] }>;
  last_compute_time_ms: number;
  last_optimization?: OptimizationTelemetry | null;
  history: MonitorHistoryPoint[];
  compare_status?: CompareStatus;
}

export interface RecordedRun {
  run_id: string;
  timestamp: number;
  policy: PolicyName;
  direction: string;
  target: string;
  num_transfers: number;
  completed_transfers: number;
  avg_fct_s: number;
  sum_fct_s: number;
  p95_fct_s: number;
  makespan_s: number;
  aggregate_goodput_bps: number;
  jains_fairness: number;
}

export interface ScenarioInfo {
  name: string;
  description: string;
  num_transfers: number;
  num_links: number;
}

export interface PolicyExperimentMetrics {
  scenario: string;
  policy: PolicyName;
  avg_fct_s: number;
  sum_fct_s: number;
  p95_fct_s: number;
  makespan_s: number;
  aggregate_goodput_bps: number;
  jains_fairness: number;
  redundant_bytes: number;
  scheduler_overhead_ms_avg: number;
  replications_triggered: number;
  replications_won: number;
}

export interface ExperimentBundle {
  scenario: string;
  description: string;
  seed: number;
  policies: Record<PolicyName, PolicyExperimentMetrics>;
  timelines: Record<PolicyName, Array<{ time_s: number; aggregate_goodput_bps: number }>>;
}
