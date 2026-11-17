export type PolicyName = "independent" | "max_min" | "srpt" | "flowweave";
export type TransferDirection = "download" | "upload";
export type TransferState = "pending" | "active" | "paused" | "completed" | "failed" | "cancelled";

export interface MeasuredMetrics {
  bytes_acked: number;
  raw_bytes_received: number;
  redundant_bytes: number;
  instant_goodput_bps: number;
  rolling_goodput_bps: number;
  avg_goodput_bps: number;
  last_rtt_ms: number;
  retries: number;
  sample_count: number;
}

export interface EstimatedMetrics {
  ewma_goodput_bps: number;
  ewma_rtt_ms: number;
  predicted_remaining_time_s: number | null;
  estimated_bottleneck_bps: number;
  confidence: number;
}

export interface TransferSnapshot {
  transfer_id: string;
  direction: TransferDirection;
  filename: string;
  url: string;
  source: string;
  destination: string;
  total_bytes: number;
  bytes_transferred: number;
  remaining_bytes: number;
  state: TransferState;
  assigned_link: string;
  route: string[];
  allocated_bps: number;
  active_chunks: number;
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
  latency_ms: number;
  loss_rate: number;
  measured_throughput_bps: number;
  estimated_available_bps: number;
  active_transfers: number;
  congestion_ratio: number;
  utilization: number;
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
    allocations_bps: Record<string, number>;
  };
}

export interface StragglerAlert {
  transfer_id: string;
  chunk_id: string;
  original_link: string;
  replica_link: string | null;
  should_replicate: boolean;
  expected_saving_s: number;
  expected_cost_s: number;
  reason: string;
}

export interface DecisionLogEntry {
  timestamp: number;
  policy: PolicyName;
  messages: string[];
}

export interface MonitorStatus {
  is_simulated: boolean;
  policy: PolicyName;
  control_interval_s: number;
  download_throughput_bps: number;
  upload_throughput_bps: number;
  aggregate_goodput_bps: number;
  average_fct_s: number;
  downlink_capacity_bps: number;
  uplink_capacity_bps: number;
  scheduler_config: {
    policy: PolicyName;
    min_fair_share_fraction: number;
    srpt_exponent: number;
    starvation_aging_per_s: number;
    congestion_headroom: number;
  };
  links: LinkState[];
  transfers: TransferSnapshot[];
  straggler_alerts: StragglerAlert[];
  decision_log: DecisionLogEntry[];
  last_compute_time_ms: number;
  history: MonitorHistoryPoint[];
}

export interface DemoFileInfo {
  filename: string;
  size_bytes: number;
  sha256: string;
  download_url: string;
  supports_ranges: boolean;
  can_upload_sample: boolean;
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
  seed: number;
  is_simulated: boolean;
  num_transfers: number;
  completed_transfers: number;
  avg_fct_s: number;
  sum_fct_s: number;
  p95_fct_s: number;
  makespan_s: number;
  aggregate_goodput_bps: number;
  jains_fairness: number;
  redundant_bytes: number;
  redundant_ratio: number;
  scheduler_overhead_ms_total: number;
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
