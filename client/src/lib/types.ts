export interface SensorReading {
  sensor_name: string;
  industry: string;
  value: number;
  unit: string;
  sensor_type: string;
  timestamp: number;
  fault_active: boolean;
  source: string;
}

export interface SourceMetrics {
  name: string;
  display_name: string;
  status: "active" | "inactive" | "error" | "initializing" | "waiting";
  error: string | null;
  total_records: number;
  records_per_sec: number;
  avg_latency_ms: number;
  p95_latency_ms: number;
  avg_e2e_latency_ms: number;
  p95_e2e_latency_ms: number;
  seconds_since_last_write: number;
  config?: SinkConfig;
  buffer_len?: number;
}

export interface SinkConfig {
  rate_hz: number;
  batch_size: number;
  max_buffer_size: number;
  backpressure: string;
  retry_count: number;
  retry_delay_s: number;
}

export interface SensorConfig {
  name: string;
  sensor_type: string;
  unit: string;
  min_value: number;
  max_value: number;
  nominal_value: number;
  noise_std: number;
  drift_rate: number;
  anomaly_probability: number;
  anomaly_magnitude: number;
  update_frequency_hz: number;
  cyclic: boolean;
  cycle_period_seconds: number;
  cycle_amplitude: number;
}

export type SensorType =
  | "temperature"
  | "pressure"
  | "flow"
  | "vibration"
  | "level"
  | "speed"
  | "power"
  | "current"
  | "voltage"
  | "humidity"
  | "ph"
  | "conductivity"
  | "position"
  | "status";

export type IndustryType =
  | "mining"
  | "utilities"
  | "manufacturing"
  | "oil_gas"
  | "aerospace"
  | "space"
  | "water_wastewater"
  | "electric_power"
  | "automotive"
  | "chemical"
  | "food_beverage"
  | "pharmaceutical"
  | "data_center"
  | "smart_building"
  | "agriculture"
  | "renewable_energy";

export type StreamingSource =
  | "kafka"
  | "rabbitmq"
  | "kinesis"
  | "event_hub"
  | "pubsub"
  | "simulator";

export type PipelineType = "sss" | "zerobus" | "websocket_demo";

export type SssProcessingMode = "realtime" | "microbatch";

export type SssRtmSinkType = "lakebase" | "websocket" | "both";

export type SssMicrobatchSink = "delta" | "lakebase" | "websocket";

export type FrequencyMode = "global" | "per-sensor";

export type SssSource = "kafka" | "rabbitmq" | "kinesis" | "event_hub" | "pubsub" | "zerobus";

export type LakebaseConnectionMode = "autoscale" | "direct";

export interface LakebaseAutoscaleConfig {
  project_id: string;
  branch_id: string;
  database: string;
  /** @deprecated No longer used — Lakebase Autoscale now authenticates via OAuth. */
  secrets_scope?: string;
  /** @deprecated No longer used — Lakebase Autoscale now authenticates via OAuth. */
  client_id_key?: string;
  /** @deprecated No longer used — Lakebase Autoscale now authenticates via OAuth. */
  client_secret_key?: string;
}

export interface LakebaseConnectionConfig {
  host: string;
  port: string;
  database: string;
  user: string;
  password: string;
  pg_table: string;
}

export interface WebSocketSinkConfig {
  app_url: string;
}

export interface TieringConfig {
  hot: string;   // e.g. "1 hour"   — time before Postgres chunks tier to Delta
  drop: string;  // e.g. "7 days"   — time before Postgres chunks are dropped
  chunk: string; // e.g. "15 minutes" — ChronoTable partition interval
}

export const DEFAULT_TIERING: TieringConfig = {
  hot: "1 hour",
  drop: "7 days",
  chunk: "15 minutes",
};

/**
 * A LakeTS continuous rollup definition. ``bucket`` is a Postgres interval
 * (e.g. ``"1 minute"``) and ``aggregates`` is the set of aggregate functions
 * applied to the ``value`` column (e.g. ``["avg", "min", "max"]``).
 */
export interface RollupConfig {
  name: string;
  bucket: string;
  aggregates: string[];
}

/** Aggregate functions offered by the rollup editor. */
export const ROLLUP_AGGREGATES = ["avg", "min", "max", "sum", "count"] as const;
export type RollupAggregate = (typeof ROLLUP_AGGREGATES)[number];

/** Sensible starter rollup: per-minute avg/min/max of value. */
export const DEFAULT_ROLLUP: RollupConfig = {
  name: "sensor_1m",
  bucket: "1 minute",
  aggregates: ["avg", "min", "max"],
};

// ---------------------------------------------------------------------------
// One-click Lakebase + LakeTS setup (POST /api/setup/pipeline-lakebase)
// ---------------------------------------------------------------------------

export interface SetupPipelineLakebaseRequest {
  pipeline_id: string;
  /** When true, provision a brand-new Lakebase project named ``lb_project_id``. */
  create_new_project: boolean;
  /** New project name (create) or existing project id (use existing). */
  lb_project_id: string;
  lb_branch_id: string;
  lb_database: string;
  table_name: string;
  secret_scope?: string;
  industry?: string | null;
  /** Tier policy; ``null`` → plain Postgres table, no tiering. */
  tiering: TieringConfig | null;
  /** Drop-after retention interval (e.g. ``"30 days"``); ``null`` → no retention. */
  retention: string | null;
  rollups: RollupConfig[];
}

export type SetupStepStatus =
  | "ok"
  | "created"
  | "already_exists"
  | "skipped"
  | "failed"
  | "error";

export interface SetupStepResult {
  step: string;
  status: SetupStepStatus;
  error?: string;
  results?: unknown;
}

export interface SetupPipelineLakebaseResponse {
  success: boolean;
  /** Names of steps that failed (empty when the run fully succeeded). */
  failed_steps?: string[];
  /** Resolved project id (the created project id, or the existing one). */
  lb_project_id: string;
  steps: SetupStepResult[];
  error?: string;
}

export interface CustomMetric {
  name: string;
  expression: string;
}

export interface MetricsConfig {
  enabled: boolean;
  target_table: string;
  custom_metrics: CustomMetric[];
  zerobus_endpoint?: string;
  zerobus_workspace_url?: string;
  zerobus_client_id?: string;
  zerobus_client_secret?: string;
}

export interface SssConnectionConfig {
  kafka?: {
    bootstrap_servers: string;
    topic: string;
    consumer_group: string;
    security_protocol: string;
    service_credential?: string;
    advanced?: Record<string, string>;
    num_partitions?: number;
  };
  kinesis?: {
    stream_name: string;
    region: string;
    service_credential?: string;
    advanced?: Record<string, string>;
    num_shards?: number;
  };
  event_hub?: {
    connection_string: string;
    event_hub_name: string;
    service_credential?: string;
    advanced?: Record<string, string>;
    num_partitions?: number;
  };
  rabbitmq?: {
    host: string;
    port: string;
    queue: string;
    username: string;
    service_credential?: string;
    advanced?: Record<string, string>;
    num_consumers?: number;
  };
  pubsub?: {
    project_id: string;
    subscription: string;
    topic_id?: string;
    service_credential?: string;
    advanced?: Record<string, string>;
    max_streams?: number;
  };
  zerobus?: {
    endpoint: string;
    table_name: string;
    client_id: string;
    /** Deprecated: new UC-secret path reads the client_secret from the UC
     * secret `{catalog}.{schema}.app_sp_oauth` inside the notebook. */
    client_secret?: string;
    workspace_url: string;
    secret_scope?: string;
  };
}

export interface SssConfig {
  processing_mode: SssProcessingMode;
  source: SssSource | null;
  connection: SssConnectionConfig;
  sink_table: string;
  checkpoint_location: string;
  /**
   * Owning pipeline id. Baked into the generated notebook as a broadcast
   * so executors can tag the X-Pipeline-Id header on every ingest POST —
   * which is how the app routes writes to the pipeline's Lakebase.
   */
  pipeline_id?: string;
  rtm_sink?: SssRtmSinkType;
  microbatch_sinks?: SssMicrobatchSink[];
  lakebase_mode?: LakebaseConnectionMode;
  lakebase?: LakebaseConnectionConfig;
  lakebase_autoscale?: LakebaseAutoscaleConfig;
  websocket_sink?: WebSocketSinkConfig;
  lakebase_buffer_size?: number;
  ws_buffer_size?: number;
  /**
   * LakeTS tier policy applied to the Lakebase sink during provisioning.
   * Absent → plain Postgres table, no tiering.
   */
  tiering?: TieringConfig;
  /**
   * Aggregate metric names broadcast over SSE to the dashboard's
   * realtime KPI charts (e.g. ``["temperature_avg", "rpm_spike_rate"]``).
   * Empty/absent → no SSE delivery; charts fall back to Lakebase polling.
   */
  sse_metrics?: string[];
  metrics: MetricsConfig;
  cluster: ClusterConfig;
  spark_conf: Record<string, string>;
  notebook_override: string | null;
  secret_scope?: string;
}

export interface ZerobusConfig {
  endpoint: string;
  table_name: string;
  client_id: string;
  client_secret: string;
  workspace_url: string;
}

export interface ClusterConfig {
  workers: number;
  worker_node_type: string;
  driver_node_type: string;
  dbr_version: string;
  spot_instances: boolean;
  auto_terminate_min: number;
  spark_conf: Record<string, string>;
}

export type ClusterState =
  | "idle"
  | "creating"
  | "starting"
  | "running"
  | "submitting_job"
  | "job_running"
  | "error"
  | "terminated";

export type AlertStatus = "open" | "acknowledged" | "cleared";

export type AlertSeverity = "info" | "warn" | "crit";

export interface AlertRecord {
  id: string;
  fingerprint: string;
  industry: string | null;
  pipeline_id: string | null;
  rule_id: string;
  sensor_id: string;
  sensor_ids: string[];
  severity: AlertSeverity | string;
  status: AlertStatus | string;
  fault_code: string | null;
  value: number | null;
  threshold: number | null;
  opened_at: string | null;
  updated_at: string | null;
  acknowledged_at: string | null;
  acknowledged_by: string | null;
  cleared_at: string | null;
  metadata?: Record<string, unknown>;
}

export interface AlertListResponse {
  alerts: AlertRecord[];
}

/**
 * Genie message lifecycle. Only COMPLETED / FAILED / CANCELLED /
 * QUERY_RESULT_EXPIRED are terminal; every other value — including statuses
 * Genie adds later (mapped to UNKNOWN) — means "keep polling".
 */
export type GenieMessageStatus =
  | "SUBMITTED"
  | "FETCHING_METADATA"
  | "FILTERING_CONTEXT"
  | "ASKING_AI"
  | "PENDING_WAREHOUSE"
  | "EXECUTING_QUERY"
  | "IN_PROGRESS"
  | "COMPLETED"
  | "FAILED"
  | "CANCELLED"
  | "QUERY_RESULT_EXPIRED"
  | "UNKNOWN";

export type GenieAllowlistStatus = "matched" | "mismatch" | "manual_required";

export interface GenieTableAllowlist {
  status: GenieAllowlistStatus;
  verified: boolean;
  authorization_enforced: boolean;
  expected_tables: string[];
  space_tables: string[] | null;
  requirement: string;
  manual_confirmed?: boolean;
}

export interface GenieStatusResponse {
  enabled: boolean;
  permission: "CAN_VIEW";
  auth: "app_service_principal";
  reason?: string;
  authorization_enforced?: boolean;
  table_allowlist?: GenieTableAllowlist;
  capabilities?: {
    can_mutate_alerts: boolean;
  };
}

export interface GenieConversationIds {
  conversation_id: string;
  message_id: string;
}

export interface GenieMessageResponse {
  conversation_id: string;
  message_id: string;
  status: GenieMessageStatus | string;
  content: string | null;
  attachments: Array<Record<string, unknown>>;
  error: string | null;
}

/** Per-pipeline Genie (`/api/genie/pipelines/{setup_id}/{mode}/status`). */
export interface GeniePipelineStatus {
  available: boolean;
  space_id: string | null;
  reason?: string;
  /** Industry of the resolved pipeline (backend echoes it). */
  industry?: string;
  /** `false` when per-pipeline Genie is not offered for the pipeline's industry. */
  supported?: boolean;
}

/** Response of the admin backfill; only `space_id` is relied on, other fields are tolerated. */
export interface GeniePipelineProvisionResult {
  space_id?: string | null;
  [key: string]: unknown;
}

export interface GeniePipelineStarters {
  questions: string[];
}

export interface GeniePipelineAskRequest {
  question: string;
}

export interface GenieAttachmentQuery {
  sql: string;
  description: string | null;
}

/**
 * Per-pipeline Genie attachment, exactly as documented in
 * docs/genie-pipeline-api.md: all four keys are always present.
 */
export interface GeniePipelineAttachment {
  attachment_id: string;
  text: string | null;
  query: GenieAttachmentQuery | null;
  suggested_questions: string[];
}

/** Same envelope as {@link GenieMessageResponse}, with typed attachments. */
export interface GeniePipelineMessageResponse
  extends Omit<GenieMessageResponse, "attachments"> {
  attachments: GeniePipelineAttachment[];
}

export interface GenieResultColumn {
  name: string;
  type: string;
}

export interface GenieQueryResult {
  columns: GenieResultColumn[];
  rows: unknown[][];
  truncated: boolean;
}

export interface GenieAskRequest {
  industry: string;
  mode: string;
  question: string;
  asset_id?: string;
  component_id?: string;
  alert_id?: string;
}

export type AlertLifecycleEventType =
  | "alert_opened"
  | "alert_updated"
  | "alert_acknowledged"
  | "alert_cleared";

export interface AlertLifecycleEvent {
  type: AlertLifecycleEventType;
  alert_id: string;
  fingerprint: string | null;
  industry: string | null;
  pipeline_id: string | null;
  rule_id: string | null;
  sensor_id: string | null;
  sensor_ids: string[];
  severity: AlertSeverity | string | null;
  status: AlertStatus | string | null;
  fault_code: string | null;
  value: number | null;
  threshold: number | null;
  opened_at: string | null;
  updated_at: string | null;
  acknowledged_at: string | null;
  acknowledged_by: string | null;
  cleared_at: string | null;
  sensor_type?: string;
  timestamp?: string;
}

/** Operator decisions recorded against an alert episode (never machine commands). */
export type AlertResponseAction = "hold_batch" | "send_to_inspect" | "request_maintenance";

export interface AlertResponseRecord {
  id: string;
  alert_id: string;
  episode_opened_at: string;
  action: AlertResponseAction;
  station: "PACK" | "INSPECT" | "MILL" | null;
  recorded_by: string;
  reference: {
    station?: string;
    parts_since?: string;
    asset?: string;
    component?: string;
    last_service?: { event_id: string; kind: string; notes: string; started_at: string };
  };
  created_at: string;
}

export interface AlertResponseEvent {
  type: "alert_response";
  industry: string | null;
  alert_id: string;
  response: AlertResponseRecord;
  created?: boolean;
  /** Set when the decision also stopped the cell (send_to_inspect). */
  production?: ProductionState;
  production_error?: string;
}

export type ProductionStateValue = "running" | "stopped";

/** A cell's production state; `changed_at` is null until the first stop. */
export interface ProductionState {
  id?: string;
  industry: string;
  state: ProductionStateValue;
  /** Machines the change applies to (sensor-name prefixes, e.g. `cnc_1`). */
  assets: string[];
  reason: string | null;
  alert_id: string | null;
  changed_by: string | null;
  changed_at: string | null;
}

export interface ProductionStateEvent {
  type: "production_state";
  industry: string;
  production: ProductionState;
}

export interface AlertEpisodeDecision {
  action: AlertResponseAction;
  station: "PACK" | "INSPECT" | "MILL" | null;
  recorded_by: string;
  created_at: string;
}

/** One open → cleared run of an alert rule, rebuilt from the event log. */
export interface AlertEpisode {
  alert_id: string;
  episode_opened_at: string;
  rule_id: string | null;
  sensor_id: string | null;
  severity: AlertSeverity | string | null;
  fault_code: string | null;
  threshold: number | null;
  peak_value: number | null;
  status: AlertStatus;
  acknowledged_at: string | null;
  acknowledged_by: string | null;
  cleared_at: string | null;
  last_event_at: string | null;
  decisions: AlertEpisodeDecision[];
}

export interface AlertHistoryResponse {
  industry: string;
  since: string;
  episodes: AlertEpisode[];
  production: ProductionState[];
}

export interface AlertResponseList {
  alert_id: string;
  episode_opened_at: string | null;
  responses: AlertResponseRecord[];
}

export interface SensorUpdateMessage {
  type: "sensor_update";
  source?: string;
  sensors?: SensorReading[];
  write_latency_ms?: number;
  e2e_latency_ms?: number;
}

export interface SourceStatusMessage {
  type: "source_status";
  sources?: Record<string, SourceMetrics>;
}

export interface ClusterStatusMessage {
  type: "cluster_status";
}

export type WsMessage =
  | SensorUpdateMessage
  | SourceStatusMessage
  | ClusterStatusMessage
  | AlertLifecycleEvent
  | AlertResponseEvent
  | ProductionStateEvent;

export type ProvisionStatus = "idle" | "loading" | "success" | "error";

export type PipelineStatus =
  | "draft"
  | "step1_complete"
  | "step2_complete"
  | "step3_complete"
  | "active"
  | "error"
  | "deleted";

export interface PipelineResources {
  job_id?: string;
  cluster_id?: string;
  uc_catalog?: string;
  uc_schema?: string;
  lakebase_table?: string;
  notebook_path?: string;
  checkpoint_volume?: string;
  catalog_schemas?: string;
  secret_scope?: string;
}

export type DashboardDataMode = "websocket" | "lakebase" | "hybrid" | "delta" | "delta_hybrid";

export interface PipelineHistoryRow {
  sensor_name: string;
  sensor_type: string;
  industry: string;
  hour: string;
  avg_value: string;
  min_value: string;
  max_value: string;
  cnt: string;
}

export interface PipelineHistoryResponse {
  columns: string[];
  data: PipelineHistoryRow[];
  row_count: number;
}

export interface StreamingMetricRow {
  query_id: string;
  query_name: string;
  batch_id: string;
  timestamp: string;
  num_input_rows: string;
  input_rows_per_second: string;
  processed_rows_per_second: string;
  batch_duration_ms: string;
  add_batch_ms: string;
  latest_offset_ms: string;
  wal_commit_ms: string;
  num_output_rows: string;
  e2e_p50_ms: string;
  e2e_p99_ms: string;
  processing_p50_ms: string;
  queuing_p50_ms: string;
  observed_metrics: string | null;
  progress: string;
}

export interface StreamingMetricsSummary {
  total_rows: number;
  last_write: string | null;
  last_batch: number;
  avg_input_rps: number;
  avg_processed_rps: number;
  avg_batch_duration_ms: number;
  total_input_rows: number;
  avg_e2e_p50_ms: number;
  avg_e2e_p99_ms: number;
  avg_processing_p50_ms: number;
  avg_processing_p99_ms: number;
  avg_queuing_p50_ms: number;
  avg_queuing_p99_ms: number;
}

export interface StreamingMetricsResponse {
  table: string;
  summary: StreamingMetricsSummary;
  rows: StreamingMetricRow[];
}

export interface DbsqlTimeseriesResponse {
  source: string;
  /** Streaming mode this series was queried for (realtime/microbatch/zerobus). */
  mode?: string;
  /** Fully-qualified per-mode table the series was read from. */
  table?: string;
  resolution: string;
  minutes: number;
  count: number;
  /** Latest cold-tier event timestamp from DBSQL; null if unavailable. */
  latest_event_at?: string | null;
  rows: Record<string, unknown>[];
}

/** Single point of the Lakebase rows/sec time-series. */
export interface LakebasePerfPoint {
  t: string | null;
  rows_per_sec: number;
}

/**
 * Write-performance readouts for a pipeline's Lakebase (hot tier).
 * Returned by ``GET /api/pipelines/{id}/lakebase-perf?minutes=N``.
 */
export interface LakebasePerfResponse {
  rows_per_sec: number;
  rows_per_min: number;
  /** Ingest lag p50 in ms; ``null`` when it cannot be computed. */
  p50_insert_lag_ms: number | null;
  /** Ingest lag p95 in ms; ``null`` when it cannot be computed. */
  p95_insert_lag_ms: number | null;
  /**
   * Backend-provided human-readable description of how ingest lag is measured.
   * Displayed verbatim in the UI when present.
   */
  lag_basis?: string;
  series: LakebasePerfPoint[];
}

/**
 * Single point of the Zerobus records/min time-series.
 * Returned in ``ZerobusMetricsResponse.series``.
 */
export interface ZerobusMetricsPoint {
  t: string | null;
  records_per_min: number;
}

export interface ZerobusLatestReading {
  sensor_name: string;
  sensor_type: string | null;
  /** DBSQL JSON may encode numeric/boolean columns as strings. */
  value: number | string;
  unit: string | null;
  fault_active: boolean | number | string;
  event_time: string | null;
}

/**
 * Zerobus → Delta ingest metrics for a pipeline.
 * Returned by ``GET /api/pipelines/{id}/zerobus-metrics?minutes=N``.
 * Zerobus writes directly to a Delta table (no message bus, no Lakebase),
 * so these readouts describe the gRPC ingest stream and the Delta target.
 */
export interface ZerobusMetricsResponse {
  table_name: string;
  total_records: number;
  total_bytes: number;
  records_per_sec: number;
  records_per_min: number;
  last_commit_time: string | null;
  /** Ingest stream state. */
  stream_status: "open" | "closed" | "none";
  error_count: number;
  series: ZerobusMetricsPoint[];
  recent_errors: string[];
  /** Latest Delta row per sensor, used to refresh the industry twin. */
  latest_readings: ZerobusLatestReading[];
}

/**
 * One point of the streaming-latency sparkline series (oldest → newest).
 * Returned in ``StreamingLatencyResponse.series``.
 */
export interface StreamingLatencyPoint {
  batch_id: number | null;
  timestamp: string | null;
  e2e_p50: number | null;
  e2e_p99: number | null;
  processing_p50: number | null;
  processing_p99: number | null;
  input_rps: number | null;
  processed_rps: number | null;
  trigger_ms: number | null;
}

/**
 * Latest latency + throughput KPIs from ``streaming_metrics_<mode>``.
 * Returned by ``GET /api/latency/streaming?industry=&mode=realtime|microbatch``.
 * All KPI fields are ``null`` when the metrics table isn't provisioned or holds
 * no rows in the window (``has_data:false`` + a ``reason``).
 */
export interface StreamingLatencyResponse {
  table: string;
  mode: string;
  has_data: boolean;
  last_batch_id: number | null;
  last_timestamp: string | null;
  num_input_rows: number | null;
  input_rps: number | null;
  processed_rps: number | null;
  trigger_ms: number | null;
  e2e_p50: number | null;
  e2e_p99: number | null;
  processing_p50: number | null;
  processing_p99: number | null;
  queuing_p50: number | null;
  queuing_p99: number | null;
  series: StreamingLatencyPoint[];
  reason?: string;
}

/**
 * Zerobus ingest latency/health KPIs for a pipeline's Delta target.
 * Returned by ``GET /api/latency/zerobus?industry=``. Zerobus is not a Spark
 * stream, so there are no p50/p99 percentiles — commit freshness
 * (``seconds_since_commit``) + throughput + error count are the signals.
 */
export interface ZerobusLatencyResponse {
  table_name: string;
  has_data: boolean;
  last_commit_time: string | null;
  seconds_since_commit: number | null;
  committed_records_total: number;
  committed_bytes_total: number;
  records_per_min: number;
  recent_error_count: number;
  recent_errors: string[];
  reason?: string;
}

export type CdfStatusValue =
  | "NONE"
  | "SNAPSHOTTING"
  | "STREAMING"
  | "UNKNOWN";

/**
 * Lakebase CDF (Lakehouse Sync) replication status for a pipeline.
 * Returned by ``GET /api/pipelines/{id}/cdf-status``.
 */
export interface CdfStatusResponse {
  enabled: boolean;
  streaming: boolean;
  status: CdfStatusValue;
  reason: string;
  /** Latest hot-tier event timestamp read directly from Lakebase. */
  hot_latest_at: string | null;
}

export interface PipelineCatalogResponse {
  catalog: string;
  schemas: string[];
  volume: string;
  status: string;
}

export interface Pipeline {
  id: string;
  user_email: string;
  name: string;
  status: PipelineStatus;
  industry: string | null;
  sensor_config: Record<string, unknown>[];
  source_type: string | null;
  source_config: Record<string, unknown>;
  sink_config: Record<string, unknown>;
  resources: PipelineResources;
  created_at: string;
  updated_at: string;
}

export interface LakebaseProvisionParams {
  mode: LakebaseConnectionMode;
  table_name: string;
  project_id?: string;
  branch_id?: string;
  database?: string;
  host?: string;
  port?: number;
  user?: string;
  password?: string;
}

export type CloudProvider = "aws" | "azure" | "gcp" | "unknown";

export interface NodeType {
  node_type_id: string;
  num_cores: number;
  memory_mb: number;
  num_gpus: number;
  category: string;
  description: string;
  is_deprecated: boolean;
  storage_label: string;
}

export interface NodeTypesResponse {
  cloud: CloudProvider;
  node_types: NodeType[];
}

export interface SparkVersion {
  key: string;
  name: string;
}

export interface SparkVersionsResponse {
  spark_versions: SparkVersion[];
}

export interface ClusterProvisionRequest {
  cluster_name?: string;
  num_workers: number;
  worker_node_type: string;
  spark_version: string;
  auto_terminate_min: number;
  spark_conf: Record<string, string>;
}

export interface ClusterProvisionResponse {
  cluster_id: string;
  state: string;
}

export interface ClusterStatusResponse {
  cluster_id: string;
  state: string;
  state_message: string;
}

export interface ValidateResponse {
  success: boolean;
  status: string;
  output: string;
  error?: string;
  notebook_path?: string;
}

export interface DeployJobRequest {
  notebook: string;
  producer_notebook?: string;
  source_type?: string;
  cluster_config: {
    spark_version: string;
    worker_node_type: string;
    num_workers: number;
    spark_conf: Record<string, string>;
  };
  job_id?: number;
}

export interface DeployJobResponse {
  job_id: number;
  job_url: string;
  notebook_path?: string;
}

export interface TaskStatus {
  state: string;
  run_id: number | null;
  state_message: string;
}

export interface JobStatusResponse {
  state: string;
  result_state?: string | null;
  run_id: number | null;
  state_message: string;
  tasks?: Record<string, TaskStatus>;
}

export interface DataProducerState {
  running: boolean;
  source_type: SssSource | null;
  source_label: string | null;
  destination: string | null;
  run_id: number | null;
  state: "IDLE" | "STARTING" | "RUNNING" | "STOPPING" | "FAILED";
  error: string | null;
}

export interface SimulatorStatusResponse {
  industries: string[];
  running: boolean;
}

// ---------------------------------------------------------------------------
// Settings Validation & Provisioning
// ---------------------------------------------------------------------------
export interface SettingsCheckResult {
  name: string;
  status: "pass" | "fail" | "skip" | "warn";
  detail: string;
}

export interface SettingsSectionResult {
  status: "pass" | "fail" | "warn";
  checks: SettingsCheckResult[];
}

export interface SettingsValidationResult {
  uc: SettingsSectionResult;
  lakebase: SettingsSectionResult;
  secrets: SettingsSectionResult;
  cluster: SettingsSectionResult;
}

export interface ProvisionedResource {
  resource: string;
  status: "created" | "already_exists" | "skipped" | "failed";
  error?: string;
}

export interface SettingsProvisionResult {
  provisioned: ProvisionedResource[];
  validation: SettingsValidationResult;
}

// ---------------------------------------------------------------------------
// Global User Settings
// ---------------------------------------------------------------------------
export type CatalogIsolationMode = "per-pipeline" | "shared";

export interface UcSettings {
  catalog_pattern: string;
  schemas: string[];
  checkpoint_volume: string;
  storage_root: string;
  catalog_isolation_mode: CatalogIsolationMode;
}

export interface LakebaseCredentialInfo {
  pg_role: string;
  secret_scope: string;
  user_key: string;
  password_key: string;
}

export interface SectionProvisionResult {
  provisioned: ProvisionedResource[];
  validation: SettingsSectionResult;
  credentials?: LakebaseCredentialInfo;
}

export interface LakebaseSettings {
  mode: LakebaseConnectionMode;
  project_id: string;
  branch_id: string;
  database: string;
  pg_schema: string;
  pg_table: string;
  host: string;
  port: string;
}

export interface SecretsSettings {
  scope: string;
  lb_user_key: string;
  lb_password_key: string;
  zb_client_id_key: string;
  zb_client_secret_key: string;
}

export interface ClusterSettings {
  workers: number;
  worker_node_type: string;
  dbr_version: string;
  auto_terminate_min: number;
}

export interface WarehouseSettings {
  warehouse_id: string;
  warehouse_name: string;
}

export interface UserSettings {
  uc: UcSettings;
  lakebase: LakebaseSettings;
  secrets: SecretsSettings;
  cluster: ClusterSettings;
  warehouse: WarehouseSettings;
}

// ---------------------------------------------------------------------------
// SQL Warehouse
// ---------------------------------------------------------------------------
export interface WarehouseInfo {
  id: string;
  name: string;
  cluster_size: string;
  state: string;
  serverless: boolean;
  auto_stop_mins: number;
}

// ---------------------------------------------------------------------------
// Source Availability & Streaming Events
// ---------------------------------------------------------------------------
export interface SourceAvailabilityResult {
  available: boolean;
  claimed_by_pipeline?: string;
  claimed_by?: string;
}

export interface ThresholdAlertEvent {
  type: "threshold_alert";
  pipeline_id: string;
  sensor_id: string;
  sensor_type: string;
  value: number;
  threshold_min?: number;
  threshold_max?: number;
  timestamp: string;
}

export interface SessionEvent {
  pipeline_id: string;
  sensor_id: string;
  sensor_type: string;
  session_id: string;
  session_start: string;
  session_end: string;
  duration_sec: number;
  reading_count: number;
  avg_value: number;
  min_value: number;
  max_value: number;
}

// ---------------------------------------------------------------------------
// Settings Status (existing resource state)
// ---------------------------------------------------------------------------
export interface SettingsStatus {
  uc: {
    catalogs: Array<{ name: string; schemas: string[] }>;
    error?: string;
  };
  lakebase: {
    project_id: string | null;
    branch_id: string | null;
    endpoint_host: string | null;
    tables: string[];
    role_exists: boolean;
    role_name: string | null;
    error?: string;
  };
  secrets: {
    scope_exists: boolean;
    scope_name: string | null;
    keys: string[];
    error?: string;
  };
  warehouse: {
    warehouses: WarehouseInfo[];
    error?: string;
  };
}
