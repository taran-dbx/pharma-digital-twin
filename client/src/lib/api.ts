import type {
  AlertHistoryResponse,
  AlertLifecycleEvent,
  AlertListResponse,
  AlertRecord,
  AlertResponseAction,
  AlertResponseEvent,
  AlertResponseList,
  GenieAskRequest,
  GenieConversationIds,
  GenieMessageResponse,
  GeniePipelineAskRequest,
  GeniePipelineMessageResponse,
  GeniePipelineProvisionResult,
  GeniePipelineStarters,
  GeniePipelineStatus,
  GenieQueryResult,
  GenieStatusResponse,
  LakebaseProvisionParams,
  NodeTypesResponse,
  SparkVersionsResponse,
  ClusterProvisionRequest,
  ClusterProvisionResponse,
  ClusterStatusResponse,
  ValidateResponse,
  DeployJobRequest,
  DeployJobResponse,
  JobStatusResponse,
  Pipeline,
  PipelineCatalogResponse,
  PipelineHistoryResponse,
  DataProducerState,
  UserSettings,
  UcSettings,
  LakebaseSettings,
  SecretsSettings,
  SettingsValidationResult,
  SettingsProvisionResult,
  SectionProvisionResult,
  SettingsStatus,
  WarehouseInfo,
  StreamingMetricsResponse,
  DbsqlTimeseriesResponse,
  SourceAvailabilityResult,
  LakebasePerfResponse,
  CdfStatusResponse,
  ZerobusMetricsResponse,
  StreamingLatencyResponse,
  ZerobusLatencyResponse,
  SetupPipelineLakebaseRequest,
  SetupPipelineLakebaseResponse,
  ProductionState,
  ProductionStateValue,
} from "@/lib/types";
import type { PipelineMode } from "@/lib/industries-api";

const BASE_URL = "/api";

/** Error thrown by {@link request} for non-2xx responses; carries the HTTP status. */
export class ApiError extends Error {
  readonly status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

export function isApiStatus(err: unknown, status: number): boolean {
  return err instanceof ApiError && err.status === status;
}

function errorDetail(body: unknown, status: number): string {
  if (body && typeof body === "object") {
    const { error, detail } = body as { error?: unknown; detail?: unknown };
    if (typeof error === "string" && error) return error;
    if (typeof detail === "string" && detail) return detail;
  }
  return `Request failed: ${status}`;
}

async function request<T>(
  endpoint: string,
  options?: RequestInit
): Promise<T> {
  const res = await fetch(`${BASE_URL}${endpoint}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...options?.headers,
    },
    credentials: "include",
  });

  if (!res.ok) {
    const body: unknown = await res.json().catch(() => ({}));
    throw new ApiError(errorDetail(body, res.status), res.status);
  }

  return res.json();
}

export interface IndustryInfo {
  id: string;
  label: string;
  sensor_count: number;
}

export interface SimulatorStatus {
  industries: string[];
  running: boolean;
}

export interface KafkaProducerStatus {
  running: boolean;
  pipeline_id: string | null;
  topic: string | null;
  run_id: number | null;
  state: "IDLE" | "STARTING" | "RUNNING" | "STOPPING" | "FAILED";
  error: string | null;
}

export interface AppDefaults {
  zerobus_endpoint: string;
  zerobus_client_id: string;
  zerobus_client_secret?: string;
  workspace_url: string;
  /** DATABRICKS_SQL_WAREHOUSE_ID — used by validation DBSQL and UC checks. */
  warehouse_id?: string;
  // Lakebase defaults — parsed from LAKEBASE_ENDPOINT on the server.
  // Used to pre-populate the autoscale form in new pipelines.
  lakebase_endpoint?: string;
  lakebase_project_id?: string;
  lakebase_branch_id?: string;
  lakebase_database?: string;
  // Workspace-specific values for the TS notebook generators.
  // All come from the backend (env vars in app.yaml) — never hardcode these.
  service_credential?: string;      // UC_SERVICE_CREDENTIAL — MSK-IAM credential name
  pipelines_project?: string;       // PIPELINES_LAKEBASE_PROJECT — streaming Lakebase project
  storage_base?: string;            // UC_STORAGE_BASE — S3 base for external metrics tables
  secret_scope?: string;            // ZEROBUS_SECRET_SCOPE — secret scope for Zerobus creds
  msk_bootstrap?: string;           // MSK_BOOTSTRAP_SERVERS — default MSK bootstrap fallback
  /** UC_CATALOG — pre-existing catalog for the one-schema industry layout (Task 3/4). */
  uc_catalog?: string;
  /** UC_SCHEMA — single schema holding all industry-prefixed managed tables (Task 3/4). */
  uc_schema?: string;
}

export const api = {
  getHealth: () => request<Record<string, unknown>>("/health"),
  getAppDefaults: () => request<AppDefaults>("/app-defaults"),
  getSources: () => request<Record<string, unknown>>("/sources"),
  getTimeseries: (params: { minutes?: number; source?: string; industry?: string | null; sensor_type?: string; pipelineId?: string }) => {
    const qs = new URLSearchParams();
    if (params.minutes) qs.set("minutes", String(params.minutes));
    if (params.sensor_type) qs.set("sensor_type", params.sensor_type);
    if (params.pipelineId) {
      return request<{ resolution: string; minutes: number; count: number; rows: Record<string, unknown>[] }>(
        `/pipelines/${params.pipelineId}/timeseries?${qs}`,
      );
    }
    // Legacy workspace-scoped fallback
    if (params.source) qs.set("source", params.source);
    if (params.industry) qs.set("industry", params.industry);
    return request<{ resolution: string; minutes: number; count: number; rows: Record<string, unknown>[] }>(`/timeseries?${qs}`);
  },
  getPipelineStats: (params: { minutes?: number; industry?: string | null; pipelineId?: string }) => {
    const qs = new URLSearchParams();
    if (params.minutes) qs.set("minutes", String(params.minutes));
    if (params.pipelineId) {
      return request<{
        total_records: number;
        records_per_min: number;
        latest_event: string | null;
        oldest_event: string | null;
        freshness_sec: number | null;
      }>(`/pipelines/${params.pipelineId}/stats?${qs}`);
    }
    if (params.industry) qs.set("industry", params.industry);
    return request<{
      total_records: number;
      records_per_min: number;
      latest_event: string | null;
      oldest_event: string | null;
      freshness_sec: number | null;
    }>(`/pipeline-stats?${qs}`);
  },
  updateSourceConfig: (name: string, config: Record<string, unknown>) =>
    request(`/sources/${name}/config`, {
      method: "PUT",
      body: JSON.stringify(config),
    }),
  getIndustries: () => request<IndustryInfo[]>("/industries"),
  getSimulatorStatus: () => request<SimulatorStatus>("/simulator/status"),
  restartSimulator: (industries: string[]) =>
    request<{ industries: string[]; status: string }>("/simulator/restart", {
      method: "POST",
      body: JSON.stringify({ industries }),
    }),

  provisionUc: (catalog: string, schema: string, pipelineId?: string) =>
    request<{ catalog: string; schema: string; status: string }>("/provision/uc", {
      method: "POST",
      body: JSON.stringify({ catalog, schema, pipeline_id: pipelineId }),
    }),

  provisionPipelineCatalog: (pipelineId: string, pipelineName: string, catalogIsolationMode?: string) =>
    request<PipelineCatalogResponse>("/provision/pipeline-catalog", {
      method: "POST",
      body: JSON.stringify({
        pipeline_id: pipelineId,
        pipeline_name: pipelineName,
        ...(catalogIsolationMode ? { catalog_isolation_mode: catalogIsolationMode } : {}),
      }),
    }),

  provisionLakebaseTable: (params: LakebaseProvisionParams & { pipeline_id?: string }) =>
    request<{ table: string; database: string; status: string }>("/provision/lakebase-table", {
      method: "POST",
      body: JSON.stringify(params),
    }),

  /**
   * One-click Lakebase + LakeTS setup: optionally creates a fresh Lakebase
   * project, creates the (possibly tiered) table, configures rollups and
   * enables CDF/sync prep — returning a per-step result list.
   */
  setupPipelineLakebase: (body: SetupPipelineLakebaseRequest) =>
    request<SetupPipelineLakebaseResponse>("/setup/pipeline-lakebase", {
      method: "POST",
      body: JSON.stringify(body),
    }),

  getNodeTypes: () => request<NodeTypesResponse>("/cluster/node-types"),

  getSparkVersions: () => request<SparkVersionsResponse>("/cluster/spark-versions"),

  provisionCluster: (config: ClusterProvisionRequest & { pipeline_id?: string }) =>
    request<ClusterProvisionResponse>("/cluster/provision", {
      method: "POST",
      body: JSON.stringify(config),
    }),

  getClusterStatus: (clusterId: string) =>
    request<ClusterStatusResponse>(`/cluster/${clusterId}/status`),

  validateOnCluster: (clusterId: string, notebook: string, timeout?: number) =>
    request<ValidateResponse>(`/cluster/${clusterId}/validate`, {
      method: "POST",
      body: JSON.stringify({ notebook, ...(timeout ? { timeout } : {}) }),
    }),

  startCluster: (clusterId: string) =>
    request<{ cluster_id: string; state: string }>(`/cluster/${clusterId}/start`, {
      method: "POST",
    }),

  runOnCluster: (clusterId: string, notebook: string) =>
    request<ValidateResponse>(`/cluster/${clusterId}/validate`, {
      method: "POST",
      body: JSON.stringify({ notebook, timeout: 600 }),
    }),

  testSourceConnectivity: (params: {
    source_type: string;
    connection: Record<string, string | undefined>;
    num_records?: number;
  }) =>
    request<{
      success: boolean;
      message?: string;
      records_sent?: number;
      error?: string;
      suggestion?: string;
    }>("/test-source", {
      method: "POST",
      body: JSON.stringify(params),
    }),

  uploadSimulatorWheel: () =>
    request<{ wheel_path: string }>("/test-connectivity/upload-wheel", { method: "POST" }),

  submitConnectivityTest: (params: { notebook: string; wheel_path: string }) =>
    request<{ run_id: number; notebook_path: string }>("/test-connectivity/submit", {
      method: "POST",
      body: JSON.stringify(params),
    }),

  getConnectivityTestStatus: (runId: number) =>
    request<{
      state: "PENDING" | "QUEUED" | "RUNNING" | "COMPLETED" | "FAILED";
      state_message?: string;
      result?: {
        success: boolean;
        source: string;
        write_test?: string;
        read_test?: string;
        message?: string;
        error?: string;
        hint?: string;
        phase?: string;
        raw?: string;
        diagnostics?: {
          dns_ok?: boolean;
          resolved_ips?: string[];
          is_private_ip?: boolean;
          tcp_reachable?: boolean;
          ssl_ok?: boolean | null;
          checks?: Array<{ name: string; ok: boolean; detail: string }>;
        };
      };
      error?: string;
    }>(`/test-connectivity/${runId}/status`),

  deployJob: (params: DeployJobRequest & { pipeline_id?: string }) =>
    request<DeployJobResponse>("/cluster/deploy-job", {
      method: "POST",
      body: JSON.stringify(params),
    }),

  getJobStatus: (jobId: number) =>
    request<JobStatusResponse>(`/job/${jobId}/status`),

  stopJob: (jobId: number) =>
    request<{ stopped: boolean }>(`/job/${jobId}/stop`, { method: "POST" }),

  restartJob: (jobId: number) =>
    request<{ restarted: boolean; run_id: number }>(`/job/${jobId}/restart`, { method: "POST" }),

  getUserSettings: () => request<Partial<UserSettings>>("/settings"),
  saveUserSettings: (settings: UserSettings) =>
    request<UserSettings>("/settings", {
      method: "PUT",
      body: JSON.stringify(settings),
    }),
  validateSettings: (settings: UserSettings) =>
    request<SettingsValidationResult>("/settings/validate", {
      method: "POST",
      body: JSON.stringify(settings),
    }),
  provisionSettings: (settings: UserSettings) =>
    request<SettingsProvisionResult>("/settings/provision", {
      method: "POST",
      body: JSON.stringify(settings),
    }),
  provisionUcSettings: (uc: UcSettings) =>
    request<SectionProvisionResult>("/settings/provision/uc", {
      method: "POST",
      body: JSON.stringify({ uc }),
    }),
  provisionLakebaseSettings: (params: {
    lakebase: LakebaseSettings;
    secrets?: SecretsSettings;
    lb_user?: string;
    lb_password?: string;
  }) =>
    request<SectionProvisionResult>("/settings/provision/lakebase", {
      method: "POST",
      body: JSON.stringify(params),
    }),
  provisionSecretsSettings: (secrets: SecretsSettings) =>
    request<SectionProvisionResult>("/settings/provision/secrets", {
      method: "POST",
      body: JSON.stringify({ secrets }),
    }),
  getSettingsStatus: () => request<SettingsStatus>("/settings/status"),

  // SQL Warehouses
  listWarehouses: () => request<WarehouseInfo[]>("/warehouses"),
  createWarehouse: (params: { name: string; cluster_size: string; auto_stop_mins: number }) =>
    request<WarehouseInfo>("/warehouses", {
      method: "POST",
      body: JSON.stringify(params),
    }),

  getMe: () => request<{ email: string }>("/me"),

  listPipelines: () => request<Pipeline[]>("/pipelines"),

  createPipeline: (data: { name: string; industry?: string }) =>
    request<Pipeline>("/pipelines", {
      method: "POST",
      body: JSON.stringify(data),
    }),

  getPipeline: (id: string) => request<Pipeline>(`/pipelines/${id}`),

  updatePipeline: (id: string, data: Partial<Pipeline>) =>
    request<Pipeline>(`/pipelines/${id}`, {
      method: "PUT",
      body: JSON.stringify(data),
    }),

  deletePipeline: (id: string) =>
    request<{ deleted: boolean; cleanup_log: string[] }>(`/pipelines/${id}`, {
      method: "DELETE",
    }),

  getPipelineHistory: (pipelineId: string, days = 90) =>
    request<PipelineHistoryResponse>(`/pipeline/${pipelineId}/history?days=${days}`),

  putPipelineSecrets: (pipelineId: string, secrets: Record<string, string>) =>
    request<{ scope: string; stored_keys: string[] }>(`/pipelines/${pipelineId}/secrets`, {
      method: "POST",
      body: JSON.stringify({ secrets }),
    }),

  listPipelineSecretKeys: (pipelineId: string) =>
    request<{ scope: string | null; keys: string[] }>(`/pipelines/${pipelineId}/secrets/keys`),

  // Per-pipeline data producer endpoints
  getProducerStatus: (pipelineId: string) =>
    request<DataProducerState>(`/pipelines/${pipelineId}/producer/status`),

  startProducer: (
    pipelineId: string,
    notebook: string,
    sourceType: string,
    destination: string,
  ) =>
    request<DataProducerState>(`/pipelines/${pipelineId}/producer/start`, {
      method: "POST",
      body: JSON.stringify({ notebook, source_type: sourceType, destination }),
    }),

  stopProducer: (pipelineId: string) =>
    request<DataProducerState>(`/pipelines/${pipelineId}/producer/stop`, {
      method: "POST",
    }),

  // Legacy kafka-producer endpoints (backward compat)
  getKafkaProducerStatus: () =>
    request<KafkaProducerStatus>("/kafka-producer/status"),

  startKafkaProducer: (pipelineId: string, notebook: string, topic: string) =>
    request<KafkaProducerStatus>("/kafka-producer/start", {
      method: "POST",
      body: JSON.stringify({ pipeline_id: pipelineId, notebook, topic }),
    }),

  stopKafkaProducer: () =>
    request<KafkaProducerStatus>("/kafka-producer/stop", { method: "POST" }),

  resetPipeline: (id: string) =>
    request<{ pipeline: Pipeline; cleanup_log: string[] }>(`/pipelines/${id}/reset`, {
      method: "POST",
    }),

  clonePipeline: (id: string, name: string) =>
    request<Pipeline>(`/pipelines/${id}/clone`, {
      method: "POST",
      body: JSON.stringify({ name }),
    }),

  getStreamingMetrics: (
    pipelineId: string,
    industry?: string | null,
    mode?: string | null,
  ) => {
    const params = new URLSearchParams();
    if (industry) params.set("industry", industry);
    if (mode) params.set("mode", mode);
    const qs = params.toString();
    return request<StreamingMetricsResponse>(
      `/pipeline/${pipelineId}/streaming-metrics${qs ? `?${qs}` : ""}`,
    );
  },

  /**
   * Cold-tier (Delta) time-series for a pipeline. `mode` selects the
   * per-mode table on the backend (realtime -> the CDF-synced Lakebase
   * mirror, microbatch -> `microbatch.sensor_data`, zerobus ->
   * `zerobus.iot_sensor_data`); omitted, the backend infers it from the
   * pipeline's own source config.
   */
  getDbsqlTimeseries: (
    pipelineId: string,
    minutes: number,
    mode?: PipelineMode,
    industry?: string,
  ) =>
    request<DbsqlTimeseriesResponse>(
      `/pipeline/${pipelineId}/dbsql-timeseries?minutes=${minutes}` +
        `${mode ? `&mode=${mode}` : ""}${industry ? `&industry=${encodeURIComponent(industry)}` : ""}`,
    ),

  getLakebasePerf: (pipelineId: string, minutes = 5, industry?: string) =>
    request<LakebasePerfResponse>(
      `/pipelines/${pipelineId}/lakebase-perf?minutes=${minutes}` +
        `${industry ? `&industry=${encodeURIComponent(industry)}` : ""}`,
    ),

  getCdfStatus: (pipelineId: string, industry?: string) =>
    request<CdfStatusResponse>(
      `/pipelines/${pipelineId}/cdf-status` +
        `${industry ? `?industry=${encodeURIComponent(industry)}` : ""}`,
    ),

  /**
   * Zerobus → Delta ingest metrics. Only meaningful for Zerobus pipelines,
   * which write directly to a Delta table (no Lakebase, no SSE).
   */
  getZerobusMetrics: (pipelineId: string, minutes = 60, industry?: string) =>
    request<ZerobusMetricsResponse>(
      `/pipelines/${pipelineId}/zerobus-metrics?minutes=${minutes}` +
        `${industry ? `&industry=${encodeURIComponent(industry)}` : ""}`,
    ),

  /**
   * Latest latency + throughput KPIs from the per-mode streaming-metrics Delta
   * table (``streaming_metrics_<mode>``). Only meaningful for the Spark
   * structured-streaming modes (realtime / microbatch).
   */
  getStreamingLatency: (
    pipelineId: string,
    industry: string,
    mode: "realtime" | "microbatch",
  ) =>
    request<StreamingLatencyResponse>(
      `/latency/streaming?pipeline_id=${encodeURIComponent(pipelineId)}` +
        `&industry=${encodeURIComponent(industry)}&mode=${mode}`,
    ),

  /**
   * Zerobus ingest latency/health KPIs derived from
   * ``system.lakeflow.zerobus_ingest`` (commit freshness, records/min
   * throughput, committed bytes, recent errors).
   */
  getZerobusLatency: (pipelineId: string, industry: string) =>
    request<ZerobusLatencyResponse>(
      `/latency/zerobus?pipeline_id=${encodeURIComponent(pipelineId)}` +
        `&industry=${encodeURIComponent(industry)}`,
    ),

  getLakebaseHealth: (pipelineId: string, industry?: string) =>
    request<{
      rows_5m: number;
      latest_at: string | null;
      latency_sec: number | null;
      inserts_per_sec_recent: number;
      error?: string;
    }>(
      `/pipelines/${pipelineId}/lakebase-health` +
        `${industry ? `?industry=${encodeURIComponent(industry)}` : ""}`,
    ),

  getLaketsGapfill: (pipelineId: string, minutes: number, industry?: string) =>
    request<{ rows: Record<string, unknown>[]; reason?: string }>(
      `/pipelines/${pipelineId}/lakets/gapfill?minutes=${minutes}` +
        `${industry ? `&industry=${encodeURIComponent(industry)}` : ""}`,
    ),

  getLaketsRate: (pipelineId: string, minutes: number, industry?: string) =>
    request<{ rows: Record<string, unknown>[]; reason?: string }>(
      `/pipelines/${pipelineId}/lakets/rate?minutes=${minutes}` +
        `${industry ? `&industry=${encodeURIComponent(industry)}` : ""}`,
    ),

  getLaketsHistogram: (
    pipelineId: string,
    sensor: string,
    minutes: number,
    industry?: string,
  ) =>
    request<{ rows: Record<string, unknown>[]; reason?: string }>(
      `/pipelines/${pipelineId}/lakets/histogram?sensor=${encodeURIComponent(sensor)}&minutes=${minutes}` +
        `${industry ? `&industry=${encodeURIComponent(industry)}` : ""}`,
    ),

  healLakebase: (pipelineId: string, industry?: string) =>
    request<{
      pipeline_id?: string;
      industry?: string;
      endpoint: string;
      steps: Array<{ step: string; [k: string]: unknown }>;
    }>(
      `/pipelines/${pipelineId}/heal-lakebase` +
        `${industry ? `?industry=${encodeURIComponent(industry)}` : ""}`,
      { method: "POST" },
    ),

  checkSourceAvailability: (
    sourceType: string,
    sourceConfig: Record<string, unknown>,
  ): Promise<SourceAvailabilityResult> =>
    request("/pipelines/check-source", {
      method: "POST",
      body: JSON.stringify({ source_type: sourceType, source_config: sourceConfig }),
    }),

  listAlerts: (params?: { status?: string; industry?: string; severity?: string }) => {
    const qs = new URLSearchParams();
    if (params?.status) qs.set("status", params.status);
    if (params?.industry) qs.set("industry", params.industry);
    if (params?.severity) qs.set("severity", params.severity);
    const suffix = qs.toString();
    return request<AlertListResponse>(`/alerts${suffix ? `?${suffix}` : ""}`);
  },

  getAlert: (alertId: string) => request<AlertRecord>(`/alerts/${alertId}`),

  acknowledgeAlert: (alertId: string) =>
    request<AlertLifecycleEvent>(`/alerts/${alertId}/acknowledge`, { method: "POST" }),

  listAlertResponses: (alertId: string) =>
    request<AlertResponseList>(`/alerts/${alertId}/responses`),

  recordAlertResponse: (alertId: string, action: AlertResponseAction) =>
    request<AlertResponseEvent>(`/alerts/${alertId}/responses`, {
      method: "POST",
      body: JSON.stringify({ action }),
    }),

  getAlertHistory: (industry: string, hours = 1) =>
    request<AlertHistoryResponse>(
      `/alerts/history?industry=${encodeURIComponent(industry)}&hours=${hours}`,
    ),

  getProduction: (industry: string) =>
    request<ProductionState>(`/industries/${encodeURIComponent(industry)}/production`),

  setProduction: (
    industry: string,
    state: ProductionStateValue,
    context?: { reason?: string; alert_id?: string },
  ) =>
    request<ProductionState & { changed: boolean }>(
      `/industries/${encodeURIComponent(industry)}/production`,
      { method: "POST", body: JSON.stringify({ state, ...context }) },
    ),

  getGenieStatus: () => request<GenieStatusResponse>("/genie/status"),

  startGenieConversation: (body: GenieAskRequest) =>
    request<GenieConversationIds>("/genie/conversations", {
      method: "POST",
      body: JSON.stringify(body),
    }),

  addGenieMessage: (conversationId: string, body: GenieAskRequest) =>
    request<GenieConversationIds>(`/genie/conversations/${conversationId}/messages`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

  getGenieMessage: (conversationId: string, messageId: string) =>
    request<GenieMessageResponse>(
      `/genie/conversations/${conversationId}/messages/${messageId}`,
    ),

  // ── Per-pipeline Genie (one space per setup × mode) ──────────────────────
  getPipelineGenieStatus: (setupId: string, mode: PipelineMode) =>
    request<GeniePipelineStatus>(`${pipelineGenieBase(setupId, mode)}/status`),

  /** Admin backfill: create-or-update the Genie space of an already-deployed mode. */
  provisionPipelineGenieSpace: (setupId: string, mode: PipelineMode) =>
    request<GeniePipelineProvisionResult>(
      `/admin/pipelines/${encodeURIComponent(setupId)}/genie/${encodeURIComponent(mode)}`,
      { method: "POST" },
    ),

  getPipelineGenieStarters: (setupId: string, mode: PipelineMode) =>
    request<GeniePipelineStarters>(`${pipelineGenieBase(setupId, mode)}/starters`),

  startPipelineGenieConversation: (
    setupId: string,
    mode: PipelineMode,
    body: GeniePipelineAskRequest,
  ) =>
    request<GenieConversationIds>(`${pipelineGenieBase(setupId, mode)}/conversations`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

  addPipelineGenieMessage: (
    setupId: string,
    mode: PipelineMode,
    conversationId: string,
    body: GeniePipelineAskRequest,
  ) =>
    request<GenieConversationIds>(
      `${pipelineGenieBase(setupId, mode)}/conversations/${encodeURIComponent(conversationId)}/messages`,
      { method: "POST", body: JSON.stringify(body) },
    ),

  getPipelineGenieMessage: (
    setupId: string,
    mode: PipelineMode,
    conversationId: string,
    messageId: string,
  ) =>
    request<GeniePipelineMessageResponse>(
      `${pipelineGenieBase(setupId, mode)}/conversations/${encodeURIComponent(conversationId)}/messages/${encodeURIComponent(messageId)}`,
    ),

  getPipelineGenieAttachmentResult: (
    setupId: string,
    mode: PipelineMode,
    conversationId: string,
    messageId: string,
    attachmentId: string,
  ) =>
    request<GenieQueryResult>(
      `${pipelineGenieBase(setupId, mode)}/conversations/${encodeURIComponent(conversationId)}/messages/${encodeURIComponent(messageId)}/attachments/${encodeURIComponent(attachmentId)}/result`,
    ),
};

function pipelineGenieBase(setupId: string, mode: PipelineMode): string {
  return `/genie/pipelines/${encodeURIComponent(setupId)}/${encodeURIComponent(mode)}`;
}
