/**
 * Typed client for the admin pipeline-setup / validation / deploy / management
 * API (`routes/industries_admin.py`, `routes/validation_admin.py`,
 * `routes/pipeline_deploy.py`).
 *
 * All endpoints are admin-gated server-side: without an admin session they
 * return 403; without the app-backend Lakebase pool they return 503. Both
 * surface here as a thrown `Error` carrying the server's `detail`, so callers
 * can degrade gracefully (toast + empty/loading state) instead of crashing.
 *
 * Mirrors the fetch/base-URL/error-handling pattern in `client/src/lib/api.ts`.
 * Backend rows are returned as-is (snake_case) — this surface is admin-only and
 * the shapes map 1:1 to the repo rows, so we keep the snake_case field names to
 * avoid a lossy remap layer.
 */

import type { PipelineMode } from "@/lib/industries-api";

const BASE_URL = "/api";
export const DEFAULT_MODE_RATE_HZ = 2000;

export interface ModeSourceConfig {
  rate_hz: number;
}

export type ModeConfigs = Partial<Record<PipelineMode, ModeSourceConfig>>;

async function request<T>(endpoint: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE_URL}${endpoint}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...options?.headers,
    },
    credentials: "include",
  });

  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    const detail =
      typeof body.detail === "object" && body.detail
        ? body.detail.error ?? JSON.stringify(body.detail)
        : body.detail;
    throw new Error(body.error || detail || `Request failed: ${res.status}`);
  }

  return res.json();
}

/* ── Step statuses / validation state ─────────────────────────────────── */

export type BackendStepStatus =
  | "pending"
  | "running"
  | "passed"
  | "failed"
  | "waiting_admin";

export interface ValidationStepRecord {
  status: BackendStepStatus;
  error: string | null;
  updated_at: string | null;
}

/** Live cluster lifecycle for the interactive validation harness. Added to
 * `validate/state` by the backend so the UI can show a real
 * provisioning → running indicator while the (5–10 min) spin-up happens.
 * Optional: absent on older state rows / before the first provision tick. */
export type ClusterState = "PENDING" | "RUNNING" | "TERMINATED" | string;

export interface ValidationState {
  version: number;
  enabled_modes: PipelineMode[];
  sequence: string[];
  steps: Record<string, ValidationStepRecord>;
  cluster_id: string | null;
  /** PENDING/RUNNING/TERMINATED — null until the first provision poll. */
  cluster_state?: ClusterState | null;
  run_ids: Record<string, string>;
}

/** Per-mode deploy-gate verdict (mirrors the backend per-mode gate). */
export interface ModeGate {
  can_deploy: boolean;
  data_purged: boolean;
  /** Not-yet-passed nested backend step keys for this mode's lane. */
  outstanding: string[];
}

export interface CanDeployResult {
  industry: string;
  /** Aggregate over all enabled modes (true only when every mode's gate is open). */
  can_deploy: boolean;
  data_purged: boolean;
  /** De-duplicated union of outstanding keys across enabled modes. */
  outstanding: string[];
  /** Per-mode verdicts so the frontend can enable Deploy per lane. */
  modes: Partial<Record<PipelineMode, ModeGate>>;
}

/* ── Setup / pipeline rows ────────────────────────────────────────────── */

export interface PipelineSetup {
  id: string;
  industry: string;
  status: string;
  flavor: string | null;
  source_type: string | null;
  bootstrap_servers: string | null;
  enabled_modes: PipelineMode[] | null;
  mode_configs?: ModeConfigs | null;
  warehouse_id: string | null;
  user_email: string | null;
  created_at: string | null;
  updated_at: string | null;
  validation_state?: ValidationState | null;
  [key: string]: unknown;
}

export interface PipelineDeployment {
  id: string;
  setup_id: string;
  mode: string;
  status: string;
  job_id: string | null;
  [key: string]: unknown;
}

export interface HealthRollup {
  overall: "draft" | "healthy" | "degraded" | "down" | "unknown";
  resources_healthy: number;
  resources_total: number;
  jobs_running: number;
  jobs_total: number;
  enabled_modes: PipelineMode[];
  deployment_count: number;
}

/** Per-mode runtime status from `build_pipeline_view` (worst-state-wins rollup):
 * `running` (job live + all tasks healthy) / `degraded` (run up but a task
 * crashed — e.g. producer died, data NOT flowing) / `stopped` (deployed, idle) /
 * `draft` (not deployed). */
export type ModeRuntimeStatus =
  | "draft"
  | "starting"
  | "running"
  | "degraded"
  | "stopped"
  | string;

/** Live per-task state within a mode's job run (for the admin pipeline view). */
export interface ModeTaskState {
  task: string;
  life_cycle?: string | null;
  result?: string | null;
  message?: string | null;
}

/** Per-mode contract object from `build_pipeline_view`. */
export interface ModeView {
  mode: string;
  status: ModeRuntimeStatus;
  health: "draft" | "healthy" | "unhealthy" | string;
  startable: boolean;
  /** Why a mode is `degraded` (e.g. "data_producer: driver stopped"), else null. */
  runtime_detail?: string | null;
  /** True when the last run failed and the full pipeline can be restarted. */
  restartable?: boolean;
  tasks?: ModeTaskState[];
  deployed?: boolean;
  flowing?: boolean;
}

export interface PipelineListItem {
  setup: PipelineSetup;
  deployments: PipelineDeployment[];
  health: HealthRollup;
  /** Top-level rollup status from `build_pipeline_view` — the authoritative
   * deploy signal: `running` (a mode's job is live) / `degraded` (deployed but a
   * task crashed) / `stopped` (deployed but idle) / `draft` (never deployed).
   * `draft` ⇒ not shown on the home. */
  status?: ModeRuntimeStatus;
  /** Per-mode breakdown so the UI can show each mode's real state, not just the
   * rollup — a single running mode must not paint the whole pipeline green. */
  modes?: ModeView[];
  validation_passed?: boolean;
}

export interface ResourceHealthItem {
  id: string;
  identifier: string;
  parent_ref: string | null;
  teardown_order: number | null;
  status: string | null;
  health: "healthy" | "degraded" | "deleted";
  metadata: Record<string, unknown>;
}

/* ── Two-tier live health (spec §1.16) ────────────────────────────────── */

/** Which tier a check belongs to: `"api"` = Tier-1 fast API/DB check that runs
 * from the app backend with **no** interactive cluster (seconds); `"e2e"` =
 * the opt-in Tier-2 interactive test that spins a cluster (~10 min). */
export type CheckTier = "api" | "e2e";

/** Per-check verdict: `pass` / `fail`, or `skip` when a prerequisite is absent
 * (e.g. no warehouse configured, no jobs deployed yet). */
export type CheckStatus = "pass" | "fail" | "skip";

/** One live health check emitted by `services/health_checks.py`. Check `name`s
 * are stable identifiers the UI keys on (e.g. `uc_catalog_schemas`,
 * `delta_tables`, `checkpoint_volume`, `metrics_table`, `lakebase_endpoints`,
 * `chronotable`, `cdf_sync`, `uc_grants`, `secrets`, `jobs`,
 * `interactive_cluster`, `kafka_topics`). */
export interface HealthCheck {
  name: string;
  tier: CheckTier;
  status: CheckStatus;
  detail: string;
  /** Only the `interactive_cluster` check carries this echo of the cluster state. */
  cluster_state?: ClusterState | null;
}

/** Tier-1 (API/DB) rollup verdict — the pipeline's live top-line health.
 * `unknown` when every Tier-1 check was skipped (e.g. no warehouse). `draft`
 * is the deploy-gate fallback when Tier-1 didn't run (no workspace client). */
export type ApiHealth = "healthy" | "unhealthy" | "unknown" | "draft";

/** Descriptor for the opt-in Tier-2 interactive end-to-end test. The e2e-tier
 * checks skipped by Tier-1 (topic connectivity, data-flow) are covered here. */
export interface E2eTestDescriptor {
  tier: "e2e";
  available: boolean;
  spins_interactive_cluster: boolean;
  eta_minutes: number;
  description: string;
  /** e.g. `POST /api/admin/industries/{industry}/validate/start`. */
  start_endpoint: string;
  /** e.g. `GET /api/admin/industries/{industry}/validate/state`. */
  state_endpoint: string;
}

export interface PipelineHealthDetail {
  /** Runtime-configured shared Unity Catalog namespace. */
  uc_catalog: string;
  uc_schema: string;
  app_sp_client_id: string;
  setup: PipelineSetup;
  deployments: PipelineDeployment[];
  /** Ledger inventory grouped by resource type (admin detail). */
  resources: Record<string, ResourceHealthItem[]>;
  /** Tier-1 live checks. Empty when no workspace client is available (tests /
   * no creds) — the UI degrades to "live checks unavailable". */
  checks: HealthCheck[];
  /** Informational interactive-cluster state — drives the always-on indicator. */
  cluster_state: ClusterState | null;
  /** Opt-in Tier-2 interactive end-to-end test descriptor. */
  e2e_test: E2eTestDescriptor;
  /** Tier-1 rollup verdict — the authoritative top-line live health. */
  health: ApiHealth;
  /** True when the Tier-1 rollup is healthy (safe to start). */
  startable: boolean;
  ready: boolean;
  /** Selected-mode validation gates all passed; distinct from runtime health. */
  validation_passed?: boolean;
  /** Deploy-gate rollup, preserved for admin detail (present when Tier-1 ran). */
  deploy_health?: string;
  deploy_startable?: boolean;
  /** Per-mode contract from `build_pipeline_view`. */
  modes?: ModeView[];
  status?: ModeRuntimeStatus;
  resource_health?: unknown;
  [key: string]: unknown;
}

export interface SetupPayload {
  flavor?: string;
  source_type?: string;
  bootstrap_servers?: string;
  enabled_modes?: PipelineMode[];
  mode_configs?: ModeConfigs;
  cluster_spec?: Record<string, unknown>;
  warehouse_id?: string;
}

export interface DeployPayload {
  consumer_notebook?: string;
  producer_notebook?: string;
  node_type?: string;
  num_workers?: number;
  single_user_name?: string;
  base_parameters?: Record<string, unknown>;
  gate_override?: boolean;
}

/* ── Setup + provision ────────────────────────────────────────────────── */

export async function saveSetup(
  industry: string,
  payload: SetupPayload,
): Promise<{ created: boolean; setup: PipelineSetup }> {
  return request(`/admin/industries/${industry}/setup`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function getIndustrySetup(industry: string): Promise<{
  setup: PipelineSetup;
  deployments: PipelineDeployment[];
  pipeline: Record<string, unknown>;
}> {
  return request(`/admin/industries/${industry}/setup`);
}

export async function updatePipelineModes(
  setupId: string,
  enabledModes: PipelineMode[],
): Promise<{
  setup: PipelineSetup;
  added: PipelineMode[];
  removed: PipelineMode[];
  retained: PipelineMode[];
}> {
  return request(`/admin/pipelines/${setupId}/modes`, {
    method: "PATCH",
    body: JSON.stringify({ enabled_modes: enabledModes }),
  });
}

export async function updatePipelineModeConfig(
  setupId: string,
  mode: PipelineMode,
  rateHz: number,
): Promise<{ mode: PipelineMode; mode_config: ModeSourceConfig; applied: string }> {
  return request(`/admin/pipelines/${setupId}/modes/${mode}/config`, {
    method: "PATCH",
    body: JSON.stringify({ rate_hz: rateHz }),
  });
}

export async function provisionIndustry(
  industry: string,
  setupId?: string,
): Promise<{ setup_id: string; summary: unknown; resources: unknown[] }> {
  return request(`/admin/industries/${industry}/provision`, {
    method: "POST",
    body: JSON.stringify(setupId ? { setup_id: setupId } : {}),
  });
}

/* ── Validation state machine ─────────────────────────────────────────── */

export async function startValidation(
  industry: string,
): Promise<{ industry: string; validation_state: ValidationState }> {
  return request(`/admin/industries/${industry}/validate/start`, {
    method: "POST",
  });
}

export async function getValidationState(
  industry: string,
): Promise<{ industry: string; validation_state: ValidationState | null }> {
  return request(`/admin/industries/${industry}/validate/state`);
}

export async function retryValidationStep(
  industry: string,
  step: string,
): Promise<{ industry: string; retried: string; validation_state: ValidationState }> {
  return request(
    `/admin/industries/${industry}/validate/retry/${encodeURIComponent(step)}`,
    { method: "POST" },
  );
}

export async function markCdfEnabled(
  industry: string,
  mode: PipelineMode = "realtime",
): Promise<{ industry: string; mode: string; validation_state: ValidationState }> {
  return request(`/admin/industries/${industry}/validate/cdf-enabled`, {
    method: "POST",
    body: JSON.stringify({ mode }),
  });
}

export async function getCanDeploy(industry: string): Promise<CanDeployResult> {
  return request(`/admin/industries/${industry}/validate/can-deploy`);
}

/* ── Deploy + per-mode lifecycle ──────────────────────────────────────── */

export async function deployPipeline(
  setupId: string,
  mode: PipelineMode,
  payload: DeployPayload = {},
): Promise<Record<string, unknown>> {
  return request(`/admin/pipelines/${setupId}/deploy/${mode}`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function startPipelineMode(
  setupId: string,
  mode: PipelineMode,
): Promise<Record<string, unknown>> {
  return request(`/admin/pipelines/${setupId}/${mode}/start`, { method: "POST" });
}

export async function stopPipelineMode(
  setupId: string,
  mode: PipelineMode,
): Promise<Record<string, unknown>> {
  return request(`/admin/pipelines/${setupId}/${mode}/stop`, { method: "POST" });
}

export async function getPipelineModeStatus(
  setupId: string,
  mode: PipelineMode,
): Promise<Record<string, unknown>> {
  return request(`/admin/pipelines/${setupId}/${mode}/status`);
}

/* ── Management + teardown ────────────────────────────────────────────── */

export async function listAdminPipelines(): Promise<{ pipelines: PipelineListItem[] }> {
  // Management list only — heavy Jobs/Lakebase fan-out. Gallery and dashboards
  // must use ``listPipelineRuntime`` instead.
  const endpoint = "/admin/pipelines";
  return request(endpoint);
}

export async function listPipelineRuntime(): Promise<{ pipelines: PipelineListItem[] }> {
  // Public runtime only — never fall back to `/admin/pipelines`. That path
  // fans out across Jobs + Lakebase freshness and is what made `/industries`
  // hang when the lightweight runtime endpoint was cold or returning 503.
  return request(`/industries/runtime`);
}

export async function getPipelineHealth(
  setupId: string,
): Promise<PipelineHealthDetail> {
  return request(`/admin/pipelines/${setupId}/health`, { cache: "no-store" });
}

export async function deletePipeline(
  setupId: string,
): Promise<Record<string, unknown>> {
  return request(`/admin/pipelines/${setupId}`, { method: "DELETE" });
}

/* ── Frontend step-id ↔ backend flat-key mapping ──────────────────────── */

const MODE_PREFIX: Record<string, PipelineMode> = {
  rt_: "realtime",
  mb_: "microbatch",
  zb_: "zerobus",
};

const BACKEND_MODE_TO_FE: Record<string, string> = {
  realtime: "rt_",
  microbatch: "mb_",
  zerobus: "zb_",
};

/**
 * Convert a frontend step id (e.g. `rt_cdf_verify`, `topics_created`) into the
 * backend flat state key (`realtime:cdf_verify`, `topics_created`).
 */
export function feStepToBackendKey(feId: string): string {
  for (const [prefix, mode] of Object.entries(MODE_PREFIX)) {
    if (feId.startsWith(prefix)) return `${mode}:${feId.slice(prefix.length)}`;
  }
  return feId; // common step — same bare name
}

/**
 * Convert a backend flat state key (`realtime:cdf_verify`) into the frontend
 * step id (`rt_cdf_verify`). Common/terminal keys pass through unchanged.
 */
export function backendKeyToFeStep(key: string): string {
  const sepIdx = key.indexOf(":");
  if (sepIdx === -1) return key;
  const mode = key.slice(0, sepIdx);
  const step = key.slice(sepIdx + 1);
  const prefix = BACKEND_MODE_TO_FE[mode];
  return prefix ? `${prefix}${step}` : key;
}

/** Non-gated terminal steps (excluded from the "is the driver still working" check). */
const TERMINAL_KEYS = new Set(["show_final_notebook", "done"]);

const DEAD_CLUSTER_STATES = new Set(["TERMINATED", "ERROR", "TERMINATING"]);

function bareStep(key: string): string {
  const sep = key.indexOf(":");
  return sep === -1 ? key : key.slice(sep + 1);
}

/**
 * Mirror of the backend `next_runnable_step` walk to derive an overall driver
 * phase from a persisted validation state, so the poller knows when to stop.
 */
export function driverPhase(
  state: ValidationState | null,
): "idle" | "running" | "waiting_admin" | "failed" | "complete" {
  if (!state || !state.sequence) return "idle";
  const clusterDead =
    state.cluster_state != null && DEAD_CLUSTER_STATES.has(state.cluster_state);
  for (const key of state.sequence) {
    if (TERMINAL_KEYS.has(key)) continue;
    const status = state.steps[key]?.status ?? "pending";
    if (status === "passed") continue;
    if (status === "waiting_admin") return "waiting_admin";
    if (status === "failed") return "failed";
    // Autoterminate must not look like an in-flight run except while we are
    // (re)provisioning a replacement cluster.
    if (clusterDead && bareStep(key) !== "provision_cluster") return "failed";
    return "running"; // pending or running — driver still has work
  }
  return "complete";
}
