/**
 * Typed client for the curated-industry catalog endpoints
 * (`routes/industries.py`: `/api/industries/catalog` + per-mode start/stop).
 *
 * Mirrors the fetch/base-URL/error-handling pattern in `client/src/lib/api.ts`,
 * and camelCase-maps the snake_case JSON the backend returns
 * (`services/industry_service.py::get_catalog`).
 */

const BASE_URL = "/api";

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
    throw new Error(body.error || body.detail || `Request failed: ${res.status}`);
  }

  return res.json();
}

export type PipelineMode = "realtime" | "microbatch" | "zerobus";

export type ModeStatus = "idle" | "starting" | "running";

export interface IndustryMode {
  id: PipelineMode;
  name: string;
  to: string;
  desc: string;
  status: ModeStatus;
  /**
   * Real pipeline row id when one has been provisioned for this mode, else
   * null. Retained for API compatibility and legacy-row data resolution; all
   * user navigation now stays in the industry-first setup/dashboard flow.
   * See `services/industry_service.py::get_catalog`.
   */
  pipelineId: string | null;
  /** Per-mode identity from the industry-first pipeline_deployment table. */
  deploymentId: string | null;
  /** Owning pipeline_setup id when the catalog exposes it (per-pipeline Genie). */
  setupId: string | null;
}

export interface IndustryKpi {
  k: string;
  v: string;
  u: string;
  s?: string;
}

export interface Industry {
  id: string;
  name: string;
  accent: string;
  icon: string;
  twinType: string;
  tag: string;
  lede: string;
  kpis: IndustryKpi[];
  sensors: string[];
  modes: IndustryMode[];
  runningCount: number;
}

export interface Catalog {
  industries: Industry[];
  isAdmin: boolean;
}

// Raw (snake_case) shapes returned by the backend — see
// `services/industry_service.py::get_catalog` and `routes/industries.py`.
interface RawIndustryMode {
  id: string;
  name: string;
  to: string;
  desc: string;
  status: string;
  pipeline_id?: string | null;
  deployment_id?: string | null;
  setup_id?: string | null;
}

interface RawIndustry {
  id: string;
  name: string;
  accent: string;
  icon: string;
  twin_type: string;
  tag: string;
  lede?: string;
  kpis?: IndustryKpi[];
  sensors?: string[];
  modes: RawIndustryMode[];
  running_count: number;
}

interface RawCatalogResponse {
  industries: RawIndustry[];
  is_admin: boolean;
}

function mapMode(raw: RawIndustryMode): IndustryMode {
  return {
    id: raw.id as PipelineMode,
    name: raw.name,
    to: raw.to,
    desc: raw.desc,
    status: raw.status as ModeStatus,
    pipelineId: raw.pipeline_id ?? null,
    deploymentId: raw.deployment_id ?? null,
    setupId: raw.setup_id ?? null,
  };
}

function mapIndustry(raw: RawIndustry): Industry {
  return {
    id: raw.id,
    name: raw.name,
    accent: raw.accent,
    icon: raw.icon,
    twinType: raw.twin_type,
    tag: raw.tag,
    lede: raw.lede ?? "",
    kpis: raw.kpis ?? [],
    sensors: raw.sensors ?? [],
    modes: raw.modes.map(mapMode),
    runningCount: raw.running_count,
  };
}

export async function getCatalog(): Promise<Catalog> {
  const raw = await request<RawCatalogResponse>("/industries/catalog");
  return {
    industries: raw.industries.map(mapIndustry),
    isAdmin: raw.is_admin,
  };
}

export async function startMode(industry: string, mode: PipelineMode): Promise<void> {
  await request(`/industries/${industry}/modes/${mode}/start`, { method: "POST" });
}

export async function stopMode(industry: string, mode: PipelineMode): Promise<void> {
  await request(`/industries/${industry}/modes/${mode}/stop`, { method: "POST" });
}

export interface ModeConfigPatch {
  rate_hz?: number;
  capture_metrics?: boolean;
}

/**
 * Persist frequency/metrics config for a curated industry's mode
 * (`routes/industries.py::update_industry_mode_config`). Stopped-only —
 * the backend returns 409 while the mode's pipeline is running/starting,
 * and 404 when no pipeline has been provisioned for the mode yet; both
 * surface as a thrown `Error` with the server's `detail` message so
 * callers can toast it and revert any optimistic UI update.
 * `capture_metrics` is ignored (not rejected) for `zerobus`.
 */
export async function updateModeConfig(
  industry: string,
  mode: PipelineMode,
  patch: ModeConfigPatch,
): Promise<void> {
  await request(`/industries/${industry}/modes/${mode}/config`, {
    method: "POST",
    body: JSON.stringify(patch),
  });
}

export interface IndustrySensorNode {
  sensor_id: string;
  name: string;
  cadence_hz: number;
}

export interface IndustryComponentNode {
  component_id: string;
  name: string;
  sensors: IndustrySensorNode[];
}

export interface IndustryAssetNode {
  asset_id: string;
  name: string;
  components: IndustryComponentNode[];
}

export interface IndustrySiteNode {
  site_id: string;
  name: string;
  assets: IndustryAssetNode[];
}

export interface IndustryAssets {
  industry: string;
  simulator_industry: string;
  record_industry: string;
  sites: IndustrySiteNode[];
}

export interface IndustryKpiBinding {
  kpi_id: string;
  label: string;
  sensor_name: string;
  unit: string;
}

export interface IndustryAlertRule {
  rule_id: string;
  sensor_id: string;
  operator: string;
  threshold: number;
  severity: string;
  fault_code: string;
  hysteresis?: number;
  /** Configured operator response, e.g. "Inspection required". */
  action?: string;
}

export interface IndustryMaintenanceEvent {
  event_id: string;
  asset_id: string;
  component_id: string;
  kind: string;
  started_at: string;
  completed_at: string | null;
  notes: string;
}

export interface IndustryReference {
  industry: string;
  kpis: IndustryKpiBinding[];
  alert_rules: IndustryAlertRule[];
  maintenance_history: IndustryMaintenanceEvent[];
}

export async function getIndustryAssets(industry: string): Promise<IndustryAssets> {
  return request<IndustryAssets>(`/industries/${industry}/assets`);
}

export async function getIndustryReference(industry: string): Promise<IndustryReference> {
  return request<IndustryReference>(`/industries/${industry}/reference`);
}
