/**
 * Client for the executive value and operations summaries
 * (`GET /api/value/summary`, `GET /api/ops/summary`). Both answer with the
 * envelope `{ success, data, error, meta }`; every request names the pipeline.
 */
import { ApiError } from "@/lib/api";

export type ValueWindow = "24h" | "7d" | "30d" | "90d";
export const VALUE_WINDOWS: { id: ValueWindow; label: string }[] = [
  { id: "24h", label: "Last 24 hours" },
  { id: "7d", label: "Last 7 days" },
  { id: "30d", label: "Last 30 days" },
  { id: "90d", label: "Last 90 days" },
];

export type AssumptionName =
  | "lines_in_scope"
  | "excursion_events_per_line_per_year"
  | "cost_per_lot_lost"
  | "capture_rate"
  | "inspection_cost"
  | "lot_release_delay_cost_per_day"
  | "release_delay_days_per_event";

export type Assumptions = Record<AssumptionName, number> & {
  lot_cost_components: Record<string, number>;
};

export interface AssumptionField {
  name: AssumptionName;
  label: string;
  min: number;
  max: number;
  integer: boolean;
}

export interface CostLine {
  item: string;
  usd: number;
}

export interface Projection {
  annual_events: number;
  caught_events: number;
  missed_events: number;
  loss_per_event: number;
  annual_exposure: number;
  cost_avoided: number;
  exposure_remaining: number;
  per_event: {
    if_missed: CostLine[];
    loss_per_event: number;
    if_caught: CostLine[];
    net_avoided_per_caught_event: number;
  };
  formula: Record<string, string>;
  warnings: string[];
}

export interface Actuals {
  excursion_events_detected: number;
  caught_in_time: number;
  not_caught_in_time: number;
  capture_rate_observed: number | null;
  lots_protected: number;
  lots_quarantined: number;
  parts_quarantined: number;
  parts_released: number;
  ncrs_opened: number;
  cmm_inspections: number;
  cost_avoided_to_date: number;
  exposure_if_scrapped: number;
  formula: Record<string, string>;
}

export interface SummaryMeta {
  pipeline_id: string;
  window: string;
  window_start: string;
  window_end: string;
  data_available: boolean;
  parts_per_lot: number;
}

export interface ValueSummary {
  assumptions: Assumptions;
  defaults: Assumptions;
  overridden: AssumptionName[];
  fields: AssumptionField[];
  projection: Projection;
  actuals: Actuals;
}

export interface DurationStats {
  count: number;
  mean: number | null;
  p50: number | null;
  p90: number | null;
  max: number | null;
}

export interface RecentEvent {
  alert_id: string;
  rule_id: string | null;
  fault_code: string | null;
  severity: string | null;
  lot_id: string;
  onset_at: string;
  onset_source: "first_reading" | "rule_duration";
  opened_at: string;
  cleared_at: string | null;
  peak_value: number | null;
  threshold: number | null;
  time_to_detect_s: number;
  time_to_acknowledge_s: number | null;
  time_to_disposition_s: number | null;
  parts_at_risk: number;
  caught_in_time: boolean;
  decisions: string[];
}

export interface OpsSummary {
  episodes: number;
  open_episodes: number;
  time_to_detect: DurationStats & { from_first_reading: number; from_rule_duration: number; samples: number[] };
  time_to_acknowledge: DurationStats & { samples: number[] };
  time_to_disposition: DurationStats;
  mttr: DurationStats;
  ncrs_opened: number;
  cmm_inspections: number;
  lots_quarantined_events: number;
  maintenance_requests: number;
  production_stops: number;
  caught_in_time: number;
  projected_rft: { label: string; pct: number | null; parts_started: number; parts_at_risk: number };
  recent_events: RecentEvent[];
}

interface Envelope<T> {
  success: boolean;
  data: T | null;
  error: string | null;
  meta: SummaryMeta & { details?: { field: string; message: string }[] };
}

export interface Enveloped<T> {
  data: T;
  meta: SummaryMeta;
}

export interface SummaryScope {
  industry: string;
  pipelineId: string;
  window: ValueWindow;
}

export function summaryQuery(scope: SummaryScope, overrides: Partial<Record<AssumptionName, number>> = {}): string {
  const params = new URLSearchParams({ industry: scope.industry, pipeline_id: scope.pipelineId, window: scope.window });
  for (const [name, value] of Object.entries(overrides)) {
    if (value !== undefined && Number.isFinite(value)) params.set(name, String(value));
  }
  return params.toString();
}

async function getEnveloped<T>(path: string, signal?: AbortSignal): Promise<Enveloped<T>> {
  const res = await fetch(`/api${path}`, { credentials: "include", signal });
  const body = (await res.json().catch(() => null)) as Envelope<T> | null;
  if (!res.ok || !body?.success || body.data === null) {
    const detail = body?.meta?.details?.map((d) => `${d.field} ${d.message}`).join("; ");
    throw new ApiError(detail || body?.error || `Request failed: ${res.status}`, res.status);
  }
  return { data: body.data, meta: body.meta };
}

export function getValueSummary(
  scope: SummaryScope,
  overrides: Partial<Record<AssumptionName, number>>,
  signal?: AbortSignal,
): Promise<Enveloped<ValueSummary>> {
  return getEnveloped<ValueSummary>(`/value/summary?${summaryQuery(scope, overrides)}`, signal);
}

export function getOpsSummary(scope: SummaryScope, signal?: AbortSignal): Promise<Enveloped<OpsSummary>> {
  return getEnveloped<OpsSummary>(`/ops/summary?${summaryQuery(scope)}`, signal);
}
