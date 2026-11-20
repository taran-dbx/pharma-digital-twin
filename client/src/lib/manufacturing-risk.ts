/**
 * Spindle 3 thermal-risk model alerts (docs/ml-risk-model.md).
 *
 * A model alert is an ordinary alert with `rule_id = MODEL_RULE_ID`: `value` is
 * the risk score, `threshold` the cut-off, and `metadata` carries the model
 * name/version and the top feature contributions. The threshold rule
 * (`mfg-alert-spindle-temp`) says the limit was *breached*; the model alert says
 * an excursion is *predicted* before it happens.
 */
import { excursionStart } from "@/lib/manufacturing-quality";
import type { AlertRecord } from "@/lib/types";
import { toEpochSeconds } from "@/lib/utils";

export const MODEL_RULE_ID = "medtech-model-spindle-risk";
const THRESHOLD_RULE_ID = "mfg-alert-spindle-temp";
/** A prediction this long before the onset still counts as warning of it. */
const MAX_LEAD_S = 15 * 60;

export type AlertKind = "predicted" | "breached";

export interface RiskFeature {
  feature: string;
  label: string;
  contribution: number;
  value: number | null;
}

export interface SpindleRisk {
  alertId: string;
  active: boolean;
  openedAt: string | null;
  /**
   * Seconds the predicted alert opened before the excursion onset (the first
   * over-limit reading the threshold alert recorded), for the latest excursion.
   */
  leadBeforeOnsetS: number | null;
  score: number;
  threshold: number | null;
  modelName: string | null;
  modelVersion: string | null;
  horizonS: number | null;
  scoredAt: string | null;
  topFeatures: RiskFeature[];
}

const FEATURE_LABELS: Record<string, string> = {
  temp_mean: "temperature (1 min mean)",
  temp_slope: "temperature trend",
  temp_max: "temperature (1 min max)",
  temp_last: "temperature now",
  temp_margin_c: "margin to validated limit",
  vibration_mean: "vibration (1 min mean)",
  vibration_slope: "vibration trend",
  vibration_max: "vibration (1 min max)",
  load_mean: "spindle load (1 min mean)",
  load_slope: "spindle load trend",
  load_max: "spindle load (1 min max)",
  power_mean: "spindle power (1 min mean)",
  power_slope: "spindle power trend",
  power_max: "spindle power (1 min max)",
  hours_since_maintenance: "hours since maintenance",
};

export function featureLabel(name: string): string {
  return FEATURE_LABELS[name] ?? name.replace(/_/g, " ");
}

function num(value: unknown): number | null {
  const n = typeof value === "string" ? Number(value) : value;
  return typeof n === "number" && Number.isFinite(n) ? n : null;
}

function str(value: unknown): string | null {
  return value === null || value === undefined || value === "" ? null : String(value);
}

export function alertKind(alert: Pick<AlertRecord, "rule_id" | "metadata" | "alert_kind">): AlertKind {
  if (alert.alert_kind === "predicted" || alert.alert_kind === "breached") return alert.alert_kind;
  if (alert.rule_id === MODEL_RULE_ID || alert.metadata?.alert_kind === "predicted") return "predicted";
  return "breached";
}

export function isPredictedAlert(alert: AlertRecord): boolean {
  return alertKind(alert) === "predicted";
}

function topFeatures(raw: unknown): RiskFeature[] {
  const list = typeof raw === "string" ? safeJson(raw) : raw;
  if (!Array.isArray(list)) return [];
  return list
    .filter((item): item is Record<string, unknown> => typeof item === "object" && item !== null)
    .map((item) => ({
      feature: String(item.feature ?? ""),
      label: featureLabel(String(item.feature ?? "")),
      contribution: num(item.contribution) ?? 0,
      value: num(item.value),
    }))
    .filter((item) => item.feature !== "");
}

function safeJson(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

/** The model output carried by one predicted alert. */
export function riskFromAlert(alert: AlertRecord): SpindleRisk | null {
  if (!isPredictedAlert(alert)) return null;
  const meta = alert.metadata ?? {};
  const score = num(alert.risk_score) ?? num(meta.risk_score) ?? num(alert.value);
  if (score === null) return null;
  return {
    alertId: alert.id,
    active: alert.status === "open" || alert.status === "acknowledged",
    openedAt: alert.opened_at,
    leadBeforeOnsetS: null,
    score,
    threshold: num(alert.threshold) ?? num(meta.risk_threshold),
    modelName: str(meta.model_name),
    modelVersion: str(meta.model_version),
    horizonS: num(meta.horizon_s),
    scoredAt: str(meta.scored_at),
    topFeatures: topFeatures(meta.top_features),
  };
}

/**
 * Lead time gained: the predicted alert opened before the excursion onset that
 * the threshold alert recorded. `null` when it did not open first.
 */
export function leadBeforeOnsetS(predicted: AlertRecord, breached: AlertRecord | undefined): number | null {
  if (!breached || !predicted.opened_at || !breached.opened_at) return null;
  const onset = excursionStart(breached);
  const lead = onset - toEpochSeconds(predicted.opened_at);
  return Number.isFinite(lead) && lead > 0 && lead <= MAX_LEAD_S ? lead : null;
}

/** The active predicted alert, else the most recently updated one. */
export function spindleRisk(alerts: AlertRecord[]): SpindleRisk | null {
  const newest = (a: AlertRecord, b: AlertRecord) =>
    String(b.updated_at ?? "").localeCompare(String(a.updated_at ?? ""));
  const predicted = alerts.filter(isPredictedAlert).sort(newest);
  const pick = predicted.find((a) => a.status === "open" || a.status === "acknowledged") ?? predicted[0];
  const risk = pick ? riskFromAlert(pick) : null;
  if (!risk || !pick) return null;
  const breached = alerts.filter((a) => a.rule_id === THRESHOLD_RULE_ID).sort(newest)[0];
  return { ...risk, leadBeforeOnsetS: leadBeforeOnsetS(pick, breached) };
}

export function riskPercent(score: number): string {
  return `${Math.round(Math.min(1, Math.max(0, score)) * 100)}%`;
}
