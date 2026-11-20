/**
 * Operator-facing model for the manufacturing machining cell
 * (MILL → INSPECT → PACK). Spindle 3 is the simulator's `cnc_1` spindle; every
 * live value comes from SSE readings and the catalog alert rule. Reference
 * context (work order, batch) is a labelled fixture until an MES is connected.
 */
import type { IndustryAlertRule, IndustryMaintenanceEvent } from "@/lib/industries-api";
import type { AlertRecord, AlertResponseAction, AlertResponseRecord } from "@/lib/types";

export const SPINDLE_SENSOR = "cnc_1_spindle_temp";
export const SPINDLE_RULE_ID = "mfg-alert-spindle-temp";
export const PACK_LINE_SENSOR = "assy_line_1_speed";

export type CellStationId = "mill" | "inspect" | "pack";

export const CELL_STATIONS: readonly CellStationId[] = ["mill", "inspect", "pack"];

export const STATION_INFO: Record<CellStationId, { code: string; name: string; asset: string }> = {
  mill: { code: "MILL", name: "Spindle 3", asset: "CNC-1 mill" },
  inspect: { code: "INSPECT", name: "Vision QA", asset: "Inspection gate" },
  pack: { code: "PACK", name: "Pack line", asset: "Assembly line 1" },
};

export type HealthState = "normal" | "watch" | "alert" | "waiting" | "no-feed" | "stopped";

export const HEALTH_LABEL: Record<HealthState, string> = {
  normal: "Normal",
  watch: "Approaching limit",
  alert: "Over limit",
  waiting: "Waiting for data",
  "no-feed": "No sensor feed",
  stopped: "Stopped",
};

/** Sensor-name prefixes of the machines a production stop halts. */
export const CELL_MACHINE_PREFIXES = ["cnc_1_", "assy_line_1_"] as const;

export function isCellMachineSensor(name: string): boolean {
  return CELL_MACHINE_PREFIXES.some((prefix) => name.startsWith(prefix));
}

export interface SpindleLimit {
  threshold: number;
  hysteresis: number;
  action: string;
  faultCode: string;
}

export function spindleLimitFrom(rules: IndustryAlertRule[]): SpindleLimit | null {
  const rule = rules.find((r) => r.rule_id === SPINDLE_RULE_ID);
  if (!rule) return null;
  return {
    threshold: rule.threshold,
    hysteresis: rule.hysteresis ?? 0,
    action: rule.action ?? "",
    faultCode: rule.fault_code,
  };
}

export function isActiveAlert(alert: AlertRecord): boolean {
  return alert.status === "open" || alert.status === "acknowledged";
}

export function activeSpindleAlert(alerts: AlertRecord[]): AlertRecord | null {
  return alerts.find((a) => a.rule_id === SPINDLE_RULE_ID && isActiveAlert(a)) ?? null;
}

/** Most recent Spindle 3 episode, open or cleared; decisions outlive the alert. */
export function latestSpindleAlert(alerts: AlertRecord[]): AlertRecord | null {
  return (
    alerts
      .filter((a) => a.rule_id === SPINDLE_RULE_ID)
      .sort((a, b) => String(b.opened_at ?? "").localeCompare(String(a.opened_at ?? "")))[0] ?? null
  );
}

export type CellSensorGroup = CellStationId | "utilities";

export interface CellSensor {
  name: string;
  label: string;
  unit: string;
  group: CellSensorGroup;
  decimals: number;
  /** Catalog alert rule that defines this sensor's approved limit, if any. */
  ruleId?: string;
}

/** Live sensors drawn on the twin. Sensors without a catalog rule have no limit. */
export const CELL_SENSORS: readonly CellSensor[] = [
  { name: SPINDLE_SENSOR, label: "Spindle temperature", unit: "°C", group: "mill", decimals: 1, ruleId: SPINDLE_RULE_ID },
  { name: "cnc_1_spindle_speed", label: "Spindle speed", unit: "RPM", group: "mill", decimals: 0 },
  { name: "cnc_1_spindle_power", label: "Spindle power", unit: "kW", group: "mill", decimals: 1 },
  { name: "cnc_1_vibration", label: "Vibration", unit: "mm/s", group: "mill", decimals: 2, ruleId: "mfg-alert-vibration" },
  { name: "cnc_1_coolant_flow", label: "Coolant flow", unit: "LPM", group: "mill", decimals: 1 },
  { name: "cnc_1_coolant_temp", label: "Coolant temperature", unit: "°C", group: "mill", decimals: 1 },
  { name: PACK_LINE_SENSOR, label: "Line speed", unit: "m/min", group: "pack", decimals: 1 },
  { name: "assy_line_1_motor_current", label: "Drive current", unit: "A", group: "pack", decimals: 1 },
  { name: "compressor_1_pressure", label: "Shop air", unit: "PSI", group: "utilities", decimals: 0 },
  { name: "hvac_zone_1_temp", label: "Cell ambient", unit: "°C", group: "utilities", decimals: 1 },
];

export function sensorOverLimit(value: number | undefined, rule: IndustryAlertRule | undefined): boolean {
  if (value === undefined || !rule) return false;
  return rule.operator === "lt" ? value < rule.threshold : value > rule.threshold;
}

export interface ResponseStep {
  action: AlertResponseAction;
  station: CellStationId;
  title: string;
  detail: string;
  /** Button label: says exactly what is recorded. */
  cta: string;
  done: string;
}

/** Recorded operator decisions; sending parts to inspection also stops the cell. */
export const RESPONSE_STEPS: readonly ResponseStep[] = [
  {
    action: "hold_batch",
    station: "pack",
    title: "Hold the batch at PACK",
    detail: "Parts at PACK stay unshipped until QA releases them.",
    cta: "Hold batch",
    done: "Batch on hold",
  },
  {
    action: "send_to_inspect",
    station: "inspect",
    title: "Stop the cell and inspect",
    detail: "Stops CNC-1 and the pack line; parts machined since the alert opened go to vision QA.",
    cta: "Stop & inspect",
    done: "Inspection requested",
  },
  {
    action: "request_maintenance",
    station: "mill",
    title: "Request spindle maintenance",
    detail: "Raises a request for Spindle 3 with its last service attached.",
    cta: "Request maintenance",
    done: "Maintenance requested",
  },
];

export type CellDecisions = Partial<Record<AlertResponseAction, AlertResponseRecord>>;

/** What a recorded decision means now: a resume after the hold releases the batch. */
export function decisionLabel(step: ResponseStep, batchHeld: boolean): string {
  return step.action === "hold_batch" && !batchHeld ? "Batch released" : step.done;
}

export function decisionsFor(alert: AlertRecord | null, records: AlertResponseRecord[] | undefined): CellDecisions {
  if (!alert?.opened_at || !records) return {};
  const episode = Date.parse(alert.opened_at);
  const decisions: CellDecisions = {};
  for (const record of records) {
    if (Date.parse(record.episode_opened_at) === episode) decisions[record.action] = record;
  }
  return decisions;
}

/**
 * The alert engine is authoritative for "Over limit"; the live value only
 * adds the early "Approaching limit" state inside the hysteresis band.
 */
export function spindleHealth(
  value: number | undefined,
  limit: SpindleLimit | null,
  alertActive: boolean,
): HealthState {
  if (alertActive) return "alert";
  if (value === undefined || !limit) return "waiting";
  if (value > limit.threshold) return "alert";
  if (value >= limit.threshold - limit.hysteresis) return "watch";
  return "normal";
}

export function healthTone(state: HealthState): "ok" | "warn" | "crit" | "idle" {
  switch (state) {
    case "normal":
      return "ok";
    case "watch":
      return "warn";
    case "alert":
      return "crit";
    case "waiting":
    case "no-feed":
    case "stopped":
      return "idle";
    default: {
      const _exhaustive: never = state;
      return _exhaustive;
    }
  }
}

/** Shown verbatim with the "Reference · MES not connected" label. */
export const REFERENCE_CONTEXT = {
  label: "Reference · MES not connected",
  workOrder: "WO-1042",
  batch: "Batch 7 · housing, 6061 aluminium",
  upstream: "Bar stock load",
  downstream: "INSPECT · vision QA",
} as const;

export function lastSpindleService(history: IndustryMaintenanceEvent[]): IndustryMaintenanceEvent | null {
  return (
    history
      .filter((e) => e.asset_id === "mfg-asset-cnc-1")
      .sort((a, b) => b.started_at.localeCompare(a.started_at))[0] ?? null
  );
}

export function formatDeviceTime(epochSeconds: number | undefined): string {
  if (epochSeconds === undefined || !Number.isFinite(epochSeconds)) return "—";
  const d = new Date(epochSeconds * 1000);
  const hms = d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
  return `${hms}.${String(d.getMilliseconds()).padStart(3, "0")}`;
}

export function formatCountdown(seconds: number): string {
  const s = Math.max(0, Math.ceil(seconds));
  return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, "0")}`;
}
