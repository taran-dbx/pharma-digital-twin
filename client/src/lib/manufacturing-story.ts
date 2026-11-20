/**
 * Operator-facing model for Trinity Medical's orthopaedic-implant cell
 * (MILL → CMM INSPECT → CLEAN / PASSIVATE → STERILE PACK; the twin draws the
 * last two as one PACK station). Spindle 3 is the simulator's `cnc_1` spindle
 * machining Ti-6Al-4V hip stems; every live value comes from SSE readings and
 * the catalog's validated-limit rule. Reference context (work order, lot, UDI)
 * is a labelled fixture until an MES is connected.
 */
import type { IndustryAlertRule, IndustryMaintenanceEvent } from "@/lib/industries-api";
import { LOT_REFERENCE } from "@/lib/manufacturing-lot";
import type {
  AlertRecord,
  AlertResponseAction,
  AlertResponseRecord,
  SignatureMeaning,
} from "@/lib/types";

export const SPINDLE_SENSOR = "cnc_1_spindle_temp";
export const SPINDLE_RULE_ID = "mfg-alert-spindle-temp";
export const PACK_LINE_SENSOR = "assy_line_1_speed";
/** CMM-1 metrology lab ambient; dimensional results depend on a stable lab. */
export const CMM_LAB_SENSOR = "hvac_zone_1_temp";

export type CellStationId = "mill" | "inspect" | "pack";

export const CELL_STATIONS: readonly CellStationId[] = ["mill", "inspect", "pack"];

export const STATION_INFO: Record<CellStationId, { code: string; name: string; asset: string }> = {
  mill: { code: "MILL", name: "Spindle 3", asset: "CNC-1 5-axis mill" },
  inspect: { code: "CMM", name: "CMM inspection", asset: "CMM-1 metrology lab" },
  pack: { code: "PACK", name: "Clean, passivate & sterile pack", asset: "Sterile pack line" },
};

export type HealthState = "normal" | "watch" | "alert" | "waiting" | "no-feed" | "stopped";

export const HEALTH_LABEL: Record<HealthState, string> = {
  normal: "Normal",
  watch: "Approaching limit",
  alert: "Over validated limit",
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
  { name: CMM_LAB_SENSOR, label: "CMM lab ambient", unit: "°C", group: "inspect", decimals: 1 },
  { name: "compressor_1_pressure", label: "Clean dry air", unit: "PSI", group: "utilities", decimals: 0 },
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
  /** Button label: says exactly what is signed. */
  cta: string;
  done: string;
  /** What the e-signature means for this decision (21 CFR 11.50-style). */
  meaning: SignatureMeaning;
  /** Preset reasons offered in the sign-off form; the first is the default. */
  reasons: readonly string[];
}

const EXCURSION_REASON = "Spindle 3 outside its validated limit (OQ-TM-CNC1-014)";

/**
 * Signed quality decisions on a Spindle 3 excursion, in the order a QA lead
 * works them. Requesting CMM inspection also stops the cell.
 */
export const RESPONSE_STEPS: readonly ResponseStep[] = [
  {
    action: "quarantine_lot",
    station: "pack",
    title: "Put the lot on QA hold",
    detail: "Quarantines the parts cut during the excursion at PACK; the rest of the lot is released after CMM review.",
    cta: "Sign QA hold",
    done: "Lot on QA hold",
    meaning: "responsible",
    reasons: [EXCURSION_REASON, "Suspect dimensional nonconformance on stems cut during the excursion"],
  },
  {
    action: "request_cmm_inspection",
    station: "inspect",
    title: "Stop the mill and send parts to CMM",
    detail: "Stops CNC-1 and the pack line; the parts at risk are measured on CMM-1 against the drawing.",
    cta: "Sign & stop for CMM",
    done: "CMM inspection requested",
    meaning: "responsible",
    reasons: [EXCURSION_REASON, "Verify stem taper and neck geometry before disposition"],
  },
  {
    action: "open_ncr",
    station: "inspect",
    title: "Open a nonconformance report",
    detail: "Raises an NCR citing the validated limit (OQ / PQ) and the lot's DHR; disposition goes to MRB.",
    cta: "Sign NCR",
    done: "NCR opened",
    meaning: "authored",
    reasons: [EXCURSION_REASON, "Process parameter excursion during production (21 CFR 820 / ISO 13485 §8.3)"],
  },
  {
    action: "request_maintenance",
    station: "mill",
    title: "Request Spindle 3 maintenance",
    detail: "Raises a work request with the last service and its thermal-compensation check attached.",
    cta: "Sign request",
    done: "Maintenance requested",
    meaning: "authored",
    reasons: ["Recurring Spindle 3 thermal drift", EXCURSION_REASON],
  },
];

/** The decision that stops the cell (the legacy `send_to_inspect` did too). */
export const STOPPING_ACTION: AlertResponseAction = "request_cmm_inspection";

/** Labels for decisions recorded by the CNC-era cell, shown in history only. */
export const LEGACY_DECISION_LABEL: Partial<Record<AlertResponseAction, string>> = {
  hold_batch: "Batch held (legacy)",
  send_to_inspect: "Sent to inspection (legacy)",
};

export const SIGNATURE_MEANING_LABEL: Record<SignatureMeaning, string> = {
  authored: "Author",
  reviewed: "Reviewer",
  approved: "Approver",
  responsible: "Responsible",
};

export type CellDecisions = Partial<Record<AlertResponseAction, AlertResponseRecord>>;

/** The QA hold on this episode (or the legacy batch hold). */
export function lotHold(decisions: CellDecisions): AlertResponseRecord | undefined {
  return decisions.quarantine_lot ?? decisions.hold_batch;
}

/** The CMM request on this episode (or the legacy send-to-inspect). */
export function cmmRequest(decisions: CellDecisions): AlertResponseRecord | undefined {
  return decisions.request_cmm_inspection ?? decisions.send_to_inspect;
}

/**
 * What a recorded decision means now. Resuming the cell lifts the PACK gate,
 * but a QA hold is a quality record: the quarantined parts stay on hold until
 * QA dispositions them.
 */
export function decisionLabel(step: ResponseStep, flowHeld: boolean, record?: AlertResponseRecord): string {
  if (step.action !== "quarantine_lot" || flowHeld) return step.done;
  const parts = record?.reference.parts_at_risk?.length;
  return parts === undefined
    ? "QA hold active · line resumed"
    : `QA hold active on ${parts} part${parts === 1 ? "" : "s"} · line resumed`;
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
  label: LOT_REFERENCE.label,
  site: "Trinity Medical · Warsaw, IN",
  workOrder: LOT_REFERENCE.workOrder,
  part: `${LOT_REFERENCE.partNumber} · ${LOT_REFERENCE.partDescription}`,
  udiDi: LOT_REFERENCE.udiDi,
  upstream: "Ti-6Al-4V bar stock · mill cert on file",
  downstream: "CMM · in-process dimensional inspection",
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
