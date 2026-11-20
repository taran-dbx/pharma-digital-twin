/** Mock API + SSE payloads for the manufacturing operator twin specs. */

export const MODES = ["realtime", "microbatch", "zerobus"] as const;

/** 2026-10-20 09:41:00 UTC — specs pin the browser to UTC. */
export const DEVICE_T0 = Date.UTC(2026, 9, 20, 9, 41, 0) / 1000;

export const MANUFACTURING_KPIS = [
  { k: "Spindle 3 temperature", v: "—", u: "°C" },
  { k: "Operating limit", v: "—", u: "°C" },
  { k: "Spindle 3 status", v: "—", u: "" },
  { k: "Open alerts", v: "—", u: "" },
  { k: "Last device event", v: "—", u: "" },
];

export const catalog = {
  is_admin: false,
  industries: [
    {
      id: "manufacturing",
      name: "Manufacturing",
      accent: "#e86247",
      icon: "factory",
      twin_type: "line",
      tag: "Machining cell · mill, inspect, pack",
      lede: "Real-time visibility into the machining cell.",
      kpis: MANUFACTURING_KPIS,
      sensors: [],
      modes: MODES.map((mode) => ({
        id: mode,
        name: mode,
        to: mode === "realtime" ? "Lakebase" : "Delta",
        desc: "",
        status: "running",
        pipeline_id: null,
        deployment_id: `manufacturing-${mode}`,
      })),
      running_count: MODES.length,
    },
  ],
};

export const assets = {
  industry: "manufacturing",
  simulator_industry: "manufacturing",
  record_industry: "manufacturing",
  sites: [
    {
      site_id: "mfg-site-detroit",
      name: "Detroit Plant",
      assets: [
        {
          asset_id: "mfg-asset-cnc-1",
          name: "CNC-1",
          components: [
            {
              component_id: "mfg-comp-spindle",
              name: "Spindle 3",
              sensors: [{ sensor_id: "mfg-sns-cnc_1_spindle_temp", name: "cnc_1_spindle_temp", cadence_hz: 5 }],
            },
          ],
        },
      ],
    },
  ],
};

export const reference = {
  industry: "manufacturing",
  kpis: [],
  alert_rules: [
    {
      rule_id: "mfg-alert-spindle-temp",
      sensor_id: "mfg-sns-cnc_1_spindle_temp",
      operator: "gt",
      threshold: 60,
      severity: "crit",
      fault_code: "M-SPINDLE-OVERTEMP",
      hysteresis: 5,
      action: "Inspection required",
    },
    {
      rule_id: "mfg-alert-vibration",
      sensor_id: "mfg-sns-cnc_1_vibration",
      operator: "gt",
      threshold: 8,
      severity: "warn",
      fault_code: "M-SPINDLE-VIBE",
    },
  ],
  maintenance_history: [
    {
      event_id: "mfg-mnt-001",
      asset_id: "mfg-asset-cnc-1",
      component_id: "mfg-comp-spindle",
      kind: "inspection",
      started_at: "2026-09-01T08:00:00Z",
      completed_at: "2026-09-01T12:00:00Z",
      notes: "Spindle bearing inspection",
    },
  ],
};

/** The realtime writer sends naive UTC ISO strings, e.g. "2026-10-20T09:41:04.000". */
function deviceTime(offsetS: number): string {
  return new Date((DEVICE_T0 + offsetS) * 1000).toISOString().replace("Z", "");
}

function reading(sensorName: string, value: number, unit: string, offsetS: number) {
  return {
    sensor_name: sensorName,
    industry: "manufacturing",
    value,
    unit,
    sensor_type: "temperature",
    timestamp: deviceTime(offsetS),
    fault_active: false,
    source: "sss",
  };
}

/** Spindle 3 inside its envelope, one device event per second. */
export const NORMAL_SPINDLE = [45.1, 46.0, 44.7, 45.6, 46.2];

/** Heat event: the last reading crosses the 60 °C limit. */
export const HOT_SPINDLE = [47.8, 52.4, 57.1, 61.9, 66.3];

export function sensorUpdates(temps: number[]) {
  return temps.map((t, i) => ({
    type: "sensor_update",
    source: "sss",
    sensors: [
      reading("cnc_1_spindle_temp", t, "°C", i),
      reading("cnc_1_spindle_speed", 3512 + i * 9, "RPM", i),
      reading("cnc_1_spindle_power", 20.4 + i * 0.2, "kW", i),
      reading("cnc_1_vibration", 2.01 + i * 0.03, "mm/s", i),
      reading("cnc_1_coolant_flow", 25.1, "LPM", i),
      reading("cnc_1_coolant_temp", 22.0, "°C", i),
      reading("assy_line_1_speed", 8.1, "m/min", i),
      reading("assy_line_1_motor_current", 12.3, "A", i),
      reading("compressor_1_pressure", 109.4, "PSI", i),
      reading("hvac_zone_1_temp", 21.0, "°C", i),
    ],
  }));
}

/** A later batch from the same stream: the spindle keeps heating. */
export function laterSpindleUpdate(temp: number, offsetS = 8) {
  return {
    type: "sensor_update",
    source: "sss",
    sensors: [reading("cnc_1_spindle_temp", temp, "°C", offsetS), reading("cnc_1_spindle_speed", 3530, "RPM", offsetS)],
  };
}

export function spindleAlertOpened(value: number) {
  const at = new Date((DEVICE_T0 + 4) * 1000).toISOString();
  return {
    type: "alert_opened",
    alert_id: "alert-spindle-1",
    fingerprint: "mfg-alert-spindle-temp|mfg-sns-cnc_1_spindle_temp|manufacturing-realtime",
    industry: "manufacturing",
    pipeline_id: "manufacturing-realtime",
    rule_id: "mfg-alert-spindle-temp",
    sensor_id: "mfg-sns-cnc_1_spindle_temp",
    sensor_ids: ["mfg-sns-cnc_1_spindle_temp"],
    severity: "crit",
    status: "open",
    fault_code: "M-SPINDLE-OVERTEMP",
    value,
    threshold: 60,
    opened_at: at,
    updated_at: at,
    acknowledged_at: null,
    acknowledged_by: null,
    cleared_at: null,
  };
}

type ResponseAction = "hold_batch" | "send_to_inspect" | "request_maintenance";

const RESPONSE_STATION: Record<ResponseAction, string> = {
  hold_batch: "PACK",
  send_to_inspect: "INSPECT",
  request_maintenance: "MILL",
};

/** What `POST /api/alerts/{id}/responses` returns for one recorded decision. */
export function responseRecorded(action: ResponseAction, offsetS = 6) {
  const episode = new Date((DEVICE_T0 + 4) * 1000).toISOString();
  const reference: Record<string, unknown> = { station: RESPONSE_STATION[action] };
  if (action === "send_to_inspect") reference.parts_since = episode;
  if (action === "request_maintenance") {
    reference.asset = "CNC-1";
    reference.component = "Spindle 3";
    reference.last_service = {
      event_id: "mfg-mnt-001",
      kind: "inspection",
      notes: "Spindle bearing inspection",
      started_at: "2026-09-01T08:00:00Z",
    };
  }
  return {
    type: "alert_response",
    industry: "manufacturing",
    alert_id: "alert-spindle-1",
    created: true,
    response: {
      id: `resp-${action}`,
      alert_id: "alert-spindle-1",
      episode_opened_at: episode,
      action,
      station: RESPONSE_STATION[action],
      recorded_by: "operator@example.com",
      reference,
      created_at: new Date((DEVICE_T0 + offsetS) * 1000).toISOString(),
    },
  };
}

/** One SSE response body; EventSource replays it on reconnect. */
export function eventStream(messages: unknown[]): string {
  return messages.map((m) => `data: ${JSON.stringify(m)}\n\n`).join("");
}

export const zerobusMetrics = {
  table_name: "catalog.schema.manufacturing_zerobus",
  total_records: 0,
  total_bytes: 0,
  records_per_sec: 0,
  records_per_min: 0,
  last_commit_time: null,
  stream_status: "none",
  error_count: 0,
  series: [],
  recent_errors: [],
  latest_readings: [],
};

export const lakebaseHealth = {
  rows_5m: 0,
  latest_at: null,
  latency_sec: 0,
  inserts_per_sec_recent: 0,
};

export const adminCatalog = { ...catalog, is_admin: true };

function iso(epochS: number): string {
  return new Date(epochS * 1000).toISOString();
}

/** Every CNC-1, pack-line and utility sensor at one device time (epoch seconds). */
export function cellUpdate(temp: number, epochS: number) {
  const at = iso(epochS).replace("Z", "");
  const r = (sensorName: string, value: number, unit: string) => ({
    ...reading(sensorName, value, unit, 0),
    timestamp: at,
  });
  return {
    type: "sensor_update",
    source: "sss",
    sensors: [
      r("cnc_1_spindle_temp", temp, "°C"),
      r("cnc_1_spindle_speed", 3540, "RPM"),
      r("cnc_1_spindle_power", 21.2, "kW"),
      r("cnc_1_vibration", 2.1, "mm/s"),
      r("cnc_1_coolant_flow", 25.1, "LPM"),
      r("cnc_1_coolant_temp", 22.1, "°C"),
      r("assy_line_1_speed", 8.1, "m/min"),
      r("assy_line_1_motor_current", 12.3, "A"),
      r("compressor_1_pressure", 109.6, "PSI"),
      r("hvac_zone_1_temp", 21.0, "°C"),
    ],
  };
}

export type ProductionStateValue = "running" | "stopped";

export interface MockProduction {
  industry: "manufacturing";
  state: ProductionStateValue;
  assets: string[];
  reason: string | null;
  alert_id: string | null;
  changed_by: string | null;
  changed_at: string;
}

export function productionState(
  state: ProductionStateValue,
  epochS: number,
  extra: Partial<MockProduction> = {},
): MockProduction {
  return {
    industry: "manufacturing",
    state,
    assets: ["cnc_1", "assy_line_1"],
    reason: null,
    alert_id: null,
    changed_by: "operator@example.com",
    changed_at: iso(epochS),
    ...extra,
  };
}

export function productionEvent(change: MockProduction) {
  return { type: "production_state", industry: "manufacturing", production: change };
}

/** A cleared Spindle 3 episode earlier in the hour, with one decision. */
export function clearedEpisode(openedS: number, clearedS: number) {
  return {
    alert_id: "alert-spindle-0",
    episode_opened_at: iso(openedS),
    rule_id: "mfg-alert-spindle-temp",
    sensor_id: "mfg-sns-cnc_1_spindle_temp",
    severity: "crit",
    fault_code: "M-SPINDLE-OVERTEMP",
    threshold: 60,
    peak_value: 68.4,
    acknowledged_at: iso(openedS + 20),
    acknowledged_by: "operator@example.com",
    cleared_at: iso(clearedS),
    last_event_at: iso(clearedS),
    decisions: [
      {
        action: "send_to_inspect",
        station: "INSPECT",
        recorded_by: "operator@example.com",
        created_at: iso(openedS + 30),
      },
    ],
    status: "cleared",
  };
}

export function lakebasePerf(nowS: number) {
  return {
    rows_per_sec: 296,
    rows_per_min: 17760,
    p50_insert_lag_ms: null,
    p95_insert_lag_ms: null,
    series: Array.from({ length: 15 }, (_, i) => ({ t: iso(nowS - (14 - i) * 60), rows_per_sec: 280 + (i % 4) * 8 })),
  };
}

export function lakebaseHealthAt(nowS: number) {
  return { rows_5m: 88800, latest_at: iso(nowS - 1), latency_sec: 0.4, inserts_per_sec_recent: 296 };
}

export function streamingLatency(nowS: number) {
  return {
    table: "catalog.schema.manufacturing_metrics",
    mode: "realtime",
    has_data: true,
    last_batch_id: 412,
    last_timestamp: iso(nowS - 5),
    num_input_rows: 88000,
    input_rps: 296,
    processed_rps: 301,
    trigger_ms: 300000,
    e2e_p50: 7,
    e2e_p99: 13,
    processing_p50: 3,
    processing_p99: 6,
    queuing_p50: 1,
    queuing_p99: 2,
    series: Array.from({ length: 6 }, (_, i) => ({
      batch_id: 407 + i,
      timestamp: iso(nowS - (5 - i) * 300),
      e2e_p50: 7,
      e2e_p99: 11 + (i % 3),
      processing_p50: 3,
      processing_p99: 6,
      input_rps: 296,
      processed_rps: 301,
      trigger_ms: 300000,
    })),
  };
}
