/**
 * Operator model of Trinity Medical's implant cell (MILL → CMM → PACK).
 * Every live number comes from SSE readings (or Zerobus latest rows) and the
 * catalog's validated-limit rules; production state comes from the cell's
 * stop/resume record. Quality values (parts at risk, right-first-time,
 * time-to-detect) are derived only from the alert episode, those readings and
 * the deterministic lot clock.
 */
import { useEffect, useMemo, useState } from "react";

import { useSensorContext, type SensorHistoryPoint } from "@/contexts/sensor-context";
import { useIndustryAlerts } from "@/components/dashboard/alert-rail";
import type { LiveSignal, SensorTile } from "@/components/dashboard/cell-station-card";
import type { CellSensorView, StationView } from "@/components/twin/machining-cell";
import { useProduction, type CellProduction } from "@/hooks/use-production";
import { useSpindleEpisode, type SpindleEpisode } from "@/hooks/use-spindle-episode";
import type {
  IndustryAlertRule,
  IndustryMaintenanceEvent,
  PipelineMode,
} from "@/lib/industries-api";
import type { LotDisposition } from "@/lib/manufacturing-lot";
import { spindleRisk, type SpindleRisk } from "@/lib/manufacturing-risk";
import {
  costOfPoorQualityUsd,
  episodeDisposition,
  excursionWindow,
  projectedRightFirstTime,
  timeToDetectSeconds,
  type ExcursionWindow,
  type ProjectedRft,
} from "@/lib/manufacturing-quality";
import { spindleHeatStateAt, type SpindleHeatState } from "@/lib/manufacturing-scenario";
import {
  CELL_SENSORS,
  CMM_LAB_SENSOR,
  PACK_LINE_SENSOR,
  SPINDLE_RULE_ID,
  SPINDLE_SENSOR,
  activeSpindleAlert,
  isActiveAlert,
  isCellMachineSensor,
  lastSpindleService,
  lotHold,
  sensorOverLimit,
  spindleHealth,
  spindleLimitFrom,
  type CellStationId,
  type HealthState,
  type SpindleLimit,
} from "@/lib/manufacturing-story";
import type { AlertRecord, SensorReading, ZerobusLatestReading } from "@/lib/types";
import { formatNumber, toEpochSeconds } from "@/lib/utils";

const STATUS_SMOOTHING_POINTS = 8;
/** A sensor counts as live if its last device event is this recent. */
const LIVE_WINDOW_S = 15;
const EMPTY_HISTORY: SensorHistoryPoint[] = [];
const MACHINE_SENSOR_COUNT = CELL_SENSORS.filter((s) => isCellMachineSensor(s.name)).length;

export interface CellFeed {
  machinesLive: number;
  machinesTotal: number;
  utilitiesLive: number;
}

export interface RuleWatch {
  ruleId: string;
  label: string;
  faultCode: string;
  value?: number;
  unit: string;
  decimals: number;
  threshold: number;
  operator: IndustryAlertRule["operator"];
  overLimit: boolean;
}

export interface MachiningCellModel {
  limit: SpindleLimit | null;
  spindleAlert: AlertRecord | null;
  /** Thermal-risk model output from the latest predicted alert, if any. */
  risk: SpindleRisk | null;
  alerts: AlertRecord[];
  openAlerts: number;
  spindle: LiveSignal & { health: HealthState; history: SensorHistoryPoint[] };
  pack: { speed: LiveSignal; current: LiveSignal; health: HealthState };
  stations: Record<CellStationId, StationView>;
  sensors: CellSensorView[];
  tiles: Record<CellStationId, SensorTile[]>;
  /** Catalog alert rules on the cell with their live reading. */
  watch: RuleWatch[];
  heatEvent: SpindleHeatState;
  /** 0..1 glow on the spindle mesh, from how close the reading is to the limit. */
  spindleHeat: number;
  spindleRpm?: number;
  /** Device time of the latest CNC-1 / pack-line reading, epoch seconds. */
  lastMachineTs?: number;
  feed: CellFeed;
  lastService: IndustryMaintenanceEvent | null;
  now: number;
  episode: SpindleEpisode;
  production: CellProduction;
  /** The PACK gate is down for a QA hold recorded on this episode, until a resume. */
  batchHeld: boolean;
  quality: CellQuality;
}

export interface CellQuality {
  /** Parts cut while Spindle 3 was outside its validated limit (latest episode). */
  disposition: LotDisposition | null;
  /** First over-limit reading → cleared (or now); null without an excursion. */
  window: ExcursionWindow | null;
  projectedRft: ProjectedRft | null;
  costOfPoorQualityUsd: number | null;
  /** First reading over the limit → alert opened, seconds (live readings only). */
  timeToDetectS?: number;
}

/**
 * Resuming the cell after CMM lifts the PACK gate for a hold recorded before
 * the resume. The QA hold itself stays on the quarantined parts.
 */
function batchHeldNow(episode: SpindleEpisode, production: CellProduction): boolean {
  const hold = lotHold(episode.decisions);
  if (!hold) return false;
  const state = production.state;
  if (state?.state !== "running" || !state.changed_at) return true;
  return toEpochSeconds(state.changed_at) <= toEpochSeconds(hold.created_at);
}

function useNowSeconds(): number {
  const [now, setNow] = useState(() => Date.now() / 1000);
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now() / 1000), 1000);
    return () => clearInterval(id);
  }, []);
  return now;
}

function numeric(value: unknown): number | undefined {
  const n = Number(value);
  return value === null || value === undefined || !Number.isFinite(n) ? undefined : n;
}

function limitText(rule: IndustryAlertRule | undefined, unit: string): string | undefined {
  if (!rule) return undefined;
  return `Limit ${rule.operator === "lt" ? "≥" : "≤"} ${formatNumber(rule.threshold, 0)} ${unit}`;
}

function cmmStation(lab: LiveSignal): StationView {
  return lab.value === undefined
    ? { health: "waiting", readout: "lab —" }
    : { health: "normal", readout: `lab ${formatNumber(lab.value, 1)} ${lab.unit}` };
}

function manufacturingReadings(sensors: Record<string, SensorReading>): SensorReading[] {
  return Object.values(sensors).filter((r) => r.industry === "manufacturing" && Number.isFinite(r.timestamp));
}

export function useMachiningCellModel({
  mode,
  rules,
  maintenance,
  zerobusReadings,
}: {
  mode: PipelineMode;
  rules: IndustryAlertRule[];
  maintenance: IndustryMaintenanceEvent[];
  zerobusReadings: ZerobusLatestReading[] | undefined;
}): MachiningCellModel {
  const { sensors, history } = useSensorContext();
  const { alerts } = useIndustryAlerts("manufacturing");
  const episode = useSpindleEpisode(alerts);
  const now = useNowSeconds();

  const lastMachineTs = useMemo(() => {
    if (mode === "zerobus") {
      const stamps = (zerobusReadings ?? [])
        .filter((z) => isCellMachineSensor(z.sensor_name) && z.event_time)
        .map((z) => Date.parse(String(z.event_time)) / 1000);
      return stamps.length > 0 ? Math.max(...stamps) : undefined;
    }
    const stamps = manufacturingReadings(sensors)
      .filter((r) => isCellMachineSensor(r.sensor_name))
      .map((r) => r.timestamp);
    return stamps.length > 0 ? Math.max(...stamps) : undefined;
  }, [mode, sensors, zerobusReadings]);

  const production = useProduction("manufacturing", lastMachineTs, now);
  const stopped = production.phase === "stopped";

  const model = useMemo(() => {
    const signal = (name: string, fallbackUnit: string): LiveSignal => {
      if (mode === "realtime") {
        const r = sensors[name];
        return r && r.industry === "manufacturing"
          ? { value: numeric(r.value), unit: r.unit || fallbackUnit, timestamp: r.timestamp }
          : { unit: fallbackUnit };
      }
      if (mode === "zerobus") {
        const r = zerobusReadings?.find((z) => z.sensor_name === name);
        const ts = r?.event_time ? Date.parse(r.event_time) / 1000 : undefined;
        return { value: numeric(r?.value), unit: r?.unit || fallbackUnit, timestamp: ts };
      }
      return { unit: fallbackUnit };
    };

    const limit = spindleLimitFrom(rules);
    const spindleAlert = activeSpindleAlert(alerts);
    const temp = signal(SPINDLE_SENSOR, "°C");
    const spindleHistory = history[SPINDLE_SENSOR] ?? EMPTY_HISTORY;
    const recent = spindleHistory.slice(-STATUS_SMOOTHING_POINTS);
    const smoothed = recent.length > 0
      ? recent.reduce((sum, p) => sum + p.v, 0) / recent.length
      : temp.value;
    const health: HealthState = stopped ? "stopped" : spindleHealth(smoothed, limit, spindleAlert !== null);

    const speed = signal(PACK_LINE_SENSOR, "m/min");
    const current = signal("assy_line_1_motor_current", "A");
    const packHealth: HealthState = stopped ? "stopped" : speed.value === undefined ? "waiting" : "normal";

    const cellSensors: CellSensorView[] = [];
    const tiles: Record<CellStationId, SensorTile[]> = { mill: [], inspect: [], pack: [] };
    const watch: RuleWatch[] = [];
    for (const def of CELL_SENSORS) {
      const live = def.name === SPINDLE_SENSOR ? temp : signal(def.name, def.unit);
      const rule = def.ruleId ? rules.find((r) => r.rule_id === def.ruleId) : undefined;
      const overLimit = def.name === SPINDLE_SENSOR
        ? spindleAlert !== null || health === "alert"
        : sensorOverLimit(live.value, rule);
      if (rule) {
        watch.push({
          ruleId: rule.rule_id,
          label: def.label,
          faultCode: rule.fault_code,
          value: live.value,
          unit: live.unit || def.unit,
          decimals: def.decimals,
          threshold: rule.threshold,
          operator: rule.operator,
          overLimit,
        });
      }
      cellSensors.push({
        name: def.name,
        label: def.label,
        group: def.group,
        value: live.value,
        unit: live.unit || def.unit,
        decimals: def.decimals,
        overLimit,
      });
      if (def.group !== "utilities") {
        tiles[def.group].push({
          name: def.name,
          label: def.label,
          unit: live.unit || def.unit,
          decimals: def.decimals,
          value: live.value,
          overLimit,
          limitText: limitText(rule, live.unit || def.unit),
          history: history[def.name] ?? EMPTY_HISTORY,
        });
      }
    }

    const seen = manufacturingReadings(sensors);
    const fresh = seen.filter((r) => now - r.timestamp < LIVE_WINDOW_S);
    const machinesSeen = seen.filter((r) => isCellMachineSensor(r.sensor_name)).length;
    const feed: CellFeed = {
      machinesLive: fresh.filter((r) => isCellMachineSensor(r.sensor_name)).length,
      machinesTotal: machinesSeen || MACHINE_SENSOR_COUNT,
      utilitiesLive: fresh.filter((r) => !isCellMachineSensor(r.sensor_name)).length,
    };

    const spindleHeat = limit && temp.value !== undefined && !stopped
      ? Math.min(1, Math.max(0, (temp.value - (limit.threshold - 15)) / 20))
      : 0;

    return {
      limit,
      spindleAlert,
      risk: spindleRisk(alerts),
      alerts,
      openAlerts: alerts.filter(isActiveAlert).length,
      spindle: { ...temp, health, history: spindleHistory },
      pack: { speed, current, health: packHealth },
      stations: {
        mill: {
          health,
          readout: temp.value === undefined ? "—" : `${formatNumber(temp.value, 1)} ${temp.unit}`,
        },
        inspect: cmmStation(signal(CMM_LAB_SENSOR, "°C")),
        pack: {
          health: packHealth,
          readout: speed.value === undefined ? "—" : `${formatNumber(speed.value, 1)} ${speed.unit}`,
        },
      },
      sensors: cellSensors,
      tiles,
      watch,
      heatEvent: spindleHeatStateAt(now),
      spindleHeat,
      spindleRpm: stopped ? 0 : signal("cnc_1_spindle_speed", "RPM").value,
      feed,
      lastService: lastSpindleService(maintenance),
      now,
    };
  }, [mode, rules, maintenance, zerobusReadings, sensors, history, alerts, now, stopped]);

  const quality = useMemo<CellQuality>(() => {
    const spindleRule = rules.find((r) => r.rule_id === SPINDLE_RULE_ID);
    const window = excursionWindow(episode.alert, now, {
      history: model.spindle.history,
      threshold: model.limit?.threshold,
      durationS: spindleRule?.duration_s ?? 0,
    });
    const disposition = episodeDisposition(window);
    return {
      disposition,
      window,
      projectedRft: projectedRightFirstTime(disposition, now),
      costOfPoorQualityUsd: costOfPoorQualityUsd(disposition),
      timeToDetectS: timeToDetectSeconds(episode.alert, model.spindle.history, model.limit?.threshold),
    };
  }, [episode.alert, now, model.spindle.history, model.limit, rules]);

  return {
    ...model,
    lastMachineTs,
    episode,
    production,
    batchHeld: batchHeldNow(episode, production),
    quality,
  };
}
