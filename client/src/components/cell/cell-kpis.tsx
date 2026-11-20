import type { ReactNode } from "react";
import { Activity, BellRing, Factory, Gauge, RadioTower, Thermometer, type LucideIcon } from "lucide-react";

import type { SensorHistoryPoint } from "@/contexts/sensor-context";
import { Sparkline, heatEventText } from "@/components/dashboard/cell-station-card";
import type { MachiningCellModel } from "@/components/cell/use-cell-model";
import type { IndustryKpi } from "@/lib/industries-api";
import { HEALTH_LABEL, formatCountdown, formatDeviceTime, healthTone, isActiveAlert } from "@/lib/manufacturing-story";
import { formatNumber } from "@/lib/utils";

type KpiTone = ReturnType<typeof healthTone> | "held";

interface KpiView {
  icon: LucideIcon;
  v: string;
  suffix?: string;
  tone: KpiTone;
  foot: ReactNode;
  /** 0..1 position of the live reading on a 0 → 125 % of limit scale. */
  meter?: number;
  spark?: SensorHistoryPoint[];
}

const CELL_STATE = "Cell state";

const KPI_ICON: Record<string, LucideIcon> = {
  [CELL_STATE]: Factory,
  "Spindle 3 temperature": Thermometer,
  "Operating limit": Gauge,
  "Spindle 3 status": Activity,
  "Open alerts": BellRing,
  "Last device event": RadioTower,
};

export function ageText(seconds: number): string {
  if (seconds < 1) return "just now";
  if (seconds < 60) return `${Math.round(seconds)} s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)} min ago`;
  return `${Math.floor(seconds / 3600)} h ago`;
}

export function clockOf(iso: string | null | undefined): string {
  return iso ? formatDeviceTime(Date.parse(iso) / 1000).slice(0, 8) : "—";
}

function LiveDot() {
  return <i className="cell-kpi__live" aria-hidden="true" />;
}

function cellStateKpi(model: MachiningCellModel): KpiView {
  const { production, feed, now } = model;
  const icon = KPI_ICON[CELL_STATE];
  switch (production.phase) {
    case "stopped":
      return {
        icon,
        v: "Stopped",
        tone: "held",
        foot: `${production.state?.reason ?? "Stopped"} · ${formatCountdown(now - (production.since ?? now))} elapsed`,
      };
    case "restarting":
      return { icon, v: "Restarting", tone: "idle", foot: "Waiting for the first CNC-1 reading" };
    case "running":
      return {
        icon,
        v: "Running",
        tone: "idle",
        foot: (
          <>
            {feed.machinesLive > 0 && <LiveDot />}
            {feed.machinesLive} of {feed.machinesTotal} machine sensors live
          </>
        ),
      };
    default: {
      const _exhaustive: never = production.phase;
      return _exhaustive;
    }
  }
}

/** Values for the curated manufacturing KPI labels in `industry_templates.py`. */
function kpiValue(label: string, model: MachiningCellModel): KpiView | null {
  const { spindle, limit, production } = model;
  const stopped = production.phase === "stopped";
  const tone: KpiTone = stopped ? "held" : healthTone(spindle.health);
  const icon = KPI_ICON[label] ?? Activity;
  switch (label) {
    case "Spindle 3 temperature": {
      const delta = limit && spindle.value !== undefined ? limit.threshold - spindle.value : undefined;
      const overLimit = model.spindleAlert !== null;
      return {
        icon,
        v: spindle.value === undefined ? "—" : formatNumber(spindle.value, 1),
        tone: stopped ? "held" : overLimit ? "crit" : healthTone(spindle.health),
        spark: spindle.history,
        foot: stopped
          ? "Last reading before the stop"
          : delta === undefined
            ? "Waiting for the spindle sensor"
            : `${formatNumber(Math.abs(delta), 1)} °C ${delta >= 0 ? "below" : "over"} the limit`,
      };
    }
    case "Operating limit":
      return {
        icon,
        v: limit ? formatNumber(limit.threshold, 0) : "—",
        tone: "idle",
        meter: limit && spindle.value !== undefined ? spindle.value / (limit.threshold * 1.25) : undefined,
        foot: limit ? `Clears below ${formatNumber(limit.threshold - limit.hysteresis, 0)} °C` : "No approved limit",
      };
    case "Spindle 3 status":
      return {
        icon,
        v: HEALTH_LABEL[spindle.health],
        tone,
        foot: stopped
          ? `Stopped ${clockOf(production.state?.changed_at)}`
          : `Prepared heat event · ${heatEventText(model.heatEvent)}`,
      };
    case "Open alerts": {
      const alert = model.episode.alert;
      const foot = !alert
        ? "None this session"
        : isActiveAlert(alert)
          ? `Open since ${clockOf(alert.opened_at)}`
          : `Last cleared ${clockOf(alert.cleared_at)}`;
      return { icon, v: String(model.openAlerts), tone: model.openAlerts > 0 ? "crit" : "idle", foot };
    }
    case "Last device event": {
      const [hms, ms] = formatDeviceTime(model.lastMachineTs).split(".");
      const age = model.lastMachineTs === undefined ? undefined : Math.max(0, model.now - model.lastMachineTs);
      return {
        icon,
        v: hms,
        suffix: ms ? `.${ms}` : undefined,
        tone: stopped ? "held" : "idle",
        foot:
          age === undefined ? (
            "Waiting for the stream"
          ) : stopped ? (
            `No machine data · ${ageText(age)}`
          ) : (
            <>
              <LiveDot />
              {ageText(age)}
            </>
          ),
      };
    }
    default:
      return null;
  }
}

export function CellKpis({
  kpis,
  model,
  powered,
}: {
  kpis: IndustryKpi[];
  model: MachiningCellModel;
  powered: boolean;
}) {
  const tiles = [{ k: CELL_STATE, v: "—", u: "" }, ...kpis];
  return (
    <div
      className={`cell-kpis${model.production.phase === "stopped" ? " is-held" : ""}`}
      data-testid="manufacturing-kpis"
    >
      {tiles.map((kpi) => {
        const live = powered ? (kpi.k === CELL_STATE ? cellStateKpi(model) : kpiValue(kpi.k, model)) : null;
        const Icon = live?.icon ?? KPI_ICON[kpi.k] ?? Activity;
        return (
          <article className={`kpi cell-kpi tone-${live?.tone ?? "idle"}`} key={kpi.k}>
            <header className="cell-kpi__head">
              <Icon aria-hidden="true" />
              <span className="k">{kpi.k}</span>
            </header>
            <div className="cell-kpi__value">
              <div className="v tnum">
                {live?.v ?? "—"}
                {live?.suffix && <small>{live.suffix}</small>}
                {kpi.u && <small> {kpi.u}</small>}
              </div>
              {live?.spark && <Sparkline points={live.spark} over={live.tone === "crit"} />}
            </div>
            {live?.meter !== undefined && (
              <span
                className="cell-kpi__meter"
                style={{ ["--fill" as string]: Math.min(1, Math.max(0, live.meter)) }}
                aria-hidden="true"
              />
            )}
            <div className="cell-kpi__foot">{live?.foot ?? "Pipeline stopped"}</div>
          </article>
        );
      })}
    </div>
  );
}
