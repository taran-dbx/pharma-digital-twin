import type { ReactNode } from "react";
import {
  Activity,
  ClipboardX,
  Factory,
  PackageX,
  RadioTower,
  ShieldCheck,
  Thermometer,
  Timer,
  type LucideIcon,
} from "lucide-react";

import type { SensorHistoryPoint } from "@/contexts/sensor-context";
import { Sparkline } from "@/components/dashboard/cell-station-card";
import type { MachiningCellModel } from "@/components/cell/use-cell-model";
import type { IndustryKpi } from "@/lib/industries-api";
import {
  lotsQuarantined,
  meanTimeToRecoverSeconds,
  ncrsOpened,
} from "@/lib/manufacturing-quality";
import { SPINDLE_RULE_ID, formatCountdown, formatDeviceTime, healthTone } from "@/lib/manufacturing-story";
import type { AlertEpisode } from "@/lib/types";
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
  "Parts at risk": PackageX,
  "First-pass yield": ShieldCheck,
  "Time to detect": Timer,
  "NCRs opened": ClipboardX,
  "Last device event": RadioTower,
};

const usd = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 });

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

/**
 * Values for the curated med-device KPI labels in `industry_templates.py`.
 * Quality tiles are derived from the latest Spindle 3 excursion, the lot clock
 * and signed decisions; "—" until there is an excursion to measure.
 */
function kpiValue(label: string, model: MachiningCellModel, episodes: AlertEpisode[]): KpiView | null {
  const { spindle, limit, production, quality } = model;
  const stopped = production.phase === "stopped";
  const icon = KPI_ICON[label] ?? Activity;
  const alert = model.episode.alert;
  switch (label) {
    case "Spindle 3 temperature": {
      const delta = limit && spindle.value !== undefined ? limit.threshold - spindle.value : undefined;
      const overLimit = model.spindleAlert !== null;
      return {
        icon,
        v: spindle.value === undefined ? "—" : formatNumber(spindle.value, 1),
        tone: stopped ? "held" : overLimit ? "crit" : healthTone(spindle.health),
        spark: spindle.history,
        meter: limit && spindle.value !== undefined ? spindle.value / (limit.threshold * 1.25) : undefined,
        foot: stopped
          ? "Last reading before the stop"
          : delta === undefined
            ? "Waiting for the spindle sensor"
            : `${formatNumber(Math.abs(delta), 1)} °C ${delta >= 0 ? "below" : "over"} the validated limit`,
      };
    }
    case "Parts at risk": {
      const d = quality.disposition;
      if (!d) return { icon, v: "0", tone: "idle", foot: "No excursion this session" };
      const n = d.partsAtRisk.length;
      const held = !!(model.episode.decisions.quarantine_lot || model.episode.decisions.hold_batch);
      return {
        icon,
        v: String(n),
        tone: n === 0 ? "idle" : held ? "held" : "crit",
        foot: `${held ? "On QA hold" : "Not yet on hold"} · ${
          quality.costOfPoorQualityUsd === null ? "—" : usd.format(quality.costOfPoorQualityUsd)
        } CoPQ if scrapped`,
      };
    }
    case "First-pass yield": {
      const rft = quality.rightFirstTime;
      if (!rft) return { icon, v: "—", tone: "idle", foot: "Right-first-time per lot after an excursion" };
      return {
        icon,
        v: formatNumber(rft.pct, 1),
        tone: rft.good === rft.total ? "ok" : "warn",
        foot: `${rft.good} of ${rft.total} right first time · ${quality.disposition?.lotIds.join(", ") ?? ""}`,
      };
    }
    case "Time to detect": {
      const mttr = meanTimeToRecoverSeconds(episodes, SPINDLE_RULE_ID);
      const mttrText = mttr === undefined ? "MTTR —" : `MTTR ${formatCountdown(mttr)}`;
      if (!alert) return { icon, v: "—", tone: "idle", foot: mttrText };
      return {
        icon,
        v: quality.timeToDetectS === undefined ? "—" : formatNumber(quality.timeToDetectS, 1),
        suffix: quality.timeToDetectS === undefined ? undefined : " s",
        tone: "idle",
        foot: `First reading over the limit → alert · ${mttrText}`,
      };
    }
    case "NCRs opened": {
      const ncrs = ncrsOpened(episodes, alert, model.episode.decisions);
      const lots = lotsQuarantined(episodes, alert, model.episode.decisions);
      return {
        icon,
        v: String(ncrs),
        tone: ncrs > 0 ? "warn" : "idle",
        foot: `${lots} lot${lots === 1 ? "" : "s"} on QA hold · last hour`,
      };
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
  episodes = [],
}: {
  kpis: IndustryKpi[];
  model: MachiningCellModel;
  powered: boolean;
  /** Recent alert episodes (last hour) for NCR, QA-hold and MTTR counts. */
  episodes?: AlertEpisode[];
}) {
  const tiles = [{ k: CELL_STATE, v: "—", u: "" }, ...kpis];
  return (
    <div
      className={`cell-kpis${model.production.phase === "stopped" ? " is-held" : ""}`}
      data-testid="manufacturing-kpis"
    >
      {tiles.map((kpi) => {
        const live = powered ? (kpi.k === CELL_STATE ? cellStateKpi(model) : kpiValue(kpi.k, model, episodes)) : null;
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
