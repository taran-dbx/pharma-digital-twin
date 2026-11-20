/**
 * Formatting and tile view-models for the executive and operations views.
 * Pure functions so the copy and the numbers are unit-tested together.
 */
import { PROJECTED_RFT_LABEL } from "@/lib/manufacturing-quality";
import type { Actuals, OpsSummary, Projection, SummaryMeta, ValueSummary } from "@/lib/value-api";

export const EMPTY = "—";

const COMPACT_STEPS: [number, string][] = [
  [1e9, "B"],
  [1e6, "M"],
  [1e3, "K"],
];

/** $1,284 · $12.9K · $4.2M — the stat-tile auto-compact rule. */
export function compactUsd(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return EMPTY;
  const sign = value < 0 ? "−" : "";
  const abs = Math.abs(value);
  if (abs < 10_000) return `${sign}$${Math.round(abs).toLocaleString("en-US")}`;
  for (const [step, suffix] of COMPACT_STEPS) {
    if (abs >= step) {
      const scaled = abs / step;
      const digits = scaled >= 100 ? 0 : 1;
      return `${sign}$${scaled.toFixed(digits).replace(/\.0$/, "")}${suffix}`;
    }
  }
  return `${sign}$${Math.round(abs)}`;
}

/** Full dollars for formulas and tables: $49,750. */
export function fullUsd(value: number): string {
  const sign = value < 0 ? "−" : "";
  return `${sign}$${Math.round(Math.abs(value)).toLocaleString("en-US")}`;
}

/** 0.4 s · 12 s · 3.5 min · 2.1 h */
export function formatDuration(seconds: number | null | undefined): string {
  if (seconds === null || seconds === undefined || !Number.isFinite(seconds) || seconds < 0) return EMPTY;
  if (seconds < 10) return `${seconds.toFixed(1).replace(/\.0$/, "")} s`;
  if (seconds < 90) return `${Math.round(seconds)} s`;
  if (seconds < 5400) return `${(seconds / 60).toFixed(1).replace(/\.0$/, "")} min`;
  return `${(seconds / 3600).toFixed(1).replace(/\.0$/, "")} h`;
}

export function formatPct(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return EMPTY;
  return `${value.toFixed(digits).replace(/\.0$/, "")}%`;
}

export function plural(count: number, one: string, many = `${one}s`): string {
  return `${count.toLocaleString("en-US")} ${count === 1 ? one : many}`;
}

export interface TileView {
  id: string;
  label: string;
  value: string;
  foot: string;
}

/** The four executive tiles: cost avoided, exposure if scrapped, exposure remaining, lots. */
export function execTiles(summary: ValueSummary, windowLabel: string): TileView[] {
  const { projection: p, actuals: a, assumptions } = summary;
  const anyEvents = a.excursion_events_detected > 0;
  return [
    {
      id: "avoided",
      label: "Cost avoided to date",
      value: anyEvents ? compactUsd(a.cost_avoided_to_date) : EMPTY,
      foot: anyEvents
        ? `${a.caught_in_time} of ${plural(a.excursion_events_detected, "excursion")} caught in time · ${windowLabel.toLowerCase()}`
        : `No excursions recorded · ${windowLabel.toLowerCase()}`,
    },
    {
      id: "exposure",
      label: "Annual exposure if scrapped",
      value: compactUsd(p.annual_exposure),
      foot: `${plural(assumptions.lines_in_scope, "line")} × ${assumptions.excursion_events_per_line_per_year} excursions × ${compactUsd(p.loss_per_event)} a lot`,
    },
    {
      id: "remaining",
      label: "Exposure remaining if scrapped",
      value: compactUsd(p.exposure_remaining),
      foot: `${compactUsd(p.cost_avoided)} a year avoided at ${formatPct(assumptions.capture_rate * 100, 0)} capture`,
    },
    {
      id: "lots",
      label: "Lots protected vs quarantined",
      value: anyEvents ? `${a.lots_protected} / ${a.lots_quarantined}` : EMPTY,
      foot: anyEvents
        ? `${plural(a.parts_quarantined, "part")} held · ${plural(a.parts_released, "part")} released`
        : "Protected = contained before the lot moved",
    },
  ];
}

/** One sentence in the funding executive's language. */
export function whatThisMeans(projection: Projection, actuals: Actuals, lines: number, windowLabel: string): string {
  const yearly =
    projection.annual_events === 0
      ? "With no excursions assumed, there is no annual exposure to protect."
      : `Across ${plural(lines, "line")}, catching ${formatPct((projection.caught_events / projection.annual_events) * 100, 0)} of spindle excursions avoids about ${compactUsd(projection.cost_avoided)} a year; ${compactUsd(projection.exposure_remaining)} stays exposed if the missed lots are scrapped.`;
  if (actuals.excursion_events_detected === 0) return yearly;
  return `${yearly} ${windowLabel}: ${actuals.caught_in_time} of ${plural(actuals.excursion_events_detected, "excursion")} were contained before the lot moved, ${compactUsd(actuals.cost_avoided_to_date)} avoided.`;
}

/** Operations KPI row. */
export function opsTiles(ops: OpsSummary): TileView[] {
  const ttd = ops.time_to_detect;
  const rft = ops.projected_rft;
  return [
    {
      id: "ttd",
      label: "Time to detect",
      value: formatDuration(ttd.p50),
      foot: ttd.count ? `median · p90 ${formatDuration(ttd.p90)} · ${ttd.from_first_reading} of ${ttd.count} from the first reading` : "No excursions yet",
    },
    {
      id: "tta",
      label: "Time to acknowledge",
      value: formatDuration(ops.time_to_acknowledge.p50),
      foot: ops.time_to_acknowledge.count ? `median · p90 ${formatDuration(ops.time_to_acknowledge.p90)}` : "Nothing acknowledged yet",
    },
    {
      id: "ttdisp",
      label: "Time to disposition",
      value: formatDuration(ops.time_to_disposition.p50),
      foot:
        `median · ${ops.caught_in_time} of ${plural(ops.episodes, "excursion")} contained in time` +
        (ops.deadline_before_alert ? ` · ${ops.deadline_before_alert} with the lot gone before the alert` : ""),
    },
    {
      id: "inspections",
      label: "NCRs · CMM inspections",
      value: ops.episodes ? `${ops.ncrs_opened} · ${ops.cmm_inspections}` : EMPTY,
      foot: `${plural(ops.production_stops, "cell stop")} · ${plural(ops.maintenance_requests, "maintenance request")}`,
    },
    {
      id: "rft",
      label: PROJECTED_RFT_LABEL,
      value: formatPct(rft.pct),
      foot: rft.parts_started ? `${rft.parts_started - rft.parts_at_risk} of ${rft.parts_started} parts started · no CMM results yet` : "No lots touched",
    },
    {
      id: "mttr",
      label: "MTTR",
      value: formatDuration(ops.mttr.mean),
      foot: ops.mttr.count ? `Mean over ${plural(ops.mttr.count, "cleared excursion")}` : "No excursion cleared yet",
    },
  ];
}

/** Banner text when the distributions or the table are sampled; null when nothing is. */
export function samplingNote(meta: SummaryMeta): string | null {
  if (!meta.truncated) return null;
  return `Totals use all ${plural(meta.episodes_total, "excursion")} in the window. The distributions show the latest ${meta.samples_shown} and the table the latest ${meta.recent_shown}.`;
}

export interface DurationBin {
  start: number;
  end: number;
  count: number;
  label: string;
}

const NICE_STEPS = [0.5, 1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200];

/** Bin durations into ≤ `maxBins` equal-width bins on a nice step. */
export function binDurations(samples: number[], maxBins = 8): DurationBin[] {
  const values = samples.filter((v) => Number.isFinite(v) && v >= 0);
  if (values.length === 0) return [];
  const max = Math.max(...values);
  const step = NICE_STEPS.find((s) => max / s < maxBins) ?? Math.ceil(max / maxBins);
  const count = Math.max(1, Math.floor(max / step) + 1);
  const bins: DurationBin[] = Array.from({ length: count }, (_, i) => ({
    start: i * step,
    end: (i + 1) * step,
    count: 0,
    label: `${formatDuration(i * step)}–${formatDuration((i + 1) * step)}`,
  }));
  for (const v of values) bins[Math.min(count - 1, Math.floor(v / step))].count += 1;
  return bins;
}

/** "Nov 20, 14:03" in the viewer's locale and time zone; "—" when unparseable. */
export function formatEventTime(iso: string | null | undefined): string {
  if (!iso) return EMPTY;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return EMPTY;
  return d.toLocaleString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", hour12: false });
}

const DECISION_LABEL: Record<string, string> = {
  quarantine_lot: "Quarantine",
  request_cmm_inspection: "CMM",
  open_ncr: "NCR",
  request_maintenance: "Maintenance",
};

export function decisionsText(decisions: string[]): string {
  return decisions.length ? decisions.map((d) => DECISION_LABEL[d] ?? d).join(" · ") : "None signed";
}

const COST_LINE_LABEL: Record<string, string> = {
  scrap: "Scrap",
  rework_and_mrb: "Rework, MRB and NCR handling",
  revalidation: "Re-validation",
  lost_run_time: "Lost run time",
  lot_release_delay: "Lot-release delay",
  override: "Cost per lot (your figure)",
  cmm_inspection_and_ncr: "CMM inspection + NCR",
};

export function costLineLabel(item: string): string {
  return COST_LINE_LABEL[item] ?? item.replace(/_/g, " ");
}
