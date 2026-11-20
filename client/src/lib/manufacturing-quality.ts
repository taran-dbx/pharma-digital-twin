/**
 * Device-quality KPIs for the implant cell, derived only from what the app
 * records: the Spindle 3 alert episode, its readings, the lot clock and the
 * signed decisions. Nothing is invented; values are "—" until there is data.
 *
 * Owners: Alex Morgan (VP Quality & Site Head) — cost of poor quality, lots
 * quarantined; Shreya (Head of Manufacturing Operations) — time-to-detect,
 * NCRs opened, right-first-time, MTTR.
 */
import type { SensorHistoryPoint } from "@/contexts/sensor-context";
import {
  LOT_REFERENCE,
  PARTS_PER_LOT,
  PART_CYCLE_S,
  lotDisposition,
  lotStartAt,
  type LotDisposition,
} from "@/lib/manufacturing-lot";
import type { CellDecisions } from "@/lib/manufacturing-story";
import type { AlertEpisode, AlertRecord } from "@/lib/types";
import { toEpochSeconds } from "@/lib/utils";

/** How far before the alert opened to look for the first reading over the limit. */
const DETECT_LOOKBACK_S = 120;

/** Alert metadata key: first over-limit reading the alert engine saw (ISO time). */
export const EXCURSION_STARTED_KEY = "excursion_started_at";

/**
 * The app's single first-pass-yield proxy. Mirrors `PROJECTED_RFT_DEFINITION`
 * in `src/iot_twin/domain/lot_context.py`; Genie and the docs use the same words.
 */
export const PROJECTED_RFT_LABEL = "Projected right-first-time";
export const PROJECTED_RFT_DEFINITION =
  "Projected right-first-time = (parts started - parts at risk) / parts started, over the lots an excursion " +
  "touched. Parts at risk are the parts whose cut overlapped the time Spindle 3 was outside its validated limit. " +
  "Projected: no CMM results exist yet.";

export interface ExcursionWindow {
  start: number;
  end: number;
  open: boolean;
}

/**
 * When Spindle 3 left its validated limit: the first over-limit reading the
 * alert engine saw, else the first one in the live readings, else
 * `opened_at - duration_s`. Containment errs inclusive (never after opening).
 */
export function excursionStart(
  alert: AlertRecord,
  options: { history?: SensorHistoryPoint[]; threshold?: number; durationS?: number } = {},
): number {
  const opened = toEpochSeconds(alert.opened_at);
  const stamped = toEpochSeconds(alert.metadata?.[EXCURSION_STARTED_KEY]);
  if (Number.isFinite(stamped)) return Math.min(opened, stamped);
  const { history = [], threshold, durationS = 0 } = options;
  if (threshold !== undefined) {
    const first = history.find((p) => {
      const t = p.t / 1000;
      return p.v > threshold && t >= opened - DETECT_LOOKBACK_S && t <= opened;
    });
    if (first) return Math.min(opened, first.t / 1000);
  }
  return opened - durationS;
}

export function excursionWindow(
  alert: AlertRecord | null,
  now: number,
  options: { history?: SensorHistoryPoint[]; threshold?: number; durationS?: number } = {},
): ExcursionWindow | null {
  if (!alert?.opened_at) return null;
  const opened = toEpochSeconds(alert.opened_at);
  if (!Number.isFinite(opened)) return null;
  const start = excursionStart(alert, options);
  const end = alert.cleared_at ? toEpochSeconds(alert.cleared_at) : now;
  return { start, end: Math.max(start, end), open: !alert.cleared_at };
}

/** The parts cut while Spindle 3 was outside its validated limit. */
export function episodeDisposition(window: ExcursionWindow | null): LotDisposition | null {
  return window ? lotDisposition(window.start, window.end) : null;
}

export interface ProjectedRft {
  pct: number;
  good: number;
  partsStarted: number;
  partsAtRisk: number;
}

/** `PROJECTED_RFT_DEFINITION`, computed; null without an excursion. */
export function projectedRightFirstTime(disposition: LotDisposition | null, now: number): ProjectedRft | null {
  if (!disposition || disposition.partsAtRisk.length === 0) return null;
  const lotStarts = [...new Set(disposition.partsAtRisk.map((p) => lotStartAt(p.cutStart)))];
  let started = 0;
  for (const lotStart of lotStarts) {
    if (now >= lotStart) started += Math.min(PARTS_PER_LOT, Math.floor((now - lotStart) / PART_CYCLE_S) + 1);
  }
  started = Math.max(started, disposition.partsAtRisk.length);
  const good = started - disposition.partsAtRisk.length;
  return { pct: (good / started) * 100, good, partsStarted: started, partsAtRisk: disposition.partsAtRisk.length };
}

/** Reference cost of poor quality if the parts at risk are scrapped. */
export function costOfPoorQualityUsd(disposition: LotDisposition | null): number | null {
  return disposition ? disposition.partsAtRisk.length * LOT_REFERENCE.unitCostUsd : null;
}

/** Seconds from the first over-limit reading to the alert opening, when that reading is known. */
export function timeToDetectSeconds(
  alert: AlertRecord | null,
  history: SensorHistoryPoint[],
  threshold: number | undefined,
): number | undefined {
  if (!alert?.opened_at) return undefined;
  const opened = toEpochSeconds(alert.opened_at);
  const stamped = toEpochSeconds(alert.metadata?.[EXCURSION_STARTED_KEY]);
  if (Number.isFinite(stamped)) return Math.max(0, opened - stamped);
  if (threshold === undefined) return undefined;
  const start = excursionStart(alert, { history, threshold, durationS: Number.NaN });
  return Number.isFinite(start) ? Math.max(0, opened - start) : undefined;
}

function episodeKey(alertId: string, openedAt: string): string {
  return `${alertId}|${toEpochSeconds(openedAt)}`;
}

/** NCRs opened across the recent episodes plus the live one. */
export function ncrsOpened(
  episodes: AlertEpisode[],
  current: AlertRecord | null,
  decisions: CellDecisions,
): number {
  const keys = new Set<string>();
  for (const e of episodes) {
    if (e.decisions.some((d) => d.action === "open_ncr")) keys.add(episodeKey(e.alert_id, e.episode_opened_at));
  }
  if (current?.opened_at && decisions.open_ncr) keys.add(episodeKey(current.id, current.opened_at));
  return keys.size;
}

/** Lots put on QA hold across the recent episodes plus the live one. */
export function lotsQuarantined(
  episodes: AlertEpisode[],
  current: AlertRecord | null,
  decisions: CellDecisions,
): number {
  const keys = new Set<string>();
  for (const e of episodes) {
    if (e.decisions.some((d) => d.action === "quarantine_lot" || d.action === "hold_batch")) {
      keys.add(episodeKey(e.alert_id, e.episode_opened_at));
    }
  }
  if (current?.opened_at && (decisions.quarantine_lot || decisions.hold_batch)) {
    keys.add(episodeKey(current.id, current.opened_at));
  }
  return keys.size;
}

/** Mean open → cleared time of the cleared episodes of `ruleId`, seconds. */
export function meanTimeToRecoverSeconds(episodes: AlertEpisode[], ruleId: string): number | undefined {
  const spans = episodes
    .filter((e) => e.rule_id === ruleId && e.cleared_at)
    .map((e) => toEpochSeconds(String(e.cleared_at)) - toEpochSeconds(e.episode_opened_at))
    .filter((s) => Number.isFinite(s) && s >= 0);
  return spans.length > 0 ? spans.reduce((a, b) => a + b, 0) / spans.length : undefined;
}
