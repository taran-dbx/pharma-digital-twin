import { describe, expect, it } from "vitest";

import { summaryQuery, type Actuals, type OpsSummary, type Projection, type ValueSummary } from "@/lib/value-api";
import {
  binDurations,
  compactUsd,
  decisionsText,
  execTiles,
  formatDuration,
  formatPct,
  opsTiles,
  whatThisMeans,
} from "@/lib/value-format";

const projection: Projection = {
  annual_events: 48,
  caught_events: 38.4,
  missed_events: 9.6,
  loss_per_event: 49_750,
  annual_exposure: 2_388_000,
  cost_avoided: 1_852_800,
  exposure_remaining: 535_200,
  per_event: { if_missed: [], loss_per_event: 49_750, if_caught: [], net_avoided_per_caught_event: 48_250 },
  formula: {},
  warnings: [],
};

const actuals = (over: Partial<Actuals> = {}): Actuals => ({
  excursion_events_detected: 4,
  caught_in_time: 3,
  not_caught_in_time: 1,
  capture_rate_observed: 0.75,
  lots_protected: 3,
  lots_quarantined: 4,
  parts_quarantined: 9,
  parts_released: 51,
  ncrs_opened: 2,
  cmm_inspections: 3,
  cost_avoided_to_date: 144_750,
  exposure_if_scrapped: 49_750,
  formula: {},
  ...over,
});

const summary = (a: Actuals = actuals()): ValueSummary => ({
  assumptions: {
    lines_in_scope: 4,
    excursion_events_per_line_per_year: 12,
    cost_per_lot_lost: 49_750,
    capture_rate: 0.8,
    inspection_cost: 1_500,
    lot_release_delay_cost_per_day: 0,
    release_delay_days_per_event: 5,
    lot_cost_components: {},
  },
  defaults: {} as ValueSummary["defaults"],
  overridden: [],
  fields: [],
  projection,
  actuals: a,
});

describe("number formatting", () => {
  it("compacts currency like a stat tile", () => {
    expect(compactUsd(1284)).toBe("$1,284");
    expect(compactUsd(12_900)).toBe("$12.9K");
    expect(compactUsd(4_200_000)).toBe("$4.2M");
    expect(compactUsd(2_000_000)).toBe("$2M");
    expect(compactUsd(-1_500)).toBe("−$1,500");
    expect(compactUsd(null)).toBe("—");
    expect(compactUsd(Number.NaN)).toBe("—");
  });

  it("formats durations by magnitude", () => {
    expect(formatDuration(0.42)).toBe("0.4 s");
    expect(formatDuration(3)).toBe("3 s");
    expect(formatDuration(42)).toBe("42 s");
    expect(formatDuration(210)).toBe("3.5 min");
    expect(formatDuration(7560)).toBe("2.1 h");
    expect(formatDuration(null)).toBe("—");
    expect(formatDuration(-1)).toBe("—");
  });

  it("formats percentages and decisions", () => {
    expect(formatPct(80)).toBe("80%");
    expect(formatPct(93.33)).toBe("93.3%");
    expect(formatPct(null)).toBe("—");
    expect(decisionsText(["quarantine_lot", "open_ncr"])).toBe("Quarantine · NCR");
    expect(decisionsText([])).toBe("None signed");
  });
});

describe("executive tiles", () => {
  it("shows the four headline figures with 'if scrapped' exposure", () => {
    const tiles = execTiles(summary(), "Last 30 days");
    expect(tiles.map((t) => t.label)).toEqual([
      "Cost avoided to date",
      "Annual exposure if scrapped",
      "Exposure remaining if scrapped",
      "Lots protected vs quarantined",
    ]);
    expect(tiles[0].value).toBe("$145K");
    expect(tiles[0].foot).toContain("3 of 4 excursions caught in time");
    expect(tiles[1].value).toBe("$2.4M");
    expect(tiles[1].foot).toBe("4 lines × 12 excursions × $49.8K a lot");
    expect(tiles[3].value).toBe("3 / 4");
  });

  it("shows empty to-date tiles when nothing was recorded", () => {
    const tiles = execTiles(summary(actuals({ excursion_events_detected: 0, caught_in_time: 0 })), "Last 7 days");
    expect(tiles[0].value).toBe("—");
    expect(tiles[0].foot).toBe("No excursions recorded · last 7 days");
    expect(tiles[1].value).toBe("$2.4M");
  });

  it("explains the outcome in one executive sentence", () => {
    const line = whatThisMeans(projection, actuals(), 4, "Last 30 days");
    expect(line).toContain("Across 4 lines, catching 80% of spindle excursions avoids about $1.9M a year");
    expect(line).toContain("$535K stays exposed if the missed lots are scrapped");
    expect(line).toContain("Last 30 days: 3 of 4 excursions were contained before the lot moved");
    const none = whatThisMeans({ ...projection, annual_events: 0 }, actuals({ excursion_events_detected: 0 }), 4, "x");
    expect(none).toBe("With no excursions assumed, there is no annual exposure to protect.");
  });
});

describe("operations tiles and bins", () => {
  const ops = (over: Partial<OpsSummary> = {}): OpsSummary => ({
    episodes: 2,
    open_episodes: 0,
    time_to_detect: { count: 2, mean: 3, p50: 3, p90: 4, max: 4, from_first_reading: 1, from_rule_duration: 1, samples: [2, 4] },
    time_to_acknowledge: { count: 1, mean: 20, p50: 20, p90: 20, max: 20, samples: [20] },
    time_to_disposition: { count: 1, mean: 25, p50: 25, p90: 25, max: 25 },
    mttr: { count: 2, mean: 95, p50: 95, p90: 95, max: 100 },
    ncrs_opened: 1,
    cmm_inspections: 2,
    lots_quarantined_events: 1,
    maintenance_requests: 0,
    production_stops: 1,
    caught_in_time: 1,
    projected_rft: { label: "projected RFT", pct: 80, parts_started: 15, parts_at_risk: 3 },
    recent_events: [],
    ...over,
  });

  it("labels RFT as projected and keeps detection provenance", () => {
    const tiles = opsTiles(ops());
    const byId = Object.fromEntries(tiles.map((t) => [t.id, t]));
    expect(byId.rft.label).toBe("Projected right-first-time");
    expect(byId.rft.value).toBe("80%");
    expect(byId.rft.foot).toBe("12 of 15 parts started · no CMM results yet");
    expect(byId.ttd.value).toBe("3 s");
    expect(byId.ttd.foot).toContain("1 of 2 from the first reading");
    expect(byId.inspections.value).toBe("1 · 2");
    expect(byId.mttr.value).toBe("1.6 min");
  });

  it("shows dashes, not zeros, before the first excursion", () => {
    const empty = opsTiles(
      ops({
        episodes: 0,
        time_to_detect: { count: 0, mean: null, p50: null, p90: null, max: null, from_first_reading: 0, from_rule_duration: 0, samples: [] },
        mttr: { count: 0, mean: null, p50: null, p90: null, max: null },
        projected_rft: { label: "projected RFT", pct: null, parts_started: 0, parts_at_risk: 0 },
      }),
    );
    const byId = Object.fromEntries(empty.map((t) => [t.id, t]));
    expect(byId.ttd.value).toBe("—");
    expect(byId.inspections.value).toBe("—");
    expect(byId.rft.value).toBe("—");
    expect(byId.mttr.foot).toBe("No excursion cleared yet");
  });

  it("bins durations on a nice step and keeps every sample", () => {
    expect(binDurations([])).toEqual([]);
    const bins = binDurations([0.2, 1.4, 3.9, 3.95, 7]);
    expect(bins.reduce((n, b) => n + b.count, 0)).toBe(5);
    expect(bins.length).toBeLessThanOrEqual(8);
    expect(bins[0].start).toBe(0);
    const step = bins[0].end - bins[0].start;
    expect(bins.every((b, i) => b.start === i * step)).toBe(true);
  });
});

describe("summary query", () => {
  it("always carries the pipeline and drops non-finite overrides", () => {
    const q = new URLSearchParams(
      summaryQuery({ industry: "manufacturing", pipelineId: "p-1", window: "7d" }, { capture_rate: 0.5, inspection_cost: Number.NaN }),
    );
    expect(q.get("pipeline_id")).toBe("p-1");
    expect(q.get("window")).toBe("7d");
    expect(q.get("capture_rate")).toBe("0.5");
    expect(q.has("inspection_cost")).toBe(false);
  });
});
