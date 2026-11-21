import { describe, expect, it } from "vitest";

import { envelopeError, summaryQuery, type Actuals, type OpsSummary, type Projection, type SummaryMeta, type ValueSummary } from "@/lib/value-api";
import {
  ackChart,
  binDurations,
  compactUsd,
  decisionsText,
  execTiles,
  formatDuration,
  formatPct,
  opsTiles,
  samplingNote,
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
  deadline_before_alert: 0,
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
  const NONE = { count: 0, mean: null, p50: null, p90: null, max: null, clock_skew_count: 0 };
  const tta = (over: Partial<OpsSummary["time_to_acknowledge"]> = {}): OpsSummary["time_to_acknowledge"] => ({
    count: 1,
    mean: 20,
    p50: 20,
    p90: 20,
    max: 20,
    episodes_total: 2,
    acknowledged_count: 1,
    unacknowledged_count: 1,
    oldest_open_unacknowledged_age_s: null,
    open_unacknowledged_past_cap_count: 0,
    open_age_cap_s: 86_400,
    clock_skew_count: 0,
    samples: [20],
    ...over,
  });
  const ttdisp = (over: Partial<OpsSummary["time_to_disposition"]> = {}): OpsSummary["time_to_disposition"] => ({
    count: 1,
    mean: 25,
    p50: 25,
    p90: 25,
    max: 25,
    episodes_total: 2,
    dispositioned_count: 1,
    undispositioned_count: 1,
    oldest_open_undispositioned_age_s: null,
    open_undispositioned_past_cap_count: 0,
    open_age_cap_s: 86_400,
    clock_skew_count: 0,
    ...over,
  });
  const ops = (over: Partial<OpsSummary> = {}): OpsSummary => ({
    episodes: 2,
    open_episodes: 0,
    time_to_detect: { count: 2, mean: 3, p50: 3, p90: 4, max: 4, clock_skew_count: 0, episodes_total: 2, from_first_reading: 1, from_rule_duration: 1, samples: [2, 4] },
    time_to_acknowledge: tta(),
    time_to_disposition: ttdisp(),
    mttr: { count: 2, mean: 95, p50: 95, p90: 95, max: 100, clock_skew_count: 0, episodes_total: 2, cleared_count: 2, open_count: 0 },
    ncrs_opened: 1,
    cmm_inspections: 2,
    lots_quarantined_events: 1,
    maintenance_requests: 0,
    production_stops: 1,
    caught_in_time: 1,
    deadline_before_alert: 1,
    projected_rft: { label: "Projected right-first-time", pct: 80, parts_started: 15, parts_at_risk: 3, lots: 1 },
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
    expect(byId.ttdisp.foot).toContain("1 with the lot gone before the alert");
  });

  it("shows dashes, not zeros, before the first excursion", () => {
    const empty = opsTiles(
      ops({
        episodes: 0,
        time_to_detect: { ...NONE, episodes_total: 0, from_first_reading: 0, from_rule_duration: 0, samples: [] },
        time_to_acknowledge: tta({ ...NONE, episodes_total: 0, acknowledged_count: 0, unacknowledged_count: 0, samples: [] }),
        time_to_disposition: ttdisp({ ...NONE, episodes_total: 0, dispositioned_count: 0, undispositioned_count: 0 }),
        mttr: { ...NONE, episodes_total: 0, cleared_count: 0, open_count: 0 },
        projected_rft: { label: "Projected right-first-time", pct: null, parts_started: 0, parts_at_risk: 0, lots: 0 },
      }),
    );
    const byId = Object.fromEntries(empty.map((t) => [t.id, t]));
    expect(byId.ttd.value).toBe("—");
    expect(byId.inspections.value).toBe("—");
    expect(byId.rft.value).toBe("—");
    expect(byId.mttr.foot).toBe("No excursion cleared yet");
  });

  it("shows TTA coverage: the median covers only the acknowledged excursions", () => {
    const byId = Object.fromEntries(
      opsTiles(ops({ episodes: 73, time_to_acknowledge: tta({ p50: 11, p90: 11, episodes_total: 73, acknowledged_count: 1, unacknowledged_count: 72 }) })).map((t) => [t.id, t]),
    );
    expect(byId.tta.value).toBe("11 s");
    expect(byId.tta.foot).toBe("median over 1 of 73 acknowledged · p90 11 s · 72 not acknowledged");
  });

  it("shows an empty TTA, never 0 s, when nothing was acknowledged", () => {
    const none = tta({ ...NONE, p50: 0, episodes_total: 73, acknowledged_count: 0, unacknowledged_count: 73, oldest_open_unacknowledged_age_s: 150, samples: [] });
    const byId = Object.fromEntries(opsTiles(ops({ episodes: 73, time_to_acknowledge: none })).map((t) => [t.id, t]));
    expect(byId.tta.value).toBe("—");
    expect(byId.tta.foot).toBe("None of 73 excursions acknowledged · oldest open waiting 2.5 min");
    expect(ackChart(ops({ episodes: 73, time_to_acknowledge: none }), 73)).toEqual({
      caption: "Alert open to the first acknowledgement or signed decision. 0 of 73 acknowledged.",
      empty: "None of 73 excursions acknowledged yet.",
    });
  });

  it("over 200 excursions with only older ones acknowledged, names the latest sample, not 'none'", () => {
    const olderOnly = tta({ count: 30, p50: 7, p90: 7, episodes_total: 230, acknowledged_count: 30, unacknowledged_count: 200, samples: [] });
    const chart = ackChart(ops({ episodes: 230, time_to_acknowledge: olderOnly }), 200);
    expect(chart.empty).toBe("None of the latest 200 excursions acknowledged.");
    expect(chart.caption).toBe("Alert open to the first acknowledgement or signed decision. 30 of 230 acknowledged.");
  });

  it("shows the waiting note with partial coverage, and the cap once reached", () => {
    const partial = tta({ p50: 11, p90: 11, episodes_total: 73, acknowledged_count: 1, unacknowledged_count: 72, oldest_open_unacknowledged_age_s: 150 });
    const foot = (t: OpsSummary["time_to_acknowledge"]) => opsTiles(ops({ episodes: 73, time_to_acknowledge: t })).find((x) => x.id === "tta")?.foot;
    expect(foot(partial)).toBe("median over 1 of 73 acknowledged · p90 11 s · 72 not acknowledged · oldest open waiting 2.5 min");
    expect(foot({ ...partial, oldest_open_unacknowledged_age_s: 86_400, open_unacknowledged_past_cap_count: 2 })).toBe(
      "median over 1 of 73 acknowledged · p90 11 s · 72 not acknowledged · 2 open over 24 h",
    );
    const disp = opsTiles(
      ops({ episodes: 4, time_to_disposition: ttdisp({ p50: 30, episodes_total: 4, dispositioned_count: 1, undispositioned_count: 3, oldest_open_undispositioned_age_s: 40 }) }),
    ).find((x) => x.id === "ttdisp");
    expect(disp?.foot).toBe("median over 1 of 4 contained · 3 not contained · oldest open waiting 40 s · 1 in time · 1 with the lot gone before the alert");
  });

  it("shows disposition and MTTR coverage", () => {
    const byId = Object.fromEntries(
      opsTiles(
        ops({
          episodes: 4,
          time_to_disposition: ttdisp({ p50: 30, episodes_total: 4, dispositioned_count: 1, undispositioned_count: 3 }),
          mttr: { count: 2, mean: 50, p50: 50, p90: 58, max: 60, clock_skew_count: 0, episodes_total: 4, cleared_count: 2, open_count: 2 },
        }),
      ).map((t) => [t.id, t]),
    );
    expect(byId.ttdisp.value).toBe("30 s");
    expect(byId.ttdisp.foot).toBe("median over 1 of 4 contained · 3 not contained · 1 in time · 1 with the lot gone before the alert");
    expect(byId.mttr.foot).toBe("Mean over 2 of 4 cleared · 2 still open");
    const none = opsTiles(
      ops({ time_to_disposition: ttdisp({ ...NONE, mean: 0, p50: 0, dispositioned_count: 0, undispositioned_count: 2, oldest_open_undispositioned_age_s: 40 }) }),
    ).find((t) => t.id === "ttdisp");
    expect(none?.value).toBe("—");
    expect(none?.foot).toBe("None of 2 excursions contained · oldest open waiting 40 s · 1 with the lot gone before the alert");
  });

  it("names the coverage in the acknowledge chart caption", () => {
    expect(ackChart(ops(), 2).caption).toBe("Alert open to the first acknowledgement or signed decision. 1 of 2 acknowledged.");
    const none = tta({ ...NONE, episodes_total: 0, acknowledged_count: 0, unacknowledged_count: 0, samples: [] });
    expect(ackChart(ops({ episodes: 0, time_to_acknowledge: none }), 0)).toEqual({
      caption: "Alert open to the first acknowledgement or signed decision.",
      empty: "No excursions in this window yet.",
    });
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

describe("sampling and errors", () => {
  const meta = (over: Partial<SummaryMeta> = {}): SummaryMeta => ({
    pipeline_id: "p",
    window: "30d",
    window_start: "",
    window_end: "",
    data_available: true,
    parts_per_lot: 15,
    episodes_total: 250,
    truncated: true,
    recent_shown: 20,
    samples_shown: 200,
    ...over,
  });

  it("says when the distributions and the table are sampled", () => {
    expect(samplingNote(meta())).toBe(
      "Totals use all 250 excursions in the window. The distributions show the latest 200 and the table the latest 20.",
    );
    expect(samplingNote(meta({ truncated: false }))).toBeNull();
  });

  it("prefers field details, then error, then FastAPI detail", () => {
    expect(envelopeError({ meta: { ...meta(), details: [{ field: "window", message: "is bad" }] } }, 422)).toBe(
      "window is bad",
    );
    expect(envelopeError({ error: "value data is unavailable right now" }, 503)).toBe("value data is unavailable right now");
    expect(envelopeError({ detail: "identity required" }, 401)).toBe("identity required");
    expect(envelopeError(null, 500)).toBe("Request failed: 500");
  });
});
