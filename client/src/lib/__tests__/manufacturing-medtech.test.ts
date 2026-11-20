import { describe, expect, it } from "vitest";

import {
  LOT_CYCLE_S,
  LOT_REFERENCE,
  PARTS_PER_LOT,
  lotContextAt,
  lotDisposition,
  lotIdAt,
  partIndexAt,
} from "@/lib/manufacturing-lot";
import {
  EXCURSION_STARTED_KEY,
  PROJECTED_RFT_DEFINITION,
  costOfPoorQualityUsd,
  episodeDisposition,
  excursionWindow,
  lotsQuarantined,
  meanTimeToRecoverSeconds,
  ncrsOpened,
  projectedRightFirstTime,
  timeToDetectSeconds,
} from "@/lib/manufacturing-quality";
import { SPINDLE_HEAT_SCENARIO, producerSignalPython } from "@/lib/manufacturing-scenario";
import {
  RESPONSE_STEPS,
  STATION_INFO,
  cmmRequest,
  decisionLabel,
  lotHold,
  type CellDecisions,
} from "@/lib/manufacturing-story";
import type { AlertEpisode, AlertRecord, AlertResponseRecord } from "@/lib/types";

const BASE = 1_800_000_000 - (1_800_000_000 % LOT_CYCLE_S);
const iso = (epoch: number) => new Date(epoch * 1000).toISOString();

function alert(opened: number, cleared?: number): AlertRecord {
  return {
    id: "a1",
    fingerprint: "fp",
    industry: "manufacturing",
    pipeline_id: null,
    rule_id: "mfg-alert-spindle-temp",
    sensor_id: "mfg-sns-cnc_1_spindle_temp",
    sensor_ids: [],
    severity: "crit",
    status: cleared ? "cleared" : "open",
    fault_code: "VPP-SPINDLE3-TEMP",
    value: 66,
    threshold: 60,
    opened_at: iso(opened),
    updated_at: iso(opened),
    acknowledged_at: null,
    acknowledged_by: null,
    cleared_at: cleared ? iso(cleared) : null,
  } as AlertRecord;
}

function decision(action: AlertResponseRecord["action"]): AlertResponseRecord {
  return {
    id: action,
    alert_id: "a1",
    episode_opened_at: iso(BASE + 225),
    action,
    station: "PACK",
    recorded_by: "qa@example.com",
    reference: {},
    created_at: iso(BASE + 230),
  };
}

describe("lot clock", () => {
  it("matches the heat-event cycle and the Python mirror's format", () => {
    expect(LOT_CYCLE_S).toBe(SPINDLE_HEAT_SCENARIO.cycleSeconds);
    expect(PARTS_PER_LOT).toBe(15);
    expect(lotIdAt(BASE)).toMatch(/^TM-HS12-\d{6}-\d{3}$/);
    // 2027-01-15 00:00:00Z is the first cycle of its UTC day; ids carry the date, so they never wrap.
    expect(lotIdAt(Date.UTC(2027, 0, 15) / 1000)).toBe("TM-HS12-270115-000");
    expect(lotIdAt(Date.UTC(2027, 0, 15, 23, 59, 59) / 1000)).toBe("TM-HS12-270115-287");
    expect(partIndexAt(BASE + 245)).toBe(13);
    expect(lotContextAt(BASE + 245)).toEqual({
      lot_id: lotIdAt(BASE),
      part_serial: `${lotIdAt(BASE)}-13`,
      work_order: "WO-TM-24817",
      part_number: "HS-TI64-12",
      udi_di: "00850012345671",
      dhr_ref: `DHR-${lotIdAt(BASE)}`,
    });
  });

  it("names the three parts at risk and releases the rest", () => {
    const d = lotDisposition(BASE + 225, BASE + 275);
    expect(d.partsAtRisk.map((p) => p.index)).toEqual([12, 13, 14]);
    expect(d.partsReleased).toBe(12);
    expect(d.dhrRefs).toEqual([`DHR-${lotIdAt(BASE)}`]);
  });

  it("stamps the lot on mill readings in the producer fragment", () => {
    const python = producerSignalPython("manufacturing");
    expect(python).toContain('LOT_UDI_DI = "00850012345671"');
    expect(python).toContain("if name.startswith(LOT_SENSOR_PREFIX):");
    expect(producerSignalPython("energy")).not.toContain("LOT_PREFIX");
  });
});

describe("quality KPIs", () => {
  it("derives projected right-first-time and cost of poor quality from the excursion", () => {
    const window = excursionWindow(alert(BASE + 225, BASE + 275), BASE + 299);
    const d = episodeDisposition(window);
    expect(d?.partsAtRisk.map((p) => p.index)).toEqual([12, 13, 14]);
    // All 15 parts of the lot started by t=299 s: (15 - 3) / 15.
    expect(projectedRightFirstTime(d, BASE + 299)).toEqual({ pct: 80, good: 12, partsStarted: 15, partsAtRisk: 3 });
    // Mid-lot, only the parts started so far count: (14 - 3) / 14.
    expect(projectedRightFirstTime(d, BASE + 275)?.partsStarted).toBe(14);
    expect(costOfPoorQualityUsd(d)).toBe(3 * LOT_REFERENCE.unitCostUsd);
    expect(projectedRightFirstTime(null, BASE)).toBeNull();
    expect(episodeDisposition(null)).toBeNull();
    expect(PROJECTED_RFT_DEFINITION).toContain("(parts started - parts at risk) / parts started");
  });

  it("starts the window at the first over-limit reading, else opened - duration", () => {
    const stamped = { ...alert(BASE + 241), metadata: { [EXCURSION_STARTED_KEY]: iso(BASE + 239) } } as AlertRecord;
    expect(excursionWindow(stamped, BASE + 250)?.start).toBe(BASE + 239);
    const history = [{ t: (BASE + 238.5) * 1000, v: 60.2 }];
    expect(excursionWindow(alert(BASE + 241), BASE + 250, { history, threshold: 60 })?.start).toBe(BASE + 238.5);
    expect(excursionWindow(alert(BASE + 241), BASE + 250, { durationS: 2 })?.start).toBe(BASE + 239);
    // A stamp from an earlier episode of the reused alert row (days ago) is ignored.
    const stale = { ...alert(BASE + 241), metadata: { [EXCURSION_STARTED_KEY]: iso(BASE - 3 * 86_400) } } as AlertRecord;
    expect(excursionWindow(stale, BASE + 250, { durationS: 2 })?.start).toBe(BASE + 239);
    expect(episodeDisposition(excursionWindow(stale, BASE + 250, { durationS: 2 }))?.partsAtRisk.length).toBe(2);
    expect(timeToDetectSeconds(stale, [], undefined)).toBeUndefined();
    const future = { ...alert(BASE + 241), metadata: { [EXCURSION_STARTED_KEY]: iso(BASE + 245) } } as AlertRecord;
    expect(excursionWindow(future, BASE + 250, { durationS: 2 })?.start).toBe(BASE + 239);
    // Inclusive: the part cut from 220 s to 240 s is at risk even though the alert opened at 241 s.
    const d = episodeDisposition(excursionWindow(alert(BASE + 241), BASE + 250, { durationS: 2 }));
    expect(d?.partsAtRisk.map((p) => p.index)).toEqual([12, 13]);
  });

  it("an open excursion keeps growing until it clears", () => {
    const open = episodeDisposition(excursionWindow(alert(BASE + 225), BASE + 255));
    expect(open?.partsAtRisk.length).toBe(2);
  });

  it("measures time to detect from the first reading over the limit", () => {
    const opened = BASE + 227;
    const history = [
      { t: (opened - 10) * 1000, v: 58 },
      { t: (opened - 2.5) * 1000, v: 60.4 },
      { t: (opened - 1) * 1000, v: 61 },
    ];
    expect(timeToDetectSeconds(alert(opened), history, 60)).toBeCloseTo(2.5);
    expect(timeToDetectSeconds(alert(opened), [], 60)).toBeUndefined();
    expect(timeToDetectSeconds(alert(opened), history, undefined)).toBeUndefined();
    const stamped = { ...alert(opened), metadata: { [EXCURSION_STARTED_KEY]: iso(opened - 2) } } as AlertRecord;
    expect(timeToDetectSeconds(stamped, [], undefined)).toBe(2);
  });

  it("counts NCRs and QA holds once per episode, legacy holds included", () => {
    const episodes: AlertEpisode[] = [
      {
        alert_id: "a1",
        episode_opened_at: iso(BASE - 75),
        rule_id: "mfg-alert-spindle-temp",
        sensor_id: null,
        severity: "crit",
        fault_code: null,
        threshold: 60,
        peak_value: 70,
        status: "cleared",
        acknowledged_at: null,
        acknowledged_by: null,
        cleared_at: iso(BASE - 15),
        last_event_at: null,
        decisions: [
          { action: "open_ncr", station: "INSPECT", recorded_by: "qa", created_at: iso(BASE - 70) },
          { action: "hold_batch", station: "PACK", recorded_by: "qa", created_at: iso(BASE - 70) },
        ],
      },
    ];
    const live: CellDecisions = { open_ncr: decision("open_ncr") };
    expect(ncrsOpened(episodes, alert(BASE + 225), live)).toBe(2);
    expect(ncrsOpened(episodes, alert(BASE - 75), live)).toBe(1);
    expect(lotsQuarantined(episodes, alert(BASE + 225), {})).toBe(1);
    expect(meanTimeToRecoverSeconds(episodes, "mfg-alert-spindle-temp")).toBe(60);
    expect(meanTimeToRecoverSeconds([], "mfg-alert-spindle-temp")).toBeUndefined();
  });
});

describe("response workflow", () => {
  it("offers the four signed med-device decisions", () => {
    expect(RESPONSE_STEPS.map((s) => s.action)).toEqual([
      "quarantine_lot",
      "request_cmm_inspection",
      "open_ncr",
      "request_maintenance",
    ]);
    for (const step of RESPONSE_STEPS) {
      expect(step.reasons.length).toBeGreaterThan(0);
      expect(step.cta).toMatch(/^Sign/);
    }
    expect(STATION_INFO.inspect.code).toBe("CMM");
  });

  it("keeps legacy decisions meaningful", () => {
    expect(lotHold({ hold_batch: decision("hold_batch") })?.action).toBe("hold_batch");
    expect(cmmRequest({ send_to_inspect: decision("send_to_inspect") })?.action).toBe("send_to_inspect");
    const hold = RESPONSE_STEPS[0];
    expect(decisionLabel(hold, true)).toBe("Lot on QA hold");
    const record = { ...decision("quarantine_lot"), reference: { parts_at_risk: ["a", "b", "c"] } };
    expect(decisionLabel(hold, false, record)).toBe("QA hold active on 3 parts · line resumed");
    expect(decisionLabel(hold, false)).toBe("QA hold active · line resumed");
  });
});
