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
  costOfPoorQualityUsd,
  episodeDisposition,
  lotsQuarantined,
  meanTimeToRecoverSeconds,
  ncrsOpened,
  rightFirstTime,
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
    expect(lotIdAt(BASE)).toMatch(/^TM-HS12-\d{4}$/);
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
  it("derives right-first-time and cost of poor quality from the excursion", () => {
    const d = episodeDisposition(alert(BASE + 225, BASE + 275), BASE + 400);
    expect(rightFirstTime(d)).toEqual({ pct: 80, good: 12, total: 15 });
    expect(costOfPoorQualityUsd(d)).toBe(3 * LOT_REFERENCE.unitCostUsd);
    expect(rightFirstTime(null)).toBeNull();
    expect(episodeDisposition(null, BASE)).toBeNull();
  });

  it("an open excursion keeps growing until it clears", () => {
    const open = episodeDisposition(alert(BASE + 225), BASE + 255);
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
    expect(decisionLabel(hold, false)).toBe("QA hold · flow resumed");
  });
});
