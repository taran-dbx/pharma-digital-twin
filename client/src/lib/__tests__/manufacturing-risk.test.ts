import { describe, expect, it } from "vitest";

import { alertFromLifecycle } from "@/contexts/sensor-context";
import {
  MODEL_RULE_ID,
  alertKind,
  featureLabel,
  isPredictedAlert,
  leadBeforeOnsetS,
  riskFromAlert,
  riskPercent,
  spindleRisk,
} from "@/lib/manufacturing-risk";
import type { AlertLifecycleEvent, AlertRecord } from "@/lib/types";

const iso = (epoch: number) => new Date(epoch * 1000).toISOString();
const T = 1_800_000_000;

function alert(over: Partial<AlertRecord>): AlertRecord {
  return {
    id: "a",
    fingerprint: "fp",
    industry: "manufacturing",
    pipeline_id: null,
    rule_id: "mfg-alert-spindle-temp",
    sensor_id: "mfg-sns-cnc_1_spindle_temp",
    sensor_ids: [],
    severity: "crit",
    status: "open",
    fault_code: null,
    value: 66,
    threshold: 60,
    opened_at: iso(T),
    updated_at: iso(T),
    acknowledged_at: null,
    acknowledged_by: null,
    cleared_at: null,
    ...over,
  };
}

function predicted(over: Partial<AlertRecord> = {}): AlertRecord {
  return alert({
    id: "p",
    rule_id: MODEL_RULE_ID,
    severity: "warn",
    value: 0.81,
    threshold: 0.6,
    opened_at: iso(T - 90),
    updated_at: iso(T - 10),
    metadata: {
      alert_kind: "predicted",
      risk_score: 0.8123,
      model_name: "cat.pharma_twin.spindle_thermal_risk",
      model_version: "2",
      horizon_s: 120,
      top_features: [
        { feature: "temp_slope", contribution: 1.2, value: 9.5 },
        { feature: "hours_since_maintenance", contribution: -0.3, value: 1900 },
      ],
    },
    ...over,
  });
}

describe("alert kind", () => {
  it("separates predicted model alerts from breached limit alerts", () => {
    expect(alertKind(predicted())).toBe("predicted");
    expect(alertKind(alert({}))).toBe("breached");
    expect(alertKind(alert({ alert_kind: "predicted" }))).toBe("predicted");
    expect(isPredictedAlert(alert({ metadata: { alert_kind: "predicted" } }))).toBe(true);
  });
});

describe("riskFromAlert", () => {
  it("reads score, model and drivers from the documented metadata", () => {
    const risk = riskFromAlert(predicted());
    expect(risk).not.toBeNull();
    expect(risk?.score).toBe(0.8123);
    expect(risk?.threshold).toBe(0.6);
    expect(risk?.modelVersion).toBe("2");
    expect(risk?.active).toBe(true);
    expect(risk?.topFeatures.map((f) => f.label)).toEqual(["temperature trend", "hours since maintenance"]);
  });

  it("accepts top_features as a JSON string and falls back to value", () => {
    const risk = riskFromAlert(
      predicted({ metadata: { top_features: '[{"feature":"load_max","contribution":0.4,"value":60}]' } }),
    );
    expect(risk?.score).toBe(0.81);
    expect(risk?.topFeatures[0].label).toBe("spindle load (1 min max)");
  });

  it("ignores threshold alerts", () => {
    expect(riskFromAlert(alert({}))).toBeNull();
  });
});

describe("spindleRisk", () => {
  it("prefers the active predicted alert and measures lead before the onset", () => {
    const breached = alert({
      id: "b",
      opened_at: iso(T),
      metadata: { excursion_started_at: iso(T - 2) },
    });
    const old = predicted({ id: "old", status: "cleared", updated_at: iso(T) });
    const risk = spindleRisk([breached, old, predicted()]);
    expect(risk?.alertId).toBe("p");
    // Predicted at T-90, first over-limit reading at T-2.
    expect(risk?.leadBeforeOnsetS).toBe(88);
  });

  it("has no lead when the prediction came after the onset", () => {
    const breached = alert({ opened_at: iso(T - 100), metadata: { excursion_started_at: iso(T - 102) } });
    expect(leadBeforeOnsetS(predicted(), breached)).toBeNull();
    expect(spindleRisk([alert({})])).toBeNull();
  });
});

describe("helpers", () => {
  it("formats percentages and unknown feature names", () => {
    expect(riskPercent(0.8123)).toBe("81%");
    expect(riskPercent(1.4)).toBe("100%");
    expect(featureLabel("new_feature_x")).toBe("new feature x");
  });

  it("keeps model fields from SSE lifecycle events", () => {
    const event = {
      type: "alert_updated",
      alert_id: "p",
      fingerprint: "fp",
      industry: "manufacturing",
      pipeline_id: null,
      rule_id: MODEL_RULE_ID,
      sensor_id: null,
      sensor_ids: [],
      severity: "warn",
      status: "open",
      fault_code: null,
      value: 0.7,
      threshold: 0.6,
      opened_at: iso(T),
      updated_at: iso(T),
      acknowledged_at: null,
      acknowledged_by: null,
      cleared_at: null,
      alert_kind: "predicted",
      risk_score: 0.7,
      metadata: { model_version: "2" },
    } satisfies AlertLifecycleEvent;
    const record = alertFromLifecycle(event);
    expect(record.alert_kind).toBe("predicted");
    expect(record.risk_score).toBe(0.7);
    expect(record.metadata?.model_version).toBe("2");
  });
});
