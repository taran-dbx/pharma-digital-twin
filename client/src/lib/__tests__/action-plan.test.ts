import { describe, expect, it } from "vitest";

import { canApply, citationLabel, citedRows, planBadge, prefillFor, MAX_COMMENT, MAX_REASON } from "@/lib/action-plan";
import type { ActionPlanRecord, ActionPlanStep } from "@/lib/types";

const step: ActionPlanStep = {
  order: 1,
  owner_role: "QA",
  action: "Place lot TM-HS12-261120-121 on QA hold.",
  response_action: "quarantine_lot",
  citations: ["lot:TM-HS12-261120-121", "alert_rule:mfg-alert-spindle-temp"],
};

function plan(overrides: Partial<ActionPlanRecord> = {}): ActionPlanRecord {
  return {
    plan_id: "0f8c2a51-1111-2222-3333-444455556666",
    alert_id: "a1",
    requested_by: "qa@example.com",
    requested_at: "2026-11-20T10:05:00Z",
    endpoint: "databricks-claude-sonnet-5-5",
    model: "claude-sonnet-5-5",
    model_version: "databricks-claude-sonnet-5-5",
    prompt_template_version: "action-plan/2026-11-20.v1",
    prompt_hash: "h",
    grounding_hash: "g",
    grounding_check: "pass",
    grounding_reasons: [],
    status: "generated",
    error: null,
    latency_ms: 900,
    usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
    plan: {
      summary: { text: "Spindle 3 peaked at 64.2 °C.", citations: ["alert:a1"] },
      disposition: {
        quarantine: { lots: [], serials: [], rationale: "r", citations: ["alert:a1"] },
        cmm_inspection: { scope: "s", serials: [], citations: ["alert:a1"] },
        ncr: { required: true, rationale: "r", citations: ["alert:a1"] },
        maintenance_request: { required: false, rationale: "r", citations: ["alert:a1"] },
      },
      risk_narrative: { text: "No model risk score was available.", citations: ["alert:a1"] },
      steps: [step],
    },
    citation_ids: ["alert:a1"],
    applied_by: null,
    applied_at: null,
    applied_response_id: null,
    advisory: true,
    ...overrides,
  };
}

describe("action plan helpers", () => {
  it("derives the badge from status and grounding check", () => {
    expect(planBadge(plan())).toBe("pass");
    expect(planBadge(plan({ grounding_check: "fail" }))).toBe("fail");
    expect(planBadge(plan({ status: "rejected", grounding_check: "fail" }))).toBe("rejected");
    expect(planBadge(plan({ status: "error", grounding_check: "fail" }))).toBe("error");
  });

  it("applies only grounded, unsigned steps that map to a decision", () => {
    expect(canApply(plan(), step, {})).toBe(true);
    expect(canApply(plan({ grounding_check: "fail" }), step, {})).toBe(false);
    expect(canApply(plan(), step, { quarantine_lot: {} })).toBe(false);
    expect(canApply(plan(), { ...step, response_action: null }, {})).toBe(false);
  });

  it("pre-fills a reviewable sign-off that names the plan and stays within limits", () => {
    const prefill = prefillFor(plan(), { ...step, action: "x".repeat(400) });
    expect(prefill.planId).toBe(plan().plan_id);
    expect(prefill.reason.startsWith("Per AI action plan 0f8c2a51:")).toBe(true);
    expect(prefill.reason.length).toBeLessThanOrEqual(MAX_REASON);
    expect(prefill.comment.length).toBeLessThanOrEqual(MAX_COMMENT);
    expect(prefillFor(plan(), step).comment).toContain("lot:TM-HS12-261120-121");
  });

  it("resolves cited rows and flags ids missing from the grounding set", () => {
    const rows = [{ id: "alert:a1", source: "alert", ref: "a1", data: { value: 64.2 } }];
    expect(citedRows(rows, ["alert:a1", "lot:nope"])).toEqual([
      { id: "alert:a1", row: rows[0] },
      { id: "lot:nope", row: null },
    ]);
  });

  it("labels citations compactly", () => {
    expect(citationLabel("lot:TM-HS12-261120-121")).toBe("lot TM-HS12-261120-121");
    expect(citationLabel("readings:abc123#4")).toBe("reading 4");
    expect(citationLabel("alert:0f8c2a51-aaaa")).toBe("alert 0f8c2a51");
    expect(citationLabel("alert_rule:mfg-alert-spindle-temp")).toBe("alert rule mfg-alert-spindle-temp");
  });
});
