import { describe, expect, it } from "vitest";

import { usesPipelineGenie } from "@/components/dashboard/genie-copilot";
import { spaceStateOf } from "@/hooks/use-pipeline-genie";

describe("per-pipeline Genie availability", () => {
  it("is used whenever a pipeline exists, regardless of industry", () => {
    for (const setupId of ["setup-mfg-1", "setup-pharma-1", "setup-energy-1"]) {
      expect(usesPipelineGenie(setupId)).toBe(true);
    }
  });

  it("falls back to the workspace-wide copilot without a pipeline", () => {
    expect(usesPipelineGenie(null)).toBe(false);
    expect(usesPipelineGenie(undefined)).toBe(false);
    expect(usesPipelineGenie("")).toBe(false);
  });
});

describe("spaceStateOf", () => {
  it("separates an unresolved pipeline and an unsupported industry from 'no space yet'", () => {
    expect(spaceStateOf(null, null, true)).toBe("unknown_pipeline");
    expect(spaceStateOf({ available: false, space_id: null, supported: false, industry: "mining" }, null)).toBe(
      "unsupported",
    );
    expect(spaceStateOf({ available: false, space_id: null, supported: true, industry: "mining" }, null)).toBe(
      "not_provisioned",
    );
  });

  it("maps status bodies to graceful states for every industry", () => {
    expect(spaceStateOf(null, null)).toBe("loading");
    expect(spaceStateOf(null, "boom")).toBe("error");
    expect(spaceStateOf({ available: true, space_id: "s1" }, null)).toBe("ready");
    expect(spaceStateOf({ available: false, space_id: null, reason: "none yet" }, null)).toBe("not_provisioned");
    expect(spaceStateOf({ available: false, space_id: "s1", reason: "allowlist" }, null)).toBe("unavailable");
  });
});
