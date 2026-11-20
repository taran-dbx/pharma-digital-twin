import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { ApiError } from "@/lib/api";
import {
  answerText,
  classifyStatus,
  isPending,
  normalizeAttachments,
  plainAnswer,
  statusLabel,
  suggestedQuestions,
} from "@/lib/genie-messages";
import { startGeniePoll, type GeniePollOutcome } from "@/lib/genie-poll";

describe("status classification", () => {
  it.each(["SUBMITTED", "FETCHING_METADATA", "FILTERING_CONTEXT", "ASKING_AI", "PENDING_WAREHOUSE", "EXECUTING_QUERY"])(
    "%s keeps polling",
    (status) => {
      expect(isPending(classifyStatus(status))).toBe(true);
    },
  );

  it.each(["COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"])("%s is terminal", (status) => {
    expect(isPending(classifyStatus(status))).toBe(false);
  });

  it("maps unknown/new statuses to UNKNOWN, which keeps polling and reads 'Working'", () => {
    expect(classifyStatus("SOMETHING_NEW")).toBe("UNKNOWN");
    expect(classifyStatus(undefined)).toBe("UNKNOWN");
    expect(isPending("UNKNOWN")).toBe(true);
    expect(statusLabel(classifyStatus("ASKING_AI"))).toBe("Working");
  });
});

describe("answer text", () => {
  const full = (overrides: Record<string, unknown>) => ({
    attachment_id: "a",
    text: null,
    query: null,
    suggested_questions: [],
    ...overrides,
  });

  it("never uses message content; prefers the error", () => {
    expect(answerText("FAILED", "boom", [])).toBe("boom");
  });

  it("returns null when attachments carry the answer", () => {
    expect(answerText("COMPLETED", null, normalizeAttachments([full({ text: "42 min" })]))).toBeNull();
    expect(answerText("COMPLETED", null, normalizeAttachments([full({ query: { sql: "SELECT 1", description: null } })]))).toBeNull();
  });

  it("falls back to fixed per-status copy", () => {
    expect(answerText("COMPLETED", null, [])).toBe("No answer");
    expect(answerText("CANCELLED", null, [])).toBe("Cancelled");
    expect(answerText("FAILED", null, [])).toBe("Genie could not complete this investigation.");
  });

  it("flattens text attachments for the static transcript (string or {content})", () => {
    expect(plainAnswer("COMPLETED", null, [{ text: { content: "Hot spindle" } }, { text: "Check coolant" }])).toBe(
      "Hot spindle\n\nCheck coolant",
    );
    expect(plainAnswer("COMPLETED", null, [])).toBe("No answer");
  });

  it("normalises all four keys and keeps a missing id as null", () => {
    const [view] = normalizeAttachments([
      { attachment_id: null, text: null, query: { sql: "SELECT 1", description: "d" }, suggested_questions: ["q"] },
    ]);
    expect(view).toEqual({
      key: "att-0",
      attachmentId: null,
      text: null,
      query: { sql: "SELECT 1", description: "d" },
      suggestedQuestions: ["q"],
    });
    expect(suggestedQuestions([view, view])).toEqual(["q"]);
  });
});

describe("startGeniePoll", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  const options = { intervalMs: 100, deadlineMs: 1000, maxErrors: 3 };

  async function run(fetchOnce: () => Promise<{ status: string }>, ms: number) {
    const outcomes: Array<GeniePollOutcome<{ status: string }>> = [];
    const statuses: string[] = [];
    startGeniePoll(fetchOnce, (s) => statuses.push(s), (o) => outcomes.push(o), options);
    await vi.advanceTimersByTimeAsync(ms);
    return { outcomes, statuses };
  }

  it("polls SUBMITTED → ASKING_AI → COMPLETED with one request in flight", async () => {
    const seq = ["SUBMITTED", "ASKING_AI", "COMPLETED"];
    let inFlight = 0;
    let maxInFlight = 0;
    const fetchOnce = vi.fn(async () => {
      inFlight += 1;
      maxInFlight = Math.max(maxInFlight, inFlight);
      await new Promise((r) => setTimeout(r, 250)); // slower than the interval
      inFlight -= 1;
      return { status: seq.shift() ?? "COMPLETED" };
    });
    const { outcomes, statuses } = await run(fetchOnce, 2000);
    expect(statuses).toEqual(["SUBMITTED", "ASKING_AI", "COMPLETED"]);
    expect(outcomes).toEqual([{ kind: "done", status: "COMPLETED", message: { status: "COMPLETED" } }]);
    expect(maxInFlight).toBe(1);
  });

  it("reports expired on 404", async () => {
    const { outcomes } = await run(() => Promise.reject(new ApiError("unknown conversation", 404)), 500);
    expect(outcomes).toEqual([{ kind: "expired" }]);
  });

  it("tolerates transient errors, then gives up after 3 in a row", async () => {
    let calls = 0;
    const flaky = () => {
      calls += 1;
      return calls <= 2 ? Promise.reject(new ApiError("bad gateway", 502)) : Promise.resolve({ status: "COMPLETED" });
    };
    expect((await run(flaky, 1000)).outcomes[0]).toMatchObject({ kind: "done" });

    const broken = () => Promise.reject(new ApiError("bad gateway", 502));
    expect((await run(broken, 1000)).outcomes).toEqual([{ kind: "error", message: "bad gateway" }]);
  });

  it("times out at the deadline", async () => {
    const { outcomes } = await run(() => Promise.resolve({ status: "ASKING_AI" }), 2000);
    expect(outcomes).toEqual([{ kind: "timeout" }]);
  });
});
