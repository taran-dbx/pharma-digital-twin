// @vitest-environment jsdom
import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const apiMock = vi.hoisted(() => ({
  getPipelineGenieStatus: vi.fn(),
  getPipelineGenieStarters: vi.fn(),
  startPipelineGenieConversation: vi.fn(),
  addPipelineGenieMessage: vi.fn(),
  getPipelineGenieMessage: vi.fn(),
  provisionPipelineGenieSpace: vi.fn(),
}));

vi.mock("@/lib/api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@/lib/api")>()),
  api: apiMock,
}));

import { ApiError } from "@/lib/api";
import { GENIE_EXPIRED_MESSAGE, GENIE_POLL_INTERVAL_MS } from "@/lib/genie-poll";
import { usePipelineGenie } from "@/hooks/use-pipeline-genie";

const READY = { available: true, space_id: "s1", supported: true, industry: "pharmaceutical" };
const IDS = { conversation_id: "c1", message_id: "m1" };
const DONE = {
  conversation_id: "c1",
  message_id: "m1",
  status: "COMPLETED",
  content: "ignored",
  error: null,
  attachments: [{ attachment_id: "a1", text: "All good.", query: null, suggested_questions: [] }],
};

async function flush(ms = 0) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms);
  });
}

async function mountReady() {
  apiMock.getPipelineGenieStatus.mockResolvedValue(READY);
  apiMock.getPipelineGenieStarters.mockResolvedValue({ questions: ["Q1"] });
  const hook = renderHook(() => usePipelineGenie("setup-1", "realtime"));
  await flush();
  return hook;
}

beforeEach(() => {
  vi.useFakeTimers();
  Object.values(apiMock).forEach((fn) => fn.mockReset());
});

afterEach(() => {
  vi.useRealTimers();
});

describe("usePipelineGenie status states", () => {
  it("loads backend starters when the space is ready", async () => {
    const { result } = await mountReady();
    expect(result.current.spaceState).toBe("ready");
    expect(result.current.starters.questions).toEqual(["Q1"]);
  });

  it("treats a 404 from /status as an unresolved pipeline, not 'not provisioned'", async () => {
    apiMock.getPipelineGenieStatus.mockRejectedValue(new ApiError("unknown pipeline", 404));
    const { result } = renderHook(() => usePipelineGenie("setup-x", "realtime"));
    await flush();
    expect(result.current.spaceState).toBe("unknown_pipeline");
    expect(apiMock.getPipelineGenieStarters).not.toHaveBeenCalled();
  });

  it("maps supported:false to unsupported and skips starters", async () => {
    apiMock.getPipelineGenieStatus.mockResolvedValue({
      available: false,
      space_id: null,
      supported: false,
      industry: "mining",
      reason: "Per-pipeline Genie is not available for industry 'mining'.",
    });
    const { result } = renderHook(() => usePipelineGenie("setup-1", "realtime"));
    await flush();
    expect(result.current.spaceState).toBe("unsupported");
    expect(apiMock.getPipelineGenieStarters).not.toHaveBeenCalled();
  });

  it("keeps 'no space yet' (200, available:false) as not_provisioned", async () => {
    apiMock.getPipelineGenieStatus.mockResolvedValue({
      available: false,
      space_id: null,
      supported: true,
      industry: "pharmaceutical",
    });
    const { result } = renderHook(() => usePipelineGenie("setup-1", "realtime"));
    await flush();
    expect(result.current.spaceState).toBe("not_provisioned");
  });

  it("reloads status after the admin creates the space", async () => {
    apiMock.getPipelineGenieStatus
      .mockResolvedValueOnce({ available: false, space_id: null, supported: true })
      .mockResolvedValue(READY);
    apiMock.getPipelineGenieStarters.mockResolvedValue({ questions: [] });
    apiMock.provisionPipelineGenieSpace.mockResolvedValue({ space_id: "s1" });
    const { result } = renderHook(() => usePipelineGenie("setup-1", "realtime"));
    await flush();
    await act(async () => {
      await result.current.provision();
    });
    await flush();
    expect(result.current.spaceState).toBe("ready");
  });
});

describe("usePipelineGenie poll lifecycle", () => {
  it("polls through pending statuses and appends the answer", async () => {
    apiMock.startPipelineGenieConversation.mockResolvedValue(IDS);
    apiMock.getPipelineGenieMessage
      .mockResolvedValueOnce({ ...DONE, status: "SUBMITTED", attachments: [] })
      .mockResolvedValueOnce({ ...DONE, status: "ASKING_AI", attachments: [] })
      .mockResolvedValue(DONE);
    const { result } = await mountReady();

    await act(async () => {
      await result.current.send("How are we doing?");
    });
    expect(result.current.pending).toBe(true);

    await flush(GENIE_POLL_INTERVAL_MS * 4);
    expect(apiMock.getPipelineGenieMessage.mock.calls.length).toBe(3);
    expect(result.current.pending).toBe(false);
    const roles = result.current.transcript.map((e) => e.role);
    expect(roles).toEqual(["user", "assistant"]);
    expect(result.current.transcript[1].attachments[0]?.text).toBe("All good.");
  });

  it("cancels polling on unmount", async () => {
    apiMock.startPipelineGenieConversation.mockResolvedValue(IDS);
    apiMock.getPipelineGenieMessage.mockResolvedValue({ ...DONE, status: "ASKING_AI", attachments: [] });
    const { result, unmount } = await mountReady();
    await act(async () => {
      await result.current.send("Q");
    });
    await flush(GENIE_POLL_INTERVAL_MS);
    const callsBefore = apiMock.getPipelineGenieMessage.mock.calls.length;
    unmount();
    await vi.advanceTimersByTimeAsync(GENIE_POLL_INTERVAL_MS * 10);
    expect(apiMock.getPipelineGenieMessage.mock.calls.length).toBe(callsBefore);
  });

  it("keeps the question for retry after an expired conversation", async () => {
    apiMock.startPipelineGenieConversation.mockResolvedValue(IDS);
    apiMock.getPipelineGenieMessage.mockRejectedValue(new ApiError("unknown conversation", 404));
    const { result } = await mountReady();
    await act(async () => {
      await result.current.send("Which station?");
    });
    await flush(GENIE_POLL_INTERVAL_MS * 2);
    expect(result.current.error).toBe(GENIE_EXPIRED_MESSAGE);
    expect(result.current.failedQuestion).toBe("Which station?");
  });

  it("retry resends the same question without duplicating the user turn", async () => {
    apiMock.startPipelineGenieConversation
      .mockRejectedValueOnce(new ApiError("boom", 500))
      .mockResolvedValue(IDS);
    apiMock.getPipelineGenieMessage.mockResolvedValue(DONE);
    const { result } = await mountReady();

    await act(async () => {
      expect(await result.current.send("Why?")).toBe(false);
    });
    expect(result.current.error).toBe("boom");
    expect(result.current.failedQuestion).toBe("Why?");
    expect(result.current.transcript.filter((e) => e.role === "user")).toHaveLength(1);

    await act(async () => {
      await result.current.retry();
    });
    await flush(GENIE_POLL_INTERVAL_MS * 2);
    expect(apiMock.startPipelineGenieConversation).toHaveBeenCalledTimes(2);
    expect(apiMock.startPipelineGenieConversation.mock.calls[1][2]).toEqual({ question: "Why?" });
    expect(result.current.error).toBeNull();
    expect(result.current.failedQuestion).toBeNull();
    expect(result.current.transcript.map((e) => e.role)).toEqual(["user", "assistant"]);
  });
});
