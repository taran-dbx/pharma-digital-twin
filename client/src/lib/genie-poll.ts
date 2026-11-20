/**
 * Shared Genie message poller: chained setTimeout (exactly one request in
 * flight), a hard deadline, tolerance for transient errors, and a distinct
 * "expired" outcome for 404 (conversation lost, e.g. after an app restart).
 */
import { isApiStatus } from "@/lib/api";
import { classifyStatus, isTerminal } from "@/lib/genie-messages";
import type { GenieMessageStatus } from "@/lib/types";

export const GENIE_POLL_INTERVAL_MS = 1500;
export const GENIE_POLL_DEADLINE_MS = 3 * 60 * 1000;
export const GENIE_POLL_MAX_ERRORS = 3;

export const GENIE_EXPIRED_MESSAGE = "This conversation expired - ask again.";
export const GENIE_TIMEOUT_MESSAGE =
  "Genie is taking too long to answer. Try again, or ask a narrower question.";

export interface PollableMessage {
  status: unknown;
}

export type GeniePollOutcome<M> =
  | { kind: "done"; message: M; status: GenieMessageStatus }
  | { kind: "expired" }
  | { kind: "timeout" }
  | { kind: "error"; message: string };

export interface GeniePollOptions {
  intervalMs: number;
  deadlineMs: number;
  maxErrors: number;
}

interface GenieTestHooks {
  pollIntervalMs?: number;
  pollDeadlineMs?: number;
}

/** Defaults, overridable by QA via `window.__genieTestHooks` (Playwright init script). */
export function pollOptions(): GeniePollOptions {
  const hooks = (globalThis as { __genieTestHooks?: GenieTestHooks }).__genieTestHooks;
  return {
    intervalMs: hooks?.pollIntervalMs ?? GENIE_POLL_INTERVAL_MS,
    deadlineMs: hooks?.pollDeadlineMs ?? GENIE_POLL_DEADLINE_MS,
    maxErrors: GENIE_POLL_MAX_ERRORS,
  };
}

/**
 * Poll `fetchOnce` until a terminal status, 404, deadline, or too many
 * consecutive errors. Returns a cancel function; no callback fires after it.
 */
export function startGeniePoll<M extends PollableMessage>(
  fetchOnce: () => Promise<M>,
  onStatus: (status: GenieMessageStatus) => void,
  onDone: (outcome: GeniePollOutcome<M>) => void,
  options: GeniePollOptions = pollOptions(),
): () => void {
  const deadline = Date.now() + options.deadlineMs;
  let cancelled = false;
  let errors = 0;
  let timer: ReturnType<typeof setTimeout> | undefined;

  const finish = (outcome: GeniePollOutcome<M>) => {
    if (cancelled) return;
    cancelled = true;
    onDone(outcome);
  };

  const schedule = () => {
    if (cancelled) return;
    if (Date.now() >= deadline) {
      finish({ kind: "timeout" });
      return;
    }
    timer = setTimeout(tick, options.intervalMs);
  };

  const tick = () => {
    fetchOnce()
      .then((message) => {
        if (cancelled) return;
        errors = 0;
        const status = classifyStatus(message.status);
        onStatus(status);
        if (isTerminal(status)) finish({ kind: "done", message, status });
        else schedule();
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        if (isApiStatus(err, 404)) {
          finish({ kind: "expired" });
          return;
        }
        errors += 1;
        if (errors >= options.maxErrors) {
          finish({ kind: "error", message: err instanceof Error ? err.message : "Genie poll failed" });
          return;
        }
        schedule();
      });
  };

  tick();
  return () => {
    cancelled = true;
    if (timer !== undefined) clearTimeout(timer);
  };
}
