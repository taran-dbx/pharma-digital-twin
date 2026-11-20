/**
 * State + polling for the per-pipeline Genie space
 * (`/api/genie/pipelines/{setup_id}/{mode}/…`). All updates are immutable;
 * mount with a `key` of setupId+mode to reset on pipeline change.
 */
import { useCallback, useEffect, useRef, useState } from "react";

import { api, isApiStatus } from "@/lib/api";
import { answerText, normalizeAttachments, type GenieAttachmentView } from "@/lib/genie-messages";
import { GENIE_EXPIRED_MESSAGE, GENIE_TIMEOUT_MESSAGE, startGeniePoll } from "@/lib/genie-poll";
import type { PipelineMode } from "@/lib/industries-api";
import type { GenieMessageStatus, GeniePipelineStatus } from "@/lib/types";

/**
 * loading → ready | not_provisioned (no space yet) | unavailable (space exists,
 * unusable) | unsupported (industry has no per-pipeline Genie) |
 * unknown_pipeline (backend 404: the id does not resolve) | error.
 */
export type SpaceState =
  | "loading"
  | "ready"
  | "not_provisioned"
  | "unavailable"
  | "unsupported"
  | "unknown_pipeline"
  | "error";

export interface PipelineTranscriptEntry {
  id: string;
  role: "user" | "assistant";
  text: string | null;
  status?: GenieMessageStatus;
  conversationId?: string;
  messageId?: string;
  attachments: GenieAttachmentView[];
}

interface Starters {
  questions: string[];
  error: string | null;
}

function errorText(err: unknown, fallback: string): string {
  return err instanceof Error ? err.message : fallback;
}

export function spaceStateOf(
  status: GeniePipelineStatus | null,
  statusError: string | null,
  unknownPipeline = false,
): SpaceState {
  if (unknownPipeline) return "unknown_pipeline";
  if (statusError) return "error";
  if (!status) return "loading";
  if (status.supported === false) return "unsupported";
  if (status.available) return "ready";
  return status.space_id ? "unavailable" : "not_provisioned";
}

function usePipelineStatus(setupId: string, mode: PipelineMode) {
  const [status, setStatus] = useState<GeniePipelineStatus | null>(null);
  const [statusError, setStatusError] = useState<string | null>(null);
  const [unknownPipeline, setUnknownPipeline] = useState(false);
  const [starters, setStarters] = useState<Starters>({ questions: [], error: null });
  const [reloadTick, setReloadTick] = useState(0);
  const reload = useCallback(() => {
    setStatus(null);
    setStatusError(null);
    setUnknownPipeline(false);
    setReloadTick((n) => n + 1);
  }, []);

  useEffect(() => {
    let cancelled = false;
    const loadStarters = () =>
      api
        .getPipelineGenieStarters(setupId, mode)
        .then((s) => {
          const questions = Array.isArray(s.questions) ? s.questions.filter((q) => typeof q === "string" && q) : [];
          if (!cancelled) setStarters({ questions, error: null });
        })
        .catch((err: unknown) => {
          if (!cancelled) setStarters({ questions: [], error: errorText(err, "Starter questions unavailable") });
        });

    api
      .getPipelineGenieStatus(setupId, mode)
      .then((body) => {
        if (cancelled) return;
        setStatus(body);
        setStatusError(null);
        setUnknownPipeline(false);
        if (body.available && body.supported !== false) void loadStarters();
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        // The backend answers "no space yet" with 200 + available:false; a 404
        // means the setup/deployment id itself does not resolve.
        if (isApiStatus(err, 404)) {
          setUnknownPipeline(true);
        } else {
          setStatusError(errorText(err, "Genie status unavailable"));
        }
      });
    return () => {
      cancelled = true;
    };
  }, [setupId, mode, reloadTick]);

  return { status, statusError, unknownPipeline, starters, reload };
}

export function usePipelineGenie(setupId: string, mode: PipelineMode) {
  const { status, statusError, unknownPipeline, starters, reload } = usePipelineStatus(setupId, mode);
  const [provisioning, setProvisioning] = useState(false);
  const [provisionError, setProvisionError] = useState<string | null>(null);
  const [conversationId, setConversationId] = useState<string | null>(null);
  const [pendingMessageId, setPendingMessageId] = useState<string | null>(null);
  const [pollStatus, setPollStatus] = useState<GenieMessageStatus>("SUBMITTED");
  const [transcript, setTranscript] = useState<PipelineTranscriptEntry[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  /** Question of the last failed turn; drives the Retry action. */
  const [failedQuestion, setFailedQuestion] = useState<string | null>(null);
  const lastQuestion = useRef<string | null>(null);

  const spaceState = spaceStateOf(status, statusError, unknownPipeline);
  const available = spaceState === "ready";
  const pending = pendingMessageId != null;

  const expire = useCallback(() => {
    setConversationId(null);
    setPendingMessageId(null);
    setFailedQuestion(lastQuestion.current);
    setError(GENIE_EXPIRED_MESSAGE);
  }, []);

  const fail = useCallback((message: string) => {
    setPendingMessageId(null);
    setFailedQuestion(lastQuestion.current);
    setError(message);
  }, []);

  const provision = useCallback(async (): Promise<boolean> => {
    if (provisioning) return false;
    setProvisioning(true);
    setProvisionError(null);
    try {
      await api.provisionPipelineGenieSpace(setupId, mode);
      reload();
      return true;
    } catch (err: unknown) {
      setProvisionError(errorText(err, "Could not create the Genie space"));
      return false;
    } finally {
      setProvisioning(false);
    }
  }, [provisioning, setupId, mode, reload]);

  const send = useCallback(
    async (text: string, options: { retry?: boolean } = {}): Promise<boolean> => {
      const trimmed = text.trim();
      if (!trimmed || !available || busy || pending) return false;
      setBusy(true);
      setError(null);
      setFailedQuestion(null);
      lastQuestion.current = trimmed;
      // A retry resends the same question without duplicating its user turn.
      if (!options.retry) {
        setTranscript((prev) => [...prev, { id: `u-${Date.now()}`, role: "user", text: trimmed, attachments: [] }]);
      }
      try {
        const body = { question: trimmed };
        const ids = conversationId
          ? await api.addPipelineGenieMessage(setupId, mode, conversationId, body)
          : await api.startPipelineGenieConversation(setupId, mode, body);
        setConversationId(ids.conversation_id);
        setPendingMessageId(ids.message_id);
        setPollStatus("SUBMITTED");
        return true;
      } catch (err: unknown) {
        if (conversationId && isApiStatus(err, 404)) expire();
        else fail(errorText(err, "Genie request failed"));
        return false;
      } finally {
        setBusy(false);
      }
    },
    [available, busy, pending, conversationId, setupId, mode, expire, fail],
  );

  useEffect(() => {
    if (!conversationId || !pendingMessageId) return undefined;
    return startGeniePoll(
      () => api.getPipelineGenieMessage(setupId, mode, conversationId, pendingMessageId),
      setPollStatus,
      (outcome) => {
        switch (outcome.kind) {
          case "done": {
            setPendingMessageId(null);
            const attachments = normalizeAttachments(outcome.message.attachments);
            const messageId = outcome.message.message_id || pendingMessageId;
            setTranscript((prev) => [
              ...prev,
              {
                id: messageId,
                role: "assistant",
                text: answerText(outcome.status, outcome.message.error, attachments),
                status: outcome.status,
                conversationId,
                messageId,
                attachments,
              },
            ]);
            return;
          }
          case "expired":
            expire();
            return;
          case "timeout":
            fail(GENIE_TIMEOUT_MESSAGE);
            return;
          case "error":
            fail(outcome.message);
            return;
          default: {
            const exhaustive: never = outcome;
            return exhaustive;
          }
        }
      },
    );
  }, [setupId, mode, conversationId, pendingMessageId, expire, fail]);

  const retry = useCallback(
    () => (failedQuestion ? send(failedQuestion, { retry: true }) : Promise.resolve(false)),
    [failedQuestion, send],
  );

  return {
    status,
    spaceState,
    starters,
    available,
    transcript,
    pollStatus,
    pending,
    busy,
    error,
    statusError,
    send,
    provision,
    provisioning,
    provisionError,
    failedQuestion,
    retry,
  };
}
