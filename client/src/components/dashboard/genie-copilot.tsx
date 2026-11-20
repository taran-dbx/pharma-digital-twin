import { useCallback, useEffect, useMemo, useState } from "react";

import { GenieComposer, GenieDrawer } from "@/components/dashboard/genie-drawer";
import { GeniePipelineCopilot } from "@/components/dashboard/genie-pipeline-copilot";
import { api, isApiStatus } from "@/lib/api";
import { plainAnswer, statusLabel } from "@/lib/genie-messages";
import {
  GENIE_EXPIRED_MESSAGE,
  GENIE_TIMEOUT_MESSAGE,
  startGeniePoll,
} from "@/lib/genie-poll";
import type {
  AlertRecord,
  GenieAllowlistStatus,
  GenieAskRequest,
  GenieMessageStatus,
  GenieStatusResponse,
} from "@/lib/types";
import type { IndustryAssets, PipelineMode } from "@/lib/industries-api";

interface TranscriptEntry {
  id: string;
  role: "user" | "assistant" | "system";
  text: string;
  status?: string;
}

interface GenieCopilotProps {
  industry: string;
  mode: PipelineMode;
  assets: IndustryAssets | null;
  alert: AlertRecord | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  /**
   * Pipeline identity (setup id, or the dashboard's pipeline/deployment id)
   * for the per-pipeline Genie space. Every industry with a pipeline uses it;
   * without one the workspace-wide copilot is shown.
   */
  setupId?: string | null;
  /** Admins get a "Create Genie space" action when the space is missing. */
  isAdmin?: boolean;
}

/** Any industry dashboard that has a pipeline talks to its per-pipeline Genie space. */
export function usesPipelineGenie(setupId: string | null | undefined): setupId is string {
  return Boolean(setupId);
}

function operatorFacingReason(raw: string): string {
  if (
    raw.includes("GENIE_SPACE_ID") ||
    raw.includes("genie-space") ||
    raw.includes("valueFrom")
  ) {
    return "Genie isn’t connected in this workspace. Ask an admin to attach a Genie space to the app.";
  }
  return raw;
}

function disabledReason(config: GenieStatusResponse | null, configError: string | null): string {
  return operatorFacingReason(
    config?.reason ||
      config?.table_allowlist?.requirement ||
      configError ||
      "Genie is not configured for this workspace.",
  );
}

function disabledStatusLabel(status: GenieAllowlistStatus | undefined, hasSpaceReason: boolean): string {
  if (hasSpaceReason) {
    return "Disabled";
  }
  switch (status) {
    case "mismatch":
      return "Allowlist mismatch";
    case "manual_required":
      return "Allowlist unverified";
    case "matched":
    case undefined:
      return "Disabled";
    default: {
      const exhaustive: never = status;
      return exhaustive;
    }
  }
}

function defaultAssetId(assets: IndustryAssets | null): string | undefined {
  return assets?.sites[0]?.assets[0]?.asset_id;
}

function defaultComponentId(assets: IndustryAssets | null, assetId?: string): string | undefined {
  if (!assets || !assetId) return undefined;
  for (const site of assets.sites) {
    const asset = site.assets.find((item) => item.asset_id === assetId);
    if (asset) return asset.components[0]?.component_id;
  }
  return undefined;
}

export function GenieCopilot({ setupId, isAdmin, ...props }: GenieCopilotProps) {
  if (usesPipelineGenie(setupId)) {
    return (
      <GeniePipelineCopilot
        key={`${setupId}:${props.mode}`}
        {...props}
        setupId={setupId}
        isAdmin={isAdmin ?? false}
      />
    );
  }
  return <StaticGenieCopilot {...props} />;
}

/**
 * Workspace-wide (static GENIE_SPACE_ID) copilot. Same endpoints as before, but
 * it now polls through the shared `startGeniePoll` (an UNKNOWN status keeps
 * polling until the 3-minute deadline) and renders `plainAnswer` text.
 */
function StaticGenieCopilot({
  industry,
  mode,
  assets,
  alert,
  open,
  onOpenChange,
}: Omit<GenieCopilotProps, "setupId">) {
  const [config, setConfig] = useState<GenieStatusResponse | null>(null);
  const [configError, setConfigError] = useState<string | null>(null);
  const [question, setQuestion] = useState("");
  const [conversationId, setConversationId] = useState<string | null>(null);
  const [pendingMessageId, setPendingMessageId] = useState<string | null>(null);
  const [pollStatus, setPollStatus] = useState<GenieMessageStatus>("UNKNOWN");
  const [transcript, setTranscript] = useState<TranscriptEntry[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const assetId = defaultAssetId(assets);
  const componentId = defaultComponentId(assets, assetId);
  const disabled = config != null && config.enabled === false;

  useEffect(() => {
    let cancelled = false;
    api
      .getGenieStatus()
      .then((body) => {
        if (!cancelled) {
          setConfig(body);
          setConfigError(null);
        }
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setConfigError(err instanceof Error ? err.message : "Genie status unavailable");
        }
      });
    return () => {
      cancelled = true;
    };
  }, [industry, mode]);

  const prompts = useMemo(() => {
    const items: Array<{ id: string; label: string; question: string }> = [];
    switch (mode) {
      case "realtime":
        items.push({
          id: "live",
          label: "Live stream",
          question: "What does the live SSE stream imply for this asset right now?",
        });
        break;
      case "microbatch":
        items.push({
          id: "batch",
          label: "Micro-batch window",
          question: "How should I read the latest micro-batch Delta window for this asset?",
        });
        break;
      case "zerobus":
        items.push({
          id: "ingest",
          label: "Zerobus ingest",
          question: "What should I inspect in Zerobus ingest for this asset?",
        });
        break;
      default: {
        const exhaustive: never = mode;
        return exhaustive;
      }
    }
    if (assetId) {
      items.push({
        id: "maint",
        label: "Maintenance",
        question: "Summarize maintenance history and likely next inspection for this asset.",
      });
    }
    if (alert) {
      items.push({
        id: "alert",
        label: "Explain this alert",
        question: `Explain this alert (${alert.fault_code || alert.rule_id}) and recommend a safe next action. Do not change alert state.`,
      });
    }
    return items;
  }, [mode, assetId, alert]);

  const askBody = useCallback(
    (text: string): GenieAskRequest => ({
      industry,
      mode,
      question: text,
      asset_id: assetId,
      component_id: componentId,
      alert_id: alert?.id,
    }),
    [industry, mode, assetId, componentId, alert],
  );

  const send = useCallback(
    async (text: string) => {
      const trimmed = text.trim();
      if (!trimmed || disabled || busy) return;
      setBusy(true);
      setError(null);
      setTranscript((prev) => [
        ...prev,
        { id: `u-${Date.now()}`, role: "user", text: trimmed },
      ]);
      try {
        const ids = conversationId
          ? await api.addGenieMessage(conversationId, askBody(trimmed))
          : await api.startGenieConversation(askBody(trimmed));
        setConversationId(ids.conversation_id);
        setPendingMessageId(ids.message_id);
        setPollStatus("SUBMITTED");
      } catch (err: unknown) {
        if (conversationId && isApiStatus(err, 404)) {
          setConversationId(null);
          setError(GENIE_EXPIRED_MESSAGE);
        } else {
          setError(err instanceof Error ? err.message : "Genie request failed");
        }
      } finally {
        setBusy(false);
        setQuestion("");
      }
    },
    [askBody, busy, conversationId, disabled],
  );

  useEffect(() => {
    if (!conversationId || !pendingMessageId) return undefined;
    return startGeniePoll(
      () => api.getGenieMessage(conversationId, pendingMessageId),
      setPollStatus,
      (outcome) => {
        setPendingMessageId(null);
        switch (outcome.kind) {
          case "done":
            setTranscript((prev) => [
              ...prev,
              {
                id: outcome.message.message_id || pendingMessageId,
                role: "assistant",
                text: plainAnswer(outcome.status, outcome.message.error, outcome.message.attachments),
                status: outcome.status,
              },
            ]);
            return;
          case "expired":
            setConversationId(null);
            setError(GENIE_EXPIRED_MESSAGE);
            return;
          case "timeout":
            setError(GENIE_TIMEOUT_MESSAGE);
            return;
          case "error":
            setError(outcome.message);
            return;
          default: {
            const exhaustive: never = outcome;
            return exhaustive;
          }
        }
      },
    );
  }, [conversationId, pendingMessageId]);

  useEffect(() => {
    if (alert && open) {
      setQuestion(
        `Explain this alert (${alert.fault_code || alert.rule_id}) and recommend a safe next action.`,
      );
    }
  }, [alert, open]);

  const loading = config == null && configError == null;
  const pending = pendingMessageId != null;
  const statusText = loading
    ? "Checking…"
    : disabled
      ? disabledStatusLabel(
          config?.table_allowlist?.status,
          Boolean(config?.reason?.includes("GENIE_SPACE_ID")),
        )
      : pending
        ? statusLabel(pollStatus)
        : "Ready";

  const notices = (
    <>
      {loading && (
        <p className="genie-drawer__hint" aria-busy="true">
          Loading Genie configuration…
        </p>
      )}
      {(disabled || configError) && (
        <p className="genie-drawer__disabled" data-testid="genie-disabled" role="status">
          {disabledReason(config, configError)}
        </p>
      )}
      {error && (
        <p className="genie-drawer__error" role="alert">
          {error}
        </p>
      )}
      <div className="genie-drawer__prompts">
        {prompts.map((item) => (
          <button
            type="button"
            key={item.id}
            className="btn sm"
            disabled={disabled || busy}
            onClick={() => void send(item.question)}
          >
            {item.label}
          </button>
        ))}
      </div>
    </>
  );

  return (
    <GenieDrawer
      open={open}
      onOpenChange={onOpenChange}
      subtitle={`${industry} · ${mode}${assetId ? ` · ${assetId}` : ""}`}
      statusText={statusText}
      notices={notices}
      announcement={[...transcript].reverse().find((e) => e.role === "assistant")?.text ?? ""}
      composer={
        <GenieComposer
          value={question}
          onChange={setQuestion}
          onSubmit={(text) => void send(text)}
          disabled={disabled || busy}
        />
      }
    >
      {transcript.length === 0 ? (
        <p className="genie-drawer__hint">
          Ask about this asset, mode, or alert. Genie can explain and recommend; it cannot change alerts.
        </p>
      ) : (
        transcript.map((entry) => (
          <article key={entry.id} className={`genie-msg genie-msg--${entry.role}`}>
            <span>{entry.role === "user" ? "You" : "Genie"}</span>
            <p>{entry.text}</p>
          </article>
        ))
      )}
      {pending && (
        <p className="genie-drawer__hint" aria-busy="true">
          {statusLabel(pollStatus)}…
        </p>
      )}
    </GenieDrawer>
  );
}
