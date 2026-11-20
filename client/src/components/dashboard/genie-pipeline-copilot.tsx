/**
 * Per-pipeline Genie copilot: talks to the Genie space provisioned for one
 * pipeline setup × mode. Gated on `/status`; shows starter chips in an empty
 * conversation, follow-up chips from Genie's suggested questions, and a
 * chart/table + SQL block per query attachment.
 */
import { useEffect, useMemo, useState } from "react";

import { GenieComposer, GenieDrawer } from "@/components/dashboard/genie-drawer";
import { GenieQueryAttachment } from "@/components/dashboard/genie-query-attachment";
import { usePipelineGenie, type PipelineTranscriptEntry, type SpaceState } from "@/hooks/use-pipeline-genie";
import { statusLabel, suggestedQuestions } from "@/lib/genie-messages";
import type { IndustryAssets, PipelineMode } from "@/lib/industries-api";
import type { AlertRecord } from "@/lib/types";

const SPACE_STATUS_TEXT: Record<SpaceState, string> = {
  loading: "Checking…",
  ready: "Ready",
  not_provisioned: "Not provisioned",
  unavailable: "Unavailable",
  unsupported: "Not available",
  unknown_pipeline: "Not found",
  error: "Unavailable",
};

interface GeniePipelineCopilotProps {
  industry: string;
  mode: PipelineMode;
  setupId: string;
  assets: IndustryAssets | null;
  alert: AlertRecord | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  isAdmin?: boolean;
}

function QuestionChips({
  questions,
  label,
  testId,
  disabled,
  onPick,
}: {
  questions: readonly string[];
  label: string;
  testId: string;
  disabled: boolean;
  onPick: (question: string) => void;
}) {
  if (questions.length === 0) return null;
  return (
    <div className="genie-drawer__prompts" role="group" aria-label={label} data-testid={testId}>
      {questions.map((q) => (
        <button
          type="button"
          key={q}
          className="btn sm genie-chip"
          disabled={disabled}
          title={q}
          onClick={() => onPick(q)}
        >
          {q}
        </button>
      ))}
    </div>
  );
}

function AssistantEntry({
  entry,
  setupId,
  mode,
}: {
  entry: PipelineTranscriptEntry;
  setupId: string;
  mode: PipelineMode;
}) {
  return (
    <article className="genie-msg genie-msg--assistant" data-testid="genie-answer">
      <span>Genie</span>
      {entry.text && <p>{entry.text}</p>}
      {entry.attachments.map((att) => (
        <div key={att.key} className="genie-msg__attachment">
          {att.text && <p>{att.text}</p>}
          {att.query && entry.conversationId && entry.messageId && (
            <GenieQueryAttachment
              setupId={setupId}
              mode={mode}
              conversationId={entry.conversationId}
              messageId={entry.messageId}
              attachmentId={att.attachmentId}
              query={att.query}
            />
          )}
        </div>
      ))}
    </article>
  );
}

export function GeniePipelineCopilot({
  industry,
  mode,
  setupId,
  assets,
  alert,
  open,
  onOpenChange,
  isAdmin = false,
}: GeniePipelineCopilotProps) {
  const genie = usePipelineGenie(setupId, mode);
  const [question, setQuestion] = useState("");
  const assetId = assets?.sites[0]?.assets[0]?.asset_id;

  useEffect(() => {
    if (alert && open) {
      setQuestion(`Explain this alert (${alert.fault_code || alert.rule_id}) and recommend a safe next action.`);
    }
  }, [alert, open]);

  // Keep the question of a failed turn (e.g. an expired conversation) in the
  // composer so it can be resent without retyping.
  const { failedQuestion } = genie;
  useEffect(() => {
    if (failedQuestion) setQuestion((current) => current || failedQuestion);
  }, [failedQuestion]);

  const followUps = useMemo(() => {
    const last = [...genie.transcript].reverse().find((e) => e.role === "assistant");
    return last ? suggestedQuestions(last.attachments) : [];
  }, [genie.transcript]);

  const { spaceState } = genie;
  const locked = !genie.available || genie.busy || genie.pending;

  const ask = (text: string) => {
    void genie.send(text).then((sent) => {
      if (sent) setQuestion("");
    });
  };

  const statusText = genie.pending ? statusLabel(genie.pollStatus) : SPACE_STATUS_TEXT[spaceState];
  const announcement = useMemo(() => {
    const last = [...genie.transcript].reverse().find((e) => e.role === "assistant");
    if (!last) return "";
    return [last.text, ...last.attachments.map((a) => a.text)].filter(Boolean).join(" ");
  }, [genie.transcript]);

  const notices = (
    <>
      {spaceState === "loading" && (
        <p className="genie-drawer__hint" aria-busy="true">
          Loading Genie space…
        </p>
      )}
      {spaceState === "unknown_pipeline" && (
        <div className="genie-drawer__disabled" data-testid="genie-disabled" role="status">
          <strong>This pipeline could not be resolved</strong>
          <p>Genie has no record of this pipeline. Reload the dashboard, or ask an admin to check the pipeline setup.</p>
        </div>
      )}
      {spaceState === "unsupported" && (
        <div className="genie-drawer__disabled" data-testid="genie-disabled" role="status">
          <strong>Genie is not available for this industry</strong>
          {genie.status?.reason && <p>{genie.status.reason}</p>}
        </div>
      )}
      {(spaceState === "not_provisioned" || spaceState === "unavailable") && (
        <div className="genie-drawer__disabled" data-testid="genie-disabled" role="status">
          <strong>
            {spaceState === "unavailable" ? "Genie space unavailable" : "Genie space not provisioned yet"}
          </strong>
          {genie.status?.reason && <p>{genie.status.reason}</p>}
          {spaceState === "not_provisioned" &&
            (isAdmin ? (
              <button
                type="button"
                className="btn sm"
                data-testid="genie-create-space"
                disabled={genie.provisioning}
                onClick={() => void genie.provision()}
              >
                {genie.provisioning ? "Creating Genie space…" : "Create Genie space"}
              </button>
            ) : (
              <p className="genie-drawer__hint">Ask an admin to create the Genie space for this pipeline.</p>
            ))}
          {genie.provisionError && (
            <p className="genie-drawer__error" data-testid="genie-create-error" role="alert">
              {genie.provisionError}
            </p>
          )}
        </div>
      )}
      {spaceState === "error" && (
        <p className="genie-drawer__error" data-testid="genie-disabled" role="alert">
          Genie space status unavailable: {genie.statusError}
        </p>
      )}
      {genie.error && (
        <div className="genie-drawer__error-row">
          <p className="genie-drawer__error" data-testid="genie-error" role="alert">
            {genie.error}
          </p>
          {genie.failedQuestion && (
            <button
              type="button"
              className="btn sm"
              data-testid="genie-retry"
              disabled={locked}
              onClick={() => void genie.retry()}
            >
              Retry
            </button>
          )}
        </div>
      )}
    </>
  );

  return (
    <GenieDrawer
      open={open}
      onOpenChange={onOpenChange}
      subtitle={`${industry} · ${mode}${assetId ? ` · ${assetId}` : ""} · pipeline space`}
      statusText={statusText}
      notices={notices}
      announcement={announcement}
      composer={
        <GenieComposer value={question} onChange={setQuestion} onSubmit={ask} disabled={locked} />
      }
    >
      {genie.transcript.length === 0 ? (
        <>
          <p className="genie-drawer__hint">
            Ask about this pipeline’s data. Genie can explain and recommend; it cannot change alerts.
          </p>
          <QuestionChips
            questions={genie.starters.questions}
            label="Starter questions"
            testId="genie-starters"
            disabled={locked}
            onPick={ask}
          />
          {genie.starters.error && <p className="genie-drawer__hint">Starter questions unavailable.</p>}
        </>
      ) : (
        genie.transcript.map((entry) =>
          entry.role === "user" ? (
            <article key={entry.id} className="genie-msg genie-msg--user">
              <span>You</span>
              <p>{entry.text}</p>
            </article>
          ) : (
            <AssistantEntry key={entry.id} entry={entry} setupId={setupId} mode={mode} />
          ),
        )
      )}
      {genie.pending && (
        <p className="genie-drawer__hint" aria-busy="true">
          {statusLabel(genie.pollStatus)}…
        </p>
      )}
      {!genie.pending && (
        <QuestionChips
          questions={followUps}
          label="Suggested follow-up questions"
          testId="genie-followups"
          disabled={locked}
          onPick={ask}
        />
      )}
    </GenieDrawer>
  );
}
