/**
 * Part 11-style sign-off for one quality decision: a reason (preset or typed),
 * an optional comment and an explicit signature intent. The server records
 * the signer from the login, plus the time, the reason and the meaning.
 */
import { useId, useState } from "react";

import { MAX_COMMENT, MAX_REASON, type SignoffPrefill } from "@/lib/action-plan";
import { SIGNATURE_MEANING_LABEL, type ResponseStep } from "@/lib/manufacturing-story";
import type { DecisionSignoff } from "@/lib/types";

const OTHER = "__other__";

export function DecisionSignoffForm({
  step,
  busy,
  primary,
  onSign,
  onCancel,
  prefill,
}: {
  step: ResponseStep;
  busy: boolean;
  primary: boolean;
  onSign: (signoff: DecisionSignoff) => void;
  onCancel: () => void;
  /** From an AI action plan: pre-fills reason and comment; the person still reviews and signs. */
  prefill?: SignoffPrefill;
}) {
  const id = useId();
  const [choice, setChoice] = useState<string>(prefill ? OTHER : (step.reasons[0] ?? OTHER));
  const [typed, setTyped] = useState(prefill?.reason ?? "");
  const [comment, setComment] = useState(prefill?.comment ?? "");
  const [intent, setIntent] = useState(false);
  const reason = (choice === OTHER ? typed : choice).trim();
  const meaning = SIGNATURE_MEANING_LABEL[step.meaning];
  const ready = intent && reason.length > 0 && !busy;

  return (
    <form
      className="cell-signoff"
      aria-label={`Sign: ${step.title}`}
      data-testid={`signoff-${step.action}`}
      onSubmit={(event) => {
        event.preventDefault();
        if (!ready) return;
        onSign({
          reason,
          comment: comment.trim() || undefined,
          signature_meaning: step.meaning,
          ...(prefill ? { action_plan_id: prefill.planId } : {}),
        });
      }}
    >
      {prefill && (
        <p className="cell-signoff__prefill" data-testid="signoff-prefill">
          Pre-filled from AI action plan {prefill.planId.slice(0, 8)}. Review and edit before signing; the plan is
          marked applied by your signature.
        </p>
      )}
      <label htmlFor={`${id}-reason`}>Reason</label>
      <select id={`${id}-reason`} value={choice} onChange={(e) => setChoice(e.target.value)}>
        {step.reasons.map((r) => (
          <option key={r} value={r}>
            {r}
          </option>
        ))}
        <option value={OTHER}>Other…</option>
      </select>
      {choice === OTHER && (
        <input
          aria-label="Other reason"
          value={typed}
          maxLength={MAX_REASON}
          onChange={(e) => setTyped(e.target.value)}
          placeholder="Why this decision"
        />
      )}
      <label htmlFor={`${id}-comment`}>Comment (optional)</label>
      <textarea
        id={`${id}-comment`}
        rows={2}
        value={comment}
        maxLength={MAX_COMMENT}
        onChange={(e) => setComment(e.target.value)}
      />
      <label className="cell-signoff__intent">
        <input type="checkbox" checked={intent} onChange={(e) => setIntent(e.target.checked)} />
        <span>
          I sign this decision as <b>{meaning}</b>. My login, the time, the reason and this meaning are recorded in the
          audit trail.
        </span>
      </label>
      <div className="cell-signoff__actions">
        <button type="button" className="btn sm ghost" onClick={onCancel} disabled={busy}>
          Cancel
        </button>
        <button type="submit" className={`btn sm${primary ? " primary" : ""}`} disabled={!ready}>
          {busy ? "Signing…" : step.cta}
        </button>
      </div>
    </form>
  );
}
