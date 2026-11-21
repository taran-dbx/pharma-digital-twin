/**
 * Pure helpers for the AI action-plan card: badge state, which steps can be
 * applied, and the pre-filled sign-off for the existing response workflow.
 * Nothing here signs or records anything — the person still reviews the form,
 * ticks the signature intent and submits.
 */
import type { ActionPlanRecord, ActionPlanStep, AlertResponseAction, GroundingRow } from "@/lib/types";

export const MAX_REASON = 280;
export const MAX_COMMENT = 1000;

export type PlanBadge = "pass" | "fail" | "rejected" | "error";

export function planBadge(plan: ActionPlanRecord): PlanBadge {
  if (plan.status === "error") return "error";
  if (plan.status === "rejected") return "rejected";
  return plan.grounding_check;
}

export const BADGE_LABEL: Record<PlanBadge, string> = {
  pass: "Grounding check passed",
  fail: "Grounding check failed",
  rejected: "Answer rejected (schema)",
  error: "Endpoint error",
};

/** A step can be applied only from a plan that passed the grounding check, and only once. */
export function canApply(
  plan: ActionPlanRecord,
  step: ActionPlanStep,
  done: Partial<Record<AlertResponseAction, unknown>>,
): boolean {
  return planBadge(plan) === "pass" && step.response_action !== null && !done[step.response_action];
}

function clip(text: string, max: number): string {
  return text.length <= max ? text : `${text.slice(0, max - 1)}…`;
}

export interface SignoffPrefill {
  planId: string;
  reason: string;
  comment: string;
}

/** Pre-fill for the sign-off form: the reason names the plan, the comment carries the step. */
export function prefillFor(plan: ActionPlanRecord, step: ActionPlanStep): SignoffPrefill {
  const reason = clip(`Per AI action plan ${plan.plan_id.slice(0, 8)}: ${step.action}`, MAX_REASON);
  const cited = step.citations.join(", ");
  const summary = plan.plan?.summary.text ?? "";
  const comment = clip(
    `AI advisory (${plan.model ?? plan.endpoint}, ${plan.prompt_template_version}); reviewed before signing. ` +
      `${summary} Cited rows: ${cited}`,
    MAX_COMMENT,
  );
  return { planId: plan.plan_id, reason, comment };
}

/** The cited rows, in citation order; unknown ids come back as `null` so the UI can flag them. */
export function citedRows(rows: GroundingRow[], ids: string[]): Array<{ id: string; row: GroundingRow | null }> {
  const byId = new Map(rows.map((row) => [row.id, row]));
  return ids.map((id) => ({ id, row: byId.get(id) ?? null }));
}

/** Short label for a citation chip, e.g. `lot:TM-HS12-261120-121` → `lot TM-HS12-261120-121`. */
export function citationLabel(id: string): string {
  const [source, ...rest] = id.split(":");
  const key = rest.join(":");
  if (source === "alert" || source === "alert_event" || source === "alert_response" || source === "risk_score") {
    return `${source.replace("_", " ")} ${key.slice(0, 8)}`;
  }
  if (source === "readings") return `reading ${key.split("#")[1] ?? key}`;
  return `${source.replace("_", " ")} ${key}`;
}
