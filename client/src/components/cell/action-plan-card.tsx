/**
 * "AI action plan" card in the incident panel. Generates an advisory plan from
 * the grounded endpoint, shows every claim with its citations (click a chip to
 * see the exact row), the grounding-check badge and the model/version, and
 * hands a step to the existing signed response form — it never applies itself.
 */
import { useCallback, useEffect, useState } from "react";
import { Bot, ShieldAlert, ShieldCheck } from "lucide-react";

import { clockOf } from "@/components/cell/cell-kpis";
import {
  BADGE_LABEL,
  canApply,
  citationLabel,
  citedRows,
  planBadge,
  prefillFor,
  type SignoffPrefill,
} from "@/lib/action-plan";
import { api } from "@/lib/api";
import type { CellDecisions } from "@/lib/manufacturing-story";
import type { ActionPlanClaim, ActionPlanRecord, AlertResponseAction, GroundingRow } from "@/lib/types";

function Citations({ ids, rows, onOpen }: { ids: string[]; rows: GroundingRow[] | null; onOpen: (id: string) => void }) {
  return (
    <span className="cell-plan__cites">
      {ids.map((id) => {
        const missing = rows !== null && !rows.some((r) => r.id === id);
        return (
          <button
            key={id}
            type="button"
            className={`cell-plan__cite${missing ? " is-missing" : ""}`}
            title={missing ? `${id} is not in the grounding set` : `Show ${id}`}
            onClick={() => onOpen(id)}
          >
            {citationLabel(id)}
          </button>
        );
      })}
    </span>
  );
}

function Claim({ label, claim, rows, onOpen }: { label: string; claim: ActionPlanClaim; rows: GroundingRow[] | null; onOpen: (id: string) => void }) {
  return (
    <p className="cell-plan__claim">
      <span>{label}</span>
      {claim.text} <Citations ids={claim.citations} rows={rows} onOpen={onOpen} />
    </p>
  );
}

export function ActionPlanCard({
  alertId,
  mode,
  decisions,
  canSign,
  onApply,
}: {
  alertId: string;
  mode?: string;
  decisions: CellDecisions;
  /** Decisions can only be signed while the episode is open. */
  canSign: boolean;
  onApply: (action: AlertResponseAction, prefill: SignoffPrefill) => void;
}) {
  const [plan, setPlan] = useState<ActionPlanRecord | null>(null);
  const [rows, setRows] = useState<GroundingRow[] | null>(null);
  const [openId, setOpenId] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setPlan(null);
    setRows(null);
    api
      .listActionPlans(alertId)
      .then((body) => {
        if (!cancelled) setPlan(body.plans[0] ?? null);
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
    };
  }, [alertId]);

  const generate = useCallback(async () => {
    setBusy(true);
    setError(null);
    try {
      setPlan(await api.generateActionPlan(alertId, mode));
      setRows(null);
      setOpenId(null);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "The AI action plan was not generated");
      const latest = await api.listActionPlans(alertId).catch(() => null);
      if (latest?.plans[0]) setPlan(latest.plans[0]);
    } finally {
      setBusy(false);
    }
  }, [alertId, mode]);

  const open = useCallback(
    async (id: string) => {
      setOpenId((current) => (current === id ? null : id));
      if (rows === null && plan) {
        try {
          setRows((await api.getActionPlanTrace(plan.plan_id)).grounding_rows);
        } catch (err: unknown) {
          setError(err instanceof Error ? err.message : "Grounding trace unavailable");
        }
      }
    },
    [plan, rows],
  );

  const badge = plan ? planBadge(plan) : null;
  const body = plan?.plan ?? null;
  const shown = openId && rows ? citedRows(rows, [openId])[0] : null;

  return (
    <section className="cell-plan" aria-label="AI action plan" data-testid="ai-action-plan">
      <header className="cell-plan__head">
        <span className="cellops-eyebrow">
          <Bot aria-hidden="true" /> AI action plan
        </span>
        {badge && (
          <span className={`cell-plan__badge is-${badge}`} data-testid="grounding-badge" title={plan?.grounding_reasons.join("\n")}>
            {badge === "pass" ? <ShieldCheck aria-hidden="true" /> : <ShieldAlert aria-hidden="true" />}
            {BADGE_LABEL[badge]}
          </span>
        )}
        <button type="button" className="btn sm" onClick={() => void generate()} disabled={busy}>
          {busy ? "Generating…" : plan ? "Regenerate" : "Generate plan"}
        </button>
      </header>

      {plan && (
        <p className="cell-plan__meta">
          {plan.model ?? plan.endpoint}
          {plan.model_version ? ` · ${plan.model_version}` : ""} · {plan.prompt_template_version} · {clockOf(plan.requested_at)}
          {plan.applied_by ? ` · applied by ${plan.applied_by.split("@")[0]} ${clockOf(plan.applied_at)}` : ""}
        </p>
      )}

      {plan && body ? (
        <div className="cell-plan__body">
          <Claim label="Summary" claim={body.summary} rows={rows} onOpen={(id) => void open(id)} />
          <Claim label="Risk score" claim={body.risk_narrative} rows={rows} onOpen={(id) => void open(id)} />
          <p className="cell-plan__claim">
            <span>Disposition</span>
            Quarantine {body.disposition.quarantine.serials.length} part(s) in {body.disposition.quarantine.lots.join(", ") || "no lot"};{" "}
            NCR {body.disposition.ncr.required ? "yes" : "no"}; maintenance request {body.disposition.maintenance_request.required ? "yes" : "no"}.{" "}
            {body.disposition.cmm_inspection.scope}{" "}
            <Citations
              ids={[...new Set([...body.disposition.quarantine.citations, ...body.disposition.cmm_inspection.citations])]}
              rows={rows}
              onOpen={(id) => void open(id)}
            />
          </p>
          <ol className="cell-plan__steps">
            {body.steps.map((step) => {
              const action = step.response_action;
              return (
              <li key={step.order}>
                <span className="cell-plan__owner">{step.owner_role}</span>
                <div>
                  {step.action} <Citations ids={step.citations} rows={rows} onOpen={(id) => void open(id)} />
                </div>
                {action && (
                  <button
                    type="button"
                    className="btn sm ghost"
                    disabled={!canSign || !canApply(plan, step, decisions)}
                    title={
                      decisions[action]
                        ? "Already signed"
                        : !canSign
                          ? "The episode is closed; decisions can no longer be signed"
                          : badge === "pass"
                          ? "Opens the signed response form pre-filled; you review and sign"
                          : "Only a plan that passed the grounding check can be applied"
                    }
                    onClick={() => onApply(action, prefillFor(plan, step))}
                  >
                    Apply via response workflow
                  </button>
                )}
              </li>
              );
            })}
          </ol>
        </div>
      ) : (
        plan && (
          <p className="cell-plan__empty">
            {plan.status === "error" ? plan.error : "The answer did not match the plan schema."} Logged in the audit trail.
          </p>
        )
      )}

      {shown && (
        <figure className="cell-plan__row" data-testid="cited-row">
          <figcaption>
            <code>{shown.id}</code>
            {shown.row ? ` · ${shown.row.source}` : " · not in the grounding set"}
          </figcaption>
          {shown.row && <pre>{JSON.stringify(shown.row.data, null, 2)}</pre>}
        </figure>
      )}

      {plan && plan.grounding_reasons.length > 0 && (
        <ul className="cell-plan__reasons" aria-label="Grounding check reasons">
          {plan.grounding_reasons.slice(0, 5).map((reason) => (
            <li key={reason}>{reason}</li>
          ))}
        </ul>
      )}
      {error && (
        <p className="cell-response__error" role="alert">
          {error}
        </p>
      )}
      <p className="cell-plan__note">
        Advisory only: grounded in the cited Lakebase / Unity Catalog rows and logged (prompt hash, rows, answer). It
        changes nothing until a qualified person signs a decision.
      </p>
    </section>
  );
}
