import { Check, Sparkles } from "lucide-react";

import { clockOf } from "@/components/cell/cell-kpis";
import type { MachiningCellModel } from "@/components/cell/use-cell-model";
import { heatEventText } from "@/components/dashboard/cell-station-card";
import {
  RESPONSE_STEPS,
  SPINDLE_RULE_ID,
  STATION_INFO,
  decisionLabel,
  formatCountdown,
  isActiveAlert,
} from "@/lib/manufacturing-story";
import type { AlertEpisode, AlertRecord, AlertResponseAction, ProductionState } from "@/lib/types";
import { formatNumber, toEpochSeconds } from "@/lib/utils";

type IncidentState = "alert" | "stopped" | "restarting" | "clear";

const STATE_LABEL: Record<IncidentState, string> = {
  alert: "Action required",
  stopped: "Cell stopped",
  restarting: "Restarting",
  clear: "All clear",
};

function who(email: string | null | undefined): string {
  if (!email) return "operator";
  return email.split("@")[0] || email;
}

function durationText(seconds: number): string {
  if (seconds < 60) return `${Math.round(seconds)} s`;
  return `${Math.floor(seconds / 60)} min ${Math.round(seconds % 60)} s`;
}

interface Step {
  key: string;
  label: string;
  at?: string | null;
  action?: { label: string; onClick: () => void; busy: boolean };
}

/** The cell's stop and resume that belong to this alert episode, if any. */
function episodeStops(alert: AlertRecord, current: ProductionState | null, changes: ProductionState[]) {
  const opened = alert.opened_at ? toEpochSeconds(alert.opened_at) : Number.NaN;
  const cleared = alert.cleared_at ? toEpochSeconds(alert.cleared_at) : Number.POSITIVE_INFINITY;
  const ordered = [...changes, ...(current?.changed_at ? [current] : [])]
    .filter((c) => c.changed_at && toEpochSeconds(c.changed_at) >= opened)
    .sort((a, b) => String(a.changed_at).localeCompare(String(b.changed_at)));
  const stop = ordered.find((c) => c.state === "stopped" && toEpochSeconds(c.changed_at) <= cleared);
  const resume = stop ? ordered.find((c) => c.state === "running" && String(c.changed_at) > String(stop.changed_at)) : undefined;
  return { stoppedAt: stop?.changed_at, resumedAt: resume?.changed_at };
}

function Lifecycle({ steps, closed }: { steps: Step[]; closed: boolean }) {
  const current = closed ? -1 : steps.findIndex((s) => !s.at);
  return (
    <ol className="cellops-steps" aria-label="Incident lifecycle">
      {steps.map((step, i) => {
        const state = step.at ? "done" : i === current ? "current" : "todo";
        return (
          <li key={step.key} className={`is-${state}`} data-testid={`lifecycle-${step.key}`}>
            <span className="cellops-steps__dot" aria-hidden="true">
              {step.at && <Check />}
            </span>
            <span className="cellops-steps__label">{step.label}</span>
            {step.at ? (
              <time className="tnum">{clockOf(step.at)}</time>
            ) : step.action ? (
              <button type="button" className="btn sm" onClick={step.action.onClick} disabled={step.action.busy}>
                {step.action.label}
              </button>
            ) : (
              <span className="cellops-steps__pending">—</span>
            )}
          </li>
        );
      })}
    </ol>
  );
}

export function IncidentPanel({
  model,
  episodes,
  changes,
  controllable,
  onRecorded,
  onExplain,
}: {
  model: MachiningCellModel;
  episodes: AlertEpisode[];
  changes: ProductionState[];
  controllable: boolean;
  onRecorded: (action: AlertResponseAction) => void;
  onExplain: (alert: AlertRecord) => void;
}) {
  const { episode, production, limit, spindle, now } = model;
  const { alert, decisions, pending, record, acknowledge } = episode;
  const active = alert !== null && isActiveAlert(alert);
  const state: IncidentState =
    production.phase === "stopped"
      ? "stopped"
      : production.phase === "restarting"
        ? "restarting"
        : active
          ? "alert"
          : "clear";
  const recorded = RESPONSE_STEPS.filter((step) => decisions[step.action]);
  const showResponse = active || recorded.length > 0;
  const submit = async (action: AlertResponseAction) => {
    if (await record(action)) onRecorded(action);
  };

  const opened = alert?.opened_at ? toEpochSeconds(alert.opened_at) : undefined;
  const value = spindle.value;
  const over = limit && value !== undefined ? value - limit.threshold : undefined;
  const stops = alert ? episodeStops(alert, production.state, changes) : { stoppedAt: undefined, resumedAt: undefined };
  const lastEpisode = episodes.find((e) => e.rule_id === SPINDLE_RULE_ID && e.status === "cleared") ?? null;

  const lifecycle: Step[] = alert
    ? [
        { key: "opened", label: "Alert opened", at: alert.opened_at },
        {
          key: "acknowledged",
          label: "Acknowledged",
          at: alert.acknowledged_at,
          action: active
            ? { label: "Acknowledge", onClick: () => void acknowledge(), busy: pending !== null }
            : undefined,
        },
        { key: "stopped", label: "Cell stopped", at: stops.stoppedAt },
        { key: "resumed", label: "Production resumed", at: stops.resumedAt },
        { key: "cleared", label: "Alert cleared", at: alert.cleared_at },
      ]
    : [];

  return (
    <section
      className={`panel cellops-incident is-${state}`}
      aria-label="Incident"
      data-testid="incident-panel"
      data-state={state}
    >
      <header className="cellops-incident__head">
        <span className="cellops-eyebrow">Incident</span>
        <span className={`cellops-chip is-${state}`}>{STATE_LABEL[state]}</span>
      </header>

      {active ? (
        <div className="cellops-incident__alert">
          <div className="cellops-incident__title">
            <b>Spindle 3 over limit</b>
            {alert.fault_code && <code>{alert.fault_code}</code>}
          </div>
          <div className="cellops-incident__readout tnum">
            <span className="is-now">
              <b>{value === undefined ? "—" : formatNumber(value, 1)}</b>
              <small>°C</small>
              <em>{production.phase === "stopped" ? "last reading" : "now"}</em>
            </span>
            <span>
              <b>{limit ? formatNumber(limit.threshold, 0) : "—"}</b>
              <small>°C</small>
              <em>limit</em>
            </span>
            <span className={over !== undefined && over > 0 ? "is-over" : ""}>
              <b>{over === undefined ? "—" : `${over > 0 ? "+" : ""}${formatNumber(over, 1)}`}</b>
              <small>°C</small>
              <em>vs limit</em>
            </span>
            <span>
              <b>{opened === undefined ? "—" : formatCountdown(now - opened)}</b>
              <em>open</em>
            </span>
          </div>
          {limit?.action && (
            <p className="cellops-incident__action" data-testid="incident-action">
              <span>Rule action</span>
              <b>{limit.action}</b>
            </p>
          )}
        </div>
      ) : (
        <div className="cellops-incident__clear">
          <b>{state === "clear" ? "No open alerts on the cell" : "Spindle 3 alert cleared"}</b>
          <span>
            Spindle 3 {value === undefined ? "—" : `${formatNumber(value, 1)} °C`} · limit{" "}
            {limit ? `${formatNumber(limit.threshold, 0)} °C` : "—"} · prepared heat event{" "}
            {heatEventText(model.heatEvent)}
          </span>
          {lastEpisode && (
            <span data-testid="last-incident">
              Last incident {clockOf(lastEpisode.episode_opened_at)} · peak{" "}
              {lastEpisode.peak_value === null ? "—" : `${formatNumber(lastEpisode.peak_value, 1)} °C`}
              {lastEpisode.cleared_at
                ? ` · ${durationText(toEpochSeconds(lastEpisode.cleared_at) - toEpochSeconds(lastEpisode.episode_opened_at))}`
                : ""}
              {lastEpisode.decisions.length > 0
                ? ` · ${lastEpisode.decisions.length} decision${lastEpisode.decisions.length === 1 ? "" : "s"}`
                : ""}
            </span>
          )}
        </div>
      )}

      {!active && model.watch.length > 0 && (
        <ul className="cellops-watch" aria-label="Alert rules on the cell" data-testid="rule-watch">
          {model.watch.map((w) => {
            const ratio =
              w.value === undefined || w.value === 0
                ? 0
                : w.operator === "lt"
                  ? w.threshold / w.value
                  : w.value / w.threshold;
            return (
              <li key={w.ruleId} className={w.overLimit ? "is-over" : ""}>
                <div className="cellops-watch__name">
                  <b>{w.label}</b>
                  <code>{w.faultCode}</code>
                </div>
                <span className="cellops-watch__value tnum">
                  {w.value === undefined ? "—" : formatNumber(w.value, w.decimals)}
                  <small>
                    {" "}
                    / {w.operator === "lt" ? "≥" : "≤"} {formatNumber(w.threshold, 0)} {w.unit}
                  </small>
                </span>
                <i className="cellops-watch__bar" aria-hidden="true">
                  <span style={{ width: `${Math.min(100, ratio * 100)}%` }} />
                </i>
              </li>
            );
          })}
        </ul>
      )}

      {state === "stopped" && (
        <div className="cellops-incident__stop" data-testid="production-stop">
          <div className="cellops-incident__stop-head">
            <div>
              <b>Production stopped</b>
              <span>
                {production.state?.reason ?? "Operator stop"} · {who(production.state?.changed_by)} ·{" "}
                {clockOf(production.state?.changed_at)}
              </span>
            </div>
            <span className="cellops-incident__elapsed tnum" aria-label="Time stopped">
              {formatCountdown(now - (production.since ?? now))}
            </span>
          </div>
          <ul>
            <li>CNC-1 and the pack line send no readings while stopped</li>
            <li>Shop utilities keep streaming to Lakebase</li>
          </ul>
          <button
            type="button"
            className="btn primary"
            onClick={() => void production.resume()}
            disabled={production.pending !== null}
          >
            {production.pending === "running" ? "Resuming…" : "Resume production"}
          </button>
        </div>
      )}

      {state === "restarting" && (
        <div className="cellops-incident__restart" role="status">
          <span className="cellops-spinner" aria-hidden="true" />
          <div>
            <b>Restarting the cell</b>
            <span>Resumed {clockOf(production.state?.changed_at)} · waiting for the first CNC-1 reading</span>
          </div>
        </div>
      )}

      {alert && (active || recorded.length > 0 || stops.stoppedAt) && (
        <Lifecycle steps={lifecycle} closed={!!alert.cleared_at} />
      )}

      {showResponse && (
        <ol className="cellops-actions" aria-label="Response">
          {RESPONSE_STEPS.map((step) => {
            const done = decisions[step.action];
            if (!active && !done) return null;
            const stopsCell = step.action === "send_to_inspect";
            return (
              <li key={step.action} className={`${done ? "is-done" : ""}${stopsCell ? " is-primary" : ""}`}>
                <span className="cell-response__station">{STATION_INFO[step.station].code}</span>
                <div className="cellops-actions__body">
                  <b>{done ? decisionLabel(step, model.batchHeld) : step.title}</b>
                  {done ? (
                    <span data-testid={`decision-${step.action}`}>
                      Recorded {clockOf(done.created_at)} by {who(done.recorded_by)}
                      {step.action === "request_maintenance" && done.reference.last_service
                        ? ` · last service: ${done.reference.last_service.notes}`
                        : ""}
                      {step.action === "hold_batch" && !model.batchHeld && production.state?.changed_at
                        ? ` · released on resume ${clockOf(production.state.changed_at)}`
                        : ""}
                    </span>
                  ) : (
                    <span>{controllable || !stopsCell ? step.detail : "Records the inspection; this mode has no cell stop."}</span>
                  )}
                </div>
                {!done && (
                  <button
                    type="button"
                    className={`btn sm${stopsCell ? " primary" : ""}`}
                    disabled={pending !== null}
                    onClick={() => void submit(step.action)}
                  >
                    {pending === step.action ? "Recording…" : step.cta}
                  </button>
                )}
              </li>
            );
          })}
        </ol>
      )}

      {(episode.error || production.error) && (
        <p className="cell-response__error" role="alert">
          {episode.error ?? production.error}
        </p>
      )}

      <footer className="cellops-incident__foot">
        {alert && (
          <button type="button" className="btn sm ghost" onClick={() => onExplain(alert)}>
            <Sparkles aria-hidden="true" />
            Ask Genie about this alert
          </button>
        )}
        <p>
          Decisions and stops are saved in Lakebase. A stop silences CNC-1 and the pack line at the producer; the
          streaming pipeline stays up, so a resume takes seconds.
        </p>
      </footer>
    </section>
  );
}
