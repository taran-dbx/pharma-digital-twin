import { clockOf } from "@/components/cell/cell-kpis";
import { RESPONSE_STEPS, SPINDLE_RULE_ID } from "@/lib/manufacturing-story";
import type { AlertEpisode, AlertHistoryResponse, ProductionState } from "@/lib/types";
import { formatNumber, toEpochSeconds } from "@/lib/utils";

const WINDOW_S = 3600;
const TICK_S = 600;
const LIST_LIMIT = 6;

const RULE_LABEL: Record<string, string> = {
  [SPINDLE_RULE_ID]: "Spindle 3 temp",
  "mfg-alert-vibration": "Spindle vibration",
};

const DECISION_LABEL = Object.fromEntries(RESPONSE_STEPS.map((s) => [s.action, s.done])) as Record<string, string>;

interface Span {
  from: number;
  to: number;
  open: boolean;
}

function stoppedSpans(changes: ProductionState[], current: ProductionState | null, start: number, now: number): Span[] {
  const ordered = [...changes].sort((a, b) => String(a.changed_at).localeCompare(String(b.changed_at)));
  const spans: Span[] = [];
  let from: number | null = null;
  if (ordered[0]?.state === "running") from = start;
  for (const change of ordered) {
    const at = toEpochSeconds(change.changed_at);
    if (change.state === "stopped" && from === null) from = at;
    if (change.state === "running" && from !== null) {
      spans.push({ from, to: at, open: false });
      from = null;
    }
  }
  if (from === null && current?.state === "stopped" && ordered.length === 0) from = start;
  if (from !== null) spans.push({ from, to: now, open: true });
  return spans;
}

function durationText(seconds: number): string {
  if (seconds < 60) return `${Math.max(1, Math.round(seconds))} s`;
  const m = Math.floor(seconds / 60);
  return m < 60 ? `${m} min ${Math.round(seconds % 60)} s` : `${Math.floor(m / 60)} h ${m % 60} min`;
}

function episodeSpan(episode: AlertEpisode, now: number): Span {
  const from = toEpochSeconds(episode.episode_opened_at);
  const cleared = episode.cleared_at ? toEpochSeconds(episode.cleared_at) : undefined;
  return { from, to: cleared ?? now, open: cleared === undefined };
}

export function AlertTimeline({
  history,
  current,
  now,
  error,
}: {
  history: AlertHistoryResponse | null;
  current: ProductionState | null;
  now: number;
  error: string | null;
}) {
  const start = now - WINDOW_S;
  const pct = (t: number) => `${Math.min(100, Math.max(0, ((t - start) / WINDOW_S) * 100))}%`;
  const width = (span: Span) =>
    `${Math.max(0.6, ((Math.min(span.to, now) - Math.max(span.from, start)) / WINDOW_S) * 100)}%`;
  const episodes = history?.episodes ?? [];
  const rules = [SPINDLE_RULE_ID, ...new Set(episodes.map((e) => e.rule_id ?? "").filter((r) => r && r !== SPINDLE_RULE_ID))];
  const stops = stoppedSpans(history?.production ?? [], current, start, now);
  const firstTick = Math.ceil(start / TICK_S) * TICK_S;
  const ticks = Array.from({ length: Math.floor((now - firstTick) / TICK_S) + 1 }, (_, i) => firstTick + i * TICK_S).filter(
    (t) => now - t > TICK_S * 0.4,
  );
  const listed = episodes.slice(0, LIST_LIMIT);

  return (
    <section className="panel cellops-timeline" aria-label="Alert history" data-testid="alert-timeline">
      <header className="cellops-timeline__head">
        <div>
          <span className="cellops-eyebrow">Alert history</span>
          <h3>Last 60 minutes</h3>
        </div>
        <span className="cellops-timeline__count">
          {episodes.length} episode{episodes.length === 1 ? "" : "s"} · {stops.length} stop{stops.length === 1 ? "" : "s"}
        </span>
      </header>

      <div className="cellops-timeline__chart" role="img" aria-label="Alert episodes and production stops over the last hour">
        {rules.map((rule) => (
          <div className="cellops-timeline__row" key={rule}>
            <span className="cellops-timeline__label">{RULE_LABEL[rule] ?? rule}</span>
            <div className="cellops-timeline__track">
              {episodes
                .filter((e) => (e.rule_id ?? "") === rule)
                .map((e) => {
                  const span = episodeSpan(e, now);
                  if (span.to < start) return null;
                  return (
                    <span
                      key={`${e.alert_id}-${e.episode_opened_at}`}
                      className={`cellops-timeline__bar sev-${e.severity ?? "info"}${span.open ? " is-open" : ""}`}
                      style={{ left: pct(span.from), width: width(span) }}
                      title={`${e.fault_code ?? rule} · ${clockOf(e.episode_opened_at)} · peak ${
                        e.peak_value === null ? "—" : formatNumber(e.peak_value, 1)
                      }`}
                    >
                      {e.decisions.map((d) => (
                        <i
                          key={d.action}
                          className="cellops-timeline__decision"
                          style={{
                            left: `${Math.min(100, Math.max(0, ((toEpochSeconds(d.created_at) - span.from) / Math.max(1, span.to - span.from)) * 100))}%`,
                          }}
                          title={`${DECISION_LABEL[d.action] ?? d.action} · ${clockOf(d.created_at)}`}
                        />
                      ))}
                    </span>
                  );
                })}
            </div>
          </div>
        ))}
        <div className="cellops-timeline__row">
          <span className="cellops-timeline__label">Cell stopped</span>
          <div className="cellops-timeline__track">
            {stops.map((span) => (
              <span
                key={span.from}
                className={`cellops-timeline__stop${span.open ? " is-open" : ""}`}
                style={{ left: pct(span.from), width: width(span) }}
                title={`Stopped ${clockOf(new Date(span.from * 1000).toISOString())}${
                  span.open ? " · still stopped" : ` · ${durationText(span.to - span.from)}`
                }`}
              />
            ))}
          </div>
        </div>
        <div className="cellops-timeline__axis" aria-hidden="true">
          {ticks.map((t) => (
            <span key={t} style={{ left: pct(t) }}>
              {clockOf(new Date(t * 1000).toISOString()).slice(0, 5)}
            </span>
          ))}
          <span className="is-now" style={{ left: "100%" }}>
            now
          </span>
        </div>
      </div>

      {error && !history ? (
        <p className="cellops-timeline__empty">Alert history unavailable · {error}</p>
      ) : listed.length === 0 ? (
        <p className="cellops-timeline__empty">No alerts in the last hour.</p>
      ) : (
        <ol className="cellops-timeline__list">
          {listed.map((e) => {
            const span = episodeSpan(e, now);
            return (
              <li key={`${e.alert_id}-${e.episode_opened_at}`} className={`is-${e.status}`}>
                <time className="tnum">{clockOf(e.episode_opened_at)}</time>
                <i className={`cellops-sev sev-${e.severity ?? "info"}`} aria-hidden="true" />
                <div className="cellops-timeline__what">
                  <b>{RULE_LABEL[e.rule_id ?? ""] ?? e.fault_code ?? "Alert"}</b>
                  <span>
                    {e.fault_code && <code>{e.fault_code}</code>}
                    peak {e.peak_value === null ? "—" : formatNumber(e.peak_value, 1)}
                    {e.threshold !== null ? ` / ${formatNumber(e.threshold, 0)}` : ""} ·{" "}
                    {span.open ? `open ${durationText(now - span.from)}` : durationText(span.to - span.from)}
                  </span>
                </div>
                <div className="cellops-timeline__decisions">
                  {e.decisions.map((d) => (
                    <span key={d.action}>{DECISION_LABEL[d.action] ?? d.action}</span>
                  ))}
                </div>
                <span className={`cellops-timeline__status is-${e.status}`}>{e.status}</span>
              </li>
            );
          })}
        </ol>
      )}
    </section>
  );
}
