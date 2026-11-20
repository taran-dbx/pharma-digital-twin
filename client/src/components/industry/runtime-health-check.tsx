/**
 * End-user runtime readiness gate for a single pipeline mode.
 *
 * This is deliberately *lighter* than the admin's full provisioning
 * validation (that lives on `/setup-validate` and covers create-time
 * concerns — catalog binding, secrets scope, cluster sizing, notebook). By
 * the time an end user is here the pipeline is already provisioned; all they
 * need to know is whether the resources are healthy *right now* before they
 * hit start. So this runs a short set of runtime checks (topic reachable,
 * job healthy, sink/endpoint active, sync streaming, partitions current,
 * metrics writable) and rolls them up to a single verdict.
 *
 * Two entry points:
 *  - "Run health check" runs the checks and shows the verdict.
 *  - "Start pipeline" runs the SAME checks as a pre-flight first; the mode
 *    only starts if every check passes, otherwise start is blocked and the
 *    failing check is named with a "contact your admin" line.
 *
 * The `healthy` verdict is server-driven: when a runtime-readiness signal is
 * available it should be passed in, and `healthy === false` fails the one
 * check flagged `critical` for the mode (blocking start with a "contact your
 * admin" line). It defaults to ready — the deployed pipeline is already
 * provisioned by the time an end user is here, and the real start call
 * enforces readiness server-side (surfacing any failure as a toast).
 */
import { useEffect, useRef, useState } from "react";

import type { PipelineMode } from "@/lib/industries-api";

type CheckState = "pending" | "running" | "passed" | "failed";
type Phase = "idle" | "running" | "passed" | "failed";

interface RuntimeCheck {
  id: string;
  label: string;
  /** The single check that fails when the mock verdict is unhealthy. */
  critical?: boolean;
}

/** Per-mode runtime checklist. Kept short and mode-appropriate: realtime
 * has a Lakebase hot store + CDF cold sync, the batch modes write straight
 * to Delta, and zerobus has no Spark job at all. */
const RUNTIME_CHECKS: Record<PipelineMode, RuntimeCheck[]> = {
  realtime: [
    { id: "kafka", label: "Kafka topic reachable" },
    { id: "job", label: "Real-time job healthy", critical: true },
    { id: "lakebase", label: "Lakebase endpoint active" },
    { id: "cdf", label: "CDF sync streaming" },
    { id: "metrics", label: "Metrics table writable" },
  ],
  microbatch: [
    { id: "kafka", label: "Kafka topic reachable" },
    { id: "job", label: "Micro-batch job healthy", critical: true },
    { id: "delta", label: "Delta table writable" },
    { id: "partitions", label: "Partitions current" },
  ],
  zerobus: [
    { id: "ingest", label: "Zerobus ingest reachable", critical: true },
    { id: "delta", label: "Delta table writable" },
    { id: "partitions", label: "Partitions current" },
  ],
};

const STEP_MS = 420;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function CheckIcon({ state }: { state: CheckState }) {
  if (state === "running") return <span className="spin" aria-label="checking" />;
  if (state === "passed") return <span className="rhc-ic ok">✓</span>;
  if (state === "failed") return <span className="rhc-ic bad">✕</span>;
  return <span className="rhc-ic pend">•</span>;
}

export interface RuntimeHealthCheckProps {
  mode: PipelineMode;
  /**
   * Server runtime-readiness verdict — true (default) = all checks pass.
   * When false, the mode's critical check fails and start is blocked.
   */
  healthy?: boolean;
  /** Parent start mutation is in flight. */
  starting: boolean;
  /** Whether the mode's pipeline is already starting (post-start). */
  modeStarting: boolean;
  /** Called once every pre-flight check passes and the user chose to start. */
  onStart: () => void;
}

export function RuntimeHealthCheck({
  mode,
  healthy = true,
  starting,
  modeStarting,
  onStart,
}: RuntimeHealthCheckProps) {
  const checks = RUNTIME_CHECKS[mode];
  const [phase, setPhase] = useState<Phase>("idle");
  const [states, setStates] = useState<Record<string, CheckState>>(() =>
    Object.fromEntries(checks.map((c) => [c.id, "pending"])),
  );
  const [failedLabel, setFailedLabel] = useState<string | null>(null);
  const runIdRef = useRef(0);

  // Reset whenever the health verdict flips or the mode changes, so a stale
  // pass/fail never lingers — and cancel any in-flight run.
  useEffect(() => {
    runIdRef.current += 1;
    setPhase("idle");
    setFailedLabel(null);
    setStates(Object.fromEntries(checks.map((c) => [c.id, "pending"])));
  }, [healthy, mode, checks]);

  // Abort any in-flight run on unmount.
  useEffect(() => () => void (runIdRef.current += 1), []);

  async function run(thenStart: boolean) {
    const myRun = (runIdRef.current += 1);
    setFailedLabel(null);
    setPhase("running");
    setStates(Object.fromEntries(checks.map((c) => [c.id, "pending"])));

    for (const check of checks) {
      if (runIdRef.current !== myRun) return;
      setStates((s) => ({ ...s, [check.id]: "running" }));
      await sleep(STEP_MS);
      if (runIdRef.current !== myRun) return;

      const fails = !healthy && !!check.critical;
      setStates((s) => ({ ...s, [check.id]: fails ? "failed" : "passed" }));
      if (fails) {
        setFailedLabel(check.label);
        setPhase("failed");
        return;
      }
    }

    if (runIdRef.current !== myRun) return;
    setPhase("passed");
    if (thenStart) onStart();
  }

  const running = phase === "running" || starting || modeStarting;

  return (
    <div className="rhc">
      <div className="rhc-head">
        Runtime health check<span>lightweight · pre-flight</span>
      </div>

      <ul className="rhc-list">
        {checks.map((c) => (
          <li key={c.id} className={states[c.id]}>
            <CheckIcon state={states[c.id]} />
            {c.label}
          </li>
        ))}
      </ul>

      {phase === "passed" && (
        <div className="fc-verdict ok" style={{ marginTop: 10 }}>
          <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth={1.6} aria-hidden="true">
            <circle cx="8" cy="8" r="6.5" />
            <path d="M5.2 8.2 7.1 10l3.7-4" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
          <div className="fc-verdict-txt">
            <strong>Healthy — ready to start</strong>
            <span>All runtime checks passed.</span>
          </div>
        </div>
      )}

      {phase === "failed" && (
        <div className="fc-verdict bad" style={{ marginTop: 10 }}>
          <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth={1.6} aria-hidden="true">
            <path d="M8 1.7 15 14H1z" strokeLinejoin="round" />
            <path d="M8 6.4v3.1" strokeLinecap="round" />
            <circle cx="8" cy="11.6" r=".5" fill="currentColor" />
          </svg>
          <div className="fc-verdict-txt">
            <strong>Not healthy — please contact your admin</strong>
            <span>
              Failed: <b>{failedLabel}</b>. Contact your admin to resolve before this pipeline can
              start.
            </span>
          </div>
        </div>
      )}

      <div className="fc-spacer" />
      <div className="fc-foot">
        {phase === "failed" ? (
          <button className="btn ghost sm" disabled={running} onClick={() => run(false)}>
            ↻ Re-run health check
          </button>
        ) : (
          <>
            <button className="btn ghost sm" disabled={running} onClick={() => run(false)}>
              {phase === "running" ? "Checking…" : phase === "passed" ? "↻ Re-check" : "Run health check"}
            </button>
            <button
              className="btn primary sm"
              disabled={running}
              onClick={() => (phase === "passed" ? onStart() : run(true))}
            >
              {modeStarting ? "starting…" : "▶ Start pipeline"}
            </button>
          </>
        )}
      </div>
    </div>
  );
}
