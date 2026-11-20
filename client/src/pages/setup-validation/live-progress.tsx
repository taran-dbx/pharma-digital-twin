/**
 * Live validation-progress affordances (Fix 2): a cluster-provisioning
 * indicator and a run header (elapsed time + current running step + spinner)
 * so a validation run — kicked from either `/setup-validate` or the health
 * page's "Re-run validation" — always shows visible, ticking progress and
 * never looks frozen during the long (5–10 min) interactive-cluster spin-up.
 *
 * Consumes the `validate/state` fields surfaced by `useValidationFlow`:
 * `cluster_state` (PENDING/RUNNING/TERMINATED) + `cluster_id`, the derived
 * `currentStepId`, and `runStartedAt` (min step `updated_at`).
 */
import { useEffect, useState } from "react";
import { CheckCircle2, Loader2, Server, AlertTriangle } from "lucide-react";

import { cn } from "@/lib/utils";
import type { ClusterState } from "@/lib/admin-api";
import { STEP_LABEL } from "./types";

/** Threshold (ms) after which we reassure the admin the long spin-up is normal. */
const LONG_SPINUP_MS = 90_000;

function formatElapsed(ms: number): string {
  const total = Math.max(0, Math.floor(ms / 1000));
  const m = Math.floor(total / 60);
  const s = total % 60;
  return m > 0 ? `${m}m ${String(s).padStart(2, "0")}s` : `${s}s`;
}

/**
 * A ticking elapsed clock. Recomputes every second while `running`, anchored
 * to `startedAt` (epoch ms) so it stays correct across reloads/resumes.
 */
function useElapsed(startedAt: number | null, running: boolean): number | null {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!running) return;
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, [running]);
  if (startedAt === null) return null;
  return Math.max(0, now - startedAt);
}

/** Cluster-provisioning indicator: PENDING (spinner) → RUNNING (check). */
export function ClusterStatus({
  clusterState,
  clusterId,
  running,
  elapsedMs,
}: {
  clusterState: ClusterState | null;
  clusterId: string | null;
  running: boolean;
  elapsedMs: number | null;
}) {
  const isRunning = clusterState === "RUNNING";
  const isTerminated = clusterState === "TERMINATED";
  // PENDING, an unknown transient state, or "no cluster reported yet while the
  // run is live" all read as "still provisioning".
  const isProvisioning = !isRunning && !isTerminated && (clusterState === "PENDING" || (running && !clusterId));

  const longSpinup = isProvisioning && elapsedMs !== null && elapsedMs > LONG_SPINUP_MS;

  let Icon = Server;
  let tone = "text-muted-foreground";
  let bg = "bg-surface-high";
  let title = "Interactive cluster";
  let sub = "Waiting to provision…";

  if (isProvisioning) {
    Icon = Loader2;
    tone = "text-primary";
    bg = "bg-primary/[0.06]";
    title = "Provisioning interactive cluster…";
    sub = longSpinup
      ? "Still provisioning — cluster spin-up can take 5–10 minutes. Hang tight."
      : "Spin-up can take 5–10 minutes.";
  } else if (isRunning) {
    Icon = CheckCircle2;
    tone = "text-success";
    bg = "bg-success/[0.08]";
    title = "Cluster running";
    sub = "Interactive harness is live.";
  } else if (isTerminated) {
    Icon = AlertTriangle;
    tone = "text-warning";
    bg = "bg-warning/[0.08]";
    title = "Cluster terminated";
    sub = "Re-run validation to provision a fresh cluster.";
  }

  const spin = isProvisioning;

  return (
    <div className={cn("flex items-center gap-3 rounded-lg border border-border/50 p-3", bg)}>
      <Icon className={cn("h-5 w-5 shrink-0", tone, spin && "animate-spin")} />
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2">
          <span className="text-sm font-semibold">{title}</span>
          {clusterState && (
            <span className="rounded-full bg-surface-highest px-2 py-0.5 text-[10px] font-ui font-semibold uppercase tracking-wider text-muted-foreground">
              {clusterState}
            </span>
          )}
        </div>
        <p className="mt-0.5 text-xs text-muted-foreground">{sub}</p>
      </div>
      {clusterId && <span className="shrink-0 font-mono text-[11px] text-muted-foreground">{clusterId}</span>}
    </div>
  );
}

/**
 * Run header: a live "running…" banner naming the current step, with a ticking
 * elapsed clock — the always-moving affordance that proves the run isn't stuck.
 */
export function RunProgressHeader({
  running,
  currentStepId,
  elapsedMs,
}: {
  running: boolean;
  currentStepId: string | null;
  elapsedMs: number | null;
}) {
  if (!running) return null;

  const stepLabel = currentStepId ? STEP_LABEL[currentStepId] ?? currentStepId : null;

  return (
    <div className="flex items-center gap-3 rounded-lg border border-primary/30 bg-primary/[0.05] p-3">
      <Loader2 className="h-4 w-4 shrink-0 animate-spin text-primary" />
      <div className="min-w-0 flex-1 text-sm">
        <span className="font-semibold text-foreground">Validation running…</span>
        {stepLabel && (
          <span className="text-muted-foreground">
            {" "}
            · <span className="font-medium text-foreground">{stepLabel}</span>
          </span>
        )}
      </div>
      {elapsedMs !== null && (
        <span className="shrink-0 font-mono text-xs tabular-nums text-muted-foreground">
          {formatElapsed(elapsedMs)}
        </span>
      )}
    </div>
  );
}

/** Shared elapsed hook re-exported so the cluster indicator can share the clock. */
export { useElapsed };
