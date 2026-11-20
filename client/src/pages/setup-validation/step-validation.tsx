/**
 * Step 4 — Validation run (spec §1.10 / §1.12 M3). Renders the resumable
 * per-step state machine: common steps, then three mode lanes (realtime /
 * microbatch / zerobus). The realtime lane's `cdf_manual` step parks in
 * `waiting_admin` with a callout until the admin confirms the manual sync.
 * The stepping driver itself lives in the parent page.
 */
import type { ReactNode } from "react";
import { Play, RotateCcw, DatabaseZap, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import type { ClusterState } from "@/lib/admin-api";
import type { PipelineMode } from "@/lib/industries-api";
import { ValidationRow } from "./components";
import { ClusterStatus, RunProgressHeader, useElapsed } from "./live-progress";
import {
  COMMON_STEPS,
  REALTIME_STEPS,
  MICROBATCH_STEPS,
  ZEROBUS_STEPS,
  MODE_GENERATION,
  type StepStatus,
  type ValidationStep,
} from "./types";

interface StepProps {
  statuses: Record<string, StepStatus>;
  stepErrors: Record<string, string>;
  running: boolean;
  started: boolean;
  onRun: () => void;
  onConfirmCdf: () => void;
  onRetry: (id: string) => void;
  /** Live-progress inputs from `useValidationFlow` (Fix 2). */
  clusterState?: ClusterState | null;
  clusterId?: string | null;
  currentStepId?: string | null;
  runStartedAt?: number | null;
  /** Which pipeline modes are enabled for this setup. Drives which lanes render. */
  enabledModes?: PipelineMode[];
}

function Lane({
  title,
  caption,
  steps,
  statuses,
  stepErrors,
  activeId,
  badge,
  onRetry,
  children,
}: {
  title: string;
  caption?: string;
  steps: ValidationStep[];
  statuses: Record<string, StepStatus>;
  stepErrors: Record<string, string>;
  activeId?: string | null;
  badge?: ReactNode;
  onRetry: (id: string) => void;
  children?: ReactNode;
}) {
  return (
    <div className="glass rounded-lg p-5">
      <div className="mb-0.5 flex items-center gap-2">
        <h3 className="font-display text-sm font-semibold">{title}</h3>
        {badge}
      </div>
      {caption && <p className="mb-2 text-[11px] text-muted-foreground">{caption}</p>}
      <div className="divide-y divide-border/30">
        {steps.map((s) => (
          <ValidationRow
            key={s.id}
            stepId={s.id}
            label={s.label}
            detail={s.detail}
            failureReason={stepErrors[s.id]}
            status={statuses[s.id] ?? "pending"}
            active={activeId === s.id}
            onRetry={onRetry}
          />
        ))}
      </div>
      {children}
    </div>
  );
}

function ServerlessBadge() {
  return (
    <span className="rounded-full bg-surface-highest px-2 py-0.5 text-[10px] font-ui font-semibold uppercase tracking-wider text-muted-foreground">
      Serverless
    </span>
  );
}

export function StepValidation({
  statuses,
  stepErrors,
  running,
  started,
  onRun,
  onConfirmCdf,
  onRetry,
  clusterState = null,
  clusterId = null,
  currentStepId = null,
  runStartedAt = null,
  enabledModes = ["realtime", "microbatch", "zerobus"],
}: StepProps) {
  const cdfWaiting = statuses["rt_cdf_manual"] === "waiting_admin";
  // Single ticking clock, shared by the run header and the cluster indicator.
  const elapsedMs = useElapsed(runStartedAt, running);
  // Only highlight the active step while a run is actually in flight.
  const activeId = running ? currentStepId : null;

  // Kafka/cluster steps only apply when realtime or microbatch is enabled.
  const hasClusterModes = enabledModes.includes("realtime") || enabledModes.includes("microbatch");

  // Count how many mode lanes will be shown, for grid-cols layout.
  const activeModes = (["realtime", "microbatch", "zerobus"] as PipelineMode[]).filter((m) =>
    enabledModes.includes(m),
  );
  const gridCols =
    activeModes.length === 1
      ? "lg:grid-cols-1"
      : activeModes.length === 2
        ? "lg:grid-cols-2"
        : "lg:grid-cols-3";

  const modeCount = enabledModes.length;
  const modeLabel =
    modeCount === 1
      ? `${modeCount} mode`
      : `${modeCount} modes`;

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <p className="max-w-md text-sm text-muted-foreground">
          Validate {modeLabel} on the{" "}
          {hasClusterModes ? "interactive harness" : "serverless runner"}, then purge test data.
          State is resumable and survives reload.
        </p>
        <div className="flex items-center gap-4">
          <Button
            className="gap-2 hover:glow-primary transition-all"
            onClick={onRun}
            disabled={running}
          >
            {running ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : started ? (
              <RotateCcw className="h-4 w-4" />
            ) : (
              <Play className="h-4 w-4" />
            )}
            {running ? "Running…" : started ? "Re-run validation" : "Run validation"}
          </Button>
        </div>
      </div>

      {/* Live-progress panel — shown once a run has started. */}
      {started && (
        <div className="space-y-3">
          <RunProgressHeader running={running} currentStepId={currentStepId} elapsedMs={elapsedMs} />
          {/* Cluster status only meaningful when a Kafka/cluster mode is active. */}
          {hasClusterModes && (
            <ClusterStatus
              clusterState={clusterState}
              clusterId={clusterId}
              running={running}
              elapsedMs={elapsedMs}
            />
          )}
        </div>
      )}

      {/* Common (Kafka/cluster) lane — only when realtime or microbatch is enabled. */}
      {hasClusterModes && (
        <Lane
          title="Common"
          steps={COMMON_STEPS}
          statuses={statuses}
          stepErrors={stepErrors}
          activeId={activeId}
          onRetry={onRetry}
        />
      )}

      <div className={`grid gap-5 ${gridCols}`}>
        {enabledModes.includes("realtime") && (
          <Lane
            title="Real-time"
            caption={MODE_GENERATION.realtime}
            steps={REALTIME_STEPS}
            statuses={statuses}
            stepErrors={stepErrors}
            activeId={activeId}
            onRetry={onRetry}
          >
            {cdfWaiting && (
              <div className="mt-3 flex items-start gap-3 rounded-lg border border-warning/40 bg-warning/[0.08] p-3">
                <DatabaseZap className="mt-0.5 h-4 w-4 shrink-0 text-warning" />
                <div className="space-y-2">
                  <p className="text-xs text-muted-foreground">
                    Shadow table + trigger are prepped in{" "}
                    <span className="font-mono">lakets_cdf</span>. Manually enable the managed
                    Lakebase → Delta sync in the Databricks UI, then confirm to verify{" "}
                    <span className="font-mono">STREAMING</span>.
                  </p>
                  <Button size="sm" variant="outline" className="gap-1.5 text-xs" onClick={onConfirmCdf}>
                    I&apos;ve enabled the sync
                  </Button>
                </div>
              </div>
            )}
          </Lane>
        )}

        {enabledModes.includes("microbatch") && (
          <Lane
            title="Micro-batch"
            caption={MODE_GENERATION.microbatch}
            steps={MICROBATCH_STEPS}
            statuses={statuses}
            stepErrors={stepErrors}
            activeId={activeId}
            onRetry={onRetry}
          />
        )}

        {enabledModes.includes("zerobus") && (
          <Lane
            title="Zerobus"
            caption={MODE_GENERATION.zerobus}
            steps={ZEROBUS_STEPS}
            statuses={statuses}
            stepErrors={stepErrors}
            activeId={activeId}
            badge={<ServerlessBadge />}
            onRetry={onRetry}
          />
        )}
      </div>
    </div>
  );
}
