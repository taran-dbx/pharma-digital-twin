/**
 * Tier-2 opt-in interactive end-to-end test panel for the pipeline-health page.
 *
 * Clicking "Run end-to-end test" fires the validate/start flow (POST
 * `e2e_test.start_endpoint`) via {@link useValidationFlow} — the same driver the
 * setup wizard uses — which spins the interactive cluster (~10 min) and steps
 * through source DNS/TCP/TLS connectivity, Kafka topics, and a
 * produce→consume→land→metrics round-trip. The already-built live-progress panel
 * (`setup-validation/live-progress.tsx`) then renders cluster provisioning →
 * per-step progress → done, so the long spin-up never looks frozen.
 *
 * This is the Tier-2 counterpart to the fast Tier-1 checks: the `e2e`-tier
 * checks that Tier-1 skips (topic connectivity, data-flow) are what this test
 * verifies — surfaced under "verified by the end-to-end test" on the page.
 */
import type { ReactNode } from "react";
import { PlayCircle, RotateCcw, Loader2, Timer, DatabaseZap } from "lucide-react";

import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import type { ValidationFlow } from "@/hooks/use-validation-flow";
import type { E2eTestDescriptor } from "@/lib/admin-api";
import type { PipelineMode } from "@/lib/industries-api";

import { ValidationRow } from "@/pages/setup-validation/components";
import { ClusterStatus, RunProgressHeader, useElapsed } from "@/pages/setup-validation/live-progress";
import {
  COMMON_STEPS,
  REALTIME_STEPS,
  MICROBATCH_STEPS,
  ZEROBUS_STEPS,
  MODE_GENERATION,
  type StepStatus,
  type ValidationStep,
} from "@/pages/setup-validation/types";
import { Eyebrow } from "./components";

function Lane({
  title,
  caption,
  steps,
  statuses,
  stepErrors,
  activeId,
  onRetry,
  children,
}: {
  title: string;
  caption?: string;
  steps: ValidationStep[];
  statuses: Record<string, StepStatus>;
  stepErrors: Record<string, string>;
  activeId: string | null;
  onRetry: (id: string) => void;
  children?: ReactNode;
}) {
  return (
    <div className="rounded-lg border border-border/50 bg-surface-high p-4">
      <h4 className="font-display text-sm font-semibold">{title}</h4>
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

export function E2eTestPanel({
  e2e,
  flow,
  enabledModes,
}: {
  e2e: E2eTestDescriptor;
  /** Lifted from the health page so the top ClusterIndicator and this panel
   * share one live cluster/validation state (single source of truth). */
  flow: ValidationFlow;
  enabledModes: PipelineMode[];
}) {
  const elapsedMs = useElapsed(flow.runStartedAt, flow.running);
  const activeId = flow.running ? flow.currentStepId : null;
  const etaLabel = `~${e2e.eta_minutes} min`;
  const hasKafkaModes =
    enabledModes.includes("realtime") || enabledModes.includes("microbatch");
  const laneCount = enabledModes.length;

  return (
    <div className="glass rounded-lg border border-primary/25 p-5">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div className="min-w-0">
          <Eyebrow>Tier-2 · opt-in</Eyebrow>
          <h3 className="mt-0.5 font-display text-sm font-semibold">End-to-end test</h3>
          <p className="mt-0.5 max-w-xl text-xs text-muted-foreground">
            {e2e.description}
          </p>
        </div>
        <Button
          className="shrink-0 gap-2 transition-all hover:glow-primary"
          onClick={() => void flow.run()}
          disabled={flow.running || flow.busy}
        >
          {flow.running || flow.busy ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : flow.started ? (
            <RotateCcw className="h-4 w-4" />
          ) : (
            <PlayCircle className="h-4 w-4" />
          )}
          {flow.running
            ? "Running…"
            : flow.started
              ? "Re-run end-to-end test"
              : "Run end-to-end test"}
        </Button>
      </div>

      <p className="mt-2 inline-flex items-center gap-1.5 text-[11px] font-ui font-medium text-muted-foreground">
        <Timer className="h-3 w-3" />
        {e2e.spins_interactive_cluster
          ? `Spins an interactive cluster (${etaLabel})`
          : `Serverless runner · no cluster (${etaLabel})`}
      </p>

      {flow.error && (
        <p className="mt-3 rounded-md border border-destructive/40 bg-destructive/[0.05] px-3 py-2 text-xs text-destructive">
          {flow.error}
        </p>
      )}

      {/* Live-progress panel (reused) — shown once a run has started. */}
      {flow.started && (
        <div className="mt-4 space-y-3">
          <RunProgressHeader running={flow.running} currentStepId={flow.currentStepId} elapsedMs={elapsedMs} />
          {hasKafkaModes && (
            <ClusterStatus
              clusterState={flow.clusterState}
              clusterId={flow.clusterId}
              running={flow.running}
              elapsedMs={elapsedMs}
            />
          )}

          {hasKafkaModes && (
            <Lane
              title="Common"
              steps={COMMON_STEPS}
              statuses={flow.statuses}
              stepErrors={flow.stepErrors}
              activeId={activeId}
              onRetry={flow.retry}
            />
          )}

          <div className={cn("grid gap-3", laneCount > 1 && "lg:grid-cols-2")}>
            {enabledModes.includes("realtime") && (
              <Lane
                title="Real-time"
                caption={MODE_GENERATION.realtime}
                steps={REALTIME_STEPS}
                statuses={flow.statuses}
                stepErrors={flow.stepErrors}
                activeId={activeId}
                onRetry={flow.retry}
              >
                {flow.cdfWaiting && (
                  <div className="mt-3 flex items-start gap-2.5 rounded-lg border border-warning/40 bg-warning/[0.08] p-3">
                    <DatabaseZap className="mt-0.5 h-4 w-4 shrink-0 text-warning" />
                    <div className="space-y-2">
                      <p className="text-xs text-muted-foreground">
                        Enable the managed Lakebase → Delta sync in the Databricks UI, then confirm
                        to verify <span className="font-mono">STREAMING</span>.
                      </p>
                      <Button
                        size="sm"
                        variant="outline"
                        className="gap-1.5 text-xs"
                        onClick={() => void flow.confirmCdf()}
                      >
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
                statuses={flow.statuses}
                stepErrors={flow.stepErrors}
                activeId={activeId}
                onRetry={flow.retry}
              />
            )}

            {enabledModes.includes("zerobus") && (
              <Lane
                title="Zerobus"
                caption={MODE_GENERATION.zerobus}
                steps={ZEROBUS_STEPS}
                statuses={flow.statuses}
                stepErrors={flow.stepErrors}
                activeId={activeId}
                onRetry={flow.retry}
              />
            )}
          </div>
        </div>
      )}

      <p className={cn("mt-3 text-[11px] text-muted-foreground", "border-t border-border/40 pt-3")}>
        This Tier-2 run is isolated from the continuously running production jobs.
      </p>
    </div>
  );
}
