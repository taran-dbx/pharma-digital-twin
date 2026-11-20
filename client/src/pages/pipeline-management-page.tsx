/**
 * Admin "Pipelines Management" list page — a read-and-operate roll-up of every
 * deployed industry pipeline. Renders under the industry-first TwinShell (top
 * bar, no side nav) like the home page, so this file only owns the page body.
 *
 * Wired (Phase 6) to `GET /api/admin/pipelines` for the list + rolled-up
 * health. Start/Stop fires the per-mode deploy lifecycle
 * (`POST .../{mode}/start|stop`) across the pipeline's enabled modes; Delete
 * tears the pipeline down (`DELETE /api/admin/pipelines/{setup_id}`). Admin-
 * gated server-side; 403/503 surface as an inline error, never a crash.
 */
import { useCallback, useEffect, useState } from "react";
import { toast } from "sonner";
import { Factory, FlaskConical, Layers, PlusCircle, Zap } from "lucide-react";
import type { LucideIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Link } from "react-router-dom";
import {
  listAdminPipelines,
  startPipelineMode,
  stopPipelineMode,
  deletePipeline as apiDeletePipeline,
  type ModeView,
  type PipelineListItem,
} from "@/lib/admin-api";
import type { PipelineMode } from "@/lib/industries-api";

import { PipelineListCard } from "./pipeline-management/pipeline-list-card";
import { HealthPill } from "./pipeline-management/health-pill";
import type { DeployedPipeline, Health, PipelineMode as CardMode } from "./pipeline-management/types";

/** Per-industry display metadata (backend rows carry no icon/tagline). */
const INDUSTRY_META: Record<string, { icon: LucideIcon; tagline: string }> = {
  pharmaceutical: { icon: FlaskConical, tagline: "Bioreactor · WFI · batch yield" },
  manufacturing: { icon: Factory, tagline: "Assembly line · CNC · robotics" },
  energy: { icon: Zap, tagline: "Turbines · grid load · substations" },
};

function titleCase(id: string): string {
  return id.replace(/[_-]+/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

/**
 * Map the REAL per-pipeline runtime rollup (`item.status` — `draft` /
 * `running` / `degraded` / `stopped`, derived by `pipeline_view.py` from
 * live Jobs-API state) onto the card's `Health` vocabulary.
 *
 * Deliberately NOT `item.health.overall` — that's a *ledger* rollup (are the
 * UC/Lakebase resources still active in the provisioning ledger?) which can
 * read "healthy" even when the job's task actually crashed or the pipeline
 * was never started. `item.status` is the truthful "is data flowing" signal.
 */
function mapRealStatusToHealth(status: string | undefined): Health {
  if (status === "running") return "healthy";
  if (status === "degraded") return "degraded";
  if (status === "starting") return "degraded";
  if (status === "stopped") return "down";
  return "draft"; // draft, or no real status resolved
}

/** Degraded/draft/stopped callout, naming the crashed task when known. */
function noteForReal(
  status: string | undefined,
  modes: ModeView[],
  validationPassed: boolean,
): string | undefined {
  if (status === "degraded") {
    const reason = modes.find((m) => m.status === "degraded")?.runtime_detail;
    return reason ? `Degraded — ${reason}` : "Degraded — a task crashed; data may not be flowing";
  }
  if (status === "starting") return "Starting — waiting for fresh data from every selected mode";
  if (status === "stopped") return "Jobs stopped — pipeline offline";
  if (status === "draft" || !status) {
    return validationPassed
      ? "Validation passed — deploy and start the selected modes"
      : "Draft — finish setup & validation to deploy";
  }
  return undefined;
}

/** Map a backend list item onto the card's `DeployedPipeline` view model. */
function toCard(item: PipelineListItem): DeployedPipeline {
  const { setup, health } = item;
  const meta = INDUSTRY_META[setup.industry];
  const realModes = item.modes ?? [];
  const modes = (
    realModes.length
      ? realModes.map((m) => m.mode)
      : (health.enabled_modes ?? setup.enabled_modes ?? [])
  ) as CardMode[];
  const runningCount = realModes.filter((m) => m.status === "running").length;
  return {
    id: setup.id,
    industryId: setup.industry,
    industry: titleCase(setup.industry),
    tagline: meta?.tagline ?? "Industry pipeline",
    icon: meta?.icon ?? Layers,
    modes,
    health: mapRealStatusToHealth(item.status),
    // Every selected mode must be deployed and validation-gated before Start.
    startable: realModes.length > 0 && realModes.every((mode) => mode.startable),
    jobsRunning: realModes.length ? runningCount : Number(health.jobs_running ?? 0),
    jobsTotal: realModes.length || Number(health.jobs_total ?? 0),
    resourcesHealthy: Number(health.resources_healthy ?? 0),
    resourcesTotal: Number(health.resources_total ?? 0),
    lastUpdated: setup.updated_at ? new Date(setup.updated_at).toLocaleString() : "—",
    note: noteForReal(item.status, realModes, Boolean(item.validation_passed)),
  };
}

/** Roll-up counts across all pipelines, for the header summary strip. */
function summarize(pipelines: DeployedPipeline[]): Record<Health, number> {
  return pipelines.reduce(
    (acc, p) => ({ ...acc, [p.health]: acc[p.health] + 1 }),
    { healthy: 0, degraded: 0, down: 0, draft: 0 } as Record<Health, number>,
  );
}

export function PipelineManagementPage() {
  const [items, setItems] = useState<PipelineListItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const { pipelines } = await listAdminPipelines();
      setItems(pipelines);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load pipelines");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  async function handleToggleRunning(id: string) {
    const item = items.find((p) => p.setup.id === id);
    if (!item) return;
    // Real running signal (per-mode Jobs-API state) over the ledger-derived
    // `health.jobs_running`, which can be stale/wrong (see `mapRealStatusToHealth`).
    const realModes = item.modes ?? [];
    const allRunning =
      realModes.length > 0 && realModes.every((mode) => mode.status === "running");
    const willRun = realModes.length > 0 ? !allRunning : item.health.jobs_running === 0;
    const modes = realModes.length
      ? realModes
          .filter((mode) => !willRun || (mode.status !== "running" && mode.startable))
          .map((mode) => mode.mode as PipelineMode)
      : ((item.health.enabled_modes ?? item.setup.enabled_modes ?? []) as PipelineMode[]);
    if (modes.length === 0) {
      toast.error("No validated deployed mode is ready to start");
      return;
    }
    const action = willRun ? startPipelineMode : stopPipelineMode;
    try {
      await Promise.all(modes.map((mode) => action(id, mode)));
      toast.success(`${willRun ? "Starting" : "Stopping"} ${titleCase(item.setup.industry)} pipeline`);
      await load();
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "Action failed");
    }
  }

  async function handleDelete(id: string) {
    const item = items.find((p) => p.setup.id === id);
    try {
      await apiDeletePipeline(id);
      toast.success(`Deleted ${item ? titleCase(item.setup.industry) : "pipeline"}`);
      await load();
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "Delete failed");
    }
  }

  const pipelines = items.map(toCard);
  const counts = summarize(pipelines);

  return (
    <div className="mx-auto max-w-5xl space-y-6 pb-16">
      {/* Header */}
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <span className="block text-xs font-ui font-medium uppercase tracking-wider text-muted-foreground">
            Admin · Pipelines
          </span>
          <h1 className="mt-1 font-display text-xl font-semibold tracking-tight">
            Deployed pipelines
          </h1>
          <p className="mt-1 max-w-lg text-sm text-muted-foreground">
            Every provisioned industry pipeline. Monitor overall health, jobs and resources, then
            drill into a pipeline or edit its configuration.
          </p>
        </div>
        <Button asChild className="gap-2 hover:glow-primary transition-all">
          <Link to="/setup-validate">
            <PlusCircle className="h-4 w-4" />
            New pipeline
          </Link>
        </Button>
      </div>

      {/* Roll-up summary strip */}
      <div className="glass flex flex-wrap items-center gap-x-6 gap-y-3 rounded-lg p-4">
        <span className="inline-flex items-center gap-2 text-sm font-medium">
          <Layers className="h-4 w-4 text-muted-foreground" />
          {pipelines.length} pipeline{pipelines.length === 1 ? "" : "s"}
        </span>
        <span className="h-4 w-px bg-border" />
        <span className="inline-flex items-center gap-2 text-sm text-muted-foreground">
          <HealthPill health="healthy" /> {counts.healthy}
        </span>
        <span className="inline-flex items-center gap-2 text-sm text-muted-foreground">
          <HealthPill health="degraded" /> {counts.degraded}
        </span>
        <span className="inline-flex items-center gap-2 text-sm text-muted-foreground">
          <HealthPill health="down" /> {counts.down}
        </span>
        <span className="inline-flex items-center gap-2 text-sm text-muted-foreground">
          <HealthPill health="draft" /> {counts.draft}
        </span>
      </div>

      {/* Pipeline list */}
      {loading ? (
        <div className="glass rounded-lg border border-border/50 p-12 text-center">
          <p className="text-sm text-muted-foreground">Loading pipelines…</p>
        </div>
      ) : error ? (
        <div className="glass rounded-lg border border-destructive/40 bg-destructive/[0.05] p-8 text-center">
          <p className="text-sm text-destructive">Couldn&apos;t load pipelines: {error}</p>
          <Button variant="outline" size="sm" className="mt-4" onClick={() => void load()}>
            Retry
          </Button>
        </div>
      ) : pipelines.length === 0 ? (
        <div className="glass rounded-lg border border-border/50 p-12 text-center">
          <p className="text-sm text-muted-foreground">
            No pipelines deployed. Create one from the setup flow to get started.
          </p>
        </div>
      ) : (
        <div className="space-y-4">
          {pipelines.map((pipeline) => (
            <PipelineListCard
              key={pipeline.id}
              pipeline={pipeline}
              onToggleRunning={handleToggleRunning}
              onDelete={handleDelete}
            />
          ))}
        </div>
      )}
    </div>
  );
}
