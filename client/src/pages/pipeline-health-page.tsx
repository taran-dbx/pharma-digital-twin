/**
 * In-app "Pipeline Resource-Health" detail page — a single deployed pipeline's
 * full resource ledger with per-resource health, an overall Healthy/Degraded
 * banner that names the failing resource, and a top actions row.
 *
 * Wired (Phase 6) to `GET /api/admin/pipelines/{setup_id}/health`. The route
 * param may be a setup id (from the pipelines list) or an industry id (from the
 * home gallery's ⚙ Status link), so we resolve it via the management list when
 * a direct lookup 404s. Admin-gated server-side; 403/503 surface inline.
 *
 * Route (wired in App.tsx): /pipelines/:id/health
 */
import { useCallback, useEffect, useMemo, useState } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { ArrowLeft } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  getPipelineHealth,
  listAdminPipelines,
  type HealthCheck,
  type ModeView,
  type PipelineHealthDetail,
  type ResourceHealthItem,
  type SetupPayload,
} from "@/lib/admin-api";
import { cn } from "@/lib/utils";
import { useValidationFlow } from "@/hooks/use-validation-flow";
import type { PipelineMode } from "@/lib/industries-api";

import { ActionsRow } from "./pipeline-health/actions-row";
import { BuiltOnRow, Eyebrow, ResourceGroupCard } from "./pipeline-health/components";
import {
  ClusterIndicator,
  E2eVerifiedList,
  FastChecksCard,
  TierHealthBanner,
} from "./pipeline-health/health-checks";
import { E2eTestPanel } from "./pipeline-health/e2e-test-panel";
import {
  BRAND,
  rollup,
  type HealthStatus,
  type PipelineHealth,
  type ResourceGroup,
} from "./pipeline-health/types";

function titleCase(id: string): string {
  return id.replace(/[_-]+/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

/** Pick a product brand mark for a ledger resource-type, when one applies. */
function brandForType(type: string): ResourceGroup["brand"] {
  if (type.includes("catalog") || type.includes("schema") || type.includes("volume")) return BRAND.unity;
  if (type.includes("lakebase") || type.includes("branch") || type.includes("endpoint")) return BRAND.lakebase;
  if (type.includes("topic") || type.includes("kafka")) return BRAND.streaming;
  return undefined;
}

/** Map the ledger's coarse health pill onto the page's status vocabulary. */
function toStatus(health: ResourceHealthItem["health"]): HealthStatus {
  if (health === "healthy") return "healthy";
  return "warning"; // degraded (deleted rows are filtered out before mapping)
}

/** Build the page view-model from the backend health detail. */
function toPipelineHealth(detail: PipelineHealthDetail): PipelineHealth {
  const industry = detail.setup.industry;
  const groups: ResourceGroup[] = Object.entries(detail.resources)
    .map(([type, items]) => ({
      id: type,
      title: titleCase(type),
      brand: brandForType(type),
      items: items
        .filter((it) => it.health !== "deleted")
        .map((it) => ({
          label: it.identifier,
          detail: `${titleCase(type)} · ${it.status ?? "unknown"}`,
          status: toStatus(it.health),
          mono: true,
        })),
    }))
    .filter((g) => g.items.length > 0);

  return {
    id: detail.setup.id,
    name: titleCase(industry),
    industry,
    catalog: `${detail.uc_catalog}.${detail.uc_schema}`,
    groups,
  };
}

const MODE_META: Record<string, { label: string; cls: string }> = {
  running: { label: "Running", cls: "text-success border-success/40 bg-success/[0.08]" },
  degraded: { label: "Degraded", cls: "text-warning border-warning/40 bg-warning/[0.08]" },
  stopped: { label: "Stopped", cls: "text-muted-foreground border-border bg-muted/30" },
  draft: { label: "Not deployed", cls: "text-muted-foreground border-border bg-muted/30" },
};

const _BROKEN_RESULTS = new Set(["FAILED", "ERROR", "TIMEDOUT"]);
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

/** Per-mode RUNTIME truth: which modes are actually running / degraded (a task
 * crashed, data not flowing) / stopped / not deployed — with each job task's
 * live state. This is the story the shallow "running" pill was hiding. */
function ModeRuntimeSection({ modes }: { modes: ModeView[] }) {
  if (!modes || modes.length === 0) return null;
  return (
    <div className="glass space-y-3 rounded-lg border border-border/50 p-5">
      <Eyebrow>Pipeline modes · runtime</Eyebrow>
      <div className="space-y-2">
        {modes.map((m) => {
          const meta = MODE_META[m.status] ?? MODE_META.draft;
          return (
            <div key={m.mode} className="rounded-lg border border-border/40 bg-surface-high p-3">
              <div className="flex items-center justify-between gap-3">
                <span className="font-display text-sm font-semibold capitalize">{m.mode}</span>
                <span className={cn("rounded-full border px-2 py-0.5 text-[11px] font-semibold", meta.cls)}>
                  {meta.label}
                </span>
              </div>
              {m.runtime_detail && (
                <p className="mt-1 text-xs text-warning">{m.runtime_detail}</p>
              )}
              {m.tasks && m.tasks.length > 0 && (
                <div className="mt-2 flex flex-wrap gap-2">
                  {m.tasks.map((t) => {
                    const broken =
                      t.life_cycle === "INTERNAL_ERROR" || _BROKEN_RESULTS.has(String(t.result));
                    return (
                      <span
                        key={t.task}
                        title={t.message ?? undefined}
                        className={cn(
                          "rounded border px-1.5 py-0.5 font-mono text-[10px]",
                          broken
                            ? "border-destructive/40 text-destructive"
                            : "border-border text-muted-foreground",
                        )}
                      >
                        {t.task}: {t.result ?? t.life_cycle ?? "?"}
                      </span>
                    );
                  })}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

export function PipelineHealthPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();

  const [detail, setDetail] = useState<PipelineHealthDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async (silent = false) => {
    if (!id) return;
    if (silent) setRefreshing(true);
    else setLoading(true);
    setError(null);
    try {
      let data: PipelineHealthDetail;
      if (UUID_PATTERN.test(id)) {
        data = await getPipelineHealth(id);
      } else {
        // Gallery links use an industry slug. Resolve it before calling the
        // UUID-only health endpoint so the browser does not emit a noisy 500.
        const { pipelines } = await listAdminPipelines();
        const match = pipelines.find(
          (p) => p.setup.industry === id || p.setup.id === id,
        );
        if (!match) throw new Error("pipeline not found");
        data = await getPipelineHealth(match.setup.id);
      }
      setDetail(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load health");
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [id]);

  useEffect(() => {
    void load();
  }, [load]);

  const health = detail ? toPipelineHealth(detail) : null;
  const pipelineId = detail?.setup.id ?? id ?? "";

  const apiChecks = useMemo(
    () => (detail?.checks ?? []).filter((c: HealthCheck) => c.tier === "api"),
    [detail],
  );
  const e2eChecks = useMemo(
    () => (detail?.checks ?? []).filter((c: HealthCheck) => c.tier === "e2e"),
    [detail],
  );

  // Single source of truth for the interactive-cluster lifecycle: the same
  // validate-flow the E2E panel drives. Lifted here so the always-visible top
  // ClusterIndicator reflects the LIVE cluster state during an E2E run — the
  // one-shot `detail.cluster_state` snapshot only reflects page-load time and
  // otherwise reads "off" while the cluster is actually spinning up / running.
  const setup = detail?.setup;
  const industry = setup?.industry ?? "";
  const enabledModes = useMemo(
    () => (setup?.enabled_modes ?? []) as PipelineMode[],
    [setup?.enabled_modes],
  );
  const hasClusterModes =
    enabledModes.includes("realtime") || enabledModes.includes("microbatch");
  const buildPayload = useCallback(
    (): SetupPayload => ({
      flavor: setup?.flavor ?? undefined,
      source_type: setup?.source_type ?? undefined,
      bootstrap_servers: setup?.bootstrap_servers ?? undefined,
      enabled_modes: setup?.enabled_modes ?? undefined,
      warehouse_id: setup?.warehouse_id ?? undefined,
    }),
    [setup],
  );
  const flow = useValidationFlow({ industry, enabledModes, buildPayload });

  // Prefer the live cluster state once an E2E run is under way; fall back to the
  // Tier-1 snapshot before any run has started.
  const liveClusterState = flow.started ? flow.clusterState : (detail?.cluster_state ?? null);

  return (
    <div className="mx-auto max-w-5xl space-y-6 pb-16">
      {/* Header */}
      <div className="space-y-3">
        <Button
          variant="ghost"
          size="sm"
          className="-ml-2 gap-1 text-muted-foreground hover:text-primary"
          onClick={() => navigate("/industries")}
        >
          <ArrowLeft className="h-3.5 w-3.5" />
          Industries
        </Button>
        <div>
          <Eyebrow>Pipeline · Resource Health</Eyebrow>
          <h1 className="mt-1 font-display text-2xl font-bold tracking-tight">
            {health?.name ?? titleCase(id ?? "Pipeline")}
          </h1>
          <p className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-sm text-muted-foreground">
            <span className="capitalize">{health?.industry ?? id}</span>
            <span className="text-border">·</span>
            <span className="break-all font-mono text-xs">{pipelineId}</span>
            {health && (
              <>
                <span className="text-border">·</span>
                <span className="break-all font-mono text-xs">{health.catalog}</span>
              </>
            )}
          </p>
        </div>
        <BuiltOnRow items={[BRAND.streaming, BRAND.zerobus, BRAND.lakebase, BRAND.unity]} />
      </div>

      {loading ? (
        <div className="glass rounded-lg border border-border/50 p-12 text-center">
          <p className="text-sm text-muted-foreground">Loading resource health…</p>
        </div>
      ) : error ? (
        <div className="glass rounded-lg border border-destructive/40 bg-destructive/[0.05] p-8 text-center">
          <p className="text-sm text-destructive">Couldn&apos;t load health: {error}</p>
          <Button variant="outline" size="sm" className="mt-4" onClick={() => void load()}>
            Retry
          </Button>
        </div>
      ) : !detail ? (
        <div className="glass rounded-lg border border-border/50 p-12 text-center">
          <p className="text-sm text-muted-foreground">
            No provisioned resources yet — this pipeline hasn&apos;t been provisioned.
          </p>
        </div>
      ) : (
        <>
          {/* Top-line health — driven by the Tier-1 fast checks. */}
          <TierHealthBanner
            health={detail.health}
            startable={Boolean(detail.startable)}
            apiChecks={apiChecks}
            hasChecks={apiChecks.length > 0}
            validationPassed={Boolean(detail.validation_passed)}
          />

          {/* Per-mode runtime truth: what's actually running / degraded / not deployed. */}
          {detail.modes && detail.modes.length > 0 && <ModeRuntimeSection modes={detail.modes} />}

          {/* Interactive-cluster status only applies to Kafka/Spark modes. */}
          {hasClusterModes && <ClusterIndicator state={liveClusterState} />}

          {/* Actions (Modify config / Restart jobs). */}
          {health && <ActionsRow health={health} />}

          {/* Tier-1 fast checks (auto-loaded; Refresh re-runs in seconds). */}
          <FastChecksCard
            checks={apiChecks}
            onRefresh={() => void load(true)}
            refreshing={refreshing}
          />

          {/* Tier-2 opt-in end-to-end test + the e2e-tier checks it verifies. */}
          <div className="space-y-3">
            <E2eTestPanel
              e2e={detail.e2e_test}
              flow={flow}
              enabledModes={enabledModes}
            />
            <E2eVerifiedList checks={e2eChecks} />
          </div>

          {/* Resource ledger inventory. */}
          {health && health.groups.length > 0 && (
            <div className="space-y-3">
              <Eyebrow>Resource ledger</Eyebrow>
              <div className="grid min-w-0 gap-5 lg:grid-cols-2">
                {health.groups.map((group) => (
                  <ResourceGroupCard
                    key={group.id}
                    group={group}
                    rollupStatus={rollup(group.items)}
                  />
                ))}
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}
