/**
 * Two-tier live-health presentational pieces (spec §1.16), consuming the
 * `GET /api/admin/pipelines/{setup_id}/health` payload:
 *
 *  - {@link TierHealthBanner} — top-line verdict rolled up from the **Tier-1**
 *    (`tier==="api"`) checks: Healthy / Unhealthy / Unknown, plus a
 *    pass/fail/skip tally and an explicit "startable / not startable" line so a
 *    non-startable pipeline reads unmistakably (Start affordance disabled).
 *  - {@link ClusterIndicator} — always-visible interactive-cluster state
 *    (Running / Provisioning / Off) derived from `cluster_state`.
 *  - {@link FastChecksCard} — the Tier-1 checklist with per-check pass/fail/skip
 *    pills + `detail`, and a Refresh that re-runs the (seconds-long) sweep.
 *  - {@link E2eVerifiedList} — the `e2e`-tier checks (topic connectivity,
 *    data-flow) surfaced as "verified by the end-to-end test", making the
 *    Tier-1 ↔ Tier-2 relationship explicit.
 *
 * Styled to match the setup-validation flow (glass cards, status pills) so the
 * page reads as one app in both light and dark themes.
 */
import {
  CheckCircle2,
  XCircle,
  MinusCircle,
  AlertTriangle,
  RefreshCw,
  Loader2,
  Server,
  ShieldCheck,
  ShieldAlert,
  HelpCircle,
} from "lucide-react";

import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import type {
  ApiHealth,
  CheckStatus,
  ClusterState,
  HealthCheck,
} from "@/lib/admin-api";
import { Eyebrow } from "./components";

/** Backend check `name` for the secret-scope access check. */
const SCOPE_ACCESS_CHECK = "secrets";

/** Friendly labels for the stable backend check `name`s. */
const CHECK_LABEL: Record<string, string> = {
  uc_catalog_schemas: "Unity Catalog + schemas",
  delta_tables: "Delta tables",
  checkpoint_volume: "Checkpoint volume",
  metrics_table: "Streaming metrics table",
  lakebase_endpoints: "Lakebase endpoints",
  chronotable: "LakeTS ChronoTable",
  cdf_sync: "CDF sync · Lakebase → Delta",
  uc_grants: "Unity Catalog grants",
  [SCOPE_ACCESS_CHECK]: "Secret scope",
  jobs: "Pipeline jobs",
  interactive_cluster: "Interactive cluster",
  kafka_topics: "Kafka topic connectivity",
};

function checkLabel(name: string): string {
  return CHECK_LABEL[name] ?? name.replace(/[_-]+/g, " ");
}

/* ── Per-check status pill ────────────────────────────────────────────── */

const CHECK_META: Record<
  CheckStatus,
  { label: string; text: string; bg: string; icon: typeof CheckCircle2 }
> = {
  pass: { label: "Pass", text: "text-success", bg: "bg-success/15", icon: CheckCircle2 },
  fail: { label: "Fail", text: "text-destructive", bg: "bg-destructive/15", icon: XCircle },
  skip: { label: "Skip", text: "text-muted-foreground", bg: "bg-surface-highest", icon: MinusCircle },
};

function CheckStatusPill({ status }: { status: CheckStatus }) {
  const meta = CHECK_META[status];
  const Icon = meta.icon;
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 rounded-md px-2 py-0.5 text-[11px] font-ui font-semibold",
        meta.bg,
        meta.text,
      )}
    >
      <Icon className="h-3 w-3" />
      {meta.label}
    </span>
  );
}

/** One check row: icon + friendly label + detail + status pill. */
function CheckRow({ check }: { check: HealthCheck }) {
  const failed = check.status === "fail";
  const meta = CHECK_META[check.status];
  const Icon = meta.icon;
  return (
    <div
      className={cn(
        "flex items-start gap-3 py-2",
        failed && "-mx-2 rounded-md bg-destructive/[0.04] px-2",
      )}
    >
      <Icon className={cn("mt-0.5 h-4 w-4 shrink-0", meta.text)} />
      <div className="min-w-0 flex-1">
        <div className="flex items-center justify-between gap-2">
          <span className="text-sm font-medium text-foreground">{checkLabel(check.name)}</span>
          <CheckStatusPill status={check.status} />
        </div>
        {check.detail && (
          <p className="mt-0.5 break-words text-xs text-muted-foreground">{check.detail}</p>
        )}
      </div>
    </div>
  );
}

/* ── Tier-1 top-line health banner ────────────────────────────────────── */

const HEALTH_META: Record<
  ApiHealth,
  { label: string; tone: string; border: string; bg: string; icon: typeof ShieldCheck }
> = {
  healthy: {
    label: "Healthy",
    tone: "text-success",
    border: "border-success/40",
    bg: "bg-success/[0.05]",
    icon: ShieldCheck,
  },
  unhealthy: {
    label: "Unhealthy",
    tone: "text-destructive",
    border: "border-destructive/40",
    bg: "bg-destructive/[0.05]",
    icon: ShieldAlert,
  },
  unknown: {
    label: "Unknown",
    tone: "text-warning",
    border: "border-warning/40",
    bg: "bg-warning/[0.05]",
    icon: HelpCircle,
  },
  draft: {
    label: "Draft",
    tone: "text-muted-foreground",
    border: "border-border/50",
    bg: "bg-surface-high",
    icon: HelpCircle,
  },
};

function tally(checks: HealthCheck[]): Record<CheckStatus, number> {
  return checks.reduce(
    (acc, c) => {
      acc[c.status] += 1;
      return acc;
    },
    { pass: 0, fail: 0, skip: 0 } as Record<CheckStatus, number>,
  );
}

/**
 * Top-line health verdict, driven by the Tier-1 (fast) checks. Shows the rollup
 * status, a pass/fail/skip tally, and an explicit startability line — so a
 * non-startable pipeline reads unmistakably and any Start affordance is off.
 */
export function TierHealthBanner({
  health,
  startable,
  apiChecks,
  hasChecks,
  validationPassed = false,
}: {
  health: ApiHealth;
  startable: boolean;
  apiChecks: HealthCheck[];
  hasChecks: boolean;
  validationPassed?: boolean;
}) {
  const meta = HEALTH_META[health] ?? HEALTH_META.unknown;
  const Icon = meta.icon;
  const counts = tally(apiChecks);
  const failing = apiChecks.filter((c) => c.status === "fail").map((c) => checkLabel(c.name));

  return (
    <div
      className={cn(
        "glass flex flex-col gap-3 rounded-lg border p-5 sm:flex-row sm:items-center sm:justify-between",
        meta.border,
        meta.bg,
      )}
    >
      <div className="flex items-start gap-3">
        <div className={cn("flex h-10 w-10 shrink-0 items-center justify-center rounded-lg bg-surface-high")}>
          <Icon className={cn("h-5 w-5", meta.tone)} />
        </div>
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <span className={cn("font-display text-base font-semibold", meta.tone)}>{meta.label}</span>
            <span className="text-xs text-muted-foreground">· fast checks (Tier-1)</span>
          </div>
          {health === "draft" && validationPassed ? (
            <p className="mt-0.5 text-xs text-muted-foreground">
              <span className="font-medium text-success">Validation passed</span> for every selected
              mode. Deploy and start the jobs before this pipeline can become Healthy.
            </p>
          ) : !hasChecks ? (
            <p className="mt-0.5 text-xs text-muted-foreground">
              Live checks unavailable — no workspace client. Showing the deploy-gate rollup and
              resource ledger below.
            </p>
          ) : startable ? (
            <p className="mt-0.5 text-xs text-muted-foreground">
              All fast checks pass — the pipeline is <span className="font-medium text-success">startable</span>.
              Topic connectivity and data-flow are verified by the end-to-end test.
            </p>
          ) : (
            <p className="mt-0.5 text-xs text-muted-foreground">
              <span className="font-medium text-foreground">Not startable</span> —
              {failing.length
                ? ` resolve: ${failing.join(", ")}.`
                : " no fast check has affirmatively passed yet."}
            </p>
          )}
        </div>
      </div>

      {hasChecks && (
        <div className="flex shrink-0 items-center gap-4 text-xs font-ui">
          <Tally label="Pass" value={counts.pass} dot="bg-success" />
          {counts.fail > 0 && <Tally label="Fail" value={counts.fail} dot="bg-destructive" />}
          {counts.skip > 0 && <Tally label="Skip" value={counts.skip} dot="bg-muted-foreground/60" />}
        </div>
      )}
    </div>
  );
}

function Tally({ label, value, dot }: { label: string; value: number; dot: string }) {
  return (
    <span className="inline-flex items-center gap-1.5 text-muted-foreground">
      <span className={cn("h-2 w-2 rounded-full", dot)} aria-hidden="true" />
      <span className="font-semibold tabular-nums text-foreground">{value}</span>
      {label}
    </span>
  );
}

/* ── Always-visible interactive-cluster indicator ─────────────────────── */

function clusterView(state: ClusterState | null): {
  label: string;
  sub: string;
  tone: string;
  bg: string;
  icon: typeof Server;
  spin: boolean;
} {
  const s = (state ?? "").toUpperCase();
  if (s === "RUNNING") {
    return {
      label: "Interactive cluster: Running",
      sub: "The interactive validation harness is live.",
      tone: "text-success",
      bg: "bg-success/[0.08]",
      icon: CheckCircle2,
      spin: false,
    };
  }
  if (s === "PENDING" || s === "RESTARTING" || s === "RESIZING") {
    return {
      label: "Interactive cluster: Provisioning",
      sub: "Spin-up can take 5–10 minutes.",
      tone: "text-primary",
      bg: "bg-primary/[0.06]",
      icon: Loader2,
      spin: true,
    };
  }
  if (s === "TERMINATING") {
    return {
      label: "Interactive cluster: Terminating",
      sub: "Shutting down after the end-to-end test.",
      tone: "text-warning",
      bg: "bg-warning/[0.08]",
      icon: AlertTriangle,
      spin: false,
    };
  }
  // TERMINATED / UNKNOWN / null — off. Tier-1 never needs it.
  return {
    label: "Interactive cluster: Off",
    sub: "No cluster running. The end-to-end test spins one on demand (~10 min).",
    tone: "text-muted-foreground",
    bg: "bg-surface-high",
    icon: Server,
    spin: false,
  };
}

/** A prominent, always-visible cluster-state indicator. */
export function ClusterIndicator({ state }: { state: ClusterState | null }) {
  const v = clusterView(state);
  const Icon = v.icon;
  return (
    <div className={cn("flex items-center gap-3 rounded-lg border border-border/50 p-3", v.bg)}>
      <Icon className={cn("h-5 w-5 shrink-0", v.tone, v.spin && "animate-spin")} />
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2">
          <span className="text-sm font-semibold">{v.label}</span>
          {state && (
            <span className="rounded-full bg-surface-highest px-2 py-0.5 text-[10px] font-ui font-semibold uppercase tracking-wider text-muted-foreground">
              {state}
            </span>
          )}
        </div>
        <p className="mt-0.5 text-xs text-muted-foreground">{v.sub}</p>
      </div>
    </div>
  );
}

/* ── Tier-1 checklist card ────────────────────────────────────────────── */

/**
 * The Tier-1 fast-checks checklist. Auto-loaded (no cluster); the Refresh
 * button re-runs the seconds-long sweep.
 */
export function FastChecksCard({
  checks,
  onRefresh,
  refreshing,
}: {
  checks: HealthCheck[];
  onRefresh: () => void;
  refreshing: boolean;
}) {
  return (
    <div className="glass rounded-lg p-5">
      <div className="mb-3 flex items-start justify-between gap-3">
        <div>
          <Eyebrow>Tier-1</Eyebrow>
          <h3 className="mt-0.5 font-display text-sm font-semibold">Health checks (fast · no cluster)</h3>
          <p className="mt-0.5 text-xs text-muted-foreground">
            Run from the app backend against live resources — seconds, no interactive cluster.
          </p>
        </div>
        <Button variant="outline" size="sm" className="shrink-0 gap-1.5" onClick={onRefresh} disabled={refreshing}>
          {refreshing ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="h-3.5 w-3.5" />}
          Refresh
        </Button>
      </div>

      {checks.length === 0 ? (
        <p className="rounded-md border border-border/50 bg-surface-high px-3 py-6 text-center text-xs text-muted-foreground">
          Live checks unavailable — no workspace client is configured for this session.
        </p>
      ) : (
        <div className="divide-y divide-border/30">
          {checks.map((c) => (
            <CheckRow key={c.name} check={c} />
          ))}
        </div>
      )}
    </div>
  );
}

/** The `e2e`-tier checks (skipped by Tier-1), listed as "verified by the end-to-end test". */
export function E2eVerifiedList({ checks }: { checks: HealthCheck[] }) {
  if (checks.length === 0) return null;
  return (
    <div className="mt-3 space-y-1.5 rounded-md border border-border/50 bg-surface-high px-3 py-2.5">
      <span className="block text-[10px] font-ui font-semibold uppercase tracking-wider text-muted-foreground">
        Verified by the end-to-end test
      </span>
      {checks.map((c) => (
        <div key={c.name} className="flex items-start gap-2 text-xs">
          <MinusCircle className="mt-0.5 h-3 w-3 shrink-0 text-muted-foreground/70" />
          <div className="min-w-0">
            <span className="font-medium text-foreground">{checkLabel(c.name)}</span>
            {c.detail && <span className="text-muted-foreground"> — {c.detail}</span>}
          </div>
        </div>
      ))}
    </div>
  );
}
