/**
 * Industry setup screen — port of `#view-setup` in the design reference
 * (the original design reference): crumbs, setup head, the
 * custom-data toolbar, the
 * pipelines grid (`<PipelineCard>` per mode), and the "How your data
 * flows" panel (`<ArchDiagram>`) + the admin-only Advanced details.
 *
 * The design's `renderProv()`/`#provList` — a separate "Provisioning"
 * panel below the arch diagram — targets an element that never exists in
 * the design's own static markup (`$('provList')` is always null there, so
 * `renderProv()` no-ops); it's dead code in the mockup, so there is
 * deliberately no equivalent section here.
 */
import { useCallback, useEffect, useRef, useState, type CSSProperties } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { toast } from "sonner";

import { PipelineCard } from "@/components/industry/pipeline-card";
import { ArchDiagram } from "@/components/industry/arch-diagram";
import {
  getCatalog,
  type Catalog,
  type IndustryMode,
} from "@/lib/industries-api";
import { listPipelineRuntime, type ModeView, type PipelineListItem } from "@/lib/admin-api";

const NOT_DEPLOYED_STATUSES = new Set(["pending", "deploying", "failed"]);

function hexToRgba(hex: string, alpha: number): string {
  const n = hex.replace("#", "");
  const bigint = parseInt(n, 16);
  return `rgba(${(bigint >> 16) & 255},${(bigint >> 8) & 255},${bigint & 255},${alpha})`;
}

function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "Failed to load industry";
}

export function IndustrySetupPage() {
  const { industry: industryId } = useParams<{ industry: string }>();
  const navigate = useNavigate();

  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  // The REAL per-mode/rollup status for this industry, from the public
  // pipeline view (`listPipelineRuntime()`) — never the curated catalog's
  // `IndustryMode.status`/`Industry.runningCount`, which are fabricated from
  // a legacy table (see `industry-gallery-page.tsx`'s docstring for the full
  // story). The endpoint returns a sanitized runtime view to every authenticated
  // app user, so admin and non-admin cards resolve from the same live state.
  // Runtime is optional enrichment — the page renders from catalog immediately.
  const [adminItem, setAdminItem] = useState<PipelineListItem | null>(null);
  const adminRequestId = useRef(0);
  const refreshAdmin = useCallback(() => {
    if (!industryId) return Promise.resolve();
    const requestId = ++adminRequestId.current;
    return listPipelineRuntime()
      .then(({ pipelines }) => {
        if (requestId !== adminRequestId.current) return;
        setAdminItem(pipelines.find((p) => p.setup.industry === industryId) ?? null);
      })
      .catch(() => {
        if (requestId !== adminRequestId.current) return;
        setAdminItem(null);
      });
  }, [industryId]);

  useEffect(() => {
    setAdminItem(null);
    void refreshAdmin();
    return () => {
      adminRequestId.current += 1;
    };
  }, [refreshAdmin]);

  const refresh = useCallback(() => {
    return Promise.all([
      getCatalog()
        .then((data) => setCatalog(data))
        .catch((err: unknown) => setError(getErrorMessage(err))),
      refreshAdmin(),
    ]);
  }, [refreshAdmin]);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    getCatalog()
      .then((data) => {
        if (!cancelled) setCatalog(data);
      })
      .catch((err: unknown) => {
        if (!cancelled) setError(getErrorMessage(err));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [industryId]);

  // Catalog alone unlocks the page — runtime chips enrich when ready.
  if (loading) {
    return (
      <section id="view-setup">
        <p className="lede">Loading industry…</p>
      </section>
    );
  }

  if (error) {
    return (
      <section id="view-setup">
        <p className="lede" style={{ color: "var(--db-destructive)" }}>
          Couldn&apos;t load this industry: {error}
        </p>
        <Link to="/industries">← Industries</Link>
      </section>
    );
  }

  const industry = catalog?.industries.find((i) => i.id === industryId);

  if (!catalog || !industry) {
    return (
      <section id="view-setup">
        <div className="crumbs">
          <Link to="/industries">Industries</Link>
          <span className="sep">/</span>
          <span>Unknown industry</span>
        </div>
        <p className="lede">
          &ldquo;{industryId}&rdquo; isn&apos;t a curated industry. Go back and pick one from the
          gallery.
        </p>
        <Link to="/industries">← Industries</Link>
      </section>
    );
  }

  const isAdmin = catalog.isAdmin;
  const total = industry.modes.length;
  const resolvedIndustryId = industry.id;

  // Real per-mode status for THIS industry, from the admin pipeline view.
  // `null`/empty means "no real view available" (non-admin viewer, 403/503,
  // or not-yet-loaded) — every consumer below then falls back to the
  // curated `industry.modes`/`industry.runningCount` exactly as before.
  const realModes: ModeView[] | null =
    adminItem?.modes && adminItem.modes.length ? adminItem.modes : null;
  const realModeById = new Map<string, ModeView>((realModes ?? []).map((m) => [m.mode, m]));
  // Public `/api/industries/runtime` strips `job_id` from deployments (see
  // sanitizer tests). Treat `modes[].deployed` as the source of truth, and
  // still honor a `status: "deployed"` row even when the id is omitted.
  const deployedModeIds = new Set<string>([
    ...(adminItem?.modes ?? [])
      .filter((mode) => mode.deployed === true)
      .map((mode) => mode.mode),
    ...(adminItem?.deployments ?? [])
      .filter(
        (deployment) =>
          !NOT_DEPLOYED_STATUSES.has(deployment.status ?? "") &&
          (Boolean(deployment.job_id) || deployment.status === "deployed"),
      )
      .map((deployment) => deployment.mode),
  ]);
  const deployedModeViews = realModes
    ? realModes.filter((mode) => deployedModeIds.has(mode.mode))
    : industry.modes
        .filter((mode) => mode.status === "running")
        .map((mode) => ({
          mode: mode.id,
          status: mode.status,
          health: "healthy",
          startable: true,
          deployed: true,
        }));
  const realRunningCount = realModes ? realModes.filter((m) => m.status === "running").length : null;
  const realRollup = adminItem?.status;

  // Header "X/Y running" + dot — driven by the real rollup/per-mode data
  // whenever it's available, never by the curated (fabricated) count.
  const headerRunningCount = realRunningCount ?? industry.runningCount;
  const headerTotal = realModes ? realModes.length : total;
  const headerDegraded = realRollup === "degraded";
  const headerColor = headerDegraded
    ? "var(--db-warning, #f79009)"
    : headerRunningCount > 0
      ? "var(--db-success, #12b76a)"
      : "var(--db-muted-foreground, #98a2b3)";
  const handleOpenDashboard = (mode: IndustryMode) => {
    navigate(`/i/${resolvedIndustryId}/${mode.id}/dashboard`);
  };

  return (
    <section
      id="view-setup"
      style={
        {
          "--ind": industry.accent,
          "--ind-soft": hexToRgba(industry.accent, 0.12),
          "--ind-line": hexToRgba(industry.accent, 0.5),
        } as CSSProperties
      }
    >
      <div className="crumbs">
        <Link to="/industries">Industries</Link>
        <span className="sep">/</span>
        <span>{industry.name}</span>
      </div>

      <div className="setup-head">
        <div className="sh-left">
          <div className="eyebrow">{industry.name} · industry</div>
          <h1>{industry.name}</h1>
          <p className="lede">{industry.lede}</p>
        </div>
        <div className="sh-right">
          <span className="badge">
            <span className="dot" style={{ background: headerColor }} />{" "}
            {headerDegraded ? "Degraded" : `${headerRunningCount}/${headerTotal} running`}
          </span>
          <button className="btn ghost sm" onClick={() => navigate("/industries")}>
            ← Industries
          </button>
        </div>
      </div>

      <div className="ctlbar">
        <div className="cb-item">
          <button className="btn sm" onClick={() => toast("Bring custom data — coming soon")}>
            ＋ Bring custom data
          </button>
        </div>
      </div>

      <div className="sec-label">
        Pipelines <span>start one or more — each running pipeline gets its own dashboard</span>
      </div>
      <div className="flavors">
        {industry.modes.map((mode) => {
          const real = realModeById.get(mode.id);
          return (
            <PipelineCard
              key={mode.id}
              mode={mode}
              industry={industry}
              isAdmin={isAdmin}
              onOpenDashboard={handleOpenDashboard}
              onChanged={refresh}
              realStatus={real?.status}
              setupId={adminItem?.setup.id}
              modeRateHz={adminItem?.setup.mode_configs?.[mode.id]?.rate_hz}
              isConfigured={
                adminItem
                  ? deployedModeIds.has(mode.id)
                  : undefined
              }
              realRuntimeDetail={real?.runtime_detail}
              realRestartable={real?.restartable}
            />
          );
        })}
      </div>

      <div className="setup-lower">
        <div className="panel">
          <div className="ptitle">
            How your data flows <span className="unit">enabled architectures</span>
          </div>
          <ArchDiagram industry={industry} modes={deployedModeViews} />
          <div className="arch-legend">
            <span>
              <span className="tierdot" style={{ background: "var(--sse)" }} />
              Kafka / live
            </span>
            <span>
              <span className="tierdot" style={{ background: "var(--lakebase)" }} />
              Lakebase (hot)
            </span>
            <span>
              <span className="tierdot" style={{ background: "var(--dbsql)" }} />
              Delta / DBSQL (cold)
            </span>
            <span>
              <span className="tierdot" style={{ background: "var(--ind)" }} />
              primary sink
            </span>
          </div>
        </div>
      </div>

      {isAdmin && (
        <details className="adv">
          <summary>
            <span className="chev">▸</span> Advanced settings{" "}
            <span className="badge" style={{ marginLeft: 6 }}>
              admin only
            </span>
          </summary>
          <div className="adv-body">
            The full manual controls still live here for debugging and edge cases — normally you
            never touch them:
            <br />
            • Source credentials &amp; connection details (<code>bootstrap_servers</code>, secrets
            scope)
            <br />
            • Processing mode (<code>realtime</code> / <code>microbatch</code>), sink selection,
            checkpoint location
            <br />
            • Cluster sizing (workers, node type, DBR), notebook template, connectivity validation
            <br />
            • LakeTS tiering (hot / drop / chunk intervals), rollups, retention &amp; CDF sync
            <br />
            <br />
            These are auto-configured per industry. Overrides here are yours to keep.
          </div>
        </details>
      )}
    </section>
  );
}
