import { useEffect, useMemo, useState, type CSSProperties } from "react";
import { Link, useNavigate } from "react-router-dom";

import { useUser } from "@/contexts/user-context";
import { getCatalog, type Catalog, type Industry } from "@/lib/industries-api";
import { listPipelineRuntime, type ModeView, type PipelineListItem } from "@/lib/admin-api";

/** Backend rollup statuses that count as "deployed" for live chip enrichment.
 * Used only to filter the optional `/industries/runtime` overlay — cards
 * themselves appear from catalog ``deploymentId`` without waiting on runtime. */
const DEPLOYED_STATUSES = new Set(["starting", "running", "degraded", "stopped"]);

/** True when the catalog reports at least one industry-first deployment id.
 * ``deploymentId`` is set only for modes with a real job (see
 * ``industry_service._list_industry_first_deployments``) — drafts never get one. */
function industryHasDeployment(industry: Industry): boolean {
  return industry.modes.some((m) => !!m.deploymentId);
}

/** Presentation for each per-mode runtime status. `draft` = the mode has no job:
 * end users see "Coming soon", admins see "Not deployed". `degraded` = the job is
 * up but a task crashed (data not flowing) → amber, not green. */
function modeStatusMeta(
  status: string,
  isAdmin: boolean,
): { label: string; color: string; pulse: boolean } {
  switch (status) {
    case "starting":
      return { label: "Starting", color: "#3b82f6", pulse: true };
    case "running":
      return { label: "Running", color: "var(--db-success, #12b76a)", pulse: true };
    case "degraded":
      return { label: "Degraded", color: "var(--db-warning, #f79009)", pulse: false };
    case "stopped":
      return { label: "Stopped", color: "var(--db-muted-foreground, #98a2b3)", pulse: false };
    default: // draft / not deployed
      return {
        label: isAdmin ? "Not deployed" : "Coming soon",
        color: "var(--db-muted-foreground, #98a2b3)",
        pulse: false,
      };
  }
}

/**
 * Icon path data ported from the `ICON` map in
 * the original design reference's `<script type="text/x-dc">`
 * block — one small stroke icon per curated industry (`Industry.icon`).
 */
const ICON_PATHS: Record<string, { d: string; linejoin?: boolean }[]> = {
  factory: [
    { d: "M2 20h20M4 20V9l5 3V9l5 3V9l5 3v8", linejoin: true },
    { d: "M8 20v-3M12 20v-3M16 20v-3" },
  ],
  pharma: [
    { d: "M9 3h6M10 3v6l-4 8a2 2 0 0 0 2 3h8a2 2 0 0 0 2-3l-4-8V3" },
    { d: "M7.5 15h9" },
  ],
  energy: [{ d: "M13 2 4 14h6l-1 8 9-12h-6l1-8z", linejoin: true }],
  auto: [
    {
      d: "M5 11l1.5-4.5A2 2 0 0 1 8.4 5h7.2a2 2 0 0 1 1.9 1.5L19 11m-14 0h14m-14 0a2 2 0 0 0-2 2v3h2m14-5a2 2 0 0 1 2 2v3h-2m-12 0h8m-9 0a1.5 1.5 0 1 1-3 0m3 0a1.5 1.5 0 1 0-3 0m15 0a1.5 1.5 0 1 1-3 0m3 0a1.5 1.5 0 1 0-3 0",
    },
  ],
};

const BRAND_LOGOS: Array<{ src: string; label: string }> = [
  { src: "/brand/data-streaming-icon-full-color.svg", label: "Spark Structured Streaming" },
  { src: "/brand/lakeflow-connect-icon-full-color.svg", label: "Zerobus" },
  { src: "/brand/lakebase-icon-full-color.svg", label: "Lakebase" },
  { src: "/brand/unity-catalog-icon-full-color.svg", label: "Unity Catalog" },
];

function IndustryIcon({ icon }: { icon: string }) {
  const paths = ICON_PATHS[icon] ?? [];
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.7} aria-hidden="true">
      {paths.map((p) => (
        <path key={p.d} d={p.d} strokeLinejoin={p.linejoin ? "round" : undefined} />
      ))}
    </svg>
  );
}

function humanizeMode(mode: string): string {
  const map: Record<string, string> = {
    realtime: "Real-time",
    microbatch: "Micro-batch",
    zerobus: "Zerobus",
  };
  return map[mode] ?? mode.charAt(0).toUpperCase() + mode.slice(1);
}

/** A per-mode chip driven by the REAL runtime status from the admin pipeline
 * view — running (green pulse) / degraded (amber, reason on hover) / stopped /
 * not-deployed ("Coming soon" for users). Never implies a mode is running when
 * it isn't. */
function ModeChip({ mode, isAdmin }: { mode: ModeView; isAdmin: boolean }) {
  const meta = modeStatusMeta(mode.status, isAdmin);
  const label = humanizeMode(mode.mode);
  const title = mode.runtime_detail
    ? `${label} · ${meta.label} — ${mode.runtime_detail}`
    : `${label} · ${meta.label}`;
  return (
    <span
      className="fchip"
      title={title}
      style={{ borderColor: meta.color, display: "inline-flex", alignItems: "center", gap: 6 }}
    >
      <span
        className={meta.pulse ? "pulse" : undefined}
        style={{
          width: 7,
          height: 7,
          borderRadius: "50%",
          background: meta.color,
          display: "inline-block",
        }}
      />
      {label}
      <span style={{ fontSize: 10, color: meta.color, fontWeight: 600 }}>{meta.label}</span>
    </span>
  );
}

function hexToRgba(hex: string, alpha: number): string {
  const n = hex.replace("#", "");
  const bigint = parseInt(n, 16);
  return `rgba(${(bigint >> 16) & 255},${(bigint >> 8) & 255},${bigint & 255},${alpha})`;
}

/** Admin-only card controls — a subtle footer row on each industry card,
 * hidden entirely for end users. `⚙ Config` opens the pipeline
 * configuration page (edit) and `Status` opens the per-industry resource
 * health view. Rendered as react-router `Link`s (so they get real hrefs /
 * open-in-new-tab), with `stopPropagation` so a click on either doesn't
 * also trigger the card's "open industry" navigation. */
function AdminCardControls({ industryId }: { industryId: string }) {
  return (
    <div className="card-admin">
      <Link
        className="btn ghost sm"
        to={`/setup-validate?industry=${industryId}`}
        title="Configure this industry's pipeline"
      >
        <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth={1.3} aria-hidden="true">
          <circle cx="8" cy="8" r="2.1" />
          <path d="M8 1.4v1.7M8 12.9v1.7M1.4 8h1.7M12.9 8h1.7M3.3 3.3l1.2 1.2M11.5 11.5l1.2 1.2M12.7 3.3l-1.2 1.2M4.5 11.5l-1.2 1.2" />
        </svg>
        Config
      </Link>
      <Link
        className="btn ghost sm"
        to={`/pipelines/${industryId}/health`}
        title="View this pipeline's resource health"
      >
        <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth={1.4} aria-hidden="true">
          <path d="M1.5 8h3l1.5-4 3 8 1.5-4h3" strokeLinejoin="round" strokeLinecap="round" />
        </svg>
        Status
      </Link>
    </div>
  );
}

function IndustryCard({
  industry,
  pipeline,
  isAdmin,
}: {
  industry: Industry;
  pipeline: PipelineListItem | undefined;
  isAdmin: boolean;
}) {
  // Real per-mode runtime from the admin pipeline view (never the curated
  // catalog, which fabricates "running"). Falls back to the curated mode names
  // as not-deployed placeholders if the admin view is unavailable.
  const modes: ModeView[] =
    pipeline?.modes && pipeline.modes.length
      ? pipeline.modes
      : industry.modes.map((m) => ({ mode: m.id, status: "draft", health: "draft", startable: false }));
  const runningCount = modes.filter((m) => m.status === "running").length;
  const startingCount = modes.filter((m) => m.status === "starting").length;
  const degradedCount = modes.filter((m) => m.status === "degraded").length;
  const total = modes.length;
  const rollup = String(pipeline?.status ?? "draft");

  // Top-right headline reflects the true worst state, not a blanket "running".
  const headline =
    rollup === "degraded"
      ? { text: "Degraded", color: "var(--db-warning, #f79009)" }
      : runningCount > 0
        ? { text: `${runningCount}/${total} running`, color: "var(--db-success, #12b76a)" }
        : startingCount > 0
          ? { text: "Starting", color: "#3b82f6" }
          : { text: "Not running", color: "var(--db-muted-foreground, #98a2b3)" };

  return (
    <article
      className="card"
      style={{
        "--ind": industry.accent,
        "--ind-soft": hexToRgba(industry.accent, 0.12),
        "--ind-line": hexToRgba(industry.accent, 0.5),
      } as CSSProperties}
    >
      <Link
        className="body card-main-link"
        to={`/i/${industry.id}`}
        aria-label={`Open ${industry.name} dashboards`}
      >
        <div className="card-top">
          <div className="icn">
            <IndustryIcon icon={industry.icon} />
          </div>
          <div className="st" style={{ color: headline.color }}>
            <span
              style={{
                width: 8,
                height: 8,
                borderRadius: "50%",
                background: headline.color,
                display: "inline-block",
                marginRight: 6,
              }}
            />
            {headline.text}
          </div>
        </div>
        <h3>{industry.name}</h3>
        <div className="sub">{industry.tag}</div>
        <div className="fchips">
          {modes.map((mode) => (
            <ModeChip key={mode.mode} mode={mode} isAdmin={isAdmin} />
          ))}
        </div>
        <div className="kv" style={{ marginTop: 12 }}>
          <div>
            <div className="v tnum">{runningCount}</div>
            <div className="k">running</div>
          </div>
          <div>
            <div className="v tnum">{degradedCount}</div>
            <div className="k">degraded</div>
          </div>
          <div>
            <div className="v tnum">{total}</div>
            <div className="k">modes</div>
          </div>
        </div>
      </Link>
      {isAdmin && <AdminCardControls industryId={industry.id} />}
    </article>
  );
}

function AddIndustryCard({ onClick }: { onClick: () => void }) {
  return (
    <button className="card custom" onClick={onClick}>
      <div className="cc">
        <div className="plus">+</div>
        Add industry pipeline
        <br />
        <span style={{ fontSize: 11, color: "var(--db-muted-foreground)" }}>Admin only</span>
      </div>
    </button>
  );
}

/** Shown when nothing is deployed yet. Admins get actionable CTAs (configure a
 * pipeline / open the full management list, which DOES show drafts); end users
 * get a "contact your admin" nudge since they can't deploy. */
function EmptyState({ isAdmin }: { isAdmin: boolean }) {
  const navigate = useNavigate();
  if (isAdmin) {
    return (
      <div className="card empty-state" style={{ gridColumn: "1 / -1", cursor: "default" }}>
        <div className="cc" style={{ gap: 14, padding: "44px 20px" }}>
          <h3 style={{ margin: 0 }}>No pipelines deployed yet</h3>
          <p className="lede" style={{ margin: 0, maxWidth: 460, textAlign: "center" }}>
            Configure and validate an industry pipeline to publish it here. Drafts and in-progress
            setups live in the full management list.
          </p>
          <div style={{ display: "flex", gap: 10, flexWrap: "wrap", justifyContent: "center" }}>
            <button className="btn primary sm" onClick={() => navigate("/setup-validate")}>
              Configure a pipeline
            </button>
            <Link className="btn ghost sm" to="/pipelines">
              All pipelines
            </Link>
          </div>
        </div>
      </div>
    );
  }
  return (
    <div className="card empty-state" style={{ gridColumn: "1 / -1", cursor: "default" }}>
      <div className="cc" style={{ gap: 12, padding: "44px 20px" }}>
        <h3 style={{ margin: 0 }}>No pipelines available yet</h3>
        <p className="lede" style={{ margin: 0, maxWidth: 420, textAlign: "center" }}>
          Your admin hasn&apos;t published any industry pipelines yet. Please contact your admin —
          they&apos;ll appear here, ready to open, once deployed.
        </p>
      </div>
    </div>
  );
}

export function IndustryGalleryPage() {
  const navigate = useNavigate();
  const { resolveIsAdmin } = useUser();
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  // Optional live-status overlay from `/industries/runtime`. Cards do NOT wait
  // on this — they appear from catalog ``deploymentId`` as soon as catalog
  // resolves. Runtime only enriches mode chips (running/degraded/stopped).
  const [adminById, setAdminById] = useState<Map<string, PipelineListItem> | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    getCatalog()
      .then((data) => {
        if (!cancelled) setCatalog(data);
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Failed to load industries");
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const isAdmin = resolveIsAdmin(catalog?.isAdmin ?? false);

  useEffect(() => {
    let cancelled = false;
    listPipelineRuntime()
      .then(({ pipelines }) => {
        if (cancelled) return;
        const byId = new Map<string, PipelineListItem>();
        for (const p of pipelines) {
          if (DEPLOYED_STATUSES.has(String(p.status ?? ""))) byId.set(p.setup.industry, p);
        }
        setAdminById(byId);
      })
      .catch(() => {
        /* 503 (cold runtime) — cards still show from catalog deploymentId;
           chips stay on draft placeholders until a later refresh succeeds. */
      });
    return () => {
      cancelled = true;
    };
  }, []);

  // Deployed = at least one mode has an industry-first ``deploymentId`` from
  // the catalog. Runtime overlay is optional enrichment, not a gate.
  const deployed = useMemo(() => {
    if (!catalog) return [];
    return catalog.industries.filter(industryHasDeployment);
  }, [catalog]);

  // Catalog alone unlocks the gallery — never block on Jobs/Lakebase runtime.
  const galleryReady = !!catalog;

  return (
    <section id="view-gallery">
      <div className="gal-head">
        <div>
          <div className="eyebrow">Digital Twin Platform</div>
          <h1 id="galTitle">{isAdmin ? "Manage industries" : "Your industry dashboards"}</h1>
          <p className="lede" id="galLede">
            {isAdmin
              ? "Add and configure industry pipelines. Each new industry gets its own catalog and Lakebase branch (kept forever), sharing one service principal. Only deployed pipelines appear here — drafts live in All pipelines."
              : "Open any industry to see its live twin and KPIs. You can change the source, bring your own data, and set the frequency — the pipeline stays provisioned for you."}
          </p>
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: 9, alignItems: "flex-end" }}>
          <div className="badge" id="galBadge">
            {!catalog
              ? "loading…"
              : isAdmin
                ? "admin · can add pipelines"
                : `${deployed.length} deployed · view & operate`}
          </div>
        </div>
      </div>

      <div className="builton">
        <span className="bo-lab">Built on</span>
        {BRAND_LOGOS.map((logo) => (
          <span className="bo-pill" key={logo.src}>
            <img className="bo-ic" src={logo.src} alt="" />
            {logo.label}
          </span>
        ))}
      </div>

      {loading && !galleryReady && (
        <p className="lede">Loading industries…</p>
      )}

      {error && (
        <p className="lede" style={{ color: "var(--db-destructive)" }}>
          Couldn&apos;t load the industry catalog: {error}
        </p>
      )}

      {catalog && galleryReady && (
        <div className="cards" id="cardGrid">
          {deployed.length === 0 ? (
            <EmptyState isAdmin={isAdmin} />
          ) : (
            deployed.map((industry) => (
              <IndustryCard
                key={industry.id}
                industry={industry}
                pipeline={adminById?.get(industry.id)}
                isAdmin={isAdmin}
              />
            ))
          )}
          {isAdmin && deployed.length > 0 && (
            <AddIndustryCard onClick={() => navigate("/setup-validate")} />
          )}
        </div>
      )}
    </section>
  );
}
