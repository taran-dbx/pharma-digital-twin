/**
 * Per-mode dashboard — the payoff screen for a curated industry's streaming
 * mode (`/i/:industry/:mode/dashboard`). Ports the `#view-dashboard` chrome
 * from the original design reference (kept outside this repo:
 * `.topbar-db`, `.kpis`, `.twin-grid`, `.sec` tier sections, `.cdf` banner)
 * using the DuBois classes already global in `dubois.css`, and mounts the
 * **existing** dashboard data components (`components/dashboard/*`,
 * `twin/PharmaPlant`) instead of re-implementing any data fetching.
 *
 * Tier gating per mode (ported from `MODEMETA` in the design script):
 * - realtime   → SSE twin + Lakebase hot (perf + sink health) + streaming
 *                metrics + CDF cold banner + DBSQL history.
 * - microbatch → live-preview twin + Delta/streaming-metrics section (Spark
 *                SSS writing straight to Delta — no separate "table health"
 *                signal exists beyond the same batch stats) + DBSQL history.
 *                No Lakebase, no CDF (microbatch never provisions a Lakebase
 *                branch — see `services/industry_templates.py::_MODES`).
 * - zerobus    → live-preview twin + Zerobus · Delta Ingest metrics + DBSQL
 *                history. No SSE, no Lakebase, no streaming metrics (Zerobus
 *                has no Spark job at all).
 *
 * Pipeline resolution: the industry catalog (`getCatalog`) only reports each
 * mode's lifecycle status, not its pipeline id — so the id is resolved via
 * the same `/api/pipelines` list `dashboard-page.tsx` already reads, trying
 * the deterministic curated name first (`iot-<industry>-<mode>`, mirroring
 * `services/industry_service.py::pipeline_name`) and falling back to any
 * pipeline tagged with this industry whose sink config matches the mode.
 * If nothing resolves, the mode's "Start pipeline" empty state is shown.
 */
import { useCallback, useEffect, useMemo, useRef, useState, type CSSProperties } from "react";
import { Link, useParams } from "react-router-dom";
import { toast } from "sonner";

import { useSensorContext } from "@/contexts/sensor-context";
import { api } from "@/lib/api";
import {
  getCatalog,
  getIndustryAssets,
  getIndustryReference,
  startMode,
  stopMode,
  type Catalog,
  type Industry,
  type IndustryAlertRule,
  type IndustryAssets,
  type IndustryMaintenanceEvent,
  type ModeStatus,
  type PipelineMode,
} from "@/lib/industries-api";
import { listPipelineRuntime, type ModeView, type PipelineListItem } from "@/lib/admin-api";
import type {
  AlertRecord,
  CdfStatusResponse,
  Pipeline,
  StreamingMetricsResponse,
  ZerobusMetricsResponse,
} from "@/lib/types";
import { formatNumber } from "@/lib/utils";

import { AlertRail } from "@/components/dashboard/alert-rail";
import { GenieCopilot } from "@/components/dashboard/genie-copilot";
import { PharmaPlant } from "@/components/twin/PharmaPlant";
import { ManufacturingCellPage } from "@/pages/manufacturing-cell-page";
import { LakebasePerfPanel } from "@/components/dashboard/lakebase-perf-panel";
import { LakebaseSinkHealth } from "@/components/dashboard/lakebase-sink-health";
import { HistoricalDbsql } from "@/components/dashboard/historical-dbsql";
import { StreamingSourceMetrics } from "@/components/dashboard/streaming-source-metrics";
import { ZerobusIngestMetrics } from "@/components/dashboard/zerobus-ingest-metrics";
import { RateChart } from "@/components/dashboard/lakets/rate-chart";
import { GapfillChart } from "@/components/dashboard/lakets/gapfill-chart";
import { HistogramChart } from "@/components/dashboard/lakets/histogram-chart";
import { useRefreshInterval, RefreshIntervalSelect } from "@/components/dashboard/refresh-interval-select";

const MODES: readonly PipelineMode[] = ["realtime", "microbatch", "zerobus"];

const DBSQL_MINUTES = 1440; // 24h — cold-tier default window for this screen.
const LAKEBASE_MINUTES = 15; // hot-tier default window.

interface ModeMeta {
  fname: string;
  pills: readonly (readonly [string, string])[];
  twinTag: readonly [string, string];
  twinNote: string;
  midTitle: string;
  midTag: readonly [string, string];
  showLakebase: boolean;
  showMetrics: boolean;
  showCdf: boolean;
  // Hot-tier chrome — only true for a mode that actually has a live SSE tier
  // (realtime). Gating on these keeps micro-batch/zerobus from advertising a
  // live surface they can never fill (SSE pill, live KPI cards, live sensor
  // stream).
  showSse: boolean;
  showLiveKpis: boolean;
  showSensorStream: boolean;
}

/** Ported from `MODEMETA` in the design script. */
const MODE_META: Record<PipelineMode, ModeMeta> = {
  realtime: {
    fname: "Real-time mode",
    // The SSE pill is rendered separately, gated on `showSse` — keeping it out
    // of the data list means micro-batch/zerobus never accidentally show it.
    pills: [["Lakebase", "var(--lakebase)"], ["DBSQL", "var(--dbsql)"]],
    twinTag: ["sse", "SSE · real-time"],
    twinNote: "refresh 1s · Server-Sent Events",
    midTitle: "Warm tier · recent history",
    midTag: ["lb", "Lakebase · hot"],
    showLakebase: true,
    showMetrics: true,
    showCdf: true,
    showSse: true,
    showLiveKpis: true,
    showSensorStream: true,
  },
  microbatch: {
    fname: "Micro-batch mode",
    pills: [["Delta", "var(--dbsql)"], ["DBSQL", "var(--dbsql)"]],
    // No SSE tier — label the twin as the simulator preview it is (neutral
    // "db" tag, not the green "sse" tag that reads as a live feed).
    twinTag: ["db", "live preview"],
    twinNote: "simulator preview · micro-batch to Delta",
    midTitle: "Delta table · micro-batch",
    midTag: ["db", "Delta · UC"],
    showLakebase: false,
    showMetrics: true,
    showCdf: false,
    showSse: false,
    showLiveKpis: false,
    showSensorStream: false,
  },
  zerobus: {
    fname: "Zerobus (Delta Ingest)",
    pills: [["Delta", "var(--dbsql)"], ["DBSQL", "var(--dbsql)"]],
    twinTag: ["db", "live preview"],
    twinNote: "simulator preview · Zerobus to Delta",
    midTitle: "Delta table · Zerobus ingest",
    midTag: ["db", "Delta · UC"],
    showLakebase: false,
    showMetrics: false,
    showCdf: false,
    showSse: false,
    showLiveKpis: false,
    showSensorStream: false,
  },
};

/** Inline SVG twins for non-pharma industries, ported verbatim from the
 * design script's `TWIN` map (the original design reference).
 * Pharmaceutical uses the real `PharmaPlant` r3f twin instead — this map only
 * covers the other three curated industries. */
const TWIN_SVG: Record<string, string> = {
  line: `<svg viewBox="0 0 600 360"><g stroke="var(--ind)" stroke-width="2" opacity=".5" fill="none"><rect x="60" y="220" width="480" height="20" rx="4"/><path d="M90 220v-70h80v70M250 220v-90h90v90M430 220v-60h70v60"/></g><g fill="none" stroke="var(--ind-line)" stroke-width="1.5">${[110, 180, 300, 470].map((x) => `<line x1="${x}" y1="230" x2="${x}" y2="240"/>`).join("")}</g><text x="130" y="120" fill="var(--db-muted-foreground)" font-size="11" text-anchor="middle">Station 1</text><text x="295" y="100" fill="var(--db-muted-foreground)" font-size="11" text-anchor="middle">Robot cell</text><text x="465" y="132" fill="var(--db-muted-foreground)" font-size="11" text-anchor="middle">QA gate</text></svg>`,
  vessel: `<svg viewBox="0 0 600 360"><g fill="none" stroke="var(--ind)" stroke-width="2" opacity=".55">${[160, 300, 440].map((x) => `<rect x="${x - 45}" y="90" width="90" height="180" rx="24"/><ellipse cx="${x}" cy="90" rx="45" ry="12"/>`).join("")}<path d="M160 270 v40 h280 v-40"/></g>${[160, 300, 440].map((x, i) => `<text x="${x}" y="300" fill="var(--db-muted-foreground)" font-size="11" text-anchor="middle">Reactor ${String.fromCharCode(65 + i)}</text>`).join("")}</svg>`,
  grid: `<svg viewBox="0 0 600 360"><g stroke="var(--ind-line)" stroke-width="1.5" fill="none"><path d="M120 200 L300 120 L480 200 M300 120 V60 M120 200 V300 M480 200 V300 M300 200 V300"/></g><text x="300" y="48" fill="var(--db-muted-foreground)" font-size="11" text-anchor="middle">Substation</text><text x="120" y="332" fill="var(--db-muted-foreground)" font-size="11" text-anchor="middle">Turbine 1</text><text x="480" y="332" fill="var(--db-muted-foreground)" font-size="11" text-anchor="middle">Turbine 2</text></svg>`,
  auto: `<svg viewBox="0 0 600 360"><g fill="none" stroke="var(--ind)" stroke-width="2" opacity=".55"><path d="M180 210 q20 -60 80 -62 h80 q60 2 80 62"/><rect x="150" y="210" width="300" height="14" rx="4"/><circle cx="230" cy="238" r="20"/><circle cx="380" cy="238" r="20"/></g><text x="220" y="120" fill="var(--db-muted-foreground)" font-size="11" text-anchor="middle">Weld cell</text><text x="300" y="102" fill="var(--db-muted-foreground)" font-size="11" text-anchor="middle">Paint booth</text><text x="390" y="120" fill="var(--db-muted-foreground)" font-size="11" text-anchor="middle">EOL test</text></svg>`,
};

function hexToRgba(hex: string, alpha: number): string {
  const n = hex.replace("#", "");
  const bigint = parseInt(n, 16);
  return `rgba(${(bigint >> 16) & 255},${(bigint >> 8) & 255},${bigint & 255},${alpha})`;
}

function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "Failed to load dashboard";
}

/** Best-effort match of a (possibly non-curated) pipeline row to a mode,
 * mirroring `dashboard-page.tsx::detectDataMode`/`getStreamingSourceKind`. */
function pipelineMatchesMode(pipeline: Pipeline, mode: PipelineMode): boolean {
  const cfg = pipeline.source_config as Record<string, unknown>;
  const sssSource = typeof cfg.source === "string" ? cfg.source : undefined;
  const isZerobus = pipeline.source_type === "zerobus" || sssSource === "zerobus";
  if (mode === "zerobus") return isZerobus;
  if (isZerobus) return false;
  const rtmSink = cfg.rtm_sink as string | undefined;
  const microbatchSinks = cfg.microbatch_sinks as string[] | undefined;
  const hasLakebase = rtmSink === "lakebase" || rtmSink === "both" || microbatchSinks?.includes("lakebase");
  return mode === "realtime" ? !!hasLakebase : !hasLakebase;
}

interface PharmaReadings {
  reactorTemp?: number;
  pressure?: number;
  wfiFlow?: number;
  yield: number;
  faults: number;
}

/** Curated pharma KPI labels → the real simulator `sensor_name` behind each,
 * so the tiles resolve their live value from the SSE `sensors` map (keyed by
 * sensor_name) instead of the never-matching hardcoded labels. Keep in sync
 * with the pharma `kpis` list in `services/industry_templates.py`. */
const KPI_SENSOR_MAP: Record<string, string> = {
  "Bioreactor temp": "bioreactor_1_temp",
  "Bioreactor pH": "bioreactor_1_ph",
  "Dissolved O₂": "bioreactor_1_dissolved_oxygen",
  "Cleanroom ΔP": "cleanroom_1_pressure",
  "Tablet weight": "tablet_weight",
  "Autoclave temp": "autoclave_chamber_temp",
};

/** Live value for a KPI tile, resolved from domain reference `sensor_name`
 * bindings (with the pharma catalog map as a last-resort fallback). */
function liveKpiValue(
  label: string,
  sensors: Record<string, { value: number }>,
  kpiBindings: Record<string, string>,
): string | null {
  const sensorName = kpiBindings[label] ?? KPI_SENSOR_MAP[label];
  if (!sensorName) return null;
  const reading = sensors[sensorName];
  if (!reading || reading.value == null) return null;
  return formatNumber(reading.value, label === "Bioreactor pH" ? 2 : 1);
}

function readingMatchesIndustry(industryId: string, readingIndustry: string | undefined): boolean {
  if (readingIndustry === industryId) return true;
  return industryId === "energy" && readingIndustry === "electric_power";
}

/**
 * Combines the curated catalog's coarse `ModeStatus` (idle/starting/running)
 * with the two states only the REAL runtime (`ModeView` from
 * `listPipelineRuntime()`) can express: `degraded` (the job is up
 * but a task crashed — data isn't flowing) and `stopped` (deployed, not
 * running). Used so the dashboard never renders as if it were live when the
 * authoritative runtime says otherwise.
 */
type EffectiveModeStatus = ModeStatus | "degraded" | "stopped";

/**
 * Shown when no pipeline row resolves for this mode, or the real runtime
 * (`ModeView`) reports it isn't actually live. Status-aware so it never
 * dead-ends: a *running* pipeline whose row hasn't loaded yet offers a refresh
 * (not a misleading "start" that loops back to the setup screen); a *starting*
 * pipeline explains it's coming up; a *degraded* pipeline explains a task
 * crashed; a *stopped* pipeline explains it was deployed then stopped; only a
 * genuinely *idle* pipeline points at the setup screen (admins) or tells an
 * end user to contact their admin.
 */
function EmptyState({
  industry,
  status,
  isAdmin,
  runtimeDetail,
  onRefresh,
}: {
  industry: Industry;
  status: EffectiveModeStatus;
  isAdmin: boolean;
  runtimeDetail?: string | null;
  onRefresh: () => void;
}) {
  if (status === "running") {
    return (
      <div className="panel" style={{ textAlign: "center", padding: "40px 24px" }}>
        <p className="ptitle" style={{ justifyContent: "center" }}>
          {industry.name} · loading dashboard
        </p>
        <p className="lede" style={{ margin: "0 auto 16px", maxWidth: "52ch" }}>
          This pipeline is running, but its resource details haven&apos;t loaded yet — this can
          happen briefly right after start while resources register. Refresh in a moment.
        </p>
        <button className="btn primary" onClick={onRefresh}>
          ↻ Refresh
        </button>
      </div>
    );
  }

  if (status === "starting") {
    return (
      <div className="panel" style={{ textAlign: "center", padding: "40px 24px" }}>
        <p className="ptitle" style={{ justifyContent: "center" }}>
          {industry.name} · starting…
        </p>
        <p className="lede" style={{ margin: "0 auto 16px", maxWidth: "52ch" }}>
          This pipeline is starting up — its dashboard will fill in with live data once it&apos;s
          running.
        </p>
        <Link className="btn" to={`/i/${industry.id}`}>
          View setup
        </Link>
      </div>
    );
  }

  if (status === "degraded") {
    return (
      <div className="panel" style={{ textAlign: "center", padding: "40px 24px" }}>
        <p className="ptitle" style={{ justifyContent: "center", color: "var(--db-warning, #f79009)" }}>
          {industry.name} · degraded
        </p>
        <p className="lede" style={{ margin: "0 auto 16px", maxWidth: "52ch" }}>
          This pipeline&apos;s job is running, but a task has crashed — data is not currently
          flowing.
          {runtimeDetail ? ` (${runtimeDetail})` : ""}
        </p>
        {isAdmin ? (
          <Link className="btn primary" to={`/i/${industry.id}`}>
            View setup
          </Link>
        ) : (
          <button className="btn primary" onClick={onRefresh}>
            ↻ Refresh
          </button>
        )}
      </div>
    );
  }

  if (status === "stopped") {
    return (
      <div className="panel" style={{ textAlign: "center", padding: "40px 24px" }}>
        <p className="ptitle" style={{ justifyContent: "center" }}>
          {industry.name} · stopped
        </p>
        <p className="lede" style={{ margin: "0 auto 16px", maxWidth: "52ch" }}>
          {isAdmin
            ? "This pipeline is deployed but stopped — start it from the industry setup screen to see this dashboard fill in with live data."
            : "This pipeline is currently stopped. Contact your admin to start it."}
        </p>
        <Link className="btn primary" to={`/i/${industry.id}`}>
          Go to setup
        </Link>
      </div>
    );
  }

  return (
    <div className="panel" style={{ textAlign: "center", padding: "40px 24px" }}>
      <p className="ptitle" style={{ justifyContent: "center" }}>
        {industry.name} · not started
      </p>
      <p className="lede" style={{ margin: "0 auto 16px", maxWidth: "52ch" }}>
        {isAdmin
          ? "This pipeline hasn't been started yet — start it from the industry setup screen to provision it and see this dashboard fill in with live data."
          : "This pipeline hasn't been started yet. Contact your admin to start it, or start it from the industry setup screen if you have access."}
      </p>
      <Link className="btn primary" to={`/i/${industry.id}`}>
        Go to setup
      </Link>
    </div>
  );
}

function SensorStreamPanel({ rows }: { rows: Array<{ name: string; value: string; unit: string; alert: boolean }> }) {
  return (
    <div className="panel">
      <div className="ptitle">
        Sensor stream <span className="unit">{rows.length} sensors</span>
      </div>
      <div className="stream">
        {rows.map((row) => (
          <div className={`row ${row.alert ? "alert" : ""}`} key={row.name}>
            <span className="nm">{row.name}</span>
            <span className="vv">{row.value}</span>
            <span className="u">{row.unit}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

type PanelPipeline = Pick<Pipeline, "id" | "resources">;

export function ModeDashboardPage() {
  const { industry: industryId, mode } = useParams<{ industry: string; mode: string }>();

  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const [pipelines, setPipelines] = useState<Pipeline[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [refreshSec, setRefreshSec] = useRefreshInterval(30);
  // Bumped by the topbar "refresh now" button — keyed into the Lakebase/LakeTS
  // panels so they remount + refetch immediately (independent of the interval).
  const [lakebaseNonce, setLakebaseNonce] = useState(0);
  // The DBSQL historical panel registers its refetch here so the manual
  // "Refresh" button (cold tier is manual-only, no auto-poll) can fire it.
  const dbsqlRefreshRef = useRef<(() => void) | null>(null);
  const streamingRefreshRef = useRef<(() => void) | null>(null);
  const [streamingMetrics, setStreamingMetrics] = useState<StreamingMetricsResponse | null>(null);
  const [zerobusMetrics, setZerobusMetrics] = useState<ZerobusMetricsResponse | null>(null);
  const [cdfStatus, setCdfStatus] = useState<CdfStatusResponse | null>(null);
  const [powerBusy, setPowerBusy] = useState(false);
  // Full pipeline row fetched by id when the catalog reports a pipeline for
  // this mode but the /api/pipelines list didn't include its row.
  const [fetchedPipeline, setFetchedPipeline] = useState<Pipeline | null>(null);
  // The REAL runtime, per mode, from the public pipeline runtime view
  // (`ModeView` via `listPipelineRuntime()`) — the same source of truth the
  // home (`industry-gallery-page.tsx`) uses to avoid showing a degraded/
  // not-deployed mode as if it were live. Available to every authenticated
  // app user; `null` means "not known" and the page falls back to the curated
  // catalog's coarser idle/starting/running status.
  const [adminPipelines, setAdminPipelines] = useState<PipelineListItem[] | null>(null);

  const { sensors, wsConnected } = useSensorContext();
  const [domainAssets, setDomainAssets] = useState<IndustryAssets | null>(null);
  const [kpiBindings, setKpiBindings] = useState<Record<string, string>>({});
  const [maintenance, setMaintenance] = useState<IndustryMaintenanceEvent[]>([]);
  const [alertRules, setAlertRules] = useState<IndustryAlertRule[]>([]);
  const [genieOpen, setGenieOpen] = useState(true);
  const [explainAlert, setExplainAlert] = useState<AlertRecord | null>(null);

  const refreshAll = useCallback(() => {
    return Promise.all([getCatalog(), api.listPipelines().catch(() => [])]).then(
      ([catalogData, pipelinesData]) => {
        setCatalog(catalogData);
        setPipelines(pipelinesData);
      },
    );
  }, []);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    refreshAll()
      .catch((err: unknown) => {
        if (!cancelled) setError(getErrorMessage(err));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [industryId, mode, refreshAll]);

  useEffect(() => {
    if (!industryId) return;
    let cancelled = false;
    Promise.all([getIndustryAssets(industryId), getIndustryReference(industryId)])
      .then(([assets, reference]) => {
        if (cancelled) return;
        setDomainAssets(assets);
        const bindings: Record<string, string> = {};
        for (const kpi of reference.kpis) bindings[kpi.label] = kpi.sensor_name;
        setKpiBindings(bindings);
        setMaintenance(reference.maintenance_history ?? []);
        setAlertRules(reference.alert_rules ?? []);
      })
      .catch(() => {
        if (!cancelled) {
          setDomainAssets(null);
          setKpiBindings({});
          setMaintenance([]);
          setAlertRules([]);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [industryId]);

  const industry = catalog?.industries.find((i) => i.id === industryId);
  const isKnownMode = (m: string | undefined): m is PipelineMode => MODES.includes(m as PipelineMode);
  const isAdmin = catalog?.isAdmin ?? false;

  // Fetch the real per-mode runtime so the dashboard can tell a genuinely
  // running mode apart from one whose job crashed or was stopped. 503 (no
  // backend / cold) leaves it `null` — the page then falls back to the
  // coarser catalog status. Uses the public runtime endpoint only (never the
  // heavy `/admin/pipelines` list).
  const refreshAdminPipelines = useCallback(() => {
    return listPipelineRuntime()
      .then(({ pipelines: items }) => setAdminPipelines(items))
      .catch(() => setAdminPipelines(null));
  }, []);

  useEffect(() => {
    void refreshAdminPipelines();
  }, [refreshAdminPipelines]);

  const realModeView: ModeView | undefined = useMemo(() => {
    if (!isKnownMode(mode)) return undefined;
    const item = adminPipelines?.find((p) => p.setup.industry === industryId);
    return item?.modes?.find((m) => m.mode === mode);
  }, [adminPipelines, industryId, mode]);

  const catalogMode = industry?.modes.find((m) => m.id === mode);

  // The REAL effective status: the curated catalog's idle/starting/running is
  // trusted for the transient "starting" case, but a `degraded` (job up, task
  // crashed) or `stopped` (deployed but not running) verdict from the real
  // runtime always wins — those are stronger, more specific signals than the
  // catalog can express, and ignoring them would show a fake-live dashboard.
  const effectiveStatus: EffectiveModeStatus = useMemo(() => {
    const fallback: EffectiveModeStatus = catalogMode?.status ?? "idle";
    if (!realModeView) return fallback;
    // The real runtime freshness gate (/api/industries/runtime: job tasks
    // RUNNING AND data flowing) is authoritative — it beats the coarser curated
    // catalog status, which can lag at "idle" while the pipeline is actually flowing.
    if (realModeView.status === "running") return "running";
    if (realModeView.status === "degraded") return "degraded";
    if (realModeView.status === "stopped") return "stopped";
    if (realModeView.status === "starting") return "starting";
    return fallback;
  }, [realModeView, catalogMode?.status]);
  // Authoritative pipeline id for this mode, resolved server-side in
  // `get_catalog` (`IndustryMode.pipelineId`). Preferred over name-matching so
  // "Open dashboard" resolves the running pipeline instead of dead-ending.
  const pipelineIdFromCatalog = catalogMode?.pipelineId ?? null;
  const deploymentIdFromCatalog = catalogMode?.deploymentId ?? null;

  // Full pipeline row from the /api/pipelines list — by catalog id first,
  // then the deterministic curated name, then a best-effort mode match.
  const listPipeline = useMemo(() => {
    if (!industryId || !isKnownMode(mode)) return null;
    if (pipelineIdFromCatalog) {
      const byId = pipelines.find((p) => p.id === pipelineIdFromCatalog);
      if (byId) return byId;
    }
    const expectedName = `iot-${industryId}-${mode}`;
    return (
      pipelines.find((p) => p.name === expectedName) ??
      pipelines.find((p) => p.industry === industryId && pipelineMatchesMode(p, mode)) ??
      null
    );
  }, [pipelines, industryId, mode, pipelineIdFromCatalog]);

  // When the catalog says a pipeline exists for this mode but the list didn't
  // carry its row, fetch it directly by id so the dashboard resolves.
  useEffect(() => {
    if (!pipelineIdFromCatalog || listPipeline) return;
    if (fetchedPipeline?.id === pipelineIdFromCatalog) return;
    let cancelled = false;
    api
      .getPipeline(pipelineIdFromCatalog)
      .then((p) => {
        if (!cancelled) setFetchedPipeline(p);
      })
      .catch(() => {
        /* leave null — the empty state handles the running-but-unresolved case */
      });
    return () => {
      cancelled = true;
    };
  }, [pipelineIdFromCatalog, listPipeline, fetchedPipeline?.id]);

  // Industry-first deployments intentionally have no legacy `pipelines` row.
  // In that case the catalog carries the per-mode `pipeline_deployment.id`; the
  // industry-aware dashboard endpoints only need this stable id plus the
  // explicit industry/mode query params, so a minimal identity object is
  // sufficient to render. Prefer it over any leftover legacy row so every
  // dashboard panel reads the same industry-first tables.
  const pipeline = deploymentIdFromCatalog
    ? { id: deploymentIdFromCatalog, resources: {} as Pipeline["resources"] }
    : listPipeline ??
      (fetchedPipeline && fetchedPipeline.id === pipelineIdFromCatalog
        ? fetchedPipeline
        : null);
  const isIndustryFirst = !!deploymentIdFromCatalog && pipeline?.id === deploymentIdFromCatalog;

  const pipelineId = pipeline?.id ?? null;
  const meta = isKnownMode(mode) ? MODE_META[mode] : null;
  // Ported from `togglePower()` in the design reference — there it just
  // flips a local `powered` boolean; here it's the real start/stop toggle
  // already used by `<PipelineCard>` on the setup screen. Driven by
  // `effectiveStatus` (not the coarser catalog status alone) so a degraded or
  // stopped mode never paints its pills/KPIs as live.
  const powered = effectiveStatus === "running";

  async function handleTogglePower() {
    if (!industryId || !isKnownMode(mode) || !catalogMode) return;
    setPowerBusy(true);
    try {
      // Use the authoritative runtime status (effectiveStatus), not the coarse
      // catalog status — the latter can lag at "idle" while actually running,
      // which would make the "Stop" button erroneously (re)start the pipeline.
      if (effectiveStatus === "running") {
        await stopMode(industryId, mode);
      } else {
        await startMode(industryId, mode);
      }
    } catch (err: unknown) {
      toast.error(`Couldn't toggle ${catalogMode.name}: ${getErrorMessage(err)}`);
    } finally {
      setPowerBusy(false);
      refreshAll();
      void refreshAdminPipelines();
    }
  }

  const isPharmaRealtime = industryId === "pharmaceutical" && mode === "realtime";

  // Live pharma readings: realtime comes from SSE; Zerobus comes from the
  // latest per-sensor rows returned with each ingest-metrics refresh.
  const pharmaReadings = useMemo<PharmaReadings | null>(() => {
    if (industryId !== "pharmaceutical") return null;
    const live =
      mode === "zerobus"
        ? (zerobusMetrics?.latest_readings ?? [])
        : isPharmaRealtime
          ? Object.values(sensors)
          : [];
    const numericValue = (value: number | string | undefined): number | undefined => {
      const numeric = Number(value);
      return Number.isFinite(numeric) ? numeric : undefined;
    };
    const byName = (name: string) =>
      numericValue(live.find((s) => s.sensor_name === name)?.value);
    const byType = (type: string) =>
      numericValue(live.find((s) => s.sensor_type === type)?.value);
    const faults = live.filter((s) =>
      s.fault_active === true ||
      s.fault_active === 1 ||
      s.fault_active === "1" ||
      s.fault_active === "true"
    ).length;
    return {
      reactorTemp: byName("bioreactor_1_temp") ?? byType("temperature"),
      pressure: byType("pressure"),
      wfiFlow: byType("flow"),
      yield: Math.max(80, 99.2 - faults * 2.5),
      faults,
    };
  }, [industryId, mode, isPharmaRealtime, sensors, zerobusMetrics?.latest_readings]);

  // Streaming metrics — realtime & microbatch only (both run Spark SSS).
  // Uses the shared {industry}_metrics table (Task 5: one-schema layout).
  // Passes `industry` and `mode` so the backend queries the correct prefixed
  // table and filters by mode — no legacy per-catalog path needed.
  useEffect(() => {
    if (!pipelineId || !meta?.showMetrics) return;
    if (!industryId || !isKnownMode(mode)) return;
    let cancelled = false;
    const fetchOnce = async () => {
      try {
        const data = await api.getStreamingMetrics(
          pipelineId,
          isIndustryFirst ? industryId : undefined,
          mode,
        );
        if (!cancelled) setStreamingMetrics(data);
      } catch {
        /* metrics table may not exist yet */
      }
    };
    fetchOnce();
    if (refreshSec <= 0) return () => { cancelled = true; };
    const id = setInterval(fetchOnce, refreshSec * 1000);
    return () => { cancelled = true; clearInterval(id); };
  }, [pipelineId, industryId, mode, meta?.showMetrics, refreshSec, isIndustryFirst]);

  // CDF (Lakehouse Sync) status — realtime only.
  useEffect(() => {
    if (!pipelineId || !meta?.showCdf) return;
    let cancelled = false;
    const poll = async () => {
      try {
        const data = await api.getCdfStatus(pipelineId, industryId);
        if (!cancelled) setCdfStatus(data);
      } catch {
        /* fail closed — banner + gate stay hidden/off */
      }
    };
    poll();
    if (refreshSec <= 0) return () => { cancelled = true; };
    const id = setInterval(poll, refreshSec * 1000);
    return () => { cancelled = true; clearInterval(id); };
  }, [pipelineId, meta?.showCdf, refreshSec, industryId]);

  const streamingMetricsRows = useMemo(() => {
    if (!streamingMetrics?.rows) return [];
    return streamingMetrics.rows.slice().reverse().slice(-20);
  }, [streamingMetrics]);
  const streamingThroughputData = useMemo(
    () => streamingMetricsRows.map((r) => ({
      batch: r.batch_id,
      input_rps: Number(r.input_rows_per_second) || 0,
      processed_rps: Number(r.processed_rows_per_second) || 0,
    })),
    [streamingMetricsRows],
  );
  const streamingBatchDurationData = useMemo(
    () => streamingMetricsRows.map((r) => ({ batch: r.batch_id, duration_ms: Number(r.batch_duration_ms) || 0 })),
    [streamingMetricsRows],
  );
  const streamingLatencyData = useMemo(
    () => streamingMetricsRows.map((r) => ({
      batch: r.batch_id,
      e2e_p50_ms: Number(r.e2e_p50_ms) || 0,
      processing_p50_ms: Number(r.processing_p50_ms) || 0,
      queuing_p50_ms: Number(r.queuing_p50_ms) || 0,
    })),
    [streamingMetricsRows],
  );

  const streamRows = useMemo(() => {
    if (mode === "realtime" && industryId) {
      const live = Object.values(sensors).filter((reading) =>
        readingMatchesIndustry(industryId, reading.industry),
      );
      if (live.length > 0) {
        return live
          .slice(0, 8)
          .map((s) => ({ name: s.sensor_name, value: formatNumber(s.value, 2), unit: s.unit, alert: s.fault_active }));
      }
    }
    return (industry?.sensors ?? []).map((name) => ({ name, value: "—", unit: "", alert: false }));
  }, [industryId, mode, sensors, industry?.sensors]);

  if (loading) {
    return (
      <section id="view-dashboard">
        <p className="lede">Loading dashboard…</p>
      </section>
    );
  }

  if (error) {
    return (
      <section id="view-dashboard">
        <p className="lede" style={{ color: "var(--db-destructive)" }}>Couldn&apos;t load this dashboard: {error}</p>
        <Link to="/industries">← Industries</Link>
      </section>
    );
  }

  if (!industry || !meta || !catalogMode || !isKnownMode(mode)) {
    return (
      <section id="view-dashboard">
        <div className="crumbs">
          <Link to="/industries">Industries</Link>
          <span className="sep">/</span>
          <span>Unknown dashboard</span>
        </div>
        <p className="lede">&ldquo;{industryId}/{mode}&rdquo; isn&apos;t a curated industry mode.</p>
        <Link to="/industries">← Industries</Link>
      </section>
    );
  }
  // `isKnownMode` is a type predicate, so `mode` is narrowed to `PipelineMode`
  // for the remainder of this render — safe to pass to mode-aware children
  // (e.g. `HistoricalDbsql`) without a cast.

  const accentStyle = {
    "--ind": industry.accent,
    "--ind-soft": hexToRgba(industry.accent, 0.12),
    "--ind-line": hexToRgba(industry.accent, 0.5),
  } as CSSProperties;
  const isLive = !!pipeline && effectiveStatus !== "degraded" && effectiveStatus !== "stopped";

  const midTier = (p: PanelPipeline) => (
    <div className="sec">
      <div className="shead">
        <h2>{meta.midTitle}</h2>
        <span className={`src-tag ${meta.midTag[0]}`}>{meta.midTag[1]}</span>
      </div>
      {mode === "realtime" && (
        <div className="grid2">
          <div className="panel">
            <div className="ptitle">Lakebase write performance <span className="unit">last {LAKEBASE_MINUTES}m</span></div>
            <LakebasePerfPanel key={`lbperf-${lakebaseNonce}`} pipelineId={p.id} minutes={LAKEBASE_MINUTES} refreshSec={refreshSec} industry={industryId} />
          </div>
          <div className="panel">
            <div className="ptitle">Insertion health</div>
            <LakebaseSinkHealth
              key={`lbhealth-${lakebaseNonce}`}
              pipelineId={p.id}
              refreshSec={refreshSec}
              databricksHost={typeof window !== "undefined" ? window.location.hostname : ""}
              jobId={p.resources?.job_id ?? null}
              industry={industryId}
            />
          </div>
        </div>
      )}
      {mode === "microbatch" && (
        <div className="panel">
          <div className="ptitle">Batch commit health <span className="unit">Spark Structured Streaming</span></div>
          <StreamingSourceMetrics
            pipelineId={p.id}
            sourceKind="sss"
            sourceLabel="Kafka"
            industry={industryId}
            mode="microbatch"
            throughputData={streamingThroughputData}
            batchDurationData={streamingBatchDurationData}
            latencyData={streamingLatencyData}
            refreshSec={refreshSec}
            emptyLabel="No batch metrics yet — Spark commit stats appear here once the micro-batch job reports its first batch."
          />
        </div>
      )}
      {mode === "zerobus" && (
        <div className="panel">
          <div className="ptitle">Ingest health <span className="unit">Zerobus SDK</span></div>
          <ZerobusIngestMetrics
            pipelineId={p.id}
            industry={industryId}
            industryFirst={isIndustryFirst}
            minutes={DBSQL_MINUTES}
            refreshSec={refreshSec}
            onMetrics={setZerobusMetrics}
          />
        </div>
      )}
    </div>
  );

  const hotTier = (p: PanelPipeline) => (
    <>
      {/* LakeTS time-series — hot-tier ChronoTable capabilities (gap-fill,
          rate-of-change, value distribution) served straight from Lakebase,
          refreshed on the topbar Lakebase interval. Realtime only. */}
      {mode === "realtime" && (
        <div className="sec">
          <div className="shead">
            <h2>LakeTS time-series</h2>
            <span className="src-tag lb">Lakebase · LakeTS</span>
            <span className="note">gap-fill · rate-of-change · distribution · from the ChronoTable</span>
          </div>
          <div className="grid2" key={`lakets-${lakebaseNonce}`}>
            <GapfillChart pipelineId={p.id} rangeMinutes={LAKEBASE_MINUTES} refreshSec={refreshSec} enabled={powered} industry={industryId} />
            <RateChart pipelineId={p.id} rangeMinutes={LAKEBASE_MINUTES} refreshSec={refreshSec} enabled={powered} industry={industryId} />
          </div>
          <div style={{ marginTop: "12px" }} key={`hist-${lakebaseNonce}`}>
            <HistogramChart pipelineId={p.id} rangeMinutes={LAKEBASE_MINUTES} refreshSec={refreshSec} enabled={powered} sensorName={industryId === "manufacturing" ? "cnc_1_spindle_temp" : "bioreactor_1_temp"} industry={industryId} />
          </div>
        </div>
      )}

      {/* Streaming metrics (realtime only; microbatch folds the same signal
          into the mid tier, since both are the same Spark SSS batch stats —
          see module doc). */}
      {mode === "realtime" && meta.showMetrics && (
        <div className="sec">
          <div className="shead">
            <h2>Streaming metrics</h2>
            <span className="src-tag sse">pipeline</span>
            <span className="note">Spark Structured Streaming</span>
            <button
              className="btn sm"
              style={{ marginLeft: "auto" }}
              onClick={() => streamingRefreshRef.current?.()}
              title="Re-fetch the latest streaming metrics now"
            >
              ↻ Refresh
            </button>
          </div>
          <div className="panel">
            <StreamingSourceMetrics
              pipelineId={p.id}
              sourceKind="sss"
              sourceLabel="Kafka"
              industry={industryId}
              mode="realtime"
              throughputData={streamingThroughputData}
              batchDurationData={streamingBatchDurationData}
              latencyData={streamingLatencyData}
              refreshSec={refreshSec}
              registerRefresh={(fn) => {
                streamingRefreshRef.current = fn;
              }}
            />
          </div>
        </div>
      )}

      {/* CDF cold-tier banner — realtime only, and only once Lakehouse
          Sync is actually streaming (the "not enabled yet" case is
          already explained inline by HistoricalDbsql's own gate below). */}
      {meta.showCdf && cdfStatus?.streaming && (
        <div className="cdf">
          <div className="ic">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6">
              <path d="M12 2v20M12 2 8 6M12 2l4 4M12 22l-4-4M12 22l4-4M2.7 7l18.6 10M2.7 7l1.5 5.4M2.7 7 8 5.6M21.3 17l-1.5-5.4M21.3 17 16 18.4M21.3 7 2.7 17M21.3 7l-5.3-1.4M21.3 7l-1.5 5.4M2.7 17l5.3 1.4M2.7 17l1.5-5.4" />
            </svg>
          </div>
          <div>
            <h3>Cold tier · Lakehouse Sync active</h3>
            <p>
              Aged rows sync from the Lakebase branch to Unity Catalog via CDF. Historical
              (DBSQL) below is served from the same schema, backed by Delta.
            </p>
          </div>
          <span className="chip"><span className="dot run" />streaming</span>
        </div>
      )}
    </>
  );

  // Historical / DBSQL — all three modes. Ungated (no CDF gate) for
  // microbatch & zerobus, whose Delta table is the primary store.
  const historical = (p: PanelPipeline) => (
    <div className="sec" id="historical-dbsql">
      <div className="shead">
        <h2>Historical · DBSQL</h2>
        <span className="src-tag db">Delta · cold</span>
        <span className="note">last {DBSQL_MINUTES / 60}h · manual refresh</span>
        <button
          className="btn sm"
          style={{ marginLeft: "auto" }}
          onClick={() => dbsqlRefreshRef.current?.()}
          title="Run the DBSQL historical query now"
        >
          ↻ Refresh
        </button>
      </div>
      <div className="panel">
        <HistoricalDbsql
          pipelineId={p.id}
          mode={mode}
          industry={isIndustryFirst ? industryId : undefined}
          rangeMinutes={DBSQL_MINUTES}
          refreshSec={0}
          enabled={mode !== "realtime" || !!cdfStatus?.streaming}
          cdfStatus={cdfStatus}
          ungated={mode !== "realtime"}
          registerRefresh={(fn) => {
            dbsqlRefreshRef.current = fn;
          }}
        />
      </div>
    </div>
  );

  if (industry.id === "manufacturing" && pipeline && isLive) {
    return (
      <ManufacturingCellPage
        industry={industry}
        mode={mode}
        modeLabel={meta.fname}
        showKpis={meta.showLiveKpis}
        rules={alertRules}
        maintenance={maintenance}
        assets={domainAssets}
        zerobus={zerobusMetrics}
        wsConnected={wsConnected}
        powered={powered}
        isAdmin={isAdmin}
        pipelineId={pipeline.id}
        setupId={catalogMode?.setupId ?? null}
        refreshSec={refreshSec}
        onRefreshSec={setRefreshSec}
        nonce={lakebaseNonce}
        onRefreshNow={() => setLakebaseNonce((n) => n + 1)}
        pipeline={{
          label:
            effectiveStatus === "starting" ? "Job starting…" : effectiveStatus === "running" ? "Stop streaming job" : "Start streaming job",
          busy: powerBusy || effectiveStatus === "starting",
          onToggle: () => void handleTogglePower(),
        }}
        ingestPanel={mode === "zerobus" ? midTier(pipeline) : undefined}
        deepDive={
          <>
            {meta.showSensorStream && (
              <div className="sec">
                <SensorStreamPanel rows={streamRows} />
              </div>
            )}
            {mode !== "zerobus" && midTier(pipeline)}
            {hotTier(pipeline)}
            {historical(pipeline)}
          </>
        }
        style={accentStyle}
      />
    );
  }

  return (
    <section id="view-dashboard" style={accentStyle}>
      <div className="crumbs">
        <Link to="/industries">Industries</Link>
        <span className="sep">/</span>
        <Link to={`/i/${industry.id}`}>{industry.name}</Link>
        <span className="sep">/</span>
        <span>Dashboard</span>
      </div>

      <div className="topbar-db">
        <div>
          <h1 className="tt">
            {industry.name} twin <span className="badge">{meta.fname}</span>
          </h1>
          <div className="ts">
            {mode === "zerobus"
              ? <>Zerobus → <b>Delta table</b> · <code>{industry.id}</code></>
              : <>Kafka → <b>{catalogMode.name}</b> → {mode === "realtime" ? "Lakebase" : "Delta table"} · <code>{industry.id}</code></>}
          </div>
        </div>
        <div className="spacer" />
        <div className="pills">
          {meta.showSse && (
            <span className={`pill ${powered ? "" : "off"}`} key="SSE">
              <span className="d" style={{ background: "var(--sse)" }} />
              SSE
            </span>
          )}
          {meta.pills.map(([label, color]) => (
            <span className={`pill ${powered ? "" : "off"}`} key={label}>
              <span className="d" style={{ background: color }} />
              {label}
            </span>
          ))}
          {meta.showMetrics && (
            <span className={`pill ${streamingMetrics ? "" : "off"}`}>
              <span className="d" style={{ background: "var(--cdf)" }} />
              Metrics
            </span>
          )}
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
          <span className="note" style={{ fontSize: "11px", whiteSpace: "nowrap" }}>
            {meta.showLakebase ? "Lakebase refresh" : "Auto-refresh"}
          </span>
          <RefreshIntervalSelect
            value={refreshSec}
            onChange={setRefreshSec}
            onRefreshNow={() => setLakebaseNonce((n) => n + 1)}
          />
        </div>
        <button
          className={`btn ${effectiveStatus === "running" ? "" : "primary"}`}
          disabled={powerBusy || effectiveStatus === "starting"}
          onClick={handleTogglePower}
        >
          {effectiveStatus === "starting"
            ? "starting…"
            : effectiveStatus === "running"
              ? "■ Stop pipeline"
              : "▶ Start pipeline"}
        </button>
      </div>

      <div className={`dash-with-alerts${genieOpen ? " has-genie" : ""}`}>
      <AlertRail
        industry={industry.id}
        pipelineId={pipeline?.id ?? null}
        assets={domainAssets}
        maintenance={maintenance}
        rules={alertRules}
        onExplainAlert={(alert) => {
          setExplainAlert(alert);
          setGenieOpen(true);
        }}
      />
      <div>
      {!pipeline || !isLive ? (
        <EmptyState
          industry={industry}
          status={effectiveStatus}
          isAdmin={isAdmin}
          runtimeDetail={realModeView?.runtime_detail}
          onRefresh={() => {
            void refreshAll();
            void refreshAdminPipelines();
          }}
        />
      ) : (
        <>
            {/* KPI row — live SSE-driven tiles; only rendered for a mode that
                actually has a live tier (realtime). */}
            {meta.showLiveKpis && (
              <div className="kpis">
                {industry.kpis.map((kpi) => {
                  const live = liveKpiValue(kpi.k, sensors, kpiBindings);
                  return (
                    <div className={`kpi ${kpi.s ?? ""}`} key={kpi.k}>
                      <div className="rail-c" />
                      <div className="v tnum">
                        {powered ? live ?? kpi.v : "—"}
                        <small> {kpi.u}</small>
                      </div>
                      <div className="k">{kpi.k}</div>
                    </div>
                  );
                })}
              </div>
            )}

            {/* Section 1 — twin (+ live sensor stream for the SSE tier only) */}
            <div className="sec">
              <div className="shead">
                <h2>{meta.showSensorStream ? "Live twin & sensor stream" : "Twin preview"}</h2>
                <span className={`src-tag ${meta.twinTag[0]}`}>{meta.twinTag[1]}</span>
                <span className="note">{meta.twinNote}</span>
              </div>
              <div className={meta.showSensorStream ? "twin-grid" : ""}>
                <div className="twin">
                  {industry.id === "pharmaceutical" ? (
                    /* PharmaPlant draws its own source badge + "drag to orbit"
                       hint, so the page-level .lbl / .hint overlays are omitted
                       here to avoid a doubled label + hint over the 3D twin. */
                    <PharmaPlant
                      readings={pharmaReadings ?? undefined}
                      sourceLabel={
                        isPharmaRealtime
                          ? wsConnected
                            ? "● LIVE · SSE"
                            : "● SSE OFFLINE"
                          : mode === "zerobus" && zerobusMetrics?.latest_readings?.length
                            ? "● LIVE · ZEROBUS"
                            : "● PREVIEW"
                      }
                    />
                  ) : (
                    <>
                      <span className="lbl">{industry.name} · {meta.showSse ? "live" : "preview"}</span>
                      <span className="hint">drag to orbit · {meta.showSse ? "mock" : "preview"}</span>
                      <div
                        style={{ position: "absolute", inset: 0 }}
                        dangerouslySetInnerHTML={{ __html: TWIN_SVG[industry.twinType] ?? "" }}
                      />
                    </>
                  )}
                </div>
                {meta.showSensorStream && <SensorStreamPanel rows={streamRows} />}
              </div>
            </div>

          {midTier(pipeline)}
          {hotTier(pipeline)}
          {historical(pipeline)}
        </>
      )}
      </div>
      <GenieCopilot
        industry={industry.id}
        mode={mode}
        setupId={catalogMode?.setupId ?? pipelineId}
        isAdmin={isAdmin}
        assets={domainAssets}
        alert={explainAlert}
        open={genieOpen}
        onOpenChange={setGenieOpen}
      />
      </div>

      <footer>
        Per-mode dashboard · industry-first UI · DuBois chrome over live pipeline data.
      </footer>
    </section>
  );
}
