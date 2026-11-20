import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Clock3, Lock, Snowflake, Flame } from "lucide-react";
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip as RechartsTooltip,
  XAxis,
  YAxis,
} from "recharts";
import { api } from "@/lib/api";
import type { CdfStatusResponse, DbsqlTimeseriesResponse } from "@/lib/types";
import type { PipelineMode } from "@/lib/industries-api";

const DBSQL = "#a855f7";

export const TOOLTIP_STYLE = {
  backgroundColor: "hsl(var(--card) / 0.9)",
  backdropFilter: "blur(12px)",
  border: "1px solid hsl(var(--border) / 0.5)",
  borderRadius: 8,
  fontSize: 11,
} as const;

export const SERIES_COLORS = ["#a855f7", "#3b82f6", "#3fb950", "#f59e0b", "#ec4899", "#22d3ee"];
const MIXED_SCALE_RATIO = 8;

export function formatAxisTick(value: number): string {
  const abs = Math.abs(value);
  if (abs >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(abs >= 10_000_000 ? 0 : 1)}M`;
  }
  if (abs >= 10_000) return `${Math.round(value / 1_000)}k`;
  if (abs >= 1_000) return `${(value / 1_000).toFixed(1)}k`;
  return String(Math.round(value));
}

export function pickCompatibleSeries(
  seriesKeys: string[],
  chartData: Array<Record<string, unknown>>,
): string[] {
  if (seriesKeys.length <= 1) return seriesKeys;
  const ranked = seriesKeys
    .map((key) => ({
      key,
      max: Math.max(0, ...chartData.map((row) => Number(row[key] ?? 0) || 0)),
    }))
    .filter((item) => item.max > 0)
    .sort((a, b) => b.max - a.max);
  if (ranked.length === 0) return seriesKeys.slice(0, 1);
  const peak = ranked[0].max;
  const compatible = ranked
    .filter((item) => peak / item.max <= MIXED_SCALE_RATIO)
    .map((item) => item.key);
  return compatible.length > 0 ? compatible : [ranked[0].key];
}

const ENABLE_STEPS: readonly string[] = [
  "Open the pipeline's Lakebase project",
  "Go to the branch overview",
  "Open the CDF tab and click Start sync",
  "Select the `lakets_cdf` schema",
  "Choose a destination and start the sync",
];

interface DbsqlChannelState {
  lastRefresh: number | null;
  error: string | null;
  loading: boolean;
}

interface HistoricalDbsqlProps {
  pipelineId: string;
  /** Streaming mode — selects the per-mode DBSQL table on the backend
   * (realtime/microbatch/zerobus). Callers that know the mode (e.g. the
   * per-mode dashboard) should always pass it; when omitted the backend
   * infers it from the pipeline's own source config. */
  mode?: PipelineMode;
  /** Industry id — lets the backend resolve the catalog as iot_<industry>
   * directly (industry-first pipelines have no legacy `pipelines` row / catalog
   * mapping). Passed by the per-mode dashboard. */
  industry?: string;
  rangeMinutes: number;
  refreshSec: number;
  /** When false the section stays mounted but skips fetching. */
  enabled: boolean;
  /** CDF status lifted to the page so the top-bar pill + section share it. */
  cdfStatus: CdfStatusResponse | null;
  /**
   * Bypass the CDF gate entirely.  Used for Zerobus pipelines, where the Delta
   * table is the PRIMARY store and is always queryable (no Lakebase Sync / CDF
   * involved).  When true the enablement card and hot/cold tier summary are
   * suppressed and the Delta time-series is queried unconditionally.
   */
  ungated?: boolean;
  /** Reports cold-tier query health up to the page (drives the DBSQL pill). */
  onStateChange?: (state: DbsqlChannelState) => void;
  /** Hands a manual-refresh callback back to the page (DBSQL pill click). */
  registerRefresh?: (refresh: () => void) => void;
}

function CdfGate({ status }: { status: CdfStatusResponse | null }) {
  return (
    <div
      className="rounded-md border p-4 space-y-3"
      style={{ borderColor: "#f59e0b55", background: "#f59e0b12" }}
    >
      <div className="flex items-center gap-2">
        <Lock className="h-4 w-4" style={{ color: "#f59e0b" }} />
        <p className="font-display text-sm font-semibold" style={{ color: "#f59e0b" }}>
          Historical analytics unavailable — Lakebase Sync (CDF) is not enabled
        </p>
      </div>
      <ol className="list-decimal pl-5 space-y-1 font-ui text-[11px] text-muted-foreground">
        {ENABLE_STEPS.map((step) => (
          <li key={step}>{step}</li>
        ))}
      </ol>
      <div className="rounded bg-surface-low border border-border/50 px-3 py-2">
        <p className="text-[10px] text-muted-foreground">
          Track progress in the pipeline's Lakebase:
        </p>
        <code className="block font-mono text-[11px] text-foreground mt-1">
          SELECT status FROM wal2delta.tables;
        </code>
        <p className="text-[10px] text-muted-foreground mt-1">
          Status moves <span className="font-mono">SNAPSHOTTING</span> →{" "}
          <span className="font-mono">STREAMING</span> once the cold tier is live.
        </p>
        {status?.status === "SNAPSHOTTING" && (
          <p className="text-[10px] mt-1" style={{ color: "#f59e0b" }}>
            Current status: SNAPSHOTTING (initial backfill in progress)
          </p>
        )}
        {status?.reason && (
          <p className="text-[9px] text-muted-foreground mt-1 truncate">{status.reason}</p>
        )}
      </div>
    </div>
  );
}

function TierSummary({
  status,
  coldLatestAt,
}: {
  status: CdfStatusResponse;
  /** Cold Delta freshness from DBSQL, refreshed with the time-series. */
  coldLatestAt?: string | null;
}) {
  const hotMs = timestampMs(status.hot_latest_at);
  const coldMs = timestampMs(coldLatestAt);
  const replicationLagMs =
    hotMs != null && coldMs != null ? Math.max(0, hotMs - coldMs) : null;

  return (
    <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
      <div className="rounded-md bg-surface-low border border-border/50 px-3 py-2.5">
        <div className="flex items-center gap-1.5">
          <Flame className="h-3.5 w-3.5" style={{ color: "#3b82f6" }} />
          <p className="text-[9px] uppercase tracking-wider text-muted-foreground">Latest Hot Event</p>
        </div>
        <p
          className="font-mono text-lg font-bold tabular-nums"
          style={{ color: "#3b82f6" }}
          title={status.hot_latest_at ?? undefined}
        >
          {formatTimestamp(status.hot_latest_at)}
        </p>
        <p className="text-[9px] text-muted-foreground">{formatAge(status.hot_latest_at)}</p>
      </div>
      <div className="rounded-md bg-surface-low border border-border/50 px-3 py-2.5">
        <div className="flex items-center gap-1.5">
          <Snowflake className="h-3.5 w-3.5" style={{ color: DBSQL }} />
          <p className="text-[9px] uppercase tracking-wider text-muted-foreground">Latest Cold Event</p>
        </div>
        <p
          className="font-mono text-lg font-bold tabular-nums"
          style={{ color: DBSQL }}
          title={coldLatestAt ?? undefined}
        >
          {formatTimestamp(coldLatestAt)}
        </p>
        <p className="text-[9px] text-muted-foreground">{formatAge(coldLatestAt)}</p>
      </div>
      <div className="rounded-md bg-surface-low border border-border/50 px-3 py-2.5">
        <div className="flex items-center gap-1.5">
          <Clock3 className="h-3.5 w-3.5" style={{ color: "#f59e0b" }} />
          <p className="text-[9px] uppercase tracking-wider text-muted-foreground">Replication Delay</p>
        </div>
        <p className="font-mono text-lg font-bold tabular-nums" style={{ color: "#f59e0b" }}>
          {formatDuration(replicationLagMs)}
        </p>
        <p className="text-[9px] text-muted-foreground">Lakebase → Delta</p>
      </div>
    </div>
  );
}

function timestampMs(value: string | null | undefined): number | null {
  if (!value) return null;
  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp) ? timestamp : null;
}

function formatTimestamp(value: string | null | undefined): string {
  const timestamp = timestampMs(value);
  if (timestamp == null) return "—";
  return new Date(timestamp).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function formatDuration(durationMs: number | null): string {
  if (durationMs == null) return "—";
  if (durationMs < 1_000) return "<1s";
  const seconds = Math.round(durationMs / 1_000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const remainder = seconds % 60;
  if (minutes < 60) return `${minutes}m ${remainder}s`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m`;
}

function formatAge(value: string | null | undefined): string {
  const timestamp = timestampMs(value);
  if (timestamp == null) return "No timestamp available";
  return `${formatDuration(Math.max(0, Date.now() - timestamp))} ago`;
}

function CdfLoading() {
  return (
    <div className="rounded-md border border-border/50 bg-surface-low p-4">
      <div className="flex items-center gap-2">
        <Clock3 className="h-4 w-4 text-muted-foreground" />
        <p className="font-ui text-xs text-muted-foreground">
          Checking Lakebase Sync status…
        </p>
      </div>
    </div>
  );
}

/**
 * Section 4 — Historical / DBSQL cold tier. Gates on ``cdf-status``: when CDF
 * is not streaming it renders an informational enablement card (no empty
 * charts); when streaming it shows the DBSQL historical time-series plus a
 * hot-vs-cold tier summary.
 */
export function HistoricalDbsql({
  pipelineId,
  mode,
  industry,
  rangeMinutes,
  refreshSec,
  enabled,
  cdfStatus,
  ungated = false,
  onStateChange,
  registerRefresh,
}: HistoricalDbsqlProps) {
  const [ts, setTs] = useState<DbsqlTimeseriesResponse | null>(null);
  const onStateChangeRef = useRef(onStateChange);
  onStateChangeRef.current = onStateChange;

  // ``ungated`` (Zerobus) treats the Delta table as always queryable.
  const streaming = ungated || cdfStatus?.streaming === true;

  // DBSQL cold-tier time-series — only fetched when CDF is streaming.  The
  // query is fixed to the DBSQL (cold) plane; it never falls back to Lakebase.
  const fetchTimeseries = useCallback(async () => {
    if (!pipelineId || !enabled || !streaming) return;
    onStateChangeRef.current?.({ lastRefresh: null, error: null, loading: true });
    try {
      const data = await api.getDbsqlTimeseries(pipelineId, rangeMinutes || 60, mode, industry);
      setTs(data);
      onStateChangeRef.current?.({ lastRefresh: Date.now(), error: null, loading: false });
    } catch (err: unknown) {
      onStateChangeRef.current?.({
        lastRefresh: null,
        error: err instanceof Error ? err.message : "Cold-tier query failed",
        loading: false,
      });
    }
  }, [pipelineId, mode, industry, enabled, streaming, rangeMinutes]);

  useEffect(() => {
    registerRefresh?.(() => {
      void fetchTimeseries();
    });
  }, [registerRefresh, fetchTimeseries]);

  useEffect(() => {
    if (!pipelineId || !enabled || !streaming) return;
    let cancelled = false;
    const run = () => {
      if (!cancelled) void fetchTimeseries();
    };
    run();
    if (refreshSec <= 0) return () => {
      cancelled = true;
    };
    const id = setInterval(run, refreshSec * 1000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [pipelineId, enabled, streaming, refreshSec, fetchTimeseries]);

  const { chartData, seriesKeys } = useMemo(() => {
    const rows = ts?.rows ?? [];
    if (rows.length === 0) return { chartData: [], seriesKeys: [] as string[] };
    const byTime: Record<string, Record<string, { sum: number; count: number }>> = {};
    for (const r of rows) {
      const tsStr = String(r.time ?? r.timestamp ?? r.bucket ?? "");
      const bucket = tsStr.slice(0, 16);
      const stype = String(r.sensor_type ?? "value");
      byTime[bucket] ??= {};
      byTime[bucket][stype] ??= { sum: 0, count: 0 };
      byTime[bucket][stype].sum += Number(r.value ?? r.avg_value ?? 0);
      byTime[bucket][stype].count += 1;
    }
    const keys = new Set<string>();
    const data = Object.entries(byTime)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([bucket, types]) => {
        const point: Record<string, unknown> = { time: bucket.slice(11) || bucket };
        for (const [stype, agg] of Object.entries(types)) {
          keys.add(stype);
          point[stype] = Math.round((agg.sum / agg.count) * 100) / 100;
        }
        return point;
      });
    return { chartData: data.slice(-120), seriesKeys: Array.from(keys) };
  }, [ts]);

  const defaultSeries = useMemo(
    () => pickCompatibleSeries(seriesKeys, chartData),
    [seriesKeys, chartData],
  );
  const [visibleSeries, setVisibleSeries] = useState<string[]>([]);
  const plottedSeries = visibleSeries.length > 0 ? visibleSeries : defaultSeries;

  if (!ungated && cdfStatus === null) {
    return <CdfLoading />;
  }

  if (!streaming) {
    return <CdfGate status={cdfStatus} />;
  }

  return (
    <div className="space-y-4">
      {/* Hot/cold tier summary is Lakebase-specific — hidden when ungated
          (Zerobus), where Delta is the sole/primary store. */}
      {!ungated && cdfStatus && (
        <TierSummary status={cdfStatus} coldLatestAt={ts?.latest_event_at} />
      )}
      <div>
        <p className="text-[9px] text-muted-foreground font-semibold mb-1 uppercase tracking-wider">
          Historical time-series · Delta cold tier
        </p>
        {seriesKeys.length > 1 && (
          <div className="flex flex-wrap gap-1.5 mb-2">
            {seriesKeys.map((key) => {
              const on = plottedSeries.includes(key);
              return (
                <button
                  key={key}
                  type="button"
                  className="btn sm"
                  aria-pressed={on}
                  onClick={() =>
                    setVisibleSeries((current) => {
                      const base = current.length > 0 ? current : defaultSeries;
                      if (base.includes(key)) {
                        const next = base.filter((item) => item !== key);
                        return next.length > 0 ? next : [key];
                      }
                      return [...base, key];
                    })
                  }
                >
                  {key}
                </button>
              );
            })}
          </div>
        )}
        {chartData.length > 0 ? (
          <ResponsiveContainer width="100%" height={250}>
            <LineChart data={chartData} margin={{ top: 12, right: 16, left: 8, bottom: 8 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border) / 0.4)" />
              <XAxis dataKey="time" stroke="hsl(var(--muted-foreground))" fontSize={9} tickLine={false} />
              <YAxis
                stroke="hsl(var(--muted-foreground))"
                fontSize={9}
                tickLine={false}
                width={44}
                tickFormatter={formatAxisTick}
              />
              <RechartsTooltip contentStyle={TOOLTIP_STYLE} />
              {plottedSeries.map((key, i) => (
                <Line
                  key={key}
                  type="monotone"
                  dataKey={key}
                  stroke={SERIES_COLORS[i % SERIES_COLORS.length]}
                  strokeWidth={2}
                  dot={chartData.length < 8}
                  name={key}
                />
              ))}
              <Legend wrapperStyle={{ fontSize: 10 }} />
            </LineChart>
          </ResponsiveContainer>
        ) : (
          <div className="flex h-[250px] items-center justify-center rounded-md bg-surface-low border border-border/50 font-ui text-xs text-muted-foreground">
            Querying cold-tier Delta data...
          </div>
        )}
      </div>
    </div>
  );
}
