import { useCallback, useEffect, useState } from "react";
import { AlertTriangle, Database } from "lucide-react";
import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip as RechartsTooltip,
  XAxis,
  YAxis,
} from "recharts";
import { api } from "@/lib/api";
import { formatNumber } from "@/lib/utils";
import type { ZerobusLatencyResponse, ZerobusMetricsResponse } from "@/lib/types";
import { LatencyKpiTile, fmtBytes, fmtFreshness, fmtNum } from "./latency-kpis";

/** Zerobus accent — pink. */
const ZEROBUS = "#ec4899";

const TOOLTIP_STYLE = {
  backgroundColor: "hsl(var(--card) / 0.9)",
  backdropFilter: "blur(12px)",
  border: "1px solid hsl(var(--border) / 0.5)",
  borderRadius: 8,
  fontSize: 11,
} as const;

interface ZerobusIngestMetricsProps {
  pipelineId: string;
  /** Lookback window in minutes. */
  minutes: number;
  /**
   * Poll interval (seconds) governed by the dashboard refresh-interval
   * selector. ``0`` = paused (manual refresh only).
   */
  refreshSec: number;
  /** Reports the latest metrics back up so the page can drive the twin. */
  onMetrics?: (metrics: ZerobusMetricsResponse | null) => void;
  /**
   * Industry id — when set, the component also self-fetches the Zerobus
   * commit-freshness/health KPIs from ``GET /api/latency/zerobus`` (age of
   * the latest commit, records/min throughput, committed bytes,
   * recent errors) and renders them as a KPI row. Omitting it hides that row.
   */
  industry?: string;
  /** True when pipelineId is a pipeline_deployment id, not a legacy pipeline id. */
  industryFirst?: boolean;
}

interface BigReadoutProps {
  label: string;
  value: string;
  accent?: boolean;
  sub?: string;
}

function BigReadout({ label, value, accent, sub }: BigReadoutProps) {
  return (
    <div className="rounded-md bg-surface-low border border-border/50 px-3 py-2.5 text-center">
      <p className="text-[9px] uppercase tracking-wider text-muted-foreground">{label}</p>
      <p
        className="font-mono text-2xl font-bold tabular-nums"
        style={accent ? { color: ZEROBUS } : undefined}
      >
        {value}
      </p>
      {sub && (
        <p className="text-[8px] text-muted-foreground mt-0.5 leading-tight">{sub}</p>
      )}
    </div>
  );
}

const STREAM_STATUS_LABEL: Record<ZerobusMetricsResponse["stream_status"], string> = {
  open: "STREAM · OPEN",
  closed: "STREAM · CLOSED",
  none: "STREAM · NONE",
};

const STREAM_STATUS_COLOR: Record<ZerobusMetricsResponse["stream_status"], string> = {
  open: "#3fb950",
  closed: "#f59e0b",
  none: "#94a3b8",
};

/** Format a byte count into a compact human-readable string. */
function formatBytes(bytes: number): string {
  if (!bytes || bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const exp = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const value = bytes / Math.pow(1024, exp);
  return `${formatNumber(value, exp === 0 ? 0 : 1)} ${units[exp]}`;
}

/**
 * Zerobus · Delta Ingest — source-aware streaming/ingest section for Zerobus
 * pipelines. Zerobus writes directly to a Delta table via gRPC (no message
 * bus, no Lakebase, no SSE), so this surfaces the ingest stream throughput,
 * the Delta target totals, stream status and any ingest errors.
 */
export function ZerobusIngestMetrics({
  pipelineId,
  minutes,
  refreshSec,
  onMetrics,
  industry,
  industryFirst = false,
}: ZerobusIngestMetricsProps) {
  const [metrics, setMetrics] = useState<ZerobusMetricsResponse | null>(null);
  const [latency, setLatency] = useState<ZerobusLatencyResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  const fetchOnce = useCallback(async (): Promise<ZerobusMetricsResponse | null> => {
    const data = await api.getZerobusMetrics(
      pipelineId,
      minutes,
      industryFirst ? industry : undefined,
    );
    return data;
  }, [pipelineId, minutes, industry, industryFirst]);

  useEffect(() => {
    if (!pipelineId) return;
    let cancelled = false;
    const run = async () => {
      try {
        const data = await fetchOnce();
        if (cancelled) return;
        setMetrics(data);
        setError(null);
        onMetrics?.(data);
      } catch (err: unknown) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : "Unable to load Zerobus ingest metrics");
      }
    };
    run();
    if (refreshSec <= 0) {
      return () => {
        cancelled = true;
      };
    }
    const id = setInterval(run, refreshSec * 1000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [pipelineId, refreshSec, fetchOnce, onMetrics]);

  // Commit-freshness/health KPIs from system.lakeflow.zerobus_ingest — only when
  // an `industry` is supplied by the caller.
  useEffect(() => {
    if (!industry) {
      setLatency(null);
      return;
    }
    let cancelled = false;
    const run = async () => {
      try {
        const data = await api.getZerobusLatency(pipelineId, industry);
        if (!cancelled) setLatency(data);
      } catch {
        /* system tables may be inaccessible — keep prior/empty */
      }
    };
    run();
    if (refreshSec <= 0) {
      return () => {
        cancelled = true;
      };
    }
    const id = setInterval(run, refreshSec * 1000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [pipelineId, industry, refreshSec]);

  const series = (metrics?.series ?? []).map((p) => ({
    t: p.t ? String(p.t).slice(11, 16) : "",
    records_per_min: p.records_per_min,
  }));

  const status = metrics?.stream_status ?? "none";
  const recentErrors = metrics?.recent_errors ?? [];

  return (
    <div className="space-y-4">
      {/* Delta target + stream status header */}
      <div className="flex flex-wrap items-center gap-2">
        <Database className="h-4 w-4" style={{ color: ZEROBUS }} />
        <span className="font-mono text-xs text-foreground/80">
          {metrics?.table_name ?? "—"}
        </span>
        <span
          className="ml-auto inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 font-mono text-[10px] font-medium ring-1"
          style={{
            background: `${STREAM_STATUS_COLOR[status]}1f`,
            color: STREAM_STATUS_COLOR[status],
            boxShadow: `inset 0 0 0 1px ${STREAM_STATUS_COLOR[status]}55`,
          }}
        >
          <span className="h-1.5 w-1.5 rounded-full" style={{ background: STREAM_STATUS_COLOR[status] }} />
          {STREAM_STATUS_LABEL[status]}
        </span>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <BigReadout
          label="Records / sec"
          value={metrics ? formatNumber(metrics.records_per_sec, 2) : "---"}
          accent
        />
        <BigReadout
          label="Current records / min"
          value={metrics ? formatNumber(metrics.records_per_min) : "---"}
          accent
        />
        <BigReadout
          label="Total Records"
          value={metrics ? formatNumber(metrics.total_records) : "---"}
          sub={metrics?.last_commit_time ? `last: ${metrics.last_commit_time.slice(11, 19)}` : undefined}
        />
        <BigReadout
          label="Total Bytes"
          value={metrics ? formatBytes(metrics.total_bytes) : "---"}
          sub={metrics ? `${metrics.error_count} errors` : undefined}
        />
      </div>

      {/* Commit-freshness/health KPIs (system.lakeflow.zerobus_ingest) — only
          when the caller passes `industry`. */}
      {industry && (
        <div className="space-y-2">
          <p className="text-[9px] text-muted-foreground font-semibold uppercase tracking-wider">
            Commit freshness &amp; health · system table
            {latency && !latency.has_data ? " · no commits in window" : ""}
          </p>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <LatencyKpiTile
              label="Commit freshness"
              value={fmtFreshness(latency?.seconds_since_commit)}
              accent="#ec4899"
              sub="age of latest commit"
            />
            <LatencyKpiTile
              label="60m avg records / min"
              value={fmtNum(latency?.records_per_min, 1)}
              accent="#ec4899"
            />
            <LatencyKpiTile
              label="Committed Bytes"
              value={fmtBytes(latency?.committed_bytes_total)}
            />
            <LatencyKpiTile
              label="Recent Errors"
              value={latency ? String(latency.recent_error_count) : "—"}
              accent={latency && latency.recent_error_count > 0 ? "#ef4444" : undefined}
            />
          </div>
          {latency && latency.recent_errors.length > 0 && (
            <ul className="space-y-0.5">
              {latency.recent_errors.slice(0, 3).map((err, i) => (
                <li key={i} className="font-mono text-[10px] text-muted-foreground truncate">
                  {err}
                </li>
              ))}
            </ul>
          )}
        </div>
      )}

      <div>
        <p className="text-[9px] text-muted-foreground font-semibold mb-1 uppercase tracking-wider">
          Records / min (last {minutes}m)
        </p>
        {series.length > 0 ? (
          <ResponsiveContainer width="100%" height={200}>
            <AreaChart data={series}>
              <defs>
                <linearGradient id="zbIngestFill" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={ZEROBUS} stopOpacity={0.35} />
                  <stop offset="100%" stopColor={ZEROBUS} stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border) / 0.4)" />
              <XAxis dataKey="t" stroke="hsl(var(--muted-foreground))" fontSize={9} tickLine={false} />
              <YAxis stroke="hsl(var(--muted-foreground))" fontSize={9} tickLine={false} />
              <RechartsTooltip contentStyle={TOOLTIP_STYLE} />
              <Area
                type="monotone"
                dataKey="records_per_min"
                stroke={ZEROBUS}
                strokeWidth={2}
                fill="url(#zbIngestFill)"
                name="records/min"
              />
            </AreaChart>
          </ResponsiveContainer>
        ) : (
          <div className="flex h-[200px] items-center justify-center rounded-md bg-surface-low border border-border/50 font-ui text-xs text-muted-foreground">
            {error ?? "Waiting for Zerobus ingest activity..."}
          </div>
        )}
      </div>

      {/* Recent ingest errors */}
      {recentErrors.length > 0 && (
        <div
          className="rounded-md border p-3 space-y-1"
          style={{ borderColor: "#ef444455", background: "#ef444412" }}
        >
          <div className="flex items-center gap-1.5">
            <AlertTriangle className="h-3.5 w-3.5 text-destructive" />
            <p className="font-display text-xs font-semibold text-destructive">
              Recent ingest errors ({metrics?.error_count ?? recentErrors.length})
            </p>
          </div>
          <ul className="space-y-0.5">
            {recentErrors.slice(0, 5).map((err, i) => (
              <li key={i} className="font-mono text-[10px] text-muted-foreground truncate">
                {err}
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}
