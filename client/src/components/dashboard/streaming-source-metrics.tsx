import { useCallback, useEffect, useState } from "react";
import {
  Bar,
  BarChart,
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
import { formatLatency, formatNumber } from "@/lib/utils";
import { QUEUING_P50_FIELD } from "@/lib/latency-series";
import type { SourceMetrics, StreamingLatencyResponse } from "@/lib/types";
import { SectionBadge } from "./section";
import { LatencyKpiTile, fmtLatency, fmtNum } from "./latency-kpis";

/** Streaming modes that emit a `streaming_metrics_<mode>` Delta table. */
export type StreamingLatencyMode = "realtime" | "microbatch";
// Re-enable after Kafka partition scaling makes event-to-insert lag trustworthy.
const SHOW_STREAMING_E2E_LATENCY = false;

const TOOLTIP_STYLE = {
  backgroundColor: "hsl(var(--card) / 0.9)",
  backdropFilter: "blur(12px)",
  border: "1px solid hsl(var(--border) / 0.5)",
  borderRadius: 8,
  fontSize: 11,
} as const;

export type StreamingSourceKind = "sss" | "zerobus";

interface ThroughputPoint {
  batch: string;
  input_rps: number;
  processed_rps: number;
}
interface DurationPoint {
  batch: string;
  duration_ms: number;
}
interface LatencyPoint {
  batch: string;
  e2e_p50_ms: number;
  processing_p50_ms: number;
  queuing_p50_ms: number;
}

interface StreamingSourceMetricsProps {
  /** Pipeline or pipeline_deployment id authorizing industry-scoped reads. */
  pipelineId: string;
  /** Resolved source kind for this pipeline (kafka/SSS vs zerobus). */
  sourceKind: StreamingSourceKind;
  sourceLabel: string;
  throughputData: ThroughputPoint[];
  batchDurationData: DurationPoint[];
  latencyData: LatencyPoint[];
  /** Polling interval seconds for the rolling /api/sources metrics. */
  refreshSec: number;
  /**
   * Message shown when no batch series has arrived yet. Defaults to the
   * "Waiting for … streaming metrics…" phrasing (realtime, where a running
   * pipeline is expected to report imminently). Callers whose job may sit idle
   * without data (e.g. micro-batch between commits) should pass an honest
   * idle string so the panel doesn't read as a stuck loader forever.
   */
  emptyLabel?: string;
  /**
   * Industry id — when set together with ``mode`` the component self-fetches
   * the latest latency KPIs from the per-mode ``streaming_metrics_<mode>``
   * Delta table (``GET /api/latency/streaming``) and renders them as KPI tiles
   * (e2e / processing / queuing p50+p99, input/processed rps, trigger interval).
   * Omitting either prop hides that KPI row (existing callers stay unchanged).
   */
  industry?: string;
  /** Streaming mode for the latency-KPI fetch (realtime | microbatch). */
  mode?: StreamingLatencyMode;
  /**
   * Register a manual-refresh callback with the parent. The parent can call it
   * (e.g. a ↻ Refresh button) to re-fetch the rolling + latency metrics on
   * demand — works even when the auto-refresh interval is Off.
   */
  registerRefresh?: (fn: () => void) => void;
}

interface MiniStatProps {
  label: string;
  value: string;
  accent?: string;
}

function MiniStat({ label, value, accent }: MiniStatProps) {
  return (
    <div className="rounded-md bg-surface-low border border-border/50 px-3 py-2">
      <p className="text-[9px] uppercase tracking-wider text-muted-foreground">
        {label}
      </p>
      <p
        className="font-mono text-lg font-semibold tabular-nums"
        style={accent ? { color: accent } : undefined}
      >
        {value}
      </p>
    </div>
  );
}

function ChartBlock({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <div>
      <p className="text-[9px] text-muted-foreground font-semibold mb-1 uppercase tracking-wider">
        {title}
      </p>
      {children}
    </div>
  );
}

/**
 * Section 2 — source-aware streaming metrics. Renders the already-computed
 * Spark Structured Streaming series (input/processed rows-per-sec, batch
 * duration, latency p50) as Recharts charts, plus the rolling rolling
 * metrics for the matching source from ``/api/sources``.
 */
export function StreamingSourceMetrics({
  pipelineId,
  sourceKind,
  sourceLabel,
  throughputData,
  batchDurationData,
  latencyData,
  refreshSec,
  emptyLabel,
  industry,
  mode,
  registerRefresh,
}: StreamingSourceMetricsProps) {
  const isZerobus = sourceKind === "zerobus";
  const [rolling, setRolling] = useState<SourceMetrics | null>(null);
  const [latency, setLatency] = useState<StreamingLatencyResponse | null>(null);

  const fetchRolling = useCallback(async () => {
    try {
      const all = (await api.getSources()) as Record<string, SourceMetrics>;
      setRolling(all?.[sourceKind] ?? null);
    } catch {
      /* sources may be unavailable */
    }
  }, [sourceKind]);

  // Latency KPIs from the per-mode streaming-metrics Delta table — only when
  // both `industry` and `mode` are supplied by the caller.
  const fetchLatency = useCallback(async () => {
    if (!industry || !mode) {
      setLatency(null);
      return;
    }
    try {
      const data = await api.getStreamingLatency(pipelineId, industry, mode);
      setLatency(data);
    } catch {
      /* metrics table may not exist yet — keep prior/empty */
    }
  }, [pipelineId, industry, mode]);

  useEffect(() => {
    void fetchRolling();
    if (refreshSec <= 0) return;
    const id = setInterval(() => void fetchRolling(), refreshSec * 1000);
    return () => clearInterval(id);
  }, [fetchRolling, refreshSec]);

  useEffect(() => {
    void fetchLatency();
    if (refreshSec <= 0) return;
    const id = setInterval(() => void fetchLatency(), refreshSec * 1000);
    return () => clearInterval(id);
  }, [fetchLatency, refreshSec]);

  // Expose a manual refresh to the parent (a ↻ button) — fires both fetches on
  // demand, independent of the auto-refresh interval (works when it's Off).
  useEffect(() => {
    if (!registerRefresh) return;
    registerRefresh(() => {
      void fetchRolling();
      void fetchLatency();
    });
    return () => registerRefresh(() => {});
  }, [registerRefresh, fetchRolling, fetchLatency]);

  const rateUnit = isZerobus ? "records/s" : "rows/s";
  const hasStreamSeries =
    throughputData.length > 0 ||
    batchDurationData.length > 0 ||
    latencyData.length > 0;
  const sparkProcessedRps = latency?.processed_rps;
  const sparkProcessingP50 = latency?.processing_p50;
  const sparkProcessingP99 = latency?.processing_p99;
  const sparkE2eP99 = latency?.e2e_p99;

  return (
    <div className="space-y-4">
      {/* Rolling source metrics (from /api/sources) */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <MiniStat
          label={isZerobus ? "Records/s" : "Processed rows/s"}
          value={
            isZerobus
              ? rolling
                ? `${formatNumber(rolling.records_per_sec ?? 0, 1)}`
                : "---"
              : sparkProcessedRps != null
                ? formatNumber(sparkProcessedRps, 1)
                : "---"
          }
        />
        <MiniStat
          label={isZerobus ? "Ingest Latency (avg)" : "Processing p50"}
          value={
            isZerobus
              ? rolling && rolling.avg_latency_ms > 0
                ? formatLatency(rolling.avg_latency_ms)
                : "---"
              : sparkProcessingP50 != null
                ? formatLatency(sparkProcessingP50)
                : "---"
          }
        />
        <MiniStat
          label={isZerobus ? "Latency p95" : "Processing p99"}
          value={
            isZerobus
              ? rolling && rolling.p95_latency_ms > 0
                ? formatLatency(rolling.p95_latency_ms)
                : "---"
              : sparkProcessingP99 != null
                ? formatLatency(sparkProcessingP99)
                : "---"
          }
          accent="#f59e0b"
        />
        <MiniStat
          label={isZerobus ? "Buffer Len" : "Spark E2E p99"}
          value={
            isZerobus
              ? rolling?.buffer_len != null
                ? formatNumber(rolling.buffer_len)
                : "---"
              : SHOW_STREAMING_E2E_LATENCY && sparkE2eP99 != null
                ? formatLatency(sparkE2eP99)
                : "---"
          }
        />
      </div>

      {/* Latency + throughput KPIs from the per-mode metrics Delta table.
          Only rendered when the caller passes `industry` + `mode`. */}
      {industry && mode && (
        <div className="space-y-2">
          <p className="text-[9px] text-muted-foreground font-semibold uppercase tracking-wider">
            Latency percentiles · metrics table
            {latency && !latency.has_data ? " · waiting for first batch" : ""}
            {latency?.last_timestamp
              ? ` · last captured ${new Date(latency.last_timestamp).toLocaleTimeString()}`
              : ""}
          </p>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <LatencyKpiTile label="E2E p50" value={fmtLatency(latency?.e2e_p50)} accent="#f59e0b" />
            <LatencyKpiTile label="E2E p99" value={fmtLatency(latency?.e2e_p99)} accent="#f59e0b" />
            <LatencyKpiTile label="Processing p50" value={fmtLatency(latency?.processing_p50)} accent="#3b82f6" />
            <LatencyKpiTile label="Processing p99" value={fmtLatency(latency?.processing_p99)} accent="#3b82f6" />
            <LatencyKpiTile label="Queuing p50" value={fmtLatency(latency?.queuing_p50)} accent="#94a3b8" />
            <LatencyKpiTile label="Queuing p99" value={fmtLatency(latency?.queuing_p99)} accent="#94a3b8" />
            <LatencyKpiTile label="Input rps" value={fmtNum(latency?.input_rps, 1)} accent="#3fb950" />
            <LatencyKpiTile
              label="Processed rps"
              value={fmtNum(latency?.processed_rps, 1)}
              accent="#3fb950"
              sub={latency?.trigger_ms != null ? `trigger ${fmtLatency(latency.trigger_ms)}` : undefined}
            />
          </div>
        </div>
      )}

      {/* Spark Structured Streaming series */}
      {hasStreamSeries ? (
        <div className="space-y-4">
          {throughputData.length > 0 && (
            <ChartBlock title={`Rows / sec — input vs processed (${rateUnit})`}>
              <ResponsiveContainer width="100%" height={170}>
                <LineChart data={throughputData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border) / 0.4)" />
                  <XAxis dataKey="batch" stroke="hsl(var(--muted-foreground))" fontSize={8} tickLine={false} />
                  <YAxis stroke="hsl(var(--muted-foreground))" fontSize={8} tickLine={false} />
                  <RechartsTooltip contentStyle={TOOLTIP_STYLE} />
                  <Line type="monotone" dataKey="input_rps" stroke="#3fb950" strokeWidth={2} dot={false} name={`Input ${rateUnit}`} />
                  <Line type="monotone" dataKey="processed_rps" stroke="#3b82f6" strokeWidth={2} dot={false} name={`Processed ${rateUnit}`} />
                  <Legend wrapperStyle={{ fontSize: 10 }} />
                </LineChart>
              </ResponsiveContainer>
            </ChartBlock>
          )}

          {batchDurationData.length > 0 && (
            <ChartBlock title={isZerobus ? "Ingest Batch Duration" : "Batch Duration"}>
              <ResponsiveContainer width="100%" height={150}>
                <BarChart data={batchDurationData} barCategoryGap="20%">
                  <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border) / 0.4)" vertical={false} />
                  <XAxis dataKey="batch" stroke="hsl(var(--muted-foreground))" fontSize={8} tickLine={false} />
                  <YAxis stroke="hsl(var(--muted-foreground))" fontSize={8} tickLine={false} unit="ms" />
                  <RechartsTooltip contentStyle={TOOLTIP_STYLE} />
                  <Bar dataKey="duration_ms" fill="#a855f7" name="Batch Duration (ms)" radius={[2, 2, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </ChartBlock>
          )}

          {latencyData.length > 0 && (
            <ChartBlock title="Latency — p50 / p99">
              <ResponsiveContainer width="100%" height={170}>
                <LineChart data={latencyData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border) / 0.4)" />
                  <XAxis dataKey="batch" stroke="hsl(var(--muted-foreground))" fontSize={8} tickLine={false} />
                  <YAxis stroke="hsl(var(--muted-foreground))" fontSize={8} tickLine={false} unit="ms" />
                  <RechartsTooltip contentStyle={TOOLTIP_STYLE} />
                  <Line type="monotone" dataKey="e2e_p50_ms" stroke="#f59e0b" strokeWidth={2} dot={false} name="E2E p50" />
                  <Line type="monotone" dataKey="processing_p50_ms" stroke="#3b82f6" strokeWidth={2} dot={false} name="Processing p50" />
                  <Line type="monotone" dataKey={QUEUING_P50_FIELD} stroke="#94a3b8" strokeWidth={2} dot={false} name="Queuing p50" />
                  <Legend wrapperStyle={{ fontSize: 10 }} />
                </LineChart>
              </ResponsiveContainer>
            </ChartBlock>
          )}
        </div>
      ) : latency?.has_data ? null : (
        // Only a true "no data anywhere" state shows the waiting message — when
        // the latency KPIs have data, the percentile tiles above already convey
        // it (the per-mode metrics table is column-based, so there's no batch
        // chart series here to plot).
        <div className="flex h-[120px] items-center justify-center rounded-md bg-surface-low border border-border/50 font-ui text-xs text-muted-foreground text-center px-4">
          {emptyLabel ?? `Waiting for ${sourceLabel} streaming metrics...`}
        </div>
      )}

      <div className="flex justify-end">
        <SectionBadge color="#3b82f6" label={`SOURCE · ${sourceLabel}`} />
      </div>
    </div>
  );
}
