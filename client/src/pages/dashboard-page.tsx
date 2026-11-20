import { useMemo, useState, useEffect, useCallback, useRef } from "react";
import { Link, Navigate, useParams, useSearchParams } from "react-router-dom";
import {
  Activity,
  AlertTriangle,
  Gauge,
  Radio,
  Wifi,
  RefreshCw,
  Factory,
  Database,
  Layers,
  Clock,
  TrendingUp,
  X,
  CheckCircle,
  Thermometer,
  Droplets,
  Pencil,
  Home,
} from "lucide-react";
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip as RechartsTooltip,
  ResponsiveContainer,
  Legend,
  PieChart,
  Pie,
  Cell,
  ScatterChart,
  Scatter,
  ZAxis,
} from "recharts";
import { toast } from "sonner";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useSensorContext } from "@/contexts/sensor-context";
import { api, type IndustryInfo } from "@/lib/api";
import { formatNumber, formatLatency, formatTimestamp } from "@/lib/utils";
import { QUEUING_P50_FIELD } from "@/lib/latency-series";
import { PharmaPlant } from "@/components/twin/PharmaPlant";
import { RealtimeKpiCard } from "@/components/dashboard/realtime-kpi-card";
import { ConnectionStatusBar } from "@/components/dashboard/connection-status-bar";
import { RefreshIntervalSelect, useRefreshInterval } from "@/components/dashboard/refresh-interval-select";
import { LakebaseSinkHealth } from "@/components/dashboard/lakebase-sink-health";
import { GapfillChart } from "@/components/dashboard/lakets/gapfill-chart";
import { RateChart } from "@/components/dashboard/lakets/rate-chart";
import { HistogramChart } from "@/components/dashboard/lakets/histogram-chart";
import { DashboardSection, SectionBadge, SECTION_ACCENTS } from "@/components/dashboard/section";
import {
  StreamingSourceMetrics,
  type StreamingLatencyMode,
  type StreamingSourceKind,
} from "@/components/dashboard/streaming-source-metrics";
import { ZerobusIngestMetrics } from "@/components/dashboard/zerobus-ingest-metrics";
import { LakebasePerfPanel } from "@/components/dashboard/lakebase-perf-panel";
import { HistoricalDbsql } from "@/components/dashboard/historical-dbsql";
import { useTimeseries } from "@/hooks/use-timeseries";
import type {
  Pipeline,
  DashboardDataMode,
  SensorReading,
  TaskStatus,
  TieringConfig,
  CdfStatusResponse,
} from "@/lib/types";

const CHART_COLORS = [
  "#6c5ce7", "#00b894", "#74b9ff", "#e17055", "#fdcb6e",
  "#81ecec", "#a29bfe", "#fab1a0", "#55efc4", "#fd79a8",
];
// Re-enable after Kafka partition scaling makes event-to-insert lag trustworthy.
const SHOW_INSERT_LAG_METRICS = false;

const TOOLTIP_STYLE = {
  backgroundColor: "hsl(var(--card) / 0.9)",
  backdropFilter: "blur(12px)",
  border: "1px solid hsl(var(--border) / 0.5)",
  borderRadius: 8,
  fontSize: 11,
};

/** Independent range options for the Lakebase (hot) section — small windows. */
const LAKEBASE_RANGE_OPTIONS = [
  { minutes: 5, label: "5m" },
  { minutes: 15, label: "15m" },
  { minutes: 60, label: "1h" },
] as const;

/** Independent range options for the Historical / DBSQL (cold) section. */
const DBSQL_RANGE_OPTIONS = [
  { minutes: 1440, label: "24h" },
  { minutes: 10080, label: "7d" },
  { minutes: 43200, label: "30d" },
] as const;

interface RangeSelectorProps {
  options: readonly { minutes: number; label: string }[];
  value: number;
  onChange: (minutes: number) => void;
  accentClass: string;
}

/** Compact, segmented range selector scoped to a single data plane. */
function RangeSelector({ options, value, onChange, accentClass }: RangeSelectorProps) {
  return (
    <div className="inline-flex rounded-md border border-border/50 text-[10px] font-mono">
      {options.map((opt, i) => (
        <button
          key={opt.minutes}
          type="button"
          onClick={() => onChange(opt.minutes)}
          className={`px-2 py-0.5 transition-colors ${i === 0 ? "rounded-l-md" : "border-l border-border/50"} ${
            i === options.length - 1 ? "rounded-r-md" : ""
          } ${value === opt.minutes ? accentClass : "text-muted-foreground hover:text-foreground"}`}
        >
          {opt.label}
        </button>
      ))}
    </div>
  );
}

function extractTiering(pipeline: Pipeline | null | undefined): TieringConfig | null {
  const sink = pipeline?.sink_config as Record<string, unknown> | undefined;
  const t = sink?.tiering as Record<string, unknown> | undefined;
  if (
    !t ||
    typeof t.hot !== "string" ||
    typeof t.drop !== "string" ||
    typeof t.chunk !== "string"
  ) {
    return null;
  }
  return { hot: t.hot, drop: t.drop, chunk: t.chunk };
}

function extractSseMetrics(pipeline: Pipeline | null | undefined): string[] {
  const sink = pipeline?.sink_config as Record<string, unknown> | undefined;
  const m = sink?.sse_metrics;
  if (!Array.isArray(m)) return [];
  return m.filter((x): x is string => typeof x === "string");
}

function SummaryCard({
  label, value, sub, icon: Icon, color, pulse,
}: {
  label: string; value: string; sub: string;
  icon: React.ComponentType<{ className?: string }>; color: string;
  pulse?: boolean;
}) {
  return (
    <Card className={`glass relative overflow-hidden border-border/50${pulse ? " animate-pulse-glow" : ""}`}>
      <CardHeader className="flex flex-row items-center justify-between pb-2">
        <CardTitle className="font-ui text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
          {label}
        </CardTitle>
        <Icon className={`h-4 w-4 ${color}`} />
      </CardHeader>
      <CardContent>
        <div className={`font-mono text-2xl font-bold tabular-nums ${color}`}>{value}</div>
        <p className="font-ui mt-1 text-[10px] text-muted-foreground">{sub}</p>
      </CardContent>
    </Card>
  );
}

function SourceCard({
  name, status, records, latency,
}: {
  name: string; status: string; records: number; latency: number;
}) {
  return (
    <Card className="bg-surface-high border-border/50 cursor-pointer transition-colors hover:border-primary/50">
      <CardContent className="space-y-2 p-4">
        <div className="flex items-center gap-2">
          <span
            className={`h-2 w-2 rounded-full shrink-0 ${
              status === "active" ? "bg-success glow-success"
                : status === "waiting" ? "animate-pulse bg-warning"
                  : status === "error" ? "bg-destructive" : "bg-muted-foreground"
            }`}
          />
          <span className="font-ui text-sm font-semibold truncate">{name}</span>
          <Badge
            variant={status === "active" ? "success" : status === "waiting" ? "warning" : "secondary"}
            className="ml-auto font-mono text-[10px] shrink-0"
          >
            {status}
          </Badge>
        </div>
        {status === "active" && (
          <div className="grid grid-cols-2 gap-1 text-[10px]">
            <span className="font-ui text-muted-foreground">Records/s</span>
            <span className="font-mono text-right tabular-nums">{formatNumber(records)}</span>
            <span className="font-ui text-muted-foreground">Avg Latency</span>
            <span className="font-mono text-right tabular-nums">{formatLatency(latency)}</span>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function SensorDetailInline({ sensor, onClose }: { sensor: SensorReading; onClose: () => void }) {
  return (
    <Card className="glass border-primary/30 glow-primary">
      <CardContent className="p-4">
        <div className="flex items-start justify-between mb-2">
          <div>
            <p className="font-display text-sm font-semibold">{sensor.sensor_name}</p>
            <div className="flex gap-1.5 mt-1">
              <Badge variant="secondary" className="font-mono text-[10px] capitalize">{sensor.sensor_type}</Badge>
              {sensor.fault_active ? (
                <Badge variant="destructive" className="gap-0.5 font-mono text-[10px]">
                  <AlertTriangle className="h-2.5 w-2.5" />FAULT
                </Badge>
              ) : (
                <Badge variant="success" className="gap-0.5 font-mono text-[10px]">
                  <CheckCircle className="h-2.5 w-2.5" />OK
                </Badge>
              )}
            </div>
          </div>
          <Button variant="ghost" size="sm" onClick={onClose} className="h-6 w-6 p-0">
            <X className="h-3.5 w-3.5" />
          </Button>
        </div>
        <div className="text-center my-3">
          <span className="font-mono text-3xl font-bold tabular-nums text-foreground">{formatNumber(sensor.value, 2)}</span>
          <span className="font-ui ml-1.5 text-sm text-muted-foreground">{sensor.unit}</span>
        </div>
        <div className="grid grid-cols-2 gap-1 text-[10px]">
          <span className="font-ui text-muted-foreground">Industry</span>
          <span className="capitalize text-right">{sensor.industry?.replace(/_/g, " ")}</span>
          <span className="font-ui text-muted-foreground">Source</span>
          <span className="text-right">{sensor.source || "---"}</span>
          <span className="font-ui text-muted-foreground">Updated</span>
          <span className="font-mono text-right tabular-nums">{formatTimestamp(sensor.timestamp)}</span>
        </div>
      </CardContent>
    </Card>
  );
}

function detectDataMode(pipeline: Pipeline | null): DashboardDataMode {
  if (!pipeline) return "hybrid";
  const cfg = pipeline.source_config as Record<string, unknown>;
  const rtmSink = cfg.rtm_sink as string | undefined;
  const microbatchSinks = cfg.microbatch_sinks as string[] | undefined;
  const sourceType = pipeline.source_type;

  const hasLakebase = rtmSink === "lakebase" || rtmSink === "both" || microbatchSinks?.includes("lakebase");
  const hasWs = rtmSink === "websocket" || rtmSink === "both" || microbatchSinks?.includes("websocket");
  const hasDelta = sourceType === "zerobus" || microbatchSinks?.includes("delta");

  if (hasDelta && hasWs) return "delta_hybrid";
  if (hasDelta) return "delta";
  if (hasLakebase && hasWs) return "hybrid";
  if (hasLakebase) return "lakebase";

  return "websocket";
}

function getPipelineSourceLabel(pipeline: Pipeline): string {
  const labels: Record<string, string> = {
    kafka: "Kafka", kinesis: "Kinesis", eventhub: "Event Hub",
    zerobus: "Zerobus", websocket: "WebSocket",
  };
  return labels[pipeline.source_type ?? ""] ?? pipeline.source_type ?? "Unknown";
}

/**
 * Determine the streaming-source family for a pipeline. Zerobus ingests
 * straight to Delta; everything else (kafka/kinesis/event_hub/...) runs
 * through Spark Structured Streaming. ``source_config.source`` is the SSS
 * source selector; ``source_type`` is the legacy top-level field.
 */
function getStreamingSourceKind(pipeline: Pipeline | null): StreamingSourceKind {
  if (!pipeline) return "sss";
  const cfg = pipeline.source_config as Record<string, unknown>;
  const sssSource = typeof cfg.source === "string" ? cfg.source : undefined;
  if (pipeline.source_type === "zerobus" || sssSource === "zerobus") return "zerobus";
  return "sss";
}

function metricModeForPipeline(
  pipeline: Pipeline | null,
): StreamingLatencyMode | undefined {
  if (!pipeline || getStreamingSourceKind(pipeline) === "zerobus") return undefined;
  const cfg = pipeline.source_config as Record<string, unknown>;
  const configuredMode = cfg.processing_mode;
  if (pipeline.source_type === "delta" || configuredMode === "microbatch") {
    return "microbatch";
  }
  const rtmSink = cfg.rtm_sink;
  if (rtmSink === "lakebase" || rtmSink === "both" || rtmSink === "websocket") {
    return "realtime";
  }
  return "realtime";
}

function getPipelineSinkLabels(pipeline: Pipeline): string[] {
  const cfg = pipeline.source_config as Record<string, unknown>;
  const rtmSink = cfg.rtm_sink as string | undefined;
  const microbatchSinks = cfg.microbatch_sinks as string[] | undefined;
  const sinks: string[] = [];
  if (rtmSink === "both") { sinks.push("Lakebase", "WebSocket"); }
  else if (rtmSink === "lakebase") { sinks.push("Lakebase"); }
  else if (rtmSink === "websocket") { sinks.push("WebSocket"); }
  if (microbatchSinks) {
    for (const s of microbatchSinks) {
      const label = s === "lakebase" ? "Lakebase" : s === "websocket" ? "WebSocket" : s;
      if (!sinks.includes(label)) sinks.push(label);
    }
  }
  return sinks.length > 0 ? sinks : ["WebSocket"];
}

interface PipelineStats {
  total_records: number;
  records_per_min: number;
  latest_event: string | null;
  oldest_event: string | null;
  freshness_sec: number | null;
}

const DATA_MODE_LABELS: Record<DashboardDataMode, { label: string; icon: React.ComponentType<{ className?: string }>; color: string }> = {
  websocket: { label: "Real-Time (WebSocket)", icon: Radio, color: "text-violet-500" },
  lakebase: { label: "Lakebase (PostgreSQL)", icon: Database, color: "text-blue-500" },
  hybrid: { label: "Hybrid (WS + Lakebase)", icon: Layers, color: "text-green-500" },
  delta: { label: "Delta (Zerobus)", icon: Database, color: "text-orange-500" },
  delta_hybrid: { label: "Hybrid (WS + Delta)", icon: Layers, color: "text-teal-500" },
};

function hasPersistentStore(mode: DashboardDataMode): boolean {
  return mode !== "websocket";
}

function hasRealtimeWs(mode: DashboardDataMode): boolean {
  return mode === "websocket" || mode === "hybrid" || mode === "delta_hybrid";
}

function shouldAutoPoll(mode: DashboardDataMode): boolean {
  return mode === "hybrid" || mode === "delta_hybrid";
}

export function DashboardPage() {
  const [searchParams] = useSearchParams();
  const routeParams = useParams<{ id?: string }>();
  // Support both URL styles: /pipelines/:id/dashboard (preferred) and legacy ?pipeline=
  const pipelineId = routeParams.id ?? searchParams.get("pipeline");

  const { sensors, sources, latencyHistory, clearSensors, wsConnected, totalUpdates } = useSensorContext();

  const [pipeline, setPipeline] = useState<Pipeline | null>(null);
  const [industries, setIndustries] = useState<IndustryInfo[]>([]);
  const [activeIndustry, setActiveIndustry] = useState<string>("pharmaceutical");
  const [selectedIndustry, setSelectedIndustry] = useState<string>("pharmaceutical");
  const [switching, setSwitching] = useState(false);

  const [lbTimeseries, setLbTimeseries] = useState<Record<string, unknown>[]>([]);
  const [lbLatest, setLbLatest] = useState<SensorReading[]>([]);
  const [pipelineStats, setPipelineStats] = useState<PipelineStats | null>(null);
  const [selectedSensor, setSelectedSensor] = useState<SensorReading | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [lastRefreshed, setLastRefreshed] = useState<Date | null>(null);

  // Independent per-section ranges.  Each only affects its own data plane —
  // there is no shared slider and no cross-source compute routing.
  const [lakebaseRangeMin, setLakebaseRangeMin] = useState<number>(LAKEBASE_RANGE_OPTIONS[0].minutes);
  const [dbsqlRangeMin, setDbsqlRangeMin] = useState<number>(DBSQL_RANGE_OPTIONS[0].minutes);

  const [streamingMetrics, setStreamingMetrics] = useState<import("@/lib/types").StreamingMetricsResponse | null>(null);
  const [cdfStatus, setCdfStatus] = useState<CdfStatusResponse | null>(null);

  // DBSQL (cold) channel health for the connection-status pill — lifted out of
  // Section 4 so the top-bar pill reflects the cold-tier query without the page
  // owning a second compute path.
  const [dbsqlState, setDbsqlState] = useState<{
    lastRefresh: number | null;
    error: string | null;
    loading: boolean;
  }>({ lastRefresh: null, error: null, loading: false });
  const refreshDbsqlRef = useRef<() => void>(() => {});

  const [dashTaskStates, setDashTaskStates] = useState<Record<string, TaskStatus>>({});

  // Rolling real-time sensor history from WebSocket
  const [sensorHistory, setSensorHistory] = useState<Array<Record<string, unknown>>>([]);

  const dataMode = useMemo(() => detectDataMode(pipeline), [pipeline]);
  const isPipelineDashboard = !!pipeline;
  const tiering = useMemo(() => extractTiering(pipeline), [pipeline]);
  const sseMetrics = useMemo(() => extractSseMetrics(pipeline), [pipeline]);

  // Streaming-source family (kafka/SSS vs zerobus). Zerobus ingests straight
  // to Delta — it does NOT feed the app's SSE and does NOT use Lakebase. This
  // single flag drives which dashboard sections render and which data planes
  // are polled.
  const sourceKind = useMemo(() => getStreamingSourceKind(pipeline), [pipeline]);
  const isZerobus = sourceKind === "zerobus";

  // Live vs Historical — purely a presentation toggle.  It emphasises which
  // sections matter (live → SSE/Lakebase, historical → DBSQL cold) but never
  // changes which compute plane a chart queries.  Each plane is fixed.
  const [displayMode, setDisplayMode] = useState<"live" | "historical">(() => {
    try {
      const stored = localStorage.getItem(`dashboard.displayMode.${pipelineId}`);
      if (stored === "live" || stored === "historical") return stored;
    } catch { /* ignore */ }
    return "live";
  });
  useEffect(() => {
    try {
      if (pipelineId) localStorage.setItem(`dashboard.displayMode.${pipelineId}`, displayMode);
    } catch { /* ignore */ }
  }, [displayMode, pipelineId]);

  // Grafana-style refresh interval (seconds).  0 = paused.  Persisted globally.
  const [refreshSec, setRefreshSec] = useRefreshInterval(30);

  // Page-level Lakebase (hot) range query.  Fixed to the Lakebase plane — it
  // feeds the warm-tier readouts and the connection-status Lakebase pill, and
  // is governed by the Lakebase range selector only.  Never swaps to DBSQL.
  const {
    rows: timeseriesRows,
    refresh: refreshTimeseries,
    loading: tsLoading,
    error: tsError,
    lastRefreshAt: tsLastRefresh,
  } = useTimeseries({
    pipelineId: pipelineId ?? undefined,
    rangeMinutes: lakebaseRangeMin,
    industry: pipeline?.industry ?? null,
    source: "lakebase",
    pollSeconds: refreshSec,
    // Zerobus does not use Lakebase — never poll the Lakebase hot tier for it.
    enabled: hasPersistentStore(dataMode) && !isZerobus,
  });

  useEffect(() => {
    setLbTimeseries(timeseriesRows);
  }, [timeseriesRows]);

  useEffect(() => {
    if (!pipelineId) return;
    api.getPipeline(pipelineId).then(setPipeline).catch(() => {});
  }, [pipelineId]);

  useEffect(() => {
    if (isPipelineDashboard) {
      const ind = pipeline?.industry ?? "pharmaceutical";
      setActiveIndustry(ind);
      setSelectedIndustry(ind);
      return;
    }
    Promise.all([api.getIndustries(), api.getSimulatorStatus()])
      .then(([indList, status]) => {
        setIndustries(indList);
        const current = status.industries[0] || "pharmaceutical";
        setActiveIndustry(current);
        setSelectedIndustry(current);
      })
      .catch((err) => {
        toast.error(`Failed to load dashboard: ${err instanceof Error ? err.message : "unknown error"}`);
      });
  }, [pipeline, isPipelineDashboard]);

  // Non-range fetches: latest snapshot (1 min), pipeline stats (5 min),
  // and streaming metrics.  The main range query is owned by useTimeseries.
  const fetchLakebaseData = useCallback(async () => {
    const industry = pipeline?.industry ?? null;

    const [latestResult, statsResult] = await Promise.allSettled([
      api.getTimeseries({ minutes: 1, industry, pipelineId: pipelineId ?? undefined }),
      api.getPipelineStats({ minutes: 5, industry, pipelineId: pipelineId ?? undefined }),
    ]);
    if (latestResult.status === "fulfilled") {
      setLbLatest((latestResult.value?.rows ?? []) as unknown as SensorReading[]);
    }
    if (statsResult.status === "fulfilled") {
      setPipelineStats(statsResult.value);
    }

    if (pipelineId && pipeline?.resources?.uc_catalog) {
      try {
        const metrics = await api.getStreamingMetrics(pipelineId);
        setStreamingMetrics(metrics);
      } catch { /* metrics may not be available */ }
    }

    setLastRefreshed(new Date());
  }, [pipeline?.industry, pipeline?.resources?.uc_catalog, pipelineId]);

  useEffect(() => {
    // Zerobus uses neither Lakebase nor the workspace-scoped stats/sources
    // path; its ingest metrics come from /zerobus-metrics instead.
    if (!hasPersistentStore(dataMode) || isZerobus) return;
    fetchLakebaseData();
    if (!shouldAutoPoll(dataMode)) return;
    if (refreshSec <= 0) return;
    const interval = setInterval(fetchLakebaseData, refreshSec * 1000);
    return () => clearInterval(interval);
  }, [dataMode, isZerobus, fetchLakebaseData, refreshSec]);

  useEffect(() => {
    const jobId = pipeline?.resources?.job_id;
    if (!jobId) return;
    const poll = async () => {
      try {
        const status = await api.getJobStatus(Number(jobId));
        if (status.tasks) setDashTaskStates(status.tasks);
      } catch { /* ignore */ }
    };
    poll();
    const interval = setInterval(poll, 5_000);
    return () => clearInterval(interval);
  }, [pipeline?.resources?.job_id]);

  // CDF status — lifted here so the top-bar pill and Section 4 share one
  // probe. Polled on the refresh interval; paused when the selector is Off.
  useEffect(() => {
    if (!pipelineId) return;
    let cancelled = false;
    const poll = async () => {
      try {
        const data = await api.getCdfStatus(pipelineId);
        if (!cancelled) setCdfStatus(data);
      } catch { /* fail closed — gate stays visible */ }
    };
    poll();
    if (refreshSec <= 0) return () => { cancelled = true; };
    const interval = setInterval(poll, refreshSec * 1000);
    return () => { cancelled = true; clearInterval(interval); };
  }, [pipelineId, refreshSec]);

  const handleSwitch = async () => {
    if (selectedIndustry === activeIndustry || switching) return;
    setSwitching(true);
    clearSensors();
    try {
      await api.restartSimulator([selectedIndustry]);
      setActiveIndustry(selectedIndustry);
      toast.success(`Switched to ${selectedIndustry.replace(/_/g, " ")} industry`);
    } catch (err) {
      toast.error(`Failed to switch: ${err instanceof Error ? err.message : "unknown error"}`);
    }
    setSwitching(false);
  };

  const effectiveSensors = useMemo(() => {
    if (dataMode === "lakebase" || dataMode === "delta") return lbLatest;
    if (dataMode === "websocket") return Object.values(sensors);
    const wsMap = new Map(Object.values(sensors).map((s) => [s.sensor_name, s]));
    const merged = new Map(lbLatest.map((s) => [s.sensor_name, s]));
    for (const [name, sensor] of wsMap) {
      merged.set(name, sensor);
    }
    return Array.from(merged.values());
  }, [dataMode, sensors, lbLatest]);

  const sensorList = effectiveSensors;
  const faultCount = useMemo(() => sensorList.filter((s) => s.fault_active).length, [sensorList]);
  const activeSources = useMemo(() => Object.values(sources).filter((s) => s.status === "active").length, [sources]);
  const avgLatency = useMemo(() => {
    if (latencyHistory.length === 0) return 0;
    const recent = latencyHistory.slice(-20);
    return recent.reduce((sum, l) => sum + l.e2e_ms, 0) / recent.length;
  }, [latencyHistory]);

  const sensorsByType = useMemo(() => {
    const grouped: Record<string, typeof sensorList> = {};
    for (const s of sensorList) {
      if (!grouped[s.sensor_type]) grouped[s.sensor_type] = [];
      grouped[s.sensor_type].push(s);
    }
    return grouped;
  }, [sensorList]);

  // For Zerobus there is no SSE stream; the 3D twin is fed from the latest
  // Delta rows instead.  Polled on the refresh interval, gated on isZerobus.
  const [zerobusLatest, setZerobusLatest] = useState<SensorReading[]>([]);
  useEffect(() => {
    if (!isZerobus || !pipelineId) return;
    let cancelled = false;
    const poll = async () => {
      try {
        const data = await api.getDbsqlTimeseries(pipelineId, 5);
        if (cancelled) return;
        setZerobusLatest((data.rows ?? []) as unknown as SensorReading[]);
      } catch { /* Delta may not be queryable yet */ }
    };
    poll();
    if (refreshSec <= 0) return () => { cancelled = true; };
    const id = setInterval(poll, refreshSec * 1000);
    return () => { cancelled = true; clearInterval(id); };
  }, [isZerobus, pipelineId, refreshSec]);

  // Twin readings for Zerobus — derived from the latest Delta rows (first
  // sensor of each type), mirroring the SSE-driven ``pharmaReadings`` shape.
  const zerobusReadings = useMemo(() => {
    const first = (type: string) =>
      zerobusLatest.find((s) => s.sensor_type === type)?.value;
    const faults = zerobusLatest.filter((s) => s.fault_active).length;
    return {
      reactorTemp: first("temperature"),
      pressure: first("pressure"),
      wfiFlow: first("flow"),
      yield: Math.max(80, 99.2 - faults * 2.5),
      faults,
    };
  }, [zerobusLatest]);

  // Map live pharmaceutical sensors → PharmaPlant 3D twin readings. The twin
  // is driven straight from the SSE sensor stream (first sensor of each type).
  const pharmaReadings = useMemo(() => {
    const live = Object.values(sensors);
    const first = (type: string) => live.find((s) => s.sensor_type === type)?.value;
    const reactorTemp = first("temperature");
    const pressure = first("pressure");
    const wfiFlow = first("flow");
    const faults = live.filter((s) => s.fault_active).length;
    // Yield has no direct sensor — derive a stable placeholder that dips with
    // active faults so it reads as a plausible domain KPI.
    const yieldPct = Math.max(80, 99.2 - faults * 2.5);
    return {
      reactorTemp,
      pressure,
      wfiFlow,
      yield: yieldPct,
      faults,
    };
  }, [sensors]);

  // Accumulate real-time sensor values for live time-series chart (throttled to 1Hz)
  const lastHistoryUpdate = useRef(0);
  useEffect(() => {
    const sensorValues = Object.values(sensors);
    if (sensorValues.length === 0) return;
    const now = Date.now();
    if (now - lastHistoryUpdate.current < 1000) return; // throttle to 1Hz
    lastHistoryUpdate.current = now;

    const timeStr = new Date(now).toLocaleTimeString("en-US", { hour12: false, minute: "2-digit", second: "2-digit" });
    const point: Record<string, unknown> = { time: timeStr };
    // Pick first sensor of each type (up to 6 types) for visible value changes
    const seen = new Set<string>();
    for (const s of sensorValues) {
      if (seen.size >= 6) break;
      if (!seen.has(s.sensor_type)) {
        seen.add(s.sensor_type);
        point[s.sensor_name] = Math.round(s.value * 100) / 100;
      }
    }
    setSensorHistory((prev) => [...prev.slice(-120), point]);
  }, [sensors]);

  const sensorDistributionData = useMemo(() => {
    return Object.entries(sensorsByType).map(([type, sensors]) => ({
      name: type,
      value: sensors.length,
    }));
  }, [sensorsByType]);

  // Section 1 is always-live SSE — the chart only shows pushed sensor values.
  const timeseriesChartData = useMemo(() => {
    return sensorHistory.slice(-60);
  }, [sensorHistory]);

  const timeseriesSensorTypes = useMemo(() => {
    const types = new Set<string>();
    for (const point of timeseriesChartData) {
      for (const key of Object.keys(point)) {
        if (key !== "time") types.add(key);
      }
    }
    return Array.from(types);
  }, [timeseriesChartData]);

  const throughputLatencyData = useMemo(() => {
    if (hasPersistentStore(dataMode) && lbTimeseries.length > 0) {
      const byMinute: Record<string, { count: number; time: string }> = {};
      for (const r of lbTimeseries) {
        const ts = String(r.time || "");
        const minute = ts.slice(0, 16);
        if (!byMinute[minute]) byMinute[minute] = { count: 0, time: minute.slice(11) };
        byMinute[minute].count += 1;
      }
      return Object.values(byMinute).slice(-30);
    }
    return latencyHistory.map((l) => ({
      time: new Date(l.time).toLocaleTimeString("en-US", { hour12: false, minute: "2-digit", second: "2-digit" }),
      inputRPS: 1,
      e2e_ms: l.e2e_ms,
    }));
  }, [dataMode, latencyHistory, lbTimeseries]);

  const faultTimelineData = useMemo(() => {
    return sensorList
      .filter((s) => s.fault_active)
      .map((s) => ({
        time: s.timestamp ? new Date(s.timestamp).getTime() : Date.now(),
        sensor: s.sensor_name,
        value: 1,
      }));
  }, [sensorList]);

  const sourceEntries = useMemo(() => {
    // Derive Lakebase sink liveness from RECENT writes (records_per_min > 0)
    // rather than cumulative total, and from stats freshness when available.
    // total_records can stay 0 for a while after a fresh provision, which
    // used to paint an actively-writing pipeline as "waiting".
    const hasRecentActivity =
      !!pipelineStats && (pipelineStats.records_per_min > 0 || pipelineStats.total_records > 0);

    if (isPipelineDashboard && pipeline) {
      const sourceLabel = getPipelineSourceLabel(pipeline);
      const sinkLabels = getPipelineSinkLabels(pipeline);
      const entries: [string, Record<string, unknown>][] = [
        ["source", {
          display_name: sourceLabel,
          status: hasRecentActivity ? "active" : "waiting",
          records_per_sec: pipelineStats ? Math.round(pipelineStats.records_per_min / 60 * 10) / 10 : 0,
          avg_latency_ms: 0,
        }],
      ];
      for (const sink of sinkLabels) {
        const key = sink.toLowerCase().replace(/\s/g, "_");
        const sinkSource = sources[key];
        entries.push([key, {
          display_name: `${sink} (Sink)`,
          status: sinkSource?.status || (hasRecentActivity ? "active" : "waiting"),
          records_per_sec: sinkSource?.records_per_sec || (
            key === "lakebase" && pipelineStats
              ? Math.round(pipelineStats.records_per_min / 60 * 10) / 10
              : 0
          ),
          avg_latency_ms: sinkSource?.avg_latency_ms || 0,
        }]);
      }
      return entries;
    }
    // Non-pipeline view: reflect real backend state only.  The old "websocket
    // active / lakebase inactive" fallback was from the pre-refactor in-app
    // sink era and is now misleading — every data source is per-pipeline.
    return Object.entries(sources);
  }, [isPipelineDashboard, pipeline, sources, pipelineStats]);

  const needsSwitch = selectedIndustry !== activeIndustry;
  const modeInfo = DATA_MODE_LABELS[dataMode];

  const handleRefresh = useCallback(async () => {
    setRefreshing(true);
    try {
      await Promise.all([fetchLakebaseData(), refreshTimeseries()]);
    } finally {
      setRefreshing(false);
    }
  }, [fetchLakebaseData, refreshTimeseries]);

  // Lift DBSQL (cold) channel health out of Section 4 for the top-bar pill.
  const handleDbsqlState = useCallback(
    (next: { lastRefresh: number | null; error: string | null; loading: boolean }) => {
      setDbsqlState(next);
    },
    [],
  );
  const registerDbsqlRefresh = useCallback((fn: () => void) => {
    refreshDbsqlRef.current = fn;
  }, []);

  const liveSelectedSensor = useMemo(() => {
    if (!selectedSensor) return null;
    return effectiveSensors.find((s) => s.sensor_name === selectedSensor.sensor_name) ?? selectedSensor;
  }, [selectedSensor, effectiveSensors]);

  const activeSensorCount = sensorList.length;

  const streamingMetricsRows = useMemo(() => {
    if (!streamingMetrics?.rows) return [];
    return streamingMetrics.rows.slice().reverse().slice(-20);
  }, [streamingMetrics]);

  const streamingThroughputData = useMemo(() => {
    return streamingMetricsRows.map((row) => ({
      batch: row.batch_id,
      input_rps: Number(row.input_rows_per_second) || 0,
      processed_rps: Number(row.processed_rows_per_second) || 0,
    }));
  }, [streamingMetricsRows]);

  const streamingBatchDurationData = useMemo(() => {
    return streamingMetricsRows.map((row) => ({
      batch: row.batch_id,
      duration_ms: Number(row.batch_duration_ms) || 0,
    }));
  }, [streamingMetricsRows]);

  const streamingLatencyData = useMemo(() => {
    return streamingMetricsRows.map((row) => ({
      batch: row.batch_id,
      e2e_p50_ms: Number(row.e2e_p50_ms) || 0,
      processing_p50_ms: Number(row.processing_p50_ms) || 0,
      queuing_p50_ms: Number(row.queuing_p50_ms) || 0,
    }));
  }, [streamingMetricsRows]);

  const observedMetricsCards = useMemo(() => {
    if (!streamingMetricsRows.length) return [];
    const last = streamingMetricsRows[streamingMetricsRows.length - 1];
    if (!last.observed_metrics) return [];
    try {
      const parsed = JSON.parse(last.observed_metrics) as Record<string, unknown>;
      return Object.entries(parsed).map(([key, val]) => ({ key, value: String(val) }));
    } catch {
      return [];
    }
  }, [streamingMetricsRows]);

  const freshnesDisplay = useMemo(() => {
    if (!pipelineStats?.freshness_sec) return "---";
    if (pipelineStats.freshness_sec < 60) return `${Math.round(pipelineStats.freshness_sec)}s`;
    return `${Math.round(pipelineStats.freshness_sec / 60)}m`;
  }, [pipelineStats]);

  // Dashboards are per-pipeline only.  If the route didn't carry a pipeline
  // id, bounce to Home — hooks above have run; this guard is safe.
  if (!pipelineId) {
    return <Navigate to="/" replace />;
  }

  return (
    <div className="flex flex-col h-[calc(100vh-4rem)] bg-background">
      {/* Header bar */}
      <div className="flex items-center gap-6 px-4 py-2 border-b border-border/50 bg-surface-low shrink-0">
        <div>
          <p className="font-display text-sm font-bold tracking-[0.3em] uppercase text-foreground/80">
            NEURAL OBSERVATORY
          </p>
        </div>
        <nav className="flex items-center gap-1">
          {(["SYSTEMS", "TELEMETRY", "ASSETS", "LOGS"] as const).map((tab) => (
            <button
              key={tab}
              className={`font-ui text-xs tracking-widest uppercase px-3 py-1.5 rounded transition-colors ${
                tab === "TELEMETRY"
                  ? "text-primary border-b border-primary bg-primary/5"
                  : "text-muted-foreground hover:text-foreground"
              }`}
            >
              {tab}
            </button>
          ))}
        </nav>

        <div className="ml-auto flex items-center gap-3">
          {/* Pipeline info */}
          <div className="flex items-center gap-2">
            <modeInfo.icon className={`h-4 w-4 shrink-0 ${modeInfo.color}`} />
            <div>
              <p className="font-display text-xs font-semibold">{pipeline ? pipeline.name : "Digital Twin"}</p>
              <p className="font-ui text-[10px] text-muted-foreground capitalize">
                {activeIndustry.replace(/_/g, " ") || "loading..."} &middot; {modeInfo.label}
              </p>
            </div>
          </div>

          {/* Sensor count + fault badge */}
          <div className="flex items-center gap-2">
            <div className="hud-label flex items-center gap-1.5">
              <span className={`h-1.5 w-1.5 rounded-full bg-success ${activeSensorCount > 0 ? "animate-pulse" : ""}`} />
              <span className="font-mono font-semibold text-success">{activeSensorCount}</span>
              <span className="text-muted-foreground">sensors</span>
            </div>
            {faultCount > 0 && (
              <div className="hud-label flex items-center gap-1.5 border-destructive/40">
                <span className="h-1.5 w-1.5 rounded-full bg-destructive animate-pulse" />
                <span className="font-mono font-semibold text-destructive">{faultCount}</span>
                <span className="text-muted-foreground">fault{faultCount !== 1 ? "s" : ""}</span>
              </div>
            )}
          </div>

          {/* Live / Historical toggle — only meaningful when both the live
              (SSE/Lakebase) and historical (DBSQL) planes exist.  Hidden for
              Zerobus, which has a single Delta plane. */}
          {!isZerobus && (
          <div className="inline-flex rounded-md border border-border/50 text-xs">
            <button
              className={`px-2.5 py-1 rounded-l-md transition-colors ${
                displayMode === "live"
                  ? "bg-primary/15 text-primary font-medium"
                  : "text-muted-foreground hover:text-foreground"
              }`}
              onClick={() => setDisplayMode("live")}
              title="Live: emphasise the SSE & Lakebase (hot) sections — does not change which compute a chart queries"
            >
              Live
            </button>
            <button
              className={`px-2.5 py-1 rounded-r-md border-l border-border/50 transition-colors ${
                displayMode === "historical"
                  ? "bg-info/15 text-info font-medium"
                  : "text-muted-foreground hover:text-foreground"
              }`}
              onClick={() => setDisplayMode("historical")}
              title="Historical: emphasise the DBSQL (cold) section — does not change which compute a chart queries"
            >
              Historical
            </button>
          </div>
          )}

          {/* Grafana-style refresh interval + manual refresh */}
          <RefreshIntervalSelect
            value={refreshSec}
            onChange={setRefreshSec}
            onRefreshNow={() => {
              refreshTimeseries();
              if (hasPersistentStore(dataMode)) fetchLakebaseData();
            }}
          />

          {/* Connection status pills.  For Kafka/SSS: SSE / Lakebase / DBSQL.
              For Zerobus the SSE & Lakebase planes don't exist, so only the
              DBSQL (Delta) pill is shown — and it's always active. */}
          <ConnectionStatusBar
            sseConnected={wsConnected}
            lakebaseLastRefresh={tsLastRefresh}
            lakebaseError={tsError}
            lakebaseLoading={tsLoading}
            dbsqlLastRefresh={dbsqlState.lastRefresh}
            dbsqlError={dbsqlState.error}
            dbsqlLoading={dbsqlState.loading}
            dbsqlActive={isZerobus || !!cdfStatus?.streaming}
            onRefreshLakebase={refreshTimeseries}
            onRefreshDbsql={refreshDbsqlRef.current}
            showSse={!isZerobus}
            showLakebase={!isZerobus}
          />

          {/* CDF (Lakehouse Sync) status pill — drives Section 4 gating for
              Kafka/SSS.  Not shown for Zerobus, where Delta is the primary
              store (no CDF involved). */}
          {!isZerobus && (
            <SectionBadge
              color={cdfStatus?.streaming ? SECTION_ACCENTS.dbsql : SECTION_ACCENTS.cdf}
              label={
                cdfStatus?.streaming
                  ? "CDF · STREAMING"
                  : cdfStatus?.status === "SNAPSHOTTING"
                    ? "CDF · SNAPSHOTTING"
                    : "CDF · OFF"
              }
            />
          )}

          {hasPersistentStore(dataMode) && (
            <Button
              variant="ghost"
              size="sm"
              className="h-7 w-7 p-0"
              disabled={refreshing}
              onClick={handleRefresh}
              title="Refresh data"
            >
              <RefreshCw className={`h-3.5 w-3.5 ${refreshing ? "animate-spin" : ""}`} />
            </Button>
          )}

          {/* Navigate back to Home */}
          <Button
            asChild
            variant="ghost"
            size="sm"
            className="gap-1.5 h-7 font-ui text-xs"
            title="Back to pipelines"
          >
            <Link to="/">
              <Home className="h-3.5 w-3.5" />
              Home
            </Link>
          </Button>

          {/* Edit pipeline — return to wizard (step 3: Deploy & Monitor) */}
          <Button
            asChild
            variant="outline"
            size="sm"
            className="gap-1.5 h-7 font-ui text-xs border-border/50"
            title="Edit pipeline configuration"
          >
            <Link to={`/pipelines/new?id=${pipelineId}&step=3`}>
              <Pencil className="h-3.5 w-3.5" />
              Edit
            </Link>
          </Button>
        </div>
      </div>

      {/* Main content area — 4 color-coded sections */}
      <div className="flex flex-1 min-h-0">
        {/* Dashboard content */}
        <div className="overflow-y-auto p-4 space-y-5 w-full">
          {/* Pipeline header card */}
          {pipeline && (
            <Card className="glass border-primary/30 glow-primary">
              <CardContent className="space-y-2 p-3">
                <div className="flex flex-wrap items-center gap-3">
                  <div>
                    <p className="font-display text-sm font-semibold">{pipeline.name}</p>
                    <p className="font-ui text-[10px] text-muted-foreground capitalize">
                      {pipeline.industry?.replace(/_/g, " ")} &middot; {modeInfo.label}
                    </p>
                  </div>
                  <Button
                    variant="outline"
                    size="sm"
                    className="gap-1.5 h-7 font-ui text-xs border-border/50 ml-auto"
                    disabled={refreshing}
                    onClick={handleRefresh}
                  >
                    <RefreshCw className={`h-3 w-3 ${refreshing ? "animate-spin" : ""}`} />
                    {refreshing ? "..." : "Refresh"}
                  </Button>
                </div>

                {/* Task health indicators */}
                {Object.keys(dashTaskStates).length > 0 && (
                  <div className="flex items-center gap-2 flex-wrap">
                    {Object.entries(dashTaskStates).map(([key, task]) => {
                      const label = key === "sss_consumer" ? "Consumer" : key === "data_producer" ? "Producer" : key;
                      const isOk = task.state === "RUNNING";
                      const isBad = task.state === "TERMINATED" || task.state === "INTERNAL_ERROR";
                      return (
                        <div key={key} className="flex items-center gap-1">
                          <span className={`h-1.5 w-1.5 rounded-full ${isOk ? "bg-success" : isBad ? "bg-destructive animate-pulse" : "bg-warning animate-pulse"}`} />
                          <span className={`text-[9px] font-mono ${isBad ? "text-destructive" : "text-muted-foreground"}`}>
                            {label}: {task.state}
                          </span>
                        </div>
                      );
                    })}
                  </div>
                )}
                {Object.values(dashTaskStates).some((t) => t.state === "TERMINATED" || t.state === "INTERNAL_ERROR") && (
                  <div className="rounded bg-destructive/10 border border-destructive/30 px-2 py-1">
                    <p className="text-[10px] text-destructive font-medium">
                      Pipeline task failure detected --- check job details
                    </p>
                  </div>
                )}
                {lastRefreshed && (
                  <p className="text-[9px] text-muted-foreground font-mono">
                    Last refreshed: {lastRefreshed.toLocaleTimeString()}
                  </p>
                )}
              </CardContent>
            </Card>
          )}

          {/* Industry selector (global only) */}
          {!isPipelineDashboard && (
            <Card className="glass border-border/50">
              <CardContent className="flex flex-wrap items-center gap-3 p-3">
                <Factory className="h-4 w-4 shrink-0 text-primary" />
                <Select value={selectedIndustry} onValueChange={setSelectedIndustry}>
                  <SelectTrigger className="glass h-8 w-44 font-ui text-xs border-border/50" aria-label="Select industry">
                    <SelectValue placeholder="Select industry" />
                  </SelectTrigger>
                  <SelectContent className="glass border-border/50">
                    {industries.map((ind) => (
                      <SelectItem key={ind.id} value={ind.id} className="font-ui text-xs">
                        {ind.label} ({ind.sensor_count})
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <Button size="sm" disabled={!needsSwitch || switching} onClick={handleSwitch} className="gap-1.5 h-8 font-ui text-xs">
                  <RefreshCw className={`h-3.5 w-3.5 ${switching ? "animate-spin" : ""}`} />
                  {switching ? "..." : "Switch"}
                </Button>
              </CardContent>
            </Card>
          )}

          {/* Selected sensor detail */}
          {liveSelectedSensor && (
            <SensorDetailInline
              sensor={liveSelectedSensor}
              onClose={() => setSelectedSensor(null)}
            />
          )}

          {/* ================= SECTION 1 — Live · SSE (green) =================
              Hidden for Zerobus pipelines: Zerobus ingests directly to Delta and
              does not feed the app's SSE stream. */}
          {!isZerobus && (
          <DashboardSection
            index={1}
            title="Live · SSE"
            subtitle="3D digital twin + live sensor stream, pushed over Server-Sent Events"
            accent="sse"
            emphasis={displayMode === "historical" ? "dimmed" : "normal"}
            badge={<SectionBadge color={SECTION_ACCENTS.sse} label={wsConnected ? "● LIVE · SSE" : "● DISCONNECTED"} />}
          >
          {/* 3D twin + live time-series side by side */}
          <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
            <PharmaPlant
              readings={pharmaReadings}
              sourceLabel={wsConnected ? "● LIVE · SSE" : "● SSE OFFLINE"}
            />
            <div className="space-y-3">
              {/* Domain KPI strip */}
              <div className="grid grid-cols-2 sm:grid-cols-3 gap-2">
                <SummaryCard label="Reactor Temp" value={pharmaReadings.reactorTemp != null ? formatNumber(pharmaReadings.reactorTemp, 1) : "---"} sub="temperature" icon={Thermometer} color="text-success" />
                <SummaryCard label="Pressure" value={pharmaReadings.pressure != null ? formatNumber(pharmaReadings.pressure, 1) : "---"} sub="pressure" icon={Gauge} color="text-success" />
                <SummaryCard label="WFI Flow" value={pharmaReadings.wfiFlow != null ? formatNumber(pharmaReadings.wfiFlow, 1) : "---"} sub="flow" icon={Droplets} color="text-success" />
                <SummaryCard label="Yield" value={`${formatNumber(pharmaReadings.yield, 1)}%`} sub="derived" icon={TrendingUp} color="text-success" />
                <SummaryCard label="Active Faults" value={String(pharmaReadings.faults)} sub={pharmaReadings.faults === 0 ? "all nominal" : "in fault"} icon={AlertTriangle} color={pharmaReadings.faults > 0 ? "text-destructive" : "text-success"} pulse={pharmaReadings.faults > 0} />
                <SummaryCard label="Sensors" value={String(sensorList.length || "---")} sub={`${Object.keys(sensorsByType).length} types`} icon={Activity} color="text-success" />
              </div>
            </div>
          </div>

          {/* Live status bar — always SSE/push */}
          <div className="flex items-center gap-4 rounded-md glass border border-border/50 px-4 py-2">
            <div className="flex items-center gap-2">
              <span className={`h-2.5 w-2.5 rounded-full ${wsConnected ? "bg-success animate-pulse" : "bg-destructive"}`} />
              <span className="font-mono text-xs font-semibold">{wsConnected ? "LIVE" : "DISCONNECTED"}</span>
            </div>
            <div className="h-4 w-px bg-border/50" />
            <div className="flex items-center gap-1.5">
              <span className="text-[10px] text-muted-foreground">Updates:</span>
              <span className="font-mono text-xs font-semibold tabular-nums text-primary">{totalUpdates.toLocaleString()}</span>
            </div>
            <div className="h-4 w-px bg-border/50" />
            <div className="flex items-center gap-1.5">
              <span className="text-[10px] text-muted-foreground">Sensors:</span>
              <span className="font-mono text-xs font-semibold">{Object.keys(sensors).length}</span>
            </div>
            <div className="h-4 w-px bg-border/50" />
            <div className="flex items-center gap-1.5">
              <span className="text-[10px] text-muted-foreground">Chart points:</span>
              <span className="font-mono text-xs">{sensorHistory.length}</span>
            </div>
          </div>

          {/* Realtime KPI row — driven by SSE.  Only rendered when the
              pipeline's sink_config.sse_metrics is non-empty. */}
          {sseMetrics.length > 0 && (
            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
              {sseMetrics.slice(0, 2).map((metric) => (
                <RealtimeKpiCard
                  key={metric}
                  metric={metric}
                  label={metric.replace(/_/g, " ")}
                />
              ))}
            </div>
          )}

          {/* Sensor Time-Series Chart (full-width) */}
          <Card className="glass border-border/50">
            <CardHeader className="pb-2">
              <CardTitle className="flex items-center gap-2 text-sm font-display">
                <Activity className="h-4 w-4 text-primary" />
                Sensor Time-Series
                <span
                  className="ml-auto inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 font-mono text-[10px] font-medium ring-1 bg-success/15 text-success ring-success/40"
                  title="Server-Sent Events — pushed directly from the Spark Streaming job"
                >
                  <span className="h-1.5 w-1.5 rounded-full bg-success" />
                  SSE
                </span>
              </CardTitle>
            </CardHeader>
            <CardContent>
              {timeseriesChartData.length > 0 ? (
                <ResponsiveContainer width="100%" height={250}>
                  <LineChart data={timeseriesChartData}>
                    <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border) / 0.4)" />
                    <XAxis dataKey="time" stroke="hsl(var(--muted-foreground))" fontSize={9} tickLine={false} />
                    <YAxis stroke="hsl(var(--muted-foreground))" fontSize={9} tickLine={false} />
                    <RechartsTooltip contentStyle={TOOLTIP_STYLE} />
                    {timeseriesSensorTypes.map((type, i) => (
                      <Line
                        key={type}
                        type="monotone"
                        dataKey={type}
                        stroke={CHART_COLORS[i % CHART_COLORS.length]}
                        strokeWidth={2}
                        dot={false}
                        name={type}
                        isAnimationActive={true}
                        animationDuration={300}
                      />
                    ))}
                    <Legend wrapperStyle={{ fontSize: 10 }} />
                  </LineChart>
                </ResponsiveContainer>
              ) : (
                <div className="flex h-[250px] items-center justify-center font-ui text-xs text-muted-foreground">
                  Waiting for time-series data...
                </div>
              )}
            </CardContent>
          </Card>

          {/* Real-time sensor ticker */}
          {sensorList.length > 0 && (
            <div className="grid grid-cols-3 md:grid-cols-6 gap-2">
              {sensorList.slice(0, 6).map((s) => (
                <div
                  key={s.sensor_name}
                  className="rounded-md glass border border-border/50 px-3 py-2 transition-all duration-300 hover:border-primary/50"
                >
                  <p className="text-[9px] text-muted-foreground truncate">{s.sensor_name}</p>
                  <p className="font-mono text-lg font-bold tabular-nums text-foreground">
                    {formatNumber(s.value, 1)}
                  </p>
                  <p className="text-[9px] text-muted-foreground">{s.unit}</p>
                  {s.fault_active && <span className="text-[8px] text-destructive font-semibold">FAULT</span>}
                </div>
              ))}
            </div>
          )}
          </DashboardSection>
          )}

          {/* ============ SECTION 2 — Streaming / Ingest Metrics (source-aware) ============
              For Zerobus this is the "Zerobus · Delta Ingest" section (pink), driven
              by /zerobus-metrics and hosting the Delta-fed 3D twin.  For Kafka/SSS it
              is the Spark Structured Streaming source-metrics section (blue). */}
          <DashboardSection
            index={2}
            title={isZerobus ? "Zerobus · Delta Ingest" : "Streaming Source Metrics"}
            subtitle={pipeline ? `${getPipelineSourceLabel(pipeline)} → ${isZerobus ? "Delta table (direct gRPC ingest)" : "Spark Structured Streaming"}` : "Source-aware ingest throughput & latency"}
            accent={isZerobus ? "zerobus" : "lakebase"}
            badge={<SectionBadge color={isZerobus ? SECTION_ACCENTS.zerobus : SECTION_ACCENTS.lakebase} label={isZerobus ? "ZEROBUS · DELTA" : "KAFKA · SSS"} />}
          >
          {isZerobus ? (
            /* Zerobus · Delta Ingest — direct gRPC ingest to a Delta table.
               No SSE, no Lakebase: the twin is fed from the latest Delta rows. */
            <div className="space-y-4">
              <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
                <PharmaPlant
                  readings={zerobusReadings}
                  sourceLabel="● INGESTING · Zerobus"
                />
                <div className="grid grid-cols-2 sm:grid-cols-3 gap-2 content-start">
                  <SummaryCard label="Reactor Temp" value={zerobusReadings.reactorTemp != null ? formatNumber(zerobusReadings.reactorTemp, 1) : "---"} sub="temperature" icon={Thermometer} color="text-pink-500" />
                  <SummaryCard label="Pressure" value={zerobusReadings.pressure != null ? formatNumber(zerobusReadings.pressure, 1) : "---"} sub="pressure" icon={Gauge} color="text-pink-500" />
                  <SummaryCard label="WFI Flow" value={zerobusReadings.wfiFlow != null ? formatNumber(zerobusReadings.wfiFlow, 1) : "---"} sub="flow" icon={Droplets} color="text-pink-500" />
                  <SummaryCard label="Yield" value={`${formatNumber(zerobusReadings.yield, 1)}%`} sub="derived" icon={TrendingUp} color="text-pink-500" />
                  <SummaryCard label="Active Faults" value={String(zerobusReadings.faults)} sub={zerobusReadings.faults === 0 ? "all nominal" : "in fault"} icon={AlertTriangle} color={zerobusReadings.faults > 0 ? "text-destructive" : "text-pink-500"} pulse={zerobusReadings.faults > 0} />
                  <SummaryCard label="Sensors (Delta)" value={String(zerobusLatest.length || "---")} sub="latest rows" icon={Activity} color="text-pink-500" />
                </div>
              </div>

              <ZerobusIngestMetrics
                pipelineId={pipelineId}
                minutes={dbsqlRangeMin}
                refreshSec={refreshSec}
              />
            </div>
          ) : (
          <>
          {/* Pipeline summary cards */}
          <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
            <SummaryCard
              label="Active Sensors"
              value={String(sensorList.length || "---")}
              sub={`${Object.keys(sensorsByType).length} types`}
              icon={Activity}
              color="text-success"
            />
            {hasPersistentStore(dataMode) && pipelineStats ? (
              <SummaryCard
                label="Throughput"
                value={pipelineStats.records_per_min > 0 ? `${formatNumber(pipelineStats.records_per_min, 1)}/min` : "---"}
                sub={`${formatNumber(pipelineStats.total_records)} in 5m`}
                icon={TrendingUp}
                color="text-info"
              />
            ) : (
              <SummaryCard
                label="Sources"
                value={String(activeSources || "---")}
                sub={`of ${sourceEntries.length} configured`}
                icon={Radio}
                color="text-info"
              />
            )}
            <SummaryCard
              label="Faults"
              value={String(faultCount)}
              sub={faultCount === 0 ? "all nominal" : `${faultCount} in fault`}
              icon={AlertTriangle}
              color={faultCount > 0 ? "text-destructive" : "text-success"}
              pulse={faultCount > 0}
            />
            <SummaryCard
              label="Avg Latency"
              value={avgLatency > 0 ? formatLatency(avgLatency) : "---"}
              sub="across pipelines"
              icon={Gauge}
              color="text-foreground"
            />
            {hasPersistentStore(dataMode) && pipelineStats ? (
              <SummaryCard
                label="Freshness"
                value={freshnesDisplay}
                sub={pipelineStats.latest_event ? `latest: ${pipelineStats.latest_event.slice(11, 19)}` : "no data"}
                icon={Clock}
                color="text-foreground"
              />
            ) : (
              <SummaryCard
                label="Freshness"
                value={hasRealtimeWs(dataMode) && sensorList.length > 0 ? "<1s" : "---"}
                sub={hasRealtimeWs(dataMode) ? "real-time stream" : "no data"}
                icon={Clock}
                color="text-foreground"
              />
            )}
          </div>

          {/* Spark/Zerobus streaming series + rolling /api/sources metrics */}
          <StreamingSourceMetrics
            pipelineId={pipelineId}
            sourceKind={sourceKind}
            sourceLabel={pipeline ? getPipelineSourceLabel(pipeline) : sourceKind}
            industry={pipeline?.industry ?? undefined}
            mode={metricModeForPipeline(pipeline)}
            throughputData={streamingThroughputData}
            batchDurationData={streamingBatchDurationData}
            latencyData={streamingLatencyData}
            refreshSec={refreshSec}
          />

          {/* Row 3: Two-column — Throughput & Latency + Sensor Distribution */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {/* Throughput & Latency */}
            <Card className="glass border-border/50">
              <CardHeader className="pb-2">
                <CardTitle className="flex items-center gap-2 text-sm font-display">
                  <Wifi className="h-4 w-4 text-primary" />
                  {hasPersistentStore(dataMode) ? "Ingestion Rate" : "Throughput & Latency"}
                </CardTitle>
              </CardHeader>
              <CardContent>
                {throughputLatencyData.length > 0 ? (
                  <ResponsiveContainer width="100%" height={200}>
                    {hasPersistentStore(dataMode) ? (
                      <BarChart data={throughputLatencyData} barCategoryGap="30%">
                        <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border) / 0.4)" vertical={false} />
                        <XAxis dataKey="time" stroke="hsl(var(--muted-foreground))" fontSize={9} tickLine={false} />
                        <YAxis stroke="hsl(var(--muted-foreground))" fontSize={9} tickLine={false} />
                        <RechartsTooltip contentStyle={TOOLTIP_STYLE} />
                        <Bar dataKey="count" fill="#59dea2" name="Records/min" radius={[2, 2, 0, 0]} />
                        <Legend wrapperStyle={{ fontSize: 10 }} />
                      </BarChart>
                    ) : (
                      <LineChart data={throughputLatencyData}>
                        <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border) / 0.4)" />
                        <XAxis dataKey="time" stroke="hsl(var(--muted-foreground))" fontSize={9} tickLine={false} />
                        <YAxis stroke="hsl(var(--muted-foreground))" fontSize={9} tickLine={false} unit="ms" />
                        <RechartsTooltip contentStyle={TOOLTIP_STYLE} />
                        <Line type="monotone" dataKey="e2e_ms" stroke="#FF3621" strokeWidth={2} dot={false} name="E2E Latency" />
                        <Line type="monotone" dataKey="inputRPS" stroke="#6c5ce7" strokeWidth={2} dot={false} name="Input RPS" />
                        <Legend wrapperStyle={{ fontSize: 10 }} />
                      </LineChart>
                    )}
                  </ResponsiveContainer>
                ) : (
                  <div className="flex h-[200px] items-center justify-center font-ui text-xs text-muted-foreground">
                    Waiting for data...
                  </div>
                )}
              </CardContent>
            </Card>

            {/* Sensor Distribution Pie */}
            <Card className="glass border-border/50">
              <CardHeader className="pb-2">
                <CardTitle className="flex items-center gap-2 text-sm font-display">
                  <Layers className="h-4 w-4 text-primary" />
                  Sensor Distribution
                </CardTitle>
              </CardHeader>
              <CardContent>
                {sensorDistributionData.length > 0 ? (
                  <ResponsiveContainer width="100%" height={200}>
                    <PieChart>
                      <Pie
                        data={sensorDistributionData}
                        cx="50%"
                        cy="50%"
                        innerRadius={50}
                        outerRadius={80}
                        paddingAngle={2}
                        dataKey="value"
                        nameKey="name"
                        label={({ name, value }) => `${name} (${value})`}
                      >
                        {sensorDistributionData.map((_, i) => (
                          <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />
                        ))}
                      </Pie>
                      <RechartsTooltip contentStyle={TOOLTIP_STYLE} />
                      <Legend wrapperStyle={{ fontSize: 10 }} />
                    </PieChart>
                  </ResponsiveContainer>
                ) : (
                  <div className="flex h-[200px] items-center justify-center font-ui text-xs text-muted-foreground">
                    Waiting for sensor data...
                  </div>
                )}
              </CardContent>
            </Card>
          </div>

          {/* Row 4: Two-column — Fault Timeline + Source Status */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {/* Fault Timeline */}
            <Card className="glass border-border/50">
              <CardHeader className="pb-2">
                <CardTitle className="flex items-center gap-2 text-sm font-display">
                  <AlertTriangle className="h-4 w-4 text-destructive" />
                  Fault Timeline
                  {faultCount > 0 && (
                    <Badge variant="destructive" className="ml-auto font-mono text-[10px]">{faultCount} active</Badge>
                  )}
                </CardTitle>
              </CardHeader>
              <CardContent>
                {faultTimelineData.length > 0 ? (
                  <ResponsiveContainer width="100%" height={200}>
                    <ScatterChart>
                      <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border) / 0.4)" />
                      <XAxis
                        dataKey="time"
                        type="number"
                        domain={["auto", "auto"]}
                        tickFormatter={(ts) => new Date(ts).toLocaleTimeString("en-US", { hour12: false, minute: "2-digit", second: "2-digit" })}
                        stroke="hsl(var(--muted-foreground))"
                        fontSize={9}
                        tickLine={false}
                        name="Time"
                      />
                      <YAxis
                        dataKey="value"
                        type="number"
                        domain={[0, 2]}
                        hide
                        name="Fault"
                      />
                      <ZAxis range={[80, 80]} />
                      <RechartsTooltip
                        contentStyle={TOOLTIP_STYLE}
                        formatter={(_val: unknown, _name: string, props: { payload?: { sensor?: string } }) => [props.payload?.sensor ?? "", "Sensor"]}
                        labelFormatter={(ts) => new Date(Number(ts)).toLocaleTimeString()}
                      />
                      <Scatter data={faultTimelineData} fill="#ef4444" />
                    </ScatterChart>
                  </ResponsiveContainer>
                ) : (
                  <div className="flex h-[200px] items-center justify-center font-ui text-xs text-muted-foreground">
                    No active faults
                  </div>
                )}
              </CardContent>
            </Card>

            {/* Source Status Cards */}
            <Card className="glass border-border/50">
              <CardHeader className="pb-2">
                <CardTitle className="flex items-center gap-2 text-sm font-display">
                  <Radio className="h-4 w-4 text-primary" />
                  Source Status
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-2 gap-3">
                  {sourceEntries.map(([key, src]) => (
                    <SourceCard
                      key={key}
                      name={String((src as Record<string, unknown>).display_name || key)}
                      status={String((src as Record<string, unknown>).status || "inactive")}
                      records={Number((src as Record<string, unknown>).records_per_sec || 0)}
                      latency={Number((src as Record<string, unknown>).avg_latency_ms || 0)}
                    />
                  ))}
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Ingestion Latency Card */}
          {isPipelineDashboard && Object.values(sources).some((s) => s.status === "active") && (
            <Card className="glass border-border/50">
              <CardHeader className="pb-2">
                <CardTitle className="flex items-center gap-2 text-sm font-display">
                  <Clock className="h-4 w-4 text-primary" />
                  Ingestion Latency
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                  {Object.values(sources)
                    .filter((s) => s.status === "active")
                    .map((src) => (
                      <div key={src.name} className="rounded-md bg-surface-low border border-border/50 px-3 py-2">
                        <p className="text-[10px] text-muted-foreground">{src.display_name}</p>
                        <div className="flex items-baseline gap-2 mt-1">
                          <div>
                            <span className="font-mono text-sm font-semibold tabular-nums">
                              {SHOW_INSERT_LAG_METRICS && src.avg_e2e_latency_ms > 0
                                ? `${Math.round(src.avg_e2e_latency_ms)}ms`
                                : "---"}
                            </span>
                            <span className="text-[9px] text-muted-foreground ml-1">avg</span>
                          </div>
                          <div>
                            <span className="font-mono text-sm font-semibold tabular-nums text-warning">
                              {SHOW_INSERT_LAG_METRICS && src.p95_e2e_latency_ms > 0
                                ? `${Math.round(src.p95_e2e_latency_ms)}ms`
                                : "---"}
                            </span>
                            <span className="text-[9px] text-muted-foreground ml-1">p95</span>
                          </div>
                        </div>
                      </div>
                    ))}
                </div>
              </CardContent>
            </Card>
          )}

          {/* Row 5: Streaming Metrics Panel (full-width, conditional) */}
          {isPipelineDashboard && streamingMetrics && streamingMetrics.summary.total_rows > 0 && (
            <Card className="glass border-border/50">
              <CardHeader className="pb-2">
                <CardTitle className="flex items-center gap-2 text-sm font-display">
                  <Activity className="h-4 w-4 text-primary" />
                  Spark Streaming Metrics
                  <Badge variant="outline" className="ml-auto font-mono text-[10px]">DBSQL</Badge>
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                {/* Summary stat cards */}
                <div className="grid grid-cols-3 gap-3">
                  <div className="rounded-md bg-surface-low border border-border/50 px-3 py-2 text-center">
                    <p className="text-[9px] text-muted-foreground uppercase tracking-wider">Batches</p>
                    <p className="font-mono text-lg font-semibold">{streamingMetrics.summary.total_rows}</p>
                  </div>
                  <div className="rounded-md bg-surface-low border border-border/50 px-3 py-2 text-center">
                    <p className="text-[9px] text-muted-foreground uppercase tracking-wider">Avg RPS</p>
                    <p className="font-mono text-lg font-semibold">{streamingMetrics.summary.avg_processed_rps}</p>
                  </div>
                  <div className="rounded-md bg-surface-low border border-border/50 px-3 py-2 text-center">
                    <p className="text-[9px] text-muted-foreground uppercase tracking-wider">Avg Duration</p>
                    <p className="font-mono text-lg font-semibold">{streamingMetrics.summary.avg_batch_duration_ms}ms</p>
                  </div>
                </div>

                {/* Throughput chart */}
                {streamingThroughputData.length > 0 && (
                  <div>
                    <p className="text-[9px] text-muted-foreground font-semibold mb-1 uppercase tracking-wider">Throughput per Batch</p>
                    <ResponsiveContainer width="100%" height={160}>
                      <LineChart data={streamingThroughputData}>
                        <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border) / 0.4)" />
                        <XAxis dataKey="batch" stroke="hsl(var(--muted-foreground))" fontSize={8} tickLine={false} />
                        <YAxis stroke="hsl(var(--muted-foreground))" fontSize={8} tickLine={false} />
                        <RechartsTooltip contentStyle={TOOLTIP_STYLE} />
                        <Line type="monotone" dataKey="input_rps" stroke="#6c5ce7" strokeWidth={2} dot={false} name="Input RPS" />
                        <Line type="monotone" dataKey="processed_rps" stroke="#00b894" strokeWidth={2} dot={false} name="Processed RPS" />
                        <Legend wrapperStyle={{ fontSize: 10 }} />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                )}

                {/* Batch Duration chart */}
                {streamingBatchDurationData.length > 0 && (
                  <div>
                    <p className="text-[9px] text-muted-foreground font-semibold mb-1 uppercase tracking-wider">Batch Duration</p>
                    <ResponsiveContainer width="100%" height={140}>
                      <BarChart data={streamingBatchDurationData} barCategoryGap="20%">
                        <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border) / 0.4)" vertical={false} />
                        <XAxis dataKey="batch" stroke="hsl(var(--muted-foreground))" fontSize={8} tickLine={false} />
                        <YAxis stroke="hsl(var(--muted-foreground))" fontSize={8} tickLine={false} />
                        <RechartsTooltip contentStyle={TOOLTIP_STYLE} />
                        <Bar dataKey="duration_ms" fill="#6c5ce7" name="Batch Duration (ms)" radius={[2, 2, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                )}

                {/* RTM Latency chart */}
                {streamingLatencyData.length > 0 && (streamingMetrics.summary.avg_e2e_p50_ms > 0 || streamingMetrics.summary.avg_processing_p50_ms > 0) && (
                  <div>
                    <p className="text-[9px] text-muted-foreground font-semibold mb-1 uppercase tracking-wider">RTM Latency (p50)</p>
                    <ResponsiveContainer width="100%" height={160}>
                      <LineChart data={streamingLatencyData}>
                        <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border) / 0.4)" />
                        <XAxis dataKey="batch" stroke="hsl(var(--muted-foreground))" fontSize={8} tickLine={false} />
                        <YAxis stroke="hsl(var(--muted-foreground))" fontSize={8} tickLine={false} unit="ms" />
                        <RechartsTooltip contentStyle={TOOLTIP_STYLE} />
                        <Line type="monotone" dataKey="e2e_p50_ms" stroke="#e17055" strokeWidth={2} dot={false} name="E2E p50" />
                        <Line type="monotone" dataKey="processing_p50_ms" stroke="#74b9ff" strokeWidth={2} dot={false} name="Processing p50" />
                        <Line type="monotone" dataKey={QUEUING_P50_FIELD} stroke="#fdcb6e" strokeWidth={2} dot={false} name="Queuing p50" />
                        <Legend wrapperStyle={{ fontSize: 10 }} />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                )}

                {/* RTM Latency summary cards */}
                {(streamingMetrics.summary.avg_e2e_p50_ms > 0 || streamingMetrics.summary.avg_processing_p50_ms > 0) && (
                  <div>
                    <p className="text-[9px] text-muted-foreground font-semibold mb-1 uppercase tracking-wider">RTM Latency Summary</p>
                    <div className="grid grid-cols-3 gap-2">
                      <div className="rounded-md bg-surface-low border border-border/50 px-2 py-1.5">
                        <p className="text-[8px] text-muted-foreground">E2E</p>
                        <p className="font-mono text-xs font-semibold">{streamingMetrics.summary.avg_e2e_p50_ms}ms <span className="text-[8px] text-muted-foreground">p50</span></p>
                        <p className="font-mono text-[10px] text-warning">{streamingMetrics.summary.avg_e2e_p99_ms}ms <span className="text-[8px] text-muted-foreground">p99</span></p>
                      </div>
                      <div className="rounded-md bg-surface-low border border-border/50 px-2 py-1.5">
                        <p className="text-[8px] text-muted-foreground">Processing</p>
                        <p className="font-mono text-xs font-semibold">{streamingMetrics.summary.avg_processing_p50_ms}ms <span className="text-[8px] text-muted-foreground">p50</span></p>
                        <p className="font-mono text-[10px] text-warning">{streamingMetrics.summary.avg_processing_p99_ms}ms <span className="text-[8px] text-muted-foreground">p99</span></p>
                      </div>
                      <div className="rounded-md bg-surface-low border border-border/50 px-2 py-1.5">
                        <p className="text-[8px] text-muted-foreground">Queuing</p>
                        <p className="font-mono text-xs font-semibold">{streamingMetrics.summary.avg_queuing_p50_ms}ms <span className="text-[8px] text-muted-foreground">p50</span></p>
                        <p className="font-mono text-[10px] text-warning">{streamingMetrics.summary.avg_queuing_p99_ms}ms <span className="text-[8px] text-muted-foreground">p99</span></p>
                      </div>
                    </div>
                  </div>
                )}

                {/* Observed metrics value cards */}
                {observedMetricsCards.length > 0 && (
                  <div>
                    <p className="text-[9px] text-muted-foreground font-semibold mb-1 uppercase tracking-wider">Observed Metrics</p>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
                      {observedMetricsCards.map(({ key, value }) => (
                        <div key={key} className="rounded-md bg-surface-low border border-border/50 px-2 py-1.5 text-center">
                          <p className="text-[8px] text-muted-foreground truncate">{key}</p>
                          <p className="font-mono text-xs font-semibold">{value}</p>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {streamingMetrics.summary.last_write && (
                  <p className="text-[9px] text-muted-foreground">
                    Last batch: #{streamingMetrics.summary.last_batch} at {streamingMetrics.summary.last_write}
                  </p>
                )}
              </CardContent>
            </Card>
          )}

          {/* Row 6: Sensor readings table (full-width) */}
          <Card className="glass border-border/50">
            <CardHeader className="pb-2">
              <CardTitle className="flex items-center gap-2 font-display text-sm">
                {hasRealtimeWs(dataMode) ? "Live Readings" : "Latest Readings"}
                {sensorList.length > 0 && (
                  <Badge variant="secondary" className="font-mono text-[10px]">{sensorList.length}</Badge>
                )}
              </CardTitle>
            </CardHeader>
            <CardContent className="p-0">
              {sensorList.length > 0 ? (
                <div className="overflow-x-auto max-h-64">
                  <table className="w-full">
                    <thead>
                      <tr className="border-b border-border/50 bg-surface-low">
                        <th className="font-ui px-3 py-1.5 text-left text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">Sensor</th>
                        <th className="font-ui px-3 py-1.5 text-left text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">Type</th>
                        <th className="font-ui px-3 py-1.5 text-right text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">Value</th>
                        <th className="font-ui px-3 py-1.5 text-center text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {sensorList
                        .sort((a, b) => a.sensor_name.localeCompare(b.sensor_name))
                        .map((sensor) => (
                          <tr
                            key={sensor.sensor_name}
                            className="border-b border-border/50 last:border-0 hover:bg-surface-high cursor-pointer transition-colors"
                            onClick={() => setSelectedSensor(sensor)}
                          >
                            <td className="font-ui px-3 py-1.5 text-xs font-medium">{sensor.sensor_name}</td>
                            <td className="font-ui px-3 py-1.5 text-xs text-muted-foreground capitalize">{sensor.sensor_type}</td>
                            <td className="px-3 py-1.5 text-right font-mono text-xs tabular-nums">
                              {formatNumber(sensor.value, 2)} <span className="font-ui text-muted-foreground">{sensor.unit}</span>
                            </td>
                            <td className="px-3 py-1.5 text-center">
                              {sensor.fault_active ? (
                                <Badge variant="destructive" className="font-mono text-[10px]">FAULT</Badge>
                              ) : (
                                <Badge variant="success" className="font-mono text-[10px]">OK</Badge>
                              )}
                            </td>
                          </tr>
                        ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="flex h-20 items-center justify-center font-ui text-xs text-muted-foreground">
                  Waiting for sensor data...
                </div>
              )}
            </CardContent>
          </Card>
          </>
          )}
          </DashboardSection>

          {/* ================= SECTION 3 — Lakebase · warm (blue) =================
              Hidden for Zerobus pipelines: Zerobus does not use Lakebase. */}
          {!isZerobus && (
          <DashboardSection
            index={3}
            title="Lakebase · warm"
            subtitle="Hot-tier Postgres write performance, sink health & LakeTS analytics"
            accent="lakebase"
            emphasis={displayMode === "historical" ? "dimmed" : "normal"}
            badge={
              <div className="flex items-center gap-2">
                <RangeSelector
                  options={LAKEBASE_RANGE_OPTIONS}
                  value={lakebaseRangeMin}
                  onChange={setLakebaseRangeMin}
                  accentClass="bg-primary/15 text-primary font-medium"
                />
                <SectionBadge color={SECTION_ACCENTS.lakebase} label={refreshSec > 0 ? `WARM · ${refreshSec}s` : "WARM · paused"} />
              </div>
            }
          >
          {/* Lakebase write performance — rows/sec & rows/min readouts + chart.
              Always queries the Lakebase hot tier; window = Lakebase range
              selector.  Polling governed by the refresh-interval selector
              (SSE unaffected). */}
          <LakebasePerfPanel pipelineId={pipelineId} minutes={lakebaseRangeMin} refreshSec={refreshSec} />

          {/* Lakebase sink health — live observability for the Spark writer */}
          <LakebaseSinkHealth
            pipelineId={pipelineId}
            refreshSec={refreshSec}
            databricksHost={typeof window !== "undefined" ? window.location.hostname : ""}
            jobId={pipeline?.resources?.job_id ?? null}
          />

          {/* LakeTS Analytics — hot-tier-only SQL functions (gap-fill / rate /
              histogram).  Always Lakebase; window = Lakebase range selector. */}
          {tiering && (
            <div className="grid grid-cols-1 gap-3 lg:grid-cols-3">
              <GapfillChart
                pipelineId={pipelineId}
                rangeMinutes={lakebaseRangeMin}
                refreshSec={refreshSec}
                enabled={true}
              />
              <RateChart
                pipelineId={pipelineId}
                rangeMinutes={lakebaseRangeMin}
                refreshSec={refreshSec}
                enabled={true}
              />
              <HistogramChart
                pipelineId={pipelineId}
                sensorName={(lbLatest[0]?.sensor_name as string | undefined) ?? ""}
                rangeMinutes={lakebaseRangeMin}
                refreshSec={refreshSec}
                enabled={true}
              />
            </div>
          )}
          </DashboardSection>
          )}

          {/* ============== SECTION 4 — DBSQL / Delta data (purple) ==============
              For Zerobus this is the PRIMARY store — the Delta table is always
              available, so the section is NOT CDF-gated.  For Kafka/SSS it is the
              cold-tier Delta analytics view, gated on Lakebase Sync (CDF). */}
          <DashboardSection
            index={isZerobus ? 3 : 4}
            title={isZerobus ? "DBSQL · Delta data" : "Historical · DBSQL cold"}
            subtitle={isZerobus
              ? "Primary Delta store for this Zerobus pipeline, queried via Databricks SQL"
              : "Cold-tier Delta analytics via Databricks SQL — requires Lakebase Sync (CDF)"}
            accent="dbsql"
            emphasis={!isZerobus && displayMode === "live" ? "dimmed" : "normal"}
            badge={
              <div className="flex items-center gap-2">
                <RangeSelector
                  options={DBSQL_RANGE_OPTIONS}
                  value={dbsqlRangeMin}
                  onChange={setDbsqlRangeMin}
                  accentClass="bg-info/15 text-info font-medium"
                />
                {isZerobus ? (
                  <SectionBadge color={SECTION_ACCENTS.dbsql} label="DELTA · PRIMARY" />
                ) : (
                  <SectionBadge
                    color={cdfStatus?.streaming ? SECTION_ACCENTS.dbsql : SECTION_ACCENTS.cdf}
                    label={cdfStatus?.streaming ? "CDF · STREAMING" : cdfStatus?.status === "SNAPSHOTTING" ? "CDF · SNAPSHOTTING" : "CDF · OFF"}
                  />
                )}
              </div>
            }
          >
          {/* Always queries the DBSQL Delta tier; window = DBSQL range selector.
              For SSS, gated on CDF streaming.  For Zerobus, Delta is the primary
              store and is always available — no CDF gate. */}
          <HistoricalDbsql
            pipelineId={pipelineId}
            rangeMinutes={dbsqlRangeMin}
            refreshSec={refreshSec}
            enabled={isZerobus || !!cdfStatus?.streaming}
            cdfStatus={cdfStatus}
            ungated={isZerobus}
            onStateChange={handleDbsqlState}
            registerRefresh={registerDbsqlRefresh}
          />
          </DashboardSection>
        </div>
      </div>
    </div>
  );
}
