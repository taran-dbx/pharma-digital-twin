import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Activity, Sparkles } from "lucide-react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { api } from "@/lib/api";
import { cn } from "@/lib/utils";

interface HistogramChartProps {
  pipelineId: string;
  rangeMinutes: number;
  refreshSec: number;
  enabled: boolean;
  sensorName: string;
  industry?: string;
}

interface RawBucket {
  bucket_idx: number;
  lower: number;
  upper: number;
  count: number;
}

interface ChartRow extends RawBucket {
  label: string;
}

function safeNumber(raw: unknown): number | null {
  if (typeof raw === "number" && Number.isFinite(raw)) return raw;
  if (typeof raw === "string") {
    const parsed = Number.parseFloat(raw);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function toBuckets(data: unknown): RawBucket[] {
  if (!Array.isArray(data)) return [];
  const out: RawBucket[] = [];
  for (const item of data) {
    if (!item || typeof item !== "object") continue;
    const obj = item as Record<string, unknown>;
    const idx = safeNumber(obj.bucket_idx);
    const lower = safeNumber(obj.lower);
    const upper = safeNumber(obj.upper);
    const count = safeNumber(obj.count);
    if (
      idx !== null &&
      lower !== null &&
      upper !== null &&
      count !== null
    ) {
      out.push({
        bucket_idx: idx,
        lower,
        upper,
        count,
      });
    }
  }
  return out.sort((a, b) => a.bucket_idx - b.bucket_idx);
}

function formatBound(v: number): string {
  if (Math.abs(v) >= 1000) return v.toFixed(0);
  if (Math.abs(v) >= 10) return v.toFixed(1);
  return v.toFixed(2);
}

function toChartRows(buckets: RawBucket[]): ChartRow[] {
  return buckets.map((b) => ({
    ...b,
    label: `${formatBound(b.lower)}–${formatBound(b.upper)}`,
  }));
}

export function HistogramChart({
  pipelineId,
  rangeMinutes,
  refreshSec,
  enabled,
  sensorName,
  industry,
}: HistogramChartProps) {
  const [buckets, setBuckets] = useState<RawBucket[]>([]);
  const [reason, setReason] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const cancelRef = useRef(false);

  const fetchOnce = useCallback(async () => {
    if (!enabled || !pipelineId || !sensorName) return;
    setLoading(true);
    setError(null);
    try {
      const res = await api.getLaketsHistogram(
        pipelineId,
        sensorName,
        rangeMinutes,
        industry,
      );
      if (cancelRef.current) return;
      setReason(res.reason ?? null);
      setBuckets(toBuckets(res.rows));
    } catch (err: unknown) {
      if (!cancelRef.current) {
        setError(err instanceof Error ? err.message : "Unknown error");
      }
    } finally {
      if (!cancelRef.current) setLoading(false);
    }
  }, [enabled, pipelineId, rangeMinutes, sensorName, industry]);

  useEffect(() => {
    cancelRef.current = false;
    fetchOnce();
    if (!enabled || refreshSec <= 0) {
      return () => {
        cancelRef.current = true;
      };
    }
    const id = setInterval(fetchOnce, refreshSec * 1000);
    return () => {
      cancelRef.current = true;
      clearInterval(id);
    };
  }, [fetchOnce, enabled, refreshSec]);

  const chartRows = useMemo(() => toChartRows(buckets), [buckets]);
  const title = sensorName
    ? `Value distribution · ${sensorName}`
    : "Value distribution";

  return (
    <LaketsCard
      title={title}
      dim={!enabled}
      tooltip={
        !enabled
          ? "Enable tiering to see LakeTS analytics"
          : "lakets.histogram — how often readings fall into each value bin"
      }
    >
      {!enabled ? (
        <EmptyState message="Enable tiering to see LakeTS analytics" />
      ) : !sensorName ? (
        <EmptyState message="No sensor selected" />
      ) : reason ? (
        <EmptyState message={reason} />
      ) : error ? (
        <EmptyState message={error} tone="error" />
      ) : chartRows.length === 0 ? (
        <EmptyState message={loading ? "Loading…" : "No data in range"} />
      ) : (
        <ResponsiveContainer width="100%" height={180}>
          <BarChart data={chartRows} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
            <CartesianGrid stroke="currentColor" strokeOpacity={0.08} vertical={false} />
            <XAxis
              dataKey="label"
              tick={{ fontSize: 9, fill: "currentColor" }}
              stroke="currentColor"
              strokeOpacity={0.3}
              interval="preserveStartEnd"
            />
            <YAxis
              tick={{ fontSize: 10, fill: "currentColor" }}
              stroke="currentColor"
              strokeOpacity={0.3}
              width={32}
              allowDecimals={false}
            />
            <Tooltip
              cursor={{ fill: "rgba(255,255,255,0.04)" }}
              contentStyle={{
                background: "rgba(0,0,0,0.8)",
                border: "1px solid rgba(255,255,255,0.1)",
                fontSize: 11,
              }}
              formatter={(value) => [value, "count"]}
              labelFormatter={(label) => `bin ${label}`}
            />
            <Bar
              dataKey="count"
              fill="#ff3621"
              fillOpacity={0.7}
              isAnimationActive={false}
            />
          </BarChart>
        </ResponsiveContainer>
      )}
    </LaketsCard>
  );
}

// ---- shared tiny helpers ----

interface LaketsCardProps {
  title: string;
  tooltip?: string;
  dim?: boolean;
  children: React.ReactNode;
}

function LaketsCard({ title, tooltip, dim, children }: LaketsCardProps) {
  return (
    <div
      className={cn(
        "flex flex-col gap-2 rounded-md border border-border/50 glass p-3",
        dim && "opacity-50",
      )}
      title={tooltip}
    >
      <div className="flex items-center justify-between gap-2">
        <span className="truncate font-ui text-xs font-medium uppercase tracking-wider text-muted-foreground">
          {title}
        </span>
        <span className="inline-flex shrink-0 items-center gap-1 rounded-full bg-primary/15 px-2 py-0.5 ring-1 ring-primary/30 text-[9px] font-medium text-primary">
          <Sparkles className="h-2.5 w-2.5" />
          LakeTS
        </span>
      </div>
      {children}
    </div>
  );
}

function EmptyState({
  message,
  tone = "muted",
}: {
  message: string;
  tone?: "muted" | "error";
}) {
  return (
    <div
      className={cn(
        "flex h-[180px] items-center justify-center gap-1.5 text-xs",
        tone === "error" ? "text-destructive" : "text-muted-foreground",
      )}
    >
      <Activity className="h-3.5 w-3.5" />
      {message}
    </div>
  );
}
