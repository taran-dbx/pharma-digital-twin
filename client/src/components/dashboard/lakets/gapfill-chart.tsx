import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Activity, Sparkles } from "lucide-react";
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { api } from "@/lib/api";
import { cn } from "@/lib/utils";

interface GapfillChartProps {
  pipelineId: string;
  rangeMinutes: number;
  refreshSec: number;
  enabled: boolean;
  industry?: string;
}

interface RawRow {
  bucket: string;
  sensor_name: string;
  value: number;
}

interface PivotRow {
  bucket: string;
  bucketEpoch: number;
  [sensor: string]: string | number;
}

// Recharts-friendly colour palette.  Deterministic order keeps colours stable
// across re-renders as long as the sensor order is stable.
const SERIES_COLORS = [
  "#ff3621", // primary (Databricks red)
  "#00a972", // success / green
  "#0073e6", // info / blue
  "#ffa400", // warning / amber
  "#8b5cf6", // violet
  "#ec4899", // pink
];

function safeNumber(raw: unknown): number | null {
  if (typeof raw === "number" && Number.isFinite(raw)) return raw;
  if (typeof raw === "string") {
    const parsed = Number.parseFloat(raw);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function toRawRows(data: unknown): RawRow[] {
  if (!Array.isArray(data)) return [];
  const out: RawRow[] = [];
  for (const item of data) {
    if (!item || typeof item !== "object") continue;
    const obj = item as Record<string, unknown>;
    const bucket = typeof obj.bucket === "string" ? obj.bucket : null;
    const sensor =
      typeof obj.sensor_name === "string" ? obj.sensor_name : null;
    const value = safeNumber(obj.value);
    if (bucket && sensor && value !== null) {
      out.push({ bucket, sensor_name: sensor, value });
    }
  }
  return out;
}

function formatBucket(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function pivot(rows: RawRow[]): { data: PivotRow[]; sensors: string[] } {
  const byBucket = new Map<string, PivotRow>();
  const sensors = new Set<string>();
  for (const r of rows) {
    sensors.add(r.sensor_name);
    const existing = byBucket.get(r.bucket);
    if (existing) {
      existing[r.sensor_name] = r.value;
    } else {
      byBucket.set(r.bucket, {
        bucket: r.bucket,
        bucketEpoch: new Date(r.bucket).getTime(),
        [r.sensor_name]: r.value,
      });
    }
  }
  const data = Array.from(byBucket.values()).sort(
    (a, b) => a.bucketEpoch - b.bucketEpoch,
  );
  return { data, sensors: Array.from(sensors).sort() };
}

export function GapfillChart({
  pipelineId,
  rangeMinutes,
  refreshSec,
  enabled,
  industry,
}: GapfillChartProps) {
  const [rows, setRows] = useState<RawRow[]>([]);
  const [reason, setReason] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const cancelRef = useRef(false);

  const fetchOnce = useCallback(async () => {
    if (!enabled || !pipelineId) return;
    setLoading(true);
    setError(null);
    try {
      const res = await api.getLaketsGapfill(pipelineId, rangeMinutes, industry);
      if (cancelRef.current) return;
      setReason(res.reason ?? null);
      setRows(toRawRows(res.rows));
    } catch (err: unknown) {
      if (!cancelRef.current) {
        setError(err instanceof Error ? err.message : "Unknown error");
      }
    } finally {
      if (!cancelRef.current) setLoading(false);
    }
  }, [enabled, pipelineId, rangeMinutes, industry]);

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

  const { data, sensors } = useMemo(() => pivot(rows), [rows]);

  return (
    <LaketsCard title="Gap-filled trend" dim={!enabled} tooltip={
      !enabled
        ? "Enable tiering to see LakeTS analytics"
        : "time_bucket_gapfill + locf — continuous line even across missing intervals"
    }>
      {!enabled ? (
        <EmptyState message="Enable tiering to see LakeTS analytics" />
      ) : reason ? (
        <EmptyState message={reason} />
      ) : error ? (
        <EmptyState message={error} tone="error" />
      ) : data.length === 0 ? (
        <EmptyState message={loading ? "Loading…" : "No data in range"} />
      ) : (
        <ResponsiveContainer width="100%" height={180}>
          <LineChart data={data} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
            <CartesianGrid stroke="currentColor" strokeOpacity={0.08} vertical={false} />
            <XAxis
              dataKey="bucket"
              tickFormatter={formatBucket}
              tick={{ fontSize: 10, fill: "currentColor" }}
              stroke="currentColor"
              strokeOpacity={0.3}
              minTickGap={32}
            />
            <YAxis
              tick={{ fontSize: 10, fill: "currentColor" }}
              stroke="currentColor"
              strokeOpacity={0.3}
              width={36}
            />
            <Tooltip
              labelFormatter={(v) => (typeof v === "string" ? formatBucket(v) : String(v))}
              contentStyle={{
                background: "rgba(0,0,0,0.8)",
                border: "1px solid rgba(255,255,255,0.1)",
                fontSize: 11,
              }}
            />
            <Legend wrapperStyle={{ fontSize: 10 }} iconSize={8} />
            {sensors.map((s, idx) => (
              <Line
                key={s}
                type="monotone"
                dataKey={s}
                stroke={SERIES_COLORS[idx % SERIES_COLORS.length]}
                strokeWidth={1.5}
                dot={false}
                isAnimationActive={false}
                connectNulls
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      )}
    </LaketsCard>
  );
}

// ---- shared tiny helpers (kept local so the file stays self-contained) ----

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
      <div className="flex items-center justify-between">
        <span className="font-ui text-xs font-medium uppercase tracking-wider text-muted-foreground">
          {title}
        </span>
        <span className="inline-flex items-center gap-1 rounded-full bg-primary/15 px-2 py-0.5 ring-1 ring-primary/30 text-[9px] font-medium text-primary">
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
