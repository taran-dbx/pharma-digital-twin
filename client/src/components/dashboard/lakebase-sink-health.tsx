import { useCallback, useEffect, useRef, useState } from "react";
import { Database, ExternalLink, Info, Loader2, Wrench } from "lucide-react";
import { toast } from "sonner";
import { api } from "@/lib/api";
import { cn } from "@/lib/utils";

interface LakebaseSinkHealthProps {
  pipelineId: string;
  /** Poll frequency in seconds.  ``0`` disables polling. */
  refreshSec: number;
  /** Databricks workspace host, e.g. ``example-demo.cloud.databricks.com``. */
  databricksHost: string;
  /** Optional Spark job id used to build the "Open Spark UI" link. */
  jobId?: string | null;
  /**
   * Industry id — when set, resolves the deterministic industry-first
   * Lakebase branch (``iot-pipelines``) instead of the legacy per-pipeline
   * ``sink_config`` lookup. Optional for backward compat with the legacy
   * per-pipeline dashboard.
   */
  industry?: string;
}

interface HealthSnapshot {
  rows_5m: number;
  latest_at: string | null;
  latency_sec: number | null;
  inserts_per_sec_recent: number;
  error?: string;
}

type Freshness = "fresh" | "stale" | "cold" | "unknown";

function classifyFreshness(latencySec: number | null): Freshness {
  if (latencySec === null || !Number.isFinite(latencySec)) return "unknown";
  if (latencySec < 30) return "fresh";
  if (latencySec < 300) return "stale";
  return "cold";
}

const FRESHNESS_STYLES: Record<
  Freshness,
  { label: string; dot: string; text: string; ring: string; bg: string }
> = {
  fresh: {
    label: "fresh",
    dot: "bg-success",
    text: "text-success",
    ring: "ring-success/30",
    bg: "bg-success/10",
  },
  stale: {
    label: "stale",
    dot: "bg-warning",
    text: "text-warning",
    ring: "ring-warning/30",
    bg: "bg-warning/10",
  },
  cold: {
    label: "cold",
    dot: "bg-destructive",
    text: "text-destructive",
    ring: "ring-destructive/30",
    bg: "bg-destructive/10",
  },
  unknown: {
    label: "no data",
    dot: "bg-muted-foreground/40",
    text: "text-muted-foreground",
    ring: "ring-border/50",
    bg: "bg-muted/20",
  },
};

function formatLatency(sec: number | null): string {
  if (sec === null || !Number.isFinite(sec)) return "—";
  if (sec < 1) return "<1s";
  if (sec < 60) return `${Math.round(sec)}s`;
  if (sec < 3600) return `${Math.round(sec / 60)}m`;
  return `${(sec / 3600).toFixed(1)}h`;
}

function formatTimestamp(iso: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleTimeString();
}

function formatRows(n: number): string {
  if (!Number.isFinite(n)) return "—";
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return String(n);
}

/**
 * Card showing how much data is flowing into the Lakebase ``sensor_readings``
 * sink — probes the table directly so it does not drift with cached stats.
 */
export function LakebaseSinkHealth({
  pipelineId,
  refreshSec,
  databricksHost,
  jobId,
  industry,
}: LakebaseSinkHealthProps) {
  const [snapshot, setSnapshot] = useState<HealthSnapshot | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [repairing, setRepairing] = useState<boolean>(false);
  const cancelRef = useRef(false);

  const fetchOnce = useCallback(async () => {
    if (!pipelineId) return;
    setLoading(true);
    setError(null);
    try {
      const data = await api.getLakebaseHealth(pipelineId, industry);
      if (!cancelRef.current) setSnapshot(data);
    } catch (err: unknown) {
      if (!cancelRef.current) {
        setError(err instanceof Error ? err.message : "Unknown error");
      }
    } finally {
      if (!cancelRef.current) setLoading(false);
    }
  }, [pipelineId, industry]);

  useEffect(() => {
    cancelRef.current = false;
    fetchOnce();
    if (refreshSec <= 0) {
      return () => {
        cancelRef.current = true;
      };
    }
    const id = setInterval(fetchOnce, refreshSec * 1000);
    return () => {
      cancelRef.current = true;
      clearInterval(id);
    };
  }, [fetchOnce, refreshSec]);

  const handleRepair = useCallback(async () => {
    if (!pipelineId || repairing) return;
    setRepairing(true);
    try {
      const result = await api.healLakebase(pipelineId, industry);
      toast.success(`Repair complete: ${result.steps.length} steps`);
      // Surface per-step results in DevTools for operators triaging a run.
      console.log("heal-lakebase result:", result);
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : "unknown";
      toast.error(`Repair failed: ${message}`);
    } finally {
      setRepairing(false);
    }
  }, [pipelineId, repairing, industry]);

  const freshness = classifyFreshness(snapshot?.latency_sec ?? null);
  const freshStyle = FRESHNESS_STYLES[freshness];
  const sparkUiHref = jobId && databricksHost
    ? `https://${databricksHost}/jobs/${jobId}`
    : null;

  return (
    <div className="flex flex-col gap-3 rounded-md border border-border/50 glass p-4">
      <div className="flex items-start justify-between gap-2">
        <div className="flex items-center gap-2">
          <Database className="h-3.5 w-3.5 text-primary" />
          <span className="font-ui text-xs font-medium uppercase tracking-wider text-muted-foreground">
            Lakebase Sink Health
          </span>
          <span
            title="Probes the sensor_readings table directly over the last 5 minutes to confirm writes are landing."
            className="text-muted-foreground"
          >
            <Info className="h-3 w-3" />
          </span>
        </div>
        <span
          className={cn(
            "inline-flex items-center gap-1 rounded-full px-2 py-0.5 ring-1 text-[10px] font-medium",
            freshStyle.bg,
            freshStyle.ring,
            freshStyle.text,
          )}
        >
          <span className={cn("h-1.5 w-1.5 rounded-full", freshStyle.dot)} />
          {freshStyle.label}
        </span>
      </div>

      <div className="flex items-baseline gap-1.5">
        <span className="font-display text-2xl font-semibold tabular-nums">
          {snapshot ? formatRows(snapshot.rows_5m) : loading ? "…" : "—"}
        </span>
        <span className="text-xs text-muted-foreground">
          rows in last 5 min
        </span>
      </div>

      <div className="grid grid-cols-2 gap-2 text-[11px]">
        <div className="flex flex-col">
          <span className="text-muted-foreground uppercase tracking-wider text-[10px]">
            Latest insert
          </span>
          <span className="tabular-nums text-foreground">
            {snapshot ? formatTimestamp(snapshot.latest_at) : "—"}
          </span>
        </div>
        <div className="flex flex-col">
          <span className="text-muted-foreground uppercase tracking-wider text-[10px]">
            Inserts / sec
          </span>
          <span className="tabular-nums text-foreground">
            {snapshot ? snapshot.inserts_per_sec_recent.toFixed(2) : "—"}
          </span>
        </div>
        <div className="flex flex-col">
          <span className="text-muted-foreground uppercase tracking-wider text-[10px]">
            Freshness lag
          </span>
          <span className="tabular-nums text-foreground">
            {snapshot ? formatLatency(snapshot.latency_sec) : "—"}
          </span>
        </div>
        <div className="flex flex-col">
          <span className="text-muted-foreground uppercase tracking-wider text-[10px]">
            Source
          </span>
          <span className="text-foreground">sensor_readings</span>
        </div>
      </div>

      {(error || snapshot?.error) && (
        <p className="text-[11px] text-destructive">
          {error ?? snapshot?.error}
        </p>
      )}

      <div className="mt-1 flex items-center justify-between gap-2">
        {sparkUiHref ? (
          <a
            href={sparkUiHref}
            target="_blank"
            rel="noreferrer noopener"
            className="inline-flex items-center gap-1 text-[11px] text-primary hover:underline"
          >
            Open Spark UI
            <ExternalLink className="h-3 w-3" />
          </a>
        ) : (
          <span />
        )}
        <button
          type="button"
          onClick={handleRepair}
          disabled={repairing}
          title="Re-apply Lakebase OAuth provisioning and restart the sink job. Use when rows_5m is stuck at 0."
          className={cn(
            "inline-flex items-center gap-1 rounded-md border border-border/60 bg-muted/20 px-2 py-1 text-[11px] text-muted-foreground transition-colors",
            "hover:bg-muted/40 hover:text-foreground",
            "disabled:cursor-not-allowed disabled:opacity-60",
          )}
        >
          {repairing ? (
            <Loader2 className="h-3 w-3 animate-spin" />
          ) : (
            <Wrench className="h-3 w-3" />
          )}
          {repairing ? "Repairing…" : "Repair permissions"}
        </button>
      </div>
    </div>
  );
}
