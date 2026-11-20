import { useEffect, useState } from "react";
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
import { formatLatency, formatNumber } from "@/lib/utils";
import type { LakebasePerfResponse } from "@/lib/types";

const LAKEBASE = "#3b82f6";
// Re-enable after Kafka partition scaling makes event-to-insert lag trustworthy.
const SHOW_INSERT_LAG = false;

const TOOLTIP_STYLE = {
  backgroundColor: "hsl(var(--card) / 0.9)",
  backdropFilter: "blur(12px)",
  border: "1px solid hsl(var(--border) / 0.5)",
  borderRadius: 8,
  fontSize: 11,
} as const;

interface LakebasePerfPanelProps {
  pipelineId: string;
  /** Lookback window in minutes (1-60). */
  minutes?: number;
  /**
   * Poll interval (seconds) governed by the dashboard refresh-interval
   * selector. ``0`` = paused (manual refresh only).
   */
  refreshSec: number;
  /**
   * Industry id — when set, resolves the deterministic industry-first
   * Lakebase branch (``iot-pipelines``) instead of the legacy per-pipeline
   * ``sink_config`` lookup. Optional for backward compat with the legacy
   * per-pipeline dashboard.
   */
  industry?: string;
}

/**
 * Default ingest-lag definition shown when the backend response omits an
 * explicit ``lag_basis`` string.  Kept verbatim so the UI always explains the
 * measurement basis.
 */
const DEFAULT_LAG_BASIS =
  "Ingest lag = now() − producer event-time, measured at Lakebase insert (p50/p95 over the window).";

/** Shown for a lag readout that cannot be computed (e.g. CDF disabled). */
const LAG_UNAVAILABLE_NOTE = "requires Lakebase Sync (CDF)";

interface BigReadoutProps {
  label: string;
  value: string;
  accent?: boolean;
  /** Small sub-line under the value (units or an unavailable note). */
  sub?: string;
  /** Hover tooltip — used to surface the lag definition. */
  title?: string;
}

function BigReadout({ label, value, accent, sub, title }: BigReadoutProps) {
  return (
    <div
      className="rounded-md bg-surface-low border border-border/50 px-3 py-2.5 text-center"
      title={title}
    >
      <p className="text-[9px] uppercase tracking-wider text-muted-foreground">
        {label}
      </p>
      <p
        className="font-mono text-2xl font-bold tabular-nums"
        style={accent ? { color: LAKEBASE } : undefined}
      >
        {value}
      </p>
      {sub && (
        <p className="text-[8px] text-muted-foreground mt-0.5 leading-tight">
          {sub}
        </p>
      )}
    </div>
  );
}

/** Resolve a lag readout to a display value + sub-line. */
function lagReadout(
  ms: number | null | undefined,
): { value: string; sub: string } {
  if (ms == null || ms <= 0) {
    return { value: "—", sub: LAG_UNAVAILABLE_NOTE };
  }
  return { value: formatLatency(ms), sub: "ms" };
}

/**
 * Section 3 readouts — big rows/sec & rows/min figures, p50/p95 insert lag,
 * and a rows/sec area chart. Polling is governed entirely by ``refreshSec``
 * (the dashboard's refresh-interval selector); SSE is unaffected.
 */
export function LakebasePerfPanel({
  pipelineId,
  minutes = 5,
  refreshSec,
  industry,
}: LakebasePerfPanelProps) {
  const [perf, setPerf] = useState<LakebasePerfResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!pipelineId) return;
    let cancelled = false;
    const fetchOnce = async () => {
      try {
        const data = await api.getLakebasePerf(pipelineId, minutes, industry);
        if (!cancelled) {
          setPerf(data);
          setError(null);
        }
      } catch (err: unknown) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Unable to load Lakebase performance");
        }
      }
    };
    fetchOnce();
    if (refreshSec <= 0) {
      return () => {
        cancelled = true;
      };
    }
    const id = setInterval(fetchOnce, refreshSec * 1000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [pipelineId, minutes, refreshSec, industry]);

  const series = (perf?.series ?? []).map((p) => ({
    t: p.t ? String(p.t).slice(11, 16) : "",
    rows_per_sec: p.rows_per_sec,
  }));

  const lagBasis = perf?.lag_basis?.trim() || DEFAULT_LAG_BASIS;
  const p50 = lagReadout(perf?.p50_insert_lag_ms);
  const p95 = lagReadout(perf?.p95_insert_lag_ms);

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <BigReadout
          label="Rows / sec"
          value={perf ? formatNumber(perf.rows_per_sec, 2) : "---"}
          accent
        />
        <BigReadout
          label="Rows / min"
          value={perf ? formatNumber(perf.rows_per_min) : "---"}
          accent
        />
        <BigReadout
          label="Insert Lag p50"
          value={SHOW_INSERT_LAG && perf ? p50.value : "---"}
          sub={SHOW_INSERT_LAG && perf ? p50.sub : undefined}
          title={lagBasis}
        />
        <BigReadout
          label="Insert Lag p95"
          value={SHOW_INSERT_LAG && perf ? p95.value : "---"}
          sub={SHOW_INSERT_LAG && perf ? p95.sub : undefined}
          title={lagBasis}
        />
      </div>

      {/* Visible lag definition — always explains the measurement basis. */}
      <p className="text-[10px] text-muted-foreground leading-snug">
        <span className="font-semibold text-foreground/70">Lag basis:</span> {lagBasis}
      </p>

      <div>
        <p className="text-[9px] text-muted-foreground font-semibold mb-1 uppercase tracking-wider">
          Rows / sec (last {minutes}m)
        </p>
        {series.length > 0 ? (
          <ResponsiveContainer width="100%" height={170}>
            <AreaChart data={series}>
              <defs>
                <linearGradient id="lbPerfFill" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={LAKEBASE} stopOpacity={0.35} />
                  <stop offset="100%" stopColor={LAKEBASE} stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border) / 0.4)" />
              <XAxis dataKey="t" stroke="hsl(var(--muted-foreground))" fontSize={9} tickLine={false} />
              <YAxis stroke="hsl(var(--muted-foreground))" fontSize={9} tickLine={false} />
              <RechartsTooltip contentStyle={TOOLTIP_STYLE} />
              <Area type="monotone" dataKey="rows_per_sec" stroke={LAKEBASE} strokeWidth={2} fill="url(#lbPerfFill)" name="rows/sec" />
            </AreaChart>
          </ResponsiveContainer>
        ) : (
          <div className="flex h-[120px] items-center justify-center rounded-md bg-surface-low border border-border/50 font-ui text-xs text-muted-foreground">
            {error ?? "Waiting for Lakebase write activity..."}
          </div>
        )}
      </div>
    </div>
  );
}
