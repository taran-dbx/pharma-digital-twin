import { formatLatency } from "@/lib/utils";

/**
 * One latency/throughput KPI tile. Matches the compact `MiniStat` styling used
 * across the dashboard panels (surface-low card, uppercase micro-label, mono
 * tabular value). A `null`/`undefined` value renders a muted "—" so an
 * unprovisioned or idle metric never reads as a real `0`.
 */
interface LatencyKpiTileProps {
  label: string;
  value: string;
  sub?: string;
  accent?: string;
}

export function LatencyKpiTile({ label, value, sub, accent }: LatencyKpiTileProps) {
  return (
    <div className="rounded-md bg-surface-low border border-border/50 px-3 py-2">
      <p className="text-[9px] uppercase tracking-wider text-muted-foreground">{label}</p>
      <p
        className="font-mono text-lg font-semibold tabular-nums"
        style={accent ? { color: accent } : undefined}
      >
        {value}
      </p>
      {sub && <p className="text-[8px] text-muted-foreground mt-0.5 leading-tight">{sub}</p>}
    </div>
  );
}

/** Format a latency reading (ms) as a compact string, or "—" when null. */
export function fmtLatency(ms: number | null | undefined): string {
  return ms == null ? "—" : formatLatency(ms);
}

/** Format a numeric reading with fixed decimals, or "—" when null. */
export function fmtNum(value: number | null | undefined, decimals = 1): string {
  if (value == null) return "—";
  return value.toLocaleString("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

/** Format a byte count into a compact human-readable string ("—" when null). */
export function fmtBytes(bytes: number | null | undefined): string {
  if (bytes == null || bytes <= 0) return bytes === 0 ? "0 B" : "—";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const exp = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const value = bytes / Math.pow(1024, exp);
  return `${value.toFixed(exp === 0 ? 0 : 1)} ${units[exp]}`;
}

/** Format a "seconds since" freshness reading, or "—" when null. */
export function fmtFreshness(seconds: number | null | undefined): string {
  if (seconds == null) return "—";
  if (seconds < 1) return "<1s";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  return `${(seconds / 3600).toFixed(1)}h`;
}
