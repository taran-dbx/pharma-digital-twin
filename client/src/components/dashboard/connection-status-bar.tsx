import { useEffect, useState } from "react";
import { cn } from "@/lib/utils";

type PillState = "connected" | "loading" | "error" | "disabled";

interface ConnectionStatusBarProps {
  sseConnected: boolean;
  lakebaseLastRefresh: number | null;
  lakebaseError: string | null;
  lakebaseLoading: boolean;
  dbsqlLastRefresh: number | null;
  dbsqlError: string | null;
  dbsqlLoading: boolean;
  /** Whether the DBSQL cold-tier plane is active (CDF streaming). */
  dbsqlActive: boolean;
  onRefreshLakebase: () => void;
  onRefreshDbsql: () => void;
  /** Hide the SSE pill (e.g. Zerobus, which has no SSE stream). */
  showSse?: boolean;
  /** Hide the Lakebase pill (e.g. Zerobus, which does not use Lakebase). */
  showLakebase?: boolean;
}

interface PillProps {
  label: string;
  state: PillState;
  lastRefresh: number | null;
  error: string | null;
  onClick?: () => void;
  /** When connected, the colour class for the dot + text accents. */
  accent: "green" | "primary" | "info";
  /** When true, pill shows a "live" indicator without a timestamp (SSE). */
  isSse?: boolean;
  now: number;
}

/**
 * Format a past timestamp as a compact "time ago" string.
 * Pure function — exported for tests.
 */
export function formatAgo(ms: number): string {
  if (!Number.isFinite(ms) || ms < 0) return "—";
  if (ms < 2_000) return "just now";
  if (ms < 60_000) return `${Math.round(ms / 1000)}s`;
  if (ms < 3_600_000) return `${Math.round(ms / 60_000)}m`;
  return `${Math.round(ms / 3_600_000)}h`;
}

const ACCENT_CLASSES: Record<
  "green" | "primary" | "info",
  { dot: string; text: string; bg: string; ring: string }
> = {
  green: {
    dot: "bg-success",
    text: "text-success",
    bg: "bg-success/10",
    ring: "ring-success/30",
  },
  primary: {
    dot: "bg-primary",
    text: "text-primary",
    bg: "bg-primary/10",
    ring: "ring-primary/30",
  },
  info: {
    dot: "bg-info",
    text: "text-info",
    bg: "bg-info/10",
    ring: "ring-info/30",
  },
};

function Pill({
  label,
  state,
  lastRefresh,
  error,
  onClick,
  accent,
  isSse,
  now,
}: PillProps) {
  const isError = state === "error";
  const isDisabled = state === "disabled";
  const isLoading = state === "loading";
  const isConnected = state === "connected";

  const tone = ACCENT_CLASSES[accent];
  const dotClass = cn(
    "h-2 w-2 rounded-full",
    isError
      ? "bg-destructive"
      : isDisabled
        ? "bg-muted-foreground/40"
        : tone.dot,
    isLoading && "animate-pulse",
  );

  const bgClass = cn(
    "bg-surface-high/40",
    isError && "bg-destructive/10 ring-destructive/30",
    !isError && !isDisabled && cn(tone.bg, tone.ring),
    isDisabled && "bg-muted/20 ring-border/30",
  );

  const textClass = cn(
    "font-ui text-[11px] font-medium",
    isError
      ? "text-destructive"
      : isDisabled
        ? "text-muted-foreground"
        : tone.text,
  );

  const relativeLabel = (() => {
    if (isDisabled) return "off";
    if (isError) return "error";
    if (isSse) {
      return isConnected ? "live" : isLoading ? "connecting" : "offline";
    }
    if (isLoading && lastRefresh === null) return "loading";
    if (lastRefresh === null) return "—";
    return formatAgo(now - lastRefresh);
  })();

  const title = (() => {
    if (error) return error;
    if (isDisabled) return `${label} disabled in current mode`;
    if (isSse) return isConnected ? "SSE live stream connected" : "SSE reconnecting";
    if (lastRefresh === null) return `${label} — no data yet`;
    return `${label} — last refresh ${new Date(lastRefresh).toLocaleTimeString()}`;
  })();

  const Wrapper = onClick && !isDisabled ? "button" : "div";

  return (
    <Wrapper
      type={Wrapper === "button" ? "button" : undefined}
      onClick={onClick && !isDisabled ? onClick : undefined}
      title={title}
      className={cn(
        "inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 ring-1 transition-colors",
        bgClass,
        Wrapper === "button" && "cursor-pointer hover:ring-2",
      )}
    >
      <span className={dotClass} aria-hidden="true" />
      <span className={textClass}>{label}</span>
      <span className="text-[10px] text-muted-foreground tabular-nums">
        {relativeLabel}
      </span>
    </Wrapper>
  );
}

/**
 * Horizontal row of three pills showing the health of the SSE, Lakebase,
 * and DBSQL data channels.  Clicking Lakebase/DBSQL triggers a manual refresh.
 */
export function ConnectionStatusBar({
  sseConnected,
  lakebaseLastRefresh,
  lakebaseError,
  lakebaseLoading,
  dbsqlLastRefresh,
  dbsqlError,
  dbsqlLoading,
  dbsqlActive,
  onRefreshLakebase,
  onRefreshDbsql,
  showSse = true,
  showLakebase = true,
}: ConnectionStatusBarProps) {
  const [now, setNow] = useState<number>(Date.now());

  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);

  // Each plane is fixed and reported independently — there is no cross-source
  // routing.  SSE is always push; Lakebase always queries the hot tier; DBSQL
  // queries the cold tier when CDF streaming is active, otherwise it's off.
  const sseState: PillState = sseConnected ? "connected" : "loading";

  const lakebaseState: PillState = lakebaseError
    ? "error"
    : lakebaseLoading && lakebaseLastRefresh === null
      ? "loading"
      : lakebaseLastRefresh !== null
        ? "connected"
        : "loading";

  const dbsqlState: PillState = !dbsqlActive
    ? "disabled"
    : dbsqlError
      ? "error"
      : dbsqlLoading && dbsqlLastRefresh === null
        ? "loading"
        : dbsqlLastRefresh !== null
          ? "connected"
          : "loading";

  return (
    <div className="flex flex-wrap items-center gap-2">
      {showSse && (
        <Pill
          label="SSE"
          state={sseState}
          lastRefresh={null}
          error={null}
          accent="green"
          isSse
          now={now}
        />
      )}
      {showLakebase && (
        <Pill
          label="Lakebase"
          state={lakebaseState}
          lastRefresh={lakebaseLastRefresh}
          error={lakebaseError}
          onClick={onRefreshLakebase}
          accent="primary"
          now={now}
        />
      )}
      <Pill
        label="DBSQL"
        state={dbsqlState}
        lastRefresh={dbsqlLastRefresh}
        error={dbsqlError}
        onClick={onRefreshDbsql}
        accent="info"
        now={now}
      />
    </div>
  );
}
