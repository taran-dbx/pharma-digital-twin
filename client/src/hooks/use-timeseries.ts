import { useCallback, useEffect, useRef, useState } from "react";
import { api } from "@/lib/api";

export type TimeseriesSource = "lakebase" | "dbsql";

interface UseTimeseriesOptions {
  /** Active pipeline id — required for the DBSQL path. */
  pipelineId?: string;
  /** Selected range in minutes.  ``0`` is treated as "live" (5 min). */
  rangeMinutes: number;
  /** Industry filter (Lakebase only). */
  industry?: string | null;
  /**
   * Which compute plane this query targets.  Fixed and explicit — the hook
   * never swaps sources on its own.  ``"lakebase"`` queries the hot tier,
   * ``"dbsql"`` queries the cold-tier Delta via Databricks SQL.
   */
  source: TimeseriesSource;
  /** Poll frequency in seconds; ``0`` disables background refresh. */
  pollSeconds?: number;
  /** ``false`` short-circuits the hook — useful before a pipeline is selected. */
  enabled?: boolean;
}

interface UseTimeseriesResult {
  rows: Record<string, unknown>[];
  source: TimeseriesSource;
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
  /** Epoch ms of the last successful fetch; ``null`` until the first success. */
  lastRefreshAt: number | null;
}

/**
 * Fetch time-series data from a single, explicit compute plane.  The plane is
 * fixed by the caller (``source``): Lakebase for the hot tier, Databricks SQL
 * for the cold tier.  There is no cross-source auto-routing — a section that
 * asks for Lakebase always gets Lakebase, and a section that asks for DBSQL
 * always gets DBSQL, regardless of the requested range.  Re-fetches when any
 * dependency changes; optionally polls on a fixed cadence.
 */
export function useTimeseries({
  pipelineId,
  rangeMinutes,
  industry = null,
  source,
  pollSeconds = 30,
  enabled = true,
}: UseTimeseriesOptions): UseTimeseriesResult {
  const [rows, setRows] = useState<Record<string, unknown>[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lastRefreshAt, setLastRefreshAt] = useState<number | null>(null);
  const cancelRef = useRef(false);

  const fetchOnce = useCallback(async () => {
    if (!enabled) return;
    setLoading(true);
    setError(null);
    try {
      const minutes = rangeMinutes === 0 ? 5 : rangeMinutes;
      if (source === "dbsql" && pipelineId) {
        const result = await api.getDbsqlTimeseries(pipelineId, minutes);
        if (!cancelRef.current) setRows((result.rows ?? []) as Record<string, unknown>[]);
      } else {
        const result = await api.getTimeseries({ minutes, industry, pipelineId });
        if (!cancelRef.current) setRows(result.rows ?? []);
      }
      if (!cancelRef.current) setLastRefreshAt(Date.now());
    } catch (err: unknown) {
      if (!cancelRef.current) {
        setError(err instanceof Error ? err.message : "Unknown error");
      }
    } finally {
      if (!cancelRef.current) setLoading(false);
    }
  }, [enabled, rangeMinutes, source, pipelineId, industry]);

  useEffect(() => {
    cancelRef.current = false;
    fetchOnce();
    if (!enabled || pollSeconds <= 0) {
      return () => {
        cancelRef.current = true;
      };
    }
    const interval = setInterval(fetchOnce, pollSeconds * 1000);
    return () => {
      cancelRef.current = true;
      clearInterval(interval);
    };
  }, [fetchOnce, enabled, pollSeconds]);

  return { rows, source, loading, error, refresh: fetchOnce, lastRefreshAt };
}
