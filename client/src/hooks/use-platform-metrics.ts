import { useEffect, useState } from "react";

import { api } from "@/lib/api";
import type { PipelineMode } from "@/lib/industries-api";
import type { LakebasePerfResponse, StreamingLatencyResponse } from "@/lib/types";

const LAKEBASE_WINDOW_MIN = 15;

export interface LakebaseHealth {
  rows_5m: number;
  latest_at: string | null;
  latency_sec: number | null;
  inserts_per_sec_recent: number;
  error?: string;
}

export interface PlatformMetrics {
  lakebase: LakebasePerfResponse | null;
  health: LakebaseHealth | null;
  latency: StreamingLatencyResponse | null;
  /** Wall clock of the last completed refresh, epoch ms. */
  refreshedAt?: number;
}

/**
 * Headline pipeline metrics: Lakebase write throughput and freshness
 * (realtime) and Spark streaming latency (realtime / micro-batch). Each source
 * fails independently; a missing one stays null.
 */
export function usePlatformMetrics({
  pipelineId,
  industry,
  mode,
  refreshSec,
  nonce,
}: {
  pipelineId: string | null;
  industry: string;
  mode: PipelineMode;
  refreshSec: number;
  nonce: number;
}): PlatformMetrics {
  const [metrics, setMetrics] = useState<PlatformMetrics>({ lakebase: null, health: null, latency: null });

  useEffect(() => {
    if (!pipelineId) return;
    let cancelled = false;
    const load = async () => {
      const [lakebase, health, latency] = await Promise.all([
        mode === "realtime"
          ? api.getLakebasePerf(pipelineId, LAKEBASE_WINDOW_MIN, industry).catch(() => null)
          : Promise.resolve(null),
        mode === "realtime" ? api.getLakebaseHealth(pipelineId, industry).catch(() => null) : Promise.resolve(null),
        mode === "zerobus"
          ? Promise.resolve(null)
          : api.getStreamingLatency(pipelineId, industry, mode).catch(() => null),
      ]);
      if (cancelled) return;
      setMetrics({
        lakebase: typeof lakebase?.rows_per_sec === "number" && Array.isArray(lakebase.series) ? lakebase : null,
        health: typeof health?.rows_5m === "number" ? health : null,
        latency: typeof latency?.has_data === "boolean" && Array.isArray(latency.series) ? latency : null,
        refreshedAt: Date.now(),
      });
    };
    void load();
    if (refreshSec <= 0) {
      return () => {
        cancelled = true;
      };
    }
    const id = setInterval(load, refreshSec * 1000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [pipelineId, industry, mode, refreshSec, nonce]);

  return metrics;
}
