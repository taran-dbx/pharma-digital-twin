import { useCallback, useEffect, useState } from "react";

import { useSensorContext } from "@/contexts/sensor-context";
import { api } from "@/lib/api";
import type { ProductionState, ProductionStateValue } from "@/lib/types";
import { toEpochSeconds } from "@/lib/utils";

const POLL_MS = 15_000;
/** After a resume, how long the cell may stay quiet before it counts as running anyway. */
const RESTART_WINDOW_S = 90;

/**
 * `restarting` is the gap between a resume and the first machine reading that
 * was produced after it: the producer polls its control file once a second and
 * the reading still has to cross Kafka, Spark and SSE.
 */
export type ProductionPhase = "running" | "stopped" | "restarting";

export interface CellProduction {
  state: ProductionState | null;
  phase: ProductionPhase;
  /** When the current state began, epoch seconds; undefined if never stopped. */
  since?: number;
  pending: ProductionStateValue | null;
  error: string | null;
  stop: (reason: string, alertId?: string) => Promise<boolean>;
  resume: () => Promise<boolean>;
}

export function useProduction(
  industry: string,
  lastMachineTs: number | undefined,
  now: number,
): CellProduction {
  const { production, applyProduction } = useSensorContext();
  const [pending, setPending] = useState<ProductionStateValue | null>(null);
  const [error, setError] = useState<string | null>(null);
  const state = production[industry] ?? null;

  useEffect(() => {
    let cancelled = false;
    const poll = () =>
      api
        .getProduction(industry)
        .then((next) => {
          if (!cancelled) applyProduction(next);
        })
        .catch(() => undefined);
    void poll();
    const id = setInterval(poll, POLL_MS);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [industry, applyProduction]);

  const change = useCallback(
    async (target: ProductionStateValue, context?: { reason?: string; alert_id?: string }) => {
      setPending(target);
      setError(null);
      try {
        applyProduction(await api.setProduction(industry, target, context));
        return true;
      } catch (err: unknown) {
        const verb = target === "stopped" ? "stopped" : "resumed";
        setError(err instanceof Error ? `Production was not ${verb}: ${err.message}` : `Production was not ${verb}`);
        return false;
      } finally {
        setPending(null);
      }
    },
    [industry, applyProduction],
  );

  const stop = useCallback(
    (reason: string, alertId?: string) => change("stopped", { reason, alert_id: alertId }),
    [change],
  );
  const resume = useCallback(() => change("running"), [change]);

  const since = state?.changed_at ? toEpochSeconds(state.changed_at) : undefined;
  let phase: ProductionPhase = "running";
  if (state?.state === "stopped") {
    phase = "stopped";
  } else if (
    since !== undefined &&
    now - since < RESTART_WINDOW_S &&
    !(lastMachineTs !== undefined && lastMachineTs >= since)
  ) {
    phase = "restarting";
  }

  return { state, phase, since, pending, error, stop, resume };
}
