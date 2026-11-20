import { useCallback, useEffect, useMemo, useState } from "react";

import { useSensorContext } from "@/contexts/sensor-context";
import { api } from "@/lib/api";
import { decisionsFor, latestSpindleAlert, type CellDecisions } from "@/lib/manufacturing-story";
import type { AlertRecord, AlertResponseAction } from "@/lib/types";

export type EpisodeAction = AlertResponseAction | "acknowledge";

export interface SpindleEpisode {
  /** Latest Spindle 3 alert episode, open or cleared. */
  alert: AlertRecord | null;
  decisions: CellDecisions;
  pending: EpisodeAction | null;
  error: string | null;
  /** Resolves true once the decision is saved (or was already saved). */
  record: (action: AlertResponseAction) => Promise<boolean>;
  acknowledge: () => Promise<boolean>;
}

/** Latest Spindle 3 episode plus the operator decisions recorded against it. */
export function useSpindleEpisode(alerts: AlertRecord[]): SpindleEpisode {
  const { responses, applyAlertResponses, applyAlertEvent, applyProduction } = useSensorContext();
  const [cleared, setCleared] = useState<AlertRecord[]>([]);
  const [pending, setPending] = useState<EpisodeAction | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    api
      .listAlerts({ industry: "manufacturing", status: "cleared" })
      .then((body) => {
        if (!cancelled) setCleared(body.alerts ?? []);
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
    };
  }, []);

  const alert = useMemo(() => {
    const live = new Set(alerts.map((a) => a.id));
    return latestSpindleAlert([...alerts, ...cleared.filter((a) => !live.has(a.id))]);
  }, [alerts, cleared]);

  const alertId = alert?.id;
  const openedAt = alert?.opened_at;
  useEffect(() => {
    if (!alertId) return;
    let cancelled = false;
    api
      .listAlertResponses(alertId)
      .then((body) => {
        if (!cancelled) applyAlertResponses(body.responses ?? []);
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
    };
  }, [alertId, openedAt, applyAlertResponses]);

  const record = useCallback(
    async (action: AlertResponseAction) => {
      if (!alertId) return false;
      setPending(action);
      setError(null);
      try {
        const event = await api.recordAlertResponse(alertId, action);
        applyAlertResponses([event.response]);
        if (event.production) applyProduction(event.production);
        if (event.production_error) setError(`Decision saved, but the cell was not stopped: ${event.production_error}`);
        return true;
      } catch (err: unknown) {
        setError(err instanceof Error ? err.message : "Decision was not recorded");
        return false;
      } finally {
        setPending(null);
      }
    },
    [alertId, applyAlertResponses, applyProduction],
  );

  const acknowledge = useCallback(async () => {
    if (!alertId) return false;
    setPending("acknowledge");
    setError(null);
    try {
      applyAlertEvent(await api.acknowledgeAlert(alertId));
      return true;
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Acknowledge failed");
      return false;
    } finally {
      setPending(null);
    }
  }, [alertId, applyAlertEvent]);

  const decisions = useMemo(
    () => decisionsFor(alert, alertId ? responses[alertId] : undefined),
    [alert, alertId, responses],
  );

  return { alert, decisions, pending, error, record, acknowledge };
}
