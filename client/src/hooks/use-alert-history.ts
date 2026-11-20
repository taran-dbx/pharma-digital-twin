import { useEffect, useState } from "react";

import { api } from "@/lib/api";
import type { AlertHistoryResponse } from "@/lib/types";

const POLL_MS = 20_000;

/**
 * Alert episodes and production stops for the last `hours`, rebuilt from the
 * event log. `changeKey` refetches immediately when SSE reports a lifecycle
 * change, a decision or a stop, so the timeline doesn't wait for the poll.
 */
export function useAlertHistory(industry: string, changeKey: string, hours = 1) {
  const [history, setHistory] = useState<AlertHistoryResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const load = () =>
      api
        .getAlertHistory(industry, hours)
        .then((body) => {
          if (cancelled) return;
          setHistory(body);
          setError(null);
        })
        .catch((err: unknown) => {
          if (!cancelled) setError(err instanceof Error ? err.message : "Alert history unavailable");
        });
    void load();
    const id = setInterval(load, POLL_MS);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [industry, hours, changeKey]);

  return { history, error };
}
