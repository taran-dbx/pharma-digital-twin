import { useEffect, useState } from "react";

import {
  getOpsSummary,
  getValueSummary,
  type AssumptionName,
  type Enveloped,
  type OpsSummary,
  type SummaryScope,
  type ValueSummary,
} from "@/lib/value-api";

const POLL_MS = 30_000;

export interface Loaded<T> {
  result: Enveloped<T> | null;
  error: string | null;
  /** True until the first answer (or error) for the current scope arrives. */
  loading: boolean;
  /** True while a refetch is in flight; the previous result stays on screen. */
  refreshing: boolean;
}

function usePolled<T>(load: (signal: AbortSignal) => Promise<Enveloped<T>>, key: string): Loaded<T> {
  const [state, setState] = useState<Loaded<T> & { key: string | null }>({
    result: null,
    error: null,
    loading: true,
    refreshing: false,
    key: null,
  });

  useEffect(() => {
    let controller = new AbortController();
    const run = () => {
      controller.abort();
      controller = new AbortController();
      const signal = controller.signal;
      setState((prev) => ({ ...prev, refreshing: true, loading: prev.key !== key && prev.result === null }));
      load(signal)
        .then((result) => {
          if (!signal.aborted) setState({ result, error: null, loading: false, refreshing: false, key });
        })
        .catch((err: unknown) => {
          if (signal.aborted) return;
          const message = err instanceof Error ? err.message : "Summary unavailable";
          setState((prev) => ({ ...prev, error: message, loading: false, refreshing: false, key }));
        });
    };
    run();
    const id = setInterval(run, POLL_MS);
    return () => {
      clearInterval(id);
      controller.abort();
    };
    // `key` captures every input of `load`.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key]);

  return { result: state.result, error: state.error, loading: state.loading, refreshing: state.refreshing };
}

export function useValueSummary(
  scope: SummaryScope,
  overrides: Partial<Record<AssumptionName, number>>,
  nonce: number,
): Loaded<ValueSummary> {
  const key = JSON.stringify([scope, overrides, nonce]);
  return usePolled((signal) => getValueSummary(scope, overrides, signal), key);
}

export function useOpsSummary(scope: SummaryScope, nonce: number): Loaded<OpsSummary> {
  const key = JSON.stringify([scope, nonce]);
  return usePolled((signal) => getOpsSummary(scope, signal), key);
}
