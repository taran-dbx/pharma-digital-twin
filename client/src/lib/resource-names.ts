/**
 * Client mirror of `src/iot_twin/core/resource_names.py`.
 *
 * Every resource name is derived from the backend's `RESOURCE_PREFIX`
 * (served as `resource_prefix` by `GET /api/app-defaults`), so this app never
 * builds a name in the upstream `iot` namespace. Deploy payloads must read the
 * prefix from app-defaults directly (no fallback); display-only UI may use
 * `useResourcePrefix()`, which shows the default until the fetch resolves.
 */
import { useEffect, useState } from "react";

import { api } from "@/lib/api";

/** Mirrors `iot_twin.core.config.DEFAULT_RESOURCE_PREFIX`. */
export const DEFAULT_RESOURCE_PREFIX = "pharma-twin";

let prefixPromise: Promise<string> | null = null;

/** Fetch (once) the backend resource prefix; falls back to the default on error. */
export function fetchResourcePrefix(): Promise<string> {
  if (!prefixPromise) {
    prefixPromise = api
      .getAppDefaults()
      .then((d) => d.resource_prefix || DEFAULT_RESOURCE_PREFIX)
      .catch(() => {
        prefixPromise = null; // retry on the next call
        return DEFAULT_RESOURCE_PREFIX;
      });
  }
  return prefixPromise;
}

/** React hook returning the resource prefix for display-only names. */
export function useResourcePrefix(): string {
  const [prefix, setPrefix] = useState(DEFAULT_RESOURCE_PREFIX);
  useEffect(() => {
    let cancelled = false;
    void fetchResourcePrefix().then((p) => {
      if (!cancelled) setPrefix(p);
    });
    return () => {
      cancelled = true;
    };
  }, []);
  return prefix;
}

/** Kafka topic `<prefix>.<industry>.<mode>`. */
export function topicName(prefix: string, industry: string, mode: string): string {
  return `${prefix}.${industry}.${mode}`;
}

/** Per-mode pipeline / job name `<prefix>-<industry>-<mode>`. */
export function pipelineName(prefix: string, industry: string, mode: string): string {
  return `${prefix}-${industry}-${mode}`;
}

/** Kafka consumer group `<prefix>-<industry>`. */
export function consumerGroup(prefix: string, industry: string): string {
  return `${prefix}-${industry}`;
}

/** Shared app secret scope `<prefix>-secrets`. */
export function sharedSecretScope(prefix: string): string {
  return `${prefix}-secrets`;
}

/** UC catalog pattern `<prefix_ident>_{name}`. */
export function catalogPattern(prefix: string): string {
  return `${prefix.replace(/-/g, "_")}_{name}`;
}
