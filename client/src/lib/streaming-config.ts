export interface TumblingWindowConfig {
  type: "tumbling";
  duration: string;
}

export interface SlidingWindowConfig {
  type: "sliding";
  duration: string;
  slide: string;
}

export type WindowConfig = TumblingWindowConfig | SlidingWindowConfig;

export interface SessionConfig {
  enabled: boolean;
  gap: string;
  timeout: string;
}

export interface ThresholdRule {
  min: number;
  max: number;
}

export interface StateOpsConfig {
  running_avg: boolean;
  min_max_tracking: boolean;
  change_detection: boolean;
  threshold_alerts: Record<string, ThresholdRule>;
}

export interface StreamingConfig {
  windows: WindowConfig[];
  session: SessionConfig;
  state_ops: StateOpsConfig;
}

export const DEFAULT_STREAMING_CONFIG: StreamingConfig = {
  windows: [{ type: "tumbling", duration: "30s" }],
  session: { enabled: false, gap: "2m", timeout: "30m" },
  state_ops: {
    running_avg: false,
    min_max_tracking: false,
    change_detection: false,
    threshold_alerts: {},
  },
};

const DURATION_RE = /^\d+[smh]$/;

export function validateStreamingConfig(config: StreamingConfig): string[] {
  const errors: string[] = [];

  if (!config) {
    errors.push("Configuration is required");
    return errors;
  }

  if (!Array.isArray(config.windows) || config.windows.length === 0) {
    errors.push("At least one window configuration is required");
  }

  for (const w of config.windows) {
    if (!w.duration || !DURATION_RE.test(w.duration)) {
      errors.push(
        `Invalid window duration: "${w.duration}". Use format like "30s", "5m", "1h"`,
      );
    }
    if (w.type === "sliding" && (!w.slide || !DURATION_RE.test(w.slide))) {
      errors.push(
        `Sliding window requires a valid slide interval: "${w.slide}"`,
      );
    }
  }

  if (config.session.enabled) {
    if (!DURATION_RE.test(config.session.gap)) {
      errors.push(`Invalid session gap: "${config.session.gap}"`);
    }
    if (!DURATION_RE.test(config.session.timeout)) {
      errors.push(`Invalid session timeout: "${config.session.timeout}"`);
    }
  }

  for (const [sensor, rule] of Object.entries(
    config.state_ops.threshold_alerts,
  )) {
    if (rule.min >= rule.max) {
      errors.push(
        `Threshold for "${sensor}": min (${rule.min}) must be less than max (${rule.max})`,
      );
    }
  }

  return errors;
}

// ---------------------------------------------------------------------------
// Tiering validation (Phase 3a) — mirrors services/lakets_service.py semantics.
// ---------------------------------------------------------------------------

const INTERVAL_UNIT_SECONDS: Record<string, number> = {
  second: 1,
  seconds: 1,
  sec: 1,
  minute: 60,
  minutes: 60,
  min: 60,
  hour: 3600,
  hours: 3600,
  day: 86400,
  days: 86400,
  week: 604800,
  weeks: 604800,
};

/**
 * Parse a Postgres interval string like ``"15 minutes"`` into seconds.
 * Returns ``null`` when the string is malformed or the unit is unknown.
 */
export function intervalToSeconds(raw: string): number | null {
  if (!raw) return null;
  const parts = raw.trim().split(/\s+/);
  if (parts.length !== 2) return null;
  const [nRaw, unit] = parts;
  const n = Number.parseFloat(nRaw);
  if (!Number.isFinite(n) || n <= 0) return null;
  const secs = INTERVAL_UNIT_SECONDS[unit.toLowerCase()];
  if (secs === undefined) return null;
  return n * secs;
}

export interface TieringConfigShape {
  hot: string;
  drop: string;
  chunk: string;
}

/**
 * Validate a tiering config.  Returns human-readable errors; empty means valid.
 * Enforces strict ordering ``chunk < hot < drop`` and warns if the hot window
 * holds fewer than four chunks.
 */
export function validateTiering(t: TieringConfigShape): string[] {
  const errors: string[] = [];

  const chunk = intervalToSeconds(t.chunk);
  const hot = intervalToSeconds(t.hot);
  const drop = intervalToSeconds(t.drop);

  if (chunk === null) errors.push(`Invalid chunk interval: "${t.chunk}"`);
  if (hot === null) errors.push(`Invalid hot interval: "${t.hot}"`);
  if (drop === null) errors.push(`Invalid drop interval: "${t.drop}"`);

  if (chunk !== null && hot !== null && chunk >= hot) {
    errors.push(
      `chunk (${t.chunk}) must be strictly less than hot (${t.hot}) — ` +
        "partitions can't be tiered before the chunk rolls.",
    );
  }
  if (hot !== null && drop !== null && hot >= drop) {
    errors.push(`hot (${t.hot}) must be strictly less than drop (${t.drop}).`);
  }
  if (chunk !== null && hot !== null && chunk < hot && hot / chunk < 4) {
    errors.push(
      `Only ${(hot / chunk).toFixed(1)} chunks fit in the hot window — ` +
        "increase hot or reduce chunk for smoother tiering.",
    );
  }

  return errors;
}
