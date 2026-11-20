/**
 * Pure chart-type selection for Genie query results.
 *
 * Rules (first match wins), after {@link normalizeResult}:
 *   - no columns                                      → "unavailable" (result not ready)
 *   - columns but no rows                             → "empty"
 *   - one time-like column, all other columns numeric → "line" (x = time, y = numerics)
 *     (a time column plus any extra non-numeric column is "long format" → table)
 *   - exactly one categorical + one numeric column    → "bar"
 *   - anything else                                   → "table"
 *
 * Columns are addressed by index so duplicate names are safe. No React/DOM.
 */
import type { GenieQueryResult, GenieResultColumn } from "@/lib/types";

/** Maximum line series plotted at once; the table keeps every column. */
export const MAX_LINE_SERIES = 6;
/** A series whose magnitude is this many times smaller than the peak is not co-plotted. */
export const MIXED_SCALE_RATIO = 8;

export type TimeKind = "date" | "timestamp";

export type GenieChartSpec =
  | { kind: "unavailable" }
  | { kind: "empty" }
  | { kind: "table" }
  | { kind: "line"; x: number; y: number[]; timeKind: TimeKind }
  | { kind: "bar"; x: number; y: number };

export type GenieChartRow = Record<string, string | number | null>;

const NUMERIC_TYPE = /^(tinyint|smallint|int|integer|bigint|long|short|byte|float|double|real|decimal|numeric)\b/;
const DATE_TYPE = /^date\b/;
const TIMESTAMP_TYPE = /^(timestamp|timestamp_ntz|timestamp_ltz|datetime)\b/;
const STRING_TYPE = /^(string|varchar|char|text)\b/;
const ISO_DATE_ONLY = /^\d{4}-\d{2}-\d{2}$/;
const ISO_DATE_PREFIX = /^\d{4}-\d{2}-\d{2}/;

export function normType(type: unknown): string {
  return String(type ?? "").trim().toLowerCase();
}

export function isNumericType(type: unknown): boolean {
  return NUMERIC_TYPE.test(normType(type));
}

/** Coerce an untrusted result into well-formed columns/rows (never mutates input). */
export function normalizeResult(raw: unknown): GenieQueryResult {
  const record = (raw ?? {}) as Partial<Record<keyof GenieQueryResult, unknown>>;
  const rawColumns = Array.isArray(record.columns) ? record.columns : [];
  const columns: GenieResultColumn[] = rawColumns.map((col, i) => {
    const c = (col ?? {}) as { name?: unknown; type?: unknown };
    const name = c.name === null || c.name === undefined || c.name === "" ? `col_${i}` : String(c.name);
    return { name, type: String(c.type ?? "") };
  });
  const rows = Array.isArray(record.rows)
    ? record.rows.filter((row): row is unknown[] => Array.isArray(row))
    : [];
  return { columns, rows, truncated: record.truncated === true };
}

function columnValues(rows: readonly unknown[][], index: number): unknown[] {
  return rows.map((row) => row[index]).filter((v) => v !== null && v !== undefined && v !== "");
}

/** "date" / "timestamp" when the column is time-like, else null. */
export function timeKindOf(column: GenieResultColumn, rows: readonly unknown[][], index: number): TimeKind | null {
  const type = normType(column.type);
  if (DATE_TYPE.test(type)) return "date";
  if (TIMESTAMP_TYPE.test(type)) return "timestamp";
  if (!STRING_TYPE.test(type)) return null;
  const values = columnValues(rows, index);
  const parseable =
    values.length > 0 &&
    values.every((v) => typeof v === "string" && ISO_DATE_PREFIX.test(v) && !Number.isNaN(Date.parse(v)));
  if (!parseable) return null;
  return values.every((v) => ISO_DATE_ONLY.test(String(v))) ? "date" : "timestamp";
}

export function selectChartSpec(result: GenieQueryResult): GenieChartSpec {
  const { columns, rows } = result;
  if (columns.length === 0) return { kind: "unavailable" };
  if (rows.length === 0) return { kind: "empty" };

  const indexes = columns.map((_, i) => i);
  const numeric = indexes.filter((i) => isNumericType(columns[i].type));
  const timeIndex = indexes.find((i) => !numeric.includes(i) && timeKindOf(columns[i], rows, i) !== null);

  if (timeIndex !== undefined) {
    const others = indexes.filter((i) => i !== timeIndex);
    const allNumeric = others.length > 0 && others.every((i) => numeric.includes(i));
    if (!allNumeric) return { kind: "table" }; // long format (time + series label) → table
    const timeKind = timeKindOf(columns[timeIndex], rows, timeIndex) ?? "timestamp";
    return { kind: "line", x: timeIndex, y: others.slice(0, MAX_LINE_SERIES), timeKind };
  }

  if (columns.length === 2 && numeric.length === 1) {
    const x = numeric[0] === 0 ? 1 : 0;
    return { kind: "bar", x, y: numeric[0] };
  }

  return { kind: "table" };
}

/** Recharts dataKey for a column index (index-keyed so duplicate names are safe). */
export function seriesKey(index: number): string {
  return `c${index}`;
}

export function toNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const n = typeof value === "number" ? value : Number(value);
  return Number.isFinite(n) ? n : null;
}

/** Epoch ms; date-only strings are read as UTC midnight. */
export function toEpochMs(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  const text = String(value);
  const ms = Date.parse(ISO_DATE_ONLY.test(text) ? `${text}T00:00:00Z` : text);
  return Number.isNaN(ms) ? null : ms;
}

/**
 * Project raw rows into recharts-ready records keyed by {@link seriesKey}.
 * Line data is sorted by time; rows with an unparseable x are dropped.
 */
export function toChartRows(result: GenieQueryResult, spec: GenieChartSpec): GenieChartRow[] {
  if (spec.kind !== "line" && spec.kind !== "bar") return [];
  const yIndexes = spec.kind === "line" ? spec.y : [spec.y];
  const projected = result.rows.map((row): GenieChartRow | null => {
    const x = spec.kind === "line" ? toEpochMs(row[spec.x]) : String(row[spec.x] ?? "—");
    if (x === null) return null;
    const entries = yIndexes.map((i) => [seriesKey(i), toNumber(row[i])] as const);
    return { [seriesKey(spec.x)]: x, ...Object.fromEntries(entries) };
  });
  const rows = projected.filter((row): row is GenieChartRow => row !== null);
  return spec.kind === "line"
    ? [...rows].sort((a, b) => Number(a[seriesKey(spec.x)]) - Number(b[seriesKey(spec.x)]))
    : rows;
}

/**
 * Series that share a scale with the largest-magnitude series. Uses absolute
 * values, so all-negative series are kept; all-zero/empty data keeps the first.
 */
export function compatibleSeries(yIndexes: readonly number[], rows: readonly GenieChartRow[]): number[] {
  if (yIndexes.length <= 1) return [...yIndexes];
  const ranked = yIndexes.map((i) => ({
    i,
    mag: Math.max(0, ...rows.map((row) => Math.abs(Number(row[seriesKey(i)] ?? 0)) || 0)),
  }));
  const peak = Math.max(...ranked.map((r) => r.mag));
  if (peak === 0) return [...yIndexes];
  return ranked.filter((r) => r.mag > 0 && peak / r.mag <= MIXED_SCALE_RATIO).map((r) => r.i);
}

/** Axis/tooltip label for a time value. DATE columns render in UTC with no time. */
export function formatTimeValue(ms: number, timeKind: TimeKind): string {
  const date = new Date(ms);
  if (timeKind === "date") {
    return date.toLocaleDateString(undefined, { timeZone: "UTC", year: "numeric", month: "short", day: "numeric" });
  }
  return date.toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
}

/** Stringify a raw cell for the table view. */
export function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}
