import { describe, expect, it } from "vitest";

import {
  compatibleSeries,
  formatTimeValue,
  normalizeResult,
  selectChartSpec,
  seriesKey,
  toChartRows,
  toEpochMs,
} from "@/lib/genie-chart";
import type { GenieQueryResult } from "@/lib/types";

function result(columns: Array<[string, string]>, rows: unknown[][], truncated = false): GenieQueryResult {
  return { columns: columns.map(([name, type]) => ({ name, type })), rows, truncated };
}

describe("selectChartSpec", () => {
  it("returns unavailable when there are no columns (result not ready)", () => {
    expect(selectChartSpec(result([], []))).toEqual({ kind: "unavailable" });
  });

  it("returns empty when columns exist but there are no rows", () => {
    expect(selectChartSpec(result([["n", "LONG"]], []))).toEqual({ kind: "empty" });
  });

  it.each(["TIMESTAMP", "timestamp_ntz", "TIMESTAMP_LTZ"])("treats %s + numeric as a line chart", (type) => {
    const spec = selectChartSpec(result([["ts", type], ["v", "DOUBLE"]], [["2026-11-18T08:00:00Z", "1.5"]]));
    expect(spec).toEqual({ kind: "line", x: 0, y: [1], timeKind: "timestamp" });
  });

  it("marks DATE columns as date-kind lines", () => {
    const spec = selectChartSpec(result([["day", "DATE"], ["n", "BIGINT"]], [["2026-11-18", "4"]]));
    expect(spec).toEqual({ kind: "line", x: 0, y: [1], timeKind: "date" });
  });

  it("detects ISO strings in STRING columns as time (date-only vs timestamp)", () => {
    const dates = selectChartSpec(result([["d", "STRING"], ["n", "INT"]], [["2026-11-17", "1"], ["2026-11-18", "2"]]));
    expect(dates).toMatchObject({ kind: "line", timeKind: "date" });
    const stamps = selectChartSpec(result([["t", "STRING"], ["n", "INT"]], [["2026-11-18T08:00:00Z", "1"]]));
    expect(stamps).toMatchObject({ kind: "line", timeKind: "timestamp" });
  });

  it.each(["INT", "BIGINT", "LONG", "SHORT", "FLOAT", "DOUBLE", "DECIMAL(10,2)", "decimal"])(
    "recognises %s as numeric for a 2-column bar",
    (type) => {
      expect(selectChartSpec(result([["station", "STRING"], ["n", type]], [["MILL", "3"]]))).toEqual({
        kind: "bar",
        x: 0,
        y: 1,
      });
    },
  );

  it("supports the numeric column first in a 2-column bar", () => {
    expect(selectChartSpec(result([["n", "LONG"], ["station", "STRING"]], [["3", "MILL"]]))).toEqual({
      kind: "bar",
      x: 1,
      y: 0,
    });
  });

  it("falls back to a table for long format (time + label + value)", () => {
    const spec = selectChartSpec(
      result([["ts", "TIMESTAMP"], ["sensor", "STRING"], ["v", "DOUBLE"]], [["2026-11-18T08:00:00Z", "a", "1"]]),
    );
    expect(spec).toEqual({ kind: "table" });
  });

  it("uses a table for all-string, all-numeric or wide categorical results", () => {
    expect(selectChartSpec(result([["a", "STRING"], ["b", "STRING"]], [["x", "y"]]))).toEqual({ kind: "table" });
    expect(selectChartSpec(result([["a", "INT"], ["b", "INT"]], [["1", "2"]]))).toEqual({ kind: "table" });
    expect(
      selectChartSpec(result([["a", "STRING"], ["b", "STRING"], ["n", "INT"]], [["x", "y", "1"]])),
    ).toEqual({ kind: "table" });
  });

  it("does not treat a STRING column with nulls only as time", () => {
    expect(selectChartSpec(result([["t", "STRING"], ["n", "INT"]], [[null, "1"]]))).toEqual({
      kind: "bar",
      x: 0,
      y: 1,
    });
  });

  it("caps line series and keeps duplicate column names distinct by index", () => {
    const cols: Array<[string, string]> = [["ts", "TIMESTAMP"], ...Array.from({ length: 8 }, (): [string, string] => ["v", "DOUBLE"])];
    const spec = selectChartSpec(result(cols, [["2026-11-18T08:00:00Z", ...Array(8).fill("1")]]));
    expect(spec).toEqual({ kind: "line", x: 0, y: [1, 2, 3, 4, 5, 6], timeKind: "timestamp" });
  });
});

describe("normalizeResult", () => {
  it("fills null names/types and drops non-array rows", () => {
    const normalized = normalizeResult({
      columns: [{ name: null, type: null }, { name: "n" }],
      rows: [["a", "1"], "bad", null],
      truncated: "yes",
    });
    expect(normalized).toEqual({
      columns: [{ name: "col_0", type: "" }, { name: "n", type: "" }],
      rows: [["a", "1"]],
      truncated: false,
    });
  });

  it("tolerates a missing payload", () => {
    expect(normalizeResult(null)).toEqual({ columns: [], rows: [], truncated: false });
  });
});

describe("toChartRows", () => {
  it("parses numeric strings, keeps nulls and sorts line rows by time", () => {
    const r = result([["ts", "TIMESTAMP"], ["v", "DOUBLE"]], [
      ["2026-11-18T08:10:00Z", "3.5"],
      ["2026-11-18T08:00:00Z", null],
      ["not-a-date", "9"],
    ]);
    const rows = toChartRows(r, selectChartSpec(r));
    expect(rows).toEqual([
      { [seriesKey(0)]: Date.parse("2026-11-18T08:00:00Z"), [seriesKey(1)]: null },
      { [seriesKey(0)]: Date.parse("2026-11-18T08:10:00Z"), [seriesKey(1)]: 3.5 },
    ]);
  });

  it("does not mutate the input rows", () => {
    const rows = [["2026-11-18T08:10:00Z", "1"], ["2026-11-18T08:00:00Z", "2"]];
    const r = result([["ts", "TIMESTAMP"], ["v", "INT"]], rows);
    toChartRows(r, selectChartSpec(r));
    expect(rows[0][0]).toBe("2026-11-18T08:10:00Z");
  });

  it("returns nothing for table specs", () => {
    expect(toChartRows(result([["a", "STRING"]], [["x"]]), { kind: "table" })).toEqual([]);
  });
});

describe("compatibleSeries", () => {
  it("keeps all-negative series", () => {
    const rows = [{ c1: -5, c2: -4 }];
    expect(compatibleSeries([1, 2], rows)).toEqual([1, 2]);
  });

  it("drops series on a much smaller scale", () => {
    const rows = [{ c1: 1000, c2: 2 }];
    expect(compatibleSeries([1, 2], rows)).toEqual([1]);
  });

  it("keeps every series when all values are zero", () => {
    expect(compatibleSeries([1, 2], [{ c1: 0, c2: 0 }])).toEqual([1, 2]);
  });
});

describe("dates", () => {
  it("reads date-only strings as UTC midnight and formats them without a time in UTC", () => {
    const ms = toEpochMs("2026-11-18");
    expect(ms).toBe(Date.UTC(2026, 10, 18));
    const label = formatTimeValue(ms as number, "date");
    expect(label).toContain("18");
    expect(label).not.toMatch(/\d{1,2}:\d{2}/);
  });
});
