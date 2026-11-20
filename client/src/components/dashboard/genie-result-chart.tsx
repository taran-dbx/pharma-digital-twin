/**
 * Presentational renderer for one Genie query result. Picks line / bar /
 * table via the pure `selectChartSpec`, always offers a table toggle, and
 * makes loading, error, not-ready, empty and truncated states explicit.
 */
import { useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip as RechartsTooltip,
  XAxis,
  YAxis,
} from "recharts";

import { SERIES_COLORS, TOOLTIP_STYLE, formatAxisTick } from "@/components/dashboard/historical-dbsql";
import {
  compatibleSeries,
  formatCell,
  formatTimeValue,
  normalizeResult,
  selectChartSpec,
  seriesKey,
  toChartRows,
  type GenieChartRow,
  type TimeKind,
} from "@/lib/genie-chart";
import type { GenieQueryResult } from "@/lib/types";

const CHART_HEIGHT = 180;
const AXIS_TICK = { fontSize: 10 } as const;
const CHART_MARGIN = { top: 6, right: 8, bottom: 0, left: -12 } as const;

interface GenieResultChartProps {
  result: GenieQueryResult | null;
  loading: boolean;
  error: string | null;
  onRetry?: () => void;
}

type View = "chart" | "table";

function TimeLineChart({
  data,
  result,
  x,
  y,
  timeKind,
}: {
  data: GenieChartRow[];
  result: GenieQueryResult;
  x: number;
  y: number[];
  timeKind: TimeKind;
}) {
  const series = compatibleSeries(y, data);
  return (
    <ResponsiveContainer width="100%" height={CHART_HEIGHT}>
      <LineChart data={data} margin={CHART_MARGIN}>
        <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.25} />
        <XAxis
          dataKey={seriesKey(x)}
          type="number"
          scale="time"
          domain={["dataMin", "dataMax"]}
          tick={AXIS_TICK}
          tickFormatter={(v: number) => formatTimeValue(v, timeKind)}
        />
        <YAxis tick={AXIS_TICK} tickFormatter={formatAxisTick} width={44} />
        <RechartsTooltip contentStyle={TOOLTIP_STYLE} labelFormatter={(v) => formatTimeValue(Number(v), timeKind)} />
        {series.map((i) => (
          <Line
            key={seriesKey(i)}
            type="monotone"
            dataKey={seriesKey(i)}
            name={result.columns[i].name}
            dot={false}
            strokeWidth={1.6}
            stroke={SERIES_COLORS[y.indexOf(i) % SERIES_COLORS.length]}
            isAnimationActive={false}
            connectNulls
          />
        ))}
      </LineChart>
    </ResponsiveContainer>
  );
}

function CategoryBarChart({ data, result, x, y }: { data: GenieChartRow[]; result: GenieQueryResult; x: number; y: number }) {
  return (
    <ResponsiveContainer width="100%" height={CHART_HEIGHT}>
      <BarChart data={data} margin={CHART_MARGIN}>
        <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.25} vertical={false} />
        <XAxis dataKey={seriesKey(x)} tick={AXIS_TICK} interval="preserveStartEnd" />
        <YAxis tick={AXIS_TICK} tickFormatter={formatAxisTick} width={44} />
        <RechartsTooltip contentStyle={TOOLTIP_STYLE} />
        <Bar dataKey={seriesKey(y)} name={result.columns[y].name} fill={SERIES_COLORS[0]} isAnimationActive={false} />
      </BarChart>
    </ResponsiveContainer>
  );
}

function ResultTable({ result }: { result: GenieQueryResult }) {
  return (
    <div
      className="genie-result__table"
      data-testid="genie-result-table"
      role="region"
      aria-label="Query result table"
      tabIndex={0}
    >
      <table>
        <thead>
          <tr>
            {result.columns.map((col, c) => (
              <th key={c} scope="col" title={col.type}>
                {col.name}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {result.rows.map((row, r) => (
            <tr key={r}>
              {result.columns.map((_, c) => (
                <td key={c}>{formatCell(row[c])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function RetryButton({ onRetry }: { onRetry?: () => void }) {
  if (!onRetry) return null;
  return (
    <button type="button" className="btn sm" onClick={onRetry}>
      Retry
    </button>
  );
}

export function GenieResultChart({ result: raw, loading, error, onRetry }: GenieResultChartProps) {
  const [view, setView] = useState<View>("chart");
  const result = useMemo(() => (raw ? normalizeResult(raw) : null), [raw]);
  const spec = useMemo(() => (result ? selectChartSpec(result) : null), [result]);
  const chartRows = useMemo(() => (result && spec ? toChartRows(result, spec) : []), [result, spec]);

  if (loading) {
    return (
      <p className="genie-drawer__hint" aria-busy="true" data-testid="genie-result-loading">
        Loading result…
      </p>
    );
  }
  if (error) {
    return (
      <div className="genie-drawer__error" role="alert" data-testid="genie-result-error">
        <span>Couldn’t load the query result: {error}</span>
        <RetryButton onRetry={onRetry} />
      </div>
    );
  }
  if (!result || !spec || spec.kind === "unavailable") {
    return (
      <div className="genie-drawer__hint genie-result__pending" data-testid="genie-result-unavailable">
        <span>Result not available yet.</span>
        <RetryButton onRetry={onRetry} />
      </div>
    );
  }
  if (spec.kind === "empty") {
    return (
      <p className="genie-drawer__hint" data-testid="genie-result-empty">
        The query returned no rows.
      </p>
    );
  }

  const chartable = (spec.kind === "line" || spec.kind === "bar") && chartRows.length > 0;
  const showChart = chartable && view === "chart";
  const rowCount = result.rows.length;

  return (
    <div className="genie-result" data-testid="genie-result" data-chart-kind={chartable ? spec.kind : "table"}>
      {chartable && (
        <div className="genie-result__views" role="group" aria-label="Result view">
          {(["chart", "table"] as const).map((option) => (
            <button
              key={option}
              type="button"
              className={`btn sm${view === option ? " primary" : ""}`}
              aria-pressed={view === option}
              data-testid={`genie-result-view-${option}`}
              onClick={() => setView(option)}
            >
              {option === "chart" ? "Chart" : "Table"}
            </button>
          ))}
        </div>
      )}
      {showChart && spec.kind === "line" && (
        <div data-testid="genie-result-line">
          <TimeLineChart data={chartRows} result={result} x={spec.x} y={spec.y} timeKind={spec.timeKind} />
        </div>
      )}
      {showChart && spec.kind === "bar" && (
        <div data-testid="genie-result-bar">
          <CategoryBarChart data={chartRows} result={result} x={spec.x} y={spec.y} />
        </div>
      )}
      {!showChart && <ResultTable result={result} />}
      <p className="genie-result__meta">
        {rowCount} row{rowCount === 1 ? "" : "s"}
        {result.truncated && (
          <span data-testid="genie-result-truncated"> · truncated to {rowCount} rows</span>
        )}
      </p>
    </div>
  );
}
