import { useId, useMemo } from "react";
import {
  Area,
  AreaChart,
  ReferenceArea,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import type { SensorHistoryPoint } from "@/contexts/sensor-context";
import type { IndustryMaintenanceEvent } from "@/lib/industries-api";
import type { SpindleHeatState } from "@/lib/manufacturing-scenario";
import {
  HEALTH_LABEL,
  REFERENCE_CONTEXT,
  RESPONSE_STEPS,
  STATION_INFO,
  decisionLabel,
  formatCountdown,
  formatDeviceTime,
  healthTone,
  type CellDecisions,
  type CellStationId,
  type HealthState,
  type SpindleLimit,
} from "@/lib/manufacturing-story";
import type { AlertRecord } from "@/lib/types";
import { formatNumber } from "@/lib/utils";

const TREND_WINDOW_MS = 3 * 60_000;
const SCALE_MIN = 30;

export interface LiveSignal {
  value?: number;
  unit: string;
  /** Device event time, epoch seconds. */
  timestamp?: number;
}

export interface SensorTile {
  name: string;
  label: string;
  unit: string;
  decimals: number;
  value?: number;
  overLimit: boolean;
  /** Approved limit from the catalog, when one exists. */
  limitText?: string;
  history: SensorHistoryPoint[];
}

export interface CellStationCardProps {
  station: CellStationId;
  spindle: LiveSignal & { health: HealthState; history: SensorHistoryPoint[] };
  limit: SpindleLimit | null;
  alert: AlertRecord | null;
  heatEvent: SpindleHeatState;
  lastService: IndustryMaintenanceEvent | null;
  pack: { speed: LiveSignal; current: LiveSignal; health: HealthState };
  tiles: Record<CellStationId, SensorTile[]>;
  decisions: CellDecisions;
  /** A hold is in force; false once a resume has released it. */
  batchHeld: boolean;
}

function Field({ label, children, testId }: { label: string; children: React.ReactNode; testId?: string }) {
  return (
    <div className="cell-card__field" data-testid={testId}>
      <span>{label}</span>
      <b>{children}</b>
    </div>
  );
}

function StatusChip({ health }: { health: HealthState }) {
  return (
    <span className={`cell-card__status tone-${healthTone(health)}`} data-testid="station-status">
      <i aria-hidden="true" />
      {HEALTH_LABEL[health]}
    </span>
  );
}

export function heatEventText(state: SpindleHeatState): string {
  switch (state.phase) {
    case "normal":
      return `next in ${formatCountdown(state.secondsToNextEvent)}`;
    case "heating":
      return "in progress · heating";
    case "overheat":
      return "in progress · running hot";
    case "cooling":
      return "in progress · cooling down";
    default: {
      const _exhaustive: never = state.phase;
      return _exhaustive;
    }
  }
}

export function Sparkline({ points, over }: { points: SensorHistoryPoint[]; over: boolean }) {
  const path = useMemo(() => {
    const last = points[points.length - 1];
    if (!last) return "";
    const recent = points.filter((p) => p.t >= last.t - 90_000);
    if (recent.length < 2) return "";
    const lo = Math.min(...recent.map((p) => p.v));
    const hi = Math.max(...recent.map((p) => p.v));
    const span = hi - lo || 1;
    const t0 = recent[0].t;
    const dt = last.t - t0 || 1;
    return recent
      .map((p, i) => `${i === 0 ? "M" : "L"}${(((p.t - t0) / dt) * 100).toFixed(1)},${(22 - ((p.v - lo) / span) * 20).toFixed(1)}`)
      .join(" ");
  }, [points]);
  return (
    <svg className={`cell-spark${over ? " is-over" : ""}`} viewBox="0 0 100 24" preserveAspectRatio="none" aria-hidden="true">
      {path && <path d={path} vectorEffect="non-scaling-stroke" />}
    </svg>
  );
}

function SensorTiles({ tiles }: { tiles: SensorTile[] }) {
  if (tiles.length === 0) return null;
  return (
    <div className="cell-tiles" data-testid="station-sensors">
      {tiles.map((tile) => (
        <div className={`cell-tile${tile.overLimit ? " is-over" : ""}`} key={tile.name}>
          <span>{tile.label}</span>
          <b className="tnum">
            {tile.value === undefined ? "—" : formatNumber(tile.value, tile.decimals)}
            <small> {tile.unit}</small>
          </b>
          <Sparkline points={tile.history} over={tile.overLimit} />
          <em>{tile.limitText ?? "No approved limit"}</em>
        </div>
      ))}
    </div>
  );
}

/** Horizontal thermal scale: normal, approaching (hysteresis band) and over-limit zones. */
function ThermalScale({ value, limit }: { value?: number; limit: SpindleLimit | null }) {
  if (!limit) return null;
  const max = limit.threshold + 20;
  const pos = (v: number) => `${(Math.min(max, Math.max(SCALE_MIN, v)) - SCALE_MIN) / (max - SCALE_MIN) * 100}%`;
  const clearAt = limit.threshold - limit.hysteresis;
  return (
    <div className="cell-scale" data-testid="thermal-scale" aria-hidden="true">
      <div className="cell-scale__track">
        <span className="cell-scale__watch" style={{ left: pos(clearAt), width: `calc(${pos(limit.threshold)} - ${pos(clearAt)})` }} />
        <span className="cell-scale__over" style={{ left: pos(limit.threshold) }} />
        {value !== undefined && <span className="cell-scale__now" style={{ left: pos(value) }} />}
      </div>
      <div className="cell-scale__ticks tnum">
        <span style={{ left: pos(SCALE_MIN) }}>{SCALE_MIN}</span>
        <span style={{ left: pos(clearAt) }}>clears {formatNumber(clearAt, 0)}</span>
        <span style={{ left: pos(limit.threshold) }} className="is-limit">
          limit {formatNumber(limit.threshold, 0)}
        </span>
        <span style={{ left: pos(max) }}>{max} °C</span>
      </div>
    </div>
  );
}

function SpindleTrend({ history, limit }: { history: SensorHistoryPoint[]; limit: SpindleLimit | null }) {
  const gradientId = useId().replace(/:/g, "");
  const data = useMemo(() => {
    const last = history[history.length - 1];
    if (!last) return [];
    return history.filter((p) => p.t >= last.t - TREND_WINDOW_MS);
  }, [history]);

  if (data.length < 2) {
    return <div className="cell-card__trend-empty">The trend appears as live readings arrive.</div>;
  }
  const values = data.map((p) => p.v);
  const lo = Math.min(...values, limit ? limit.threshold - 25 : Infinity);
  const hi = Math.max(...values, limit ? limit.threshold + 8 : -Infinity);
  return (
    <div className="cell-card__trend" data-testid="spindle-trend">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={data} margin={{ top: 8, right: 6, bottom: 0, left: -18 }}>
          <defs>
            <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="var(--cell-trend)" stopOpacity={0.32} />
              <stop offset="100%" stopColor="var(--cell-trend)" stopOpacity={0} />
            </linearGradient>
          </defs>
          <XAxis
            dataKey="t"
            type="number"
            domain={["dataMin", "dataMax"]}
            tickFormatter={(t: number) => formatDeviceTime(t / 1000).slice(0, 8)}
            tick={{ fontSize: 10, fill: "var(--db-muted-foreground)" }}
            stroke="var(--db-border)"
            minTickGap={48}
          />
          <YAxis
            domain={[Math.floor(lo), Math.ceil(hi)]}
            tick={{ fontSize: 10, fill: "var(--db-muted-foreground)" }}
            stroke="var(--db-border)"
            width={44}
          />
          <Tooltip
            contentStyle={{
              background: "var(--db-popover)",
              border: "1px solid var(--db-border)",
              borderRadius: 6,
              fontSize: 12,
            }}
            labelFormatter={(t: number) => `device ${formatDeviceTime(t / 1000)}`}
            formatter={(v: number) => [`${formatNumber(v, 1)} °C`, "Spindle 3"]}
          />
          {limit && (
            <ReferenceArea
              y1={limit.threshold - limit.hysteresis}
              y2={limit.threshold}
              fill="var(--cell-warn)"
              fillOpacity={0.1}
              strokeOpacity={0}
            />
          )}
          {limit && (
            <ReferenceLine
              y={limit.threshold}
              stroke="var(--cell-crit)"
              strokeDasharray="5 4"
              label={{
                value: `limit ${limit.threshold} °C`,
                position: "insideTopRight",
                fontSize: 10,
                fill: "var(--cell-crit)",
              }}
            />
          )}
          <Area
            type="monotone"
            dataKey="v"
            stroke="var(--cell-trend)"
            strokeWidth={1.8}
            fill={`url(#${gradientId})`}
            dot={false}
            isAnimationActive={false}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

function CardHead({ station, health, asset }: { station: CellStationId; health: HealthState; asset: string }) {
  const info = STATION_INFO[station];
  return (
    <div className="cell-card__head">
      <div>
        <span className="cell-card__code">{info.code}</span>
        <h3>{info.name}</h3>
        <span className="cell-card__asset">{asset}</span>
      </div>
      <StatusChip health={health} />
    </div>
  );
}

function DecisionNotes({
  station,
  decisions,
  batchHeld,
}: {
  station: CellStationId;
  decisions: CellDecisions;
  batchHeld: boolean;
}) {
  const recorded = RESPONSE_STEPS.filter((step) => step.station === station && decisions[step.action]);
  if (recorded.length === 0) return null;
  return (
    <div className="cell-card__decisions" data-testid="station-decisions">
      {recorded.map((step) => (
        <div key={step.action}>
          <b>{decisionLabel(step, batchHeld)}</b>
          <span>
            recorded {formatDeviceTime(Date.parse(decisions[step.action]!.created_at) / 1000).slice(0, 8)}
          </span>
        </div>
      ))}
    </div>
  );
}

function SpindleCard({
  spindle,
  limit,
  alert,
  heatEvent,
  lastService,
  tiles,
  decisions,
  batchHeld,
}: CellStationCardProps) {
  const tone = healthTone(spindle.health);
  return (
    <>
      <div className="cell-card__main">
        <CardHead station="mill" health={spindle.health} asset={`${STATION_INFO.mill.asset} · Detroit Plant`} />
        <div className={`cell-card__thermal tone-${tone}`}>
          <div className="cell-card__now" data-testid="spindle-temperature">
            <span className="tnum">{spindle.value === undefined ? "—" : formatNumber(spindle.value, 1)}</span>
            <small>{spindle.unit}</small>
            <em>Spindle temperature</em>
          </div>
          <div className="cell-card__limit" data-testid="operating-limit">
            <span className="tnum">{limit ? formatNumber(limit.threshold, 0) : "—"}</span>
            <small>°C</small>
            <em>Approved operating limit</em>
          </div>
        </div>
        <ThermalScale value={spindle.value} limit={limit} />
        <SpindleTrend history={spindle.history} limit={limit} />
        <SensorTiles tiles={tiles.mill.filter((t) => t.name !== "cnc_1_spindle_temp")} />
      </div>

      <div className="cell-card__side">
        <div className="cell-card__fields">
          <Field label="Device event time" testId="device-event-time">
            {formatDeviceTime(spindle.timestamp)}
          </Field>
          {alert && limit?.action && (
            <Field label="Action" testId="spindle-action">
              <span className="cell-card__action">{limit.action}</span>
            </Field>
          )}
          <Field label="Prepared heat event" testId="heat-event">
            {heatEventText(heatEvent)}
          </Field>
        </div>
        <DecisionNotes station="mill" decisions={decisions} batchHeld={batchHeld} />
        <div className="cell-card__context" aria-label={REFERENCE_CONTEXT.label}>
          <span className="cell-card__context-label">{REFERENCE_CONTEXT.label}</span>
          <Field label="Work order">{REFERENCE_CONTEXT.workOrder}</Field>
          <Field label="Batch">{REFERENCE_CONTEXT.batch}</Field>
          <Field label="Upstream">{REFERENCE_CONTEXT.upstream}</Field>
          <Field label="Downstream">{REFERENCE_CONTEXT.downstream}</Field>
          <Field label="Last service">
            {lastService ? `${lastService.notes} · ${lastService.started_at.slice(0, 10)}` : "—"}
          </Field>
        </div>
      </div>
    </>
  );
}

function InspectCard({ decisions, batchHeld }: CellStationCardProps) {
  const requested = decisions.send_to_inspect;
  return (
    <>
      <div className="cell-card__main">
        <CardHead station="inspect" health="no-feed" asset={STATION_INFO.inspect.asset} />
        <p className="cell-card__note">
          No inspection sensor is streamed in this demo. Pass and fail results come from the QA system once it is
          connected.
        </p>
        {requested?.reference.parts_since && (
          <p className="cell-card__note">
            Parts machined since {formatDeviceTime(Date.parse(requested.reference.parts_since) / 1000).slice(0, 8)} are
            queued for vision QA.
          </p>
        )}
        <DecisionNotes station="inspect" decisions={decisions} batchHeld={batchHeld} />
      </div>
      <div className="cell-card__side">
        <div className="cell-card__context">
          <span className="cell-card__context-label">{REFERENCE_CONTEXT.label}</span>
          <Field label="Upstream">MILL · Spindle 3</Field>
          <Field label="Downstream">PACK · pack line</Field>
        </div>
      </div>
    </>
  );
}

function PackCard({ pack, tiles, decisions, batchHeld }: CellStationCardProps) {
  return (
    <>
      <div className="cell-card__main">
        <CardHead station="pack" health={pack.health} asset={STATION_INFO.pack.asset} />
        <SensorTiles tiles={tiles.pack} />
        <DecisionNotes station="pack" decisions={decisions} batchHeld={batchHeld} />
      </div>
      <div className="cell-card__side">
        <div className="cell-card__fields">
          <Field label="Device event time">{formatDeviceTime(pack.speed.timestamp)}</Field>
        </div>
        <div className="cell-card__context">
          <span className="cell-card__context-label">{REFERENCE_CONTEXT.label}</span>
          <Field label="Upstream">INSPECT · vision QA</Field>
          <Field label="Batch">{REFERENCE_CONTEXT.batch}</Field>
        </div>
      </div>
    </>
  );
}

export function CellStationCard(props: CellStationCardProps & { stacked?: boolean }) {
  let body: React.ReactNode;
  switch (props.station) {
    case "mill":
      body = <SpindleCard {...props} />;
      break;
    case "inspect":
      body = <InspectCard {...props} />;
      break;
    case "pack":
      body = <PackCard {...props} />;
      break;
    default: {
      const _exhaustive: never = props.station;
      body = _exhaustive;
    }
  }
  return (
    <div
      className={`panel cell-card${props.stacked ? " cell-card--stacked" : ""}`}
      aria-label={`${STATION_INFO[props.station].name} twin card`}
    >
      {body}
    </div>
  );
}
