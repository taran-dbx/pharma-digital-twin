import type { ReactNode } from "react";
import { Activity, Database, Radio, Timer, Waves, type LucideIcon } from "lucide-react";

import type { SensorHistoryPoint } from "@/contexts/sensor-context";
import { ageText, clockOf } from "@/components/cell/cell-kpis";
import type { CellFeed } from "@/components/cell/use-cell-model";
import type { ProductionPhase } from "@/hooks/use-production";
import type { PlatformMetrics } from "@/hooks/use-platform-metrics";
import type { PipelineMode } from "@/lib/industries-api";
import type { ZerobusMetricsResponse } from "@/lib/types";
import { formatNumber, toEpochSeconds } from "@/lib/utils";

/** Lakebase data older than this reads as stale. */
const STALE_WRITE_S = 30;

export type NodeState = "ok" | "warn" | "off";

export interface PipelineNode {
  key: string;
  label: string;
  metric: string;
  state: NodeState;
}

export interface PlatformInputs {
  mode: PipelineMode;
  powered: boolean;
  wsConnected: boolean;
  phase: ProductionPhase;
  feed: CellFeed;
  metrics: PlatformMetrics;
  zerobus: ZerobusMetricsResponse | null;
  now: number;
}

function ms(value: number | null | undefined): string {
  return value === null || value === undefined ? "—" : `${formatNumber(value, 0)} ms`;
}

function rate(value: number | null | undefined, unit: string): string {
  return value === null || value === undefined ? "—" : `${formatNumber(value, 0)} ${unit}`;
}

/** Age of the newest Lakebase row when the metrics were fetched, not now: the poll runs every refresh interval. */
function writeAge(metrics: PlatformMetrics, now: number): number | undefined {
  const latest = metrics.health?.latest_at;
  if (!latest) return undefined;
  const at = metrics.refreshedAt !== undefined ? metrics.refreshedAt / 1000 : now;
  return Math.max(0, at - toEpochSeconds(latest));
}

/** Producer → transport → compute → store → this page, with one live number each. */
export function pipelineNodes(input: PlatformInputs): PipelineNode[] {
  const { mode, powered, wsConnected, phase, feed, metrics, zerobus, now } = input;
  const on = (ok: boolean): NodeState => (!powered ? "off" : ok ? "ok" : "warn");
  const producer: PipelineNode = {
    key: "producer",
    label: "Cell producer",
    metric:
      phase === "stopped"
        ? "cell stopped · utilities only"
        : mode === "realtime"
          ? `${feed.machinesLive + feed.utilitiesLive} sensors live`
          : "simulator",
    state: phase === "stopped" ? "warn" : on(true),
  };
  const latency = metrics.latency;
  switch (mode) {
    case "realtime": {
      const age = writeAge(metrics, now);
      return [
        producer,
        { key: "kafka", label: "Kafka", metric: rate(latency?.input_rps, "rec/s"), state: on(!!latency?.has_data) },
        { key: "spark", label: "Spark RTM", metric: `p50 ${ms(latency?.e2e_p50)}`, state: on(!!latency?.has_data) },
        {
          key: "lakebase",
          label: "Lakebase",
          metric: rate(metrics.lakebase?.rows_per_sec, "rows/s"),
          state: on(age !== undefined && age < STALE_WRITE_S),
        },
        { key: "app", label: "Twin · SSE", metric: wsConnected ? "connected" : "offline", state: on(wsConnected) },
      ];
    }
    case "microbatch":
      return [
        producer,
        { key: "kafka", label: "Kafka", metric: rate(latency?.input_rps, "rec/s"), state: on(!!latency?.has_data) },
        { key: "spark", label: "Spark micro-batch", metric: `p50 ${ms(latency?.e2e_p50)}`, state: on(!!latency?.has_data) },
        { key: "delta", label: "Delta", metric: "Unity Catalog", state: on(!!latency?.has_data) },
      ];
    case "zerobus":
      return [
        producer,
        {
          key: "zerobus",
          label: "Zerobus",
          metric: rate(zerobus?.records_per_sec, "rec/s"),
          state: on(zerobus?.stream_status === "open"),
        },
        { key: "delta", label: "Delta", metric: zerobus?.last_commit_time ? "committing" : "—", state: on(!!zerobus?.last_commit_time) },
      ];
    default: {
      const _exhaustive: never = mode;
      return _exhaustive;
    }
  }
}

export function PipelineChain({ nodes }: { nodes: PipelineNode[] }) {
  return (
    <ol className="cellops-chain" aria-label="Data pipeline" data-testid="pipeline-chain">
      {nodes.map((node) => (
        <li key={node.key} className={`is-${node.state}`}>
          <i aria-hidden="true" />
          <span>{node.label}</span>
          <b className="tnum">{node.metric}</b>
        </li>
      ))}
    </ol>
  );
}

interface Metric {
  key: string;
  icon: LucideIcon;
  label: string;
  value: string;
  unit?: string;
  foot: ReactNode;
  tone?: "warn" | "held";
  spark?: SensorHistoryPoint[];
}

function Trend({ points: series }: { points: SensorHistoryPoint[] }) {
  const t0 = series[0].t;
  const dt = series[series.length - 1].t - t0 || 1;
  const lo = Math.min(...series.map((p) => p.v));
  const span = Math.max(...series.map((p) => p.v)) - lo || 1;
  const xy = series.map((p) => `${(((p.t - t0) / dt) * 100).toFixed(1)},${(26 - ((p.v - lo) / span) * 22).toFixed(1)}`);
  return (
    <svg className="cellops-metric__trend" viewBox="0 0 100 28" preserveAspectRatio="none" aria-hidden="true">
      <path className="area" d={`M0,28 L${xy.join(" L")} L100,28 Z`} />
      <path className="line" d={`M${xy.join(" L")}`} vectorEffect="non-scaling-stroke" />
    </svg>
  );
}

function points(series: { t: string | null; v: number | null }[]): SensorHistoryPoint[] {
  return series
    .filter((p) => p.t && p.v !== null && Number.isFinite(p.v))
    .map((p) => ({ t: toEpochSeconds(p.t) * 1000, v: Number(p.v) }));
}

function metricCards(input: PlatformInputs): Metric[] {
  const { mode, phase, feed, metrics, zerobus, wsConnected, now } = input;
  const cards: Metric[] = [];
  const { lakebase, health, latency } = metrics;
  if (mode === "realtime") {
    const age = writeAge(metrics, now);
    cards.push(
      {
        key: "lakebase-writes",
        icon: Database,
        label: "Lakebase writes",
        value: lakebase ? formatNumber(lakebase.rows_per_sec, 0) : "—",
        unit: "rows/s",
        foot: lakebase ? `${formatNumber(lakebase.rows_per_min, 0)} rows/min · last 15 min` : "Waiting for Lakebase",
        spark: lakebase ? points(lakebase.series.map((p) => ({ t: p.t, v: p.rows_per_sec }))) : undefined,
      },
      {
        key: "freshness",
        icon: Timer,
        label: "Write freshness",
        value: age === undefined ? "—" : age < 10 ? formatNumber(age, 1) : formatNumber(age, 0),
        unit: "s",
        tone: age !== undefined && age >= STALE_WRITE_S ? "warn" : undefined,
        foot: health ? `${formatNumber(health.rows_5m, 0)} rows in the last 5 min` : "Waiting for Lakebase",
      },
    );
  }
  if (mode !== "zerobus") {
    cards.push({
      key: "spark-latency",
      icon: Activity,
      label: mode === "realtime" ? "Spark RTM latency" : "Spark batch latency",
      value: latency?.has_data ? `${formatNumber(latency.e2e_p50 ?? 0, 0)} / ${formatNumber(latency.e2e_p99 ?? 0, 0)}` : "—",
      unit: "ms",
      foot: latency?.has_data
        ? `p50 / p99 · batch #${latency.last_batch_id ?? "—"} · ${rate(latency.processed_rps, "rows/s")}`
        : latency?.reason ?? "Waiting for streaming metrics",
      spark: latency?.has_data
        ? points(latency.series.map((p) => ({ t: p.timestamp, v: p.e2e_p99 })))
        : undefined,
    });
  } else {
    cards.push({
      key: "zerobus",
      icon: Waves,
      label: "Zerobus ingest",
      value: zerobus ? formatNumber(zerobus.records_per_sec, 0) : "—",
      unit: "rec/s",
      foot: zerobus?.last_commit_time
        ? `Last commit ${ageText(Math.max(0, now - toEpochSeconds(zerobus.last_commit_time)))}`
        : "No commits yet",
    });
  }
  if (mode === "realtime") {
    cards.push(
      {
        key: "cell-feed",
        icon: Radio,
        label: "Machine sensors live",
        value: `${feed.machinesLive}`,
        unit: `of ${feed.machinesTotal}`,
        tone: phase === "stopped" ? "held" : undefined,
        foot:
          phase === "stopped"
            ? `Cell stopped · ${feed.utilitiesLive} utility sensors still streaming`
            : `${feed.utilitiesLive} utility sensors live`,
      },
      {
        key: "sse",
        icon: Radio,
        label: "Twin stream",
        value: wsConnected ? "Live" : "Offline",
        tone: wsConnected ? undefined : "warn",
        foot: "Server-sent events from the RTM job to this page",
      },
    );
  }
  return cards;
}

export function PlatformHealth({ input, children }: { input: PlatformInputs; children?: ReactNode }) {
  const cards = metricCards(input);
  return (
    <section className="cellops-platform" aria-label="Data platform" data-testid="platform-health">
      <header className="cellops-section-head">
        <div>
          <span className="cellops-eyebrow">Data platform</span>
          <h2>Pipeline health</h2>
        </div>
        {input.metrics.refreshedAt !== undefined && (
          <span className="cellops-section-head__note">
            Refreshed {clockOf(new Date(input.metrics.refreshedAt).toISOString())}
          </span>
        )}
      </header>
      <div className="cellops-platform__grid">
        {cards.map((card) => (
          <article key={card.key} className={`cellops-metric${card.tone ? ` tone-${card.tone}` : ""}`} data-testid={`metric-${card.key}`}>
            <header>
              <card.icon aria-hidden="true" />
              <span>{card.label}</span>
            </header>
            <div className="cellops-metric__value tnum">
              {card.value}
              {card.unit && <small> {card.unit}</small>}
            </div>
            {card.spark && card.spark.length > 1 && <Trend points={card.spark} />}
            <footer>{card.foot}</footer>
          </article>
        ))}
      </div>
      {children}
    </section>
  );
}
