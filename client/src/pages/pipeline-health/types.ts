/**
 * Prototype data + types for the in-app "Pipeline Resource-Health" detail page
 * (route: /pipelines/:id/health). Local-only mock data — no backend calls.
 *
 * Models the full resource ledger for one deployed pipeline ("Pharmaceutical"):
 * Unity Catalog objects, the real-time Lakebase project/branch/endpoints +
 * LakeTS ChronoTable/CDF sync, Kafka topics, the seven orchestration jobs,
 * the streaming-metrics table, and the identity/secret resources.
 *
 * Exactly one resource is intentionally in a `down` state (the LakeTS Tiering
 * job) so the degraded styling + overall banner call-out are exercisable.
 */

/** Health of a single resource. Ordered worst → best for banner rollup. */
export type HealthStatus = "down" | "warning" | "idle" | "healthy";

/** Severity used to roll a group / the whole pipeline up to one status. */
const SEVERITY: Record<HealthStatus, number> = {
  down: 3,
  warning: 2,
  idle: 0, // idle is a benign steady state — it never degrades the pipeline
  healthy: 1,
};

export interface ResourceItem {
  /** Resource name — rendered monospace when `mono` (catalog/table/topic names). */
  label: string;
  /** Short status detail / config summary (schedule, spec, binding). */
  detail?: string;
  status: HealthStatus;
  /** Last-checked / last-run time, right-aligned next to the pill. */
  timestamp?: string;
  mono?: boolean;
}

export interface ResourceGroup {
  id: string;
  title: string;
  /** Optional product brand mark shown beside the title. */
  brand?: { src: string; label: string };
  /** Optional footnote callout under the rows (e.g. CDF / ChronoTable notes). */
  note?: string;
  items: ResourceItem[];
}

/** Product brand marks (SVGs under client/public/brand). */
export const BRAND = {
  unity: { src: "/brand/unity-catalog-icon-full-color.svg", label: "Unity Catalog" },
  lakebase: { src: "/brand/lakebase-icon-full-color.svg", label: "Lakebase" },
  streaming: { src: "/brand/data-streaming-icon-full-color.svg", label: "Spark Structured Streaming" },
  zerobus: { src: "/brand/lakeflow-connect-icon-full-color.svg", label: "Zerobus" },
} as const;

export interface PipelineHealth {
  id: string;
  name: string;
  industry: string;
  catalog: string;
  groups: ResourceGroup[];
}

/** The single mock pipeline this prototype renders. */
export const PHARMA_HEALTH: PipelineHealth = {
  id: "pharmaceutical",
  name: "Pharmaceutical",
  industry: "Pharmaceutical",
  catalog: "iot_pharmaceutical",
  groups: [
    {
      id: "unity_catalog",
      title: "Unity Catalog",
      brand: BRAND.unity,
      items: [
        {
          label: "iot_pharmaceutical",
          detail: "Catalog · bound to metastore ✓",
          status: "healthy",
          timestamp: "checked 30s ago",
          mono: true,
        },
        {
          label: "Schemas (5)",
          detail: "realtime · microbatch · zerobus · metrics · checkpoints",
          status: "healthy",
        },
        {
          label: "realtime.sensor_readings",
          detail: "Managed Delta table",
          status: "healthy",
          mono: true,
        },
        {
          label: "microbatch.sensor_data",
          detail: "Managed Delta table",
          status: "healthy",
          mono: true,
        },
        {
          label: "zerobus.iot_sensor_data",
          detail: "Managed Delta table",
          status: "healthy",
          mono: true,
        },
        {
          label: "metrics.streaming_metrics",
          detail: "External Delta table",
          status: "healthy",
          mono: true,
        },
        {
          label: "checkpoints.stream_checkpoints",
          detail: "UC volume · stream checkpoints",
          status: "healthy",
          mono: true,
        },
      ],
    },
    {
      id: "lakebase",
      title: "Lakebase (real-time)",
      brand: BRAND.lakebase,
      note:
        "ChronoTable partitions are current — the Partition Manager keeps a rolling 24h / 96-chunk forward window; the next chunk is created in ~8m.",
      items: [
        {
          label: "iot-pipelines",
          detail: "Lakebase project",
          status: "healthy",
          mono: true,
        },
        {
          label: "pharmaceutical",
          detail: "Branch · active",
          status: "healthy",
          mono: true,
        },
        {
          label: "Primary endpoint (RW)",
          detail: "Read-write · running",
          status: "healthy",
          timestamp: "up 6d",
        },
        {
          label: "Read replica endpoint",
          detail: "Read-only · running",
          status: "healthy",
          timestamp: "up 6d",
        },
        {
          label: "ChronoTable sensor_readings",
          detail: "Partitions current · next chunk in 8m · 96-chunk / 24h horizon",
          status: "healthy",
          mono: true,
        },
        {
          label: "Rollups",
          detail: "1m / 5m / 1h aggregates fresh",
          status: "healthy",
        },
        {
          label: "CDF sync",
          detail: "STREAMING ✓ · committed_lsn advancing",
          status: "healthy",
          timestamp: "lag < 1s",
        },
      ],
    },
    {
      id: "kafka",
      title: "Kafka topics",
      brand: BRAND.streaming,
      items: [
        {
          label: "iot.pharmaceutical.realtime",
          detail: "6 partitions · RF 3",
          status: "healthy",
          mono: true,
        },
        {
          label: "iot.pharmaceutical.microbatch",
          detail: "6 partitions · RF 3",
          status: "healthy",
          mono: true,
        },
      ],
    },
    {
      id: "jobs",
      title: "Jobs (7)",
      items: [
        {
          label: "iot-pharmaceutical-realtime",
          detail: "Continuous · Kafka → Spark → Lakebase + LakeTS",
          status: "healthy",
          timestamp: "RUNNING",
          mono: true,
        },
        {
          label: "iot-pharmaceutical-microbatch",
          detail: "Continuous · bounded consumer → Delta",
          status: "healthy",
          timestamp: "RUNNING",
          mono: true,
        },
        {
          label: "iot-pharmaceutical-zerobus",
          detail: "Serverless · on-demand producer",
          status: "idle",
          timestamp: "last run OK · 2h ago",
          mono: true,
        },
        {
          label: "LakeTS · Partition Manager",
          detail: "Every 10m · roll forward partition window",
          status: "healthy",
          timestamp: "last run 4m ago",
        },
        {
          label: "LakeTS · Rollup Refresh",
          detail: "Every 15m · refresh 1m/5m/1h rollups",
          status: "healthy",
          timestamp: "last run 9m ago",
        },
        {
          label: "LakeTS · Tiering",
          detail: "Every 30m · offload cold chunks to Delta",
          status: "down",
          timestamp: "last run failed · 12m ago",
        },
        {
          label: "LakeTS · Retention",
          detail: "Daily · drop chunks past horizon",
          status: "healthy",
          timestamp: "last run 6h ago",
        },
      ],
    },
    {
      id: "metrics",
      title: "Metrics",
      items: [
        {
          label: "streaming_metrics",
          detail: "Receiving rows · delta-rs → external Delta",
          status: "healthy",
          timestamp: "last batch 3s ago",
          mono: true,
        },
      ],
    },
    {
      id: "identity",
      title: "Identity & secrets",
      items: [
        {
          label: "iot-sss-industries",
          detail: "Service principal · shared across industries",
          status: "healthy",
          mono: true,
        },
        {
          label: "iot-secrets",
          detail: "Secret scope · MSK + Lakebase credentials",
          status: "healthy",
          mono: true,
        },
      ],
    },
  ],
};

/** Roll a set of items up to a single worst-case status (idle is benign). */
export function rollup(items: ResourceItem[]): HealthStatus {
  return items.reduce<HealthStatus>((worst, item) => {
    return SEVERITY[item.status] > SEVERITY[worst] ? item.status : worst;
  }, "healthy");
}

/** The single most-severe resource across the whole pipeline, if degraded. */
export function findWorstResource(
  health: PipelineHealth,
): { group: string; item: ResourceItem } | null {
  let worst: { group: string; item: ResourceItem } | null = null;
  for (const group of health.groups) {
    for (const item of group.items) {
      if (item.status === "healthy" || item.status === "idle") continue;
      if (!worst || SEVERITY[item.status] > SEVERITY[worst.item.status]) {
        worst = { group: group.title, item };
      }
    }
  }
  return worst;
}
