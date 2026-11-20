/**
 * Prototype data + types for the admin "Classic Pipeline Creation &
 * Validation" flow (spec §Section 1). Local-only placeholder data — no
 * backend calls. Mirrors the numbers from the design brief:
 * Pharmaceutical, `b-1.example…:9098`, 6 partitions → 2 workers, RF 3.
 */
import type { LucideIcon } from "lucide-react";
import {
  Factory,
  FlaskConical,
  Zap,
  Car,
  Fuel,
  Gauge,
  Mountain,
  Rocket,
} from "lucide-react";

export type Flavor = "classic" | "serverless";
export type SourceType = "kafka" | "kinesis";
export type Mode = "realtime" | "microbatch" | "zerobus";

export const MODE_LABELS: Record<Mode, string> = {
  realtime: "Real-time",
  microbatch: "Micro-batch",
  zerobus: "Zerobus",
};

export function hasKafkaModes(modes: readonly Mode[]): boolean {
  return modes.includes("realtime") || modes.includes("microbatch");
}

export function formatModes(modes: readonly Mode[]): string {
  return modes.map((mode) => MODE_LABELS[mode]).join(" · ");
}

export type StepStatus =
  | "pending"
  | "running"
  | "passed"
  | "failed"
  | "waiting_admin";

export interface IndustryOption {
  id: string;
  name: string;
  tag: string;
  sensors: number;
  icon: LucideIcon;
  /** Already has a setup — one-per-industry uniqueness rule (§1.2). */
  exists?: boolean;
}

export const INDUSTRIES: IndustryOption[] = [
  { id: "manufacturing", name: "Manufacturing", tag: "Assembly · CNC · robotics", sensors: 19, icon: Factory, exists: true },
  { id: "pharmaceutical", name: "Pharmaceutical", tag: "Bioreactor · WFI · batch yield", sensors: 30, icon: FlaskConical },
  { id: "energy", name: "Energy", tag: "Turbines · grid load", sensors: 32, icon: Zap, exists: true },
  { id: "automotive", name: "Automotive", tag: "Body shop · paint · powertrain", sensors: 28, icon: Car },
  { id: "oil_gas", name: "Oil & Gas", tag: "Wellhead · pipeline · flare", sensors: 26, icon: Fuel },
  { id: "utilities", name: "Utilities", tag: "Substations · SCADA", sensors: 18, icon: Gauge },
  { id: "mining", name: "Mining", tag: "Haul · crusher · conveyor", sensors: 17, icon: Mountain },
  { id: "aerospace", name: "Aerospace", tag: "Airframe · avionics test", sensors: 25, icon: Rocket },
];

/** Product brand marks (SVGs under client/public/brand). */
export const BRAND = {
  streaming: { src: "/brand/data-streaming-icon-full-color.svg", label: "Spark Structured Streaming" },
  zerobus: { src: "/brand/lakeflow-connect-icon-full-color.svg", label: "Zerobus" },
  lakebase: { src: "/brand/lakebase-icon-full-color.svg", label: "Lakebase" },
  unity: { src: "/brand/unity-catalog-icon-full-color.svg", label: "Unity Catalog" },
} as const;

/** Worker sizing per §1.3 M1: clamp(ceil(partitions / cores), 1, 8). */
export const CORES_PER_WORKER = 4;
export function computeWorkers(partitions: number): number {
  return Math.min(Math.max(Math.ceil(partitions / CORES_PER_WORKER), 1), 8);
}

/** Default UC service credential *name* (an identifier, not a secret). */
const DEFAULT_UC_SC_NAME = "iot-msk-service-cred";

/** Fixed prototype source config (Kafka / MSK). */
export const SOURCE_DEFAULTS = {
  bootstrapServers: "b-1.example.abc123.c9.kafka.us-east-1.amazonaws.com:9098",
  region: "us-east-1",
  replicationFactor: 3,
  brokerCount: 3,
  partitions: 6,
  mskArn: "arn:aws:kafka:us-east-1:123456789012:cluster/example/abc123-…-4",
  serviceCredential: DEFAULT_UC_SC_NAME,
} as const;

/** Generation rate — records/sec plus an estimated wire throughput. */
export const INGEST_RATE = {
  recordsPerSec: 2000,
  label: "2,000 rec/s · ~0.4 MB/s",
} as const;

/** Per-mode generation semantics (record flush behaviour). */
export const MODE_GENERATION: Record<string, string> = {
  realtime: "flush each row ASAP — unbatched (lowest latency)",
  microbatch: "batched every 10s",
  zerobus: "flush per ack batch",
};

export const COMPUTE = {
  nodeType: "m5d.large",
  vcpu: "4 vCPU · 8 GB",
  dbr: "18.2 · Spark 4.1 (real-time mode)",
  autoTerminateMin: 90,
} as const;

export interface SsotField {
  field: string;
  column: string;
  type: string;
  transform: string;
}

/** SSOT field-map (§1.9) — the canonical SensorRecord, industry-invariant. */
export const SSOT_FIELDS: SsotField[] = [
  { field: "sensor_name", column: "sensor_name", type: "TEXT", transform: "passthrough" },
  { field: "industry", column: "industry", type: "TEXT", transform: "passthrough" },
  { field: "value", column: "value", type: "DOUBLE PRECISION", transform: "cast(double)" },
  { field: "unit", column: "unit", type: "TEXT", transform: "passthrough" },
  { field: "sensor_type", column: "sensor_type", type: "TEXT", transform: "passthrough" },
  { field: "timestamp", column: "event_time", type: "TIMESTAMPTZ", transform: "epoch → timestamptz" },
  { field: "min_value", column: "min_value", type: "DOUBLE PRECISION", transform: "cast(double)" },
  { field: "max_value", column: "max_value", type: "DOUBLE PRECISION", transform: "cast(double)" },
  { field: "nominal_value", column: "nominal_value", type: "DOUBLE PRECISION", transform: "cast(double)" },
  { field: "fault_active", column: "fault_active", type: "BOOLEAN", transform: "cast(bool)" },
  { field: "metadata", column: "metadata", type: "JSONB", transform: "passthrough" },
];

/** Streaming-metrics tiles (§1.11) — captured via delta-rs. */
export interface MetricTile {
  label: string;
  value: string;
  unit: string;
  hint: string;
}
export const METRIC_TILES: MetricTile[] = [
  { label: "E2E latency p50", value: "42", unit: "ms", hint: "Databricks runtime" },
  { label: "E2E latency p99", value: "128", unit: "ms", hint: "Databricks runtime" },
  { label: "Processing p50", value: "18", unit: "ms", hint: "per micro-batch" },
  { label: "Processing p99", value: "64", unit: "ms", hint: "per micro-batch" },
  { label: "Input rate", value: "12.4k", unit: "rps", hint: "num_input_rows / s" },
  { label: "Processed rate", value: "12.38k", unit: "rps", hint: "processed_rows / s" },
  { label: "Trigger interval", value: "10,000", unit: "ms", hint: "processingTime" },
  { label: "Source queuing p99", value: "9", unit: "ms", hint: "Kafka backlog" },
];

/* ── Validation state machine (§1.12 M3) ───────────────────────────── */

export interface ValidationStep {
  id: string;
  label: string;
  detail?: string;
  /** cdf_manual parks in waiting_admin until the admin confirms (§1.10). */
  manual?: boolean;
}

export const COMMON_STEPS: ValidationStep[] = [
  { id: "provision_cluster", label: "Provision interactive cluster", detail: "general-purpose · on-demand (no spot) · non-Photon · DBR 18.2 (Spark 4.1)" },
  { id: "source_connectivity", label: "Source connectivity", detail: "DNS → TCP → TLS → auth (UC service credential)" },
  { id: "topics_created", label: "Topics created", detail: "iot.pharmaceutical.realtime + .microbatch · 6 partitions · RF 3" },
  { id: "schema_conformance", label: "Schema conformance gate", detail: "sample records → SSOT columns line up" },
];

export const REALTIME_STEPS: ValidationStep[] = [
  { id: "rt_partitions_precreate", label: "Partition pre-create", detail: "24h / 96-chunk forward window + Partition Manager run" },
  { id: "rt_write_test", label: "Write test", detail: "Kafka → Spark real-time → Lakebase + LakeTS" },
  { id: "rt_cdf_manual", label: "Enable CDF sync", detail: "Admin manually enables Lakebase → Delta sync", manual: true },
  { id: "rt_cdf_verify", label: "Verify CDF live", detail: "poll STREAMING + committed_lsn" },
  { id: "rt_read_back", label: "Read-back", detail: "read from Lakebase directly (round-trip)" },
  { id: "rt_dbsql_read", label: "DBSQL read", detail: "poll committed_lsn / STREAMING → read synced Delta" },
  { id: "rt_metrics_check", label: "Metrics check", detail: "delta-rs rows + Databricks latency percentiles" },
  { id: "rt_cleanup", label: "Cleanup", detail: "empty test data · delete validation checkpoint" },
];

export const MICROBATCH_STEPS: ValidationStep[] = [
  { id: "mb_write_test", label: "Write test", detail: "bounded consumer → Delta (UC) · trigger 10s" },
  { id: "mb_dbsql_read", label: "DBSQL read", detail: "assert rows land → read Delta table" },
  { id: "mb_metrics_check", label: "Metrics check", detail: "delta-rs rows + latency percentiles" },
  { id: "mb_cleanup", label: "Cleanup", detail: "empty rows · delete validation checkpoint" },
];

export const ZEROBUS_STEPS: ValidationStep[] = [
  { id: "zb_create_table", label: "Create target Delta table", detail: "<UC_CATALOG>.<UC_SCHEMA>.<industry>_zerobus (SSOT)" },
  { id: "zb_auth_probe", label: "Auth / endpoint probe", detail: "OAuth (https://…/oidc/v1/token) → ZerobusSdk create_stream" },
  { id: "zb_produce_test", label: "Produce test", detail: "serverless producer → wait_for_ack()" },
  { id: "zb_dbsql_read", label: "DBSQL read", detail: "assert rows → read Delta" },
  { id: "zb_cleanup", label: "Cleanup", detail: "cancel serverless run · empty rows (keep table)" },
];

/** Every validation step across all lanes, in walk order. */
export const ALL_STEPS: ValidationStep[] = [
  ...COMMON_STEPS,
  ...REALTIME_STEPS,
  ...MICROBATCH_STEPS,
  ...ZEROBUS_STEPS,
];

/**
 * Step id → label, for the deploy-gate "outstanding" summary. The setup page
 * maps backend flat step keys (`realtime:cdf_verify`) to these frontend ids
 * (`rt_cdf_verify`) via `backendKeyToFeStep` before looking up a label.
 */
export const STEP_LABEL: Record<string, string> = Object.fromEntries(
  ALL_STEPS.map((s) => [s.id, s.label]),
);

export const STEP_LABELS = [
  "Flavor & Industry",
  "Source",
  "Setup Preview",
  "Validation",
  "Metrics",
  "Notebook",
];
