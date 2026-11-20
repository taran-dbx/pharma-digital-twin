/* TEMPORARY preview harness for screenshotting the two-tier health page.
 * Not committed (client/ is gitignored; this file is deleted after capture). */
import { ArrowLeft } from "lucide-react";
import { Button } from "@/components/ui/button";
import type { HealthCheck, PipelineSetup, E2eTestDescriptor } from "@/lib/admin-api";
import { ActionsRow } from "./pipeline-health/actions-row";
import { BuiltOnRow, Eyebrow, ResourceGroupCard } from "./pipeline-health/components";
import {
  ClusterIndicator,
  E2eVerifiedList,
  FastChecksCard,
  TierHealthBanner,
} from "./pipeline-health/health-checks";
import { E2eTestPanel } from "./pipeline-health/e2e-test-panel";
import type { ValidationFlow } from "@/hooks/use-validation-flow";
import { BRAND, PHARMA_HEALTH, rollup } from "./pipeline-health/types";

/** Static stub so the preview harness needs no live validate-flow. */
const PREVIEW_FLOW: ValidationFlow = {
  statuses: {},
  stepErrors: {},
  running: false,
  started: false,
  cdfWaiting: false,
  phase: "idle",
  error: null,
  busy: false,
  setupId: "b3f1c2a4-1111-2222-3333-444455556666",
  clusterState: "RUNNING",
  clusterId: "0706-abc123",
  currentStepId: null,
  runStartedAt: null,
  canDeploy: false,
  outstanding: [],
  modeGates: {},
  run: async () => {},
  confirmCdf: async () => {},
  retry: async () => {},
  deploy: async () => ({ ok: false, message: "" }),
  saveDraft: async () => ({ ok: false, message: "" }),
  remove: async () => ({ ok: false, message: "" }),
};

const CHECKS: HealthCheck[] = [
  { name: "uc_catalog_schemas", tier: "api", status: "pass", detail: "catalog iot_pharmaceutical + all schemas present" },
  { name: "delta_tables", tier: "api", status: "pass", detail: "3 Delta table(s) present for microbatch, realtime, zerobus" },
  { name: "checkpoint_volume", tier: "api", status: "pass", detail: "stream_checkpoints volume present" },
  { name: "metrics_table", tier: "api", status: "pass", detail: "iot_pharmaceutical.metrics.streaming_metrics: 12,458 rows (latest 2026-07-06 11:22:04)" },
  { name: "lakebase_endpoints", tier: "api", status: "pass", detail: "branch pharmaceutical: primary/replica {primary: ACTIVE, replica: ACTIVE}" },
  { name: "chronotable", tier: "api", status: "pass", detail: "sensor_readings: ChronoTable OK, 96 forward partitions (to 2026-07-07 11:00), columns match SSOT" },
  { name: "cdf_sync", tier: "api", status: "fail", detail: "lakets_cdf._shadow_sensor_readings not STREAMING/advancing — enable managed Lakebase CDF sync" },
  { name: "uc_grants", tier: "api", status: "pass", detail: "streaming SP iot-sss-industries granted on iot_pharmaceutical" },
  { name: "secrets", tier: "api", status: "pass", detail: "scope iot-secrets: 2 secrets present" },
  { name: "jobs", tier: "api", status: "skip", detail: "no jobs deployed yet" },
  { name: "interactive_cluster", tier: "api", status: "pass", detail: "interactive cluster 0706-abc123 is RUNNING", cluster_state: "RUNNING" },
  { name: "kafka_topics", tier: "e2e", status: "skip", detail: "topic existence needs MSK-IAM (cluster-only via dbutils.credentials); verified by the interactive e2e test for iot.pharmaceutical.realtime, iot.pharmaceutical.microbatch" },
];

const SETUP: PipelineSetup = {
  id: "b3f1c2a4-1111-2222-3333-444455556666",
  industry: "pharmaceutical",
  status: "stopped",
  flavor: "classic",
  source_type: "kafka",
  bootstrap_servers: "b-1.example.abc123.c9.kafka.us-east-1.amazonaws.com:9098",
  enabled_modes: ["realtime", "microbatch", "zerobus"],
  warehouse_id: "abc123warehouse",
  user_email: "admin@example.com",
  created_at: null,
  updated_at: null,
};

const E2E: E2eTestDescriptor = {
  tier: "e2e",
  available: true,
  spins_interactive_cluster: true,
  eta_minutes: 10,
  description:
    "Opt-in end-to-end test: spins the interactive validation cluster (~10 min) to verify source DNS/TCP/TLS connectivity, Kafka topics, and a produce→consume→land→metrics round-trip. Distinct from the fast Tier-1 API health above.",
  start_endpoint: "POST /api/admin/industries/pharmaceutical/validate/start",
  state_endpoint: "GET /api/admin/industries/pharmaceutical/validate/state",
};

export function HealthPreviewPage() {
  const apiChecks = CHECKS.filter((c) => c.tier === "api");
  const e2eChecks = CHECKS.filter((c) => c.tier === "e2e");
  const health = PHARMA_HEALTH;

  return (
    <div className="mx-auto max-w-5xl space-y-6 pb-16">
      <div className="space-y-3">
        <Button variant="ghost" size="sm" className="-ml-2 gap-1 text-muted-foreground">
          <ArrowLeft className="h-3.5 w-3.5" />
          Industries
        </Button>
        <div>
          <Eyebrow>Pipeline · Resource Health</Eyebrow>
          <h1 className="mt-1 font-display text-2xl font-bold tracking-tight">Pharmaceutical</h1>
          <p className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-sm text-muted-foreground">
            <span className="capitalize">pharmaceutical</span>
            <span className="text-border">·</span>
            <span className="font-mono text-xs">{SETUP.id}</span>
            <span className="text-border">·</span>
            <span className="font-mono text-xs">iot_pharmaceutical</span>
          </p>
        </div>
        <BuiltOnRow items={[BRAND.streaming, BRAND.zerobus, BRAND.lakebase, BRAND.unity]} />
      </div>

      <TierHealthBanner health="unhealthy" startable={false} apiChecks={apiChecks} hasChecks />
      <ClusterIndicator state="RUNNING" />
      <ActionsRow health={health} />
      <FastChecksCard checks={apiChecks} onRefresh={() => {}} refreshing={false} />
      <div className="space-y-3">
        <E2eTestPanel
          e2e={E2E}
          flow={PREVIEW_FLOW}
          enabledModes={["realtime", "microbatch", "zerobus"]}
        />
        <E2eVerifiedList checks={e2eChecks} />
      </div>
      <div className="space-y-3">
        <Eyebrow>Resource ledger</Eyebrow>
        <div className="grid gap-5 lg:grid-cols-2">
          {health.groups.map((group) => (
            <ResourceGroupCard key={group.id} group={group} rollupStatus={rollup(group.items)} />
          ))}
        </div>
      </div>
    </div>
  );
}
