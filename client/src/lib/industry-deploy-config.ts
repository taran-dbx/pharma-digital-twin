/**
 * Assembles the per-mode DEPLOY payload for an industry pipeline — the
 * client-generated consumer/producer notebooks plus the deterministic config
 * the backend deploy endpoint (`POST /api/admin/pipelines/{setup}/deploy/{mode}`)
 * bakes into the Databricks job(s).
 *
 * The config values here MUST mirror the interactive validation harness
 * (`services/validation_steps.py` / `services/validation_config.py` /
 * `models/metrics_schema.py`) so a DEPLOYED job behaves like what was
 * validated:
 *   - UC namespace   configured `UC_CATALOG.UC_SCHEMA`
 *   - topic          `iot.<industry>.<mode>`         (dot notation, §1.14)
 *   - service cred   `iot-rtm-sc`                    (MSK-IAM via UC)
 *   - realtime sink  Lakebase `sensor_readings` on the `iot-pipelines`
 *                    project, per-industry branch (branch == industry),
 *                    database `databricks_postgres`
 *   - microbatch sink Delta `<UC_CATALOG>.<UC_SCHEMA>.<industry>_microbatch`
 *   - metrics        `<UC_CATALOG>.<UC_SCHEMA>.<industry>_metrics`
 *
 * Two deliberate divergences from the validation harness:
 *   - the REALTIME trigger is `5 minutes` (product invariant — never change it),
 *     whereas the validation write-test runs `15 seconds`;
 *   - the checkpoint lives under `.../stream_checkpoints/<mode>` (a stable
 *     deploy path) rather than the validation-only `.../validation/<mode>`
 *     path, which is torn down at cleanup.
 */
import { api, type AppDefaults } from "@/lib/api";
import type { DeployPayload } from "@/lib/admin-api";
import type { PipelineMode } from "@/lib/industries-api";
import {
  generateMicrobatchConsumerNotebook,
  generateRealtimeConsumerNotebook,
  type IndustryConsumerConfig,
} from "@/lib/notebook-template";
import { generateDataProducerNotebook } from "@/lib/data-producer-notebook";

/**
 * Resolve a workspace-specific value from appDefaults, raising if absent.
 * All workspace values must come from the backend (app.yaml env vars) — never
 * hardcode MSK bootstrap strings, service credential names, project names,
 * S3 URIs, or secret scope names in this file.
 */
function requireAppDefault(
  appDefaults: AppDefaults,
  key: keyof AppDefaults,
  label: string,
): string {
  const val = appDefaults[key] as string | undefined;
  if (!val) {
    throw new Error(
      `Cannot deploy: backend app default '${label}' (${key}) is not configured. ` +
        "Check that the corresponding env var is set in app.yaml.",
    );
  }
  return val;
}

/** Lakebase Postgres database — must be `databricks_postgres` (Lakehouse Sync). */
const LAKEBASE_DATABASE = "databricks_postgres";

/** Real-time LakeTS ChronoTable name. */
const REALTIME_PG_TABLE = "sensor_readings";

/** Product invariant — the real-time RTM trigger is always 5 minutes. */
const REALTIME_TRIGGER = "5 minutes";

/** Micro-batch processing-time trigger (mirrors the validation harness). */
const MICROBATCH_TRIGGER = "15 seconds";

/** Kafka partition count the topic pre-flight creates the topic with. */
const KAFKA_NUM_PARTITIONS = 3;

// Catalog sub-objects — mirror `services/industry_catalog_service.py` and
// `models/metrics_schema.py`.
// NOTE: ZEROBUS_SCHEMA / ZEROBUS_TABLE removed (Task 4) — the zerobus table
// now lives in the one-schema layout as `{uc_catalog}.{uc_schema}.{industry}_zerobus`,
// not in a per-industry catalog sub-schema.
const CHECKPOINT_VOLUME = "stream_checkpoints";

/** Catalog-name slug: lower-case, non-`[a-z0-9_]` collapsed to `_`
 * (mirrors `industry_catalog_service.catalog_name`). */
function catalogSlug(industry: string): string {
  return industry.toLowerCase().replace(/[^a-z0-9_]+/g, "_");
}

/** Topic slug: lower-case, non-`[a-z0-9]` collapsed to `_`
 * (mirrors `validation_config.topic_name`). */
function topicSlug(industry: string): string {
  return industry.toLowerCase().replace(/[^a-z0-9]+/g, "_");
}

export function industryTopic(industry: string, mode: PipelineMode): string {
  return `iot.${topicSlug(industry)}.${mode}`;
}

function checkpointLocation(
  catalog: string,
  schema: string,
  industry: string,
  mode: PipelineMode,
): string {
  // Micro-batch switched sink type (.toTable() -> .foreachBatch()); a Structured
  // Streaming checkpoint records the sink, so the new query needs a FRESH
  // checkpoint path — bump the suffix so an existing pipeline redeploys cleanly
  // instead of failing with a sink-mismatch on the old commit log.
  const suffix = mode === "microbatch" ? `${mode}_fb` : mode;
  return `/Volumes/${catalog}/${schema}/${CHECKPOINT_VOLUME}/${industry}/${suffix}`;
}

/**
 * Return the FQN of the managed metrics table in the one-schema layout:
 * ``{uc_catalog}.{uc_schema}.{industry}_metrics`` (Task 3/4).
 *
 * This mirrors `industry_catalog_service.industry_table(industry, "metrics")`.
 * The mode is no longer encoded in the table name — a shared managed table
 * carries all modes, distinguished by the ``mode`` column.
 */
function managedMetricsTableFqn(
  uc_catalog: string,
  uc_schema: string,
  industry: string,
): string {
  return `${uc_catalog}.${uc_schema}.${catalogSlug(industry)}_metrics`;
}

/** Kafka producer connection shared by the two Spark modes — MSK-IAM handled
 * entirely by the UC service credential (no inline SASL keys). */
function kafkaProducerConnection(
  industry: string,
  topic: string,
  bootstrapServers: string,
  serviceCredential: string,
) {
  return {
    kafka: {
      bootstrap_servers: bootstrapServers,
      topic,
      consumer_group: `iot-${topicSlug(industry)}`,
      security_protocol: "SASL_SSL",
      service_credential: serviceCredential,
      num_partitions: KAFKA_NUM_PARTITIONS,
    },
  };
}

/**
 * Build the deploy payload for one mode: uploads the simulator wheel, resolves
 * the app defaults (Lakebase project, workspace url, zerobus creds), generates
 * the consumer + producer notebooks, and returns the `DeployPayload` with the
 * gate overridden (the caller guarantees the gate for a one-click start).
 *
 * `bootstrapServers` should be the setup row's configured bootstrap string.
 * Pass an empty string when the row has no configured bootstrap — the function
 * falls back to `appDefaults.msk_bootstrap` (set via MSK_BOOTSTRAP_SERVERS in
 * app.yaml) so no workspace-specific bootstrap string ever lives in this file.
 */
export async function buildIndustryDeployPayload(
  industry: string,
  mode: PipelineMode,
  bootstrapServers: string,
  producerRateHz: number,
): Promise<DeployPayload> {
  const [appDefaults, wheel] = await Promise.all([
    api.getAppDefaults(),
    api.uploadSimulatorWheel(),
  ]);
  const wheelPath = wheel.wheel_path;

  if (mode === "zerobus") {
    return buildZerobusPayload(
      industry,
      wheelPath,
      appDefaults,
      producerRateHz,
    );
  }

  // Resolve workspace values from backend app-defaults (never hardcode here).
  const serviceCredential = requireAppDefault(appDefaults, "service_credential", "UC_SERVICE_CREDENTIAL");
  const pipelinesProject = requireAppDefault(appDefaults, "pipelines_project", "PIPELINES_LAKEBASE_PROJECT");
  // One-schema layout: catalog/schema come from the backend (UC_CATALOG / UC_SCHEMA).
  const ucCatalog = requireAppDefault(appDefaults, "uc_catalog", "UC_CATALOG");
  const ucSchema = requireAppDefault(appDefaults, "uc_schema", "UC_SCHEMA");

  // Fall back to the workspace's configured MSK bootstrap when the setup row
  // has none; if neither is set the caller will receive a clear error below.
  const effectiveBootstrap = bootstrapServers || appDefaults.msk_bootstrap || "";

  const topic = industryTopic(industry, mode);

  // Managed metrics table in the one-schema layout — shared across modes,
  // distinguished by the `mode` column. Mirrors industry_table(industry, "metrics").
  const metricsTableFqnValue = managedMetricsTableFqn(ucCatalog, ucSchema, industry);

  const consumerConfig: IndustryConsumerConfig =
    mode === "realtime"
      ? {
          industry,
          mode: "realtime",
          topic,
          bootstrapServers: effectiveBootstrap,
          serviceCredential,
          checkpointLocation: checkpointLocation(ucCatalog, ucSchema, industry, mode),
          // The realtime sink lives on the SHARED per-pipeline Lakebase project
          // (PIPELINES_LAKEBASE_PROJECT / `get_industry_lakebase` / the freshness
          // probe), NOT the app-backend's own project from app-defaults.
          lakebaseProjectId: pipelinesProject,
          // Per-industry branch (branch name == industry) on that project.
          lakebaseBranch: industry,
          database: LAKEBASE_DATABASE,
          pgTable: REALTIME_PG_TABLE,
          metricsTableFqn: metricsTableFqnValue,
          zerobusEndpoint: appDefaults.zerobus_endpoint ?? "",
          zerobusClientId: appDefaults.zerobus_client_id ?? "",
          realtimeInterval: REALTIME_TRIGGER,
          // App SSE bridge — the consumer ALSO POSTs each batch to the app so
          // the live dashboard's (global) SSE stream shows this pipeline.
          appUrl: window.location.origin,
          pipelineId: `industry:${industry}`,
        }
      : {
          industry,
          mode: "microbatch",
          topic,
          bootstrapServers: effectiveBootstrap,
          serviceCredential,
          checkpointLocation: checkpointLocation(ucCatalog, ucSchema, industry, mode),
          deltaTable: `${ucCatalog}.${ucSchema}.${catalogSlug(industry)}_microbatch`,
          metricsTableFqn: metricsTableFqnValue,
          zerobusEndpoint: appDefaults.zerobus_endpoint ?? "",
          zerobusClientId: appDefaults.zerobus_client_id ?? "",
          microbatchInterval: MICROBATCH_TRIGGER,
        };

  const consumerNotebook =
    mode === "realtime"
      ? generateRealtimeConsumerNotebook(consumerConfig)
      : generateMicrobatchConsumerNotebook(consumerConfig);

  const producerNotebook = generateDataProducerNotebook({
    wheelPath,
    industry,
    updateRateHz: producerRateHz,
    sensorConfig: [],
    source: "kafka",
    connection: kafkaProducerConnection(industry, topic, effectiveBootstrap, serviceCredential),
  });

  return {
    consumer_notebook: consumerNotebook,
    producer_notebook: producerNotebook,
    gate_override: true,
  };
}

/** Zerobus has no Spark consumer — deploy only the serverless producer that
 * pushes synthetic rows straight into the managed Delta table via the Zerobus SDK.
 *
 * Target table: ``{uc_catalog}.{uc_schema}.{industry}_zerobus`` — the one-schema
 * managed table pre-created by provisioning.  Mirrors
 * ``industry_catalog_service.industry_table(industry, "zerobus")``.
 *
 * Authentication: the notebook reads the app SP OAuth client-secret from the UC
 * secret ``{uc_catalog}.{uc_schema}.app_sp_oauth`` via
 * ``dbutils.secrets.get(catalog=…, schema=…, key="app_sp_oauth")``.
 */
function buildZerobusPayload(
  industry: string,
  wheelPath: string,
  appDefaults: AppDefaults,
  producerRateHz: number,
): DeployPayload {
  const ucCatalog = requireAppDefault(appDefaults, "uc_catalog", "UC_CATALOG");
  const ucSchema = requireAppDefault(appDefaults, "uc_schema", "UC_SCHEMA");
  // Table FQN mirrors industry_table(industry, "zerobus") in Python.
  const zerobusTableFqn = `${ucCatalog}.${ucSchema}.${catalogSlug(industry)}_zerobus`;

  const producerNotebook = generateDataProducerNotebook({
    wheelPath,
    industry,
    updateRateHz: producerRateHz,
    sensorConfig: [],
    source: "zerobus",
    connection: {
      zerobus: {
        endpoint: requireAppDefault(appDefaults, "zerobus_endpoint", "ZEROBUS_SERVER_ENDPOINT"),
        table_name: zerobusTableFqn,
        // client_id is the app SP's non-secret OAuth client ID — passed in config.
        // client_secret is NOT passed: the notebook reads it from the UC secret
        // `{uc_catalog}.{uc_schema}.app_sp_oauth` via dbutils.secrets.get(catalog=…).
        client_id: requireAppDefault(appDefaults, "zerobus_client_id", "ZEROBUS_CLIENT_ID"),
        workspace_url: requireAppDefault(appDefaults, "workspace_url", "DATABRICKS_HOST"),
      },
    },
  });

  return {
    producer_notebook: producerNotebook,
    gate_override: true,
  };
}
