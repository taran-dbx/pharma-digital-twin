import type { SssConfig, SssSource, SssRtmSinkType, SssMicrobatchSink, LakebaseConnectionMode } from "@/lib/types";

/** Keys that should never be hardcoded in generated notebooks (credentials). */
const SENSITIVE_OPTION_KEYS = new Set([
  "sasl.username", "sasl.password",
  "kafka.sasl.username", "kafka.sasl.password",
  "sasl.jaas.config", "kafka.sasl.jaas.config",
  "ssl.keystore.password", "ssl.truststore.password", "ssl.key.password",
]);

function advancedOptionsLines(
  advanced?: Record<string, string>,
  excludeSensitive = false,
): string {
  if (!advanced) return "";
  return Object.entries(advanced)
    .filter(([k, v]) => v !== "" && !(excludeSensitive && SENSITIVE_OPTION_KEYS.has(k)))
    .map(([k, v]) => `    .option("${k}", "${v}")`)
    .join("\n");
}

function sourceReadStream(config: SssConfig): string {
  const src = config.source;
  const conn = config.connection;

  if (src === "kafka" && conn.kafka) {
    const k = conn.kafka;
    const cred = k.service_credential;
    // When using UC service credential, exclude sensitive keys — auth is handled by the credential
    const extra = advancedOptionsLines(k.advanced, !!cred);

    const credLine = cred ? `\n    .option("databricks.serviceCredential", "${cred}")` : "";
    const saslLine = cred ? "" : `\n    .option("kafka.security.protocol", "${k.security_protocol}")`;

    return `df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "${k.bootstrap_servers}")
    .option("subscribe", "${k.topic}")
    .option("kafka.group.id", "${k.consumer_group}")${saslLine}
    .option("startingOffsets", "${k.advanced?.startingOffsets || "latest"}")${credLine}
${extra ? extra + "\n" : ""}    .load()
)`;
  }

  if (src === "kinesis" && conn.kinesis) {
    const k = conn.kinesis;
    const cred = k.service_credential;
    const extra = advancedOptionsLines(k.advanced);
    const credLine = cred ? `\n    .option("serviceCredential", "${cred}")` : "";

    return `df = (
    spark.readStream
    .format("kinesis")
    .option("streamName", "${k.stream_name}")
    .option("region", "${k.region}")
    .option("initialPosition", "${k.advanced?.initialPosition || "latest"}")${credLine}
${extra ? extra + "\n" : ""}    .load()
)`;
  }

  if (src === "event_hub" && conn.event_hub) {
    const eh = conn.event_hub;
    const cred = eh.service_credential;
    const extra = advancedOptionsLines(eh.advanced);

    if (cred) {
      return `# Resolve Event Hub connection string via UC service credential
cred_provider = dbutils.credentials.getServiceCredentialsProvider("${cred}")
eh_conn_str = cred_provider.get("eventhub.connection_string", "")

eh_conf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(eh_conn_str)
}

df = (
    spark.readStream
    .format("eventhubs")
    .options(**eh_conf)
${extra ? extra + "\n" : ""}    .load()
)`;
    }

    return `eh_conf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
        "${eh.connection_string}"
    )
}

df = (
    spark.readStream
    .format("eventhubs")
    .options(**eh_conf)
${extra ? extra + "\n" : ""}    .load()
)`;
  }

  if (src === "rabbitmq" && conn.rabbitmq) {
    const r = conn.rabbitmq;
    const cred = r.service_credential;
    const extra = advancedOptionsLines(r.advanced);

    if (cred) {
      return `# Resolve RabbitMQ credentials via UC service credential
cred_provider = dbutils.credentials.getServiceCredentialsProvider("${cred}")
rmq_username = cred_provider.get("rabbitmq.username", "guest")
rmq_password = cred_provider.get("rabbitmq.password", "")

df = (
    spark.readStream
    .format("org.apache.spark.sql.rabbitmq")
    .option("host", "${r.host}")
    .option("port", "${r.port}")
    .option("queueName", "${r.queue}")
    .option("username", rmq_username)
    .option("password", rmq_password)
${extra ? extra + "\n" : ""}    .load()
)`;
    }

    return `df = (
    spark.readStream
    .format("org.apache.spark.sql.rabbitmq")
    .option("host", "${r.host}")
    .option("port", "${r.port}")
    .option("queueName", "${r.queue}")
    .option("username", "${r.username}")
${extra ? extra + "\n" : ""}    .load()
)`;
  }

  if (src === "pubsub" && conn.pubsub) {
    const p = conn.pubsub;
    const cred = p.service_credential;
    const extra = advancedOptionsLines(p.advanced);

    if (cred) {
      return `# Resolve Pub/Sub credentials via UC service credential
cred_provider = dbutils.credentials.getServiceCredentialsProvider("${cred}")
gcp_credentials_json = cred_provider.get("pubsub.credentials_json", "")

df = (
    spark.readStream
    .format("pubsub")
    .option("projectId", "${p.project_id}")
    .option("subscriptionId", "${p.subscription}")
    .option("credentialsJson", gcp_credentials_json)
${extra ? extra + "\n" : ""}    .load()
)`;
    }

    return `df = (
    spark.readStream
    .format("pubsub")
    .option("projectId", "${p.project_id}")
    .option("subscriptionId", "${p.subscription}")
${extra ? extra + "\n" : ""}    .load()
)`;
  }

  return `# TODO: Configure readStream for source "${src}"
df = spark.readStream.format("rate").load()`;
}

function triggerClause(config: SssConfig): string {
  if (config.processing_mode === "realtime") {
    return `.trigger(realTime="5 minutes")`;
  }
  const interval = config.spark_conf["spark.sql.streaming.trigger.processingTime"] || "10 seconds";
  return `.trigger(processingTime="${interval}")`;
}

function metricsTableSetupBlock(config: SssConfig): string {
  if (!config.metrics.enabled || !config.metrics.target_table) return "";

  const table = config.metrics.target_table;

  return `
# Metrics table: ${table} (provisioned via Global Settings — UC Catalog tab)
print(f"Using metrics table: ${table}")
`;
}

function metricsListenerBlock(config: SssConfig): string {
  if (!config.metrics.enabled) return "";

  const zbCfg = config.metrics;
  const useZerobus = !!(zbCfg.zerobus_endpoint && zbCfg.zerobus_client_id && zbCfg.zerobus_client_secret);

  if (useZerobus) {
    const endpoint = zbCfg.zerobus_endpoint!;
    const wsUrl = zbCfg.zerobus_workspace_url || "";
    const table = config.metrics.target_table;
    const scopeName = zbCfg.zerobus_client_id ? (config.secret_scope || "") : "";

    return `
# ---------------------------------------------------------------------------
# Streaming Metrics Listener (Zerobus)
# ---------------------------------------------------------------------------
from pyspark.sql.streaming import StreamingQueryListener
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties

_zb_client_id = dbutils.secrets.get("${scopeName}", "zerobus-client-id")
_zb_client_secret = dbutils.secrets.get("${scopeName}", "zerobus-client-secret")

_zb_sdk = ZerobusSdk("${endpoint}", "${wsUrl}")
_zb_stream = _zb_sdk.create_stream(
    _zb_client_id,
    _zb_client_secret,
    TableProperties("${table}"),
    StreamConfigurationOptions(record_type=RecordType.JSON),
)

class MetricsListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"Query started: {event.id}")

    def onQueryProgress(self, event):
        import json as _json
        p = event.progress
        row = {
            "query_id": str(p.id),
            "timestamp": p.timestamp,
            "batch_id": int(p.batchId),
            "metrics_json": _json.dumps(p.jsonValue()),
        }
        try:
            _zb_stream.ingest_record_offset(row)
        except Exception as e:
            print(f"Metrics write error: {e}")

    def onQueryTerminated(self, event):
        try:
            _zb_stream.close()
        except Exception:
            pass
        print(f"Query terminated: {event.id}")

spark.streams.addListener(MetricsListener())
`;
  }

  return `
# ---------------------------------------------------------------------------
# Streaming Metrics Listener (Delta fallback)
# ---------------------------------------------------------------------------
from pyspark.sql.streaming import StreamingQueryListener
import json

class MetricsListener(StreamingQueryListener):
    METRICS_TABLE = "${config.metrics.target_table}"

    def onQueryStarted(self, event):
        print(f"Query started: {event.id}")

    def onQueryProgress(self, event):
        p = event.progress
        row = {
            "query_id": str(p.id),
            "timestamp": p.timestamp,
            "batch_id": p.batchId,
            "metrics_json": json.dumps(p.jsonValue()),
        }
        try:
            spark.createDataFrame([row]).write.mode("append").saveAsTable(self.METRICS_TABLE)
        except Exception as e:
            print(f"Metrics write error: {e}")

    def onQueryTerminated(self, event):
        print(f"Query terminated: {event.id}")

spark.streams.addListener(MetricsListener())
`;
}

function sparkConfBlock(config: SssConfig): string {
  const entries = Object.entries(config.spark_conf).filter(
    ([k]) => !k.startsWith("spark.sql.streaming.trigger")
  );
  if (entries.length === 0) return "";

  const lines = entries
    .map(([k, v]) => `spark.conf.set("${k}", "${v}")`)
    .join("\n");

  return `
# ---------------------------------------------------------------------------
# Spark Configuration
# ---------------------------------------------------------------------------
${lines}
`;
}

function lakebaseCredentialsBlock(config: SssConfig): string {
  const mode: LakebaseConnectionMode = config.lakebase_mode || "direct";
  const pgTable = config.lakebase?.pg_table || "sensor_readings";

  if (mode === "autoscale") {
    const auto = config.lakebase_autoscale;
    const projectId = auto?.project_id || "";
    const branchId = auto?.branch_id || "production";
    const db = auto?.database || "databricks_postgres";

    // When project_id is empty, emit a runtime resolver that tries a
    // cluster widget first, then fails with a clear, actionable message.
    const projectIdBlock = projectId
      ? `project_id = "${projectId}"`
      : `# lakebase_autoscale.project_id was not set in the pipeline wizard.
# Try a job/notebook widget, then fail clearly so the user knows what to fix.
try:
    project_id = dbutils.widgets.get("lakebase_project_id").strip()
except Exception:
    project_id = ""
if not project_id:
    raise RuntimeError(
        "Lakebase project_id is not configured. "
        "Fill 'Project ID' in the Sink sub-step of the pipeline wizard "
        "(Pipeline Config → Sink → Lakebase Autoscale), "
        "or pass 'lakebase_project_id' as a notebook widget."
    )`;

    return `
# ---------------------------------------------------------------------------
# Lakebase Autoscale — OAuth-native: resolve host via REST API,
# credentials fetched per-partition in LakebaseForeachWriter.open()
# ---------------------------------------------------------------------------
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

${projectIdBlock}
branch_id = "${branchId}"

# Discover the default branch (branch_id may be a logical name like "production")
_branches = w.api_client.do("GET", f"/api/2.0/postgres/projects/{project_id}/branches")
_branch_list = _branches.get("branches", [])
if not _branch_list:
    raise RuntimeError(f"No Lakebase branches found for project '{project_id}'")

# Use branch_id if it matches, otherwise pick the default branch
_branch = None
for b in _branch_list:
    if branch_id in (b.get("uid", ""), b.get("name", "").split("/")[-1]):
        _branch = b
        break
    if b.get("status", {}).get("default"):
        _branch = b
if _branch is None:
    _branch = _branch_list[0]

_branch_name = _branch["name"]
print(f"Using Lakebase branch: {_branch_name}")

# Discover the read-write endpoint for this branch
_ep_resp = w.api_client.do("GET", f"/api/2.0/postgres/{_branch_name}/endpoints")
_endpoints = _ep_resp.get("endpoints", [])
if not _endpoints:
    raise RuntimeError(f"No Lakebase endpoints found for branch {_branch_name}")

PG_HOST = _endpoints[0]["status"]["hosts"]["host"]
PG_DB = "${db}"
PG_PORT = 5432
PG_TABLE = "${pgTable}"

# Endpoint path used by POST /api/2.0/postgres/credentials — driver builds
# this once and broadcasts to executors, which mint fresh OAuth tokens per
# partition open. No static secrets required.
ENDPOINT_NAME = f"{_branch_name}/endpoints/primary"

print(f"Lakebase host: {PG_HOST}")
print(f"Lakebase endpoint path: {ENDPOINT_NAME}")
`;
  }

  const lb = config.lakebase;
  const host = lb?.host || "<LAKEBASE_HOST>";
  const port = lb?.port || "5432";
  const db = lb?.database || "databricks_postgres";
  // Direct mode still needs an endpoint path for OAuth credential minting —
  // infer a sensible default from project/branch when available, otherwise
  // leave as a placeholder users can replace post-hoc.
  const auto = config.lakebase_autoscale;
  const directProject = auto?.project_id || "<PROJECT_ID>";
  const directBranch = auto?.branch_id || "production";

  return `
# ---------------------------------------------------------------------------
# Lakebase Direct Connection — OAuth-native
# ---------------------------------------------------------------------------
PG_HOST = "${host}"
PG_PORT = ${port}
PG_DB = "${db}"
PG_TABLE = "${pgTable}"

# OAuth credential minting needs the Lakebase endpoint path. Direct mode
# doesn't discover this, so fall back to the branch from lakebase_autoscale
# if the user filled it in the wizard.
ENDPOINT_NAME = "projects/${directProject}/branches/${directBranch}/endpoints/primary"
`;
}

function lakebaseForeachWriterBlock(config: SssConfig): string {
  return `${lakebaseCredentialsBlock(config)}
# ---------------------------------------------------------------------------
# Lakebase ForeachWriter — now a NO-OP.
#
# Historically we INSERTed directly from Spark executors via psycopg2 with a
# fresh OAuth token per partition. In practice that pattern had several
# failure modes on long-running streams:
#   - Tokens broadcast at driver start aged out after ~1h → silent auth fails
#   - executemany inside asyncpg pool rolled back when the connection returned
#     to the pool without an explicit transaction commit
#   - Executors caught up on hours-old Kafka backlogs, producing rows whose
#     event_time was stale enough to never show up in "last 5 minutes" views
#
# We now write through the app instead. AppForeachWriter POSTs rows to
# /api/sss/ingest with an X-Pipeline-Id header; the FastAPI app owns
# connection pooling and token refresh and stamps event_time=NOW() so
# the dashboard's live window always sees fresh rows.
# ---------------------------------------------------------------------------

class LakebaseForeachWriter:
    """No-op — kept as a class so existing DualForeachWriter wiring compiles.
    All real writes happen in AppForeachWriter -> app -> pipeline Lakebase."""

    MAX_BUFFER_SIZE = 1

    def __init__(self):
        pass

    def open(self, partition_id, epoch_id):
        return True

    def process(self, row):
        pass

    def close(self, error):
        pass
`;
}

function appForeachWriterBlock(config: SssConfig): string {
  const appUrl = config.websocket_sink?.app_url || "<APP_URL>";
  const pipelineId = config.pipeline_id || "";

  return `
# ---------------------------------------------------------------------------
# App ForeachWriter — POSTs rows to the FastAPI app's /api/sss/ingest.
#
# The app handles SSE broadcast AND Lakebase persistence (keyed by
# X-Pipeline-Id header). This is the single code path for both; Lakebase
# writes live in the app's OAuth-refreshing pool rather than per-executor
# connections that age out mid-stream.
#
# Tokens for the app callback expire ~1h; we mint a fresh one per
# open() so long-running streams don't stall after the first hour.
# ---------------------------------------------------------------------------
import json
import urllib.request
import urllib.error

APP_URL = "${appUrl}"
PIPELINE_ID = "${pipelineId}"

# Client credentials for the workspace's OAuth client flow. We stash them on
# the driver once, then broadcast — executors mint short-lived tokens on
# every open() so nothing ever goes stale.
import requests as _req
_workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
_ci = dbutils.secrets.get(scope, "zerobus-client-id")
_cs = dbutils.secrets.get(scope, "zerobus-client-secret")
print(f"App OAuth client id present: {bool(_ci)}")

app_url_bc = spark.sparkContext.broadcast(APP_URL)
app_pipeline_id_bc = spark.sparkContext.broadcast(PIPELINE_ID)
app_workspace_url_bc = spark.sparkContext.broadcast(_workspace_url)
app_client_id_bc = spark.sparkContext.broadcast(_ci)
app_client_secret_bc = spark.sparkContext.broadcast(_cs)

class AppForeachWriter:
    """ForeachWriter that POSTs sensor rows to the FastAPI app. The app
    broadcasts them over SSE and (given X-Pipeline-Id) inserts into the
    pipeline's Lakebase. Mints its own OAuth token per partition open so
    the writer survives arbitrarily long streams without token rotation
    back on the driver."""

    MAX_BUFFER_SIZE = ${config.ws_buffer_size ?? 20}

    def __init__(self):
        self.buffer = []
        self.base_url = None
        self.token = None

    def _mint_token(self):
        resp = _req.post(
            f"{app_workspace_url_bc.value}/oidc/v1/token",
            data={
                "grant_type": "client_credentials",
                "client_id": app_client_id_bc.value,
                "client_secret": app_client_secret_bc.value,
                "scope": "all-apis",
            },
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()["access_token"]

    def open(self, partition_id, epoch_id):
        self.base_url = app_url_bc.value
        try:
            self.token = self._mint_token()
        except Exception as e:
            print(f"[AppForeachWriter] token mint failed: {e}")
            return False
        self.buffer = []
        return True

    def process(self, row):
        # Skip rows with null/missing core fields — float(None) would crash
        # the stream query and trigger Spark's retry-from-checkpoint loop.
        if row.value is None or row.sensor_name is None:
            return
        self.buffer.append({
            "sensor_name": row.sensor_name,
            "industry": row.industry,
            "value": float(row.value),
            "unit": row.unit,
            "sensor_type": row.sensor_type,
            "timestamp": row.timestamp,
            "fault_active": bool(row.fault_active) if row.fault_active is not None else False,
        })
        if len(self.buffer) >= self.MAX_BUFFER_SIZE:
            self._flush()

    def _post(self):
        payload = json.dumps(self.buffer).encode("utf-8")
        req = urllib.request.Request(
            f"{self.base_url}/api/sss/ingest",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.token}",
                "X-Pipeline-Id": app_pipeline_id_bc.value,
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            pass

    def _flush(self):
        if not self.buffer:
            return
        try:
            self._post()
        except urllib.error.HTTPError as e:
            if e.code in (401, 403):
                # Token expired mid-partition — re-mint and retry once.
                try:
                    self.token = self._mint_token()
                    self._post()
                except Exception as e2:
                    print(f"[AppForeachWriter] retry after {e.code} failed: {e2}")
            else:
                try:
                    body_snippet = e.read()[:200]
                except Exception:
                    body_snippet = b""
                print(f"[AppForeachWriter] HTTP {e.code}: {body_snippet!r}")
        except Exception as e:
            print(f"[AppForeachWriter] POST failed: {type(e).__name__}: {e}")
        self.buffer.clear()

    def close(self, error):
        if not error:
            try:
                self._flush()
            except Exception:
                pass
`;
}

function dualForeachWriterBlock(_config: SssConfig): string {
  return `
# ---------------------------------------------------------------------------
# Dual ForeachWriter — writes to both Lakebase and WebSocket
# ---------------------------------------------------------------------------
class DualForeachWriter:
    """Delegates to both LakebaseForeachWriter and AppForeachWriter."""

    def __init__(self):
        self.lakebase = LakebaseForeachWriter()
        self.app = AppForeachWriter()

    def open(self, partition_id, epoch_id):
        lb_ok = self.lakebase.open(partition_id, epoch_id)
        app_ok = self.app.open(partition_id, epoch_id)
        return lb_ok and app_ok

    def process(self, row):
        self.lakebase.process(row)
        self.app.process(row)

    def close(self, error):
        self.lakebase.close(error)
        self.app.close(error)
`;
}

function rtmSinkBlock(config: SssConfig): string {
  const sink = config.rtm_sink;
  if (!sink) return "";

  const parts: string[] = [];

  // "lakebase" sink means writes go through the app (/api/sss/ingest with
  // X-Pipeline-Id), which in turn persists to the pipeline's Lakebase.
  // AppForeachWriter is therefore always needed for any real-time sink type
  // that must reach Lakebase. LakebaseForeachWriter is a no-op kept only
  // for the "both" path (DualForeachWriter references it).
  if (sink === "both") {
    parts.push(lakebaseForeachWriterBlock(config));
  }
  // All sink types that need to reach the app (and from there, Lakebase)
  if (sink === "lakebase" || sink === "websocket" || sink === "both") {
    parts.push(appForeachWriterBlock(config));
  }
  if (sink === "both") {
    parts.push(dualForeachWriterBlock(config));
  }

  return parts.join("\n");
}

function rtmWriterInstance(sink: SssRtmSinkType): string {
  switch (sink) {
    case "lakebase":
      // Lakebase writes go through the app's /api/sss/ingest endpoint.
      // AppForeachWriter POSTs each batch with X-Pipeline-Id so the app can
      // route the insert to the correct pipeline's Lakebase.
      return "AppForeachWriter()";
    case "websocket":
      return "AppForeachWriter()";
    case "both":
      return "DualForeachWriter()";
  }
}

function rtmSinkLabel(sink: SssRtmSinkType): string {
  switch (sink) {
    case "lakebase":
      return "Lakebase (PostgreSQL)";
    case "websocket":
      return "WebSocket (App)";
    case "both":
      return "Lakebase + WebSocket";
  }
}

function microbatchSinkLabel(sinks: SssMicrobatchSink[]): string {
  const labels: Record<SssMicrobatchSink, string> = {
    delta: "Delta Table",
    lakebase: "Lakebase (PostgreSQL)",
    websocket: "WebSocket (App)",
  };
  return sinks.map((s) => labels[s]).join(" + ");
}

function microbatchForeachBatchBlock(config: SssConfig): string {
  const sinks = config.microbatch_sinks || ["delta"];
  const hasDelta = sinks.includes("delta");
  const hasLakebase = sinks.includes("lakebase");
  const hasWebsocket = sinks.includes("websocket");

  const lines: string[] = [];

  lines.push(`def write_micro_batch(batch_df, batch_id):`);
  lines.push(`    if batch_df.isEmpty():`);
  lines.push(`        return`);

  if (hasDelta) {
    lines.push(``);
    lines.push(`    # Write to Delta table`);
    lines.push(`    batch_df.write.mode("append").saveAsTable("${config.sink_table || "<catalog.schema.table>"}")`);
  }

  if (hasLakebase) {
    lines.push(``);
    lines.push(`    # Write to Lakebase via JDBC batch insert — OAuth-native`);
    lines.push(`    rows = batch_df.collect()`);
    lines.push(`    if rows:`);
    lines.push(`        import psycopg2, psycopg2.extras`);
    lines.push(`        from datetime import datetime, timezone`);
    lines.push(`        from databricks.sdk import WorkspaceClient`);
    lines.push(`        _w_mb = WorkspaceClient()`);
    lines.push(`        _cred_mb = _w_mb.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": ENDPOINT_NAME})`);
    lines.push(`        _pg_user_mb = _w_mb.current_user.me().user_name`);
    lines.push(`        _pg_password_mb = _cred_mb["token"]`);
    lines.push(`        conn = psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=_pg_user_mb, password=_pg_password_mb, port=PG_PORT, sslmode="require")`);
    lines.push(`        cur = conn.cursor()`);
    lines.push(`        values = [`);
    lines.push(`            (datetime.fromtimestamp(r.timestamp, tz=timezone.utc), r.sensor_name, r.industry, r.value, r.unit, r.sensor_type, r.fault_active)`);
    lines.push(`            for r in rows`);
    lines.push(`        ]`);
    lines.push(`        for i in range(0, len(values), ${config.lakebase_buffer_size ?? 50}):`);
    lines.push(`            psycopg2.extras.execute_values(`);
    lines.push(`                cur, """INSERT INTO {t} (event_time, sensor_name, industry, value, unit, sensor_type, fault_active, source) VALUES %s""".format(t=PG_TABLE),`);
    lines.push(`                values[i:i+${config.lakebase_buffer_size ?? 50}], template="(%s, %s, %s, %s, %s, %s, %s, 'sss')"`);
    lines.push(`            )`);
    lines.push(`        conn.commit()`);
    lines.push(`        cur.close()`);
    lines.push(`        conn.close()`);
  }

  if (hasWebsocket) {
    lines.push(``);
    lines.push(`    # POST to WebSocket App`);
    lines.push(`    import json, urllib.request`);
    lines.push(`    rows = [{"sensor_name": r.sensor_name, "industry": r.industry, "value": float(r.value), "unit": r.unit, "sensor_type": r.sensor_type, "timestamp": r.timestamp, "fault_active": r.fault_active} for r in batch_df.collect()]`);
    lines.push(`    for i in range(0, len(rows), ${config.ws_buffer_size ?? 20}):`);
    lines.push(`        chunk = rows[i:i+${config.ws_buffer_size ?? 20}]`);
    lines.push(`        payload = json.dumps(chunk).encode("utf-8")`);
    lines.push(`        req = urllib.request.Request(f"{APP_URL}/api/sss/ingest", data=payload, headers={"Content-Type": "application/json", "Authorization": f"Bearer {APP_TOKEN}"}, method="POST")`);
    lines.push(`        try:`);
    lines.push(`            urllib.request.urlopen(req, timeout=10)`);
    lines.push(`        except Exception as e:`);
    lines.push(`            print(f"[foreachBatch] HTTP POST failed: {e}")`);
  }

  return lines.join("\n");
}

export function generateNotebook(config: SssConfig): string {
  const sourceBlock = sourceReadStream(config);
  const trigger = triggerClause(config);
  const sparkConf = sparkConfBlock(config);
  const metricsTableSetup = metricsTableSetupBlock(config);
  const metricsListener = metricsListenerBlock(config);
  const useZerobusMetrics = config.metrics.enabled && !!(
    config.metrics.zerobus_endpoint && config.metrics.zerobus_client_id && config.metrics.zerobus_client_secret
  );
  const zbMetricsPip = useZerobusMetrics
    ? `
# ---------------------------------------------------------------------------
# Install Zerobus Ingest SDK for streaming metrics
# ---------------------------------------------------------------------------
import subprocess as _sp
_sp.check_call(["pip", "install", "databricks-zerobus-ingest-sdk", "-q"])
`
    : "";

  const mode = config.processing_mode === "realtime" ? "Real-Time Mode" : "Microbatch";
  const srcLabel = sourceLabel(config.source);
  const isRtm = config.processing_mode === "realtime" && config.rtm_sink;

  const needsPsycopg2 = isRtm
    ? (config.rtm_sink === "lakebase" || config.rtm_sink === "both")
    : (config.microbatch_sinks || []).includes("lakebase");
  const psycopg2Pip = needsPsycopg2
    ? `
# ---------------------------------------------------------------------------
# Ensure psycopg2 is available for Lakebase writes, and that executors have
# a modern databricks-sdk with api_client.do for OAuth credential minting.
# ---------------------------------------------------------------------------
import subprocess as _sp_pg
_sp_pg.check_call(["pip", "install", "psycopg2-binary", "--upgrade", "databricks-sdk", "-q"])
`
    : "";
  const mbSinks = config.microbatch_sinks || [];
  const isMbMultiSink = config.processing_mode === "microbatch" && (mbSinks.length > 1 || mbSinks.includes("lakebase") || mbSinks.includes("websocket"));

  const sinkLabel = isRtm
    ? rtmSinkLabel(config.rtm_sink!)
    : isMbMultiSink
      ? microbatchSinkLabel(mbSinks as SssMicrobatchSink[])
      : `\`${config.sink_table || "<table>"}\``;

  const foreachWriterDefs = isRtm ? rtmSinkBlock(config) : "";

  // Generate .observe() for custom metrics so they appear in progress.observedMetrics
  const customMetrics = config.metrics.custom_metrics || [];
  const observeBlock = customMetrics.length > 0
    ? `
# ---------------------------------------------------------------------------
# Custom Observed Metrics
# ---------------------------------------------------------------------------
from pyspark.sql.functions import expr as _expr

parsed = parsed.observe(
    "custom_metrics",
${customMetrics.map((m) => `    _expr("${m.expression}").alias("${m.name}")`).join(",\n")}
)
print("Registered ${customMetrics.length} custom observed metric(s): ${customMetrics.map((m) => m.name).join(", ")}")
`
    : "";

  let writeBlock: string;
  if (isRtm) {
    writeBlock = `writer = ${rtmWriterInstance(config.rtm_sink!)}

query = (
    parsed.writeStream
    .foreach(writer)
    .outputMode("update")
    .option("checkpointLocation", "${config.checkpoint_location || "/checkpoints/" + (config.sink_table || "bronze")}")
    ${trigger}
    .start()
)`;
  } else if (isMbMultiSink) {
    const mbOutputMode = config.processing_mode === "realtime" ? "update" : "append";
    writeBlock = `${microbatchForeachBatchBlock(config)}

query = (
    parsed.writeStream
    .foreachBatch(write_micro_batch)
    .outputMode("${mbOutputMode}")
    .option("checkpointLocation", "${config.checkpoint_location || "/checkpoints/" + (config.sink_table || "bronze")}")
    ${trigger}
    .start()
)`;
  } else {
    writeBlock = `query = (
    parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "${config.checkpoint_location || "/checkpoints/" + (config.sink_table || "bronze")}")
    ${trigger}
    .toTable("${config.sink_table || "<catalog.schema.table>"}")
)`;
  }

  // scope is needed whenever AppForeachWriter is generated (it reads
  // dbutils.secrets.get(scope, "zerobus-client-id/secret") to mint tokens).
  // That covers all rtm_sink values ("lakebase", "websocket", "both") and
  // microbatch websocket sinks.
  const needsSecretsScope = (isRtm && !!config.rtm_sink)
    || (isMbMultiSink && mbSinks.includes("websocket"));
  const workspaceScope = config.secret_scope || "";
  const secretsSetup = needsSecretsScope
    ? `
# ---------------------------------------------------------------------------
# Workspace Secrets scope
# ---------------------------------------------------------------------------
scope = "${workspaceScope}"
`
    : "";

  const needsLakebaseCreds = isRtm ? (config.rtm_sink === "lakebase" || config.rtm_sink === "both") : mbSinks.includes("lakebase");
  const lakebaseCredsBlock = needsLakebaseCreds && !isRtm ? lakebaseCredentialsBlock(config) : "";

  const needsAppUrl = isMbMultiSink && mbSinks.includes("websocket");
  const appUrlBlock = needsAppUrl
    ? `
APP_URL = "${config.websocket_sink?.app_url || "<APP_URL>"}"

# Generate OAuth M2M token for app callback
import requests as _req
_workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
_ci = dbutils.secrets.get(scope, "zerobus-client-id")
_cs = dbutils.secrets.get(scope, "zerobus-client-secret")
_token_resp = _req.post(
    f"{_workspace_url}/oidc/v1/token",
    data={"grant_type": "client_credentials", "client_id": _ci, "client_secret": _cs, "scope": "all-apis"},
)
_token_resp.raise_for_status()
APP_TOKEN = _token_resp.json()["access_token"]
print(f"OAuth M2M token obtained (length={len(APP_TOKEN)})")
`
    : "";

  const writeLabel = isRtm ? " via foreach()" : isMbMultiSink ? " via foreachBatch()" : " to Delta Bronze";

  const sections = [
    `# Databricks notebook source`,
    `print("=" * 60)`,
    `print("IoT Digital Twin — Spark Structured Streaming")`,
    `print("Source: ${srcLabel}  |  Mode: ${mode}  |  Sink: ${sinkLabel}")`,
    `print("=" * 60)`,
  ];

  if (sparkConf) sections.push(sparkConf.trim());
  if (secretsSetup) sections.push(secretsSetup.trim());
  if (lakebaseCredsBlock) sections.push(lakebaseCredsBlock.trim());
  if (appUrlBlock) sections.push(appUrlBlock.trim());
  if (metricsTableSetup) sections.push(metricsTableSetup.trim());
  if (zbMetricsPip) sections.push(zbMetricsPip.trim());
  if (psycopg2Pip) sections.push(psycopg2Pip.trim());

  sections.push(`# ---------------------------------------------------------------------------
# Read Stream — ${srcLabel}
# ---------------------------------------------------------------------------
${sourceBlock}`);

  if (metricsListener) sections.push(metricsListener.trim());
  if (foreachWriterDefs) sections.push(foreachWriterDefs.trim());

  sections.push(`# ---------------------------------------------------------------------------
# Parse payload and write${writeLabel}
# ---------------------------------------------------------------------------
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, BooleanType, MapType

# NOTE: This parse schema is kept in lock-step with the SSOT field-map in
# models/sensor_schema.py (all 11 SensorRecord fields). tests/test_sensor_schema.py
# fails if it drifts. Epoch "timestamp" is parsed as DoubleType and mapped to the
# event_time TIMESTAMPTZ column at insert time; "metadata" is a MAP.
schema = (
    StructType()
    .add("sensor_name", StringType())
    .add("industry", StringType())
    .add("value", DoubleType())
    .add("unit", StringType())
    .add("sensor_type", StringType())
    .add("timestamp", DoubleType())
    .add("min_value", DoubleType())
    .add("max_value", DoubleType())
    .add("nominal_value", DoubleType())
    .add("fault_active", BooleanType())
    .add("metadata", MapType(StringType(), StringType()))
)

parsed = (
    df.selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .withColumn("ingestion_time", current_timestamp())
)
${observeBlock}
${writeBlock}

try:
    print("[SSS] Streaming query started — awaiting termination …")
    query.awaitTermination()
except Exception as e:
    print(f"[SSS] Streaming query failed: {e}")
    import traceback
    traceback.print_exc()
    raise`);

  return sections.join("\n\n");
}

function sourceLabel(src: SssSource | null): string {
  const labels: Record<string, string> = {
    kafka: "Apache Kafka",
    kinesis: "Amazon Kinesis",
    event_hub: "Azure Event Hub",
    rabbitmq: "RabbitMQ",
    pubsub: "Google Pub/Sub",
  };
  return src ? labels[src] || src : "Unknown";
}

// ===========================================================================
// Industry-first pipeline consumers (spike-VERIFIED 2026-07-05, DBR 18.2 RTM).
//
// These generators emit the exact MSK -> Spark -> (Lakebase | Delta) consumer
// notebooks that were proven end-to-end in the Phase-0/Phase-4 spikes (spec
// §1.10, §1.11, §1.15). They are deliberately Kafka-only and self-contained,
// separate from the legacy multi-source `generateNotebook` above.
//
// Verified invariants (asserted by tests/test_consumer_notebook_templates.py):
//   * RTM/micro-batch read Kafka with ONLY `databricks.serviceCredential` —
//     NO `kafka.sasl.*` / `kafka.security.protocol` (the UC service credential
//     handles MSK-IAM).
//   * Real-time writer: `.outputMode("update").foreach(LakebaseWriter())
//     .trigger(realTime="15 seconds")`; ForeachWriter.process() indexes a
//     POSITIONAL tuple and INSERTs into the LakeTS ChronoTable via a psycopg
//     connection to the Lakebase branch PRIMARY endpoint (driver-minted OAuth
//     token via `w.postgres.generate_database_credential`, broadcast to
//     executors — never mint on the worker).
//   * Micro-batch writer: Delta `.trigger(processingTime="10 seconds")`.
//   * Streaming metrics: a StreamingQueryListener that BUFFERS progress rows
//     into a queue and returns immediately (no Spark work in the callback); a
//     background daemon thread flushes batches via delta-rs
//     `write_deltalake(mode="append")` to the UC EXTERNAL metrics table
//     (progress kept as STRING; UC temp-table-credentials regenerated per
//     batch; `AWS_REGION=us-east-1` + `AWS_S3_ALLOW_UNSAFE_RENAME=true`).
// ===========================================================================

const NB_SEP = "\n\n# COMMAND ----------\n\n";
const NB_HEADER = "# Databricks notebook source";
// DEFAULT_SERVICE_CREDENTIAL and DEFAULT_APP_SECRET_SCOPE removed — workspace
// values must come from the injected IndustryConsumerConfig (populated from
// appDefaults in industry-deploy-config.ts), never hardcoded here.
const DEFAULT_LAKEBASE_BRANCH = "production";
const DEFAULT_LAKEBASE_DATABASE = "databricks_postgres";
const DEFAULT_PG_TABLE = "sensor_readings";
const DEFAULT_REALTIME_INTERVAL = "15 seconds";
const DEFAULT_MICROBATCH_INTERVAL = "15 seconds";

export interface IndustryConsumerConfig {
  /** Industry key, e.g. "pharmaceutical" (row values only; schema invariant). */
  industry: string;
  /** Only the two Spark modes generate a consumer (zerobus is producer-only). */
  mode: "realtime" | "microbatch";
  /** Kafka topic — `iot.<industry>.<mode>` (dot notation, §1.14). */
  topic: string;
  /** Public `:9198` bootstrap list (private `:9098` is unreachable, §1.7). */
  bootstrapServers: string;
  /** UC service credential mapped to the MSK-IAM role (default `iot-rtm-sc`). */
  serviceCredential?: string;
  /** Streaming checkpoint location (a distinct one is used for validation). */
  checkpointLocation: string;

  // --- real-time (Lakebase LakeTS ChronoTable) target ---
  /** `iot-pipelines` per-industry branch project id. */
  lakebaseProjectId?: string;
  /** Never-expiring per-industry branch (default `production`). */
  lakebaseBranch?: string;
  /** Postgres database — must be `databricks_postgres` (Lakehouse Sync, G1). */
  database?: string;
  /** ChronoTable name (default `sensor_readings`). */
  pgTable?: string;

  // --- micro-batch (UC Delta) target ---
  /** Fully-qualified Delta table `iot_<industry>.microbatch.<table>`. */
  deltaTable?: string;

  // --- streaming metrics (both Spark modes, always on §1.11) ---
  /** Managed metrics table FQN: ``{uc_catalog}.{uc_schema}.{industry}_metrics``. */
  metricsTableFqn?: string;
  /** @deprecated External metrics table S3 location — no longer used (Task 4
   * switched to Zerobus push into the managed table). Kept optional so existing
   * call sites don't need immediate cleanup. */
  metricsTableLocation?: string;
  /** Zerobus ingest endpoint for the metrics writer (from appDefaults). */
  zerobusEndpoint?: string;
  /** App SP OAuth client-id for the metrics Zerobus stream (from appDefaults). */
  zerobusClientId?: string;

  /** Real-time trigger interval (default `15 seconds`). */
  realtimeInterval?: string;
  /** Micro-batch trigger interval (default `10 seconds`). */
  microbatchInterval?: string;

  // --- real-time SSE bridge (app callback for the live dashboard) ---
  /** App public origin (e.g. `https://<app>.databricksapps.com`). When set on a
   * realtime consumer, each micro-batch is ALSO POSTed to `${appUrl}/api/sss/ingest`
   * so the dashboard's global SSE stream (KPIs / 3D twin) shows this pipeline.
   * Lakebase persistence still happens in the psycopg `LakebaseWriter`. */
  appUrl?: string;
  /** `X-Pipeline-Id` header value for the app POST. For industry-first realtime
   * pipelines this is `industry:<industry>` — matching `get_industry_lakebase`'s
   * cache key — which tells `sss_ingest` to broadcast-only (no duplicate insert). */
  pipelineId?: string;
  /** Workspace secret scope holding `zerobus-client-id`/`zerobus-client-secret`
   * used to mint the OAuth token for the app POST (default `iot-secrets`). */
  secretScope?: string;
}

/**
 * Spike-VERIFIED Kafka read shared by both Spark modes: MSK-IAM is handled
 * entirely by the UC service credential, so this read carries **no**
 * `kafka.sasl.*` / `kafka.security.protocol` options.  Changing this breaks the
 * proven RTM path — the guard test forbids sasl/security.protocol here.
 */
const kafkaReadTemplate = (serviceCredential: string): string => `raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    # Read only NEW messages. Starting from the earliest offset replays historical
    # rows whose event_time is older than the LakeTS ChronoTable's rolling
    # partitions → psycopg CheckViolation (no partition of sensor_readings for
    # row). Live pipelines only need current data; the producer streams fresh rows.
    .option("startingOffsets", "latest")
    .option("databricks.serviceCredential", "${serviceCredential}")
    .load()
)`;

/**
 * SSOT `from_json` parse schema — all 11 canonical SensorRecord fields, kept in
 * lock-step with models/sensor_schema.py (raw epoch `timestamp` as DoubleType,
 * `metadata` as a MAP; the epoch->TIMESTAMPTZ transform happens on select).
 */
const SSOT_FROM_JSON_SCHEMA = `SCHEMA = (
    StructType()
    .add("sensor_name", StringType())
    .add("industry", StringType())
    .add("value", DoubleType())
    .add("unit", StringType())
    .add("sensor_type", StringType())
    .add("timestamp", DoubleType())
    .add("min_value", DoubleType())
    .add("max_value", DoubleType())
    .add("nominal_value", DoubleType())
    .add("fault_active", BooleanType())
    .add("metadata", MapType(StringType(), StringType()))
)`;

/**
 * Buffered Zerobus streaming-metrics listener (Task 4, §1.11, S1-S6).
 *
 * The callback only enqueues — a daemon thread flushes rows via the Zerobus
 * SDK into ``{uc_catalog}.{uc_schema}.{industry}_metrics`` (managed table,
 * pre-created by provisioning).  ``saveAsTable`` / Spark write / delta-rs are
 * deliberately NOT used.
 *
 * Columns written: mode, query_id, query_name, run_id, batch_id, timestamp
 * (epoch-µs INT), num_input_rows, input_rps, processed_rps, trigger_ms,
 * e2e_p50/p99, processing_p50/p99, queuing_p50/p99.
 * Raw ``progress`` blob dropped (managed table — no delta-rs STRING constraint).
 */
function metricsListenerBlockIndustry(cfg: IndustryConsumerConfig): string {
  const fqn = cfg.metricsTableFqn || "<uc_catalog>.<uc_schema>.<industry>_metrics";
  const zbEndpoint = cfg.zerobusEndpoint || "";
  const zbClientId = cfg.zerobusClientId || "";
  const mode = cfg.mode;
  return `# ---------------------------------------------------------------------------
# Streaming metrics — buffered async Zerobus push to ${fqn}.
#   * onQueryProgress() ONLY enqueues — no Spark work in the callback.
#   * A daemon thread flushes via Zerobus SDK ingest_record() (never saveAsTable,
#     never Spark write, never delta-rs).
#   * Auth: UC secret {catalog}.{schema}.app_sp_oauth (same schema as the table).
#     Primary: dbutils.secrets.get(catalog=..., schema=..., key="app_sp_oauth") [env >= 4].
#     Fallback: UC-secrets REST API via the notebook SDK auth.
#   * timestamp = epoch-microseconds INT (Zerobus maps to TIMESTAMP column).
#   * Raw progress blob dropped — managed table, no delta-rs STRING constraint.
# ---------------------------------------------------------------------------
import json as _json
import queue as _queue
import threading as _threading
from datetime import datetime, timezone
from pyspark.sql.streaming import StreamingQueryListener

METRICS_TABLE = "${fqn}"
MODE = "${mode}"

# Derive catalog/schema from the managed-table FQN to read the UC secret.
_m_parts = METRICS_TABLE.split(".")
_zb_uc_catalog = _m_parts[0]
_zb_uc_schema  = _m_parts[1]
try:
    _zb_client_secret = dbutils.secrets.get(
        catalog=_zb_uc_catalog, schema=_zb_uc_schema, key="app_sp_oauth"
    )
except Exception as _dbs_e:
    from databricks.sdk import WorkspaceClient as _WC_m
    _secret_path = f"{_zb_uc_catalog}.{_zb_uc_schema}.app_sp_oauth"
    _zb_client_secret = _WC_m().api_client.do(
        "GET",
        f"/api/2.1/unity-catalog/secrets/{_secret_path}",
        query={"include_value": "true"},
    ).get("value", "")

ZB_ENDPOINT = "${zbEndpoint}"
ZB_CLIENT_ID = "${zbClientId}"
_zb_ws_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl', '')}"
_zb_ws_url = _zb_ws_url if _zb_ws_url.startswith("http") else f"https://{_zb_ws_url}"

from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties

_zb_metrics_sdk = ZerobusSdk(ZB_ENDPOINT, _zb_ws_url)


def _open_metrics_stream(client_secret):
    return _zb_metrics_sdk.create_stream(
        ZB_CLIENT_ID,
        client_secret,
        TableProperties(METRICS_TABLE),
        StreamConfigurationOptions(record_type=RecordType.JSON),
    )


_zb_metrics_stream = _open_metrics_stream(_zb_client_secret)

_metrics_q: "_queue.Queue" = _queue.Queue()


def _lat_f(x):
    return float(x) if x is not None else None


def _metrics_record(progress_json: str) -> dict:
    p = _json.loads(progress_json)
    dur = p.get("durationMs", {}) or {}
    # RTM exposes latency percentiles under a top-level "latencies" object with
    # UPPERCASE percentile keys (P50/P99).
    lat = p.get("latencies", {}) or {}
    e2e = lat.get("e2eLatencyMs", {}) or {}
    proc = lat.get("processingLatencyMs", {}) or {}
    queuing = lat.get("sourceQueuingLatencyMs", {}) or {}
    # Micro-batch progress has NO RTM "latencies" object — fall back to the batch
    # execution time from durationMs.addBatch as the processing-latency KPI.
    _proc_fallback = float(dur.get("addBatch", 0.0) or 0.0) if not proc else None
    return {
        "mode": MODE,
        "query_id": str(p.get("id", "")),
        "query_name": p.get("name") or "",
        "run_id": str(p.get("runId", "")),
        "batch_id": int(p.get("batchId", 0)),
        # epoch-microseconds INT — Zerobus maps to TIMESTAMP column.
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
        "num_input_rows": int(p.get("numInputRows", 0)),
        "input_rps": float(p.get("inputRowsPerSecond", 0.0) or 0.0),
        "processed_rps": float(p.get("processedRowsPerSecond", 0.0) or 0.0),
        "trigger_ms": float(dur.get("triggerExecution", 0.0) or 0.0),
        "e2e_p50": _lat_f(e2e.get("P50")),
        "e2e_p99": _lat_f(e2e.get("P99")),
        "processing_p50": _lat_f(proc.get("P50")) if proc else _proc_fallback,
        "processing_p99": _lat_f(proc.get("P99")) if proc else _proc_fallback,
        "queuing_p50": _lat_f(queuing.get("P50")),
        "queuing_p99": _lat_f(queuing.get("P99")),
        # progress blob deliberately omitted — managed table; consumers
        # query the typed columns, not raw JSON.
    }


def _metrics_flush_loop():
    while True:
        batch = [_metrics_q.get()]  # block for the first row
        while True:
            try:
                batch.append(_metrics_q.get_nowait())
            except _queue.Empty:
                break
        for rec in batch:
            try:
                ack = _zb_metrics_stream.ingest_record(rec)
                try:
                    ack.wait_for_ack(timeout_sec=30)
                except TypeError:
                    ack.wait_for_ack()
            except Exception as _e:  # never kill the daemon
                print(f"[metrics] zerobus ingest failed: {type(_e).__name__}: {_e}")


_threading.Thread(target=_metrics_flush_loop, daemon=True).start()


class BufferedDeltaMetricsListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"[metrics] query started: {event.id}")

    def onQueryProgress(self, event):
        # BUFFER ONLY — return immediately. No Spark work in the callback.
        try:
            # Spark 4.1 (DBR 18.x): StreamingQueryProgress.json is a STRING
            # PROPERTY, not a method — calling it raises "'str' object is not
            # callable". Older runtimes expose json() as a method. Read it
            # defensively so the listener works on both.
            _pj = getattr(event.progress, "json", None)
            _progress_json = _pj() if callable(_pj) else (
                _pj if isinstance(_pj, str) else _json.dumps(event.progress)
            )
            _metrics_q.put(_metrics_record(_progress_json))
        except Exception as _e:
            print(f"[metrics] enqueue failed: {type(_e).__name__}: {_e}")

    def onQueryTerminated(self, event):
        print(f"[metrics] query terminated: {event.id}")


spark.streams.addListener(BufferedDeltaMetricsListener())`;
}


function realtimeConfigCell(cfg: IndustryConsumerConfig): string {
  const cred = cfg.serviceCredential || "";
  return `INDUSTRY = "${cfg.industry}"
TOPIC = "${cfg.topic}"
BOOTSTRAP = "${cfg.bootstrapServers}"
SERVICE_CREDENTIAL = "${cred}"
CHECKPOINT = "${cfg.checkpointLocation}"

# Lakebase branch PRIMARY (READ_WRITE) endpoint — writes go to primary.
PROJECT_ID = "${cfg.lakebaseProjectId || "<lakebase_project_id>"}"
BRANCH_ID = "${cfg.lakebaseBranch || DEFAULT_LAKEBASE_BRANCH}"
PG_DATABASE = "${cfg.database || DEFAULT_LAKEBASE_DATABASE}"
PG_TABLE = "${cfg.pgTable || DEFAULT_PG_TABLE}"
TRIGGER_INTERVAL = "${cfg.realtimeInterval || DEFAULT_REALTIME_INTERVAL}"

# App SSE bridge — POST each micro-batch to the app so the live dashboard's
# global SSE stream (KPIs / 3D twin) shows this pipeline. Lakebase persistence
# still happens in LakebaseWriter below; this is a broadcast-only side channel.
APP_URL = "${cfg.appUrl || "<APP_URL>"}"
PIPELINE_ID = "${cfg.pipelineId || `industry:${cfg.industry}`}"
APP_SECRET_SCOPE = "${cfg.secretScope || ""}"`;
}

const RTM_SPARK_CONF = `# Real-Time Mode (RTM) Spark configuration (spike-verified, §1).
# On the DEPLOYED job cluster these are set at cluster-creation time
# (build_job_cluster_spec) — they are STATIC cluster confs, so re-setting them
# here at runtime raises [CANNOT_MODIFY_CONFIG]. Set defensively so the notebook
# works whether or not the cluster already pre-set them.
for _rtm_k, _rtm_v in [
    ("spark.databricks.streaming.realTimeMode.enabled", "true"),
    ("spark.databricks.streaming.realTimeMode.streamStreamJoin.enabled", "true"),
    ("spark.databricks.streaming.realTimeMode.controlMessage.enabled", "true"),
]:
    try:
        if spark.conf.get(_rtm_k, None) != _rtm_v:
            spark.conf.set(_rtm_k, _rtm_v)
    except Exception as _rtm_e:
        print(f"[rtm-conf] {_rtm_k} already fixed at cluster level (skipping): {_rtm_e}")`;

const LAKEBASE_FOREACH_WRITER = `# ---------------------------------------------------------------------------
# LakebaseWriter — batched psycopg ForeachWriter to the Lakebase branch PRIMARY
# endpoint. Rows are buffered and flushed with psycopg2.extras.execute_values
# (BATCH_SIZE or MAX_BATCH_AGE_S) so RTM acknowledgements stay sub-second
# without a per-row autocommit round-trip.
#
# Driver fail-fast mints once (WorkspaceClient default auth) so a bad endpoint
# fails the notebook before the stream starts. Executors must NOT call
# WorkspaceClient — that raises invalid_client. They remint Postgres passwords
# on every connect via the same OIDC client_credentials flow AppSseWriter uses,
# then POST /api/2.0/postgres/credentials. Reconnect therefore gets a new token
# instead of reusing an hour-old broadcast that Postgres rejects as
# "OAuth: User is not authorized".
#
# GOTCHA (spike-verified): RTM ForeachWriter.process() receives a POSITIONAL
# tuple, NOT a named Row — index the row (row[0]..row[10]), do not use r.field.
# Column order matches the select() below and the INSERT column list.
# ---------------------------------------------------------------------------
import json
import os
import time as _lb_time
import psycopg2
import requests as _lb_req
from psycopg2.extras import execute_values
from databricks.sdk import WorkspaceClient

# SSE still needs explicit SP client creds on executors (HTTP OIDC). Lakebase
# writes do NOT: mint the DB token here on the driver with default auth.
_lb_ws_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
_lb_ci = dbutils.secrets.get(APP_SECRET_SCOPE, "sss-industries-client-id")


def _read_app_sp_secret():
    return dbutils.secrets.get(APP_SECRET_SCOPE, "sss-industries-client-secret")


_lb_cs = _read_app_sp_secret()
# Target the writable PRIMARY endpoint BY NAME. On these branches endpoint_type
# is None and list_endpoints returns the read REPLICA first, so any type-based
# selection falls back to the replica → "cannot execute INSERT in a read-only
# transaction". The .../endpoints/primary endpoint is the read-write one — the
# same resolution the app uses (pipeline_db._resolve_endpoint_host).
_lb_primary_name = f"projects/{PROJECT_ID}/branches/{BRANCH_ID}/endpoints/primary"
_lb_w = WorkspaceClient()
_lb_host = _lb_w.postgres.get_endpoint(name=_lb_primary_name).status.hosts.host
_lb_pg_user = os.getenv("DATABRICKS_CLIENT_ID") or _lb_w.current_user.me().user_name
_lb_w.postgres.generate_database_credential(endpoint=_lb_primary_name)
print(f"[LakebaseWriter] primary endpoint {_lb_host} as {_lb_pg_user} (executor remints via OIDC)")

_lb_ci_bc = spark.sparkContext.broadcast(_lb_ci)
_lb_cs_bc = spark.sparkContext.broadcast(_lb_cs)
_lb_ws_url_bc = spark.sparkContext.broadcast(_lb_ws_url)
_lb_host_bc = spark.sparkContext.broadcast(_lb_host)
_lb_user_bc = spark.sparkContext.broadcast(_lb_pg_user)
_lb_primary_name_bc = spark.sparkContext.broadcast(_lb_primary_name)

_INSERT_SQL = (
    "INSERT INTO {t} (sensor_name, industry, value, unit, sensor_type, "
    "event_time, min_value, max_value, nominal_value, fault_active, metadata, "
    "source) VALUES %s"
)
_INSERT_TEMPLATE = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'sss')"


class LakebaseWriter:
    TOKEN_TTL_S = 20 * 60  # remint well before the ~1h OAuth token expiry
    BATCH_SIZE = 100
    MAX_BATCH_AGE_S = 0.10  # flush bound for sub-second active-stream latency
    MAX_RECONNECT_ATTEMPTS = 12
    BACKOFF_BASE_S = 1.0
    BACKOFF_MAX_S = 30.0

    def _fresh_pg_token(self):
        # Same OIDC client_credentials path as AppSseWriter. Do not construct
        # WorkspaceClient on executors (invalid_client).
        oidc = _lb_req.post(
            f"{_lb_ws_url_bc.value}/oidc/v1/token",
            data={
                "grant_type": "client_credentials",
                "client_id": _lb_ci_bc.value,
                "client_secret": _lb_cs_bc.value,
                "scope": "all-apis",
            },
            timeout=15,
        )
        oidc.raise_for_status()
        cred = _lb_req.post(
            f"{_lb_ws_url_bc.value}/api/2.0/postgres/credentials",
            headers={
                "Authorization": f"Bearer {oidc.json()['access_token']}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            json={"endpoint": _lb_primary_name_bc.value},
            timeout=15,
        )
        cred.raise_for_status()
        return cred.json()["token"]

    def _connect(self):
        self.conn = psycopg2.connect(
            host=_lb_host_bc.value,
            port=5432,
            dbname=PG_DATABASE,
            user=_lb_user_bc.value,
            password=self._fresh_pg_token(),
            sslmode="require",
        )
        self.conn.autocommit = False
        self.cur = self.conn.cursor()
        self.sql = _INSERT_SQL.format(t=PG_TABLE)
        self._connected_at = _lb_time.monotonic()

    def _disconnect(self):
        try:
            if getattr(self, "cur", None) is not None:
                self.cur.close()
        except Exception:
            pass
        try:
            if getattr(self, "conn", None) is not None:
                self.conn.close()
        except Exception:
            pass
        self.cur = None
        self.conn = None

    def _reconnect(self):
        self._connect_with_backoff()

    def _delay(self, attempt):
        return min(self.BACKOFF_BASE_S * (2 ** (attempt - 1)), self.BACKOFF_MAX_S)

    def _connect_with_backoff(self):
        last_error = None
        for attempt in range(1, self.MAX_RECONNECT_ATTEMPTS + 1):
            try:
                self._disconnect()
                self._connect()
                return
            except (psycopg2.OperationalError, psycopg2.InterfaceError, _lb_req.RequestException) as exc:
                last_error = exc
                if attempt == self.MAX_RECONNECT_ATTEMPTS:
                    raise
                delay = self._delay(attempt)
                print("[LakebaseWriter] connect retry in", delay, "seconds")
                _lb_time.sleep(delay)
        if last_error is not None:
            raise last_error

    def _rollback(self):
        if getattr(self, "conn", None) is None:
            return
        try:
            self.conn.rollback()
        except Exception:
            pass

    def _flush_with_backoff(self):
        if not self.buffer:
            return
        try:
            execute_values(self.cur, self.sql, self.buffer, template=_INSERT_TEMPLATE)
            self.conn.commit()
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            # A lost connection can hide a successful commit; retry preserves
            # at-least-once delivery and may duplicate that ambiguous batch.
            self._rollback()
            self._connect_with_backoff()
            try:
                execute_values(self.cur, self.sql, self.buffer, template=_INSERT_TEMPLATE)
                self.conn.commit()
            except Exception:
                self._rollback()
                raise
        except Exception:
            self._rollback()
            raise
        self.buffer = []
        self._last_flush = _lb_time.monotonic()

    def _flush(self):
        self._flush_with_backoff()

    def open(self, partition_id, epoch_id):
        self.buffer = []
        self._connect_with_backoff()
        self._last_flush = _lb_time.monotonic()
        return True

    def process(self, row):
        # Proactively remint Postgres OAuth before the current token expires.
        if _lb_time.monotonic() - getattr(self, "_connected_at", 0) > self.TOKEN_TTL_S:
            self._flush()
            self._reconnect()
        # row is a POSITIONAL tuple (not a named Row) — index by position.
        metadata = row[10]
        if metadata is not None and not isinstance(metadata, str):
            metadata = json.dumps(dict(metadata))
        self.buffer.append((
            row[0], row[1], row[2], row[3], row[4], row[5],
            row[6], row[7], row[8], row[9], metadata,
        ))
        if (
            len(self.buffer) >= self.BATCH_SIZE
            or _lb_time.monotonic() - self._last_flush >= self.MAX_BATCH_AGE_S
        ):
            self._flush()

    def close(self, error):
        try:
            if error is None:
                self._flush()
            else:
                self._rollback()
        finally:
            self._disconnect()
`;

const REALTIME_APP_SSE_WRITER = `# ---------------------------------------------------------------------------
# App SSE bridge — bounded, best-effort background delivery to /api/sss/ingest.
# ForeachWriter.process() only coalesces rows and performs non-blocking queue
# operations; OAuth and HTTP stay on a daemon sender so the Lakebase source of
# truth never waits on dashboard transport. When the queue is full, the oldest
# stale dashboard batch is discarded in favor of the latest sensor state.
# ---------------------------------------------------------------------------
import json as _sse_json
import queue as _sse_queue
import threading as _sse_threading
import time as _sse_time
import urllib.error as _sse_urlerr
import urllib.request as _sse_url

import requests as _sse_req

_sse_app_url_bc = spark.sparkContext.broadcast(APP_URL)
_sse_pipeline_id_bc = spark.sparkContext.broadcast(PIPELINE_ID)
# Reuse the SSS SP creds already broadcast by the LakebaseWriter block (this cell
# is only ever included AFTER LAKEBASE_FOREACH_WRITER in the realtime notebook).
# The app callback token is minted from the SAME SSS SP that owns the Lakebase
# write — one principal for both sinks.
_sse_ws_url_bc = _lb_ws_url_bc
_sse_ci_bc = _lb_ci_bc
_sse_cs_bc = _lb_cs_bc


class AppSseWriter:
    """Coalesces sensor rows for best-effort background dashboard delivery."""

    BATCH_SIZE = 100
    MAX_BATCH_AGE_S = 0.10
    QUEUE_SIZE = 2
    JOIN_TIMEOUT_S = 0.25
    _STOP = object()

    def __init__(self):
        self.buffer = []
        self.base_url = None
        self.queue = None
        self.sender = None
        self._last_enqueue = 0.0

    def _mint_token(self):
        response = _sse_req.post(
            f"{_sse_ws_url_bc.value}/oidc/v1/token",
            data={
                "grant_type": "client_credentials",
                "client_id": _sse_ci_bc.value,
                "client_secret": _sse_cs_bc.value,
                "scope": "all-apis",
            },
            timeout=15,
        )
        response.raise_for_status()
        return response.json()["access_token"]

    def open(self, partition_id, epoch_id):
        self.base_url = _sse_app_url_bc.value
        self.buffer = []
        self._last_enqueue = _sse_time.monotonic()
        self.queue = _sse_queue.Queue(maxsize=self.QUEUE_SIZE)
        self.sender = _sse_threading.Thread(
            target=self._sender_loop,
            args=(self.queue,),
            name=f"app-sse-{partition_id}-{epoch_id}",
            daemon=True,
        )
        self.sender.start()
        return True

    def process(self, row):
        # POSITIONAL tuple (same order as parsed.select()).
        if row[2] is None or row[0] is None:
            return
        event_time = row[5]
        self.buffer.append({
            "sensor_name": row[0],
            "industry": row[1],
            "value": float(row[2]),
            "unit": row[3],
            "sensor_type": row[4],
            "timestamp": (
                event_time.isoformat()
                if hasattr(event_time, "isoformat")
                else event_time
            ),
            "fault_active": bool(row[9]) if row[9] is not None else False,
        })
        if (
            len(self.buffer) >= self.BATCH_SIZE
            or _sse_time.monotonic() - self._last_enqueue >= self.MAX_BATCH_AGE_S
        ):
            self._enqueue()

    def _enqueue(self):
        if not self.buffer or self.queue is None:
            return
        batch = self.buffer
        self.buffer = []
        self._last_enqueue = _sse_time.monotonic()
        try:
            self.queue.put_nowait(batch)
            return
        except _sse_queue.Full:
            pass

        # Dashboard state is ephemeral: replace one stale queued batch with the
        # latest batch instead of applying backpressure to Lakebase processing.
        try:
            self.queue.get_nowait()
        except _sse_queue.Empty:
            pass
        try:
            self.queue.put_nowait(batch)
        except _sse_queue.Full:
            pass

    def _post(self, batch, token):
        payload = _sse_json.dumps(batch).encode("utf-8")
        request = _sse_url.Request(
            f"{self.base_url}/api/sss/ingest",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
                "X-Pipeline-Id": _sse_pipeline_id_bc.value,
            },
            method="POST",
        )
        with _sse_url.urlopen(request, timeout=15):
            pass

    def _send(self, batch, token):
        try:
            if token is None:
                token = self._mint_token()
            self._post(batch, token)
        except _sse_urlerr.HTTPError as error:
            if error.code in (401, 403):
                try:
                    token = self._mint_token()
                    self._post(batch, token)
                except Exception as retry_error:
                    print(
                        f"[AppSseWriter] retry after {error.code} failed: "
                        f"{retry_error}"
                    )
            else:
                print(f"[AppSseWriter] HTTP {error.code}")
        except Exception as error:
            print(f"[AppSseWriter] POST failed: {type(error).__name__}: {error}")
        return token

    def _sender_loop(self, sender_queue):
        token = None
        while True:
            batch = sender_queue.get()
            if batch is self._STOP:
                return
            token = self._send(batch, token)

    def _signal_stop(self):
        if self.queue is None:
            return
        for _ in range(self.QUEUE_SIZE + 1):
            try:
                self.queue.put_nowait(self._STOP)
                return
            except _sse_queue.Full:
                try:
                    self.queue.get_nowait()
                except _sse_queue.Empty:
                    pass

    def close(self, error):
        if error is None:
            self._enqueue()
        else:
            self.buffer = []
        self._signal_stop()
        if self.sender is not None:
            self.sender.join(timeout=self.JOIN_TIMEOUT_S)


class RealtimeDualWriter:
    """Fan each row to BOTH the Lakebase ChronoTable (source of truth — the
    persisted history the freshness probe + dashboard read) AND the app SSE
    bridge (live dashboard). Mirrors the legacy DualForeachWriter.

    Lakebase GATES the writer: if its open() fails we return False so Spark
    retries from checkpoint (unchanged from the LakebaseWriter-only shape). SSE
    is best-effort and isolated from open, process, and close failures so it
    never blocks or fails the persisted write."""

    def __init__(self):
        self.lakebase = LakebaseWriter()
        self.app = AppSseWriter()
        self._app_ok = False

    def open(self, partition_id, epoch_id):
        lb_ok = self.lakebase.open(partition_id, epoch_id)
        try:
            self._app_ok = self.app.open(partition_id, epoch_id)
        except Exception as app_error:
            print(f"[AppSseWriter] open failed: {app_error}")
            self._app_ok = False
        return lb_ok

    def process(self, row):
        self.lakebase.process(row)
        if self._app_ok:
            try:
                self.app.process(row)
            except Exception as app_error:
                print(f"[AppSseWriter] enqueue failed: {app_error}")

    def close(self, error):
        try:
            self.lakebase.close(error)
        finally:
            if self._app_ok:
                try:
                    self.app.close(error)
                except Exception as app_error:
                    print(f"[AppSseWriter] close failed: {app_error}")`;

/**
 * Real-time consumer notebook: MSK -> Spark RTM -> Lakebase LakeTS ChronoTable
 * via a psycopg ForeachWriter, with the buffered delta-rs metrics listener.
 */
export function generateRealtimeConsumerNotebook(cfg: IndustryConsumerConfig): string {
  const cred = cfg.serviceCredential || "";

  const pip = `# 18.2 cluster libs assumed: databricks-sdk==0.120.0, kafka-python>=3,
# aws-msk-iam-sasl-signer-python, psycopg2-binary, databricks-zerobus-ingest-sdk.
# Do NOT \`pip install --upgrade boto3\` on this runtime (breaks the kernel).
%pip install databricks-sdk==0.120.0 psycopg2-binary databricks-zerobus-ingest-sdk -q`;

  const parseBlock = `# ---------------------------------------------------------------------------
# Parse Kafka value JSON into the SSOT sensor schema, then transform to the
# ChronoTable column order (positional for the ForeachWriter). Epoch seconds
# -> event_time TIMESTAMPTZ via timestamp_seconds.
# ---------------------------------------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, DoubleType, BooleanType, MapType

${SSOT_FROM_JSON_SCHEMA}

parsed = (
    raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("d"))
    .select(
        F.col("d.sensor_name").alias("sensor_name"),
        F.col("d.industry").alias("industry"),
        F.col("d.value").alias("value"),
        F.col("d.unit").alias("unit"),
        F.col("d.sensor_type").alias("sensor_type"),
        F.timestamp_seconds(F.col("d.timestamp")).alias("event_time"),
        F.col("d.min_value").alias("min_value"),
        F.col("d.max_value").alias("max_value"),
        F.col("d.nominal_value").alias("nominal_value"),
        F.col("d.fault_active").alias("fault_active"),
        F.col("d.metadata").alias("metadata"),
    )
    # Fresh-only floor: drop replayed/stale rows whose event_time predates the
    # LakeTS ChronoTable's rolling partitions. A reused streaming checkpoint can
    # resume OLD Kafka offsets (so startingOffsets=latest alone isn't enough),
    # and inserting a stale event_time raises psycopg CheckViolation
    # "no partition of relation sensor_readings found for row".
    .filter(F.col("event_time") >= F.expr("current_timestamp() - INTERVAL 15 MINUTES"))
)`;

  const writeBlock = `# ---------------------------------------------------------------------------
# Real-time write: RealtimeDualWriter fans each row to BOTH sinks —
#   1. Lakebase ChronoTable (source of truth): direct executor psycopg write,
#      per-open OAuth mint + auto-reconnect (survives the ~1h token expiry).
#   2. App SSE bridge: POST to /api/sss/ingest so the live dashboard streams it.
# Lakebase GATES the writer (its open() must succeed); SSE is best-effort.
# ---------------------------------------------------------------------------
# CHECKPOINT HYGIENE: real-time mode is a LATEST-ONLY hot tier — it must never
# resume a stale checkpoint. A reused checkpoint replays OLD Kafka offsets whose
# event_time predates the LakeTS ChronoTable's rolling partitions -> psycopg
# CheckViolation "no partition of relation sensor_readings found for row" (and
# startingOffsets=latest is ignored once a checkpoint exists). Clearing the
# checkpoint on every (re)start guarantees a fresh start at latest — the correct
# semantics for a live hot tier (we only ever want current data).
#
# SECRET ROTATION: Databricks Apps rotate the app SP's OAuth secret and restart
# the app, which republishes it to the UC secret. Executors hold the copy read
# at start, so once token minting is rejected the stream fails; wait here for a
# secret the token endpoint accepts, re-broadcast it and restart the stream.
AUTH_RECOVERY_POLL_S = 60
AUTH_RECOVERY_MAX_WAIT_S = 30 * 60


def _is_sp_auth_failure(exc):
    text = str(exc)
    return "/oidc/v1/token" in text and ("401" in text or "invalid_client" in text)


def _sp_secret_accepted(client_secret):
    response = _lb_req.post(
        f"{_lb_ws_url}/oidc/v1/token",
        data={
            "grant_type": "client_credentials",
            "client_id": _lb_ci,
            "client_secret": client_secret,
            "scope": "all-apis",
        },
        timeout=15,
    )
    return response.status_code == 200


def _await_accepted_sp_secret():
    deadline = _lb_time.monotonic() + AUTH_RECOVERY_MAX_WAIT_S
    while _lb_time.monotonic() < deadline:
        try:
            client_secret = _read_app_sp_secret()
            if _sp_secret_accepted(client_secret):
                return client_secret
            print("[RTM] UC secret still rejected by the token endpoint; waiting for the app to republish it")
        except Exception as _sec_e:
            print(f"[RTM] secret check failed ({type(_sec_e).__name__}): {_sec_e}")
        _lb_time.sleep(AUTH_RECOVERY_POLL_S)
    raise RuntimeError(
        f"App SP OAuth secret was not republished within {AUTH_RECOVERY_MAX_WAIT_S // 60} min; "
        "failing the task so the continuous job restarts"
    )


def _start_query():
    try:
        dbutils.fs.rm(CHECKPOINT, recurse=True)
        print(f"[RTM] cleared checkpoint {CHECKPOINT} — starting fresh at latest")
    except Exception as _ck_e:
        print(f"[RTM] checkpoint clear skipped ({type(_ck_e).__name__}): {_ck_e}")
    return (
        parsed.writeStream
        .outputMode("update")
        .foreach(RealtimeDualWriter())
        .option("checkpointLocation", CHECKPOINT)
        .trigger(realTime=TRIGGER_INTERVAL)
        .start()
    )


while True:
    query = _start_query()
    print("[RTM] streaming query started — awaiting termination …")
    try:
        query.awaitTermination()
        break
    except Exception as _q_e:
        if not _is_sp_auth_failure(_q_e):
            raise
        print("[RTM] OAuth token minting was rejected (app SP secret rotated); waiting for a fresh secret")
    _lb_cs = _await_accepted_sp_secret()
    _lb_cs_bc = spark.sparkContext.broadcast(_lb_cs)
    _sse_cs_bc = _lb_cs_bc
    try:
        _zb_metrics_stream = _open_metrics_stream(_lb_cs)
    except Exception as _zb_e:
        print(f"[metrics] zerobus stream reopen failed ({type(_zb_e).__name__}): {_zb_e}")
    print("[RTM] fresh app SP secret accepted; restarting the stream")`;

  const cells = [
    NB_HEADER,
    pip,
    "dbutils.library.restartPython()",
    realtimeConfigCell(cfg),
    RTM_SPARK_CONF,
    `# ---------------------------------------------------------------------------
# Read Kafka (MSK-IAM via UC service credential "${cred}"; no sasl options).
# ---------------------------------------------------------------------------
${kafkaReadTemplate(cred)}`,
    parseBlock,
    // LakebaseWriter (direct executor psycopg, per-open mint + auto-reconnect)
    // MUST come before REALTIME_APP_SSE_WRITER — the latter reuses its broadcast
    // SSS SP creds, and RealtimeDualWriter (defined there) composes both.
    LAKEBASE_FOREACH_WRITER,
    REALTIME_APP_SSE_WRITER,
    metricsListenerBlockIndustry(cfg),
    writeBlock,
  ];
  return cells.join(NB_SEP);
}

/**
 * Micro-batch consumer notebook: MSK -> Spark micro-batch -> UC Delta table
 * (`processingTime="10 seconds"`), with the buffered delta-rs metrics listener.
 */
export function generateMicrobatchConsumerNotebook(cfg: IndustryConsumerConfig): string {
  const cred = cfg.serviceCredential || "";
  const deltaTable = cfg.deltaTable || "<catalog>.microbatch.<table>";
  const interval = cfg.microbatchInterval || DEFAULT_MICROBATCH_INTERVAL;
  const triggerSecs = parseInt(interval, 10) || 15;

  const pip = `# 18.2 cluster libs assumed: databricks-sdk==0.120.0, kafka-python>=3,
# aws-msk-iam-sasl-signer-python, databricks-zerobus-ingest-sdk.
# Do NOT \`pip install --upgrade boto3\` on this runtime (breaks the kernel).
%pip install databricks-sdk==0.120.0 databricks-zerobus-ingest-sdk -q`;

  const configCell = `INDUSTRY = "${cfg.industry}"
TOPIC = "${cfg.topic}"
BOOTSTRAP = "${cfg.bootstrapServers}"
SERVICE_CREDENTIAL = "${cred}"
CHECKPOINT = "${cfg.checkpointLocation}"
DELTA_TABLE = "${deltaTable}"
TRIGGER_INTERVAL = "${interval}"`;

  const parseBlock = `# ---------------------------------------------------------------------------
# Parse Kafka value JSON into the SSOT sensor schema (same read + schema as the
# real-time consumer — the schema is industry-invariant). Epoch seconds ->
# event_time TIMESTAMP; metadata stays a MAP for the Delta target.
# ---------------------------------------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, DoubleType, BooleanType, MapType

${SSOT_FROM_JSON_SCHEMA}

parsed = (
    raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("d"))
    .select(
        F.col("d.sensor_name").alias("sensor_name"),
        F.col("d.industry").alias("industry"),
        F.col("d.value").alias("value"),
        F.col("d.unit").alias("unit"),
        F.col("d.sensor_type").alias("sensor_type"),
        F.timestamp_seconds(F.col("d.timestamp")).alias("event_time"),
        F.col("d.min_value").alias("min_value"),
        F.col("d.max_value").alias("max_value"),
        F.col("d.nominal_value").alias("nominal_value"),
        F.col("d.fault_active").alias("fault_active"),
        F.col("d.metadata").alias("metadata"),
    )
)`;

  const writeBlock = `# ---------------------------------------------------------------------------
# Micro-batch write via foreachBatch: append to the UC Delta table AND record
# the REAL end-to-end latency for the batch. Micro-batch progress has no RTM
# "latencies" field, so we measure the event-time lag (now - event_time)
# percentiles here. The row is enqueued to the SAME _metrics_q the Zerobus
# metrics listener uses — MODE and epoch-µs timestamp match the managed table.
# ---------------------------------------------------------------------------
from datetime import datetime, timezone
import time as _mb_time

_TRIGGER_SECS = ${triggerSecs}


def write_micro_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    _t0 = _mb_time.monotonic()
    batch_df.write.format("delta").mode("append").saveAsTable(DELTA_TABLE)
    _write_ms = (_mb_time.monotonic() - _t0) * 1000.0
    try:
        _agg = batch_df.select(
            F.expr("percentile_approx(unix_millis(current_timestamp()) - unix_millis(event_time), 0.5)").alias("p50"),
            F.expr("percentile_approx(unix_millis(current_timestamp()) - unix_millis(event_time), 0.99)").alias("p99"),
            F.count(F.lit(1)).alias("n"),
        ).first()
        _n = int(_agg["n"] or 0)
        _e50 = float(_agg["p50"]) if _agg["p50"] is not None else None
        _e99 = float(_agg["p99"]) if _agg["p99"] is not None else None
        _rps = (_n / _TRIGGER_SECS) if _TRIGGER_SECS else float(_n)
        _metrics_q.put({
            "mode": MODE,
            "query_id": "", "query_name": "microbatch-e2e", "run_id": "",
            "batch_id": int(batch_id),
            # epoch-microseconds INT — Zerobus maps to TIMESTAMP column.
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
            "num_input_rows": _n, "input_rps": _rps, "processed_rps": _rps,
            "trigger_ms": _write_ms,
            "e2e_p50": _e50, "e2e_p99": _e99,
            "processing_p50": _write_ms, "processing_p99": _write_ms,
            "queuing_p50": (max(0.0, _e50 - _write_ms) if _e50 is not None else None),
            "queuing_p99": (max(0.0, _e99 - _write_ms) if _e99 is not None else None),
        })
    except Exception as _e:
        print(f"[micro-batch] e2e latency record failed: {type(_e).__name__}: {_e}")


query = (
    parsed.writeStream
    .foreachBatch(write_micro_batch)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="${interval}")
    .start()
)

print("[micro-batch] streaming query started — awaiting termination …")
query.awaitTermination()`;

  const cells = [
    NB_HEADER,
    pip,
    "dbutils.library.restartPython()",
    configCell,
    `# ---------------------------------------------------------------------------
# Read Kafka (MSK-IAM via UC service credential "${cred}"; no sasl options).
# ---------------------------------------------------------------------------
${kafkaReadTemplate(cred)}`,
    parseBlock,
    metricsListenerBlockIndustry(cfg),
    writeBlock,
  ];
  return cells.join(NB_SEP);
}
