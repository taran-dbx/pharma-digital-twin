/**
 * Unified data producer notebook generator.
 *
 * Generates a Databricks notebook that runs the IoT Simulator and pushes
 * records to the user-selected streaming source (Kafka, Kinesis, EventHub,
 * RabbitMQ, or Pub/Sub).
 *
 * Kafka uses the simulator's built-in KafkaSink. All other sources use
 * CallbackSink + the source's native cloud SDK.
 */
import { producerSignalPython } from "@/lib/manufacturing-scenario";
import type { SssSource, SssConnectionConfig } from "@/lib/types";

const SEP = "\n# COMMAND ----------\n";
const HEADER = "# Databricks notebook source";

/**
 * Generates a startup delay cell that waits for the SSS consumer cluster
 * to provision before the producer starts sending data.
 *
 * In a continuous job, all tasks start in parallel. The producer runs on
 * serverless (starts in ~10s) while the SSS consumer needs a job cluster
 * (~3-5 min to provision). This delay prevents data loss during cluster
 * startup.
 */
function generateStartupDelayCell(delaySec: number = 120): string {
  return `
import time, logging

logging.basicConfig(level=logging.INFO)
_startup_logger = logging.getLogger("startup_delay")

STARTUP_DELAY_SEC = ${delaySec}
_startup_logger.info(
    f"Waiting {STARTUP_DELAY_SEC}s for SSS consumer cluster to provision..."
)
time.sleep(STARTUP_DELAY_SEC)
_startup_logger.info("Startup delay complete — beginning data production.")
`.trim();
}

/**
 * A continuous job only starts a new run once every task has ended, so a dead
 * consumer next to a healthy producer stalls the pipeline. The producer watches
 * its sibling tasks and stops when one ends, which lets the job restart the run.
 * The run ids arrive as the `job_run_id` / `task_run_id` task parameters
 * (`PRODUCER_RUN_PARAMETERS` in deployment_service.py).
 * Needs `threading`, `_thread`, `time`, `logger`, and a `try: sim.run()` that
 * re-raises when `_peer_task_ended` is set.
 */
export const PEER_TASK_WATCH_PYTHON = `
from databricks.sdk import WorkspaceClient as _PeerWorkspaceClient

PEER_TASK_POLL_S = 30
_PEER_ENDED_STATES = {"TERMINATING", "TERMINATED", "INTERNAL_ERROR", "SKIPPED"}
_peer_task_ended = threading.Event()


def _job_run_ids():
    try:
        return dbutils.widgets.get("job_run_id"), dbutils.widgets.get("task_run_id")
    except Exception as exc:
        logger.info(f"[peer watch] no run id parameters ({type(exc).__name__}); watch disabled")
        return None, None


def _ended_peer_tasks(client, parent_run_id, own_run_id):
    run = client.jobs.get_run(int(parent_run_id))
    return [
        task.task_key
        for task in run.tasks or []
        if str(task.run_id) != str(own_run_id)
        and task.state is not None
        and task.state.life_cycle_state is not None
        and task.state.life_cycle_state.value in _PEER_ENDED_STATES
    ]


def _watch_peer_tasks(parent_run_id, own_run_id):
    client = _PeerWorkspaceClient()
    while True:
        time.sleep(PEER_TASK_POLL_S)
        try:
            ended = _ended_peer_tasks(client, parent_run_id, own_run_id)
        except Exception as exc:
            logger.warning(f"[peer watch] run lookup failed, retrying: {type(exc).__name__}: {exc}")
            continue
        if ended:
            logger.error(f"[peer watch] task(s) {ended} ended; stopping the producer so the job restarts the run")
            _peer_task_ended.set()
            _thread.interrupt_main()
            return


_peer_parent_run_id, _peer_own_run_id = _job_run_ids()
if _peer_parent_run_id and _peer_own_run_id:
    threading.Thread(
        target=_watch_peer_tasks, args=(_peer_parent_run_id, _peer_own_run_id), daemon=True
    ).start()
else:
    logger.info("[peer watch] not running as a multi-task job; watch disabled")
`.trim();

const PEER_TASK_ENDED_RAISE = `    if _peer_task_ended.is_set():
        raise RuntimeError("A sibling task ended; continuous job restart required") from None`;

export interface DataProducerNotebookConfig {
  wheelPath: string;
  industry: string;
  updateRateHz: number;
  sensorConfig: unknown[];
  source: SssSource;
  connection: SssConnectionConfig;
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------
export function generateDataProducerNotebook(config: DataProducerNotebookConfig): string {
  switch (config.source) {
    case "kafka":
      return generateKafkaProducer(config);
    case "kinesis":
      return generateKinesisProducer(config);
    case "event_hub":
      return generateEventHubProducer(config);
    case "rabbitmq":
      return generateRabbitMQProducer(config);
    case "pubsub":
      return generatePubSubProducer(config);
    case "zerobus":
      return generateZerobusProducer(config);
    default:
      throw new Error(`Unsupported producer source: ${config.source}`);
  }
}

/**
 * Returns the "destination" label for the producer (topic, stream, queue, etc.).
 * Used by the frontend to show what the producer is pushing to.
 */
export function getProducerDestination(
  source: SssSource,
  connection: SssConnectionConfig,
): string {
  switch (source) {
    case "kafka":
      return connection.kafka?.topic || "";
    case "kinesis":
      return connection.kinesis?.stream_name || "";
    case "event_hub":
      return connection.event_hub?.event_hub_name || "";
    case "rabbitmq":
      return connection.rabbitmq?.queue || "";
    case "pubsub":
      return connection.pubsub?.topic_id || connection.pubsub?.subscription || "";
    case "zerobus":
      return connection.zerobus?.table_name || "";
    default:
      return "";
  }
}

// ---------------------------------------------------------------------------
// Kafka producer (uses KafkaSink from the IoT simulator wheel)
// ---------------------------------------------------------------------------
function generateKafkaProducer(config: DataProducerNotebookConfig): string {
  const k = config.connection.kafka;
  if (!k) throw new Error("Kafka connection config is required");

  const { wheelPath, industry, updateRateHz } = config;
  const cred = k.service_credential?.trim() || "";
  const hasCred = !!cred;
  const hasInlineCreds = !!(k.advanced?.["sasl.username"] && k.advanced?.["sasl.password"]);

  // The hasCred (IAM/SASL service-credential) path runs an MSK-IAM topic
  // pre-flight that needs the AWS MSK SASL OAUTHBEARER token signer.
  const installCell = hasCred
    ? `%pip install "${wheelPath}[kafka]" aws-msk-iam-sasl-signer-python --quiet`
    : `%pip install "${wheelPath}[kafka]" --quiet`;
  const restartCell = "dbutils.library.restartPython()";

  let mainCell: string;

  if (hasCred) {
    const numPartitions = k.num_partitions ?? 3;
    mainCell = `
import json
import logging
import _thread
import threading
import time

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from botocore.credentials import CredentialProvider
from iot_simulator import Simulator
from iot_simulator.sinks.callback import CallbackSink
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

try:
    from kafka.net.sasl.oauth import AbstractTokenProvider
except ImportError:
    try:
        from kafka.sasl.oauth import AbstractTokenProvider
    except ImportError:
        from kafka.oauth.abstract import AbstractTokenProvider

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("iot_producer")

BOOTSTRAP = "${k.bootstrap_servers}"
TOPIC = "${k.topic}"
CRED_NAME = "${cred}"
NUM_PARTITIONS = ${numPartitions}
TARGET_RECORDS_PER_SEC = ${updateRateHz}

provider = dbutils.credentials.getServiceCredentialsProvider(CRED_NAME)
logger.info(f"UC credential provider type: {type(provider)}")

MSK_REGION = "us-east-1"
try:
    _host = BOOTSTRAP.split(",")[0].split(":")[0]
    _parts = _host.split(".")
    if "kafka" in _parts:
        MSK_REGION = _parts[_parts.index("kafka") + 1]
except (ValueError, IndexError):
    logger.warning(f"Could not derive region from bootstrap; using {MSK_REGION}")


class UCServiceCredentialProvider(CredentialProvider):
    """Load current, renewable credentials from the UC service credential."""

    METHOD = "databricks-uc-service-credential"
    CANONICAL_NAME = "DatabricksUCServiceCredential"

    def load(self):
        # The botocore session returned by dbutils owns refresh. Do not freeze
        # or copy its temporary values into the environment.
        return provider.get_credentials()


uc_credentials = UCServiceCredentialProvider()


class _MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = (
            MSKAuthTokenProvider.generate_auth_token_from_credentials_provider(
                MSK_REGION,
                uc_credentials,
            )
        )
        return token


# --- Pre-flight: ensure TOPIC exists on the cluster using MSK IAM auth ---
# The credentialed (IAM/SASL service-credential) path does NOT otherwise create
# the topic. MSK with auto.create.topics.enable=false silently queues messages
# against a missing topic forever — the #1 historical failure for MSK-IAM
# pipelines. This block is idempotent and non-fatal: any failure logs a warning
# and lets the producer continue.
def _ensure_topic_iam():
    logger.info(f"MSK IAM pre-flight using region {MSK_REGION!r}")

    admin = None
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP,
            security_protocol="SASL_SSL",
            sasl_mechanism="OAUTHBEARER",
            sasl_oauth_token_provider=_MSKTokenProvider(),
            request_timeout_ms=15000,
        )
        existing = admin.list_topics()
        if TOPIC in existing:
            logger.info(f"Topic {TOPIC!r} already exists on cluster.")
            return
        # Try RF=3 (3-broker clusters), fall back to RF=2 (2-broker clusters).
        for rf in (3, 2):
            try:
                logger.info(f"Topic {TOPIC!r} not found — creating with {NUM_PARTITIONS} partitions, RF={rf}.")
                admin.create_topics([NewTopic(name=TOPIC, num_partitions=NUM_PARTITIONS, replication_factor=rf)])
                logger.info(f"Created topic {TOPIC!r} (RF={rf}).")
                return
            except TopicAlreadyExistsError:
                logger.info(f"Topic {TOPIC!r} was created concurrently — continuing.")
                return
            except KafkaError as _rf_e:
                logger.warning(f"create_topics RF={rf} failed: {type(_rf_e).__name__}: {_rf_e}")
        logger.warning(f"Could not create topic {TOPIC!r} at RF=3 or RF=2 — continuing anyway.")
    finally:
        if admin is not None:
            try:
                admin.close()
            except Exception as close_error:
                logger.debug(f"Kafka admin close failed: {close_error}")

try:
    _ensure_topic_iam()
except Exception as _e:
    logger.warning(f"MSK IAM topic pre-flight error (continuing anyway): {type(_e).__name__}: {_e}")

# One long-lived producer keeps connection setup and Kafka delivery off Spark's
# critical path. send() is asynchronous; linger_ms=0 starts network delivery
# immediately, while Kafka may still combine records already waiting together.
logger.info("Using native async Kafka producer (linger_ms=0, acks=1)")
MAX_CONSECUTIVE_SEND_ERRORS = 20
SEND_STALL_TIMEOUT_S = 60
_send_counters = {
    "ok": 0,
    "err": 0,
    "enqueued": 0,
    "consecutive_err": 0,
    "last_err": None,
    "last_success_at": time.monotonic(),
}
_counter_lock = threading.Lock()
_fatal_send_error = threading.Event()


def _on_send_success(_metadata):
    with _counter_lock:
        _send_counters["ok"] += 1
        _send_counters["consecutive_err"] = 0
        _send_counters["last_success_at"] = time.monotonic()


def _on_send_error(exc):
    with _counter_lock:
        _send_counters["err"] += 1
        _send_counters["consecutive_err"] += 1
        _send_counters["last_err"] = f"{type(exc).__name__}: {exc}"
        should_stop = (
            _send_counters["consecutive_err"] >= MAX_CONSECUTIVE_SEND_ERRORS
            and not _fatal_send_error.is_set()
        )
        if should_stop:
            _fatal_send_error.set()
    if should_stop:
        logger.error(
            "Kafka producer reached %d consecutive send failures; "
            "failing the task so the continuous job can restart",
            MAX_CONSECUTIVE_SEND_ERRORS,
        )
        _thread.interrupt_main()


producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    security_protocol="SASL_SSL",
    sasl_mechanism="OAUTHBEARER",
    sasl_oauth_token_provider=_MSKTokenProvider(),
    linger_ms=0,
    acks=1,
    max_in_flight_requests_per_connection=5,
)


${producerSignalPython(industry)}


def on_records(records):
    if _fatal_send_error.is_set():
        raise RuntimeError(
            f"Kafka producer unhealthy after {MAX_CONSECUTIVE_SEND_ERRORS} "
            "consecutive send failures"
        )
    for record in records:
        # The event clock starts at the actual producer enqueue, not while the
        # simulator record waits in an internal queue.
        payload = json.loads(record.to_json())
        if not _production_allows(payload):
            continue
        payload["timestamp"] = time.time()
        payload = _apply_scenario(payload)
        future = producer.send(
            TOPIC,
            value=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            key=record.sensor_name.encode("utf-8"),
        )
        with _counter_lock:
            _send_counters["enqueued"] += 1
        future.add_callback(_on_send_success).add_errback(_on_send_error)


def _heartbeat():
    previous_ok = 0
    while True:
        time.sleep(15)
        with _counter_lock:
            snap = dict(_send_counters)
            stalled = (
                snap["enqueued"] > snap["ok"]
                and time.monotonic() - snap["last_success_at"]
                >= SEND_STALL_TIMEOUT_S
                and not _fatal_send_error.is_set()
            )
            if stalled:
                _fatal_send_error.set()
        sent_rps = (snap["ok"] - previous_ok) / 15.0
        previous_ok = snap["ok"]
        logger.info(
            f"[producer heartbeat] acked={snap['ok']} ack_rps={sent_rps:.1f} "
            f"failed={snap['err']} consecutive_failed={snap['consecutive_err']} "
            f"last_err={snap['last_err']}"
        )
        if stalled:
            logger.error(
                "Kafka producer received no acknowledgements for %ds; "
                "failing the task so the continuous job can restart",
                SEND_STALL_TIMEOUT_S,
            )
            _thread.interrupt_main()


threading.Thread(target=_heartbeat, daemon=True).start()

${PEER_TASK_WATCH_PYTHON}

# update_rate_hz is simulator TICKS/sec; each tick emits one record per sensor.
# Convert the user-facing records/sec target so generation cannot outrun Kafka.
probe = Simulator(industries=["${industry}"], update_rate_hz=1)
sensor_count = max(1, probe.sensor_count)
SIM_TICK_HZ = TARGET_RECORDS_PER_SEC / sensor_count
CALLBACK_BATCH_SIZE = sensor_count

sim = Simulator(industries=["${industry}"], update_rate_hz=SIM_TICK_HZ)
_calibrate_simulator(sim, disable_random_faults=SCENARIO_ENABLED)
sim.add_sink(
    CallbackSink(on_records, rate_hz=SIM_TICK_HZ, batch_size=CALLBACK_BATCH_SIZE)
)

logger.info(
    f"Producer target={TARGET_RECORDS_PER_SEC:.1f} rec/s, "
    f"sensors={sensor_count}, simulator={SIM_TICK_HZ:.2f} ticks/s"
)
try:
    sim.run()
except KeyboardInterrupt:
    if _fatal_send_error.is_set():
        raise RuntimeError(
            "Kafka producer unhealthy; continuous job restart required"
        ) from None
${PEER_TASK_ENDED_RAISE}
    raise
finally:
    producer.flush(timeout=30)
    producer.close(timeout=30)
`.trim();
  } else if (hasInlineCreds) {
    const mechanism = k.advanced?.["sasl.mechanism"] || "PLAIN";
    mainCell = `
import logging, time, ssl

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("iot_producer")

BOOTSTRAP = "${k.bootstrap_servers}"
TOPIC = "${k.topic}"

from iot_simulator.sinks.kafka import KafkaSink

ssl_ctx = ssl.create_default_context()

sink = KafkaSink(
    bootstrap_servers=BOOTSTRAP,
    topic=TOPIC,
    security_protocol="${k.security_protocol}",
    sasl_mechanism="${mechanism}",
    sasl_username="${k.advanced?.["sasl.username"] || ""}",
    sasl_password="${k.advanced?.["sasl.password"] || ""}",
    compression=None,
    acks=1,
    batch_size=50,
    extra_producer_config={
        "acks": 1,
        "ssl_context": ssl_ctx,
    },
)

from iot_simulator import Simulator

sim = Simulator(industries=["${industry}"], update_rate_hz=${updateRateHz})
sim.add_sink(sink, rate_hz=${updateRateHz})

logger.info(f"Simulator sensors: {sim.sensor_count}")
logger.info("Producer running until job is cancelled...")

sim.run()
`.trim();
  } else {
    const numPartitions = k.num_partitions ?? 3;
    mainCell = `
import logging, time, threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("iot_producer")

BOOTSTRAP = "${k.bootstrap_servers}"
TOPIC = "${k.topic}"
NUM_PARTITIONS = ${numPartitions}
SECURITY_PROTOCOL = "${k.security_protocol}"

# --- Pre-flight: ensure the topic exists. MSK with auto.create.topics.enable=false
# will silently queue messages against a missing topic forever, which is the
# #1 cause of "producer looks healthy but consumer sees 0 records".
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError
try:
    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP,
        security_protocol=SECURITY_PROTOCOL,
        request_timeout_ms=15000,
    )
    existing = admin.list_topics()
    if TOPIC in existing:
        logger.info(f"Topic {TOPIC!r} already exists on cluster.")
    else:
        logger.info(f"Topic {TOPIC!r} not found — creating with {NUM_PARTITIONS} partitions.")
        admin.create_topics([NewTopic(name=TOPIC, num_partitions=NUM_PARTITIONS, replication_factor=2)])
        logger.info(f"Created topic {TOPIC!r}.")
    admin.close()
except TopicAlreadyExistsError:
    logger.info(f"Topic {TOPIC!r} was created concurrently — continuing.")
except KafkaError as _e:
    logger.warning(f"Topic existence check failed (continuing anyway): {type(_e).__name__}: {_e}")

from iot_simulator.sinks.kafka import KafkaSink

# Error callback so send failures are visible instead of swallowed.
_send_counters = {"ok": 0, "err": 0, "last_err": None}

def _on_send_success(record_metadata):
    _send_counters["ok"] += 1

def _on_send_error(exc):
    _send_counters["err"] += 1
    _send_counters["last_err"] = f"{type(exc).__name__}: {exc}"

sink = KafkaSink(
    bootstrap_servers=BOOTSTRAP,
    topic=TOPIC,
    security_protocol=SECURITY_PROTOCOL,
    compression=None,
    acks=1,
    batch_size=50,
    extra_producer_config={"acks": 1},
)
# KafkaSink exposes its underlying kafka-python producer; attach error callbacks
# via a lightweight wrapper around sink._producer.send.
_orig_producer = getattr(sink, "_producer", None) or getattr(sink, "producer", None)
if _orig_producer is not None:
    _orig_send = _orig_producer.send
    def _send_with_cb(topic, value=None, key=None, headers=None, partition=None, timestamp_ms=None):
        fut = _orig_send(topic, value=value, key=key, headers=headers, partition=partition, timestamp_ms=timestamp_ms)
        try:
            fut.add_callback(_on_send_success).add_errback(_on_send_error)
        except Exception:
            pass
        return fut
    _orig_producer.send = _send_with_cb
else:
    logger.warning("Could not locate underlying kafka producer to attach error callbacks — send failures will not be counted.")

# Heartbeat thread: print produced/failure counts every 15s.
def _heartbeat():
    while True:
        time.sleep(15)
        snap = dict(_send_counters)
        logger.info(f"[producer heartbeat] sent={snap['ok']} failed={snap['err']} last_err={snap['last_err']}")

threading.Thread(target=_heartbeat, daemon=True).start()

from iot_simulator import Simulator

sim = Simulator(industries=["${industry}"], update_rate_hz=${updateRateHz})
sim.add_sink(sink, rate_hz=${updateRateHz})

logger.info(f"Simulator sensors: {sim.sensor_count}")
logger.info("Producer running until job is cancelled...")

sim.run()
`.trim();
  }

  const startupDelayCell = generateStartupDelayCell();
  const exitCell = 'dbutils.notebook.exit("Producer stopped")';

  return [HEADER, installCell, restartCell, startupDelayCell, mainCell, exitCell].join(SEP);
}

// ---------------------------------------------------------------------------
// Kinesis producer (boto3 — pre-installed on Databricks)
// ---------------------------------------------------------------------------
function generateKinesisProducer(config: DataProducerNotebookConfig): string {
  const k = config.connection.kinesis;
  if (!k) throw new Error("Kinesis connection config is required");

  const { wheelPath, industry, updateRateHz } = config;
  const cred = k.service_credential?.trim() || "";

  const installCell = `%pip install "${wheelPath}" --quiet`;
  const restartCell = "dbutils.library.restartPython()";

  let credBlock: string;
  if (cred) {
    credBlock = `
CRED_NAME = "${cred}"
provider = dbutils.credentials.getServiceCredentialsProvider(CRED_NAME)
session_creds = provider.get_credentials()
frozen = session_creds.get_frozen_credentials()

client = boto3.client(
    "kinesis",
    region_name=REGION,
    aws_access_key_id=frozen.access_key,
    aws_secret_access_key=frozen.secret_key,
    aws_session_token=frozen.token,
)
logger.info(f"Kinesis client ready (credential: {CRED_NAME}, region: {REGION})")
`.trim();
  } else {
    credBlock = `
# Using default credential chain (instance profile / env vars)
client = boto3.client("kinesis", region_name=REGION)
logger.info(f"Kinesis client ready (default creds, region: {REGION})")
`.trim();
  }

  const mainCell = `
import boto3, json, logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("iot_producer")

STREAM_NAME = "${k.stream_name}"
REGION = "${k.region}"

${credBlock}

from iot_simulator import Simulator
from iot_simulator.sinks.callback import CallbackSink

def on_records(records):
    entries = [
        {"Data": r.to_json().encode(), "PartitionKey": r.sensor_name}
        for r in records
    ]
    # PutRecords supports up to 500 records per call
    for i in range(0, len(entries), 500):
        chunk = entries[i:i + 500]
        resp = client.put_records(StreamName=STREAM_NAME, Records=chunk)
        failed = resp.get("FailedRecordCount", 0)
        if failed:
            logger.warning(f"Kinesis: {failed}/{len(chunk)} records failed")

sim = Simulator(industries=["${industry}"], update_rate_hz=${updateRateHz})
sim.add_sink(CallbackSink(on_records, rate_hz=${updateRateHz}, batch_size=50))

logger.info(f"Simulator sensors: {sim.sensor_count}")
logger.info(f"Producing to Kinesis stream '{STREAM_NAME}' until job is cancelled...")

sim.run()
`.trim();

  const startupDelayCell = generateStartupDelayCell();
  const exitCell = 'dbutils.notebook.exit("Producer stopped")';
  return [HEADER, installCell, restartCell, startupDelayCell, mainCell, exitCell].join(SEP);
}

// ---------------------------------------------------------------------------
// Event Hub producer (azure-eventhub SDK)
// ---------------------------------------------------------------------------
function generateEventHubProducer(config: DataProducerNotebookConfig): string {
  const eh = config.connection.event_hub;
  if (!eh) throw new Error("Event Hub connection config is required");

  const { wheelPath, industry, updateRateHz } = config;
  const cred = eh.service_credential?.trim() || "";

  const installCell = `%pip install "${wheelPath}" azure-eventhub --quiet`;
  const restartCell = "dbutils.library.restartPython()";

  let credBlock: string;
  if (cred) {
    credBlock = `
CRED_NAME = "${cred}"
provider = dbutils.credentials.getServiceCredentialsProvider(CRED_NAME)
conn_str = provider.get("eventhub.connection_string", "")
if not conn_str:
    raise ValueError(f"UC credential '{CRED_NAME}' does not contain 'eventhub.connection_string'")

producer_client = EventHubProducerClient.from_connection_string(
    conn_str, eventhub_name=EVENT_HUB_NAME,
)
logger.info(f"Event Hub client ready (credential: {CRED_NAME})")
`.trim();
  } else {
    credBlock = `
CONN_STR = "${eh.connection_string}"
producer_client = EventHubProducerClient.from_connection_string(
    CONN_STR, eventhub_name=EVENT_HUB_NAME,
)
logger.info("Event Hub client ready (inline connection string)")
`.trim();
  }

  const mainCell = `
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("iot_producer")

from azure.eventhub import EventHubProducerClient, EventData

EVENT_HUB_NAME = "${eh.event_hub_name}"

${credBlock}

from iot_simulator import Simulator
from iot_simulator.sinks.callback import CallbackSink

def on_records(records):
    batch = producer_client.create_batch()
    for r in records:
        try:
            batch.add(EventData(r.to_json()))
        except ValueError:
            # Batch full — send and create a new one
            producer_client.send_batch(batch)
            batch = producer_client.create_batch()
            batch.add(EventData(r.to_json()))
    if batch:
        producer_client.send_batch(batch)

sim = Simulator(industries=["${industry}"], update_rate_hz=${updateRateHz})
sim.add_sink(CallbackSink(on_records, rate_hz=${updateRateHz}, batch_size=50))

logger.info(f"Simulator sensors: {sim.sensor_count}")
logger.info(f"Producing to Event Hub '{EVENT_HUB_NAME}' until job is cancelled...")

try:
    sim.run()
finally:
    producer_client.close()
`.trim();

  const startupDelayCell = generateStartupDelayCell();
  const exitCell = 'dbutils.notebook.exit("Producer stopped")';
  return [HEADER, installCell, restartCell, startupDelayCell, mainCell, exitCell].join(SEP);
}

// ---------------------------------------------------------------------------
// RabbitMQ producer (pika SDK)
// ---------------------------------------------------------------------------
function generateRabbitMQProducer(config: DataProducerNotebookConfig): string {
  const r = config.connection.rabbitmq;
  if (!r) throw new Error("RabbitMQ connection config is required");

  const { wheelPath, industry, updateRateHz } = config;
  const cred = r.service_credential?.trim() || "";

  const installCell = `%pip install "${wheelPath}" pika --quiet`;
  const restartCell = "dbutils.library.restartPython()";

  let credBlock: string;
  if (cred) {
    credBlock = `
CRED_NAME = "${cred}"
provider = dbutils.credentials.getServiceCredentialsProvider(CRED_NAME)
rmq_user = provider.get("rabbitmq.username", "guest")
rmq_pass = provider.get("rabbitmq.password", "")
logger.info(f"RabbitMQ credentials resolved via UC credential '{CRED_NAME}'")
`.trim();
  } else {
    credBlock = `
rmq_user = "${r.username}"
rmq_pass = ""  # Set via advanced config or UC credential for production
logger.info("RabbitMQ using inline credentials")
`.trim();
  }

  const mainCell = `
import pika, logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("iot_producer")

HOST = "${r.host}"
PORT = ${r.port || "5672"}
QUEUE = "${r.queue}"

${credBlock}

creds = pika.PlainCredentials(rmq_user, rmq_pass)
params = pika.ConnectionParameters(host=HOST, port=int(PORT), credentials=creds)
connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.queue_declare(queue=QUEUE, durable=True)
logger.info(f"RabbitMQ channel open — queue '{QUEUE}' on {HOST}:{PORT}")

from iot_simulator import Simulator
from iot_simulator.sinks.callback import CallbackSink

def on_records(records):
    for r in records:
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE,
            body=r.to_json(),
            properties=pika.BasicProperties(delivery_mode=2),
        )

sim = Simulator(industries=["${industry}"], update_rate_hz=${updateRateHz})
sim.add_sink(CallbackSink(on_records, rate_hz=${updateRateHz}, batch_size=50))

logger.info(f"Simulator sensors: {sim.sensor_count}")
logger.info(f"Producing to RabbitMQ queue '{QUEUE}' until job is cancelled...")

try:
    sim.run()
finally:
    connection.close()
`.trim();

  const startupDelayCell = generateStartupDelayCell();
  const exitCell = 'dbutils.notebook.exit("Producer stopped")';
  return [HEADER, installCell, restartCell, startupDelayCell, mainCell, exitCell].join(SEP);
}

// ---------------------------------------------------------------------------
// Pub/Sub producer (google-cloud-pubsub SDK)
// ---------------------------------------------------------------------------
function generatePubSubProducer(config: DataProducerNotebookConfig): string {
  const p = config.connection.pubsub;
  if (!p) throw new Error("Pub/Sub connection config is required");

  const { wheelPath, industry, updateRateHz } = config;
  const cred = p.service_credential?.trim() || "";
  const topicId = p.topic_id || p.subscription || "";

  const installCell = `%pip install "${wheelPath}" google-cloud-pubsub --quiet`;
  const restartCell = "dbutils.library.restartPython()";

  let credBlock: string;
  if (cred) {
    credBlock = `
import json as _json
from google.oauth2 import service_account

CRED_NAME = "${cred}"
provider = dbutils.credentials.getServiceCredentialsProvider(CRED_NAME)
creds_json = _json.loads(provider.get("pubsub.credentials_json", "{}"))
credentials = service_account.Credentials.from_service_account_info(creds_json)

publisher = pubsub_v1.PublisherClient(credentials=credentials)
logger.info(f"Pub/Sub client ready (credential: {CRED_NAME})")
`.trim();
  } else {
    credBlock = `
# Using Application Default Credentials
publisher = pubsub_v1.PublisherClient()
logger.info("Pub/Sub client ready (default credentials)")
`.trim();
  }

  const mainCell = `
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("iot_producer")

from google.cloud import pubsub_v1

PROJECT_ID = "${p.project_id}"
TOPIC_ID = "${topicId}"

${credBlock}

topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
logger.info(f"Publishing to {topic_path}")

from iot_simulator import Simulator
from iot_simulator.sinks.callback import CallbackSink

def on_records(records):
    futures = []
    for r in records:
        future = publisher.publish(topic_path, data=r.to_json().encode())
        futures.append(future)
    # Wait for all publishes to complete
    for f in futures:
        f.result(timeout=30)

sim = Simulator(industries=["${industry}"], update_rate_hz=${updateRateHz})
sim.add_sink(CallbackSink(on_records, rate_hz=${updateRateHz}, batch_size=50))

logger.info(f"Simulator sensors: {sim.sensor_count}")
logger.info(f"Producing to Pub/Sub topic '{TOPIC_ID}' until job is cancelled...")

sim.run()
`.trim();

  const startupDelayCell = generateStartupDelayCell();
  const exitCell = 'dbutils.notebook.exit("Producer stopped")';
  return [HEADER, installCell, restartCell, startupDelayCell, mainCell, exitCell].join(SEP);
}

// ===========================================================================
// Industry-first Kafka producer (spike-VERIFIED 2026-07-05, §1.8/§1.15).
//
// Feeds the iot_simulator into a kafka-python KafkaProducer that authenticates
// to MSK with SASL_SSL / OAUTHBEARER via an MSK-IAM token provider (subclassing
// kafka.net.sasl.oauth.AbstractTokenProvider -> MSKAuthTokenProvider
// .generate_auth_token("us-east-1")). AWS creds are hydrated straight from the
// UC service credential (getServiceCredentialsProvider returns a renewable
// botocore Session consumed directly by the MSK signer's credential provider).
//
// The topic is created up-front by the provisioning engine (§1.8), so the
// producer does NOT create it here.
//
// Generation semantics (§1.15):
//   * realtime  -> asynchronous send with linger_ms=0; flush only at shutdown.
//   * microbatch -> batch records before flushing (throughput over latency).
//   * recordsPerSec is converted to simulator ticks/sec using sensor_count.
//
// Guard-tested by tests/test_producer_notebook_template.py.
// ===========================================================================
const DEFAULT_MSK_REGION = "us-east-1";
// DEFAULT_PRODUCER_SERVICE_CREDENTIAL removed — workspace values must come from
// the injected IndustryProducerConfig.serviceCredential (populated from
// appDefaults in industry-deploy-config.ts), never hardcoded here.

export interface IndustryProducerConfig {
  wheelPath: string;
  industry: string;
  /** Generation rate in records/sec (§1.15, replaces update_rate_hz). */
  recordsPerSec: number;
  /** realtime = unbatched per-record flush; microbatch = batched flush. */
  mode: "realtime" | "microbatch";
  bootstrapServers: string;
  topic: string;
  /** UC service credential mapped to the MSK-IAM role (default `iot-rtm-sc`). */
  serviceCredential?: string;
  region?: string;
  /** Micro-batch flush size (records per batch). */
  batchSize?: number;
}

export function generateIndustryKafkaProducer(config: IndustryProducerConfig): string {
  const {
    wheelPath,
    industry,
    recordsPerSec,
    mode,
    bootstrapServers,
    topic,
  } = config;
  const cred = config.serviceCredential?.trim() || "";
  const region = config.region?.trim() || DEFAULT_MSK_REGION;
  const isRealtime = mode === "realtime";
  const batchSize = config.batchSize ?? 50;
  // linger_ms=0 => no client-side batching (lowest latency) for real-time.
  const lingerMs = isRealtime ? 0 : 100;

  // Do NOT upgrade boto3/botocore on this runtime; install only the MSK-IAM
  // signer + kafka-python with --no-deps to avoid pulling a new botocore.
  const installCell = `%pip install "${wheelPath}" "kafka-python>=3" aws-msk-iam-sasl-signer-python --quiet`;
  const restartCell = "dbutils.library.restartPython()";

  const flushComment = isRealtime
    ? "# REAL-TIME: enqueue asynchronously; flush only during shutdown."
    : "# MICRO-BATCH: accumulate a batch, then flush (throughput over latency).";

  const sendLoop = isRealtime
    ? `def on_records(records):
    # Real-time: zero-linger send is asynchronous; no per-record flush.
    if _fatal_send_error.is_set():
        raise RuntimeError("Kafka producer stopped after sustained send failures")
    records = _wrap_records(records)
    for r in records:
        payload = json.loads(r.to_json())
        payload["timestamp"] = time.time()
        future = producer.send(
            TOPIC,
            value=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            key=r.sensor_name.encode("utf-8"),
        )
        with _counter_lock:
            _send_counters["enqueued"] += 1
        future.add_callback(_on_send_success).add_errback(_on_send_error)`
    : `def on_records(records):
    # Micro-batch: enqueue the whole batch, then a single flush.
    if _fatal_send_error.is_set():
        raise RuntimeError("Kafka producer stopped after sustained send failures")
    records = _wrap_records(records)
    for r in records:
        future = producer.send(TOPIC, value=r.to_json().encode("utf-8"), key=r.sensor_name.encode("utf-8"))
        with _counter_lock:
            _send_counters["enqueued"] += 1
        future.add_callback(_on_send_success).add_errback(_on_send_error)
    producer.flush()`;

  const mainCell = `
import _thread
import json
import logging
import threading
import time

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from botocore.credentials import CredentialProvider
from iot_simulator import Simulator
from iot_simulator.sinks.callback import CallbackSink
from kafka import KafkaProducer
from scenario_engine import ScenarioEngine

# kafka-python 3.x: the token provider MUST subclass
# kafka.net.sasl.oauth.AbstractTokenProvider (older layout under kafka.sasl.oauth).
try:
    from kafka.net.sasl.oauth import AbstractTokenProvider
except ImportError:
    from kafka.sasl.oauth import AbstractTokenProvider

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("iot_producer")
# Missing scenario_engine after wheel install is a hard failure.

BOOTSTRAP = "${bootstrapServers}"
TOPIC = "${topic}"
CRED_NAME = "${cred}"
REGION = "${region}"
BATCH_SIZE = ${batchSize}
TARGET_RECORDS_PER_SEC = ${recordsPerSec}
INDUSTRY = "${industry}"
SIMULATOR_INDUSTRY = "electric_power" if INDUSTRY == "energy" else INDUSTRY
_scenario = ScenarioEngine(industry=INDUSTRY, seed=42, scenario_id=f"{INDUSTRY}-default")


def _wrap_records(records):
    return _scenario.wrap_records(records)

# --- MSK-IAM auth: hydrate AWS creds from the UC service credential ---
# The returned botocore Session owns temporary-credential refresh. The custom
# provider below is invoked for every MSK token instead of freezing credentials.
provider = dbutils.credentials.getServiceCredentialsProvider(CRED_NAME)


class UCServiceCredentialProvider(CredentialProvider):
    METHOD = "databricks-uc-service-credential"
    CANONICAL_NAME = "DatabricksUCServiceCredential"

    def load(self):
        return provider.get_credentials()


uc_credentials = UCServiceCredentialProvider()


class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = (
            MSKAuthTokenProvider.generate_auth_token_from_credentials_provider(
                REGION,
                uc_credentials,
            )
        )
        return token


producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    security_protocol="SASL_SSL",
    sasl_mechanism="OAUTHBEARER",
    sasl_oauth_token_provider=MSKTokenProvider(),
    linger_ms=${lingerMs},
    acks=1,
)
logger.info(f"KafkaProducer ready (SASL_SSL/OAUTHBEARER) -> {TOPIC}")

MAX_CONSECUTIVE_SEND_ERRORS = 20
SEND_STALL_TIMEOUT_S = 60
_send_counters = {
    "ok": 0,
    "err": 0,
    "enqueued": 0,
    "consecutive_err": 0,
    "last_err": None,
    "last_success_at": time.monotonic(),
}
_counter_lock = threading.Lock()
_fatal_send_error = threading.Event()


def _on_send_success(_metadata):
    with _counter_lock:
        _send_counters["ok"] += 1
        _send_counters["consecutive_err"] = 0
        _send_counters["last_success_at"] = time.monotonic()


def _on_send_error(exc):
    with _counter_lock:
        _send_counters["err"] += 1
        _send_counters["consecutive_err"] += 1
        _send_counters["last_err"] = f"{type(exc).__name__}: {exc}"
        should_stop = (
            _send_counters["consecutive_err"] >= MAX_CONSECUTIVE_SEND_ERRORS
            and not _fatal_send_error.is_set()
        )
        if should_stop:
            _fatal_send_error.set()
    if should_stop:
        logger.error(
            "Kafka producer reached %d consecutive send failures; restarting",
            MAX_CONSECUTIVE_SEND_ERRORS,
        )
        _thread.interrupt_main()


def _heartbeat():
    previous_ok = 0
    while True:
        time.sleep(15)
        with _counter_lock:
            snap = dict(_send_counters)
            stalled = (
                snap["enqueued"] > snap["ok"]
                and time.monotonic() - snap["last_success_at"]
                >= SEND_STALL_TIMEOUT_S
                and not _fatal_send_error.is_set()
            )
            if stalled:
                _fatal_send_error.set()
        ack_rps = (snap["ok"] - previous_ok) / 15.0
        previous_ok = snap["ok"]
        logger.info(
            f"[producer heartbeat] acked={snap['ok']} ack_rps={ack_rps:.1f} "
            f"failed={snap['err']} consecutive_failed={snap['consecutive_err']}"
        )
        if stalled:
            logger.error(
                "Kafka producer received no acknowledgements for %ds; restarting",
                SEND_STALL_TIMEOUT_S,
            )
            _thread.interrupt_main()


threading.Thread(target=_heartbeat, daemon=True).start()

${PEER_TASK_WATCH_PYTHON}

${flushComment}
${sendLoop}

probe = Simulator(industries=[SIMULATOR_INDUSTRY], update_rate_hz=1)
sensor_count = max(1, probe.sensor_count)
SIM_TICK_HZ = TARGET_RECORDS_PER_SEC / sensor_count
CALLBACK_BATCH_SIZE = ${isRealtime ? "sensor_count" : "BATCH_SIZE"}

sim = Simulator(industries=[SIMULATOR_INDUSTRY], update_rate_hz=SIM_TICK_HZ)
sim.add_sink(
    CallbackSink(
        on_records,
        rate_hz=SIM_TICK_HZ,
        batch_size=CALLBACK_BATCH_SIZE,
    )
)

logger.info(
    f"Producing to '{TOPIC}' at {TARGET_RECORDS_PER_SEC:.1f} rec/s "
    f"({SIM_TICK_HZ:.2f} ticks/s, {sensor_count} sensors, ${mode}, "
    f"scenario={_scenario is not None})"
)

try:
    sim.run()
except KeyboardInterrupt:
    if _fatal_send_error.is_set():
        raise RuntimeError(
            "Kafka producer unhealthy; continuous job restart required"
        ) from None
${PEER_TASK_ENDED_RAISE}
    raise
finally:
    producer.flush(timeout=30)
    producer.close(timeout=30)
`.trim();

  const startupDelayCell = generateStartupDelayCell();
  const exitCell = 'dbutils.notebook.exit("Producer stopped")';
  return [HEADER, installCell, restartCell, startupDelayCell, mainCell, exitCell].join(SEP);
}

// ---------------------------------------------------------------------------
// Zerobus producer (databricks-zerobus-ingest-sdk)
// ---------------------------------------------------------------------------
// Target: ``{uc_catalog}.{uc_schema}.{industry}_zerobus`` (one-schema layout,
// Task 3/4).  The table FQN is injected via ``zb.table_name``; catalog/schema
// are derived from it at runtime so the notebook contains no workspace literals.
//
// Auth: the app SP OAuth client-id is injected via ``zb.client_id``; the
// client-secret is read from the UC secret ``{catalog}.{schema}.app_sp_oauth``
// via ``dbutils.secrets.get(catalog=…, schema=…, key="app_sp_oauth")``.  This
// requires serverless environment version ≥ 4 (``client="4"`` in the job spec).
// A REST-API fallback is also present for robustness.
// ---------------------------------------------------------------------------
function generateZerobusProducer(config: DataProducerNotebookConfig): string {
  const zb = config.connection.zerobus;
  if (!zb) throw new Error("Zerobus connection config is required");

  const { wheelPath, industry, updateRateHz } = config;
  const clientId = zb.client_id || "";

  const installCell = `%pip install "${wheelPath}" databricks-zerobus-ingest-sdk --quiet`;
  const restartCell = "dbutils.library.restartPython()";

  const workspaceUrl = zb.workspace_url || "";

  const mainCell = `
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("iot_producer")

ENDPOINT = "${zb.endpoint}"
TABLE_NAME = "${zb.table_name}"
CLIENT_ID = "${clientId}"
WORKSPACE_URL = "${workspaceUrl}" or f"https://{spark.conf.get('spark.databricks.workspaceUrl', '')}"
# Zerobus OAuth builds <workspace_url>/oidc/v1/token; a bare host (no scheme)
# yields "No scheme supplied" -> invalid UC token. Ensure an https:// prefix.
WORKSPACE_URL = WORKSPACE_URL if WORKSPACE_URL.startswith("http") else f"https://{WORKSPACE_URL}"

# ---------------------------------------------------------------------------
# Read the app SP OAuth client-secret from the UC secret
# "{catalog}.{schema}.app_sp_oauth" — same catalog/schema as the target table.
# Primary path: dbutils.secrets.get(catalog=..., schema=..., key=...)  [env >= 4].
# Fallback:     REST API via the notebook SDK auth.
# ---------------------------------------------------------------------------
_m_parts = TABLE_NAME.split(".")
_uc_catalog = _m_parts[0]
_uc_schema  = _m_parts[1]
try:
    client_secret = dbutils.secrets.get(
        catalog=_uc_catalog, schema=_uc_schema, key="app_sp_oauth"
    )
    logger.info("app_sp_oauth read via UC dbutils.secrets.get (env >= 4 path)")
except Exception as _dbs_e:
    logger.warning(
        f"dbutils.secrets.get(catalog=…) failed ({type(_dbs_e).__name__}: {_dbs_e}); "
        "falling back to UC-secrets REST API"
    )
    from databricks.sdk import WorkspaceClient as _WC
    _secret_path = f"{_uc_catalog}.{_uc_schema}.app_sp_oauth"
    _resp = _WC().api_client.do(
        "GET",
        f"/api/2.1/unity-catalog/secrets/{_secret_path}",
        query={"include_value": "true"},
    )
    client_secret = _resp.get("value", "")
    logger.info("app_sp_oauth read via UC REST API fallback")

# ---------------------------------------------------------------------------
# We deliberately do NOT use iot_simulator's ZerobusSink here. That sink
# ingests SensorRecord.to_dict() RAW, which emits the record field
# "timestamp" (epoch SECONDS float) and metadata dict[str, Any]. But the UC
# table's schema is keyed by COLUMN name — it has "event_time" (TIMESTAMP),
# not "timestamp", and "metadata" is map<string,string>. In Zerobus JSON
# mode, an unmatched "timestamp" key is SILENTLY DROPPED (event_time -> NULL)
# and a datetime.isoformat() string fails ingest with INVALID_ARGUMENT. So we
# drive the Simulator through a CallbackSink and ingest column-keyed rows via
# the SDK directly: event_time as epoch MICROSECONDS int, metadata stringified
# (mirrors the proven validation path in services/zerobus_validation.py).
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
from zerobus.sdk.sync import ZerobusSdk
from iot_simulator import Simulator
from iot_simulator.sinks.callback import CallbackSink

INDUSTRY = "${industry}"
SIMULATOR_INDUSTRY = "electric_power" if INDUSTRY == "energy" else INDUSTRY
from scenario_engine import ScenarioEngine
_scenario = ScenarioEngine(industry=INDUSTRY, seed=42, scenario_id=f"{INDUSTRY}-default")

_sdk = ZerobusSdk(ENDPOINT, WORKSPACE_URL)
_stream = _sdk.create_stream(
    CLIENT_ID, client_secret,
    TableProperties(TABLE_NAME),
    StreamConfigurationOptions(record_type=RecordType.JSON),
)

def _to_row(rec):
    # Map the simulator record onto the UC table's COLUMN names/types.
    return {
        "sensor_name": rec.sensor_name,
        "industry": rec.industry,
        "value": float(rec.value),
        "unit": rec.unit,
        "sensor_type": rec.sensor_type,
        # epoch MICROSECONDS int (NOT "timestamp", NOT an ISO string).
        "event_time": int(rec.timestamp * 1_000_000),
        "min_value": float(rec.min_value),
        "max_value": float(rec.max_value),
        "nominal_value": float(rec.nominal_value),
        "fault_active": bool(rec.fault_active),
        "metadata": {str(k): str(v) for k, v in (rec.metadata or {}).items()},
    }

def on_records(records):
    records = _scenario.wrap_records(records)
    acks = []
    for rec in records:
        try:
            acks.append(_stream.ingest_record(_to_row(rec)))
        except Exception as e:
            logger.error(f"[zerobus] ingest_record failed: {type(e).__name__}: {e}")
    for a in acks:
        try:
            a.wait_for_ack(timeout_sec=30)
        except TypeError:
            a.wait_for_ack()
        except Exception as e:
            logger.error(f"[zerobus] wait_for_ack failed: {type(e).__name__}: {e}")

sim = Simulator(industries=[SIMULATOR_INDUSTRY], update_rate_hz=${updateRateHz})
sim.add_sink(CallbackSink(on_records, rate_hz=${updateRateHz}, batch_size=20))

logger.info(f"Simulator sensors: {sim.sensor_count}")
logger.info(
    f"Producing to Zerobus table '{TABLE_NAME}' via {ENDPOINT} "
    f"(column-mapped rows, scenario={_scenario is not None})"
)

try:
    sim.run()
finally:
    try:
        _stream.close()
    except Exception:
        pass
`.trim();

  const exitCell = 'dbutils.notebook.exit("Producer stopped")';
  return [HEADER, installCell, restartCell, mainCell, exitCell].join(SEP);
}
