import type { SssSource, SssConnectionConfig } from "@/lib/types";

const SEP = "\n# COMMAND ----------\n";
const HEADER = "# Databricks notebook source";

function cell(content: string): string {
  return content.trim();
}

function kafkaNetworkDiagnosticsCell(bootstrapServers: string, securityProtocol: string): string {
  return `
import json, socket, ssl, ipaddress

BOOTSTRAP = "${bootstrapServers}"
SECURITY_PROTOCOL = "${securityProtocol}"

result = {"success": False, "source": "kafka"}
diag = {"checks": []}

host_port_pairs = []
for entry in BOOTSTRAP.split(","):
    entry = entry.strip()
    if ":" in entry:
        h, p = entry.rsplit(":", 1)
        host_port_pairs.append((h, int(p)))
    else:
        host_port_pairs.append((entry, 9092))

broker_host, broker_port = host_port_pairs[0]

def _is_private(ip_str):
    try:
        return ipaddress.ip_address(ip_str).is_private
    except ValueError:
        return False

# --- DNS resolution ---
try:
    addrs = socket.getaddrinfo(broker_host, broker_port)
    resolved = list(set(a[4][0] for a in addrs))
    private = any(_is_private(ip) for ip in resolved)
    diag["dns_ok"] = True
    diag["resolved_ips"] = resolved
    diag["is_private_ip"] = private
    diag["checks"].append({"name": "DNS", "ok": True, "detail": f"{broker_host} -> {', '.join(resolved)}"})
except Exception as e:
    diag["dns_ok"] = False
    diag["is_private_ip"] = False
    diag["checks"].append({"name": "DNS", "ok": False, "detail": str(e)[:200]})
    result["phase"] = "network_diagnostics"
    result["diagnostics"] = diag
    result["error"] = f"Cannot resolve hostname '{broker_host}'. Verify the bootstrap server address is correct."
    result["hint"] = "Check for typos in the hostname. If using MSK Serverless, ensure the endpoint is correct from the MSK console."
    dbutils.notebook.exit(json.dumps(result))

# --- TCP connectivity ---
tcp_ok = False
try:
    s = socket.create_connection((broker_host, broker_port), timeout=10)
    s.close()
    tcp_ok = True
    diag["tcp_reachable"] = True
    diag["checks"].append({"name": "TCP", "ok": True, "detail": f"Port {broker_port} reachable"})
except Exception as e:
    diag["tcp_reachable"] = False
    detail = str(e)[:200]
    diag["checks"].append({"name": "TCP", "ok": False, "detail": detail})

if not tcp_ok:
    result["phase"] = "network_diagnostics"
    result["diagnostics"] = diag
    if diag.get("is_private_ip"):
        result["error"] = f"Broker resolves to private IP ({', '.join(diag.get('resolved_ips', []))}) which is not reachable from serverless compute."
        result["hint"] = "Use the public bootstrap endpoint (e.g. MSK public endpoints on port 9198 for IAM auth), or run on a cluster with VPC connectivity to the broker."
    else:
        result["error"] = f"Port {broker_port} is not reachable on {broker_host}. Connection timed out."
        result["hint"] = f"Check that security groups / firewall rules allow inbound traffic on port {broker_port}. For MSK, verify the cluster is in ACTIVE state."
    dbutils.notebook.exit(json.dumps(result))

# --- SSL/TLS handshake (if protocol uses SSL) ---
uses_ssl = "SSL" in SECURITY_PROTOCOL.upper()
if uses_ssl:
    try:
        ctx = ssl.create_default_context()
        with socket.create_connection((broker_host, broker_port), timeout=10) as raw:
            with ctx.wrap_socket(raw, server_hostname=broker_host) as ssock:
                diag["ssl_ok"] = True
                diag["checks"].append({"name": "SSL", "ok": True, "detail": ssock.version()})
    except Exception as e:
        diag["ssl_ok"] = False
        diag["checks"].append({"name": "SSL", "ok": False, "detail": str(e)[:200]})
        result["phase"] = "network_diagnostics"
        result["diagnostics"] = diag
        result["error"] = f"TCP connection succeeded but SSL/TLS handshake failed on {broker_host}:{broker_port}."
        result["hint"] = "Check the broker TLS configuration and ensure the correct security protocol is selected. If the broker uses a custom CA, it may not be trusted by default."
        dbutils.notebook.exit(json.dumps(result))
else:
    diag["ssl_ok"] = None
    diag["checks"].append({"name": "SSL", "ok": True, "detail": "Skipped (non-SSL protocol)"})

# Network diagnostics passed — continue to write/read tests
print(f"Network OK: {broker_host}:{broker_port} (DNS={diag['dns_ok']}, TCP={diag['tcp_reachable']}, SSL={diag.get('ssl_ok', 'n/a')})")
`.trim();
}

function kafkaNotebook(
  conn: NonNullable<SssConnectionConfig["kafka"]>,
  wheelPath: string,
): string {
  const { bootstrap_servers, topic, security_protocol, service_credential } = conn;
  const cred = service_credential?.trim() || "";

  const pipCell = cell(`%pip install "${wheelPath}[kafka]" --quiet`);
  const restartCell = cell("dbutils.library.restartPython()");

  const diagnosticsCell = cell(kafkaNetworkDiagnosticsCell(bootstrap_servers, security_protocol));

  const saslSecure = security_protocol.startsWith("SASL");

  let writeTest: string;
  if (cred) {
    writeTest = `
import time, asyncio

TOPIC = "${topic}"
CRED_NAME = "${cred}"
NUM_RECORDS = 5

# --- Phase 1: Write test (Spark writer with UC credential is the primary path) ---
write_ok = False
write_msg = ""
iot_sim_msg = ""

# Step A: Try Spark Kafka writer first
try:
    test_rows = [{"key": f"conn_test_{i}", "value": json.dumps({"sensor_name": f"conn_test_{i}", "value": float(i), "ts": time.time()})} for i in range(NUM_RECORDS)]
    test_df = spark.createDataFrame(test_rows)
    (test_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .write.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("topic", TOPIC)
        .option("databricks.serviceCredential", CRED_NAME)
        .save())
    write_ok = True
    write_msg = f"{NUM_RECORDS} records produced via Spark Kafka writer (UC credential)"
except Exception as spark_err:
    import traceback as _tb
    err_str = str(spark_err)
    full_tb = _tb.format_exc()
    # Combine error string and traceback for comprehensive matching
    search_str = err_str + " " + full_tb
    hint = ""
    if "UnknownTopicOrPartition" in search_str or "UNKNOWN_TOPIC_OR_PARTITION" in search_str or "unknown topic" in search_str.lower() or "not present in metadata" in search_str:
        hint = "Topic does not exist on the broker. Create it manually (e.g. via AWS MSK Console) or enable auto.create.topics.enable on the broker configuration."
    elif "TOPIC_AUTHORIZATION_FAILED" in search_str:
        hint = "The UC service credential may lack write permissions to this topic."
    elif "SaslAuthenticationException" in search_str or "AuthenticationException" in search_str:
        hint = "Authentication failed. Check UC service credential configuration and broker security settings."
    elif "timed out" in err_str.lower() or "TimeoutException" in err_str or "Unable to bootstrap" in err_str:
        hint = "Connection timed out reaching the broker. Verify bootstrap servers address and port, and check network/VPC connectivity."
    elif "connection" in err_str.lower() or "dns" in err_str.lower():
        hint = "Connection failed. Check broker address, port, and network/VPC connectivity."
    write_msg = f"Write failed | Hint: {hint} | Raw: {err_str[:300]}" if hint else f"Spark Kafka writer failed: {err_str[:500]}"

# Step B: If Spark write failed, try IoT Simulator KafkaSink (works for SCRAM/PLAIN creds)
if not write_ok:
    try:
        provider = dbutils.credentials.getServiceCredentialsProvider(CRED_NAME)
        sasl_user = (
            provider.get("kafka.api.key", "")
            or provider.get("KAFKA_API_KEY", "")
            or provider.get("sasl.username", "")
            or provider.get("username", "")
        )
        sasl_pass = (
            provider.get("kafka.api.secret", "")
            or provider.get("KAFKA_API_SECRET", "")
            or provider.get("sasl.password", "")
            or provider.get("password", "")
        )
        if sasl_user and sasl_pass:
            from iot_simulator.sinks.kafka import KafkaSink
            from iot_simulator.models import SensorRecord
            sink = KafkaSink(
                bootstrap_servers=BOOTSTRAP, topic=TOPIC,
                security_protocol="${security_protocol}",
                sasl_mechanism="PLAIN",
                sasl_username=sasl_user, sasl_password=sasl_pass,
                compression=None, acks=1, batch_size=NUM_RECORDS,
            )
            records = [
                SensorRecord(
                    sensor_name=f"conn_test_{i}", industry="connectivity_test",
                    value=float(i), unit="test", sensor_type="gauge",
                    timestamp=time.time(), min_value=0.0, max_value=100.0,
                    nominal_value=50.0, fault_active=False,
                ) for i in range(NUM_RECORDS)
            ]
            asyncio.run(sink.connect())
            asyncio.run(sink.write(records))
            asyncio.run(sink.flush())
            asyncio.run(sink.close())
            write_ok = True
            iot_sim_msg = f"{NUM_RECORDS} records produced via IoT Simulator KafkaSink"
        else:
            iot_sim_msg = "IoT Simulator skipped (no SASL user/pass in credential — likely IAM auth)"
    except Exception as sim_err:
        iot_sim_msg = f"IoT Simulator KafkaSink failed: {sim_err}"

if not write_ok:
    result["error"] = write_msg
    result["hint"] = hint if hint else None
    result["phase"] = "write_test"
    result["write_test"] = write_msg
    result["diagnostics"] = diag
    if iot_sim_msg:
        result["iot_simulator"] = iot_sim_msg
    dbutils.notebook.exit(json.dumps(result))

result["write_test"] = iot_sim_msg if iot_sim_msg and write_ok and "IoT Simulator" in (iot_sim_msg or "") else write_msg
if iot_sim_msg:
    result["iot_simulator"] = iot_sim_msg
`.trim();
  } else {
    writeTest = `
import time, asyncio

TOPIC = "${topic}"
NUM_RECORDS = 5

write_ok = False
write_msg = ""
try:
    from iot_simulator.sinks.kafka import KafkaSink
    from iot_simulator.models import SensorRecord
    sink = KafkaSink(
        bootstrap_servers=BOOTSTRAP, topic=TOPIC,
        security_protocol="${security_protocol}",
        ${saslSecure ? `sasl_mechanism="PLAIN",\n        sasl_username="${conn.advanced?.["sasl.username"] || ""}",\n        sasl_password="${conn.advanced?.["sasl.password"] || ""}",` : ""}
        compression=None, acks=1, batch_size=NUM_RECORDS,
    )
    records = [
        SensorRecord(
            sensor_name=f"conn_test_{i}", industry="connectivity_test",
            value=float(i), unit="test", sensor_type="gauge",
            timestamp=time.time(), min_value=0.0, max_value=100.0,
            nominal_value=50.0, fault_active=False,
        ) for i in range(NUM_RECORDS)
    ]
    asyncio.run(sink.connect())
    asyncio.run(sink.write(records))
    asyncio.run(sink.flush())
    asyncio.run(sink.close())
    write_ok = True
    write_msg = f"{NUM_RECORDS} records produced via IoT Simulator KafkaSink"
except Exception as e:
    result["error"] = f"Write test failed: {e}"
    result["phase"] = "write_test"
    result["diagnostics"] = diag
    dbutils.notebook.exit(json.dumps(result))

result["write_test"] = write_msg
`.trim();
  }

  const readTest = `
# --- Phase 2: Spark read-back verification ---
read_msg = ""
try:
    kafka_opts = {
        "kafka.bootstrap.servers": BOOTSTRAP,
        "subscribe": TOPIC,
        "startingOffsets": "earliest",
    }
    ${cred ? `kafka_opts["databricks.serviceCredential"] = CRED_NAME` : `kafka_opts["kafka.security.protocol"] = "${security_protocol}"`}
    df = spark.read.format("kafka").options(**kafka_opts).load()
    count = df.limit(20).count()
    read_msg = f"{count} records read via Spark Kafka reader"
except Exception as e:
    read_msg = f"Read-back failed: {e}"

result["read_test"] = read_msg
result["diagnostics"] = diag
result["success"] = write_ok
result["message"] = f"Write: {result['write_test']} | Read: {read_msg}"
dbutils.notebook.exit(json.dumps(result))
`.trim();

  return [HEADER, pipCell, restartCell, diagnosticsCell, cell(writeTest), cell(readTest)].join(SEP);
}

function streamingReadTest(
  sourceFormat: string,
  optionsCode: string,
  sourceName: string,
): string {
  return `
import json, time

result = {"success": False, "source": "${sourceName}"}

try:
${optionsCode}

    query = (
        spark.readStream
        .format("${sourceFormat}")
        .options(**read_opts)
        .load()
        .writeStream
        .format("memory")
        .queryName("connectivity_test")
        .start()
    )
    time.sleep(8)
    if query.isActive:
        query.stop()
        result["success"] = True
        result["message"] = "Stream started and ran for 8 seconds without error. Source is reachable."
        result["read_test"] = "Streaming read OK"
    else:
        ex = query.exception()
        if ex:
            result["error"] = str(ex)
            result["phase"] = "read_test"
        else:
            result["success"] = True
            result["message"] = "Stream completed (source may be empty). Connectivity OK."
            result["read_test"] = "Stream completed"
except Exception as e:
    result["error"] = str(e)
    result["phase"] = "read_test"

dbutils.notebook.exit(json.dumps(result))
`.trim();
}

function kinesisNotebook(conn: NonNullable<SssConnectionConfig["kinesis"]>): string {
  const { stream_name, region, service_credential } = conn;
  const cred = service_credential?.trim() || "";

  const opts = `    read_opts = {
        "streamName": "${stream_name}",
        "region": "${region}",
        "initialPosition": "TRIM_HORIZON",
    }${cred ? `\n    read_opts["serviceCredential"] = "${cred}"` : ""}`;

  return [HEADER, cell(streamingReadTest("kinesis", opts, "kinesis"))].join(SEP);
}

function eventHubNotebook(conn: NonNullable<SssConnectionConfig["event_hub"]>): string {
  const { connection_string, service_credential } = conn;
  const cred = service_credential?.trim() || "";

  let opts: string;
  if (cred) {
    opts = `    cred_provider = dbutils.credentials.getServiceCredentialsProvider("${cred}")
    eh_conn_str = cred_provider.get("eventhub.connection_string", "")
    read_opts = {
        "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(eh_conn_str),
    }`;
  } else {
    opts = `    read_opts = {
        "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt("${connection_string}"),
    }`;
  }

  return [HEADER, cell(streamingReadTest("eventhubs", opts, "event_hub"))].join(SEP);
}

function rabbitmqNotebook(conn: NonNullable<SssConnectionConfig["rabbitmq"]>): string {
  const { host, port, queue, username, service_credential } = conn;
  const cred = service_credential?.trim() || "";

  let opts: string;
  if (cred) {
    opts = `    cred_provider = dbutils.credentials.getServiceCredentialsProvider("${cred}")
    rmq_user = cred_provider.get("rabbitmq.username", "guest")
    rmq_pass = cred_provider.get("rabbitmq.password", "")
    read_opts = {
        "host": "${host}",
        "port": "${port}",
        "queueName": "${queue}",
        "username": rmq_user,
        "password": rmq_pass,
    }`;
  } else {
    opts = `    read_opts = {
        "host": "${host}",
        "port": "${port}",
        "queueName": "${queue}",
        "username": "${username}",
    }`;
  }

  return [HEADER, cell(streamingReadTest("org.apache.spark.sql.rabbitmq", opts, "rabbitmq"))].join(SEP);
}

function pubsubNotebook(conn: NonNullable<SssConnectionConfig["pubsub"]>): string {
  const { project_id, subscription, service_credential } = conn;
  const cred = service_credential?.trim() || "";

  let opts: string;
  if (cred) {
    opts = `    cred_provider = dbutils.credentials.getServiceCredentialsProvider("${cred}")
    gcp_creds = cred_provider.get("pubsub.credentials_json", "")
    read_opts = {
        "projectId": "${project_id}",
        "subscriptionId": "${subscription}",
        "credentialsJson": gcp_creds,
    }`;
  } else {
    opts = `    read_opts = {
        "projectId": "${project_id}",
        "subscriptionId": "${subscription}",
    }`;
  }

  return [HEADER, cell(streamingReadTest("pubsub", opts, "pubsub"))].join(SEP);
}

export function generateConnectivityTestNotebook(
  source: SssSource,
  connection: SssConnectionConfig,
  wheelPath: string,
): string {
  switch (source) {
    case "kafka":
      if (!connection.kafka) throw new Error("Kafka connection config is required");
      return kafkaNotebook(connection.kafka, wheelPath);
    case "kinesis":
      if (!connection.kinesis) throw new Error("Kinesis connection config is required");
      return kinesisNotebook(connection.kinesis);
    case "event_hub":
      if (!connection.event_hub) throw new Error("Event Hub connection config is required");
      return eventHubNotebook(connection.event_hub);
    case "rabbitmq":
      if (!connection.rabbitmq) throw new Error("RabbitMQ connection config is required");
      return rabbitmqNotebook(connection.rabbitmq);
    case "pubsub":
      if (!connection.pubsub) throw new Error("Pub/Sub connection config is required");
      return pubsubNotebook(connection.pubsub);
    default:
      throw new Error(`Unsupported source: ${source}`);
  }
}
