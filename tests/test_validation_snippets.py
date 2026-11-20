"""Content tests for the cluster-side validation snippets (spec §1.7–1.11).

The snippets run on the harness cluster where they can't be unit-executed, but
their *content* must carry the VERIFIED configs — so we assert those tokens are
present (service credential, public :9198 bootstrap, RTM read options, a bounded
producer + distinct validation checkpoint for test-data isolation, the buffered
delta-rs metrics listener, the specific CDF shadow table, https:// OIDC).
"""

from __future__ import annotations

from iot_twin.services.validation import validation_snippets as snip
from tests._fakes import fake_credential

# Name of the UC secret holding the Zerobus OAuth value (a key name, not a secret).
ZEROBUS_OAUTH_ENTRY = "oauth-secret-key"


def test_connectivity_probe_checks_both_and_egress():
    code = snip.connectivity_probe("b-1:9198", "b-1:9098")
    assert "9198" in code and "9098" in code
    assert "checkip.amazonaws.com" in code  # egress IP diagnostic (§1.7)
    assert snip.OK_SENTINEL in code and snip.FAIL_SENTINEL in code


def test_topic_create_uses_msk_iam_oauth():
    code = snip.topic_create("b-1:9198", [("iot.x.realtime", 3, 3)], "us-east-1", "iot-rtm-sc")
    assert "OAUTHBEARER" in code
    assert "AbstractTokenProvider" in code  # kafka-python 3.x subclass
    assert "MSKAuthTokenProvider" in code
    assert "us-east-1" in code
    assert "iot.x.realtime" in code


def _rtm_write_test_code() -> str:
    return snip.rtm_write_test(
        "iot.x.realtime",
        "b-1:9198",
        "iot-rtm-sc",
        project_id="iot-pipelines",
        branch="x",
        pg_database="databricks_postgres",
        table="sensor_readings",
        checkpoint_path="/Volumes/x/checkpoints/v/validation/realtime",
        metrics_table_fqn="iot_x.metrics.streaming_metrics",
        metrics_table_location="s3://bkt/iot-digital-twin/iot_x/metrics/streaming_metrics/",
        pg_host="example-pg.database.us-east-1.cloud.databricks.com",
        pg_user="app-user",
        pg_token=fake_credential("lakebase"),
        zerobus_endpoint="workspace.zerobus.us-east-1.cloud.databricks.com",
        workspace_url="https://workspace.cloud.databricks.com",
        zerobus_client_id="app-client-id",
        zerobus_secret_key=ZEROBUS_OAUTH_ENTRY,
        region="us-east-1",
    )


def test_rtm_write_test_verified_read_config():
    code = _rtm_write_test_code()
    # Service credential handles MSK-IAM — no plaintext SASL on the Spark read.
    assert '"databricks.serviceCredential"' in code
    assert "iot-rtm-sc" in code
    assert "kafka.sasl" not in code
    # Bounded test window reads from earliest into a distinct validation checkpoint.
    assert '"earliest"' in code
    assert "/validation/realtime" in code
    # Real-time trigger + update output mode.
    assert "realTime=" in code
    assert 'outputMode("update")' in code


def test_rtm_write_test_lands_data_not_noop():
    """The RTM consumer must write via a psycopg ForeachWriter — never noop."""
    code = _rtm_write_test_code()
    assert '.format("noop")' not in code
    assert ".foreach(" in code
    # psycopg ForeachWriter with positional-tuple process() into the ChronoTable.
    assert "psycopg2" in code
    assert "class LakebaseWriter" in code
    assert "def process(self, row)" in code
    assert "row[0]" in code and "row[10]" in code  # positional, not named Row
    assert "INSERT INTO" in code
    assert "sensor_readings" in code
    # Creds are INJECTED by the harness driver as literals — executors can't call
    # generate_database_credential (no default auth) and the cluster SDK lacks the
    # .postgres API. So the snippet has NO cluster-side credential resolution; the
    # pg_host/pg_user literals appear and psycopg2 connects with them.
    assert "generate_database_credential" not in code
    assert "example-pg.database.us-east-1.cloud.databricks.com" in code  # injected pg_host
    assert "app-user" in code  # injected pg_user
    assert "psycopg2.connect" in code


def test_rtm_write_test_retries_transient_postgres_failures_with_backoff():
    """[C7232][defect][regression] Validation RTM tolerates Lakebase recovery."""
    code = _rtm_write_test_code()

    assert "MAX_RECONNECT_ATTEMPTS = 12" in code
    assert "BACKOFF_BASE_S = 1.0" in code
    assert "BACKOFF_MAX_S = 30.0" in code
    assert "def _connect_with_backoff(self):" in code
    assert "def _execute_with_backoff(self, params):" in code
    assert "_t.sleep(delay)" in code


def test_rtm_write_test_wires_buffered_metrics_listener():
    """The buffered Zerobus metrics listener must match the production transport."""
    code = _rtm_write_test_code()
    assert "BufferedZerobusMetricsListener" in code
    assert "spark.streams.addListener(" in code
    assert "_zb_metrics_stream.ingest_record(rec)" in code
    assert "write_deltalake" not in code
    assert "saveAsTable" not in code
    assert "onQueryProgress" in code
    assert "iot_x.metrics.streaming_metrics" in code  # metrics table fqn
    assert '"mode": "realtime"' in code
    assert ZEROBUS_OAUTH_ENTRY in code
    assert "workspace.zerobus.us-east-1.cloud.databricks.com" in code
    assert "dbutils.secrets.get(" in code
    assert "catalog=_zb_uc_catalog" in code
    assert "schema=_zb_uc_schema" in code
    assert "timestamp() * 1_000_000" in code
    assert "wait_for_ack(timeout_sec=30)" in code
    assert "spark.streams.removeListener(_metrics_listener)" in code
    assert 'getattr(q, "recentProgress", [])' in code
    assert "metrics_written=" in code


def test_rtm_produce_uses_curated_industry_c7245():
    """Producer preserves validation source and actual industry [C7245][regression]."""
    code = snip.rtm_produce(
        "iot.x.realtime",
        "b-1:9198",
        "us-east-1",
        "iot-rtm-sc",
        num_records=100,
        industry="manufacturing",
    )
    assert "KafkaProducer" in code
    assert "SASL_SSL" in code
    assert "OAUTHBEARER" in code
    assert "AbstractTokenProvider" in code
    assert "MSKAuthTokenProvider" in code
    assert "getServiceCredentialsProvider" in code
    assert "iot-rtm-sc" in code
    # api_version MUST be pinned (auto-detect hangs on MSK before SASL handshake).
    assert "api_version=(2, 8, 1)" in code
    assert "iot.x.realtime" in code
    assert "range(100)" in code
    assert "\"industry\": 'manufacturing'" in code
    assert '"metadata": {"source": \'validation\'}' in code
    # Returns the earliest event_time floor so the consumer can drop stale rows.
    assert "_min_ts = time.time()" in code
    assert "min_ts=_min_ts" in code


def test_rtm_write_test_freshness_filter_drops_stale_rows():
    """With a floor, the consumer must gate rows on event_time >= floor so stale
    prior-run messages (past-dated -> no ChronoTable partition) never INSERT."""
    code = snip.rtm_write_test(
        "iot.x.realtime",
        "b-1:9198",
        "iot-rtm-sc",
        project_id="iot-pipelines",
        branch="x",
        pg_database="databricks_postgres",
        table="sensor_readings",
        checkpoint_path="/Volumes/x/checkpoints/v/validation/realtime",
        metrics_table_fqn="iot_x.metrics.streaming_metrics",
        metrics_table_location="s3://bkt/iot_x/metrics/streaming_metrics/",
        pg_host="example-pg.database.us-east-1.cloud.databricks.com",
        pg_user="app-user",
        pg_token=fake_credential("lakebase"),
        region="us-east-1",
        min_event_epoch=1751780000.0,
    )
    assert "timestamp_seconds" in code
    assert "1751780000.0" in code
    assert 'F.col("event_time") >=' in code


def test_rtm_write_test_no_floor_no_filter():
    """Without a floor the freshness gate is a no-op (backwards compatible)."""
    code = _rtm_write_test_code()  # no min_event_epoch
    assert "_MIN_EVENT_EPOCH = None" in code
    assert "if _MIN_EVENT_EPOCH is not None:" in code


def test_remove_path_deletes_checkpoint():
    code = snip.remove_path("/Volumes/x/checkpoints/v/validation/realtime")
    assert "dbutils.fs.rm(" in code
    assert "/validation/realtime" in code


def test_microbatch_write_test_processing_time_trigger():
    code = snip.microbatch_write_test(
        "iot.x.microbatch",
        "b-1:9198",
        "iot-rtm-sc",
        "iot_x.microbatch.sensor_data",
        "/Volumes/x/checkpoints/v/validation/microbatch",
    )
    assert 'processingTime="10 seconds"' in code
    assert '"latest"' in code
    assert "iot_x.microbatch.sensor_data" in code
    # Must parse to the SSOT schema the table actually has — a raw-string write
    # collides with the real schema (DELTA_METADATA_MISMATCH).
    assert "CAST(value AS STRING) AS raw" not in code
    assert "from_json" in code
    assert 'alias("event_time")' in code
    assert "timestamp_seconds" in code
    assert 'alias("sensor_name")' in code
    # No metrics args → no listener wired.
    assert "BufferedZerobusMetricsListener" not in code


def test_microbatch_write_test_wires_metrics_listener_when_given():
    """With per-mode metrics args, micro-batch now captures streaming metrics too
    (previously only rtm_write_test had the listener → micro-batch metrics empty)."""
    code = snip.microbatch_write_test(
        "iot.x.microbatch",
        "b-1:9198",
        "iot-rtm-sc",
        "iot_x.microbatch.sensor_data",
        "/Volumes/x/checkpoints/v/validation/microbatch",
        metrics_table_fqn="iot_x.metrics.streaming_metrics_microbatch",
        metrics_table_location="s3://bkt/iot-digital-twin/iot_x/metrics/streaming_metrics_microbatch/",
        region="us-east-1",
        zerobus_endpoint="workspace.zerobus.us-east-1.cloud.databricks.com",
        workspace_url="https://workspace.cloud.databricks.com",
        zerobus_client_id="app-client-id",
        zerobus_secret_key=ZEROBUS_OAUTH_ENTRY,
    )
    assert "BufferedZerobusMetricsListener" in code
    assert "spark.streams.addListener(" in code
    assert "_zb_metrics_stream.ingest_record(rec)" in code
    assert "write_deltalake" not in code
    assert "saveAsTable" not in code
    assert "streaming_metrics_microbatch" in code
    assert "\"mode\": 'microbatch'" in code
    assert "spark.streams.removeListener(_metrics_listener)" in code
    assert 'getattr(stream, "recentProgress", [])' in code
    # Still parses to the SSOT schema + writes the table.
    assert "from_json" in code and "toTable(" in code


def test_cdf_verify_polls_streaming_and_lsn():
    code = snip.cdf_verify("sensor_readings")
    assert "wal2delta.tables" in code
    assert "STREAMING" in code
    assert "committed_lsn" in code


def test_cdf_verify_keys_on_specific_shadow_table():
    """cdf_verify must key on THIS pipeline's shadow lakets_cdf._shadow_<table>,
    not match ANY streaming table in wal2delta.tables (spec §1.10.3)."""
    code = snip.cdf_verify("sensor_readings")
    assert "_shadow_sensor_readings" in code
    assert "lakets_cdf" in code
    # A different pipeline's shadow table must not be conflated.
    other = snip.cdf_verify("iot_sensor_data")
    assert "_shadow_iot_sensor_data" in other
    assert "_shadow_sensor_readings" not in other


def test_zerobus_produce_https_prefix_gotcha():
    code = snip.zerobus_produce(
        "myws.cloud.databricks.com", "ws.zerobus.x", "iot_x.zerobus.iot_sensor_data"
    )
    assert "/oidc/v1/token" in code
    assert "https://" in code  # https:// prefix is the known gotcha (§1.10)
