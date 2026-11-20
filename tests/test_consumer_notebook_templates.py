"""Guard tests for the industry-first consumer notebook generators.

The RTM / micro-batch consumers are generated client-side in TypeScript
(``client/src/lib/notebook-template.ts``); there is no TS test runner in this
repo, so — following the same approach as ``tests/test_sensor_schema.py`` — these
guards read the template source and assert the spike-VERIFIED Python patterns
(spec §1.10, §1.11, §1.15) are present. If the template drifts from the proven
config, these tests fail.

Verified invariants asserted here:
  * RTM/micro-batch read Kafka with ONLY ``databricks.serviceCredential`` — no
    ``kafka.sasl.*`` / ``kafka.security.protocol`` (UC credential handles MSK-IAM).
  * Real-time writer: ``.outputMode("update").foreach(LakebaseWriter())
    .trigger(realTime=...)`` with a POSITIONAL-tuple ForeachWriter INSERTing into
    the LakeTS ChronoTable using a driver-minted ``generate_database_credential``
    token broadcast to executors.
  * Micro-batch writer: Delta ``.foreachBatch(write_micro_batch)`` (saveAsTable)
    on ``.trigger(processingTime="15 seconds")``.
  * Metrics: buffered Zerobus listener (queue + daemon thread + Zerobus SDK
    ``ingest_record`` into ``{industry}_metrics`` managed table; NO
    ``saveAsTable`` and NO Spark write / delta-rs; ``mode`` STRING column;
    timestamp as epoch-µs INT; progress blob dropped — Task 4).
  * Parse schema = all 11 SSOT fields (models/sensor_schema.py).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from iot_twin.models import sensor_schema as ss

NOTEBOOK_TS = (
    Path(__file__).resolve().parent.parent / "client" / "src" / "lib" / "notebook-template.ts"
)
DEPLOY_CONFIG_TS = (
    Path(__file__).resolve().parent.parent / "client" / "src" / "lib" / "industry-deploy-config.ts"
)

pytestmark = pytest.mark.skipif(
    not NOTEBOOK_TS.exists(), reason="notebook template not present (client/ gitignored)"
)


def _ts() -> str:
    return NOTEBOOK_TS.read_text()


def _kafka_read_template() -> str:
    """Extract the shared ``kafkaReadTemplate`` template-literal body."""
    text = _ts()
    m = re.search(r"const kafkaReadTemplate = .*?=> `(.*?)`;", text, re.DOTALL)
    assert m is not None, "could not locate kafkaReadTemplate"
    return m.group(1)


def _ssot_schema_block() -> str:
    text = _ts()
    m = re.search(r"const SSOT_FROM_JSON_SCHEMA = `(.*?)`;", text, re.DOTALL)
    assert m is not None, "could not locate SSOT_FROM_JSON_SCHEMA"
    return m.group(1)


def _metrics_listener_block() -> str:
    """Region from the Zerobus-metrics header to addListener()."""
    text = _ts()
    m = re.search(
        r"# Streaming metrics — buffered async Zerobus push.*?"
        r"spark\.streams\.addListener\(BufferedDeltaMetricsListener\(\)\)",
        text,
        re.DOTALL,
    )
    assert m is not None, (
        "could not locate Zerobus metrics listener block in notebook-template.ts — "
        "expected header '# Streaming metrics — buffered async Zerobus push'"
    )
    return m.group(0)


def _lakebase_writer_block() -> str:
    """Extract the generated Lakebase ForeachWriter template."""
    text = _ts()
    match = re.search(
        r"const LAKEBASE_FOREACH_WRITER = `(.*?)`;",
        text,
        re.DOTALL,
    )
    assert match is not None, "could not locate LAKEBASE_FOREACH_WRITER"
    return match.group(1)


class TestKafkaRead:
    def test_uses_service_credential(self) -> None:
        block = _kafka_read_template()
        assert "databricks.serviceCredential" in block

    def test_no_sasl_or_security_protocol(self) -> None:
        # MSK-IAM is handled by the UC service credential; sasl/security.protocol
        # options must NOT appear in the verified read (spec §1.10).
        block = _kafka_read_template()
        assert "kafka.sasl" not in block
        assert "security.protocol" not in block

    def test_reads_kafka_latest(self) -> None:
        # Reads only NEW messages ("latest"). "earliest" replayed historical rows
        # whose event_time predates the LakeTS ChronoTable's rolling partitions →
        # psycopg CheckViolation "no partition of relation sensor_readings".
        block = _kafka_read_template()
        assert 'spark.readStream.format("kafka")' in block
        assert '.option("startingOffsets", "latest")' in block
        assert '"earliest"' not in block


class TestParseSchema:
    def test_all_eleven_ssot_fields_present(self) -> None:
        block = _ssot_schema_block()
        for spec in ss.SENSOR_FIELD_MAP:
            assert f'"{spec.record_field}"' in block, (
                f"parse schema missing SSOT field {spec.record_field}"
            )

    def test_field_names_match_ssot_order(self) -> None:
        block = _ssot_schema_block()
        names = re.findall(r'\.add\("([^"]+)"', block)
        assert names == [f for f, _ in ss.kafka_schema_fields()]

    def test_epoch_timestamp_is_double_metadata_is_map(self) -> None:
        block = _ssot_schema_block()
        ts_line = next(ln for ln in block.splitlines() if '"timestamp"' in ln)
        md_line = next(ln for ln in block.splitlines() if '"metadata"' in ln)
        assert "DoubleType" in ts_line
        assert "MapType" in md_line


class TestRealtimeWriter:
    def test_checkpoint_uses_configured_shared_uc_volume_c7216(self) -> None:
        """Production checkpoints must use the shared UC schema [C7216][defect]."""
        text = DEPLOY_CONFIG_TS.read_text()
        assert "checkpointLocation(ucCatalog, ucSchema, industry, mode)" in text
        assert "/Volumes/${catalog}/${schema}/${CHECKPOINT_VOLUME}/${industry}/${suffix}" in text
        assert "/Volumes/${catalog}/${CHECKPOINT_SCHEMA}" not in text

    def test_all_delta_targets_use_one_catalog_schema_c7217(self) -> None:
        """Deploy targets must not recreate per-industry catalogs [C7217][regression]."""
        text = DEPLOY_CONFIG_TS.read_text()
        assert "industryCatalog(" not in text
        assert "MICROBATCH_SCHEMA" not in text
        assert "`${ucCatalog}.${ucSchema}.${catalogSlug(industry)}_microbatch`" in text

    def test_writer_chain_is_verified_rtm_shape(self) -> None:
        text = _ts()
        assert '.outputMode("update")' in text
        # The RTM sink is now a dual writer: it fans each row to the Lakebase
        # ChronoTable (source of truth) AND the app SSE bridge. The stream is
        # driven by RealtimeDualWriter, which itself delegates to LakebaseWriter.
        assert ".foreach(RealtimeDualWriter())" in text
        assert "self.lakebase = LakebaseWriter()" in text
        assert ".trigger(realTime=" in text

    def test_realtime_spark_conf_enables_rtm(self) -> None:
        text = _ts()
        assert "spark.databricks.streaming.realTimeMode.enabled" in text

    def test_default_realtime_interval_is_15_seconds(self) -> None:
        text = _ts()
        assert 'DEFAULT_REALTIME_INTERVAL = "15 seconds"' in text

    def test_foreachwriter_uses_positional_tuple(self) -> None:
        text = _ts()
        assert "def process(self, row):" in text
        # POSITIONAL tuple access (row[0]..row[10]), not named-Row (r.field).
        assert "row[0]" in text and "row[10]" in text
        assert "POSITIONAL tuple" in text

    def test_lakebase_oauth_primary_endpoint(self) -> None:
        text = _ts()
        assert "postgres.generate_database_credential" in text
        # Writes must target the writable PRIMARY endpoint BY NAME. endpoint_type
        # is None on these branches and the replica is listed first, so any
        # type/list-based selection lands on the read replica → "cannot execute
        # INSERT in a read-only transaction". Resolve `.../endpoints/primary`.
        assert "endpoints/primary" in text
        assert "postgres.get_endpoint" in text
        assert 'sslmode="require"' in text

    def test_lakebase_connect_uses_driver_minted_token_c7258(self) -> None:
        """Executors must not call WorkspaceClient; driver only fail-fast mints [C7258][defect]."""
        block = _lakebase_writer_block()
        before, _, _rest = block.partition("class LakebaseWriter:")
        assert "generate_database_credential" in before
        assert "WorkspaceClient()" in before
        assert "_lb_primary_name_bc = spark.sparkContext.broadcast(" in before
        connect = re.search(
            r"    def _connect\(self\):(.*?)\n    def _disconnect",
            block,
            re.DOTALL,
        )
        assert connect is not None
        body = connect.group(1)
        assert "generate_database_credential" not in body
        assert "WorkspaceClient(" not in body
        assert "self._fresh_pg_token()" in body
        assert "_lb_token_bc" not in body
        assert "user=_lb_user_bc.value" in body

    def test_lakebase_reconnect_remints_oauth_via_oidc_c7272(self) -> None:
        """Reconnect must mint a new Postgres OAuth token, not reuse a broadcast [C7272][defect]."""
        block = _lakebase_writer_block()
        assert "_lb_token_bc" not in block
        assert "TOKEN_TTL_S = 20 * 60" in block
        mint = re.search(
            r"    def _fresh_pg_token\(self\):(.*?)\n    def _connect",
            block,
            re.DOTALL,
        )
        assert mint is not None
        body = mint.group(1)
        assert "/oidc/v1/token" in body
        assert "/api/2.0/postgres/credentials" in body
        assert "WorkspaceClient(" not in body
        assert 'json={"endpoint": _lb_primary_name_bc.value}' in body
        assert 'return cred.json()["token"]' in body
        reconnect = re.search(
            r"    def _reconnect\(self\):(.*?)\n    def _delay",
            block,
            re.DOTALL,
        )
        assert reconnect is not None
        assert "self._connect_with_backoff()" in reconnect.group(1)
        assert "_lb_req.RequestException" in block

    def test_inserts_into_chronotable_with_all_columns(self) -> None:
        text = _ts()
        # The INSERT column list must carry all SSOT columns + source.
        for col in (
            "sensor_name",
            "industry",
            "value",
            "unit",
            "sensor_type",
            "event_time",
            "min_value",
            "max_value",
            "nominal_value",
            "fault_active",
            "metadata",
        ):
            assert col in text, f"INSERT missing column {col}"

    def test_batches_lakebase_commits_c7218(self) -> None:
        """Lakebase writes must amortize acknowledgements [C7218][performance]."""
        block = _lakebase_writer_block()
        assert "execute_values(" in block
        assert "self.conn.autocommit = False" in block
        assert "self.conn.commit()" in block
        assert "self.cur.execute(self.sql, params)" not in block
        assert '"source) VALUES %s"' in block
        assert (
            "_INSERT_TEMPLATE = \"(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'sss')\""
        ) in block

    def test_flushes_by_size_age_and_clean_close_c7218(self) -> None:
        """Batching must retain a sub-second flush bound [C7218][regression]."""
        block = _lakebase_writer_block()
        assert "BATCH_SIZE = 100" in block
        assert "MAX_BATCH_AGE_S = 0.10" in block
        assert "len(self.buffer) >= self.BATCH_SIZE" in block
        assert "_lb_time.monotonic() - self._last_flush >= self.MAX_BATCH_AGE_S" in block
        open_block = re.search(
            r"    def open\(self, partition_id, epoch_id\):(.*?)"
            r"\n    def process",
            block,
            re.DOTALL,
        )
        assert open_block is not None
        assert open_block.group(1).index("self._connect_with_backoff()") < open_block.group(
            1
        ).index("self._last_flush = _lb_time.monotonic()")
        close_block = re.search(r"    def close\(self, error\):(.*)$", block, re.DOTALL)
        assert close_block is not None
        assert "if error is None:\n                self._flush()" in close_block.group(1)
        assert "else:\n                self._rollback()" in close_block.group(1)

    def test_batch_failure_rolls_back_and_retries_c7218(self) -> None:
        """Failed commits must not be acknowledged as delivered [C7218][defect]."""
        block = _lakebase_writer_block()
        assert "self.conn.rollback()" in block
        assert "except (psycopg2.OperationalError, psycopg2.InterfaceError):" in block
        assert "self._reconnect()" in block
        assert "raise" in block
        flush_block = re.search(
            r"    def _flush_with_backoff\(self\):(.*?)\n    def _flush",
            block,
            re.DOTALL,
        )
        assert flush_block is not None
        flush_body = flush_block.group(1)
        assert flush_body.count("self.buffer = []") == 1
        assert flush_body.rindex("self.buffer = []") > flush_body.rindex("except Exception:")
        assert "ambiguous batch" in flush_body


class TestRealtimeAppSseBridge:
    """The realtime consumer ALSO POSTs each batch to the app for the live SSE
    dashboard, in addition to the psycopg Lakebase write (dual writer)."""

    def _sse_block(self) -> str:
        text = _ts()
        m = re.search(r"const REALTIME_APP_SSE_WRITER = `(.*?)`;", text, re.DOTALL)
        assert m is not None, "could not locate REALTIME_APP_SSE_WRITER"
        return m.group(1)

    def test_dual_writer_delegates_to_lakebase_and_app(self) -> None:
        block = self._sse_block()
        assert "class RealtimeDualWriter" in block
        assert "self.lakebase = LakebaseWriter()" in block
        assert "self.app = AppSseWriter()" in block

    def test_lakebase_gates_writer_sse_is_best_effort(self) -> None:
        """Only Lakebase may gate checkpoint progress [C7219][regression]."""
        block = self._sse_block()
        dual_writer = block.split("class RealtimeDualWriter:", 1)[1]
        assert "lb_ok = self.lakebase.open" in dual_writer
        assert "return lb_ok" in dual_writer
        assert "try:\n            self._app_ok = self.app.open" in dual_writer
        assert "if self._app_ok:" in dual_writer

    def test_process_only_enqueues_dashboard_rows_c7219(self) -> None:
        """RTM process must perform no HTTP or OAuth I/O [C7219][performance]."""
        block = self._sse_block()
        app_writer = block.split("class AppSseWriter:", 1)[1].split("class RealtimeDualWriter:", 1)[
            0
        ]
        process = re.search(
            r"    def process\(self, row\):(.*?)\n    def _enqueue",
            app_writer,
            re.DOTALL,
        )
        assert process is not None
        process_body = process.group(1)
        assert "self._enqueue()" in process_body
        assert "_post(" not in process_body
        assert "urlopen(" not in process_body
        assert "_mint_token(" not in process_body

    def test_uses_bounded_daemon_sender_c7219(self) -> None:
        """Dashboard delivery runs on a bounded daemon [C7219][regression]."""
        block = self._sse_block()
        assert "import queue as _sse_queue" in block
        assert "import threading as _sse_threading" in block
        assert "QUEUE_SIZE = 2" in block
        assert "_sse_queue.Queue(maxsize=self.QUEUE_SIZE)" in block
        assert "_sse_threading.Thread(" in block
        assert "args=(self.queue,)" in block
        assert "def _sender_loop(self, sender_queue):" in block
        assert "batch = sender_queue.get()" in block
        assert "daemon=True" in block
        assert "self.sender.join(timeout=self.JOIN_TIMEOUT_S)" in block

    def test_queue_full_is_nonblocking_and_best_effort_c7219(self) -> None:
        """Stale dashboard batches are dropped, not backpressured [C7219][performance][regression]."""
        block = self._sse_block()
        assert "self.queue.put_nowait(batch)" in block
        assert "except _sse_queue.Full:" in block
        assert "self.queue.get_nowait()" in block
        assert "oldest\n# stale dashboard batch is discarded" in block

    def test_posts_to_app_ingest_with_pipeline_id_header(self) -> None:
        block = self._sse_block()
        assert "/api/sss/ingest" in block
        assert '"X-Pipeline-Id": _sse_pipeline_id_bc.value' in block
        assert 'method="POST"' in block

    def test_mints_oauth_token_client_credentials_all_apis(self) -> None:
        block = self._sse_block()
        assert "/oidc/v1/token" in block
        assert '"grant_type": "client_credentials"' in block
        assert '"scope": "all-apis"' in block
        # The deploy service normalizes these legacy template references to the
        # single app SP + UC secret immediately before upload.
        assert 'dbutils.secrets.get(APP_SECRET_SCOPE, "sss-industries-client-id")' in _ts()
        assert "def open(self, partition_id, epoch_id):" in block

    def test_retries_once_on_401_403(self) -> None:
        block = self._sse_block()
        assert "if error.code in (401, 403):" in block
        assert "token = self._mint_token()" in block

    def test_config_cell_defines_app_url_and_pipeline_id(self) -> None:
        text = _ts()
        assert "APP_URL = " in text
        assert "PIPELINE_ID = " in text
        # industry-first realtime pipelines default to the industry: cache key.
        assert "industry:${cfg.industry}" in text


class TestMicrobatchWriter:
    def test_processing_time_fifteen_seconds(self) -> None:
        text = _ts()
        assert 'DEFAULT_MICROBATCH_INTERVAL = "15 seconds"' in text
        assert ".trigger(processingTime=" in text

    def test_writes_to_delta_table_via_foreachbatch(self) -> None:
        # Micro-batch now writes via foreachBatch (so it can also record the real
        # event-time e2e latency), appending to the Delta table with saveAsTable.
        text = _ts()
        assert '.format("delta")' in text
        assert ".foreachBatch(write_micro_batch)" in text
        assert "saveAsTable(DELTA_TABLE)" in text


class TestBufferedDeltaMetricsListener:
    def test_buffers_via_queue_and_daemon_thread(self) -> None:
        block = _metrics_listener_block()
        assert "queue" in block.lower()
        assert "daemon=True" in block
        # callback enqueues only — no Spark work.
        assert "_metrics_q.put" in block

    def test_flushes_via_zerobus_not_spark_save(self) -> None:
        # Task 4: the daemon thread flushes via Zerobus SDK ingest_record(),
        # NOT Spark .save(LOCATION) and NOT delta-rs.
        block = _metrics_listener_block()
        assert "_zb_metrics_stream.ingest_record(rec)" in block
        assert ".save(METRICS_TABLE_LOCATION)" not in block
        assert ".save(" not in block
        assert "spark.createDataFrame(" not in block
        assert "write_deltalake" not in block

    def test_never_uses_saveastable(self) -> None:
        # saveAsTable from the listener launches a contending Spark job (§1.11).
        # The Zerobus write path must never use saveAsTable.
        block = _metrics_listener_block()
        assert ".saveAsTable(" not in block

    def test_no_static_aws_keys(self) -> None:
        block = _metrics_listener_block()
        assert "AWS_ACCESS_KEY_ID" not in block
        assert "AWS_SECRET_ACCESS_KEY" not in block

    def test_mode_column_present(self) -> None:
        # Task 4: managed table carries a `mode` column; the writer must include it.
        block = _metrics_listener_block()
        assert '"mode": MODE' in block

    def test_timestamp_is_epoch_microseconds(self) -> None:
        # Task 4: timestamp is epoch-µs INT (Zerobus maps to TIMESTAMP column),
        # NOT a Python datetime object.
        block = _metrics_listener_block()
        assert "timestamp() * 1_000_000" in block

    def test_no_raw_progress_blob(self) -> None:
        # Task 4: raw progress blob dropped (managed table; no delta-rs STRING
        # constraint). The key '"progress": progress_json' must not appear.
        block = _metrics_listener_block()
        assert '"progress": progress_json' not in block


class TestPipInstall:
    def test_does_not_upgrade_boto3(self) -> None:
        text = _ts()
        # No actual %pip install command may upgrade boto3 (breaks the kernel).
        pip_cmds = [ln for ln in text.splitlines() if ln.lstrip().startswith("%pip install")]
        assert pip_cmds, "expected at least one %pip install command"
        for cmd in pip_cmds:
            assert "boto3" not in cmd
        assert "databricks-sdk==0.120.0" in text
        # Task 4: deltalake replaced by databricks-zerobus-ingest-sdk for metrics write.
        assert "databricks-zerobus-ingest-sdk" in text
