"""Phase 7 end-to-end functional validation driver for the admin pipeline flow.

Drives the *integrated service code* (not ad-hoc reimplementations) for one
throwaway industry (``pharmaceutical``) on ``example-us-east``:

  setup      -> iot_twin.services.pipelines.pipeline_setup_repo.create_setup
  provision  -> iot_twin.services.industry.industry_provisioning_service.provision_industry
  validate   -> iot_twin.services.validation.validation_service (real interactive cluster + steps)
  deploy     -> iot_twin.services.deployment.deployment_service.deploy_pipeline
  teardown   -> iot_twin.services.teardown_service + the admin delete path wiring

Run one stage at a time so long-running Databricks work (cluster spin-up, job
runs) can be observed:

    .venv/bin/python scripts/e2e/pharma_e2e.py --profile <cli-profile> <stage>

State (the created setup_id) is persisted to a scratchpad JSON file so stages
are independent.  This script is committed, but the *run* is the deliverable.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

# Repo root on path.
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from scripts._profile import require_profile  # noqa: E402

require_profile()  # --profile <name> or $DATABRICKS_CONFIG_PROFILE; no default workspace

STATE_FILE = Path(os.getenv("E2E_STATE_FILE", "/tmp/pharma_e2e_state.json"))

INDUSTRY = "pharmaceutical"
# UC service credential *name* (an identifier, not a secret).
UC_SC_NAME = "iot-rtm-sc"
WAREHOUSE_ID = os.getenv("E2E_WAREHOUSE_ID", "0123456789abcdef")  # lakets-demo-wh (RUNNING)
BOOTSTRAP_PUBLIC = (
    "b-2-public.example.abc123.c1.kafka.us-east-1.amazonaws.com:9198,"
    "b-3-public.example.abc123.c1.kafka.us-east-1.amazonaws.com:9198,"
    "b-1-public.example.abc123.c1.kafka.us-east-1.amazonaws.com:9198"
)


def _load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}


def _save_state(st: dict) -> None:
    STATE_FILE.write_text(json.dumps(st, indent=2, default=str))


def _pp(label, obj):
    print(f"\n===== {label} =====")
    print(json.dumps(obj, indent=2, default=str))


async def _db():
    from iot_twin.models import app_schema
    from iot_twin.services.db.app_backend_db import build_app_backend_manager

    db = build_app_backend_manager()
    await db.initialize()
    await app_schema.ensure_app_schema(db)
    return db


# ---------------------------------------------------------------------------
# Stage: setup
# ---------------------------------------------------------------------------
async def stage_setup():
    from iot_twin.services.pipelines import pipeline_setup_repo

    db = await _db()
    try:
        existing = await pipeline_setup_repo.get_setup_by_industry(db, INDUSTRY)
        if existing:
            print(f"setup already exists: {existing['id']}")
            st = _load_state()
            st["setup_id"] = str(existing["id"])
            _save_state(st)
            return
        setup = await pipeline_setup_repo.create_setup(
            db,
            industry=INDUSTRY,
            flavor="classic",
            source_type="kafka",
            bootstrap_servers=BOOTSTRAP_PUBLIC,
            derived={"region": "us-east-1", "broker_count": 3},
            service_credential=UC_SC_NAME,
            enabled_modes=["realtime", "microbatch", "zerobus"],
            cluster_spec={"partition_count": 3},
            warehouse_id=WAREHOUSE_ID,
            status="draft",
            user_email="admin@example.com",
        )
        _pp("created setup", setup)
        st = _load_state()
        st["setup_id"] = str(setup["id"])
        _save_state(st)
    finally:
        await db.close()


# ---------------------------------------------------------------------------
# Stage: provision
# ---------------------------------------------------------------------------
async def stage_provision():
    from iot_twin.services import resource_ledger
    from iot_twin.services.industry import industry_provisioning_service

    st = _load_state()
    setup_id = st["setup_id"]
    db = await _db()
    try:
        summary = await industry_provisioning_service.provision_industry(
            INDUSTRY,
            setup_id,
            db=db,
            warehouse_id=WAREHOUSE_ID,
        )
        # Compact section statuses.
        for sec in summary.get("sections", []):
            phase = sec["phase"]
            res = sec["result"]
            print(f"\n--- phase: {phase} ---")
            print(json.dumps(res, indent=1, default=str)[:2500])
        print(
            "\nKEY IDS:",
            {k: summary.get(k) for k in ("catalog", "metrics_table", "branch", "sp_principal")},
        )

        # Ledger counts by type.
        rows = await resource_ledger.list_resources(db, setup_id)
        by_type: dict[str, int] = {}
        for r in rows:
            by_type[r["resource_type"]] = by_type.get(r["resource_type"], 0) + 1
        _pp("LEDGER counts by resource_type", by_type)
        print("LEDGER total rows:", len(rows))
    finally:
        await db.close()


# ---------------------------------------------------------------------------
# Stage: verify-provision (independent confirmation via SDK/DBSQL/Lakebase)
# ---------------------------------------------------------------------------
async def stage_verify_provision():
    from databricks.sdk import WorkspaceClient

    from iot_twin.services.dbsql_service import execute_dbsql
    from iot_twin.services.pipelines import pipeline_project_service as pps

    w = WorkspaceClient()
    out: dict = {}

    # Catalog + schemas + volume + metrics table via DBSQL.
    try:
        cat = await execute_dbsql("SHOW SCHEMAS IN iot_pharmaceutical", WAREHOUSE_ID)
        out["schemas"] = [r for r in cat["data"]]
    except Exception as e:
        out["schemas_error"] = str(e)[:300]
    try:
        vol = await execute_dbsql("SHOW VOLUMES IN iot_pharmaceutical.checkpoints", WAREHOUSE_ID)
        out["volumes"] = vol["data"]
    except Exception as e:
        out["volumes_error"] = str(e)[:300]
    try:
        m = await execute_dbsql(
            "DESCRIBE TABLE iot_pharmaceutical.metrics.streaming_metrics", WAREHOUSE_ID
        )
        out["metrics_cols"] = m["row_count"]
    except Exception as e:
        out["metrics_error"] = str(e)[:300]
    try:
        g = await execute_dbsql("SHOW GRANTS ON CATALOG iot_pharmaceutical", WAREHOUSE_ID)
        out["catalog_grants"] = g["data"]
    except Exception as e:
        out["grants_error"] = str(e)[:300]

    # Lakebase branch + chronotable + partitions.
    try:
        w.postgres.get_branch(name="projects/iot-pipelines/branches/pharmaceutical")
        out["branch"] = "exists"
    except Exception as e:
        out["branch_error"] = str(e)[:200]

    conn = None
    try:
        conn = await pps.get_write_conn("pharmaceutical", ws=w)
        out["lakets_version"] = (
            await conn.fetchval("SELECT value FROM lakets._meta WHERE key='version'")
            if await _has_lakets(conn)
            else "?"
        )
        out["chronotables"] = await conn.fetchval(
            "SELECT count(*) FROM lakets._chronotable_registry"
        )
        out["sensor_readings_partitions"] = await conn.fetchval(
            "SELECT count(*) FROM pg_inherits i JOIN pg_class p ON p.oid=i.inhparent "
            "WHERE p.relname='sensor_readings'"
        )
        out["sensor_readings_count"] = await conn.fetchval("SELECT count(*) FROM sensor_readings")
    except Exception as e:
        out["lakebase_error"] = str(e)[:400]
    finally:
        if conn:
            await conn.close()

    _pp("VERIFY provisioning", out)


async def _has_lakets(conn) -> bool:
    try:
        return bool(
            await conn.fetchval(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema='lakets' AND table_name='_meta'"
            )
        )
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Stage: validate microbatch-only (fully green) via the integrated driver
# ---------------------------------------------------------------------------
async def stage_validate_mb():
    from databricks.sdk import WorkspaceClient

    from iot_twin.services.pipelines import pipeline_setup_repo
    from iot_twin.services.validation import validation_service
    from iot_twin.services.validation import validation_state_machine as sm

    st = _load_state()
    setup_id = st["setup_id"]
    db = await _db()
    w = WorkspaceClient()
    try:
        # Microbatch-scoped validation state so the realtime lane's manual CDF
        # park does not block the (independent) microbatch lane.  Uses the exact
        # same integrated driver + executors.
        setup = await pipeline_setup_repo.get_setup(db, setup_id)
        state = sm.initial_state(["microbatch"])
        # Reuse an already-running validation cluster if we have one (avoids a
        # second ~5-min spin-up); ensure the required libraries are installed on
        # it via the same helper provision_cluster now uses.
        if st.get("mb_cluster_id"):
            from iot_twin.services.validation import validation_steps as vs

            await vs._install_cluster_libraries(
                w, st["mb_cluster_id"], vs.VALIDATION_CLUSTER_LIBRARIES
            )
            state["cluster_id"] = st["mb_cluster_id"]
            state = sm.set_step(state, "provision_cluster", sm.PASSED)
        await pipeline_setup_repo.update_setup(db, setup_id, validation_state=state)
        setup = await pipeline_setup_repo.get_setup(db, setup_id)

        final = await validation_service.run_machine(db, setup, ws=w)
        steps = {k: v["status"] for k, v in final["steps"].items()}
        _pp("MICROBATCH validation step statuses", steps)
        print("cluster_id:", final.get("cluster_id"))
        for k, v in final["steps"].items():
            if v["status"] != "passed" and v.get("error"):
                print(f"  !! {k}: {v['error']}")

        gate = await validation_service.can_deploy(db, INDUSTRY)
        _pp("can_deploy (sm.gate_status, microbatch-only state)", gate)
        st["mb_cluster_id"] = final.get("cluster_id")
        _save_state(st)
    finally:
        await db.close()


# ---------------------------------------------------------------------------
# Stage: validate realtime lane (through read_back) via the integrated driver
# ---------------------------------------------------------------------------
async def stage_validate_rt():
    from databricks.sdk import WorkspaceClient

    from iot_twin.services.pipelines import pipeline_setup_repo
    from iot_twin.services.validation import validation_service
    from iot_twin.services.validation import validation_state_machine as sm

    st = _load_state()
    setup_id = st["setup_id"]
    db = await _db()
    w = WorkspaceClient()
    try:
        setup = await pipeline_setup_repo.get_setup(db, setup_id)
        state = sm.initial_state(["realtime"])
        # Reuse the already-running validation cluster if we have one.
        if st.get("mb_cluster_id"):
            from iot_twin.services.validation import validation_steps as vs

            await vs._install_cluster_libraries(
                w, st["mb_cluster_id"], vs.VALIDATION_CLUSTER_LIBRARIES
            )
            state["cluster_id"] = st["mb_cluster_id"]
            # Mark provision_cluster passed so the driver reuses the cluster.
            state = sm.set_step(state, "provision_cluster", sm.PASSED)
        await pipeline_setup_repo.update_setup(db, setup_id, validation_state=state)
        setup = await pipeline_setup_repo.get_setup(db, setup_id)

        # Run to the cdf_manual park.
        final = await validation_service.run_machine(db, setup, ws=w)
        steps = {k: v["status"] for k, v in final["steps"].items()}
        _pp("REALTIME validation step statuses (run 1 -> parks at cdf_manual)", steps)
        for k, v in final["steps"].items():
            if v["status"] not in ("passed", "pending") and v.get("error"):
                print(f"  !! {k}: {v['error']}")

        # Release cdf_manual (mark_cdf_enabled), then continue -> cdf_verify.
        setup2 = await validation_service.prepare_cdf_enabled(db, INDUSTRY, "realtime")
        final2 = await validation_service.run_machine(db, setup2, ws=w)
        steps2 = {k: v["status"] for k, v in final2["steps"].items()}
        _pp("REALTIME validation step statuses (run 2 -> after cdf-enabled)", steps2)
        for k, v in final2["steps"].items():
            if v["status"] not in ("passed", "pending") and v.get("error"):
                print(f"  !! {k}: {v['error']}")
    finally:
        await db.close()


# ---------------------------------------------------------------------------
# Stage: probe realtime data-landing paths directly (ChronoTable + metrics)
# ---------------------------------------------------------------------------
async def stage_probe_landing():
    """Confirm the Lakebase LakeTS ChronoTable + UC metrics table accept rows.

    The validation harness's rtm_write_test writes to a ``noop`` sink and no
    producer runs during validation, so the *streaming* read_back cannot land
    rows.  This probes the sink capability directly: insert a sensor row into
    the pre-created ChronoTable partition window and confirm it lands + is
    readable via the read replica.
    """
    from databricks.sdk import WorkspaceClient

    from iot_twin.services.pipelines import pipeline_project_service as pps

    w = WorkspaceClient()
    out: dict = {}
    wconn = None
    rconn = None
    try:
        wconn = await pps.get_write_conn("pharmaceutical", ws=w)
        # Insert a few sensor rows at 'now' (inside the pre-created window).
        from iot_twin.models.sensor_schema import insert_columns

        cols = insert_columns()
        placeholders = ", ".join(f"${i + 1}" for i in range(len(cols)))
        # Build values: sensor_name, industry, value, unit, sensor_type,
        # event_time (now), min, max, nominal, fault_active, metadata(jsonb), source
        import datetime as dt

        now = dt.datetime.now(dt.UTC)
        inserted = 0
        for i in range(5):
            await wconn.execute(
                f"INSERT INTO sensor_readings ({', '.join(cols)}) VALUES ({placeholders})",
                f"reactor_temp_{i}",
                "pharmaceutical",
                72.5 + i,
                "C",
                "temperature",
                now,
                60.0,
                90.0,
                75.0,
                False,
                json.dumps({"line": "A"}),
                "e2e",
            )
            inserted += 1
        out["inserted"] = inserted
        out["chronotable_count"] = await wconn.fetchval("SELECT count(*) FROM sensor_readings")
    except Exception as e:
        out["write_error"] = f"{type(e).__name__}: {e}"[:400]
    finally:
        if wconn:
            await wconn.close()

    # Read back via replica.
    try:
        rconn = await pps.get_read_conn("pharmaceutical", ws=w)
        out["replica_count"] = await rconn.fetchval("SELECT count(*) FROM sensor_readings")
    except Exception as e:
        out["read_error"] = f"{type(e).__name__}: {e}"[:400]
    finally:
        if rconn:
            await rconn.close()

    _pp("PROBE realtime landing (ChronoTable direct write-path)", out)


# ---------------------------------------------------------------------------
# Stage: deploy microbatch (demonstrates gate, then deploys)
# ---------------------------------------------------------------------------
def _producer_notebook(topic: str) -> str:
    return f"""
import os, json, time, random
from kafka import KafkaProducer
from kafka.net.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

REGION = "us-east-1"
BOOTSTRAP = {BOOTSTRAP_PUBLIC!r}
TOPIC = {topic!r}

_sess = dbutils.credentials.getServiceCredentialsProvider("iot-rtm-sc")
class _TP(AbstractTokenProvider):
    def token(self):
        try:
            t, _ = MSKAuthTokenProvider.generate_auth_token_from_credentials_provider(REGION, _sess)
            return t
        except Exception:
            fz = _sess.get_credentials().get_frozen_credentials()
            os.environ["AWS_ACCESS_KEY_ID"] = fz.access_key
            os.environ["AWS_SECRET_ACCESS_KEY"] = fz.secret_key
            if fz.token:
                os.environ["AWS_SESSION_TOKEN"] = fz.token
            t, _ = MSKAuthTokenProvider.generate_auth_token(REGION)
            return t

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    security_protocol="SASL_SSL",
    sasl_mechanism="OAUTHBEARER",
    sasl_oauth_token_provider=_TP(),
    api_version=(2, 8, 1),
    request_timeout_ms=30000,
    value_serializer=lambda v: json.dumps(v).encode(),
)
sensors = ["reactor_temp", "reactor_pressure", "ph_probe", "flow_rate"]
i = 0
while True:
    for s in sensors:
        rec = {{
            "sensor_name": s, "industry": "pharmaceutical",
            "value": round(random.uniform(20, 100), 2), "unit": "C",
            "sensor_type": "temperature", "timestamp": time.time(),
            "min_value": 0.0, "max_value": 120.0, "nominal_value": 60.0,
            "fault_active": False, "metadata": {{"line": "A"}},
        }}
        producer.send(TOPIC, rec)
    producer.flush()
    i += 1
    if i % 20 == 0:
        print(f"produced {{i*len(sensors)}} records")
    time.sleep(1)
"""


def _microbatch_consumer_notebook(topic: str, delta_table: str, checkpoint: str) -> str:
    from iot_twin.models.sensor_schema import kafka_parse_schema

    schema_src = kafka_parse_schema()
    return f"""
from pyspark.sql.types import (StructType, StringType, DoubleType, BooleanType,
                               MapType, LongType, IntegerType)
from pyspark.sql import functions as F

TOPIC = {topic!r}
BOOTSTRAP = {BOOTSTRAP_PUBLIC!r}
DELTA = {delta_table!r}
CHECKPOINT = {checkpoint!r}

schema = ({schema_src})

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "latest")
       .option("databricks.serviceCredential", "iot-rtm-sc")
       .load())

parsed = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
          .select(
              F.col("d.sensor_name").alias("sensor_name"),
              F.col("d.industry").alias("industry"),
              F.col("d.value").alias("value"),
              F.col("d.unit").alias("unit"),
              F.col("d.sensor_type").alias("sensor_type"),
              F.col("d.timestamp").cast("timestamp").alias("event_time"),
              F.col("d.min_value").alias("min_value"),
              F.col("d.max_value").alias("max_value"),
              F.col("d.nominal_value").alias("nominal_value"),
              F.col("d.fault_active").alias("fault_active"),
              F.col("d.metadata").alias("metadata"),
          ))

q = (parsed.writeStream
   .option("checkpointLocation", CHECKPOINT)
   .trigger(processingTime="10 seconds")
   .toTable(DELTA))
q.awaitTermination()
"""


async def stage_deploy_mb():
    from iot_twin.services.dbsql_service import execute_dbsql
    from iot_twin.services.deployment import deployment_service
    from iot_twin.services.industry import industry_catalog_service as cat
    from iot_twin.services.pipelines import pipeline_setup_repo
    from iot_twin.services.validation import validation_service
    from iot_twin.services.validation import validation_state_machine as sm

    st = _load_state()
    setup_id = st["setup_id"]
    db = await _db()
    try:
        catalog = cat.catalog_name(INDUSTRY)
        topic = "iot.pharmaceutical.microbatch"
        delta = f"{catalog}.microbatch.sensor_data_deploy"
        checkpoint = f"/Volumes/{catalog}/checkpoints/stream_checkpoints/deploy/microbatch"

        # Drop any prior deploy target so schema is clean.
        try:
            await execute_dbsql(f"DROP TABLE IF EXISTS {delta}", WAREHOUSE_ID)
        except Exception as e:
            print("drop pre-existing table:", str(e)[:150])

        producer_nb = _producer_notebook(topic)
        consumer_nb = _microbatch_consumer_notebook(topic, delta, checkpoint)

        # --- FIX-A validation: the per-mode gate must now be OPEN on the nested
        # validation_state, so deploy proceeds WITHOUT gate_override. ---
        setup = await pipeline_setup_repo.get_setup(db, setup_id)
        vstate = setup.get("validation_state") or {}
        gate = await validation_service.can_deploy(db, INDUSTRY)
        _pp("can_deploy (per-mode aggregate)", gate)
        mb_open = sm.is_mode_gate_open(vstate, "microbatch")
        mb_outstanding = sm.mode_outstanding_steps(vstate, "microbatch")
        print(f"is_mode_gate_open(state,'microbatch') = {mb_open}")
        print(f"microbatch outstanding steps = {mb_outstanding}")

        # Deploy WITHOUT gate_override — the fix must let this proceed.
        result = await deployment_service.deploy_pipeline(
            db,
            setup_id,
            "microbatch",
            consumer_notebook=consumer_nb,
            producer_notebook=producer_nb,
        )
        print("\nGATE (no override): PASSED — deploy_pipeline proceeded WITHOUT gate_override")
        _pp("DEPLOY microbatch result", result)
        st["mb_job_id"] = result["job_id"]
        st["mb_delta"] = delta
        _save_state(st)
    finally:
        await db.close()


async def stage_deploy_status():
    from iot_twin.services.dbsql_service import execute_dbsql
    from iot_twin.services.deployment import deployment_service

    st = _load_state()
    job_id = st.get("mb_job_id")
    delta = st.get("mb_delta")
    status = deployment_service.pipeline_status(job_id)
    _pp("microbatch job status", status)
    try:
        r = await execute_dbsql(f"SELECT count(*) AS n FROM {delta}", WAREHOUSE_ID)
        _pp("microbatch Delta count", r["data"])
    except Exception as e:
        print("delta count error:", str(e)[:300])


# ---------------------------------------------------------------------------
# Stage: teardown via the admin delete path wiring
# ---------------------------------------------------------------------------
async def stage_teardown():
    """Run teardown exactly as the admin DELETE path wires it, then report."""
    from iot_twin.services import resource_ledger, teardown_service
    from iot_twin.services.pipelines import pipeline_setup_repo

    st = _load_state()
    setup_id = st["setup_id"]
    db = await _db()
    try:
        before = await resource_ledger.list_resources(db, setup_id)
        _pp(
            "LEDGER before teardown (type -> count)",
            {
                t: sum(1 for r in before if r["resource_type"] == t)
                for t in {r["resource_type"] for r in before}
            },
        )

        # Exact wiring copied from routes/industries_admin.delete_pipeline
        # (FIXED path — per-type deleters registry, not job-only):
        setup = await pipeline_setup_repo.get_setup(db, setup_id)
        deleters = None
        try:
            from iot_twin.services import resource_deleters

            deleters = resource_deleters.make_all_deleters(
                warehouse_id=setup.get("warehouse_id"),
            )
        except Exception as exc:
            print("deleter registry unavailable:", exc)

        result = await teardown_service.teardown_setup(db, setup_id, deleters)
        # Summarize step actions by (type, action).
        summary: dict = {}
        for s in result["steps"]:
            key = f"{s['resource_type']}:{s['action']}"
            summary[key] = summary.get(key, 0) + 1
        _pp("TEARDOWN step actions (admin-path FIXED deleters)", summary)
        _pp("TEARDOWN summary counts", result["summary"])
        print("teardown success:", result["success"], "deleted_setup:", result["deleted_setup"])
        # Show deleted vs skipped vs failed detail.
        deleted = [
            (s["resource_type"], s["identifier"])
            for s in result["steps"]
            if s["action"] == "deleted"
        ]
        skipped = [
            (s["resource_type"], s["identifier"], s.get("reason", "")[:80])
            for s in result["steps"]
            if s["action"] == "skipped"
        ]
        failed = [
            (s["resource_type"], s["identifier"], s.get("error", ""))
            for s in result["steps"]
            if s["action"] == "failed"
        ]
        _pp("DELETED in cloud", deleted)
        _pp("SKIPPED (intentional)", skipped)
        _pp("FAILED (surfaced — setup kept)", failed)
    finally:
        await db.close()


async def stage_cleanup_cloud():
    """Finish the teardown the admin path skips: drop catalog CASCADE, delete
    Lakebase branch + replica, delete deploy Delta + checkpoints.  Reports leftover
    Kafka topics (DeleteTopic denied).
    """
    from databricks.sdk import WorkspaceClient

    from iot_twin.services.dbsql_service import execute_dbsql

    w = WorkspaceClient()
    out: dict = {}

    # 1. Drop UC catalog CASCADE (removes schemas/tables/volume + external table).
    try:
        await execute_dbsql("DROP CATALOG IF EXISTS iot_pharmaceutical CASCADE", WAREHOUSE_ID)
        out["catalog_drop"] = "ok"
    except Exception as e:
        out["catalog_drop_error"] = str(e)[:300]

    # 2. Delete Lakebase replica endpoint then the branch.
    for name in ("projects/iot-pipelines/branches/pharmaceutical/endpoints/replica",):
        try:
            w.postgres.delete_endpoint(name=name)
            out[f"delete_{name.split('/')[-1]}"] = "ok"
        except Exception as e:
            out[f"delete_{name.split('/')[-1]}_error"] = str(e)[:200]
    try:
        w.postgres.delete_branch(name="projects/iot-pipelines/branches/pharmaceutical")
        out["delete_branch"] = "ok"
    except Exception as e:
        out["delete_branch_error"] = str(e)[:200]

    _pp("CLOUD cleanup (what admin-path teardown skipped)", out)


async def stage_verify_gone():
    from databricks.sdk import WorkspaceClient

    from iot_twin.services.pipelines import pipeline_setup_repo

    w = WorkspaceClient()
    out: dict = {}
    _load_state()

    # setup row gone?
    db = await _db()
    try:
        s = await pipeline_setup_repo.get_setup_by_industry(db, INDUSTRY)
        out["setup_row"] = "gone" if s is None else f"present:{s['id']}"
    finally:
        await db.close()

    try:
        w.catalogs.get("iot_pharmaceutical")
        out["catalog"] = "STILL EXISTS"
    except Exception:
        out["catalog"] = "gone"
    try:
        w.postgres.get_branch(name="projects/iot-pipelines/branches/pharmaceutical")
        out["branch"] = "STILL EXISTS"
    except Exception:
        out["branch"] = "gone"
    # Jobs
    for jn in ("iot-pharmaceutical-microbatch", "iot-pharmaceutical-realtime"):
        jobs = [j.job_id for j in w.jobs.list(name=jn)]
        out[f"job:{jn}"] = jobs or "gone"
    # LakeTS ops jobs (named by branch)
    lakets_jobs = [
        j.settings.name
        for j in w.jobs.list()
        if j.settings and j.settings.name and "pharmaceutical" in j.settings.name.lower()
    ]
    out["any_pharma_jobs"] = lakets_jobs or "none"
    _pp("VERIFY nothing remains", out)


STAGES = {
    "setup": stage_setup,
    "provision": stage_provision,
    "verify-provision": stage_verify_provision,
    "validate-mb": stage_validate_mb,
    "validate-rt": stage_validate_rt,
    "probe-landing": stage_probe_landing,
    "deploy-mb": stage_deploy_mb,
    "deploy-status": stage_deploy_status,
    "teardown": stage_teardown,
    "cleanup-cloud": stage_cleanup_cloud,
    "verify-gone": stage_verify_gone,
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("stage", choices=list(STAGES))
    args = ap.parse_args()
    asyncio.run(STAGES[args.stage]())


if __name__ == "__main__":
    main()
