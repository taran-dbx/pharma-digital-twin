"""App SP secret rotation must not stall the realtime pipeline.

Databricks Apps rotate the app SP's OAuth secret and restart the app. The app
republishes the secret to the UC secret on startup, the realtime consumer waits
for that copy and restarts its stream, and the producer ends the run when its
consumer sibling dies so the continuous job restarts it.

The notebook Python is rendered from the TS generators with esbuild + node.
"""

from __future__ import annotations

import asyncio
import json
import logging
import shutil
import subprocess
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

import iot_twin.app as app_module
import iot_twin.services.deployment.deployment_service as ds
import iot_twin.services.industry.industry_service as isvc
from tests._fakes import fake_credential

ROOT = Path(__file__).resolve().parent.parent
CLIENT = ROOT / "client"
NOTEBOOK_TS = CLIENT / "src" / "lib" / "notebook-template.ts"
PRODUCER_TS = CLIENT / "src" / "lib" / "data-producer-notebook.ts"
ESBUILD = CLIENT / "node_modules" / ".bin" / "esbuild"

needs_node = pytest.mark.skipif(
    shutil.which("node") is None or not ESBUILD.exists() or not NOTEBOOK_TS.exists(),
    reason="node, client esbuild, or the notebook templates are unavailable",
)

AUTH_FAILURE = (
    "[STREAM_FAILED] Query terminated with exception: requests.exceptions.HTTPError: "
    "401 Client Error: Unauthorized for url: https://ws.cloud.databricks.com/oidc/v1/token"
)

CONSUMER_CONFIG = {
    "industry": "manufacturing",
    "mode": "realtime",
    "topic": "iot.manufacturing.realtime",
    "bootstrapServers": "b-1.example:9198",
    "checkpointLocation": "/Volumes/cat/sch/stream_checkpoints/manufacturing/realtime",
    "lakebaseProjectId": "iot-pipelines",
    "lakebaseBranch": "manufacturing",
    "metricsTableFqn": "cat.sch.manufacturing_metrics",
}


def _render(ts_file: Path, expression: str) -> str:
    bundle = subprocess.run(
        [
            str(ESBUILD),
            str(ts_file),
            "--bundle",
            "--format=cjs",
            "--platform=node",
            f"--tsconfig={CLIENT / 'tsconfig.json'}",
        ],
        check=True,
        capture_output=True,
        text=True,
        cwd=CLIENT,
    ).stdout
    script = (
        f"{bundle}\nconst m = module.exports;process.stdout.write(JSON.stringify({expression}));"
    )
    return json.loads(
        subprocess.run(["node", "-e", script], check=True, capture_output=True, text=True).stdout
    )


def _consumer_notebook(monkeypatch: pytest.MonkeyPatch) -> str:
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "app-sp-id")
    monkeypatch.setenv("UC_CATALOG", "cat")
    monkeypatch.setenv("UC_SCHEMA", "sch")
    source = _render(
        NOTEBOOK_TS, f"m.generateRealtimeConsumerNotebook({json.dumps(CONSUMER_CONFIG)})"
    )
    return ds._normalize_consumer_credentials(source)


def _between(text: str, start: str, end: str) -> str:
    begin = text.index(start)
    return text[begin : text.index(end, begin)]


class _FakeResponse:
    def __init__(self, status_code: int):
        self.status_code = status_code


def _recovery_namespace(notebook: str, secrets: list[str], accepted: set[str]) -> dict:
    """Execute the consumer's recovery helpers with fake secret reads and token endpoint."""
    reads = iter(secrets)

    def post(_url, data, timeout):
        return _FakeResponse(200 if data["client_secret"] in accepted else 401)

    ns: dict = {
        "_read_app_sp_secret": lambda: next(reads),
        "_lb_req": SimpleNamespace(post=post),
        "_lb_time": SimpleNamespace(monotonic=time.monotonic, sleep=lambda _s: None),
        "_lb_ci": "app-sp-id",
        "_lb_ws_url": "https://ws.cloud.databricks.com",
    }
    block = _between(notebook, "AUTH_RECOVERY_POLL_S = ", "def _start_query")
    exec(compile(block, "<consumer-recovery>", "exec"), ns)
    return ns


def _python_cells(notebook: str) -> list[str]:
    cells = notebook.split("# COMMAND ----------")
    return [
        "\n".join(line for line in cell.splitlines() if not line.lstrip().startswith("%"))
        for cell in cells
    ]


@needs_node
class TestConsumerRecovery:
    def test_consumer_waits_for_a_republished_secret_and_restarts_the_stream_C7445_defect_regression(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7445][defect][regression] A rejected secret is polled until the app republishes one."""
        notebook = _consumer_notebook(monkeypatch)
        ns = _recovery_namespace(notebook, ["stale", "stale", "fresh"], accepted={"fresh"})

        assert ns["_await_accepted_sp_secret"]() == "fresh"
        loop = notebook[notebook.index("while True:\n    query = _start_query()") :]
        assert "_lb_cs_bc = spark.sparkContext.broadcast(_lb_cs)" in loop
        assert "_sse_cs_bc = _lb_cs_bc" in loop
        assert "_zb_metrics_stream = _open_metrics_stream(_lb_cs)" in loop

    def test_only_token_rejections_trigger_recovery_C7446_regression(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7446][regression] Other stream failures still fail the task."""
        ns = _recovery_namespace(_consumer_notebook(monkeypatch), [], accepted=set())

        assert ns["_is_sp_auth_failure"](RuntimeError(AUTH_FAILURE))
        assert not ns["_is_sp_auth_failure"](
            RuntimeError(
                'psycopg2.errors.CheckViolation: no partition of relation "sensor_readings"'
            )
        )
        assert not ns["_is_sp_auth_failure"](
            RuntimeError("401 Client Error for url: https://app/api/sss/ingest")
        )

    def test_recovery_gives_up_so_the_job_can_restart_C7447_regression(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7447][regression] A secret that never refreshes fails the task instead of waiting forever."""
        ns = _recovery_namespace(_consumer_notebook(monkeypatch), ["stale"] * 10, accepted=set())
        ns["AUTH_RECOVERY_MAX_WAIT_S"] = 0

        with pytest.raises(RuntimeError, match="not republished"):
            ns["_await_accepted_sp_secret"]()

    def test_recovery_rereads_the_uc_secret_the_deploy_injects_C7448_regression(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7448][regression] The re-read goes to the same UC secret the app republishes."""
        notebook = _consumer_notebook(monkeypatch)
        reader = _between(notebook, "def _read_app_sp_secret():", "_lb_cs = _read_app_sp_secret()")

        assert (
            'catalog="cat"' in reader
            and 'schema="sch"' in reader
            and 'key="app_sp_oauth"' in reader
        )
        for i, cell in enumerate(_python_cells(notebook)):
            compile(cell, f"<consumer cell {i}>", "exec")


class _FakeTaskState:
    def __init__(self, life_cycle: str):
        self.life_cycle_state = SimpleNamespace(value=life_cycle)


def _peer_namespace(parameters: dict[str, str]) -> dict:
    fragment = _render(PRODUCER_TS, "m.PEER_TASK_WATCH_PYTHON")
    started: list[tuple] = []

    class _RecordingThreads:
        Event = threading.Event

        @staticmethod
        def Thread(*, target, args, daemon):
            return SimpleNamespace(start=lambda: started.append(args))

    def widget(name: str) -> str:
        if name not in parameters:
            raise ValueError(f"No input widget named {name} is defined")
        return parameters[name]

    ns: dict = {
        "threading": _RecordingThreads,
        "_thread": SimpleNamespace(interrupt_main=lambda: None),
        "time": time,
        "logger": logging.getLogger("producer.peer"),
        "dbutils": SimpleNamespace(widgets=SimpleNamespace(get=widget)),
    }
    exec(compile(fragment, "<peer-watch>", "exec"), ns)
    ns["_started_watches"] = started
    return ns


@needs_node
class TestProducerPeerWatch:
    def test_producer_stops_when_the_consumer_task_has_ended_C7449_defect_regression(self) -> None:
        """[C7449][defect][regression] A dead consumer sibling ends the producer so the run restarts."""
        ns = _peer_namespace(ds.PRODUCER_RUN_PARAMETERS | {"job_run_id": "1", "task_run_id": "2"})
        assert ns["_started_watches"] == [("1", "2")]
        tasks = [
            SimpleNamespace(task_key="data_producer", run_id=2, state=_FakeTaskState("RUNNING")),
            SimpleNamespace(task_key="rtm_consumer", run_id=3, state=_FakeTaskState("TERMINATED")),
        ]
        client = SimpleNamespace(
            jobs=SimpleNamespace(get_run=lambda _rid: SimpleNamespace(tasks=tasks))
        )

        assert ns["_ended_peer_tasks"](client, "1", "2") == ["rtm_consumer"]
        tasks[1].state = _FakeTaskState("PENDING")
        assert ns["_ended_peer_tasks"](client, "1", "2") == []
        assert _peer_namespace({})["_started_watches"] == [], "no run ids, no watch"

    def test_producer_task_receives_its_run_ids_and_both_variants_watch_C7450_regression(
        self,
    ) -> None:
        """[C7450][regression] The deploy passes run ids to the producer; both Kafka variants watch."""
        tasks = ds.build_spark_tasks(
            mode="realtime",
            consumer_notebook_path="/c",
            producer_notebook_path="/p",
            node_type="m7gd.xlarge",
            num_workers=1,
            single_user_name="sp",
        )
        producer = next(t for t in tasks if t.task_key == "data_producer")
        assert producer.notebook_task.base_parameters == {
            "job_run_id": "{{job.run_id}}",
            "task_run_id": "{{task.run_id}}",
        }
        source = PRODUCER_TS.read_text()
        assert source.count("${PEER_TASK_WATCH_PYTHON}") == 2
        assert source.count("${PEER_TASK_ENDED_RAISE}") == 2


class TestAppPublishesSecret:
    def test_app_startup_republishes_the_rotated_secret_C7451_defect_regression(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """[C7451][defect][regression] Startup copies the app's current secret into the UC secret."""
        puts: list[tuple] = []

        async def fake_put(catalog, schema, name, value, *, ws=None):
            puts.append((catalog, schema, name, value))
            return {"status": "updated"}

        monkeypatch.setattr(ds, "_new_ws", lambda: object())
        monkeypatch.setattr(ds.uc_secrets, "put_secret", fake_put)
        for key, value in {
            "DATABRICKS_CLIENT_ID": "app-sp-id",
            "DATABRICKS_CLIENT_SECRET": fake_credential("rotated"),
            "UC_CATALOG": "cat",
            "UC_SCHEMA": "sch",
        }.items():
            monkeypatch.setenv(key, value)

        asyncio.run(app_module.publish_runtime_oauth_secret())
        assert puts == [("cat", "sch", "app_sp_oauth", fake_credential("rotated"))]

        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET")
        with caplog.at_level(logging.INFO, logger="iot_digital_twin"):
            asyncio.run(app_module.publish_runtime_oauth_secret())
        assert len(puts) == 1
        assert "Skipping UC secret refresh" in caplog.text

    def test_dashboard_start_publishes_the_secret_before_unpausing_C7452_defect_regression(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7452][defect][regression] Starting from the dashboard never resumes a job on a stale secret."""
        calls: list[str] = []

        async def fake_sync(_w=None):
            calls.append("sync")
            return {"status": "updated"}

        async def fake_job_id(_state, _pipeline_id):
            return "798"

        workspace = SimpleNamespace(
            jobs=SimpleNamespace(update=lambda **_k: calls.append("unpause"))
        )
        monkeypatch.setattr(isvc, "_get_job_id", fake_job_id)
        monkeypatch.setattr(isvc.deployment_service, "sync_runtime_oauth_secret", fake_sync)
        state = SimpleNamespace(workspace_client=workspace)

        out = asyncio.run(isvc._deploy_and_start(state, "manufacturing", "realtime", "pid"))
        assert out["status"] == "running"
        assert calls == ["sync", "unpause"]

        async def failing_sync(_w=None):
            raise ds.RuntimeOAuthCredentialError("missing runtime OAuth configuration")

        calls.clear()
        monkeypatch.setattr(isvc.deployment_service, "sync_runtime_oauth_secret", failing_sync)
        out = asyncio.run(isvc._deploy_and_start(state, "manufacturing", "realtime", "pid"))
        assert out["status"] == "starting"
        assert "missing runtime OAuth configuration" in out["error"]
        assert calls == []
