import asyncio

import pytest

import iot_twin.services.industry.industry_service as isvc

UC_CATALOG = "test_catalog"
UC_SCHEMA = "test_schema"
PIPELINES_LAKEBASE_PROJECT = "test_pipelines"


@pytest.fixture(autouse=True)
def _one_schema_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin one-schema UC + pipelines project env for deterministic helpers."""
    monkeypatch.setenv("UC_CATALOG", UC_CATALOG)
    monkeypatch.setenv("UC_SCHEMA", UC_SCHEMA)
    monkeypatch.setenv("PIPELINES_LAKEBASE_PROJECT", PIPELINES_LAKEBASE_PROJECT)


@pytest.fixture(autouse=True)
def _event_loop() -> None:
    """Sync tests call ``get_event_loop().run_until_complete`` — ensure a loop exists."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield
    loop.close()
    asyncio.set_event_loop(None)


def _run(c):
    return asyncio.get_event_loop().run_until_complete(c)


# --- The two tests specified verbatim in the task brief -------------------


def test_start_zerobus_skips_lakebase(monkeypatch):
    calls = {"branch": 0, "sp": 0}
    monkeypatch.setattr(
        isvc,
        "_resolve_app_sp",
        lambda s: calls.__setitem__("sp", calls["sp"] + 1) or {"client_id": "x"},
    )
    monkeypatch.setattr(
        isvc,
        "_provision_lakebase_branch",
        lambda *a, **k: calls.__setitem__("branch", calls["branch"] + 1),
    )
    monkeypatch.setattr(isvc, "_provision_catalog_schema", lambda *a, **k: None)
    monkeypatch.setattr(isvc, "_deploy_and_start", lambda *a, **k: {"status": "starting"})
    out = _run(isvc.start_mode(None, "automotive", "zerobus"))
    assert out["pipeline"] == "iot-automotive-zerobus"
    assert calls["branch"] == 0  # no Lakebase for zerobus
    assert calls["sp"] == 1


def test_start_realtime_creates_branch(monkeypatch):
    calls = {"branch": 0}
    monkeypatch.setattr(isvc, "_resolve_app_sp", lambda s: {"client_id": "x"})
    monkeypatch.setattr(
        isvc,
        "_provision_lakebase_branch",
        lambda *a, **k: calls.__setitem__("branch", calls["branch"] + 1),
    )
    monkeypatch.setattr(isvc, "_provision_catalog_schema", lambda *a, **k: None)
    monkeypatch.setattr(isvc, "_deploy_and_start", lambda *a, **k: {"status": "starting"})
    _run(isvc.start_mode(None, "manufacturing", "realtime"))
    assert calls["branch"] == 1


# --- Additional coverage ---------------------------------------------------


def test_start_mode_includes_topic_step_for_streaming_modes(monkeypatch):
    monkeypatch.setattr(isvc, "_resolve_app_sp", lambda s: {"client_id": "x"})
    monkeypatch.setattr(isvc, "_provision_catalog_schema", lambda *a, **k: None)
    monkeypatch.setattr(isvc, "_provision_lakebase_branch", lambda *a, **k: None)
    monkeypatch.setattr(isvc, "_deploy_and_start", lambda *a, **k: {"status": "starting"})

    out = _run(isvc.start_mode(None, "energy", "microbatch"))

    topic_steps = [s for s in out["steps"] if s.get("step") == "topic"]
    assert topic_steps == [{"step": "topic", "topic": "iot.energy.microbatch"}]
    # microbatch has uses_lakebase=False -> no lakebase_branch step recorded
    assert not any(s.get("step") == "lakebase_branch" for s in out["steps"])


def test_start_mode_writes_running_status_when_job_started(monkeypatch):
    ensure_calls = []
    set_calls = []

    async def fake_ensure(state, name, industry, status):
        ensure_calls.append(status)
        return "pid-1"

    async def fake_set(state, name, status):
        set_calls.append(status)

    monkeypatch.setattr(isvc, "_ensure_pipeline_row", fake_ensure)
    monkeypatch.setattr(isvc, "_set_pipeline_status", fake_set)
    monkeypatch.setattr(isvc, "_resolve_app_sp", lambda s: {"client_id": "x"})
    monkeypatch.setattr(isvc, "_provision_catalog_schema", lambda *a, **k: None)
    monkeypatch.setattr(
        isvc, "_deploy_and_start", lambda *a, **k: {"status": "running", "job_id": "42"}
    )

    out = _run(isvc.start_mode(None, "automotive", "zerobus"))

    assert ensure_calls == ["starting"]
    assert set_calls == ["running"]
    assert out["status"] == "running"


def test_start_mode_settles_to_idle_when_job_not_started(monkeypatch):
    # start_mode is synchronous: by the time it returns, the mode is either
    # running or it isn't. When the deploy step couldn't bring up a job
    # (nothing deployed yet, or the unpause/run failed), the row must be
    # settled to the stopped status — NEVER stranded at "starting", which would
    # render a perpetual "provisioning…" spinner on the card that never resolves.
    set_calls = []

    async def fake_ensure(*a, **k):
        return None

    async def fake_set(state, name, status):
        set_calls.append(status)

    monkeypatch.setattr(isvc, "_ensure_pipeline_row", fake_ensure)
    monkeypatch.setattr(isvc, "_set_pipeline_status", fake_set)
    monkeypatch.setattr(isvc, "_resolve_app_sp", lambda s: {"client_id": "x"})
    monkeypatch.setattr(isvc, "_provision_catalog_schema", lambda *a, **k: None)
    monkeypatch.setattr(isvc, "_deploy_and_start", lambda *a, **k: {"status": "starting"})

    out = _run(isvc.start_mode(None, "automotive", "zerobus"))

    assert set_calls == [isvc._STOPPED_STATUS]
    assert out["status"] == "idle"


def test_stop_mode_sets_stopped_status_and_reports_idle(monkeypatch):
    set_calls = []

    async def fake_lookup(state, name):
        return None, None

    async def fake_set(state, name, status):
        set_calls.append(status)

    monkeypatch.setattr(isvc, "_get_pipeline_id_and_job", fake_lookup)
    monkeypatch.setattr(isvc, "_set_pipeline_status", fake_set)

    out = _run(isvc.stop_mode(None, "manufacturing", "realtime"))

    assert set_calls == [isvc._STOPPED_STATUS]
    assert out["status"] == "idle"
    assert out["pipeline"] == "iot-manufacturing-realtime"
    assert out["steps"][0]["result"]["status"] == "skipped"


def test_stop_mode_stops_existing_job(monkeypatch):
    stop_calls = []

    async def fake_lookup(state, name):
        return "pid-1", "42"

    def fake_stop_job(job_id, state=None):
        stop_calls.append(job_id)
        return {"status": "stopped", "job_id": job_id}

    monkeypatch.setattr(isvc, "_get_pipeline_id_and_job", fake_lookup)
    monkeypatch.setattr(isvc, "_stop_job", fake_stop_job)
    monkeypatch.setattr(isvc, "_set_pipeline_status", lambda *a, **k: _noop())

    out = _run(isvc.stop_mode(None, "energy", "microbatch"))

    assert stop_calls == ["42"]
    assert out["steps"][0]["result"] == {"status": "stopped", "job_id": "42"}


async def _noop():
    return None


# --- Real (unmocked) behaviour of the thin seams themselves ---------------


def test_deploy_and_start_zerobus_without_job_returns_starting():
    # Zerobus has no continuous Spark job, but it DOES have a serverless
    # producer job that only the deploy flow can create. With no job deployed
    # yet, start must report "starting" truthfully — never a fabricated
    # "running" (the old behaviour left the mode stuck showing "not deployed").
    out = _run(isvc._deploy_and_start(None, "automotive", "zerobus", None))
    assert out["status"] == "starting"
    assert "note" in out


def test_deploy_and_start_zerobus_with_job_triggers_run(monkeypatch):
    # When a zerobus job exists, start triggers a run (serverless run-once
    # producer → run_now) so data actually lands, and reports it as running.
    class _FakeRun:
        run_id = 777

    class _FakeJobs:
        def __init__(self):
            self.ran = None

        def run_now(self, job_id):
            self.ran = job_id
            return _FakeRun()

    class _FakeWorkspace:
        def __init__(self):
            self.jobs = _FakeJobs()

    async def fake_get_job_id(_state, _pipeline_id):
        return "555"

    async def fake_sync(_w=None):
        return {"status": "updated"}

    fake_w = _FakeWorkspace()
    monkeypatch.setattr(isvc, "_get_job_id", fake_get_job_id)
    monkeypatch.setattr(isvc.deployment_service, "sync_runtime_oauth_secret", fake_sync)

    class _State:
        workspace_client = fake_w

    out = _run(isvc._deploy_and_start(_State(), "automotive", "zerobus", "pid"))
    assert out["status"] == "running"
    assert out["job_id"] == "555"
    assert out["run_id"] == 777
    assert fake_w.jobs.ran == 555


def test_deploy_and_start_realtime_without_job_returns_starting():
    out = _run(isvc._deploy_and_start(None, "manufacturing", "realtime", None))
    assert out["status"] == "starting"
    assert "note" in out


def test_provision_catalog_schema_calls_provision_uc_catalog(monkeypatch):
    calls = {}

    async def fake_provision_uc_catalog(uc):
        calls["uc"] = uc
        return [{"resource": "ok", "status": "created"}]

    monkeypatch.setattr(
        "iot_twin.services.provisioning_service.provision_uc_catalog", fake_provision_uc_catalog
    )

    result = _run(isvc._provision_catalog_schema("pharmaceutical", "realtime"))

    assert calls["uc"] == {"catalog_pattern": "iot_pharmaceutical", "schemas": ["realtime"]}
    assert result == [{"resource": "ok", "status": "created"}]


def test_provision_lakebase_branch_calls_setup_pipeline_lakebase(monkeypatch):
    calls = {}

    async def fake_setup(state, params):
        calls["params"] = params
        return {"success": True, "steps": []}

    monkeypatch.setattr(
        "iot_twin.services.pipelines.pipeline_setup_service.setup_pipeline_lakebase", fake_setup
    )

    result = _run(isvc._provision_lakebase_branch(None, "energy", "realtime", "pid-1"))

    assert calls["params"]["lb_project_id"] == "iot-energy-realtime"
    assert calls["params"]["create_new_project"] is True
    assert calls["params"]["pipeline_id"] == "pid-1"
    assert result == {"success": True, "steps": []}
