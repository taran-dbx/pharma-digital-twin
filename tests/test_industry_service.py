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


def test_naming_helpers():
    assert isvc.pipeline_name("energy", "microbatch") == "iot-energy-microbatch"
    assert isvc.catalog_name("energy") == "iot_energy"
    assert isvc.topic_name("energy", "microbatch") == "iot.energy.microbatch"


def test_get_catalog_marks_running(monkeypatch):
    async def fake_list(_state):
        return [{"name": "iot-pharmaceutical-realtime", "status": "running"}]

    monkeypatch.setattr(isvc, "_list_pipelines", fake_list)
    cat = asyncio.get_event_loop().run_until_complete(isvc.get_catalog(None))
    pharma = next(c for c in cat if c["id"] == "pharmaceutical")
    rt = next(m for m in pharma["modes"] if m["id"] == "realtime")
    assert rt["status"] == "running"
    assert pharma["running_count"] == 1
    energy = next(c for c in cat if c["id"] == "energy")
    assert all(m["status"] == "idle" for m in energy["modes"])


def test_infer_mode_zerobus():
    row = {"source_type": "zerobus", "source_config": {}}
    assert isvc._infer_mode(row) == "zerobus"


def test_infer_mode_sss_lakebase_sink_is_realtime():
    row = {"source_type": "sss", "source_config": {"rtm_sink": "lakebase"}}
    assert isvc._infer_mode(row) == "realtime"


def test_infer_mode_sss_delta_sink_is_microbatch():
    row = {"source_type": "sss", "source_config": {"rtm_sink": "delta"}}
    assert isvc._infer_mode(row) == "microbatch"


def test_infer_mode_unknown_source_type_is_none():
    row = {"source_type": "dbsql", "source_config": {}}
    assert isvc._infer_mode(row) is None


def test_get_catalog_reconciles_legacy_pharma_demo_pipeline(monkeypatch):
    """The real live-workspace row: named "Demo", not the deterministic name."""

    async def fake_list(_state):
        return [
            {
                "id": "11111111-1111-1111-1111-111111111111",
                "name": "Demo",
                "status": "step2_complete",
                "industry": "pharmaceutical",
                "source_type": "sss",
                "source_config": {"source": "kafka", "rtm_sink": "lakebase"},
            }
        ]

    monkeypatch.setattr(isvc, "_list_pipelines", fake_list)
    cat = asyncio.get_event_loop().run_until_complete(isvc.get_catalog(None))
    pharma = next(c for c in cat if c["id"] == "pharmaceutical")
    rt = next(m for m in pharma["modes"] if m["id"] == "realtime")
    assert rt["status"] == "running"
    assert pharma["running_count"] >= 1
    # The reconciled row's id should be threaded through so admins can
    # deep-link to it (e.g. from an "Edit configuration" action).
    assert rt["pipeline_id"] == "11111111-1111-1111-1111-111111111111"
    # Modes with no matched pipeline row at all report pipeline_id: None.
    mb = next(m for m in pharma["modes"] if m["id"] == "microbatch")
    assert mb["pipeline_id"] is None


def test_get_catalog_reconciles_legacy_zerobus_pipeline(monkeypatch):
    async def fake_list(_state):
        return [
            {
                "name": "Auto Ingest",
                "status": "step2_complete",
                "industry": "automotive",
                "source_type": "zerobus",
                "source_config": {},
            }
        ]

    monkeypatch.setattr(isvc, "_list_pipelines", fake_list)
    cat = asyncio.get_event_loop().run_until_complete(isvc.get_catalog(None))
    auto = next(c for c in cat if c["id"] == "automotive")
    zb = next(m for m in auto["modes"] if m["id"] == "zerobus")
    assert zb["status"] == "running"


def test_get_catalog_draft_row_does_not_reconcile(monkeypatch):
    async def fake_list(_state):
        return [
            {
                "name": "Some Draft",
                "status": "draft",
                "industry": "pharmaceutical",
                "source_type": "sss",
                "source_config": {"rtm_sink": "lakebase"},
            }
        ]

    monkeypatch.setattr(isvc, "_list_pipelines", fake_list)
    cat = asyncio.get_event_loop().run_until_complete(isvc.get_catalog(None))
    pharma = next(c for c in cat if c["id"] == "pharmaceutical")
    assert all(m["status"] == "idle" for m in pharma["modes"])
    assert pharma["running_count"] == 0


def test_get_catalog_exact_name_match_still_wins(monkeypatch):
    async def fake_list(_state):
        return [{"name": "iot-energy-microbatch", "status": "running"}]

    monkeypatch.setattr(isvc, "_list_pipelines", fake_list)
    cat = asyncio.get_event_loop().run_until_complete(isvc.get_catalog(None))
    energy = next(c for c in cat if c["id"] == "energy")
    mb = next(m for m in energy["modes"] if m["id"] == "microbatch")
    assert mb["status"] == "running"
    assert energy["running_count"] == 1


def test_get_catalog_resolves_industry_first_deployment_id(monkeypatch):
    """[C7107][defect][regression] Admin deploys must expose a dashboard id."""

    class FakeBackendDb:
        async def fetch(self, query):
            assert "pipeline_deployment" in query
            return [
                {
                    "deployment_id": "22222222-2222-2222-2222-222222222222",
                    "industry": "pharmaceutical",
                    "mode": "zerobus",
                }
            ]

    class FakeState:
        app_backend_db = FakeBackendDb()

    async def fake_list(_state):
        return []

    monkeypatch.setattr(isvc, "_list_pipelines", fake_list)
    cat = asyncio.get_event_loop().run_until_complete(isvc.get_catalog(FakeState()))
    pharma = next(c for c in cat if c["id"] == "pharmaceutical")
    zerobus = next(m for m in pharma["modes"] if m["id"] == "zerobus")

    assert zerobus["pipeline_id"] is None
    assert zerobus["deployment_id"] == "22222222-2222-2222-2222-222222222222"


# ---------------------------------------------------------------------------
# resolve_pipeline_for_mode — the shared matching helper get_catalog and the
# stopped-only config endpoint (routes/industries.py) both use.
# ---------------------------------------------------------------------------
def test_resolve_pipeline_for_mode_exact_name_match(monkeypatch):
    async def fake_list(_state):
        return [{"id": "pid-1", "name": "iot-energy-microbatch", "status": "running"}]

    monkeypatch.setattr(isvc, "_list_pipelines", fake_list)
    row = asyncio.get_event_loop().run_until_complete(
        isvc.resolve_pipeline_for_mode(None, "energy", "microbatch")
    )
    assert row is not None
    assert row["id"] == "pid-1"


def test_resolve_pipeline_for_mode_reconciled_legacy_row(monkeypatch):
    async def fake_list(_state):
        return [
            {
                "id": "pid-2",
                "name": "Demo",
                "status": "step2_complete",
                "industry": "pharmaceutical",
                "source_type": "sss",
                "source_config": {"rtm_sink": "lakebase"},
            }
        ]

    monkeypatch.setattr(isvc, "_list_pipelines", fake_list)
    row = asyncio.get_event_loop().run_until_complete(
        isvc.resolve_pipeline_for_mode(None, "pharmaceutical", "realtime")
    )
    assert row is not None
    assert row["id"] == "pid-2"


def test_resolve_pipeline_for_mode_no_match_returns_none(monkeypatch):
    async def fake_list(_state):
        return []

    monkeypatch.setattr(isvc, "_list_pipelines", fake_list)
    row = asyncio.get_event_loop().run_until_complete(
        isvc.resolve_pipeline_for_mode(None, "energy", "microbatch")
    )
    assert row is None


# ---------------------------------------------------------------------------
# update_pipeline_config — merges a patch into a pipeline row's source_config
# JSONB by id, reusing the same COALESCE(...) || $n::jsonb merge technique
# routes/pipelines.py::update_pipeline already uses.
# ---------------------------------------------------------------------------
def test_update_pipeline_config_merges_and_returns_source_config():
    class FakeDb:
        async def fetchrow(self, query, *args):
            assert "source_config" in query
            assert "COALESCE" in query
            return {"source_config": {"rate_hz": 5, "capture_metrics": True}}

    class FakeState:
        db_available = True
        db = FakeDb()

    merged = asyncio.get_event_loop().run_until_complete(
        isvc.update_pipeline_config(FakeState(), "pid-1", {"rate_hz": 5, "capture_metrics": True})
    )
    assert merged == {"rate_hz": 5, "capture_metrics": True}


def test_update_pipeline_config_no_db_returns_empty_dict():
    class FakeState:
        db_available = False
        db = None

    merged = asyncio.get_event_loop().run_until_complete(
        isvc.update_pipeline_config(FakeState(), "pid-1", {"rate_hz": 5})
    )
    assert merged == {}


def test_get_catalog_exposes_industry_first_setup_id(monkeypatch):
    """Genie per-pipeline routes need pipeline_setup.id next to deployment_id."""

    class FakeBackendDb:
        async def fetch(self, query):
            assert "ps.id AS setup_id" in query
            return [
                {
                    "deployment_id": "22222222-2222-2222-2222-222222222222",
                    "setup_id": "11111111-1111-1111-1111-111111111111",
                    "industry": "manufacturing",
                    "mode": "realtime",
                }
            ]

    class FakeState:
        app_backend_db = FakeBackendDb()

    async def fake_list(_state):
        return []

    monkeypatch.setattr(isvc, "_list_pipelines", fake_list)
    cat = asyncio.get_event_loop().run_until_complete(isvc.get_catalog(FakeState()))
    mfg = next(c for c in cat if c["id"] == "manufacturing")
    realtime = next(m for m in mfg["modes"] if m["id"] == "realtime")
    idle = next(m for m in mfg["modes"] if m["id"] == "zerobus")

    assert realtime["setup_id"] == "11111111-1111-1111-1111-111111111111"
    assert realtime["deployment_id"] == "22222222-2222-2222-2222-222222222222"
    assert idle["setup_id"] is None and idle["deployment_id"] is None
