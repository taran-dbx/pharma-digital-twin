"""Round-3 follow-ups: shielded create+record, dashboard-matching CDC filter,
CDC-aware snippets/telemetry, shared pipeline id resolution, cache refresh."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from iot_twin.services import genie_service as gs
from iot_twin.services.genie import genie_space_content as content
from iot_twin.services.genie import genie_space_service as gss

CDC = "_pg_change_type IN ('insert', 'update_postimage')"
SHADOW_SHORT = "lb__shadow_manufacturing_sensor_readings_history"
S1 = "11111111-1111-4111-8111-111111111111"
D1 = "33333333-3333-4333-8333-333333333333"


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UC_CATALOG", "cat")
    monkeypatch.setenv("UC_SCHEMA", "sch")
    monkeypatch.delenv("DATABRICKS_SQL_WAREHOUSE_ID", raising=False)


def _space(mode: str) -> dict:
    return json.loads(gss.build_serialized_space("manufacturing", mode))


# ---------------------------------------------------------------------------
# N1 shield
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_ledger_written_even_if_outer_timeout_fires_mid_create() -> None:
    db = MagicMock()
    db.fetch = AsyncMock(return_value=[])
    recorded: list[tuple] = []

    async def _fetchrow(query: str, *args):
        if "insert into provisioned_resource" in query.lower():
            recorded.append(args)
        return {"id": "r"}

    db.fetchrow = AsyncMock(side_effect=_fetchrow)
    w = MagicMock()
    w.tables.exists.return_value = SimpleNamespace(table_exists=True)

    def _slow_create(**_kwargs):
        import time

        time.sleep(0.3)
        return SimpleNamespace(space_id="sp-late")

    w.genie.create_space.side_effect = _slow_create
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(
            gss.ensure_pipeline_space(
                db,
                setup_id="s",
                deployment_id="d",
                industry="manufacturing",
                mode="zerobus",
                warehouse_id="wh",
                w=w,
            ),
            timeout=0.05,
        )
    for _ in range(40):
        if recorded:
            break
        await asyncio.sleep(0.05)
    assert recorded and "sp-late" in recorded[0]


# ---------------------------------------------------------------------------
# N2 dashboard-matching realtime filter
# ---------------------------------------------------------------------------
class TestRealtimeFilter:
    def test_instructions_use_dashboard_filter_and_industry(self) -> None:
        body = "".join(_space("realtime")["instructions"]["text_instructions"][0]["content"])
        assert CDC in body
        assert f"{SHADOW_SHORT}.industry = 'manufacturing'" in body
        assert "_pg_change_type = 'insert'" not in body

    def test_examples_use_dashboard_filter_and_industry(self) -> None:
        examples = _space("realtime")["instructions"]["example_question_sqls"]
        tel = [e["sql"][0] for e in examples if SHADOW_SHORT in e["sql"][0]]
        assert len(tel) >= 4
        for sql in tel:
            assert CDC in sql
            assert "industry = 'manufacturing'" in sql

    def test_non_realtime_examples_unfiltered(self) -> None:
        sql = " ".join(
            e["sql"][0] for e in _space("microbatch")["instructions"]["example_question_sqls"]
        )
        assert "_pg_change_type" not in sql

    def test_row_filter_helper(self) -> None:
        assert content.telemetry_row_filter("realtime", "manufacturing") == (
            f"{CDC} AND industry = 'manufacturing'"
        )
        assert content.telemetry_row_filter("zerobus", "manufacturing") is None
        assert "o''x" in content.telemetry_row_filter("realtime", "o'x")


# ---------------------------------------------------------------------------
# N3 snippets apply the filter
# ---------------------------------------------------------------------------
class TestSnippetsCdc:
    def test_realtime_measures_and_filters_apply_cdc(self) -> None:
        snippets = _space("realtime")["instructions"]["sql_snippets"]
        q = f"{SHADOW_SHORT}._pg_change_type IN ('insert', 'update_postimage')"
        for key in ("measures", "filters"):
            for item in snippets[key]:
                assert q in item["sql"][0], (key, item.get("display_name"))
                assert f"{SHADOW_SHORT}.industry = 'manufacturing'" in item["sql"][0]

    def test_zerobus_snippets_have_no_cdc(self) -> None:
        snippets = _space("zerobus")["instructions"]["sql_snippets"]
        blob = json.dumps(snippets)
        assert "_pg_change_type" not in blob


# ---------------------------------------------------------------------------
# N4 pipeline telemetry summary
# ---------------------------------------------------------------------------
class TestPipelineTelemetry:
    @pytest.mark.asyncio
    async def test_realtime_pipeline_scope_reads_shadow_with_filter(self) -> None:
        runner = AsyncMock(return_value={"data": []})
        await gs.fetch_telemetry_summary(
            "manufacturing",
            "realtime",
            ["s1"],
            execute_sql=runner,
            warehouse_id="wh",
            pipeline_scope=True,
        )
        sql = runner.call_args.args[0]
        assert f"cat.sch.{SHADOW_SHORT}" in sql
        assert CDC in sql and "industry = 'manufacturing'" in sql

    @pytest.mark.asyncio
    async def test_microbatch_pipeline_scope_reads_mode_table(self) -> None:
        runner = AsyncMock(return_value={"data": []})
        await gs.fetch_telemetry_summary(
            "manufacturing",
            "microbatch",
            ["s1"],
            execute_sql=runner,
            warehouse_id="wh",
            pipeline_scope=True,
        )
        sql = runner.call_args.args[0]
        assert "cat.sch.manufacturing_microbatch" in sql
        assert "_pg_change_type" not in sql

    @pytest.mark.asyncio
    async def test_static_scope_unchanged(self) -> None:
        runner = AsyncMock(return_value={"data": []})
        await gs.fetch_telemetry_summary(
            "manufacturing",
            "realtime",
            ["s1"],
            execute_sql=runner,
            warehouse_id="wh",
        )
        sql = runner.call_args.args[0]
        assert "cat.sch.manufacturing_realtime" in sql

    @pytest.mark.asyncio
    async def test_pipeline_route_requests_pipeline_scope(self, monkeypatch) -> None:
        from iot_twin.routes import genie_pipelines

        captured: dict = {}

        async def _bounded(industry, mode, names, **kwargs):
            captured.update(kwargs)
            return []

        monkeypatch.setattr(gs, "bounded_telemetry_summary", _bounded)
        scope = genie_pipelines.PipelineScope(S1, "realtime", "manufacturing", "sp")
        state = SimpleNamespace(app_backend_db=None, app_backend_available=False)
        await genie_pipelines._pack_for(state, scope, "u")
        assert captured.get("pipeline_scope") is True


# ---------------------------------------------------------------------------
# N5 shared resolution + cache refresh after update
# ---------------------------------------------------------------------------
class TestSharedResolution:
    def _db(self, setups: dict, deployments: dict):
        async def _get_setup(_db, sid):
            return setups.get(str(sid))

        async def _get_deployment(_db, did):
            return deployments.get(str(did))

        return _get_setup, _get_deployment

    @pytest.mark.asyncio
    async def test_resolves_setup_and_deployment_ids(self, monkeypatch) -> None:
        get_setup, get_dep = self._db(
            {S1: {"id": S1, "industry": "manufacturing"}},
            {D1: {"id": D1, "setup_id": S1, "mode": "realtime"}},
        )
        monkeypatch.setattr(gss.pipeline_setup_repo, "get_setup", get_setup)
        monkeypatch.setattr(gss.deployment_repo, "get_deployment", get_dep)
        assert (await gss.resolve_pipeline_setup(None, S1, "realtime"))["id"] == S1
        assert (await gss.resolve_pipeline_setup(None, D1, "realtime"))["id"] == S1
        assert (await gss.resolve_pipeline_setup(None, D1.upper(), "realtime"))["id"] == S1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "pid,mode",
        [
            ("nope", "realtime"),
            (D1, "zerobus"),
            ("44444444-4444-4444-8444-444444444444", "realtime"),
        ],
    )
    async def test_not_found(self, monkeypatch, pid: str, mode: str) -> None:
        get_setup, get_dep = self._db({}, {D1: {"id": D1, "setup_id": S1, "mode": "realtime"}})
        monkeypatch.setattr(gss.pipeline_setup_repo, "get_setup", get_setup)
        monkeypatch.setattr(gss.deployment_repo, "get_deployment", get_dep)
        with pytest.raises(gss.PipelineNotFound):
            await gss.resolve_pipeline_setup(None, pid, mode)

    def test_route_and_provisioning_share_resolver(self) -> None:
        from iot_twin.routes import genie_pipelines
        from iot_twin.services.genie import genie_provisioning

        assert not hasattr(genie_pipelines, "_setup_for")
        assert not hasattr(genie_pipelines, "_canonical_uuid")
        assert not hasattr(genie_provisioning, "_resolve_setup")
        assert not hasattr(genie_provisioning, "_canonical_uuid")


@pytest.mark.asyncio
async def test_allowlist_cache_invalidated_after_update_succeeds(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(gss, "invalidate_allowlist_cache", lambda sid=None: calls.append(sid))
    db = MagicMock()
    db.fetch = AsyncMock(
        return_value=[
            {
                "id": "r",
                "resource_type": "genie_space",
                "identifier": "sp-1",
                "status": "active",
                "metadata": {"mode": "realtime"},
            }
        ]
    )
    w = MagicMock()
    order: list[str] = []
    w.genie.update_space.side_effect = lambda *a, **k: order.append(f"update:{len(calls)}")
    await gss.ensure_pipeline_space(
        db,
        setup_id="s",
        deployment_id="d",
        industry="manufacturing",
        mode="realtime",
        warehouse_id="wh",
        w=w,
    )
    assert calls == ["sp-1", "sp-1"]
    assert order == ["update:1"]


# ---------------------------------------------------------------------------
# Round-4: tracked shielded task + parenthesized realtime filters
# ---------------------------------------------------------------------------
def _create_db() -> MagicMock:
    db = MagicMock()
    db.fetch = AsyncMock(return_value=[])
    db.fetchrow = AsyncMock(return_value={"id": "r"})
    return db


def _slow_ws(result=None, error: Exception | None = None, delay: float = 0.2) -> MagicMock:
    import time

    w = MagicMock()
    w.tables.exists.return_value = SimpleNamespace(table_exists=True)

    def _create(**_kwargs):
        time.sleep(delay)
        if error is not None:
            raise error
        return SimpleNamespace(space_id=result)

    w.genie.create_space.side_effect = _create
    return w


async def _drain() -> None:
    for _ in range(40):
        if not gss._BACKGROUND_CREATES:
            return
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_post_timeout_failure_is_logged_and_task_discarded(caplog) -> None:
    caplog.set_level("ERROR", logger="iot_digital_twin.services.genie_space")
    w = _slow_ws(error=RuntimeError("quota exceeded"))
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(
            gss.ensure_pipeline_space(
                _create_db(),
                setup_id="s",
                deployment_id="d",
                industry="manufacturing",
                mode="zerobus",
                warehouse_id="wh",
                w=w,
            ),
            timeout=0.05,
        )
    assert len(gss._BACKGROUND_CREATES) == 1
    await _drain()
    assert set() == gss._BACKGROUND_CREATES
    assert any(
        "quota exceeded" in r.getMessage()
        or (r.exc_info and "quota exceeded" in str(r.exc_info[1]))
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_post_timeout_success_is_logged_and_task_discarded(caplog) -> None:
    caplog.set_level("INFO", logger="iot_digital_twin.services.genie_space")
    w = _slow_ws(result="sp-bg")
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(
            gss.ensure_pipeline_space(
                _create_db(),
                setup_id="s",
                deployment_id="d",
                industry="manufacturing",
                mode="zerobus",
                warehouse_id="wh",
                w=w,
            ),
            timeout=0.05,
        )
    await _drain()
    assert set() == gss._BACKGROUND_CREATES
    assert any("sp-bg" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_normal_create_leaves_no_tracked_task() -> None:
    w = _slow_ws(result="sp-ok", delay=0)
    out = await gss.ensure_pipeline_space(
        _create_db(),
        setup_id="s",
        deployment_id="d",
        industry="manufacturing",
        mode="zerobus",
        warehouse_id="wh",
        w=w,
    )
    assert out["space_id"] == "sp-ok"
    await asyncio.sleep(0)
    assert set() == gss._BACKGROUND_CREATES


def test_realtime_filter_snippets_parenthesized() -> None:
    filters = _space("realtime")["instructions"]["sql_snippets"]["filters"]
    for item in filters:
        sql = item["sql"][0]
        assert sql.startswith("(") and sql.endswith(")"), sql
        assert sql.count("(") == sql.count(")")
