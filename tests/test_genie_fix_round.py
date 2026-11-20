"""Review fixes for per-pipeline Genie: allowlist fail-closed + cache, content,
intelligence-table precheck, bounded telemetry and deploy-hook timeouts."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from iot_twin.services import genie_service as gs
from iot_twin.services.deployment import deployment_service as ds
from iot_twin.services.genie import genie_space_content as content
from iot_twin.services.genie import genie_space_service as gss


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UC_CATALOG", "cat")
    monkeypatch.setenv("UC_SCHEMA", "sch")
    monkeypatch.delenv("GENIE_ALLOWLIST_CONFIRMED", raising=False)
    monkeypatch.delenv("DATABRICKS_SQL_WAREHOUSE_ID", raising=False)
    gs.invalidate_allowlist_cache()
    yield
    gs.invalidate_allowlist_cache()


def _client(serialized: str | None) -> MagicMock:
    client = MagicMock()
    client.genie.get_space.return_value = SimpleNamespace(serialized_space=serialized)
    return client


def _expected(mode: str = "realtime") -> list[str]:
    return list(gss.expected_tables("manufacturing", mode))


# ---------------------------------------------------------------------------
# #7 allowlist fail-closed
# ---------------------------------------------------------------------------
class TestAllowlistFailClosed:
    def test_unknown_data_source_kind_is_mismatch(self) -> None:
        payload = json.loads(gss.build_serialized_space("manufacturing", "realtime"))
        payload["data_sources"]["volumes"] = [{"identifier": "cat.sch.files"}]
        status = gs.space_table_allowlist_status(_client(json.dumps(payload)), "sp", _expected())
        assert status["status"] == "mismatch"
        assert status["authorization_enforced"] is False

    def test_empty_unknown_kind_is_ignored(self) -> None:
        payload = json.loads(gss.build_serialized_space("manufacturing", "realtime"))
        payload["data_sources"]["volumes"] = []
        status = gs.space_table_allowlist_status(_client(json.dumps(payload)), "sp", _expected())
        assert status["status"] == "matched"

    def test_manual_override_not_honored_for_pipeline_spaces(self, monkeypatch) -> None:
        monkeypatch.setenv("GENIE_ALLOWLIST_CONFIRMED", "true")
        client = MagicMock()
        client.genie.get_space.side_effect = RuntimeError("uninspectable")
        status = gs.space_table_allowlist_status(client, "sp", _expected())
        assert status["status"] == "manual_required"
        assert status["authorization_enforced"] is False
        assert "GENIE_ALLOWLIST_CONFIRMED" not in status["requirement"]
        with pytest.raises(gs.GenieDisabledError):
            gs.require_table_allowlist(client, "sp", _expected())

    def test_manual_override_still_honored_for_static_space(self, monkeypatch) -> None:
        monkeypatch.setenv("GENIE_ALLOWLIST_CONFIRMED", "true")
        monkeypatch.setenv("GENIE_SPACE_ID", "static")
        client = MagicMock()
        client.genie.get_space.side_effect = RuntimeError("uninspectable")
        assert gs.space_table_allowlist_status(client)["authorization_enforced"] is True

    def test_neutral_mismatch_reason_for_pipeline(self) -> None:
        client = _client(gss.build_serialized_space("manufacturing", "microbatch"))
        status = gs.space_table_allowlist_status(client, "sp", _expected())
        assert "intelligence_*" not in status["requirement"]
        assert "pipeline" in status["requirement"]


# ---------------------------------------------------------------------------
# #8 allowlist cache
# ---------------------------------------------------------------------------
class TestAllowlistCache:
    def test_verified_result_cached(self) -> None:
        client = _client(gss.build_serialized_space("manufacturing", "realtime"))
        gs.require_table_allowlist(client, "sp", _expected())
        gs.require_table_allowlist(client, "sp", _expected())
        assert client.genie.get_space.call_count == 1

    def test_mismatch_not_cached(self) -> None:
        client = _client(gss.build_serialized_space("manufacturing", "microbatch"))
        for _ in range(2):
            with pytest.raises(gs.GenieDisabledError):
                gs.require_table_allowlist(client, "sp", _expected())
        assert client.genie.get_space.call_count == 2

    def test_cache_expires(self, monkeypatch) -> None:
        clock = {"now": 1000.0}
        monkeypatch.setattr(gs, "_monotonic", lambda: clock["now"])
        client = _client(gss.build_serialized_space("manufacturing", "realtime"))
        gs.require_table_allowlist(client, "sp", _expected())
        clock["now"] += gs.ALLOWLIST_CACHE_TTL_S + 1
        gs.require_table_allowlist(client, "sp", _expected())
        assert client.genie.get_space.call_count == 2

    def test_cache_keyed_by_expected_set(self) -> None:
        client = _client(gss.build_serialized_space("manufacturing", "realtime"))
        gs.require_table_allowlist(client, "sp", _expected())
        with pytest.raises(gs.GenieDisabledError):
            gs.require_table_allowlist(client, "sp", _expected("zerobus"))

    def test_static_space_not_cached(self, monkeypatch) -> None:
        monkeypatch.setenv("GENIE_SPACE_ID", "static")
        intel = gs.allowed_uc_tables("", "")
        client = _client(
            json.dumps({"data_sources": {"tables": [{"identifier": t} for t in intel]}})
        )
        gs.require_table_allowlist(client)
        gs.require_table_allowlist(client)
        assert client.genie.get_space.call_count == 2

    @pytest.mark.asyncio
    async def test_ensure_pipeline_space_invalidates(self) -> None:
        client = _client(gss.build_serialized_space("manufacturing", "realtime"))
        gs.require_table_allowlist(client, "sp-1", _expected())
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
        db.fetchrow = AsyncMock(return_value={"id": "r"})
        w = MagicMock()
        await gss.ensure_pipeline_space(
            db,
            setup_id="s",
            deployment_id="d",
            industry="manufacturing",
            mode="realtime",
            warehouse_id="wh",
            w=w,
        )
        gs.require_table_allowlist(client, "sp-1", _expected())
        assert client.genie.get_space.call_count == 2


# ---------------------------------------------------------------------------
# #5 intelligence precheck before create
# ---------------------------------------------------------------------------
class TestIntelligencePrecheck:
    def _db(self) -> MagicMock:
        db = MagicMock()
        db.fetch = AsyncMock(return_value=[])
        db.fetchrow = AsyncMock(return_value={"id": "r"})
        return db

    @pytest.mark.asyncio
    async def test_missing_tables_block_create_with_sync_hint(self) -> None:
        w = MagicMock()

        def _exists(full_name: str):
            return SimpleNamespace(table_exists="alert_snapshot" not in full_name)

        w.tables.exists.side_effect = _exists
        with pytest.raises(gss.GenieSpaceError) as err:
            await gss.ensure_pipeline_space(
                self._db(),
                setup_id="s",
                deployment_id="d",
                industry="manufacturing",
                mode="realtime",
                warehouse_id="wh",
                w=w,
            )
        message = str(err.value)
        assert "intelligence_alert_snapshot" in message
        assert "/api/admin/intelligence/sync" in message and "redeploy" in message
        w.genie.create_space.assert_not_called()

    @pytest.mark.asyncio
    async def test_existing_tables_allow_create(self) -> None:
        w = MagicMock()
        w.tables.exists.return_value = SimpleNamespace(table_exists=True)
        w.genie.create_space.return_value = SimpleNamespace(space_id="sp")
        out = await gss.ensure_pipeline_space(
            self._db(),
            setup_id="s",
            deployment_id="d",
            industry="manufacturing",
            mode="realtime",
            warehouse_id="wh",
            w=w,
        )
        assert out["space_id"] == "sp"
        checked = {c.args[0] for c in w.tables.exists.call_args_list}
        assert "cat.sch.intelligence_assets" in checked

    @pytest.mark.asyncio
    async def test_check_error_does_not_block(self) -> None:
        w = MagicMock()
        w.tables.exists.side_effect = RuntimeError("no permission to check")
        w.genie.create_space.return_value = SimpleNamespace(space_id="sp")
        out = await gss.ensure_pipeline_space(
            self._db(),
            setup_id="s",
            deployment_id="d",
            industry="manufacturing",
            mode="realtime",
            warehouse_id="wh",
            w=w,
        )
        assert out["space_id"] == "sp"


# ---------------------------------------------------------------------------
# #6 content
# ---------------------------------------------------------------------------
class TestContent:
    def _space(self) -> dict:
        return json.loads(gss.build_serialized_space("manufacturing", "realtime"))

    def test_text_lines_end_with_newline(self) -> None:
        lines = self._space()["instructions"]["text_instructions"][0]["content"]
        assert lines and all(line.endswith("\n") for line in lines)

    def test_industry_filter_and_bridge_instructions(self) -> None:
        body = "".join(self._space()["instructions"]["text_instructions"][0]["content"])
        assert "industry = 'manufacturing'" in body
        assert "sensor_bindings.sensor_name" in body
        assert "sensor_id" in body
        assert "intelligence_alert_rules" in body and "intelligence_alert_snapshot" in body

    def test_maintenance_example_filters_industry(self) -> None:
        refs = {
            "sensor_bindings": "cat.sch.intelligence_sensor_bindings",
            "maintenance_records": "cat.sch.intelligence_maintenance_records",
        }
        sql = content._maintenance_sql("cat.sch.manufacturing_realtime", refs, "manufacturing")
        assert "b.industry = 'manufacturing'" in sql
        assert "r.industry = 'manufacturing'" in sql

    def test_industry_literal_escaped(self) -> None:
        refs = {"sensor_bindings": "c.s.b", "maintenance_records": "c.s.m"}
        assert "o''brien" in content._maintenance_sql("c.s.t", refs, "o'brien")

    def test_oee_quality_ignores_null_bounds(self) -> None:
        sql = content._health_sql("cat.sch.manufacturing_realtime", "oee_pct")
        assert "min_value IS NULL OR max_value IS NULL THEN NULL" in sql
        assert "COALESCE(quality, 1)" in sql


# ---------------------------------------------------------------------------
# #3 bounded telemetry summary
# ---------------------------------------------------------------------------
class TestBoundedTelemetry:
    @pytest.mark.asyncio
    async def test_timeout_returns_empty(self, monkeypatch) -> None:
        async def _slow(*_a, **_k):
            await asyncio.sleep(5)
            return [{"x": 1}]

        monkeypatch.setattr(gs, "fetch_telemetry_summary", _slow)
        monkeypatch.setattr(gs, "TELEMETRY_TIMEOUT_S", 0.05)
        assert await gs.bounded_telemetry_summary("manufacturing", "realtime", ["s"]) == []

    @pytest.mark.asyncio
    async def test_passes_through_result(self, monkeypatch) -> None:
        monkeypatch.setattr(gs, "fetch_telemetry_summary", AsyncMock(return_value=[{"x": 1}]))
        rows = await gs.bounded_telemetry_summary("manufacturing", "realtime", ["s"], db="db")
        assert rows == [{"x": 1}]

    def test_default_timeout_is_five_seconds(self) -> None:
        assert gs.TELEMETRY_TIMEOUT_S == 5.0


# ---------------------------------------------------------------------------
# #4 deploy hook timeout
# ---------------------------------------------------------------------------
class TestDeployHookTimeout:
    @pytest.mark.asyncio
    async def test_timeout_becomes_warning(self, monkeypatch) -> None:
        async def _slow(*_a, **_k):
            await asyncio.sleep(5)

        monkeypatch.setattr(gss, "ensure_pipeline_space", _slow)
        monkeypatch.setattr(ds, "GENIE_SPACE_TIMEOUT_S", 0.05)
        result = await ds._attach_genie_space(
            MagicMock(),
            setup={"industry": "manufacturing", "warehouse_id": "wh"},
            setup_id="s",
            deployment_id="d",
            mode="realtime",
            result={"job_id": 1},
            w=MagicMock(),
        )
        assert result["job_id"] == 1
        assert "timed out" in result["warnings"][0]

    def test_default_timeout_is_sixty_seconds(self) -> None:
        assert ds.GENIE_SPACE_TIMEOUT_S == 60.0
