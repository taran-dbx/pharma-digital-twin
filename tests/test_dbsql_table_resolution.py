"""TDD: Task 5 — dashboard + DBSQL read one-schema prefixed tables.

Asserts:
  1. zerobus cold table resolves to
     ``example_eu_catalog.rtm_lakets_zb.{industry}_zerobus``
  2. metrics table resolves to
     ``example_eu_catalog.rtm_lakets_zb.{industry}_metrics``
     and the streaming-latency SQL filters by ``mode``.
  3. pipeline_freshness._dbsql_flowing targets the prefixed one-schema FQN,
     NOT the old ``iot_<industry>.<schema>.<table>`` path.
"""

from __future__ import annotations

import importlib

import pytest

# Canonical new-workspace constants (config values baked into the app env).
UC_CATALOG = "example_eu_catalog"
UC_SCHEMA = "rtm_lakets_zb"


@pytest.fixture(autouse=True)
def _uc_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Inject UC_CATALOG / UC_SCHEMA for every test in this module."""
    monkeypatch.setenv("UC_CATALOG", UC_CATALOG)
    monkeypatch.setenv("UC_SCHEMA", UC_SCHEMA)
    # Force re-import so env vars are picked up by module-level cached reads.
    for mod_name in (
        "iot_twin.services.industry.industry_catalog_service",
        "iot_twin.services.pipelines.pipeline_freshness",
    ):
        if mod_name in __import__("sys").modules:
            importlib.reload(__import__("sys").modules[mod_name])


# ---------------------------------------------------------------------------
# 1. industry_table() builds the one-schema FQN
# ---------------------------------------------------------------------------
class TestIndustryTableResolution:
    def test_zerobus_cold_table_resolves_to_prefixed_one_schema(self) -> None:
        from iot_twin.services.industry.industry_catalog_service import industry_table

        fqn = industry_table("pharmaceutical", "zerobus")
        assert fqn == f"{UC_CATALOG}.{UC_SCHEMA}.pharmaceutical_zerobus"

    def test_microbatch_cold_table_resolves_to_prefixed_one_schema(self) -> None:
        from iot_twin.services.industry.industry_catalog_service import industry_table

        fqn = industry_table("pharmaceutical", "microbatch")
        assert fqn == f"{UC_CATALOG}.{UC_SCHEMA}.pharmaceutical_microbatch"

    def test_realtime_cold_table_resolves_to_prefixed_one_schema(self) -> None:
        from iot_twin.services.industry.industry_catalog_service import industry_table

        fqn = industry_table("energy", "realtime")
        assert fqn == f"{UC_CATALOG}.{UC_SCHEMA}.energy_realtime"

    def test_metrics_table_resolves_to_prefixed_one_schema(self) -> None:
        from iot_twin.services.industry.industry_catalog_service import industry_table

        fqn = industry_table("pharmaceutical", "metrics")
        assert fqn == f"{UC_CATALOG}.{UC_SCHEMA}.pharmaceutical_metrics"

    def test_multi_word_industry_is_sanitised(self) -> None:
        from iot_twin.services.industry.industry_catalog_service import industry_table

        fqn = industry_table("Oil & Gas", "zerobus")
        assert fqn == f"{UC_CATALOG}.{UC_SCHEMA}.oil_gas_zerobus"


# ---------------------------------------------------------------------------
# 2. pipeline_freshness._dbsql_flowing targets the prefixed FQN
# ---------------------------------------------------------------------------
class TestFreshnessTableResolution:
    @pytest.mark.asyncio
    async def test_microbatch_freshness_queries_prefixed_table(self) -> None:
        """After Task 5 _dbsql_flowing must target {industry}_microbatch, not
        iot_{industry}.microbatch.sensor_data."""
        from iot_twin.services.pipelines import pipeline_freshness as pf

        pf.clear_cache()
        seen: dict = {}

        async def run_sql(sql: str) -> dict:
            seen["sql"] = sql
            return {"data": [{"1": 1}]}

        flowing = await pf._dbsql_flowing(run_sql, "pharmaceutical", "microbatch", 10)
        assert flowing is True
        assert f"{UC_CATALOG}.{UC_SCHEMA}.pharmaceutical_microbatch" in seen["sql"]
        # Must NOT use the old per-catalog schema path.
        assert "iot_pharmaceutical.microbatch.sensor_data" not in seen["sql"]

    @pytest.mark.asyncio
    async def test_zerobus_freshness_queries_prefixed_table(self) -> None:
        """After Task 5 _dbsql_flowing must target {industry}_zerobus, not
        iot_{industry}.zerobus.iot_sensor_data."""
        from iot_twin.services.pipelines import pipeline_freshness as pf

        pf.clear_cache()
        seen: dict = {}

        async def run_sql(sql: str) -> dict:
            seen["sql"] = sql
            return {"data": []}

        flowing = await pf._dbsql_flowing(run_sql, "pharma", "zerobus", 10)
        assert flowing is False  # no rows in result
        assert f"{UC_CATALOG}.{UC_SCHEMA}.pharma_zerobus" in seen["sql"]
        assert "iot_pharma.zerobus.iot_sensor_data" not in seen["sql"]

    @pytest.mark.asyncio
    async def test_freshness_error_is_not_flowing(self) -> None:
        """Errors during the probe are always treated as not-flowing (defensive)."""
        from iot_twin.services.pipelines import pipeline_freshness as pf

        pf.clear_cache()

        async def run_sql(sql: str) -> dict:
            raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND")

        flowing = await pf._dbsql_flowing(run_sql, "pharmaceutical", "microbatch", 10)
        assert flowing is False


# ---------------------------------------------------------------------------
# 3. Metrics read: {industry}_metrics table + mode filter in SQL
# ---------------------------------------------------------------------------
class TestMetricsTableResolution:
    def test_metrics_fqn_is_prefixed_one_schema(self) -> None:
        """The managed metrics table lives in the shared catalog/schema, NOT in
        per-industry iot_<industry>.metrics.streaming_metrics_<mode>."""
        from iot_twin.services.industry.industry_catalog_service import industry_table

        expected = f"{UC_CATALOG}.{UC_SCHEMA}.pharmaceutical_metrics"
        assert industry_table("pharmaceutical", "metrics") == expected

    def test_metrics_table_is_same_for_all_modes(self) -> None:
        """A SINGLE {industry}_metrics table holds all modes — distinguished by
        the ``mode`` column, not by separate tables per mode."""
        from iot_twin.services.industry.industry_catalog_service import industry_table

        # All modes map to the same {industry}_metrics table.
        assert industry_table("energy", "metrics") == f"{UC_CATALOG}.{UC_SCHEMA}.energy_metrics"

    def test_mode_column_present_in_managed_metrics_ddl(self) -> None:
        """The managed metrics DDL includes a leading ``mode`` column."""
        from iot_twin.models.metrics_schema import METRICS_MODE_COLUMN, metrics_managed_columns

        cols = metrics_managed_columns()
        assert cols[0].column == METRICS_MODE_COLUMN

    @pytest.mark.asyncio
    async def test_streaming_latency_uses_industry_metrics_with_mode_filter(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The /api/latency/streaming endpoint SQL must query {industry}_metrics
        and include a WHERE mode = '<mode>' predicate (via DBSQL).

        This test drives the route implementation via a mock execute_dbsql —
        it bypasses routes/__init__.py by importing the module directly.
        """
        import importlib.util
        import sys
        from pathlib import Path

        route_path = (
            Path(__file__).parent.parent / "src" / "iot_twin" / "routes" / "streaming_latency.py"
        )
        spec = importlib.util.spec_from_file_location("_test_streaming_latency", str(route_path))
        mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
        sys.modules["_test_streaming_latency"] = mod
        spec.loader.exec_module(mod)  # type: ignore[union-attr]

        captured: dict = {}

        async def mock_execute_dbsql(sql: str, warehouse_id: str) -> dict:
            captured["sql"] = sql
            return {
                "data": [
                    {
                        "batch_id": 1,
                        "timestamp": "2026-08-27T10:00:00.000+00:00",
                        "num_input_rows": 500,
                        "input_rps": 250.0,
                        "processed_rps": 240.0,
                        "trigger_ms": 2000.0,
                        "e2e_p50": 120.0,
                        "e2e_p99": 300.0,
                        "processing_p50": 40.0,
                        "processing_p99": 90.0,
                        "queuing_p50": 15.0,
                        "queuing_p99": 60.0,
                    }
                ],
                "row_count": 1,
            }

        async def mock_resolve_warehouse(db, db_available, user_email):  # noqa: ANN001
            return "wh-test"

        async def mock_is_bound_pipeline(*_args, **_kwargs) -> bool:
            return True

        monkeypatch.setattr(mod, "execute_dbsql", mock_execute_dbsql)
        monkeypatch.setattr(mod, "resolve_warehouse_id", mock_resolve_warehouse)
        monkeypatch.setattr(mod, "_is_bound_pipeline", mock_is_bound_pipeline)

        from fastapi import FastAPI
        from httpx import ASGITransport, AsyncClient

        from iot_twin.core.state import AppState

        state = AppState()
        state.db = None
        state.db_available = False

        app = FastAPI()
        app.state.app_state = state
        app.include_router(mod.router)

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                "/api/latency/streaming?pipeline_id=00000000-0000-4000-8000-000000000001&industry=pharmaceutical&mode=realtime"
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["has_data"] is True
        # Table must be the prefixed one-schema table.
        assert body["table"] == f"{UC_CATALOG}.{UC_SCHEMA}.pharmaceutical_metrics"
        # SQL must include a mode filter.
        assert "captured" in captured or "sql" in captured
        assert f"{UC_CATALOG}.{UC_SCHEMA}.pharmaceutical_metrics" in captured["sql"]
        assert "mode" in captured["sql"]
        assert "realtime" in captured["sql"]


# ---------------------------------------------------------------------------
# 4. get_streaming_metrics mode allowlist (SQL injection guard)
# ---------------------------------------------------------------------------
class TestStreamingMetricsModeValidation:
    """Bad ``mode`` values must be rejected with 400 and must never reach SQL.

    The guard must match the sibling ``get_streaming_latency`` endpoint which
    validates against ``metrics_schema.ALL_METRIC_MODES``.  Since ``_DBSQL_MODES``
    is the superset used in ``get_streaming_metrics`` the allowlist is at
    least as strict.
    """

    @pytest.mark.asyncio
    async def test_bad_mode_returns_400_not_interpolated(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``mode=realtime'; DROP TABLE ...; --`` must yield 400, not execute."""
        import importlib.util
        import sys
        from pathlib import Path

        route_path = Path(__file__).parent.parent / "src" / "iot_twin" / "routes" / "sensor_data.py"
        spec = importlib.util.spec_from_file_location("_test_sensor_data", str(route_path))
        mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
        sys.modules["_test_sensor_data"] = mod
        spec.loader.exec_module(mod)  # type: ignore[union-attr]

        sql_calls: list[str] = []

        async def mock_execute_dbsql(sql: str, warehouse_id: str) -> dict:
            sql_calls.append(sql)
            return {"data": [], "row_count": 0}

        async def mock_get_pipeline_catalog(*a, **kw):  # noqa: ANN002
            return None

        async def mock_resolve_warehouse(db, db_available, user_email):  # noqa: ANN001
            return "wh-test"

        async def mock_is_industry_deployment(*_args, **_kwargs) -> bool:
            return True

        monkeypatch.setattr(mod, "execute_dbsql", mock_execute_dbsql)
        monkeypatch.setattr(mod, "get_pipeline_catalog", mock_get_pipeline_catalog)
        monkeypatch.setattr(mod, "resolve_warehouse_id", mock_resolve_warehouse)
        monkeypatch.setattr(mod, "_is_industry_deployment", mock_is_industry_deployment)

        from fastapi import FastAPI
        from httpx import ASGITransport, AsyncClient

        from iot_twin.core.state import AppState

        state = AppState()
        state.db = None
        state.db_available = False

        app = FastAPI()
        app.state.app_state = state
        app.include_router(mod.router)

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                "/api/pipeline/pipe-1/streaming-metrics"
                "?industry=pharmaceutical"
                "&mode=realtime%27%3B+DROP+TABLE+foo%3B+--"
            )

        assert resp.status_code == 400
        # The injected string must never have been sent to DBSQL.
        assert sql_calls == [], f"execute_dbsql was called with: {sql_calls}"
        body = resp.json()
        assert "error" in body

    @pytest.mark.asyncio
    async def test_valid_mode_reaches_sql(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A valid ``mode=realtime`` must still work correctly."""
        import importlib.util
        import sys
        from pathlib import Path

        route_path = Path(__file__).parent.parent / "src" / "iot_twin" / "routes" / "sensor_data.py"
        spec = importlib.util.spec_from_file_location("_test_sensor_data2", str(route_path))
        mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
        sys.modules["_test_sensor_data2"] = mod
        spec.loader.exec_module(mod)  # type: ignore[union-attr]

        captured: dict = {}

        async def mock_execute_dbsql(sql: str, warehouse_id: str) -> dict:
            captured.setdefault("calls", []).append(sql)
            return {"data": [], "row_count": 0}

        async def mock_get_pipeline_catalog(*a, **kw):  # noqa: ANN002
            return None

        async def mock_resolve_warehouse(db, db_available, user_email):  # noqa: ANN001
            return "wh-test"

        async def allow_industry_deployment(*_args, **_kwargs) -> bool:
            return True

        monkeypatch.setattr(mod, "execute_dbsql", mock_execute_dbsql)
        monkeypatch.setattr(mod, "get_pipeline_catalog", mock_get_pipeline_catalog)
        monkeypatch.setattr(mod, "resolve_warehouse_id", mock_resolve_warehouse)
        monkeypatch.setattr(mod, "_is_industry_deployment", allow_industry_deployment)

        from fastapi import FastAPI
        from httpx import ASGITransport, AsyncClient

        from iot_twin.core.state import AppState

        state = AppState()
        state.db = None
        state.db_available = False

        app = FastAPI()
        app.state.app_state = state
        app.include_router(mod.router)

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                "/api/pipeline/pipe-1/streaming-metrics?industry=pharmaceutical&mode=realtime"
            )

        assert resp.status_code == 200
        # SQL was called and contains the safe allowlisted mode value.
        assert captured.get("calls"), "execute_dbsql was never called"
        combined = " ".join(captured["calls"])
        assert "realtime" in combined
        # Must NOT contain any SQL-injection sentinel characters from a crafted input.
        assert "DROP" not in combined.upper()
