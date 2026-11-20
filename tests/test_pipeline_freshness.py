"""Unit tests for services/pipelines/pipeline_freshness.py — the per-mode "is data flowing?"
signal that gates ``running`` vs ``starting`` in the readiness contract.

The sink probes are exercised with fakes (a stub Lakebase handle / a stub DBSQL
runner) so no real Lakebase / warehouse is touched. The defensive contract —
any error is treated as *not flowing* and never raises — is covered explicitly.
"""

from __future__ import annotations

import pytest

from iot_twin.services.pipelines import pipeline_freshness as pf

_UC_CATALOG = "example_eu_catalog"
_UC_SCHEMA = "rtm_lakets_zb"


@pytest.fixture(autouse=True)
def _clear_cache(monkeypatch):
    monkeypatch.setenv("UC_CATALOG", _UC_CATALOG)
    monkeypatch.setenv("UC_SCHEMA", _UC_SCHEMA)
    pf.clear_cache()
    yield
    pf.clear_cache()


# --- realtime (Lakebase) sink ------------------------------------------------
class _FakeHandle:
    def __init__(self, value=None, raises: Exception | None = None):
        self._value = value
        self._raises = raises
        self.calls: list[tuple] = []

    async def fetchval(self, sql: str, *args):
        self.calls.append((sql, args))
        if self._raises is not None:
            raise self._raises
        return self._value


class TestRealtimeFlowing:
    @pytest.mark.asyncio
    async def test_flowing_when_recent_row_exists(self):
        handle = _FakeHandle(value=1)
        assert await pf._realtime_flowing(handle, 10) is True
        # cheap LIMIT 1 existence probe on event_time within the window
        sql, args = handle.calls[0]
        assert "sensor_readings" in sql and "LIMIT 1" in sql
        assert args == (10,)

    @pytest.mark.asyncio
    async def test_not_flowing_when_no_recent_row(self):
        assert await pf._realtime_flowing(_FakeHandle(value=None), 10) is False

    @pytest.mark.asyncio
    async def test_error_is_not_flowing(self):
        handle = _FakeHandle(raises=RuntimeError("relation does not exist"))
        assert await pf._realtime_flowing(handle, 10) is False


# --- microbatch / zerobus (DBSQL) sinks --------------------------------------
class TestDbsqlFlowing:
    @pytest.mark.asyncio
    async def test_microbatch_flowing_when_rows(self):
        seen: dict = {}

        async def run_sql(sql: str) -> dict:
            seen["sql"] = sql
            return {"data": [{"1": 1}]}

        assert await pf._dbsql_flowing(run_sql, "pharma", "microbatch", 10) is True
        # One-schema prefixed FQN (Task 5).
        assert f"{_UC_CATALOG}.{_UC_SCHEMA}.pharma_microbatch" in seen["sql"]
        assert "INTERVAL 10 MINUTES" in seen["sql"]

    @pytest.mark.asyncio
    async def test_zerobus_targets_prefixed_table(self):
        seen: dict = {}

        async def run_sql(sql: str) -> dict:
            seen["sql"] = sql
            return {"data": []}

        assert await pf._dbsql_flowing(run_sql, "pharma", "zerobus", 10) is False
        # One-schema prefixed FQN (Task 5).
        assert f"{_UC_CATALOG}.{_UC_SCHEMA}.pharma_zerobus" in seen["sql"]

    @pytest.mark.asyncio
    async def test_error_is_not_flowing(self):
        async def run_sql(sql: str) -> dict:
            raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND")

        assert await pf._dbsql_flowing(run_sql, "pharma", "microbatch", 10) is False


# --- mode_flowing dispatch + cache -------------------------------------------
class TestModeFlowing:
    @pytest.mark.asyncio
    async def test_dbsql_mode_without_run_sql_is_not_flowing(self):
        assert await pf.mode_flowing(None, "pharma", "microbatch", run_sql=None) is False

    @pytest.mark.asyncio
    async def test_empty_industry_or_mode_is_not_flowing(self):
        assert await pf.mode_flowing(None, "", "microbatch") is False
        assert await pf.mode_flowing(None, "pharma", "") is False

    @pytest.mark.asyncio
    async def test_cache_short_circuits_repeat_probe(self):
        calls = {"n": 0}

        async def run_sql(sql: str) -> dict:
            calls["n"] += 1
            return {"data": [{"1": 1}]}

        first = await pf.mode_flowing(None, "pharma", "microbatch", run_sql=run_sql)
        second = await pf.mode_flowing(None, "pharma", "microbatch", run_sql=run_sql)
        assert first is True and second is True
        assert calls["n"] == 1  # second call served from the TTL cache

    @pytest.mark.asyncio
    async def test_use_cache_false_always_probes(self):
        calls = {"n": 0}

        async def run_sql(sql: str) -> dict:
            calls["n"] += 1
            return {"data": [{"1": 1}]}

        await pf.mode_flowing(None, "pharma", "microbatch", run_sql=run_sql, use_cache=False)
        await pf.mode_flowing(None, "pharma", "microbatch", run_sql=run_sql, use_cache=False)
        assert calls["n"] == 2


# --- flowing_lookup ----------------------------------------------------------
def _dep(mode: str, job_id: str = "j1", status: str = "deployed") -> dict:
    return {"mode": mode, "status": status, "job_id": job_id}


class TestFlowingLookup:
    @pytest.mark.asyncio
    async def test_only_probes_running_modes(self):
        setup = {"industry": "pharma", "enabled_modes": ["microbatch"]}
        deployments = [_dep("microbatch", "j1")]
        # job not running → nothing to probe, no run_sql needed
        result = await pf.flowing_lookup(None, setup, deployments, {"j1": False})
        assert result == {}

    @pytest.mark.asyncio
    async def test_probes_running_mode_via_injected_run_sql(self):
        setup = {"industry": "pharma", "enabled_modes": ["microbatch"]}
        deployments = [_dep("microbatch", "j1")]

        async def run_sql(sql: str) -> dict:
            return {"data": [{"1": 1}]}

        result = await pf.flowing_lookup(None, setup, deployments, {"j1": True}, run_sql=run_sql)
        assert result == {"microbatch": True}

    @pytest.mark.asyncio
    async def test_degraded_running_job_not_probed(self):
        setup = {"industry": "pharma", "enabled_modes": ["microbatch"]}
        deployments = [_dep("microbatch", "j1")]
        job = {"j1": {"running": True, "degraded": True}}
        result = await pf.flowing_lookup(None, setup, deployments, job)
        assert result == {}

    @pytest.mark.asyncio
    async def test_no_industry_returns_empty(self):
        result = await pf.flowing_lookup(None, {"industry": None}, [], {})
        assert result == {}
