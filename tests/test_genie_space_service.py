"""Per-pipeline manufacturing Genie space builder + lifecycle (mocked SDK)."""

from __future__ import annotations

import json
import re
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from databricks.sdk.errors import NotFound

from iot_twin.services import resource_deleters
from iot_twin.services.genie import genie_space_service as gss
from iot_twin.services.industry.industry_templates import CURATED_INDUSTRIES
from iot_twin.services.teardown_service import teardown_setup

HEX32 = re.compile(r"^[0-9a-f]{32}$")


@pytest.fixture(autouse=True)
def _uc_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UC_CATALOG", "cat")
    monkeypatch.setenv("UC_SCHEMA", "sch")
    monkeypatch.delenv(gss.PARENT_PATH_ENV, raising=False)


def _ledger_db(rows: list[dict] | None = None) -> MagicMock:
    db = MagicMock()
    db.fetch = AsyncMock(return_value=list(rows or []))
    db.execute = AsyncMock(return_value=None)

    async def _fetchrow(query: str, *args):
        q = query.lower()
        if "insert into provisioned_resource" in q:
            return {"id": "res-new", "resource_type": "genie_space"}
        if "update provisioned_resource" in q:
            return {"id": args[-1] if args else "res"}
        return None

    db.fetchrow = AsyncMock(side_effect=_fetchrow)
    return db


def _space_row(space_id: str = "sp-1", mode: str = "realtime", **extra) -> dict:
    return {
        "id": "res-1",
        "setup_id": "setup-1",
        "deployment_id": "dep-1",
        "resource_type": "genie_space",
        "identifier": space_id,
        "status": "active",
        "metadata": {"mode": mode, "industry": "manufacturing"},
        **extra,
    }


# ---------------------------------------------------------------------------
# Expected tables + serialized_space builder
# ---------------------------------------------------------------------------
class TestExpectedTables:
    def test_mode_metrics_and_intelligence_tables(self) -> None:
        tables = gss.expected_tables("manufacturing", "microbatch")
        assert "cat.sch.manufacturing_microbatch" in tables
        assert "cat.sch.manufacturing_metrics" in tables
        assert "cat.sch.intelligence_assets" in tables
        assert "cat.sch.intelligence_alert_snapshot" in tables
        assert "cat.sch.manufacturing_realtime" not in tables
        assert list(tables) == sorted(tables)
        assert len(tables) == 7

    def test_rejects_unknown_mode(self) -> None:
        with pytest.raises(ValueError):
            gss.expected_tables("manufacturing", "batch")

    def test_supports_every_offered_industry(self) -> None:
        for industry in CURATED_INDUSTRIES:
            assert gss.industry_supported(industry) is True
        assert gss.industry_supported("Manufacturing") is True
        assert gss.industry_supported("retail") is False
        assert gss.industry_supported(None) is False


class TestBuildSerializedSpace:
    def _space(self, mode: str = "realtime") -> dict:
        return json.loads(gss.build_serialized_space("manufacturing", mode))

    def test_version_and_sorted_tables(self) -> None:
        space = self._space()
        assert space["version"] == 2
        ids = [t["identifier"] for t in space["data_sources"]["tables"]]
        assert ids == sorted(ids)
        assert set(ids) == set(gss.expected_tables("manufacturing", "realtime"))

    def test_column_configs_sorted_on_telemetry_table(self) -> None:
        space = self._space()
        tel = next(
            t
            for t in space["data_sources"]["tables"]
            if t["identifier"] == gss.telemetry_table("manufacturing", "realtime")
        )
        names = [c["column_name"] for c in tel["column_configs"]]
        assert names == sorted(names)
        assert "fault_active" in names
        for cfg in tel["column_configs"]:
            if "description" in cfg:
                assert isinstance(cfg["description"], list)

    def test_single_text_instruction_with_manufacturing_definitions(self) -> None:
        space = self._space()
        texts = space["instructions"]["text_instructions"]
        assert len(texts) == 1
        assert HEX32.match(texts[0]["id"])
        body = " ".join(texts[0]["content"]).lower()
        assert "downtime" in body and "fault_active" in body
        assert "availability" in body and "oee" in body
        assert "anomaly" in body and "min_value" in body and "max_value" in body
        assert "nominal_value" in body
        assert "lb__shadow_manufacturing_sensor_readings_history" in body

    def test_example_sqls_shape_sorted_and_mode_scoped(self) -> None:
        space = self._space("zerobus")
        examples = space["instructions"]["example_question_sqls"]
        assert len(examples) >= 4
        assert [e["id"] for e in examples] == sorted(e["id"] for e in examples)
        for ex in examples:
            assert HEX32.match(ex["id"])
            assert isinstance(ex["question"], list) and ex["question"]
            assert isinstance(ex["sql"], list) and ex["sql"]
        sql = " ".join(s for ex in examples for s in ex["sql"])
        assert "manufacturing_zerobus" in sql
        assert "manufacturing_realtime" not in sql

    def test_sql_snippets_qualified_and_sorted(self) -> None:
        space = self._space()
        snippets = space["instructions"]["sql_snippets"]
        for key in ("measures", "filters", "expressions"):
            items = snippets[key]
            assert items, key
            assert [i["id"] for i in items] == sorted(i["id"] for i in items)
            for item in items:
                assert item["sql"] and isinstance(item["sql"], list)
                assert "lb__shadow_manufacturing_sensor_readings_history." in item["sql"][0]

    def test_sample_questions_match_starters(self) -> None:
        space = self._space("microbatch")
        questions = space["config"]["sample_questions"]
        assert [q["id"] for q in questions] == sorted(q["id"] for q in questions)
        texts = {q["question"][0] for q in questions}
        assert texts == set(gss.starter_questions("manufacturing", "microbatch"))

    def test_ids_unique_and_deterministic(self) -> None:
        first = gss.build_serialized_space("manufacturing", "realtime")
        assert first == gss.build_serialized_space("manufacturing", "realtime")
        space = json.loads(first)
        inst = space["instructions"]
        ids = [t["id"] for t in inst["text_instructions"]]
        ids += [e["id"] for e in inst["example_question_sqls"]]
        for key in ("measures", "filters", "expressions"):
            ids += [s["id"] for s in inst["sql_snippets"][key]]
        assert len(ids) == len(set(ids))
        assert all(HEX32.match(i) for i in ids)


class TestStarterQuestions:
    @pytest.mark.parametrize("mode", ["realtime", "microbatch", "zerobus"])
    def test_manufacturing_topics_per_mode(self, mode: str) -> None:
        questions = gss.starter_questions("manufacturing", mode)
        blob = " ".join(questions).lower()
        for topic in ("oee", "downtime", "anomal", "fault", "maintenance"):
            assert topic in blob, (mode, topic)
        assert len(questions) == len(set(questions))

    def test_mode_aware(self) -> None:
        assert gss.starter_questions("manufacturing", "realtime") != gss.starter_questions(
            "manufacturing", "microbatch"
        )
        assert gss.starter_questions("manufacturing", "zerobus") != gss.starter_questions(
            "manufacturing", "microbatch"
        )

    def test_unknown_mode(self) -> None:
        with pytest.raises(ValueError):
            gss.starter_questions("manufacturing", "nope")


# ---------------------------------------------------------------------------
# Ledger lookup
# ---------------------------------------------------------------------------
class TestFindPipelineSpace:
    @pytest.mark.asyncio
    async def test_returns_active_row_for_mode(self) -> None:
        rows = [
            {
                "resource_type": "job",
                "identifier": "1",
                "status": "active",
                "metadata": {"mode": "realtime"},
            },
            _space_row("old", status="deleted"),
            _space_row("mb", mode="microbatch"),
            _space_row("rt"),
        ]
        db = _ledger_db(rows)
        found = await gss.find_pipeline_space(db, "setup-1", "realtime")
        assert found["identifier"] == "rt"
        assert await gss.find_space_id(db, "setup-1", "microbatch") == "mb"
        assert await gss.find_space_id(db, "setup-1", "zerobus") is None


# ---------------------------------------------------------------------------
# ensure_pipeline_space — create vs update idempotency
# ---------------------------------------------------------------------------
class TestEnsurePipelineSpace:
    @pytest.mark.asyncio
    async def test_creates_and_records_when_absent(self) -> None:
        db = _ledger_db([])
        w = MagicMock()
        w.genie.create_space.return_value = SimpleNamespace(space_id="sp-new")
        out = await gss.ensure_pipeline_space(
            db,
            setup_id="setup-1",
            deployment_id="dep-1",
            industry="manufacturing",
            mode="realtime",
            warehouse_id="wh-1",
            w=w,
        )
        assert out["space_id"] == "sp-new"
        assert out["action"] == "created"
        kwargs = w.genie.create_space.call_args.kwargs
        assert kwargs["warehouse_id"] == "wh-1"
        assert json.loads(kwargs["serialized_space"])["version"] == 2
        assert "parent_path" not in kwargs or kwargs["parent_path"] is None
        w.genie.update_space.assert_not_called()
        insert = next(
            c
            for c in db.fetchrow.call_args_list
            if "insert into provisioned_resource" in c.args[0].lower()
        )
        assert "genie_space" in insert.args
        assert "sp-new" in insert.args

    @pytest.mark.asyncio
    async def test_parent_path_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(gss.PARENT_PATH_ENV, "/Workspace/Shared/genie")
        db = _ledger_db([])
        w = MagicMock()
        w.genie.create_space.return_value = SimpleNamespace(space_id="sp-new")
        await gss.ensure_pipeline_space(
            db,
            setup_id="setup-1",
            deployment_id="dep-1",
            industry="manufacturing",
            mode="realtime",
            warehouse_id="wh-1",
            w=w,
        )
        assert w.genie.create_space.call_args.kwargs["parent_path"] == "/Workspace/Shared/genie"

    @pytest.mark.asyncio
    async def test_updates_existing_space_on_redeploy(self) -> None:
        db = _ledger_db([_space_row("sp-1")])
        w = MagicMock()
        w.genie.update_space.return_value = SimpleNamespace(space_id="sp-1")
        out = await gss.ensure_pipeline_space(
            db,
            setup_id="setup-1",
            deployment_id="dep-1",
            industry="manufacturing",
            mode="realtime",
            warehouse_id="wh-2",
            w=w,
        )
        assert out == {**out, "space_id": "sp-1", "action": "updated"}
        w.genie.create_space.assert_not_called()
        kwargs = w.genie.update_space.call_args.kwargs
        assert w.genie.update_space.call_args.args[0] == "sp-1" or kwargs.get("space_id") == "sp-1"
        assert kwargs["warehouse_id"] == "wh-2"
        assert json.loads(kwargs["serialized_space"])["version"] == 2
        inserts = [
            c
            for c in db.fetchrow.call_args_list
            if "insert into provisioned_resource" in c.args[0].lower()
        ]
        assert inserts == []

    @pytest.mark.asyncio
    async def test_recreates_when_recorded_space_was_deleted(self) -> None:
        db = _ledger_db([_space_row("sp-gone")])
        w = MagicMock()
        w.genie.update_space.side_effect = NotFound("space not found")
        w.genie.create_space.return_value = SimpleNamespace(space_id="sp-fresh")
        out = await gss.ensure_pipeline_space(
            db,
            setup_id="setup-1",
            deployment_id="dep-1",
            industry="manufacturing",
            mode="realtime",
            warehouse_id="wh-1",
            w=w,
        )
        assert out["space_id"] == "sp-fresh"
        assert out["action"] == "recreated"
        queries = [c.args[0].lower() for c in db.fetchrow.call_args_list]
        assert any("update provisioned_resource set status" in q for q in queries)
        assert any("insert into provisioned_resource" in q for q in queries)

    @pytest.mark.asyncio
    async def test_requires_warehouse(self) -> None:
        with pytest.raises(gss.GenieSpaceError):
            await gss.ensure_pipeline_space(
                _ledger_db([]),
                setup_id="s",
                deployment_id="d",
                industry="manufacturing",
                mode="realtime",
                warehouse_id="",
                w=MagicMock(),
            )

    @pytest.mark.asyncio
    async def test_rejects_unknown_industry(self) -> None:
        with pytest.raises(gss.GenieSpaceError):
            await gss.ensure_pipeline_space(
                _ledger_db([]),
                setup_id="s",
                deployment_id="d",
                industry="retail",
                mode="realtime",
                warehouse_id="wh",
                w=MagicMock(),
            )

    @pytest.mark.asyncio
    async def test_missing_space_id_in_create_response(self) -> None:
        w = MagicMock()
        w.genie.create_space.return_value = SimpleNamespace(space_id=None)
        with pytest.raises(gss.GenieSpaceError):
            await gss.ensure_pipeline_space(
                _ledger_db([]),
                setup_id="s",
                deployment_id="d",
                industry="manufacturing",
                mode="realtime",
                warehouse_id="wh",
                w=w,
            )


# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------
class TestTeardown:
    @pytest.mark.asyncio
    async def test_deleter_trashes_space(self) -> None:
        w = MagicMock()
        await gss.make_genie_space_deleter(w)(_space_row("sp-9"))
        w.genie.trash_space.assert_called_once_with("sp-9")

    @pytest.mark.asyncio
    async def test_deleter_tolerates_already_absent(self) -> None:
        w = MagicMock()
        w.genie.trash_space.side_effect = NotFound("gone")
        await gss.make_genie_space_deleter(w)(_space_row("sp-9"))

    @pytest.mark.asyncio
    async def test_deleter_propagates_other_errors(self) -> None:
        w = MagicMock()
        w.genie.trash_space.side_effect = RuntimeError("permission denied")
        with pytest.raises(RuntimeError):
            await gss.make_genie_space_deleter(w)(_space_row("sp-9"))

    @pytest.mark.asyncio
    async def test_deleter_ignores_rows_without_identifier(self) -> None:
        w = MagicMock()
        await gss.make_genie_space_deleter(w)({"identifier": ""})
        w.genie.trash_space.assert_not_called()

    def test_registered_in_all_deleters(self) -> None:
        registry = resource_deleters.make_all_deleters(ws=MagicMock(), warehouse_id="wh")
        assert "genie_space" in registry

    @pytest.mark.asyncio
    async def test_teardown_setup_trashes_space(self) -> None:
        row = _space_row("sp-t")
        db = MagicMock()
        db.fetch = AsyncMock(return_value=[row])
        db.fetchrow = AsyncMock(return_value={"id": "res-1"})
        db.execute = AsyncMock(return_value="DELETE 1")
        w = MagicMock()
        result = await teardown_setup(
            db, "setup-1", {"genie_space": gss.make_genie_space_deleter(w)}
        )
        w.genie.trash_space.assert_called_once_with("sp-t")
        assert result["steps"][0]["action"] == "deleted"


# ---------------------------------------------------------------------------
# Query-result shaping (attachment result endpoint)
# ---------------------------------------------------------------------------
def _statement(rows: list[list], *, total: int | None = None, truncated: bool | None = None):
    columns = [
        SimpleNamespace(
            name="sensor_name", type_name=SimpleNamespace(value="STRING"), type_text="STRING"
        ),
        SimpleNamespace(
            name="downtime_minutes", type_name=SimpleNamespace(value="LONG"), type_text=None
        ),
    ]
    manifest = SimpleNamespace(
        schema=SimpleNamespace(columns=columns),
        total_row_count=total,
        truncated=truncated,
    )
    return SimpleNamespace(
        statement_response=SimpleNamespace(
            manifest=manifest, result=SimpleNamespace(data_array=rows)
        )
    )


class TestQueryResultPayload:
    def test_shapes_columns_and_rows(self) -> None:
        out = gss.query_result_payload(_statement([["s1", "3"]], total=1))
        assert out == {
            "columns": [
                {"name": "sensor_name", "type": "STRING"},
                {"name": "downtime_minutes", "type": "LONG"},
            ],
            "rows": [["s1", "3"]],
            "truncated": False,
        }

    def test_caps_at_500_rows(self) -> None:
        rows = [[f"s{i}", str(i)] for i in range(750)]
        out = gss.query_result_payload(_statement(rows, total=750))
        assert len(out["rows"]) == gss.MAX_RESULT_ROWS == 500
        assert out["truncated"] is True

    def test_truncated_when_more_rows_exist_server_side(self) -> None:
        out = gss.query_result_payload(_statement([["a", "1"]], total=9000))
        assert out["truncated"] is True
        out = gss.query_result_payload(_statement([["a", "1"]], truncated=True))
        assert out["truncated"] is True

    def test_empty_response(self) -> None:
        empty = {"columns": [], "rows": [], "truncated": False}
        assert gss.query_result_payload(SimpleNamespace(statement_response=None)) == empty
        assert gss.query_result_payload(None) == empty
