"""genie_service fixes for per-pipeline spaces: allowlist parsing, serializers,
warehouse resolution and attachment query results (mocked SDK)."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from iot_twin.services import genie_service as gs
from iot_twin.services.genie import genie_space_service as gss


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UC_CATALOG", "cat")
    monkeypatch.setenv("UC_SCHEMA", "sch")
    monkeypatch.delenv("GENIE_ALLOWLIST_CONFIRMED", raising=False)
    monkeypatch.delenv("DATABRICKS_SQL_WAREHOUSE_ID", raising=False)


def _space_client(serialized: str | None) -> MagicMock:
    client = MagicMock()
    client.genie.get_space.return_value = SimpleNamespace(serialized_space=serialized)
    return client


# ---------------------------------------------------------------------------
# parse_space_tables / inspect_space_tables
# ---------------------------------------------------------------------------
class TestParseSpaceTables:
    def test_tables_and_metric_views(self) -> None:
        raw = json.dumps(
            {
                "version": 2,
                "data_sources": {
                    "tables": [{"identifier": "c.s.b"}, {"identifier": "c.s.a"}],
                    "metric_views": [{"identifier": "c.s.mv"}],
                },
            }
        )
        assert gs.parse_space_tables(raw) == ["c.s.a", "c.s.b", "c.s.mv"]

    def test_empty_data_sources(self) -> None:
        assert gs.parse_space_tables(json.dumps({"version": 2})) == []

    @pytest.mark.parametrize("raw", [None, "", "not json", "[1, 2]", '{"data_sources": 3}'])
    def test_unparseable_returns_none(self, raw) -> None:
        assert gs.parse_space_tables(raw) is None

    def test_builder_output_round_trips(self) -> None:
        raw = gss.build_serialized_space("manufacturing", "realtime")
        assert gs.parse_space_tables(raw) == list(gss.expected_tables("manufacturing", "realtime"))


class TestInspectSpaceTables:
    def test_requests_serialized_space(self) -> None:
        raw = json.dumps({"data_sources": {"tables": [{"identifier": "c.s.t"}]}})
        client = _space_client(raw)
        assert gs.inspect_space_tables(client, "sp-1") == ["c.s.t"]
        client.genie.get_space.assert_called_once_with("sp-1", include_serialized_space=True)

    def test_legacy_tables_attribute_still_supported(self) -> None:
        client = MagicMock()
        client.genie.get_space.return_value = SimpleNamespace(tables=["c.s.t"])
        assert gs.inspect_space_tables(client, "sp-1") == ["c.s.t"]

    def test_sdk_failure_returns_none(self) -> None:
        client = MagicMock()
        client.genie.get_space.side_effect = RuntimeError("403")
        assert gs.inspect_space_tables(client, "sp-1") is None


class TestPipelineAllowlist:
    def test_matches_expected_pipeline_tables(self) -> None:
        expected = list(gss.expected_tables("manufacturing", "zerobus"))
        client = _space_client(gss.build_serialized_space("manufacturing", "zerobus"))
        status = gs.space_table_allowlist_status(client, "sp-1", expected)
        assert status["status"] == "matched"
        assert status["authorization_enforced"] is True
        assert status["expected_tables"] == sorted(expected)

    def test_extra_table_is_mismatch(self) -> None:
        expected = list(gss.expected_tables("manufacturing", "realtime"))
        raw = json.dumps(
            {
                "data_sources": {
                    "tables": [{"identifier": t} for t in [*expected, "cat.sch.secret_payroll"]]
                }
            }
        )
        status = gs.space_table_allowlist_status(_space_client(raw), "sp-1", expected)
        assert status["status"] == "mismatch"
        assert status["authorization_enforced"] is False
        with pytest.raises(gs.GenieDisabledError):
            gs.require_table_allowlist(_space_client(raw), "sp-1", expected)

    def test_other_mode_tables_are_mismatch(self) -> None:
        expected = list(gss.expected_tables("manufacturing", "realtime"))
        client = _space_client(gss.build_serialized_space("manufacturing", "microbatch"))
        assert gs.space_table_allowlist_status(client, "sp-1", expected)["verified"] is False

    def test_static_default_still_intelligence_only(self, monkeypatch) -> None:
        monkeypatch.setenv("GENIE_SPACE_ID", "static")
        intel = gs.allowed_uc_tables("", "")
        raw = json.dumps({"data_sources": {"tables": [{"identifier": t} for t in intel]}})
        assert gs.space_table_allowlist_status(_space_client(raw))["status"] == "matched"


# ---------------------------------------------------------------------------
# Attachment serialization
# ---------------------------------------------------------------------------
class TestSerializeAttachments:
    def test_sdk_attachments(self) -> None:
        attachments = [
            SimpleNamespace(
                attachment_id="a1",
                text=SimpleNamespace(content="Downtime was 12 minutes."),
                query=None,
                suggested_questions=None,
            ),
            SimpleNamespace(
                attachment_id="a2",
                text=None,
                query=SimpleNamespace(
                    query="SELECT 1", description="Downtime by sensor", title="Downtime"
                ),
                suggested_questions=None,
            ),
            SimpleNamespace(
                attachment_id="a3",
                text=None,
                query=None,
                suggested_questions=SimpleNamespace(questions=["What about OEE?"]),
            ),
        ]
        assert gs._serialize_attachments(attachments) == [
            {
                "attachment_id": "a1",
                "text": "Downtime was 12 minutes.",
                "query": None,
                "suggested_questions": [],
            },
            {
                "attachment_id": "a2",
                "text": None,
                "query": {"sql": "SELECT 1", "description": "Downtime by sensor"},
                "suggested_questions": [],
            },
            {
                "attachment_id": "a3",
                "text": None,
                "query": None,
                "suggested_questions": ["What about OEE?"],
            },
        ]

    def test_dict_attachments_normalized(self) -> None:
        out = gs._serialize_attachments(
            [
                {
                    "attachment_id": "a1",
                    "text": {"content": "hi"},
                    "query": {"query": "SELECT 2", "description": "d"},
                    "suggested_questions": {"questions": ["q?"]},
                }
            ]
        )
        assert out == [
            {
                "attachment_id": "a1",
                "text": "hi",
                "query": {"sql": "SELECT 2", "description": "d"},
                "suggested_questions": ["q?"],
            }
        ]

    def test_empty(self) -> None:
        assert gs._serialize_attachments(None) == []
        assert gs._serialize_attachments([]) == []

    def test_serialize_message_exposes_attachments(self) -> None:
        message = SimpleNamespace(
            id="m1",
            conversation_id="c1",
            status=SimpleNamespace(value="COMPLETED"),
            content="q",
            error=None,
            attachments=[
                SimpleNamespace(
                    attachment_id="a1",
                    text=None,
                    query=SimpleNamespace(query="SELECT 1", description=None),
                    suggested_questions=None,
                )
            ],
        )
        out = gs.serialize_message(message, conversation_id="c1", message_id="m1")
        assert out["status"] == "COMPLETED"
        assert out["attachments"][0]["query"] == {"sql": "SELECT 1", "description": None}


# ---------------------------------------------------------------------------
# Pipeline-scoped SDK calls
# ---------------------------------------------------------------------------
def _authorized_client(mode: str = "realtime") -> MagicMock:
    client = _space_client(gss.build_serialized_space("manufacturing", mode))
    client.genie.start_conversation.return_value = SimpleNamespace(
        response=SimpleNamespace(conversation_id="c1", message_id="m1")
    )
    return client


class TestPipelineCalls:
    def test_start_conversation_uses_pipeline_space(self) -> None:
        expected = list(gss.expected_tables("manufacturing", "realtime"))
        client = _authorized_client()
        out = gs.start_conversation(
            "OEE?",
            {"industry": "manufacturing"},
            space_id="sp-p",
            client=client,
            expected_tables=expected,
        )
        assert out == {"conversation_id": "c1", "message_id": "m1"}
        assert client.genie.start_conversation.call_args.kwargs["space_id"] == "sp-p"
        content = client.genie.start_conversation.call_args.kwargs["content"]
        assert "[UNTRUSTED_OBSERVATIONS]" in content

    def test_attachment_result_capped(self) -> None:
        expected = list(gss.expected_tables("manufacturing", "realtime"))
        client = _authorized_client()
        manifest = SimpleNamespace(
            schema=SimpleNamespace(columns=[SimpleNamespace(name="n", type_text="INT")]),
            total_row_count=900,
            truncated=False,
        )
        client.genie.get_message_attachment_query_result.return_value = SimpleNamespace(
            statement_response=SimpleNamespace(
                manifest=manifest,
                result=SimpleNamespace(data_array=[[str(i)] for i in range(900)]),
            )
        )
        out = gs.get_attachment_query_result(
            "c1",
            "m1",
            "a1",
            space_id="sp-p",
            client=client,
            expected_tables=expected,
        )
        client.genie.get_message_attachment_query_result.assert_called_once_with(
            space_id="sp-p",
            conversation_id="c1",
            message_id="m1",
            attachment_id="a1",
        )
        assert out["columns"] == [{"name": "n", "type": "INT"}]
        assert len(out["rows"]) == 500
        assert out["truncated"] is True

    def test_attachment_result_blocked_on_mismatch(self) -> None:
        expected = list(gss.expected_tables("manufacturing", "realtime"))
        client = _authorized_client("microbatch")
        with pytest.raises(gs.GenieDisabledError):
            gs.get_attachment_query_result(
                "c1",
                "m1",
                "a1",
                space_id="sp-p",
                client=client,
                expected_tables=expected,
            )
        client.genie.get_message_attachment_query_result.assert_not_called()


# ---------------------------------------------------------------------------
# fetch_telemetry_summary warehouse resolution
# ---------------------------------------------------------------------------
class TestTelemetryWarehouse:
    @pytest.mark.asyncio
    async def test_resolves_warehouse_from_app_settings(self) -> None:
        db = MagicMock()
        db.fetchrow = AsyncMock(
            return_value={"settings": {"warehouse": {"warehouse_id": "wh-app"}}}
        )
        runner = AsyncMock(return_value={"data": [{"sensor_name": "s", "avg_value": 1.0}]})
        rows = await gs.fetch_telemetry_summary(
            "manufacturing",
            "realtime",
            ["s"],
            execute_sql=runner,
            db=db,
            db_available=True,
            user_email="u@x",
        )
        assert rows == [{"sensor_name": "s", "avg_value": 1.0}]
        assert runner.call_args.args[1] == "wh-app"

    @pytest.mark.asyncio
    async def test_env_override_still_wins(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATABRICKS_SQL_WAREHOUSE_ID", "wh-env")
        runner = AsyncMock(return_value={"data": []})
        await gs.fetch_telemetry_summary("manufacturing", "realtime", ["s"], execute_sql=runner)
        assert runner.call_args.args[1] == "wh-env"

    @pytest.mark.asyncio
    async def test_no_warehouse_skips_query(self) -> None:
        runner = AsyncMock()
        rows = await gs.fetch_telemetry_summary(
            "manufacturing",
            "realtime",
            ["s"],
            execute_sql=runner,
            db=None,
            db_available=False,
        )
        assert rows == []
        runner.assert_not_called()

    @pytest.mark.asyncio
    async def test_explicit_warehouse_wins(self) -> None:
        runner = AsyncMock(return_value={"data": []})
        await gs.fetch_telemetry_summary(
            "manufacturing",
            "realtime",
            ["s"],
            execute_sql=runner,
            warehouse_id="wh-x",
        )
        assert runner.call_args.args[1] == "wh-x"
