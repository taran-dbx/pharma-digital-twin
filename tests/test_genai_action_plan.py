"""Foundation Model call and the audited action-plan flow, with a mocked endpoint [qa]."""

from __future__ import annotations

import time

import pytest
from databricks.sdk import errors as sdk_errors

from iot_twin.services.genai import action_plan as ap
from iot_twin.services.genai import fm_client
from iot_twin.services.genai import grounding as g
from iot_twin.services.genai.audit_store import GenaiAuditStore
from tests._genai_fixtures import NOW, fake_client, make_alert, plan_text, reply


async def _no_sleep(_s: float) -> None:
    return None


@pytest.mark.asyncio
class TestFmClient:
    async def test_success_returns_text_model_and_usage(self) -> None:
        client = fake_client(reply('{"ok": 1}'))
        result = await fm_client.query_chat(
            ({"role": "system", "content": "s"}, {"role": "user", "content": "u"}),
            endpoint="databricks-claude-haiku-5-5",
            client=client,
        )
        assert result.text == '{"ok": 1}'
        assert result.usage == {
            "prompt_tokens": 1200,
            "completion_tokens": 400,
            "total_tokens": 1600,
        }
        call = client.serving_endpoints.calls[0]
        assert call["name"] == "databricks-claude-haiku-5-5"
        assert [m.role.value for m in call["messages"]] == ["system", "user"]
        assert "temperature" not in call  # Claude 5.x endpoints reject it

    async def test_endpoint_defaults_and_env_override(self, monkeypatch) -> None:
        monkeypatch.delenv(fm_client.ENDPOINT_ENV, raising=False)
        assert fm_client.endpoint_name() == "databricks-claude-sonnet-5-5"
        monkeypatch.setenv(fm_client.ENDPOINT_ENV, "databricks-meta-llama-3-3-70b-instruct")
        assert fm_client.endpoint_name() == "databricks-meta-llama-3-3-70b-instruct"

    async def test_transient_errors_are_retried(self) -> None:
        client = fake_client(sdk_errors.TooManyRequests("slow down"), reply("{}"))
        result = await fm_client.query_chat((), client=client, retries=2, sleep=_no_sleep)
        assert result.attempts == 2

    async def test_timeout_is_bounded_and_retried(self) -> None:
        def slow():
            time.sleep(0.3)
            return reply("{}")

        client = fake_client(slow)
        with pytest.raises(fm_client.ModelCallError) as info:
            await fm_client.query_chat(
                (), client=client, timeout_s=0.05, retries=1, sleep=_no_sleep
            )
        assert info.value.kind == "timeout" and info.value.attempts == 2
        assert len(client.serving_endpoints.calls) == 2

    async def test_permission_errors_are_not_retried(self) -> None:
        client = fake_client(sdk_errors.PermissionDenied("no CAN_QUERY"))
        with pytest.raises(fm_client.ModelCallError) as info:
            await fm_client.query_chat((), client=client, retries=3, sleep=_no_sleep)
        assert info.value.kind == "endpoint" and info.value.attempts == 1

    async def test_describe_endpoint_reads_the_served_model_once(self, monkeypatch) -> None:
        from types import SimpleNamespace

        monkeypatch.setattr(fm_client, "_described", {})
        foundation = SimpleNamespace(
            name="system.ai.databricks-claude-sonnet-5-5", display_name="Claude Sonnet 5.5"
        )
        endpoint = SimpleNamespace(
            config=SimpleNamespace(
                served_entities=[SimpleNamespace(foundation_model=foundation, entity_version=None)]
            )
        )
        calls = []
        client = SimpleNamespace(
            serving_endpoints=SimpleNamespace(get=lambda name: calls.append(name) or endpoint)
        )
        expected = ("system.ai.databricks-claude-sonnet-5-5", "Claude Sonnet 5.5")
        assert await fm_client.describe_endpoint("ep", client) == expected
        assert await fm_client.describe_endpoint("ep", client) == expected
        assert calls == ["ep"]
        broken = SimpleNamespace(serving_endpoints=SimpleNamespace())
        assert await fm_client.describe_endpoint("other", broken) == (None, None)

    async def test_list_content_is_joined(self) -> None:
        answer = reply("")
        answer.choices[0].message.content = [{"type": "text", "text": "{"}, {"text": "}"}]
        result = await fm_client.query_chat((), client=fake_client(answer))
        assert result.text == "{}"


@pytest.mark.asyncio
class TestGenerate:
    async def _run(self, client, alert=None, store=None):
        alert = alert or make_alert()
        store = store or GenaiAuditStore()
        request = ap.PlanRequest(
            alert=alert, requested_by="qa@example.com", client=client, retries=0
        )
        record = await ap.generate_action_plan(request, g.GroundingSources(), store, now=NOW)
        return record, store

    async def test_success_is_audited_with_the_exact_grounding(self) -> None:
        alert = make_alert()
        grounding = await g.assemble_grounding(alert, g.GroundingSources(), NOW)
        record, store = await self._run(fake_client(reply(plan_text(grounding))), alert)
        assert record.status == "generated" and record.grounding_check == "pass"
        assert record.grounding_rows == grounding.rows()
        assert record.grounding_hash == grounding.digest()
        assert record.endpoint == "databricks-claude-sonnet-5-5"
        assert record.model == "claude-sonnet-5-5"
        assert record.model_version == "databricks-claude-sonnet-5-5"
        assert record.total_tokens == 1600 and record.latency_ms is not None
        assert record.plan["steps"][0]["response_action"] == "quarantine_lot"
        assert (await store.get(record.id)) == record
        assert record.summary()["advisory"] is True

    async def test_malformed_json_is_rejected_but_logged(self) -> None:
        record, store = await self._run(fake_client(reply("Sure! Here is the plan: …")))
        assert record.status == "rejected" and record.grounding_check == "fail"
        assert record.plan is None
        assert any("malformed JSON" in r for r in record.grounding_reasons)
        assert record.response["raw_text"].startswith("Sure!")
        assert await store.get(record.id) is not None

    async def test_ungrounded_plan_is_flagged(self) -> None:
        alert = make_alert()
        grounding = await g.assemble_grounding(alert, g.GroundingSources(), NOW)
        text = plan_text(
            grounding, summary={"text": "Peak 99.9 °C.", "citations": ["alert:made-up"]}
        )
        record, _ = await self._run(fake_client(reply(text)), alert)
        assert record.status == "generated" and record.grounding_check == "fail"
        assert any("alert:made-up" in r for r in record.grounding_reasons)
        assert any("99.9" in r for r in record.grounding_reasons)

    async def test_endpoint_timeout_is_audited_then_raised(self, monkeypatch) -> None:
        async def timeout(*_a, **_k):
            raise fm_client.ModelCallError("x: timed out after 1s", kind="timeout", attempts=3)

        monkeypatch.setattr(fm_client, "query_chat", timeout)
        store = GenaiAuditStore()
        with pytest.raises(ap.ActionPlanModelError) as info:
            await self._run(None, store=store)
        record = info.value.record
        assert info.value.kind == "timeout"
        assert record.status == "error" and record.grounding_check == "fail"
        assert record.response == {"plan": None, "attempts": 3}
        assert await store.get(record.id) == record

    async def test_plan_never_touches_alerts_or_responses(self) -> None:
        alert = make_alert()
        before = alert.to_dict()
        grounding = await g.assemble_grounding(alert, g.GroundingSources(), NOW)
        await self._run(fake_client(reply(plan_text(grounding))), alert)
        assert alert.to_dict() == before


def test_readings_query_targets_the_mode_table(monkeypatch) -> None:
    monkeypatch.setenv("UC_CATALOG", "cat")
    monkeypatch.setenv("UC_SCHEMA", "sch")
    sql = ap.readings_query(make_alert(), "microbatch", NOW)
    assert "FROM cat.sch.manufacturing_microbatch WHERE" in sql
    assert "sensor_name = 'cnc_1_spindle_temp'" in sql
    assert ap.readings_query(make_alert(), None, NOW) is None
