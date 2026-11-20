"""Live action plan against a real Foundation Model endpoint [live].

Skipped unless PHARMA_TWIN_LIVE_TESTS=1 and PHARMA_TWIN_LIVE_PROFILE names a
CLI profile. Read-only inference on a synthetic episode; nothing is created.
"""

from __future__ import annotations

import os

import pytest

LIVE = os.getenv("PHARMA_TWIN_LIVE_TESTS") == "1" and bool(os.getenv("PHARMA_TWIN_LIVE_PROFILE"))


@pytest.mark.skipif(not LIVE, reason="set PHARMA_TWIN_LIVE_TESTS=1 and PHARMA_TWIN_LIVE_PROFILE")
@pytest.mark.asyncio
async def test_live_plan_is_grounded() -> None:
    from databricks.sdk import WorkspaceClient

    from iot_twin.services.genai import action_plan as ap
    from iot_twin.services.genai.audit_store import GenaiAuditStore
    from iot_twin.services.genai.grounding import GroundingSources
    from tests._genai_fixtures import NOW, make_alert

    client = WorkspaceClient(profile=os.environ["PHARMA_TWIN_LIVE_PROFILE"])
    request = ap.PlanRequest(alert=make_alert(), requested_by="live@example.com", client=client)
    record = await ap.generate_action_plan(request, GroundingSources(), GenaiAuditStore(), now=NOW)
    assert record.status == "generated", record.grounding_reasons
    assert record.grounding_check == "pass", record.grounding_reasons
