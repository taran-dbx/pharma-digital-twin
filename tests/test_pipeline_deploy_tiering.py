"""Tests for the wizard-driven Lakebase-table provisioning route.

Verifies that ``sink_config.tiering`` persisted on the pipelines row flows
through to ``iot_twin.services.provisioning_service.provision_lakebase_table`` when a
pipeline is deployed via ``/api/provision/lakebase-table``.

These tests cover:
  - happy path: valid tiering is forwarded and ``tiering_applied`` surfaces
    in the response
  - pipeline without sink_config: tiering is NOT passed, service is not
    delegated to (legacy direct-asyncpg fallback)
  - malformed tiering (missing keys / wrong types): gracefully skipped
  - sse_metrics are echoed back in the response so the client can confirm
    what was persisted

Only newly introduced helpers in ``iot_twin.routes.provisioning`` and the top of the
``provision_lakebase_table`` route are exercised; the underlying service
function is mocked.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from iot_twin.core.state import AppState
from iot_twin.routes import provisioning as prov_route
from tests._fakes import fake_credential


# ---------------------------------------------------------------------------
# Helpers for building request-like objects & app state
# ---------------------------------------------------------------------------
def _make_request(body: dict) -> SimpleNamespace:
    """Build a minimal async request object that returns ``body`` from .json()."""

    async def _json() -> dict:
        return body

    return SimpleNamespace(json=_json)


def _make_state(sink_config: object | None) -> AppState:
    """Return an AppState whose db.fetchrow returns the given sink_config."""
    state = AppState()
    state.db_available = True
    db = MagicMock()
    if sink_config is _SENTINEL_NO_ROW:
        db.fetchrow = AsyncMock(return_value=None)
    else:
        db.fetchrow = AsyncMock(return_value={"sink_config": sink_config})
    db.execute = AsyncMock(return_value=None)
    state.db = db
    return state


_SENTINEL_NO_ROW = object()


# ---------------------------------------------------------------------------
# _extract_valid_tiering
# ---------------------------------------------------------------------------
class TestExtractValidTiering:
    def test_returns_clean_block_when_all_keys_present(self) -> None:
        tiering = prov_route._extract_valid_tiering(
            {"tiering": {"hot": "1 hour", "drop": "7 days", "chunk": "15 minutes"}}
        )
        assert tiering == {"hot": "1 hour", "drop": "7 days", "chunk": "15 minutes"}

    def test_accepts_missing_chunk(self) -> None:
        """chunk is optional — service applies a default."""
        tiering = prov_route._extract_valid_tiering(
            {"tiering": {"hot": "1 hour", "drop": "7 days"}}
        )
        assert tiering == {"hot": "1 hour", "drop": "7 days"}

    def test_none_when_sink_config_empty(self) -> None:
        assert prov_route._extract_valid_tiering({}) is None

    def test_none_when_tiering_missing(self) -> None:
        assert prov_route._extract_valid_tiering({"other": "x"}) is None

    def test_none_when_hot_missing(self) -> None:
        assert prov_route._extract_valid_tiering({"tiering": {"drop": "7 days"}}) is None

    def test_none_when_drop_missing(self) -> None:
        assert prov_route._extract_valid_tiering({"tiering": {"hot": "1 hour"}}) is None

    def test_none_when_hot_empty_string(self) -> None:
        assert prov_route._extract_valid_tiering({"tiering": {"hot": "", "drop": "7 days"}}) is None

    def test_none_when_tiering_is_not_dict(self) -> None:
        assert prov_route._extract_valid_tiering({"tiering": ["1 hour", "7 days"]}) is None

    def test_none_when_hot_not_string(self) -> None:
        assert (
            prov_route._extract_valid_tiering({"tiering": {"hot": 3600, "drop": "7 days"}}) is None
        )


# ---------------------------------------------------------------------------
# _extract_sse_metrics
# ---------------------------------------------------------------------------
class TestExtractSseMetrics:
    def test_returns_clean_string_list(self) -> None:
        assert prov_route._extract_sse_metrics({"sse_metrics": ["temp", "humidity"]}) == [
            "temp",
            "humidity",
        ]

    def test_empty_when_missing(self) -> None:
        assert prov_route._extract_sse_metrics({}) == []

    def test_empty_when_not_list(self) -> None:
        assert prov_route._extract_sse_metrics({"sse_metrics": "temp,humidity"}) == []

    def test_filters_non_strings(self) -> None:
        assert prov_route._extract_sse_metrics(
            {"sse_metrics": ["temp", 123, None, "humidity"]}
        ) == ["temp", "humidity"]


# ---------------------------------------------------------------------------
# _load_pipeline_sink_config
# ---------------------------------------------------------------------------
class TestLoadPipelineSinkConfig:
    @pytest.mark.asyncio
    async def test_returns_empty_when_pipeline_id_absent(self) -> None:
        state = _make_state(None)
        assert await prov_route._load_pipeline_sink_config(state, None) == {}

    @pytest.mark.asyncio
    async def test_returns_empty_when_db_unavailable(self) -> None:
        state = AppState()
        state.db_available = False
        state.db = None
        assert await prov_route._load_pipeline_sink_config(state, "pid") == {}

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_row(self) -> None:
        state = _make_state(_SENTINEL_NO_ROW)
        assert await prov_route._load_pipeline_sink_config(state, "pid") == {}

    @pytest.mark.asyncio
    async def test_parses_json_string(self) -> None:
        payload = {"tiering": {"hot": "1h", "drop": "7d"}}
        state = _make_state(json.dumps(payload))
        result = await prov_route._load_pipeline_sink_config(state, "pid")
        assert result == payload

    @pytest.mark.asyncio
    async def test_passes_through_dict(self) -> None:
        payload = {"tiering": {"hot": "1h", "drop": "7d"}}
        state = _make_state(payload)
        result = await prov_route._load_pipeline_sink_config(state, "pid")
        assert result == payload

    @pytest.mark.asyncio
    async def test_empty_on_malformed_json(self) -> None:
        state = _make_state("not-valid-json{")
        assert await prov_route._load_pipeline_sink_config(state, "pid") == {}

    @pytest.mark.asyncio
    async def test_empty_on_null_sink_config(self) -> None:
        state = _make_state(None)
        assert await prov_route._load_pipeline_sink_config(state, "pid") == {}

    @pytest.mark.asyncio
    async def test_empty_when_db_raises(self) -> None:
        state = AppState()
        state.db_available = True
        db = MagicMock()
        db.fetchrow = AsyncMock(side_effect=RuntimeError("connection lost"))
        state.db = db
        # Should not raise
        assert await prov_route._load_pipeline_sink_config(state, "pid") == {}


# ---------------------------------------------------------------------------
# provision_lakebase_table route — wiring of sink_config.tiering
# ---------------------------------------------------------------------------
class TestProvisionRouteForwardsTiering:
    """End-to-end route behaviour for the wizard deploy flow."""

    @pytest.mark.asyncio
    async def test_tiering_is_forwarded_to_service(self) -> None:
        """Pipeline with sink_config.tiering → service called with tiering block."""
        sink_config = {
            "tiering": {"hot": "1 hour", "drop": "7 days", "chunk": "15 minutes"},
            "sse_metrics": ["temperature", "pressure"],
        }
        state = _make_state(sink_config)

        request = _make_request(
            {
                "mode": "direct",
                "table_name": "sensor_readings",
                "host": "pg.example",
                "user": "u",
                "password": fake_credential("lakebase"),
                "port": 5432,
                "pipeline_id": "pipe-123",
            }
        )

        svc_mock = AsyncMock(
            return_value={
                "status": "created",
                "lakets_version": "0.1.0",
                "tiering_applied": {
                    "hot": "1 hour",
                    "drop": "7 days",
                    "chunk": "15 minutes",
                },
            }
        )

        with patch.object(
            prov_route.provisioning_service, "provision_lakebase_table", new=svc_mock
        ):
            response = await prov_route.provision_lakebase_table(
                request=request,
                user_email="u@test.com",
                state=state,
            )

        # Service was called, and received the full tiering block
        svc_mock.assert_awaited_once()
        lb_arg = svc_mock.await_args.args[0]
        assert lb_arg["tiering"] == {
            "hot": "1 hour",
            "drop": "7 days",
            "chunk": "15 minutes",
        }
        assert lb_arg["mode"] == "direct"
        assert lb_arg["pg_table"] == "sensor_readings"

        # Response carries tiering_applied + sse_metrics for client confirmation
        assert response["status"] == "created"
        assert response["tiering_applied"] == {
            "hot": "1 hour",
            "drop": "7 days",
            "chunk": "15 minutes",
        }
        assert response["sse_metrics"] == ["temperature", "pressure"]
        assert response["lakets_version"] == "0.1.0"

    @pytest.mark.asyncio
    async def test_missing_sink_config_skips_tiering(self) -> None:
        """Pipeline with no sink_config → service is NOT called, no crash."""
        state = _make_state(_SENTINEL_NO_ROW)

        request = _make_request(
            {
                "mode": "direct",
                "table_name": "sensor_readings",
                "host": "pg.example",
                "user": "u",
                "password": fake_credential("lakebase"),
                "port": 5432,
                "pipeline_id": "pipe-missing",
            }
        )

        svc_mock = AsyncMock()
        # Mock asyncpg so the fallback direct path completes cleanly
        conn = MagicMock()
        conn.execute = AsyncMock(return_value=None)
        conn.close = AsyncMock(return_value=None)

        with (
            patch.object(prov_route.provisioning_service, "provision_lakebase_table", new=svc_mock),
            patch("asyncpg.connect", new=AsyncMock(return_value=conn)),
        ):
            response = await prov_route.provision_lakebase_table(
                request=request,
                user_email="u@test.com",
                state=state,
            )

        svc_mock.assert_not_awaited()
        assert response["status"] == "created"
        assert response["table"] == "sensor_readings"
        # No sse_metrics surface when sink_config is absent
        assert "sse_metrics" not in response
        assert "tiering_applied" not in response

    @pytest.mark.asyncio
    async def test_malformed_tiering_skips_service(self) -> None:
        """Missing hot/drop keys → treated as no tiering, service not called."""
        # Missing "drop" makes tiering malformed; sse_metrics is still valid.
        sink_config = {
            "tiering": {"hot": "1 hour"},  # drop absent → invalid
            "sse_metrics": ["temperature"],
        }
        state = _make_state(sink_config)

        request = _make_request(
            {
                "mode": "direct",
                "table_name": "sensor_readings",
                "host": "pg.example",
                "user": "u",
                "password": fake_credential("lakebase"),
                "port": 5432,
                "pipeline_id": "pipe-malformed",
            }
        )

        svc_mock = AsyncMock()
        conn = MagicMock()
        conn.execute = AsyncMock(return_value=None)
        conn.close = AsyncMock(return_value=None)

        with (
            patch.object(prov_route.provisioning_service, "provision_lakebase_table", new=svc_mock),
            patch("asyncpg.connect", new=AsyncMock(return_value=conn)),
        ):
            response = await prov_route.provision_lakebase_table(
                request=request,
                user_email="u@test.com",
                state=state,
            )

        svc_mock.assert_not_awaited()
        assert response["status"] == "created"
        # valid sse_metrics still surface even though tiering was rejected
        assert response["sse_metrics"] == ["temperature"]
        assert "tiering_applied" not in response

    @pytest.mark.asyncio
    async def test_tiering_as_non_dict_skips_service(self) -> None:
        """tiering = [...] (wrong shape) → gracefully skipped."""
        state = _make_state({"tiering": ["hot", "drop"]})

        request = _make_request(
            {
                "mode": "direct",
                "table_name": "sensor_readings",
                "host": "pg.example",
                "user": "u",
                "password": fake_credential("lakebase"),
                "port": 5432,
                "pipeline_id": "pipe-bad-shape",
            }
        )

        svc_mock = AsyncMock()
        conn = MagicMock()
        conn.execute = AsyncMock(return_value=None)
        conn.close = AsyncMock(return_value=None)

        with (
            patch.object(prov_route.provisioning_service, "provision_lakebase_table", new=svc_mock),
            patch("asyncpg.connect", new=AsyncMock(return_value=conn)),
        ):
            response = await prov_route.provision_lakebase_table(
                request=request,
                user_email="u@test.com",
                state=state,
            )

        svc_mock.assert_not_awaited()
        assert response["status"] == "created"

    @pytest.mark.asyncio
    async def test_service_failure_surfaces_as_500(self) -> None:
        """If the service reports failure, the route returns 500 — not a mislead 200."""
        from fastapi.responses import JSONResponse

        sink_config = {"tiering": {"hot": "1 hour", "drop": "7 days"}}
        state = _make_state(sink_config)

        request = _make_request(
            {
                "mode": "direct",
                "table_name": "sensor_readings",
                "host": "pg.example",
                "user": "u",
                "password": fake_credential("lakebase"),
                "port": 5432,
                "pipeline_id": "pipe-x",
            }
        )

        svc_mock = AsyncMock(
            return_value={
                "status": "failed",
                "error": "install_lakets blew up",
            }
        )

        with patch.object(
            prov_route.provisioning_service, "provision_lakebase_table", new=svc_mock
        ):
            response = await prov_route.provision_lakebase_table(
                request=request,
                user_email="u@test.com",
                state=state,
            )

        assert isinstance(response, JSONResponse)
        assert response.status_code == 500

    @pytest.mark.asyncio
    async def test_autoscale_mode_forwards_project_and_branch(self) -> None:
        """Autoscale wizard deploy: project_id/branch_id go into the lb dict."""
        sink_config = {"tiering": {"hot": "1 hour", "drop": "7 days"}}
        state = _make_state(sink_config)

        request = _make_request(
            {
                "mode": "autoscale",
                "table_name": "sensor_readings",
                "project_id": "proj-1",
                "branch_id": "production",
                "pipeline_id": "pipe-123",
            }
        )

        svc_mock = AsyncMock(return_value={"status": "created", "lakets_version": "0.1.0"})

        with patch.object(
            prov_route.provisioning_service, "provision_lakebase_table", new=svc_mock
        ):
            response = await prov_route.provision_lakebase_table(
                request=request,
                user_email="u@test.com",
                state=state,
            )

        svc_mock.assert_awaited_once()
        lb_arg = svc_mock.await_args.args[0]
        assert lb_arg["mode"] == "autoscale"
        assert lb_arg["project_id"] == "proj-1"
        assert lb_arg["branch_id"] == "production"
        assert lb_arg["tiering"] == {"hot": "1 hour", "drop": "7 days"}
        assert response["status"] == "created"
