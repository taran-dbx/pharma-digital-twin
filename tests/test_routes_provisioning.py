"""Tests for routes/provisioning.py — pipeline-catalog workspace binding.

Verifies that after creating a per-pipeline UC catalog, the route explicitly
binds the new catalog READ_WRITE to the current workspace so it is always
accessible from this workspace (guarding against metastore isolation defaults
that otherwise leave a freshly-created catalog unbound). The binding must be
idempotent and non-fatal: a failure is swallowed/logged and never breaks
provisioning.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from iot_twin.core.state import AppState

WORKSPACE_ID = 1234567890123456


def _build_app() -> FastAPI:
    from iot_twin.routes.provisioning import router as provisioning_router

    app = FastAPI()
    app.include_router(provisioning_router)
    return app


def _state_no_existing_catalog() -> AppState:
    """AppState whose pipelines row has no provisioned catalog yet.

    Forces ``provision_pipeline_catalog`` past its idempotent early-return so
    catalog creation (and the subsequent workspace binding) actually runs.
    """

    async def _fetchrow(query: str, *args, **kwargs):
        # The route issues two distinct fetchrow queries: the pipeline
        # resources lookup (idempotency guard) and the user_settings lookup.
        if "user_settings" in query:
            return {"settings": "{}"}
        return {"resources": "{}"}

    db = MagicMock()
    db.fetchrow = AsyncMock(side_effect=_fetchrow)
    db.execute = AsyncMock(return_value=None)
    db.fetchval = AsyncMock(return_value=None)
    state = AppState()
    state.db = db
    state.db_available = True
    state.db_error = None
    return state


def _mock_ws_client() -> MagicMock:
    """WorkspaceClient mock with catalog/schema/volume + workspace bindings wired."""
    w = MagicMock()
    w.catalogs.create = MagicMock(return_value=None)
    w.schemas.create = MagicMock(return_value=None)
    w.volumes.create = MagicMock(return_value=None)
    w.grants.update = MagicMock(return_value=None)
    w.get_workspace_id = MagicMock(return_value=WORKSPACE_ID)
    w.workspace_bindings.update_bindings = MagicMock(return_value=None)
    return w


def _set_state(app: FastAPI, state: AppState) -> None:
    app.state.app_state = state


async def _post_provision(app: FastAPI, body: dict) -> object:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.post("/api/provision/pipeline-catalog", json=body)


class TestPipelineCatalogWorkspaceBinding:
    """POST /api/provision/pipeline-catalog binds the new catalog to this workspace."""

    @pytest.mark.asyncio
    async def test_binds_new_catalog_read_write_to_current_workspace(self) -> None:
        from databricks.sdk.service.catalog import WorkspaceBindingBindingType

        w = _mock_ws_client()
        app = _build_app()
        _set_state(app, _state_no_existing_catalog())

        with patch("databricks.sdk.WorkspaceClient", return_value=w):
            resp = await _post_provision(
                app,
                {"pipeline_id": "abc-123-def-456", "pipeline_name": "Sensors"},
            )

        assert resp.status_code == 200
        body = resp.json()
        catalog_name = body["catalog"]

        # The binding call must target the just-created catalog only.
        w.workspace_bindings.update_bindings.assert_called_once()
        _, kwargs = w.workspace_bindings.update_bindings.call_args
        assert kwargs["securable_type"] == "catalog"
        assert kwargs["securable_name"] == catalog_name
        added = kwargs["add"]
        assert len(added) == 1
        binding = added[0]
        assert binding.workspace_id == WORKSPACE_ID
        assert binding.binding_type == WorkspaceBindingBindingType.BINDING_TYPE_READ_WRITE

        # And the response advertises the binding result.
        entry = body["workspace_binding"]
        assert entry["status"] == "bound"
        assert catalog_name in entry["resource"]

    @pytest.mark.asyncio
    async def test_binding_failure_is_swallowed_and_marked_skipped(self) -> None:
        w = _mock_ws_client()
        w.workspace_bindings.update_bindings = MagicMock(
            side_effect=RuntimeError("metastore admin required")
        )
        app = _build_app()
        _set_state(app, _state_no_existing_catalog())

        with patch("databricks.sdk.WorkspaceClient", return_value=w):
            resp = await _post_provision(
                app,
                {"pipeline_id": "abc-123-def-456", "pipeline_name": "Sensors"},
            )

        # Non-fatal: provisioning still succeeds.
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "created"
        entry = body["workspace_binding"]
        assert entry["status"] == "skipped"

    @pytest.mark.asyncio
    async def test_workspace_id_falls_back_to_env(self, monkeypatch) -> None:
        w = _mock_ws_client()
        # SDK method missing entirely -> fall back to env DATABRICKS_WORKSPACE_ID.
        del w.get_workspace_id
        monkeypatch.setenv("DATABRICKS_WORKSPACE_ID", str(WORKSPACE_ID))
        app = _build_app()
        _set_state(app, _state_no_existing_catalog())

        with patch("databricks.sdk.WorkspaceClient", return_value=w):
            resp = await _post_provision(
                app,
                {"pipeline_id": "abc-123-def-456", "pipeline_name": "Sensors"},
            )

        assert resp.status_code == 200
        w.workspace_bindings.update_bindings.assert_called_once()
        _, kwargs = w.workspace_bindings.update_bindings.call_args
        assert kwargs["add"][0].workspace_id == WORKSPACE_ID


ZEROBUS_APP_ID = "11111111-2222-3333-4444-555555555555"


class TestZerobusServicePrincipalGrants:
    """POST /api/provision/pipeline-catalog grants the Zerobus SP UC privileges.

    The bug: grants were issued with ``securable_type=SecurableType.SCHEMA``
    (the enum), which the SDK interpolates verbatim into the REST path
    (``/permissions/SecurableType.SCHEMA/...``), producing a malformed request
    that threw and was silently swallowed. The grants must:
      * be issued with the lowercase string securable type the API expects
        (``"catalog"`` / ``"schema"``),
      * target the SP's application_id as the principal,
      * carry the right privileges (USE_CATALOG on the catalog; USE_SCHEMA,
        MODIFY, SELECT, CREATE_TABLE on each schema),
      * surface their outcome in the response, and
      * report failures (not swallow them) while staying non-fatal.
    """

    @pytest.mark.asyncio
    async def test_grants_use_lowercase_securable_type_and_app_id(self, monkeypatch) -> None:
        from databricks.sdk.service.catalog import Privilege

        monkeypatch.setenv("ZEROBUS_CLIENT_ID", ZEROBUS_APP_ID)
        w = _mock_ws_client()
        app = _build_app()
        _set_state(app, _state_no_existing_catalog())

        with patch("databricks.sdk.WorkspaceClient", return_value=w):
            resp = await _post_provision(
                app,
                {"pipeline_id": "abc-123-def-456", "pipeline_name": "Sensors"},
            )

        assert resp.status_code == 200
        body = resp.json()
        catalog_name = body["catalog"]

        # Collect every grants.update invocation keyed by securable name.
        calls = w.grants.update.call_args_list
        assert calls, "no grants.update calls were made for the Zerobus SP"

        securable_types: set[str] = set()
        catalog_call = None
        schema_calls = {}
        for _args, kwargs in calls:
            st = kwargs["securable_type"]
            # MUST be a plain lowercase string, never the enum / its repr.
            assert isinstance(st, str), f"securable_type must be str, got {type(st)}"
            assert st == st.lower(), f"securable_type must be lowercase, got {st!r}"
            assert "SecurableType" not in st
            securable_types.add(st)
            full_name = kwargs["full_name"]
            if st == "catalog":
                catalog_call = kwargs
            elif st == "schema":
                schema_calls[full_name] = kwargs

        assert "catalog" in securable_types
        assert "schema" in securable_types

        # Catalog grant: USE_CATALOG to the SP app id.
        cat_change = catalog_call["changes"][0]
        assert cat_change.principal == ZEROBUS_APP_ID
        assert Privilege.USE_CATALOG in cat_change.add

        # Raw schema grant: write privileges to the SP app id.
        raw_full = f"{catalog_name}.raw"
        assert raw_full in schema_calls
        raw_change = schema_calls[raw_full]["changes"][0]
        assert raw_change.principal == ZEROBUS_APP_ID
        for priv in (
            Privilege.USE_SCHEMA,
            Privilege.MODIFY,
            Privilege.SELECT,
            Privilege.CREATE_TABLE,
        ):
            assert priv in raw_change.add

    @pytest.mark.asyncio
    async def test_grants_outcome_is_surfaced_in_response(self, monkeypatch) -> None:
        monkeypatch.setenv("ZEROBUS_CLIENT_ID", ZEROBUS_APP_ID)
        w = _mock_ws_client()
        app = _build_app()
        _set_state(app, _state_no_existing_catalog())

        with patch("databricks.sdk.WorkspaceClient", return_value=w):
            resp = await _post_provision(
                app,
                {"pipeline_id": "abc-123-def-456", "pipeline_name": "Sensors"},
            )

        assert resp.status_code == 200
        body = resp.json()
        grants = body["grants"]
        assert isinstance(grants, list) and grants
        # Every entry succeeded and names the SP it granted to.
        for entry in grants:
            assert entry["status"] == "created"
            assert entry["principal"] == ZEROBUS_APP_ID
        statuses = {e["status"] for e in grants}
        assert statuses == {"created"}

    @pytest.mark.asyncio
    async def test_grant_failure_is_reported_not_swallowed(self, monkeypatch) -> None:
        monkeypatch.setenv("ZEROBUS_CLIENT_ID", ZEROBUS_APP_ID)
        w = _mock_ws_client()
        w.grants.update = MagicMock(
            side_effect=RuntimeError("PERMISSION_DENIED: not catalog owner")
        )
        app = _build_app()
        _set_state(app, _state_no_existing_catalog())

        with patch("databricks.sdk.WorkspaceClient", return_value=w):
            resp = await _post_provision(
                app,
                {"pipeline_id": "abc-123-def-456", "pipeline_name": "Sensors"},
            )

        # Non-fatal: provisioning still returns 200.
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "created"

        grants = body["grants"]
        assert isinstance(grants, list) and grants
        # The failure is visible, not silently swallowed.
        failed = [e for e in grants if e["status"] == "failed"]
        assert failed, "grant failure was swallowed instead of surfaced"
        assert all("error" in e and e["error"] for e in failed)
        assert any("PERMISSION_DENIED" in e["error"] for e in failed)

    @pytest.mark.asyncio
    async def test_no_grants_when_zerobus_client_id_unset(self, monkeypatch) -> None:
        monkeypatch.delenv("ZEROBUS_CLIENT_ID", raising=False)
        w = _mock_ws_client()
        app = _build_app()
        _set_state(app, _state_no_existing_catalog())

        with patch("databricks.sdk.WorkspaceClient", return_value=w):
            resp = await _post_provision(
                app,
                {"pipeline_id": "abc-123-def-456", "pipeline_name": "Sensors"},
            )

        assert resp.status_code == 200
        body = resp.json()
        w.grants.update.assert_not_called()
        assert body.get("grants", []) == []
