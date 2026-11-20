"""Tests for routes/settings.py — focused on idempotent app-access granting
during secret-scope provisioning (`POST /api/settings/provision/secrets`).

The streaming service principal must be granted ``CAN_USE`` on the app whether
it was freshly created OR already existed (creds present in the scope). These
tests assert ``grant_app_can_use`` is invoked on BOTH paths.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from tests._fakes import fake_credential


def _build_app() -> FastAPI:
    from iot_twin.routes.settings import router as settings_router

    app = FastAPI()
    app.include_router(settings_router)
    return app


def _secret(key: str) -> MagicMock:
    s = MagicMock()
    s.key = key
    return s


def _make_ws(existing_keys: list[str], sp_app_id: str | None = None) -> MagicMock:
    """Build a WorkspaceClient mock.

    ``existing_keys`` controls what ``secrets.list_secrets`` returns (driving the
    already-exists vs. fresh-create branch). ``sp_app_id``, when provided, is the
    application_id returned by ``service_principals.list`` (already-exists path).
    """
    w = MagicMock()
    w.secrets.list_secrets.return_value = [_secret(k) for k in existing_keys]
    w.secrets.put_secret.return_value = None

    # Fresh-create path wiring
    created_sp = MagicMock()
    created_sp.id = "12345"
    created_sp.application_id = "fresh-app-id"
    w.service_principals.create.return_value = created_sp
    w.api_client.do.return_value = {"secret": fake_credential("oauth")}

    # Already-exists path wiring: list SP by displayName
    if sp_app_id is not None:
        listed_sp = MagicMock()
        listed_sp.application_id = sp_app_id
        w.service_principals.list.return_value = [listed_sp]
    else:
        w.service_principals.list.return_value = []
    return w


async def _post_provision_secrets(w: MagicMock, scope: str = "iot_scope") -> dict[str, Any]:
    app = _build_app()
    transport = ASGITransport(app=app)
    body = {"secrets": {"scope": scope}}
    with (
        patch(
            "iot_twin.routes.settings.provision_secrets_scope",
            AsyncMock(return_value={"resource": "Secret scope", "status": "created"}),
        ),
        patch(
            "iot_twin.routes.settings.validate_secrets", AsyncMock(return_value={"status": "pass"})
        ),
        patch("databricks.sdk.WorkspaceClient", return_value=w),
        patch("iot_twin.routes.settings.grant_app_can_use") as grant_mock,
        patch("iot_twin.routes.settings._app_name", return_value="iot-digital-twin"),
    ):
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post("/api/settings/provision/secrets", json=body)
        return {"resp": resp, "grant": grant_mock}


class TestProvisionSecretsAppGrant:
    @pytest.mark.asyncio
    async def test_grant_called_on_fresh_sp_creation(self) -> None:
        """When the SP is freshly created, grant_app_can_use is invoked with
        the newly minted application_id."""
        w = _make_ws(existing_keys=[])  # no creds yet -> create branch
        out = await _post_provision_secrets(w)

        assert out["resp"].status_code == 200
        out["grant"].assert_called_once()
        args = out["grant"].call_args.args
        # (w, app_name, application_id)
        assert args[0] is w
        assert args[1] == "iot-digital-twin"
        assert args[2] == "fresh-app-id"

        provisioned = out["resp"].json()["provisioned"]
        assert any(
            p.get("resource") == "App access (CAN_USE) for SP" and p.get("status") == "granted"
            for p in provisioned
        )

    @pytest.mark.asyncio
    async def test_grant_called_on_already_exists_path(self) -> None:
        """When creds already exist, the SP is looked up by displayName and the
        app grant is re-applied (idempotent)."""
        w = _make_ws(
            existing_keys=["zerobus-client-id", "zerobus-client-secret"],
            sp_app_id="existing-app-id",
        )
        out = await _post_provision_secrets(w, scope="iot_scope")

        assert out["resp"].status_code == 200
        # SP must NOT be created on the already-exists path
        w.service_principals.create.assert_not_called()
        # SP looked up by displayName matching the create-call naming scheme
        w.service_principals.list.assert_called_once()
        _, kwargs = w.service_principals.list.call_args
        filt = kwargs.get("filter", "")
        assert "iot-sss-iot_scope" in filt

        out["grant"].assert_called_once()
        args = out["grant"].call_args.args
        assert args[0] is w
        assert args[1] == "iot-digital-twin"
        assert args[2] == "existing-app-id"

        provisioned = out["resp"].json()["provisioned"]
        assert any(
            p.get("resource") == "App access (CAN_USE) for SP"
            and p.get("status") in ("granted", "already")
            for p in provisioned
        )

    @pytest.mark.asyncio
    async def test_already_exists_sp_not_found_does_not_grant(self) -> None:
        """If the SP can't be resolved on the already-exists path, no grant is
        attempted and the flow stays healthy."""
        w = _make_ws(
            existing_keys=["zerobus-client-id", "zerobus-client-secret"],
            sp_app_id=None,  # list returns []
        )
        out = await _post_provision_secrets(w)

        assert out["resp"].status_code == 200
        out["grant"].assert_not_called()
