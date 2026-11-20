"""Tests for the UC-secret read/write helper (Task 3).

The app SP's own OAuth secret is stored as a Unity Catalog secret so the
Zerobus producer notebooks can read it at runtime.  These tests mock the native
SDK ``secrets_uc`` surface (``ws.secrets_uc.create_secret`` /
``update_secret`` / ``get_secret``) rather than the raw REST api-client.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from iot_twin.services import uc_secrets


def _make_ws(*, get_value: str | None = ""):
    """Return a mocked WorkspaceClient with secrets_uc wired."""
    ws = MagicMock()
    # create_secret and update_secret return a mock Secret by default
    ws.secrets_uc.create_secret = MagicMock(return_value=MagicMock())
    ws.secrets_uc.update_secret = MagicMock(return_value=MagicMock())
    # get_secret returns a Secret whose effective_value is configurable
    secret_result = MagicMock()
    secret_result.effective_value = get_value
    ws.secrets_uc.get_secret = MagicMock(return_value=secret_result)
    return ws


class TestPutSecret:
    @pytest.mark.asyncio
    async def test_creates_secret_via_native_sdk(self) -> None:
        ws = _make_ws()
        res = await uc_secrets.put_secret(
            "example_eu_catalog",
            "rtm_lakets_zb",
            "app_sp_oauth",
            "top-secret",
            ws=ws,
        )
        ws.secrets_uc.create_secret.assert_called_once()
        secret_arg = ws.secrets_uc.create_secret.call_args.args[0]
        assert secret_arg.catalog_name == "example_eu_catalog"
        assert secret_arg.schema_name == "rtm_lakets_zb"
        assert secret_arg.name == "app_sp_oauth"
        assert secret_arg.value == "top-secret"
        assert res["status"] in ("created", "updated", "ok")

    @pytest.mark.asyncio
    async def test_already_exists_falls_back_to_update(self) -> None:
        ws = _make_ws()
        # create raises "already exists", the update then succeeds
        ws.secrets_uc.create_secret.side_effect = Exception("secret already exists")
        res = await uc_secrets.put_secret(
            "cat",
            "sch",
            "app_sp_oauth",
            "v2",
            ws=ws,
        )
        ws.secrets_uc.create_secret.assert_called_once()
        ws.secrets_uc.update_secret.assert_called_once()
        update_call = ws.secrets_uc.update_secret.call_args
        # first positional arg is the full name
        assert "cat.sch.app_sp_oauth" in update_call.args[0]
        assert res["status"] in ("updated", "created", "ok")

    @pytest.mark.asyncio
    async def test_update_path_failure_is_not_fatal(self) -> None:
        # Both the create (already-exists) and the update raise; the helper must
        # return status="failed" rather than propagating, so a teardown→re-provision
        # re-run never hard-fails provisioning.
        ws = _make_ws()
        ws.secrets_uc.create_secret.side_effect = Exception("secret already exists")
        ws.secrets_uc.update_secret.side_effect = Exception(
            "PERMISSION_DENIED: cannot update secret"
        )
        res = await uc_secrets.put_secret("cat", "sch", "app_sp_oauth", "v3", ws=ws)
        assert res["status"] == "failed"
        assert "PERMISSION_DENIED" in res["error"]


class TestGetSecret:
    @pytest.mark.asyncio
    async def test_returns_effective_value_via_native_sdk(self) -> None:
        ws = _make_ws(get_value="the-secret")
        val = await uc_secrets.get_secret("cat", "sch", "app_sp_oauth", ws=ws)
        ws.secrets_uc.get_secret.assert_called_once_with("cat.sch.app_sp_oauth", include_value=True)
        assert val == "the-secret"

    @pytest.mark.asyncio
    async def test_returns_empty_string_when_effective_value_absent(self) -> None:
        ws = _make_ws(get_value=None)
        val = await uc_secrets.get_secret("cat", "sch", "app_sp_oauth", ws=ws)
        assert val == ""
