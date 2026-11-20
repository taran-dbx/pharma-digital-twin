"""Tests for services/app_permissions.py — granting CAN_USE to a streaming SP."""

from __future__ import annotations

from unittest.mock import MagicMock

from iot_twin.services.app_permissions import (
    DEFAULT_APP_NAME,
    _app_name,
    grant_app_can_use,
)


class TestAppName:
    def test_reads_env_var(self, monkeypatch) -> None:
        monkeypatch.setenv("DATABRICKS_APP_NAME", "my-custom-app")
        assert _app_name() == "my-custom-app"

    def test_falls_back_to_constant(self, monkeypatch) -> None:
        monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)
        assert _app_name() == DEFAULT_APP_NAME
        assert DEFAULT_APP_NAME == "iot-digital-twin"

    def test_blank_env_var_falls_back(self, monkeypatch) -> None:
        monkeypatch.setenv("DATABRICKS_APP_NAME", "   ")
        assert _app_name() == DEFAULT_APP_NAME


class TestGrantAppCanUse:
    def test_calls_update_permissions_with_can_use(self) -> None:
        w = MagicMock()
        w.apps = MagicMock()
        w.apps.update_permissions = MagicMock(return_value=None)

        grant_app_can_use(w, "iot-digital-twin", "sp-app-id-123")

        w.apps.update_permissions.assert_called_once()
        call = w.apps.update_permissions.call_args
        assert call.kwargs["app_name"] == "iot-digital-twin"
        acl = call.kwargs["access_control_list"]
        assert len(acl) == 1
        entry = acl[0]
        # The ACL entry targets the SP application id with CAN_USE.
        from databricks.sdk.service.apps import AppPermissionLevel

        assert entry.service_principal_name == "sp-app-id-123"
        assert entry.permission_level == AppPermissionLevel.CAN_USE

    def test_failure_is_swallowed_and_logged(self, caplog) -> None:
        w = MagicMock()
        w.apps = MagicMock()
        w.apps.update_permissions = MagicMock(side_effect=RuntimeError("403 forbidden"))

        # Must not raise — failure is non-fatal.
        with caplog.at_level("WARNING"):
            grant_app_can_use(w, "iot-digital-twin", "sp-app-id-123")

        assert any(
            "403 forbidden" in r.getMessage() or "app" in r.getMessage().lower()
            for r in caplog.records
        )

    def test_missing_apps_api_does_not_raise(self) -> None:
        # Older SDKs / odd clients: w.apps may raise on access — still non-fatal.
        w = MagicMock()
        type(w).apps = property(lambda self: (_ for _ in ()).throw(AttributeError("no apps")))
        grant_app_can_use(w, "iot-digital-twin", "sp-app-id-123")


class TestIsAdmin:
    def test_is_admin_true_for_group_member(self, monkeypatch) -> None:
        from iot_twin.services import app_permissions as ap

        monkeypatch.setenv("ADMIN_GROUP", "iot-admins")
        monkeypatch.setattr(ap, "_user_groups", lambda req: {"iot-admins", "users"})
        assert ap.is_admin(object()) is True

    def test_is_admin_false_for_non_member(self, monkeypatch) -> None:
        from iot_twin.services import app_permissions as ap

        monkeypatch.setenv("ADMIN_GROUP", "iot-admins")
        monkeypatch.setattr(ap, "_user_groups", lambda req: {"users"})
        assert ap.is_admin(object()) is False
