"""Tests for provisioning_service.provision_lakebase_role (OAuth-native flow)
and lakets_service.install_lakets_if_missing.

These cover the post-rewrite contract where the service principal itself is
registered as a Lakebase Postgres role authenticated via LAKEBASE_OAUTH_V1,
and LakeTS can be installed idempotently on an endpoint.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from iot_twin.services import provisioning_service
from iot_twin.services.lakets import lakets_service
from tests._fakes import fake_credential

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


SP_CLIENT_ID = "00000000-0000-0000-0000-000000000000"
PROJECT_ID = "iot-demo"
BRANCH_ID = "production"
ENDPOINT_NAME = f"projects/{PROJECT_ID}/branches/{BRANCH_ID}/endpoints/primary"


def _mock_ws_client() -> MagicMock:
    """Return a WorkspaceClient mock with postgres + current_user wired."""
    w = MagicMock()
    w.current_user = MagicMock()
    w.current_user.me = MagicMock(return_value=MagicMock(user_name="app@sp.example.com"))
    w.postgres = MagicMock()
    endpoint = MagicMock()
    endpoint.status.hosts.host = "pg.example.com"
    w.postgres.get_endpoint = MagicMock(return_value=endpoint)
    w.postgres.generate_database_credential = MagicMock(
        return_value=MagicMock(token=fake_credential("token"))
    )
    w.postgres.create_role = MagicMock(return_value=None)
    w.postgres.list_endpoints = MagicMock(return_value=[MagicMock(name=ENDPOINT_NAME)])
    return w


def _stub_conn() -> MagicMock:
    conn = MagicMock()
    conn.execute = AsyncMock(return_value=None)
    conn.fetchval = AsyncMock(return_value=None)
    conn.close = AsyncMock(return_value=None)
    return conn


# ---------------------------------------------------------------------------
# provision_lakebase_role
# ---------------------------------------------------------------------------


class TestProvisionLakebaseRoleOAuth:
    """New OAuth-native contract — SP is registered as a PG role."""

    @pytest.mark.asyncio
    async def test_provision_registers_sp_role_as_oauth(self) -> None:
        """create_role must be called with LAKEBASE_OAUTH_V1 + SERVICE_PRINCIPAL."""
        w = _mock_ws_client()
        conn = _stub_conn()

        with (
            patch(
                "databricks.sdk.WorkspaceClient",
                return_value=w,
            ),
            patch(
                "asyncpg.connect",
                new=AsyncMock(return_value=conn),
            ),
        ):
            results = await provisioning_service.provision_lakebase_role(
                sp_client_id=SP_CLIENT_ID,
                project_id=PROJECT_ID,
                branch_id=BRANCH_ID,
                database="databricks_postgres",
                endpoint_name=ENDPOINT_NAME,
                sink_tables=("sensor_readings",),
            )

        # create_role invoked once with the OAuth auth method + SP identity.
        assert w.postgres.create_role.call_count == 1
        call_kwargs = w.postgres.create_role.call_args.kwargs
        role_spec = call_kwargs["role"].spec

        from databricks.sdk.service.postgres import (
            RoleAuthMethod,
            RoleIdentityType,
        )

        assert role_spec.auth_method == RoleAuthMethod.LAKEBASE_OAUTH_V1
        assert role_spec.identity_type == RoleIdentityType.SERVICE_PRINCIPAL
        assert role_spec.postgres_role == SP_CLIENT_ID

        # Returns a non-empty list of dicts.
        assert isinstance(results, list)
        assert all(isinstance(r, dict) and "resource" in r for r in results)

    @pytest.mark.asyncio
    async def test_provision_grants_dml_to_sp_on_each_sink_table(self) -> None:
        """With multiple sink_tables, GRANT DML must run for each."""
        w = _mock_ws_client()
        conn = _stub_conn()

        with (
            patch(
                "databricks.sdk.WorkspaceClient",
                return_value=w,
            ),
            patch(
                "asyncpg.connect",
                new=AsyncMock(return_value=conn),
            ),
        ):
            results = await provisioning_service.provision_lakebase_role(
                sp_client_id=SP_CLIENT_ID,
                project_id=PROJECT_ID,
                branch_id=BRANCH_ID,
                database="databricks_postgres",
                endpoint_name=ENDPOINT_NAME,
                sink_tables=("sensor_readings", "metrics"),
            )

        executed = [
            call.args[0]
            for call in conn.execute.await_args_list
            if call.args and isinstance(call.args[0], str)
        ]
        dml_grants = [s for s in executed if "INSERT, SELECT, UPDATE, DELETE" in s]
        assert any("sensor_readings" in s for s in dml_grants)
        assert any("metrics" in s for s in dml_grants)
        # Both grants reported as created.
        created_dml = [
            r for r in results if r["resource"].startswith("GRANT DML") and r["status"] == "created"
        ]
        assert len(created_dml) == 2

    @pytest.mark.asyncio
    async def test_provision_is_idempotent(self) -> None:
        """Second run reports already_exists where the first reported created."""
        w = _mock_ws_client()
        conn = _stub_conn()

        # First run — everything succeeds.
        with (
            patch(
                "databricks.sdk.WorkspaceClient",
                return_value=w,
            ),
            patch(
                "asyncpg.connect",
                new=AsyncMock(return_value=conn),
            ),
        ):
            first = await provisioning_service.provision_lakebase_role(
                sp_client_id=SP_CLIENT_ID,
                project_id=PROJECT_ID,
                branch_id=BRANCH_ID,
                database="databricks_postgres",
                endpoint_name=ENDPOINT_NAME,
            )

        assert all(r["status"] in ("created", "skipped") for r in first)

        # Second run — create_role raises "already exists"; grants no-op.
        w2 = _mock_ws_client()
        w2.postgres.create_role = MagicMock(side_effect=Exception("role already exists"))
        conn2 = _stub_conn()

        with (
            patch(
                "databricks.sdk.WorkspaceClient",
                return_value=w2,
            ),
            patch(
                "asyncpg.connect",
                new=AsyncMock(return_value=conn2),
            ),
        ):
            second = await provisioning_service.provision_lakebase_role(
                sp_client_id=SP_CLIENT_ID,
                project_id=PROJECT_ID,
                branch_id=BRANCH_ID,
                database="databricks_postgres",
                endpoint_name=ENDPOINT_NAME,
            )

        # No entries should be status=failed on the repeat.
        assert not any(r["status"] == "failed" for r in second)
        role_entry = next(r for r in second if "Postgres role" in r["resource"])
        assert role_entry["status"] == "already_exists"

    @pytest.mark.asyncio
    async def test_provision_skips_lakets_grant_when_schema_missing(self) -> None:
        """If GRANT USAGE ON SCHEMA lakets fails with 'does not exist', the
        step is reported as skipped and the rest continues."""
        w = _mock_ws_client()
        conn = _stub_conn()

        async def _execute(sql: str, *args, **kwargs):
            if "SCHEMA lakets" in sql:
                raise Exception('schema "lakets" does not exist')
            return None

        conn.execute = AsyncMock(side_effect=_execute)

        with (
            patch(
                "databricks.sdk.WorkspaceClient",
                return_value=w,
            ),
            patch(
                "asyncpg.connect",
                new=AsyncMock(return_value=conn),
            ),
        ):
            results = await provisioning_service.provision_lakebase_role(
                sp_client_id=SP_CLIENT_ID,
                project_id=PROJECT_ID,
                branch_id=BRANCH_ID,
                database="databricks_postgres",
                endpoint_name=ENDPOINT_NAME,
                sink_tables=("sensor_readings",),
            )

        lakets_entry = next(r for r in results if "SCHEMA lakets" in r["resource"])
        assert lakets_entry["status"] == "skipped"
        # The DML grant on the sink table still ran.
        dml_entry = next(
            r
            for r in results
            if r["resource"].startswith("GRANT DML") and "sensor_readings" in r["resource"]
        )
        assert dml_entry["status"] == "created"


# ---------------------------------------------------------------------------
# ensure_streaming_sp — scope + SP + OAuth secret + app grant (refactored out
# of routes/settings.py's /api/settings/provision/secrets handler)
# ---------------------------------------------------------------------------


def _secret(key: str) -> MagicMock:
    s = MagicMock()
    s.key = key
    return s


def _make_sp_ws(existing_keys: list[str], sp_app_id: str | None = None) -> MagicMock:
    """WorkspaceClient mock for ensure_streaming_sp tests.

    ``existing_keys`` drives the already-exists vs. fresh-create branch.
    ``sp_app_id`` is the application_id returned by service_principals.list on
    the already-exists path.
    """
    w = MagicMock()
    w.secrets.create_scope.return_value = None
    w.secrets.list_secrets.return_value = [_secret(k) for k in existing_keys]
    w.secrets.put_secret.return_value = None

    created_sp = MagicMock()
    created_sp.id = "12345"
    created_sp.application_id = "fresh-app-id"
    w.service_principals.create.return_value = created_sp
    w.api_client.do.return_value = {"secret": fake_credential("oauth")}

    if sp_app_id is not None:
        listed_sp = MagicMock()
        listed_sp.application_id = sp_app_id
        w.service_principals.list.return_value = [listed_sp]
    else:
        w.service_principals.list.return_value = []
    return w


class TestEnsureStreamingSP:
    """ensure_streaming_sp creates scope+SP+OAuth-secret idempotently and grants
    the SP CAN_USE on the app."""

    @pytest.fixture(autouse=True)
    def _stub_scope(self):
        # provision_secrets_scope builds its OWN WorkspaceClient() (real SDK,
        # default auth) — correct in prod, but it must be stubbed here so these
        # tests exercise the SP create / already-exists / permission paths
        # instead of bailing out early at a failed (unauthenticated) scope step.
        with patch(
            "iot_twin.services.provisioning_service.provision_secrets_scope",
            new=AsyncMock(return_value={"resource": "Secret scope", "status": "created"}),
        ):
            yield

    @pytest.mark.asyncio
    async def test_fresh_create_returns_client_id_and_grants(self) -> None:
        w = _make_sp_ws(existing_keys=[])
        with (
            patch("iot_twin.services.provisioning_service.grant_app_can_use") as grant_mock,
            patch(
                "iot_twin.services.provisioning_service._app_name", return_value="iot-digital-twin"
            ),
        ):
            out = await provisioning_service.ensure_streaming_sp(w, "iot-secrets")

        assert out["client_id"] == "fresh-app-id"
        assert out["status"] in ("created", "ok")
        # SP created and OAuth secret minted.
        w.service_principals.create.assert_called_once()
        w.api_client.do.assert_called_once()
        # App grant applied with the fresh application_id.
        grant_mock.assert_called_once()
        assert grant_mock.call_args.args[2] == "fresh-app-id"
        assert any(
            r.get("resource") == "App access (CAN_USE) for SP" and r.get("status") == "granted"
            for r in out["results"]
        )

    @pytest.mark.asyncio
    async def test_already_exists_reapplies_grant_without_recreating(self) -> None:
        w = _make_sp_ws(
            existing_keys=["zerobus-client-id", "zerobus-client-secret"],
            sp_app_id="existing-app-id",
        )
        with (
            patch("iot_twin.services.provisioning_service.grant_app_can_use") as grant_mock,
            patch(
                "iot_twin.services.provisioning_service._app_name", return_value="iot-digital-twin"
            ),
        ):
            out = await provisioning_service.ensure_streaming_sp(w, "iot-secrets")

        w.service_principals.create.assert_not_called()
        w.service_principals.list.assert_called_once()
        grant_mock.assert_called_once()
        assert grant_mock.call_args.args[2] == "existing-app-id"
        assert out["client_id"] == "existing-app-id"

    @pytest.mark.asyncio
    async def test_permission_error_is_skipped_not_fatal(self) -> None:
        w = _make_sp_ws(existing_keys=[])
        w.service_principals.create.side_effect = Exception("not authorized")
        with (
            patch("iot_twin.services.provisioning_service.grant_app_can_use"),
            patch(
                "iot_twin.services.provisioning_service._app_name", return_value="iot-digital-twin"
            ),
        ):
            out = await provisioning_service.ensure_streaming_sp(w, "iot-secrets")

        # Non-fatal: surfaces a skipped/failed result but returns a dict.
        assert isinstance(out["results"], list)
        assert out["client_id"] is None
        assert any(
            r.get("status") == "skipped"
            for r in out["results"]
            if "credential" in r.get("resource", "").lower()
        )


class TestProvisionLakebaseRoleBackCompat:
    """Legacy callers that pass role_name/password kwargs must not crash."""

    @pytest.mark.asyncio
    async def test_legacy_password_kwarg_is_ignored_with_warning(self) -> None:
        w = _mock_ws_client()
        conn = _stub_conn()

        with (
            patch(
                "databricks.sdk.WorkspaceClient",
                return_value=w,
            ),
            patch(
                "asyncpg.connect",
                new=AsyncMock(return_value=conn),
            ),
        ):
            results = await provisioning_service.provision_lakebase_role(
                sp_client_id=SP_CLIENT_ID,
                project_id=PROJECT_ID,
                branch_id=BRANCH_ID,
                endpoint_name=ENDPOINT_NAME,
                password=fake_credential("legacy"),  # intentional legacy kwarg
                role_name="iot_app_user",
            )

        assert isinstance(results, list)
        assert not any(r["status"] == "failed" for r in results)


# ---------------------------------------------------------------------------
# install_lakets_if_missing
# ---------------------------------------------------------------------------


class TestInstallLaketsIfMissing:
    """Idempotent LakeTS install helper."""

    @pytest.mark.asyncio
    async def test_install_lakets_noop_when_already_installed(self) -> None:
        w = _mock_ws_client()
        conn = _stub_conn()
        conn.fetchval = AsyncMock(return_value="0.1.0")

        with (
            patch(
                "databricks.sdk.WorkspaceClient",
                return_value=w,
            ),
            patch(
                "asyncpg.connect",
                new=AsyncMock(return_value=conn),
            ),
        ):
            result = await lakets_service.install_lakets_if_missing(
                endpoint_name=ENDPOINT_NAME,
                database="databricks_postgres",
            )

        assert result["status"] == "already_exists"
        assert result["version"] == "0.1.0"
        # Must not have executed any SQL beyond the version probe.
        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_install_lakets_runs_sql_when_missing(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- LakeTS body\n")
        monkeypatch.setenv("LAKETS_SQL_PATH", str(sql_file))

        w = _mock_ws_client()
        conn = _stub_conn()
        # First fetchval (probe) raises (table missing); second returns version.
        conn.fetchval = AsyncMock(
            side_effect=[
                Exception('relation "lakets._version" does not exist'),
                "0.1.3",
            ]
        )

        with (
            patch(
                "databricks.sdk.WorkspaceClient",
                return_value=w,
            ),
            patch(
                "asyncpg.connect",
                new=AsyncMock(return_value=conn),
            ),
        ):
            result = await lakets_service.install_lakets_if_missing(
                endpoint_name=ENDPOINT_NAME,
                database="databricks_postgres",
            )

        assert result["status"] == "created"
        assert result["version"] == "0.1.3"
        # The SQL body was executed.
        executed = [call.args[0] for call in conn.execute.await_args_list if call.args]
        assert any("LakeTS body" in s for s in executed)


# ---------------------------------------------------------------------------
# lakets_service.enable_sync — shadow-table + mirror-trigger preparation
# ---------------------------------------------------------------------------


class TestLaketsEnableSync:
    """enable_sync wraps SELECT lakets.enable_sync(table, schema) idempotently."""

    @pytest.mark.asyncio
    async def test_enable_sync_created(self) -> None:
        conn = MagicMock()
        # Probe: ChronoTable exists, not yet synced.
        conn.fetchrow = AsyncMock(return_value={"id": 1, "sync_enabled": False})
        conn.fetchval = AsyncMock(return_value=None)
        conn.execute = AsyncMock(return_value=None)

        result = await lakets_service.enable_sync(conn, "sensor_readings")

        assert result["status"] == "created"
        assert "sensor_readings" in result["resource"]
        # The lakets.enable_sync(...) call was issued with table + schema args.
        call = conn.fetchval.await_args
        assert "lakets.enable_sync" in call.args[0]
        assert call.args[1] == "sensor_readings"
        assert call.args[2] == "public"

    @pytest.mark.asyncio
    async def test_enable_sync_idempotent_when_already_synced(self) -> None:
        conn = MagicMock()
        # Probe finds the registry row with sync_enabled already TRUE.
        conn.fetchrow = AsyncMock(return_value={"id": 1, "sync_enabled": True})
        conn.fetchval = AsyncMock(return_value=None)
        conn.execute = AsyncMock(return_value=None)

        result = await lakets_service.enable_sync(conn, "sensor_readings")

        assert result["status"] == "already_exists"
        # Must NOT re-invoke lakets.enable_sync when already synced.
        conn.fetchval.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_enable_sync_idempotent_on_duplicate_error(self) -> None:
        conn = MagicMock()
        # Registry probe says not synced, but the call races and reports
        # an "already" condition — must be swallowed as already_exists.
        conn.fetchrow = AsyncMock(return_value={"id": 1, "sync_enabled": False})
        conn.fetchval = AsyncMock(
            side_effect=Exception("shadow table _shadow_sensor_readings already exists")
        )

        result = await lakets_service.enable_sync(conn, "sensor_readings")

        assert result["status"] == "already_exists"

    @pytest.mark.asyncio
    async def test_enable_sync_failure_surfaces(self) -> None:
        conn = MagicMock()
        conn.fetchrow = AsyncMock(return_value={"id": 1, "sync_enabled": False})
        conn.fetchval = AsyncMock(side_effect=Exception("boom"))

        result = await lakets_service.enable_sync(conn, "sensor_readings")

        assert result["status"] == "failed"
        assert "boom" in result["error"]

    @pytest.mark.asyncio
    async def test_enable_sync_custom_schema(self) -> None:
        conn = MagicMock()
        conn.fetchrow = AsyncMock(return_value={"id": 1, "sync_enabled": False})
        conn.fetchval = AsyncMock(return_value=None)

        result = await lakets_service.enable_sync(conn, "sr", schema="iot")

        assert result["status"] == "created"
        call = conn.fetchval.await_args
        assert call.args[2] == "iot"
