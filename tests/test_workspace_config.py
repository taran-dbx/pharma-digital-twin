"""Tests for core/workspace_config.py — the central workspace configuration module."""

from __future__ import annotations

import pytest

# Required env vars and the WorkspaceConfig field each maps to.
_REQUIRED_ENV: dict[str, str] = {
    "UC_CATALOG": "uc_catalog",
    "UC_SCHEMA": "uc_schema",
    "LAKEBASE_PROJECT": "lakebase_project",
    "LAKEBASE_ENDPOINT": "lakebase_endpoint",
    "DATABRICKS_SQL_WAREHOUSE_ID": "warehouse_id",
    "ZEROBUS_SERVER_ENDPOINT": "zerobus_endpoint",
    "DATABRICKS_CLIENT_ID": "app_sp_client_id",
    "INDUSTRIES": "industries",  # list field — tested separately
}

# Secret scope and UC service credential *names* (identifiers, not secrets).
SCOPE_NAME = "test-scope"
UC_SC_NAME = "test-credential"
CUSTOM_SCOPE_NAME = "my-scope"
ZEROBUS_SCOPE_NAME = "zb-scope"

_FULL_ENV: dict[str, str] = {
    "UC_CATALOG": "test_catalog",
    "UC_SCHEMA": "test_schema",
    "LAKEBASE_PROJECT": "iot-app-backend",
    "LAKEBASE_ENDPOINT": "projects/iot-app-backend/branches/production/endpoints/primary",
    "DATABRICKS_SQL_WAREHOUSE_ID": "0123456789abcdef",
    "ZEROBUS_SERVER_ENDPOINT": "2222222222222222.zerobus.eu-central-1.cloud.databricks.com",
    "DATABRICKS_CLIENT_ID": "00000000-0000-0000-0000-00000000000a",
    "INDUSTRIES": "pharmaceutical,manufacturing",
    "PGHOST": "example-pg.database.eu-central-1.cloud.databricks.com",
    # Optional env vars for the three call-time-only fields.
    "SECRET_SCOPE": SCOPE_NAME,
    "UC_SERVICE_CREDENTIAL": UC_SC_NAME,
    "PIPELINES_LAKEBASE_PROJECT": "test-pipelines",
}


@pytest.fixture()
def full_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch all required (and optional) env vars for workspace config."""
    for key, val in _FULL_ENV.items():
        monkeypatch.setenv(key, val)


class TestGetConfigReadsEnvVars:
    """get_config() must read every field from the corresponding env var."""

    def test_uc_catalog(self, full_env: None) -> None:
        from iot_twin.core.workspace_config import get_config

        cfg = get_config()
        assert cfg.uc_catalog == "test_catalog"

    def test_uc_schema(self, full_env: None) -> None:
        from iot_twin.core.workspace_config import get_config

        cfg = get_config()
        assert cfg.uc_schema == "test_schema"

    def test_lakebase_project(self, full_env: None) -> None:
        from iot_twin.core.workspace_config import get_config

        cfg = get_config()
        assert cfg.lakebase_project == "iot-app-backend"

    def test_lakebase_endpoint(self, full_env: None) -> None:
        from iot_twin.core.workspace_config import get_config

        cfg = get_config()
        assert (
            cfg.lakebase_endpoint
            == "projects/iot-app-backend/branches/production/endpoints/primary"
        )

    def test_warehouse_id(self, full_env: None) -> None:
        from iot_twin.core.workspace_config import get_config

        cfg = get_config()
        assert cfg.warehouse_id == "0123456789abcdef"

    def test_zerobus_endpoint(self, full_env: None) -> None:
        from iot_twin.core.workspace_config import get_config

        cfg = get_config()
        assert cfg.zerobus_endpoint == "2222222222222222.zerobus.eu-central-1.cloud.databricks.com"

    def test_app_sp_client_id(self, full_env: None) -> None:
        from iot_twin.core.workspace_config import get_config

        cfg = get_config()
        assert cfg.app_sp_client_id == "00000000-0000-0000-0000-00000000000a"

    def test_industries_parsed_as_list(self, full_env: None) -> None:
        from iot_twin.core.workspace_config import get_config

        cfg = get_config()
        assert cfg.industries == ["pharmaceutical", "manufacturing"]

    def test_pg_host_optional_present(self, full_env: None) -> None:
        from iot_twin.core.workspace_config import get_config

        cfg = get_config()
        assert cfg.pg_host == "example-pg.database.eu-central-1.cloud.databricks.com"

    def test_pg_host_optional_absent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """PGHOST may be empty or missing at startup — must not raise."""
        for key, val in _FULL_ENV.items():
            monkeypatch.setenv(key, val)
        monkeypatch.delenv("PGHOST", raising=False)
        from iot_twin.core.workspace_config import get_config

        cfg = get_config()
        assert cfg.pg_host == ""

    def test_config_is_frozen(self, full_env: None) -> None:
        from iot_twin.core.workspace_config import get_config

        cfg = get_config()
        with pytest.raises((AttributeError, TypeError)):
            cfg.uc_catalog = "modified"  # type: ignore[misc]


class TestOptionalFields:
    """Optional fields (secret_scope, service_credential, pipelines_project) default to ''."""

    def test_secret_scope_from_secret_scope_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for key, val in _FULL_ENV.items():
            monkeypatch.setenv(key, val)
        monkeypatch.setenv("SECRET_SCOPE", CUSTOM_SCOPE_NAME)
        monkeypatch.delenv("ZEROBUS_SECRET_SCOPE", raising=False)
        from iot_twin.core.workspace_config import get_config

        assert get_config().secret_scope == CUSTOM_SCOPE_NAME

    def test_secret_scope_falls_back_to_zerobus_scope(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        for key, val in _FULL_ENV.items():
            monkeypatch.setenv(key, val)
        monkeypatch.delenv("SECRET_SCOPE", raising=False)
        monkeypatch.setenv("ZEROBUS_SECRET_SCOPE", ZEROBUS_SCOPE_NAME)
        from iot_twin.core.workspace_config import get_config

        assert get_config().secret_scope == ZEROBUS_SCOPE_NAME

    def test_secret_scope_empty_when_neither_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for key, val in _FULL_ENV.items():
            monkeypatch.setenv(key, val)
        monkeypatch.delenv("SECRET_SCOPE", raising=False)
        monkeypatch.delenv("ZEROBUS_SECRET_SCOPE", raising=False)
        from iot_twin.core.workspace_config import get_config

        assert get_config().secret_scope == ""

    def test_service_credential_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for key, val in _FULL_ENV.items():
            monkeypatch.setenv(key, val)
        monkeypatch.setenv("UC_SERVICE_CREDENTIAL", "my-cred")
        from iot_twin.core.workspace_config import get_config

        assert get_config().service_credential == "my-cred"

    def test_service_credential_empty_when_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for key, val in _FULL_ENV.items():
            monkeypatch.setenv(key, val)
        monkeypatch.delenv("UC_SERVICE_CREDENTIAL", raising=False)
        from iot_twin.core.workspace_config import get_config

        assert get_config().service_credential == ""

    def test_pipelines_project_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for key, val in _FULL_ENV.items():
            monkeypatch.setenv(key, val)
        monkeypatch.setenv("PIPELINES_LAKEBASE_PROJECT", "my-project")
        from iot_twin.core.workspace_config import get_config

        assert get_config().pipelines_project == "my-project"

    def test_pipelines_project_empty_when_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for key, val in _FULL_ENV.items():
            monkeypatch.setenv(key, val)
        monkeypatch.delenv("PIPELINES_LAKEBASE_PROJECT", raising=False)
        from iot_twin.core.workspace_config import get_config

        assert get_config().pipelines_project == ""

    def test_genie_space_id_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for key, val in _FULL_ENV.items():
            monkeypatch.setenv(key, val)
        monkeypatch.setenv("GENIE_SPACE_ID", "space-xyz")
        from iot_twin.core.workspace_config import get_config

        assert get_config().genie_space_id == "space-xyz"

    def test_genie_space_id_empty_when_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for key, val in _FULL_ENV.items():
            monkeypatch.setenv(key, val)
        monkeypatch.delenv("GENIE_SPACE_ID", raising=False)
        from iot_twin.core.workspace_config import get_config

        assert get_config().genie_space_id == ""


class TestCallTimeGetters:
    """Standalone getter functions must read from env on every call (call-time)."""

    def test_get_secret_scope_reads_secret_scope_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SECRET_SCOPE", "call-time-scope")
        monkeypatch.delenv("ZEROBUS_SECRET_SCOPE", raising=False)
        from iot_twin.core.workspace_config import get_secret_scope

        assert get_secret_scope() == "call-time-scope"

    def test_get_secret_scope_falls_back_to_zerobus(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("SECRET_SCOPE", raising=False)
        monkeypatch.setenv("ZEROBUS_SECRET_SCOPE", "zb-call-scope")
        from iot_twin.core.workspace_config import get_secret_scope

        assert get_secret_scope() == "zb-call-scope"

    def test_get_secret_scope_returns_empty_when_unset(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("SECRET_SCOPE", raising=False)
        monkeypatch.delenv("ZEROBUS_SECRET_SCOPE", raising=False)
        from iot_twin.core.workspace_config import get_secret_scope

        assert get_secret_scope() == ""

    def test_get_service_credential_reads_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("UC_SERVICE_CREDENTIAL", "call-time-cred")
        from iot_twin.core.workspace_config import get_service_credential

        assert get_service_credential() == "call-time-cred"

    def test_get_service_credential_returns_empty_when_unset(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("UC_SERVICE_CREDENTIAL", raising=False)
        from iot_twin.core.workspace_config import get_service_credential

        assert get_service_credential() == ""

    def test_get_pipelines_project_reads_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PIPELINES_LAKEBASE_PROJECT", "call-time-project")
        from iot_twin.core.workspace_config import get_pipelines_project

        assert get_pipelines_project() == "call-time-project"

    def test_get_pipelines_project_returns_empty_when_unset(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("PIPELINES_LAKEBASE_PROJECT", raising=False)
        from iot_twin.core.workspace_config import get_pipelines_project

        assert get_pipelines_project() == ""

    def test_get_genie_space_id_reads_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("GENIE_SPACE_ID", "space-from-resource")
        from iot_twin.core.workspace_config import get_genie_space_id

        assert get_genie_space_id() == "space-from-resource"

    def test_get_genie_space_id_returns_empty_when_unset(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("GENIE_SPACE_ID", raising=False)
        from iot_twin.core.workspace_config import get_genie_space_id

        assert get_genie_space_id() == ""


class TestGetConfigRaisesOnMissingRequired:
    """get_config() must raise RuntimeError naming the missing var."""

    @pytest.mark.parametrize("missing_var", list(_REQUIRED_ENV.keys()))
    def test_raises_when_var_missing(
        self, monkeypatch: pytest.MonkeyPatch, missing_var: str
    ) -> None:
        for key, val in _FULL_ENV.items():
            monkeypatch.setenv(key, val)
        monkeypatch.delenv(missing_var, raising=False)
        from iot_twin.core.workspace_config import get_config

        with pytest.raises(RuntimeError, match=missing_var):
            get_config()

    def test_error_message_names_missing_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for key, val in _FULL_ENV.items():
            monkeypatch.setenv(key, val)
        monkeypatch.delenv("UC_CATALOG", raising=False)
        monkeypatch.delenv("DATABRICKS_SQL_WAREHOUSE_ID", raising=False)
        from iot_twin.core.workspace_config import get_config

        with pytest.raises(RuntimeError) as exc_info:
            get_config()
        msg = str(exc_info.value)
        assert "UC_CATALOG" in msg
        assert "DATABRICKS_SQL_WAREHOUSE_ID" in msg

    def test_pghost_empty_does_not_raise(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """PGHOST blank/absent must not trigger the missing-var error."""
        for key, val in _FULL_ENV.items():
            monkeypatch.setenv(key, val)
        monkeypatch.setenv("PGHOST", "")
        from iot_twin.core.workspace_config import get_config

        cfg = get_config()  # must not raise
        assert cfg.pg_host == ""


class TestWorkspaceConfigStructure:
    """WorkspaceConfig must be a frozen dataclass with the expected field set."""

    def test_expected_fields(self, full_env: None) -> None:
        from iot_twin.core.workspace_config import WorkspaceConfig, get_config

        cfg = get_config()
        assert isinstance(cfg, WorkspaceConfig)
        expected = {
            "uc_catalog",
            "uc_schema",
            "lakebase_project",
            "lakebase_endpoint",
            "pg_host",
            "warehouse_id",
            "zerobus_endpoint",
            "app_sp_client_id",
            "industries",
            # Optional fields added in review pass.
            "secret_scope",
            "service_credential",
            "pipelines_project",
            "genie_space_id",
        }
        actual = {f for f in cfg.__dataclass_fields__}
        assert actual == expected

    def test_industries_is_list(self, full_env: None) -> None:
        from iot_twin.core.workspace_config import get_config

        cfg = get_config()
        assert isinstance(cfg.industries, list)
        assert all(isinstance(i, str) for i in cfg.industries)
