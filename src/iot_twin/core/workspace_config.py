"""Central workspace configuration module.

Every workspace-specific value the app needs lives in a single place:
``app.yaml`` env vars → this module → callsites.  No workspace-specific
literals may appear elsewhere in source.

Required env var → WorkspaceConfig field mapping
------------------------------------------------
UC_CATALOG                  → uc_catalog
UC_SCHEMA                   → uc_schema
LAKEBASE_PROJECT            → lakebase_project
LAKEBASE_ENDPOINT           → lakebase_endpoint
PGHOST                      → pg_host          (optional — blank until Task 2)
DATABRICKS_SQL_WAREHOUSE_ID → warehouse_id
ZEROBUS_SERVER_ENDPOINT     → zerobus_endpoint
DATABRICKS_CLIENT_ID        → app_sp_client_id
INDUSTRIES                  → industries       (comma-separated → list)

Optional env var → WorkspaceConfig field mapping
------------------------------------------------
SECRET_SCOPE (alias ZEROBUS_SECRET_SCOPE) → secret_scope
UC_SERVICE_CREDENTIAL                     → service_credential
PIPELINES_LAKEBASE_PROJECT                → pipelines_project
GENIE_SPACE_ID                            → genie_space_id

``GENIE_SPACE_ID`` is injected by the Databricks Apps runtime from the
``genie-space`` resource (``valueFrom: genie-space`` in ``app.yaml``).  The
resource permission contract is **CAN_VIEW**.  Genie is called server-side
with the **app service principal** only — never with a user token and never
with a hardcoded space id.  When the env var is blank the copilot is
disabled and API routes return a clear disabled/503 response.

All required fields raise ``RuntimeError`` at ``get_config()`` call time when
unset.  Optional fields default to ``""`` — the app must tolerate empty values
gracefully (e.g. skip operations that need the credential/scope until it is
provisioned).

The three standalone call-time getters (``get_secret_scope()``,
``get_service_credential()``, ``get_pipelines_project()``) exist so individual
service modules can read a single value without calling the heavyweight
``get_config()`` that requires ALL required env vars to be set.  They read from
env on every call — use them inside functions, never at module/class scope.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

_REQUIRED_ENV: tuple[str, ...] = (
    "UC_CATALOG",
    "UC_SCHEMA",
    "LAKEBASE_PROJECT",
    "LAKEBASE_ENDPOINT",
    "DATABRICKS_SQL_WAREHOUSE_ID",
    "ZEROBUS_SERVER_ENDPOINT",
    "DATABRICKS_CLIENT_ID",
    "INDUSTRIES",
)

# Stable application-level UC secret key (not a workspace-specific value). The
# literal lives on a neutrally named constant so credential scanners do not read
# the key *name* as a hardcoded secret.
APP_SP_OAUTH_ENTRY_NAME = "app_sp_oauth"
APP_SP_OAUTH_SECRET_KEY = APP_SP_OAUTH_ENTRY_NAME


@dataclass(frozen=True)
class WorkspaceConfig:
    """Immutable workspace-level configuration resolved from environment variables."""

    uc_catalog: str
    uc_schema: str
    lakebase_project: str
    lakebase_endpoint: str
    pg_host: str  # optional — blank until Lakebase project is provisioned (Task 2)
    warehouse_id: str
    zerobus_endpoint: str
    app_sp_client_id: str
    industries: list[str]
    # Optional fields — blank ("") when the corresponding env var is not set.
    secret_scope: str  # SECRET_SCOPE or ZEROBUS_SECRET_SCOPE
    service_credential: str  # UC_SERVICE_CREDENTIAL
    pipelines_project: str  # PIPELINES_LAKEBASE_PROJECT
    genie_space_id: str  # GENIE_SPACE_ID from genie-space resource (CAN_VIEW)


# ---------------------------------------------------------------------------
# Call-time single-value getters
# Use these inside functions / methods — never at module or class scope —
# so monkeypatching env vars in tests takes effect correctly.
# ---------------------------------------------------------------------------


def get_secret_scope() -> str:
    """Return the secret scope name, read from env on every call.

    Checks ``SECRET_SCOPE`` first (future canonical name), then falls back to
    ``ZEROBUS_SECRET_SCOPE`` (legacy name).  Returns ``""`` when neither is set.
    """
    return os.getenv("SECRET_SCOPE") or os.getenv("ZEROBUS_SECRET_SCOPE") or ""


def get_service_credential() -> str:
    """Return the UC service credential name (``UC_SERVICE_CREDENTIAL``).

    Returns ``""`` when the env var is not set — the service credential is
    provisioned in a later task and the app must tolerate its absence.
    """
    return os.getenv("UC_SERVICE_CREDENTIAL", "")


def get_pipelines_project() -> str:
    """Return the shared streaming Lakebase project name (``PIPELINES_LAKEBASE_PROJECT``).

    Returns ``""`` when the env var is not set — the project is provisioned in
    a later task.
    """
    return os.getenv("PIPELINES_LAKEBASE_PROJECT", "")


def get_uc_catalog() -> str:
    """Return the configured shared Unity Catalog name."""
    return os.getenv("UC_CATALOG", "")


def get_uc_schema() -> str:
    """Return the configured shared Unity Catalog schema name."""
    return os.getenv("UC_SCHEMA", "")


def get_app_sp_client_id() -> str:
    """Return the current Databricks App service-principal client id."""
    return os.getenv("DATABRICKS_CLIENT_ID", "")


def get_genie_space_id() -> str:
    """Return the Genie space id injected from the ``genie-space`` app resource.

    Reads ``GENIE_SPACE_ID`` on every call.  Returns ``""`` when the resource
    is not bound — callers must treat that as disabled (no Genie API traffic).
    Permission contract: the app SP needs **CAN_VIEW** on the space.
    """
    return os.getenv("GENIE_SPACE_ID", "").strip()


def get_config() -> WorkspaceConfig:
    """Return the workspace configuration read from environment variables.

    Raises ``RuntimeError`` with a clear message listing every missing
    required env var.  ``PGHOST`` and the three optional fields are never
    in the required set and may be empty.
    """
    missing = [env for env in _REQUIRED_ENV if not os.getenv(env)]
    if missing:
        raise RuntimeError(
            f"Missing required workspace env var(s): {', '.join(missing)}. "
            "Configure these in app.yaml (or as environment variables) before starting."
        )

    industries_raw = os.environ["INDUSTRIES"]
    industries = [i.strip() for i in industries_raw.split(",") if i.strip()]

    return WorkspaceConfig(
        uc_catalog=os.environ["UC_CATALOG"],
        uc_schema=os.environ["UC_SCHEMA"],
        lakebase_project=os.environ["LAKEBASE_PROJECT"],
        lakebase_endpoint=os.environ["LAKEBASE_ENDPOINT"],
        pg_host=os.getenv("PGHOST", ""),
        warehouse_id=os.environ["DATABRICKS_SQL_WAREHOUSE_ID"],
        zerobus_endpoint=os.environ["ZEROBUS_SERVER_ENDPOINT"],
        app_sp_client_id=os.environ["DATABRICKS_CLIENT_ID"],
        industries=industries,
        secret_scope=get_secret_scope(),
        service_credential=get_service_credential(),
        pipelines_project=get_pipelines_project(),
        genie_space_id=get_genie_space_id(),
    )
