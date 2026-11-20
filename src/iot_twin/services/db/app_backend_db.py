"""Sole Lakebase connection manager for the ``iot-app-backend`` project.

The app-backend metadata model (``pipeline_setup`` / ``pipeline_deployment`` /
``provisioned_resource`` / ``pipelines`` / ``user_settings`` / ``source_bindings``
— see ``models/app_schema.py`` and ``core/lakebase.py``) all live in the single
``iot-app-backend`` Lakebase project.  ``build_app_backend_manager()`` returns
the one connection manager used by the entire application: the lifespan assigns it
to BOTH ``state.db`` and ``state.app_backend_db`` so existing callsites on either
field keep working without modification.

Credentials always go through the Databricks SDK OAuth path
(``w.postgres.generate_database_credential`` for the ``iot-app-backend`` endpoint),
so any ``PGUSER``/``PGPASSWORD`` pair injected by the platform cannot accidentally
override the OAuth flow.

Connection coordinates (host, endpoint, database) are resolved from env vars
at module import time for fast path re-use across calls to
``build_app_backend_manager()``, but can be overridden per the table below:

  PGHOST          (or APP_BACKEND_PGHOST)    → pool host
  LAKEBASE_ENDPOINT (or APP_BACKEND_LAKEBASE_ENDPOINT) → credential endpoint
  PGDATABASE      (or APP_BACKEND_PGDATABASE) → database name (default: databricks_postgres)
"""

from __future__ import annotations

import logging
import os

from iot_twin.core.lakebase import LakebaseConnectionManager, _get_lakebase_credentials

logger = logging.getLogger("iot_digital_twin.services.app_backend_db")

# ---------------------------------------------------------------------------
# app-backend project coordinates (overridable via env, but never via PGHOST)
# ---------------------------------------------------------------------------
APP_BACKEND_PROJECT = os.getenv("LAKEBASE_PROJECT", "iot-app-backend")
APP_BACKEND_ENDPOINT = os.getenv(
    "APP_BACKEND_LAKEBASE_ENDPOINT",
    os.getenv("LAKEBASE_ENDPOINT", ""),
)
# Host resolved from APP_BACKEND_PGHOST; falls back to PGHOST but never to a
# workspace-specific literal — missing host causes initialize() to raise clearly.
APP_BACKEND_HOST = os.getenv("APP_BACKEND_PGHOST") or os.getenv("PGHOST", "")
APP_BACKEND_DATABASE = os.getenv("APP_BACKEND_PGDATABASE", "databricks_postgres")


class AppBackendConnectionManager(LakebaseConnectionManager):
    """A ``LakebaseConnectionManager`` pinned to the ``iot-app-backend`` project.

    Overrides credential resolution to *always* use SDK OAuth against the
    app-backend endpoint, ignoring any ``PGUSER``/``PGPASSWORD`` static creds
    that Databricks may inject for the legacy (``iot-lakets-demo``) project.
    """

    async def initialize(self) -> None:  # type: ignore[override]
        if not self._pg_host:
            raise RuntimeError("app-backend Lakebase host not configured")

        import asyncpg

        self._pg_user, self._pg_password = _get_lakebase_credentials(self._endpoint_name)
        self._uses_static_creds = False

        ssl_mode = os.getenv("PGSSLMODE", "require")
        self._pool = await asyncpg.create_pool(
            host=self._pg_host,
            database=self._pg_database,
            user=self._pg_user,
            password=self._pg_password,
            port=self._pg_port,
            min_size=self._pool_min,
            max_size=self._pool_max,
            ssl=ssl_mode,
        )
        logger.info(
            "app-backend Lakebase pool created — host=%s db=%s user=%s",
            self._pg_host,
            self._pg_database,
            self._pg_user,
        )
        import asyncio

        self._refresh_task = asyncio.create_task(self._token_refresh_loop())


def build_app_backend_manager() -> AppBackendConnectionManager:
    """Construct (but do not initialize) the app-backend connection manager.

    Host, database, and endpoint are pinned to the ``iot-app-backend`` project
    constants above (env-overridable) — never resolved from ``PGHOST`` /
    ``LAKEBASE_ENDPOINT``, so the legacy connection is untouched.
    """
    return AppBackendConnectionManager(
        pg_host=APP_BACKEND_HOST,
        pg_database=APP_BACKEND_DATABASE,
        endpoint_name=APP_BACKEND_ENDPOINT,
    )
