"""Full-path smoke: wizard→provisioning glue against iot-lakets-demo.

Spins up a minimal FastAPI app with the provisioning router, injects a mock
state.db that returns a synthetic pipeline carrying ``sink_config.tiering``,
and POSTs to ``/api/provision/lakebase-table``.  The route's extractors pull
tiering + sse_metrics out of the pipeline row, build the ``lb`` dict, and
forward to ``provisioning_service.provision_lakebase_table`` — which hits the
real iot-lakets-demo Lakebase and applies (or confirms) ChronoTable + tier
policy.  Afterwards the script inspects ``lakets.*`` registries to prove the
end-to-end write landed.

Usage:
    LAKETS_SQL_PATH=/path/to/lakets.sql \\
    .venv/bin/python -m scripts.smoke_pipeline_deploy --profile <cli-profile>
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

_ROOT = Path(__file__).resolve().parent.parent
# Repo root (for ``scripts.*``) and src/ (for ``iot_twin``) must be importable.
for _path in (_ROOT / "src", _ROOT):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from scripts._profile import require_profile

from iot_twin.core.state import AppState
from iot_twin.routes.provisioning import router as provisioning_router

PROJECT_ID = os.getenv("LAKETS_PROJECT_ID", "iot-lakets-demo")
BRANCH_ID = os.getenv("LAKETS_BRANCH_ID", "production")
DATABASE = os.getenv("PGDATABASE", "databricks_postgres")
TABLE = os.getenv("PG_TABLE", "sensor_readings")
LAKETS_SQL_PATH = os.getenv(
    "LAKETS_SQL_PATH",
    str(_ROOT / "src" / "iot_twin" / "assets" / "lakets.sql"),
)


FAKE_PIPELINE_ID = "11111111-1111-1111-1111-111111111111"
FAKE_SINK_CONFIG = {
    "tiering": {
        "hot": os.getenv("LAKETS_HOT", "1 hour"),
        "drop": os.getenv("LAKETS_DROP", "7 days"),
        "chunk": os.getenv("LAKETS_CHUNK", "15 minutes"),
    },
    "sse_metrics": ["temperature_avg", "rpm_spike_rate"],
}


def _build_app() -> FastAPI:
    app = FastAPI()
    state = AppState()
    state.db_available = True
    state.db = MagicMock()
    state.db.fetchrow = AsyncMock(
        return_value={
            "id": FAKE_PIPELINE_ID,
            "sink_config": json.dumps(FAKE_SINK_CONFIG),
        }
    )
    # ``track_resource`` is called after provisioning succeeds; mock execute.
    state.db.execute = AsyncMock(return_value=None)
    app.state.app_state = state
    app.include_router(provisioning_router)
    return app


async def _inspect_registries() -> None:
    """Query iot-lakets-demo directly to confirm provisioning landed."""
    import asyncpg
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    parent = f"projects/{PROJECT_ID}/branches/{BRANCH_ID}"
    endpoints = list(w.postgres.list_endpoints(parent=parent))
    endpoint = w.postgres.get_endpoint(name=endpoints[0].name)
    cred = w.postgres.generate_database_credential(endpoint=endpoints[0].name)

    conn = await asyncpg.connect(
        host=endpoint.status.hosts.host,
        database=DATABASE,
        user=w.current_user.me().user_name,
        password=cred.token,
        port=5432,
        ssl="require",
    )
    try:
        cts = await conn.fetch(
            "SELECT schema_name, table_name, chunk_interval "
            "FROM lakets._chronotable_registry ORDER BY id"
        )
        policies = await conn.fetch(
            "SELECT policy_type, config, last_run_at FROM lakets._policy_registry ORDER BY id"
        )
    finally:
        await conn.close()

    print("\n==== lakets._chronotable_registry ====")
    for r in cts:
        print(f"  {r['schema_name']}.{r['table_name']}  chunk={r['chunk_interval']}")
    print("\n==== lakets._policy_registry ====")
    for p in policies:
        print(f"  type={p['policy_type']}  config={p['config']}  last_run={p['last_run_at']}")


async def main() -> int:
    print(f"Profile: {os.getenv('DATABRICKS_CONFIG_PROFILE', '<none>')}")
    print(f"Pipeline: {FAKE_PIPELINE_ID}")
    print(f"Target:   project={PROJECT_ID} branch={BRANCH_ID} db={DATABASE} table={TABLE}")
    print(f"Tiering:  {FAKE_SINK_CONFIG['tiering']}")
    print()

    app = _build_app()
    # Set the LakeTS path via env so resolve_lakets_source picks it up in the
    # provisioning service when LakeTS isn't yet installed on the target.
    os.environ.setdefault("LAKETS_SQL_PATH", LAKETS_SQL_PATH)

    body = {
        "mode": "autoscale",
        "project_id": PROJECT_ID,
        "branch_id": BRANCH_ID,
        "database": DATABASE,
        "table_name": TABLE,
        "pipeline_id": FAKE_PIPELINE_ID,
    }

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://smoke") as c:
        response = await c.post(
            "/api/provision/lakebase-table",
            json=body,
            timeout=120,
        )

    print(f"HTTP {response.status_code}")
    try:
        print(json.dumps(response.json(), indent=2, default=str))
    except Exception:
        print(response.text)

    await _inspect_registries()

    if response.status_code in (200, 409):
        return 0
    return 1


if __name__ == "__main__":
    require_profile()  # --profile <name> or $DATABRICKS_CONFIG_PROFILE; no default workspace
    sys.exit(asyncio.run(main()))
