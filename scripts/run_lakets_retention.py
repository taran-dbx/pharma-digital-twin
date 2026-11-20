"""Run LakeTS retention against a streaming Lakebase.

Entry point for a Databricks Job task (scheduled daily).  Connects using
autoscale OAuth (or direct PG creds), discovers tables with retention policies,
and invokes ``lakets.execute_retention`` for each.

Usage (local, against iot-lakets-demo):
    DATABRICKS_CONFIG_PROFILE=<cli-profile> \\
    LAKETS_PROJECT_ID=iot-lakets-demo \\
    .venv/bin/python -m scripts.run_lakets_retention
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
# Repo root (for ``scripts.*``) and src/ (for ``iot_twin``) must be importable.
for _path in (_ROOT / "src", _ROOT):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from iot_twin.routes.lakets import _open_lakebase_conn
from iot_twin.services.lakets import lakets_service


async def main() -> int:
    lb = {
        "mode": os.getenv("LAKEBASE_MODE", "autoscale"),
        "project_id": os.getenv("LAKETS_PROJECT_ID", ""),
        "branch_id": os.getenv("LAKETS_BRANCH_ID", "production"),
        "database": os.getenv("PGDATABASE", "databricks_postgres"),
        "host": os.getenv("PGHOST", ""),
        "user": os.getenv("PGUSER", ""),
        "password": os.getenv("PGPASSWORD", ""),
        "port": int(os.getenv("PGPORT", "5432")),
    }
    conn = await _open_lakebase_conn(lb)
    try:
        results = await lakets_service.run_retention(conn)
    finally:
        await conn.close()

    print(json.dumps(results, indent=2, default=str))
    return 0 if all(isinstance(v, int) for v in results.values()) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
