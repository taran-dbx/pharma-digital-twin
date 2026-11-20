"""Bootstrap a fresh Lakebase project for LakeTS smoke testing.

Creates:
- Lakebase project  ``iot-lakets-demo``
- Branch             ``production``
- Postgres role      (``databricks_superuser`` via ``provision_lakebase_role``)

Then runs ``provision_lakebase_table`` (which installs LakeTS on the same
connection) in two modes:
    Option A — autoscale (SDK OAuth, current user)
    Option B — direct    (role + password saved during step 3)

Usage:
    LAKETS_SQL_PATH=/path/to/lakets.sql \\
    .venv/bin/python -m scripts.bootstrap_lakets_demo --profile <cli-profile>
"""

from __future__ import annotations

import asyncio
import os
import secrets as secrets_mod
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
# Repo root (for ``scripts.*``) and src/ (for ``iot_twin``) must be importable.
for _path in (_ROOT / "src", _ROOT):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from scripts._profile import require_profile

from iot_twin.services.provisioning_service import (
    provision_lakebase_project_and_branch,
    provision_lakebase_role,
    provision_lakebase_table,
)

PROJECT_ID = os.getenv("LAKETS_PROJECT_ID", "iot-lakets-demo")
BRANCH_ID = os.getenv("LAKETS_BRANCH_ID", "production")
DATABASE = os.getenv("PGDATABASE", "databricks_postgres")
DEFAULT_LAKETS_SQL = str(_ROOT / "src" / "iot_twin" / "assets" / "lakets.sql")


def _print_header(title: str) -> None:
    print()
    print("=" * 70)
    print(title)
    print("=" * 70)


def _print_results(results) -> None:
    if isinstance(results, dict):
        results = [results]
    for r in results:
        status = r.get("status", "?")
        marker = {"created": "+", "already_exists": "=", "skipped": "·"}.get(status, "!")
        print(f"  [{marker}] {r.get('resource', '?')}: {status}")
        if r.get("error"):
            print(f"      error: {r['error']}")
        if r.get("lakets_version"):
            print(f"      lakets_version: {r['lakets_version']}")
        if r.get("lakets_error"):
            print(f"      lakets_error: {r['lakets_error']}")
        if r.get("tiering_applied"):
            t = r["tiering_applied"]
            print(f"      tiering_applied: hot={t['hot']}  drop={t['drop']}  chunk={t['chunk']}")
        if r.get("tiering_error"):
            print(f"      tiering_error: {r['tiering_error']}")


async def main() -> int:
    profile = os.getenv("DATABRICKS_CONFIG_PROFILE", "<none>")
    lakets_sql_path = os.getenv("LAKETS_SQL_PATH", DEFAULT_LAKETS_SQL)

    print(f"Profile: {profile}")
    print(f"Project: {PROJECT_ID}  Branch: {BRANCH_ID}  Database: {DATABASE}")
    print(f"LakeTS SQL: {lakets_sql_path}")
    print()

    # ── Step 1: Project + branch ──────────────────────────────────────────
    _print_header("1. provision_lakebase_project_and_branch")
    pb_results = await provision_lakebase_project_and_branch(PROJECT_ID, BRANCH_ID)
    _print_results(pb_results)
    if any(r.get("status") == "failed" for r in pb_results):
        print("\n❌ Project/branch provisioning failed. Aborting.")
        return 1

    # ── Step 2: Register the current SP as an OAuth PG role ───────────────
    role_password = secrets_mod.token_urlsafe(24)  # back-compat placeholder
    _print_header("2. provision_lakebase_role (OAuth-native)")
    role_results = await provision_lakebase_role(
        project_id=PROJECT_ID,
        branch_id=BRANCH_ID,
        database=DATABASE,
    )
    _print_results(role_results)

    # Password-based direct mode is no longer part of the new flow; the
    # autoscale OAuth path below exercises the same code paths regardless.
    password_set = False

    # ── Step 3: Option A — autoscale OAuth ────────────────────────────────
    _print_header("3A. provision_lakebase_table + install_lakets + ChronoTable (autoscale)")
    auto_result = await provision_lakebase_table(
        {
            "mode": "autoscale",
            "project_id": PROJECT_ID,
            "branch_id": BRANCH_ID,
            "database": DATABASE,
            "pg_table": "sensor_readings",
            "lakets_sql_path": lakets_sql_path,
            "tiering": {
                "hot": os.getenv("LAKETS_HOT", "1 hour"),
                "drop": os.getenv("LAKETS_DROP", "7 days"),
                "chunk": os.getenv("LAKETS_CHUNK", "15 minutes"),
            },
        }
    )
    _print_results(auto_result)

    # ── Step 4: Option B — direct mode with role password ─────────────────
    if not password_set:
        _print_header("3B. SKIPPED — no role password was set")
        print("  (OAuth identity lacks ALTER ROLE; expected on autoscale SP runtime)")
        return 0 if auto_result.get("lakets_version") else 1

    # Fetch endpoint host for direct-mode connection
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    parent = f"projects/{PROJECT_ID}/branches/{BRANCH_ID}"
    endpoints = list(w.postgres.list_endpoints(parent=parent))
    if not endpoints:
        print("❌ No endpoints on branch — cannot run direct-mode smoke test")
        return 1
    endpoint = w.postgres.get_endpoint(name=endpoints[0].name)
    pg_host = endpoint.status.hosts.host

    from iot_twin.models.sql_ddl import LAKEBASE_ROLE_NAME

    _print_header("3B. provision_lakebase_table + install_lakets  (direct)")
    direct_result = await provision_lakebase_table(
        {
            "mode": "direct",
            "host": pg_host,
            "port": 5432,
            "user": LAKEBASE_ROLE_NAME,
            "password": role_password,
            "database": DATABASE,
            "pg_table": "sensor_readings",
            "lakets_sql_path": lakets_sql_path,
        }
    )
    _print_results(direct_result)

    print()
    ok = (
        auto_result.get("lakets_version") is not None
        and direct_result.get("lakets_version") is not None
    )
    print(f"{'✓' if ok else '❌'} Both smoke tests " + ("PASSED" if ok else "FAILED"))
    return 0 if ok else 1


if __name__ == "__main__":
    require_profile()  # --profile <name> or $DATABRICKS_CONFIG_PROFILE; no default workspace
    sys.exit(asyncio.run(main()))
