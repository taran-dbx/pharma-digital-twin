"""Integration tests for the iot-app-backend data-model DDL (Phase 1, Task 1.2).

Runs against the live ``iot-app-backend`` Lakebase project inside a throwaway
schema (see ``app_backend_conn`` in conftest).  Asserts the three tables plus
their FK (ON DELETE CASCADE) / UNIQUE constraints exist after the idempotent
``ensure_*`` creators run, and that a setup delete cascades to deployments and
resources (no orphans).
"""

from __future__ import annotations

import json

import pytest

from iot_twin.models import app_schema

pytestmark = pytest.mark.asyncio


async def _current_schema(conn) -> str:
    return await conn.fetchval("SELECT current_schema()")


async def _table_exists(conn, table: str) -> bool:
    schema = await _current_schema(conn)
    return bool(
        await conn.fetchval(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2",
            schema,
            table,
        )
    )


async def _fk_delete_rule(conn, table: str, column: str) -> str | None:
    """Return the ON DELETE rule for the FK on ``table.column`` (or None)."""
    schema = await _current_schema(conn)
    return await conn.fetchval(
        """
        SELECT rc.delete_rule
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        JOIN information_schema.referential_constraints rc
          ON tc.constraint_name = rc.constraint_name
         AND tc.constraint_schema = rc.constraint_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = $1
          AND tc.table_name = $2
          AND kcu.column_name = $3
        LIMIT 1
        """,
        schema,
        table,
        column,
    )


async def _has_unique(conn, table: str, columns: set[str]) -> bool:
    """True if a UNIQUE constraint/index covers exactly ``columns`` on ``table``."""
    schema = await _current_schema(conn)
    rows = await conn.fetch(
        """
        SELECT tc.constraint_name, array_agg(kcu.column_name) AS cols
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'UNIQUE'
          AND tc.table_schema = $1
          AND tc.table_name = $2
        GROUP BY tc.constraint_name
        """,
        schema,
        table,
    )
    return any(set(r["cols"]) == columns for r in rows)


async def test_ensure_creates_all_three_tables(app_backend_conn) -> None:
    await app_schema.ensure_app_schema(app_backend_conn)
    assert await _table_exists(app_backend_conn, "pipeline_setup")
    assert await _table_exists(app_backend_conn, "pipeline_deployment")
    assert await _table_exists(app_backend_conn, "provisioned_resource")


async def test_ensure_is_idempotent(app_backend_conn) -> None:
    await app_schema.ensure_app_schema(app_backend_conn)
    # Running again must not raise.
    await app_schema.ensure_app_schema(app_backend_conn)
    assert await _table_exists(app_backend_conn, "pipeline_setup")


async def test_pipeline_setup_industry_unique(app_backend_conn) -> None:
    await app_schema.ensure_app_schema(app_backend_conn)
    assert await _has_unique(app_backend_conn, "pipeline_setup", {"industry"})


async def test_pipeline_setup_has_per_mode_config_column(app_backend_conn) -> None:
    await app_schema.ensure_app_schema(app_backend_conn)
    schema = await _current_schema(app_backend_conn)
    data_type = await app_backend_conn.fetchval(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_schema = $1 AND table_name = 'pipeline_setup' "
        "AND column_name = 'mode_configs'",
        schema,
    )
    assert data_type == "jsonb"


async def test_deployment_fk_cascade_and_unique(app_backend_conn) -> None:
    await app_schema.ensure_app_schema(app_backend_conn)
    assert await _fk_delete_rule(app_backend_conn, "pipeline_deployment", "setup_id") == "CASCADE"
    assert await _has_unique(app_backend_conn, "pipeline_deployment", {"setup_id", "mode"})


async def test_resource_fks(app_backend_conn) -> None:
    await app_schema.ensure_app_schema(app_backend_conn)
    assert await _fk_delete_rule(app_backend_conn, "provisioned_resource", "setup_id") == "CASCADE"
    # deployment_id FK is present and nullable.
    assert (
        await _fk_delete_rule(app_backend_conn, "provisioned_resource", "deployment_id") is not None
    )
    schema = await _current_schema(app_backend_conn)
    nullable = await app_backend_conn.fetchval(
        "SELECT is_nullable FROM information_schema.columns "
        "WHERE table_schema = $1 AND table_name = 'provisioned_resource' "
        "AND column_name = 'deployment_id'",
        schema,
    )
    assert nullable == "YES"


async def test_delete_setup_cascades_to_children(app_backend_conn) -> None:
    conn = app_backend_conn
    await app_schema.ensure_app_schema(conn)

    setup_id = await conn.fetchval(
        "INSERT INTO pipeline_setup (industry) VALUES ($1) RETURNING id",
        "pharmaceutical",
    )
    dep_id = await conn.fetchval(
        "INSERT INTO pipeline_deployment (setup_id, mode) VALUES ($1, $2) RETURNING id",
        setup_id,
        "realtime",
    )
    await conn.execute(
        "INSERT INTO provisioned_resource "
        "(setup_id, deployment_id, resource_type, identifier, metadata) "
        "VALUES ($1, $2, $3, $4, $5::jsonb)",
        setup_id,
        dep_id,
        "kafka_topic",
        "iot.pharmaceutical.realtime",
        json.dumps({"partitions": 3}),
    )

    # Deleting the setup must cascade to deployment + resource rows.
    await conn.execute("DELETE FROM pipeline_setup WHERE id = $1", setup_id)
    assert (
        await conn.fetchval(
            "SELECT COUNT(*) FROM pipeline_deployment WHERE setup_id = $1", setup_id
        )
        == 0
    )
    assert (
        await conn.fetchval(
            "SELECT COUNT(*) FROM provisioned_resource WHERE setup_id = $1", setup_id
        )
        == 0
    )
