"""Throwaway local-Postgres fixtures for the AI action-plan tests (skip without Postgres)."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio

from tests._local_pg import local_postgres


@pytest.fixture(scope="module")
def pg_server() -> Iterator:
    with local_postgres() as server:
        yield server


@pytest_asyncio.fixture()
async def pg(pg_server) -> AsyncIterator:
    asyncpg = pytest.importorskip("asyncpg")
    if pg_server is None:
        pytest.skip("no local Postgres binaries (initdb/pg_ctl)")
    name = f"g_{uuid.uuid4().hex[:12]}"
    admin = await asyncpg.connect(
        host=pg_server.socket_dir, port=pg_server.port, user=pg_server.superuser
    )
    await admin.execute(f"CREATE DATABASE {name}")
    conn = await asyncpg.connect(
        host=pg_server.socket_dir, port=pg_server.port, user=pg_server.superuser, database=name
    )
    try:
        yield conn, pg_server, name
    finally:
        await conn.close()
        await admin.execute(f"DROP DATABASE {name}")
        await admin.close()
