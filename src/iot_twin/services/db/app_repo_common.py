"""Shared helpers for the app-backend repositories.

Keeps the repos DB-agnostic: JSONB columns are written as ``json.dumps`` text
with an explicit ``::jsonb`` cast and decoded with ``json.loads`` on read, so
the same code works against a raw ``asyncpg.Connection`` (used by the tests) or
the ``LakebaseConnectionManager`` / ``PipelineLakebaseHandle`` (no jsonb codec
registered on either).
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any, Protocol


class DBExecutor(Protocol):
    """Minimal async DB surface shared by every executor in the codebase."""

    async def fetch(self, query: str, *args) -> list: ...
    async def fetchrow(self, query: str, *args): ...
    async def fetchval(self, query: str, *args): ...
    async def execute(self, query: str, *args): ...


def row_to_dict(row: Any, json_fields: Iterable[str] = ()) -> dict | None:
    """Convert an asyncpg ``Record`` (or dict) to a plain dict.

    Any column in ``json_fields`` that comes back as a JSON string is decoded
    to a Python object (asyncpg returns JSONB as ``str`` without a codec).
    """
    if row is None:
        return None
    data = dict(row)
    for field in json_fields:
        value = data.get(field)
        if isinstance(value, (str, bytes, bytearray)):
            try:
                data[field] = json.loads(value)
            except (ValueError, TypeError):
                pass
    return data


def build_insert(
    table: str,
    values: Mapping[str, Any],
    json_fields: Iterable[str] = (),
) -> tuple[str, list[Any]]:
    """Return an ``INSERT ... RETURNING *`` statement + positional args.

    ``None`` values are omitted so column defaults apply.  Columns named in
    ``json_fields`` get a ``::jsonb`` cast and their value is ``json.dumps``-ed.
    """
    json_set = set(json_fields)
    columns: list[str] = []
    placeholders: list[str] = []
    args: list[Any] = []
    idx = 1
    for column, value in values.items():
        if value is None:
            continue
        columns.append(column)
        if column in json_set:
            placeholders.append(f"${idx}::jsonb")
            args.append(json.dumps(value))
        else:
            placeholders.append(f"${idx}")
            args.append(value)
        idx += 1
    cols_sql = ", ".join(columns)
    vals_sql = ", ".join(placeholders)
    query = f"INSERT INTO {table} ({cols_sql}) VALUES ({vals_sql}) RETURNING *"
    return query, args


def build_update(
    table: str,
    row_id: Any,
    values: Mapping[str, Any],
    json_fields: Iterable[str] = (),
    touch_updated_at: bool = True,
) -> tuple[str, list[Any]]:
    """Return an ``UPDATE ... WHERE id = $n RETURNING *`` statement + args.

    Only the provided columns are set.  ``touch_updated_at`` appends
    ``updated_at = now()`` for tables that carry that column.
    """
    json_set = set(json_fields)
    assignments: list[str] = []
    args: list[Any] = []
    idx = 1
    for column, value in values.items():
        if column in json_set:
            assignments.append(f"{column} = ${idx}::jsonb")
            args.append(json.dumps(value))
        else:
            assignments.append(f"{column} = ${idx}")
            args.append(value)
        idx += 1
    if touch_updated_at:
        assignments.append("updated_at = now()")
    set_sql = ", ".join(assignments)
    args.append(row_id)
    query = f"UPDATE {table} SET {set_sql} WHERE id = ${idx} RETURNING *"
    return query, args
