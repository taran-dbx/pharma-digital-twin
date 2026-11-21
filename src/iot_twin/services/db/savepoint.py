"""Savepoints for statements that may fail and be retried inside a transaction.

In Postgres a failed statement aborts the whole enclosing transaction: every
later statement fails with "current transaction is aborted" until rollback.
Code that catches an error and retries differently (e.g. the legacy-column
fallback in ``alert_responses``) must therefore run each attempt in a nested
``conn.transaction()`` — an asyncpg savepoint — when it is inside a
transaction. Outside a transaction (pool managers, fakes) this is a no-op.
"""

from __future__ import annotations

import contextlib
from contextlib import AbstractAsyncContextManager
from typing import Any


def in_transaction(db: Any) -> bool:
    check = getattr(db, "is_in_transaction", None)
    return callable(check) and check() is True


def savepoint(db: Any) -> AbstractAsyncContextManager[Any]:
    """``db.transaction()`` (a savepoint) inside a transaction, else a no-op."""
    if in_transaction(db) and callable(getattr(db, "transaction", None)):
        return db.transaction()
    return contextlib.nullcontext()
