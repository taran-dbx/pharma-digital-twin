"""SQL-safe quoting for Unity Catalog identifiers.

Logical FQNs (e.g. from :func:`industry_catalog_service.industry_table`) stay
unquoted for SDK/API use.  Generated Spark/DBSQL strings must go through
:func:`sql_ident` / :func:`sql_fqn` so hyphenated catalog names like
``example-catalog`` parse correctly.
"""

from __future__ import annotations

import re

_VALID_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def sql_ident(name: str) -> str:
    """Return ``name`` wrapped in backticks when it is not a simple identifier."""
    if _VALID_IDENT.match(name):
        return name
    escaped = name.replace("`", "``")
    return f"`{escaped}`"


def sql_fqn(*parts: str) -> str:
    """Join identifier parts with ``.``, quoting each part when required."""
    return ".".join(sql_ident(part) for part in parts)
