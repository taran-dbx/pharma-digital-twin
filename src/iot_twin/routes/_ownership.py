"""Request-boundary ownership checks shared by the routes.

Any route that accepts a resource name in its body (Lakebase project ids
in particular) checks it here first, so a request can never make the app touch
a resource outside this deployment's ``RESOURCE_PREFIX`` namespace. A rejected
name is a client error (403), not a server failure.
"""

from __future__ import annotations

from fastapi.responses import JSONResponse

from iot_twin.core.config import NotOwnedError, assert_owned_name

FORBIDDEN = 403


def not_owned_response(exc: NotOwnedError) -> JSONResponse:
    """Return the 403 body used for every ownership rejection."""
    return JSONResponse(status_code=FORBIDDEN, content={"error": str(exc)})


def reject_foreign_project(project_id: str | None) -> JSONResponse | None:
    """Return a 403 response when ``project_id`` is set but not owned, else ``None``."""
    if not project_id:
        return None
    try:
        assert_owned_name(project_id, what="Lakebase project_id")
    except NotOwnedError as exc:
        return not_owned_response(exc)
    return None
