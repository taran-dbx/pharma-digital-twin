"""User identity extraction from Databricks Apps OAuth header."""

from __future__ import annotations

import base64
import json
import logging
import os
from collections import OrderedDict

from fastapi import HTTPException, Request

logger = logging.getLogger("iot_digital_twin.auth")

_MAX_CACHE_SIZE = 1024
_user_cache: OrderedDict[str, str] = OrderedDict()
ANONYMOUS_USER = "anonymous"
IDENTITY_BYPASS_ENV = "IOT_ALLOW_ANONYMOUS_IDENTITY"


async def get_current_user(request: Request) -> str:
    """FastAPI dependency that resolves the current user email.

    In Databricks Apps the platform injects ``x-forwarded-access-token``
    which is a JWT whose ``sub`` claim contains the user email.
    We decode the payload directly (no network call needed since the
    proxy already validated the token).
    """
    token = request.headers.get("x-forwarded-access-token")
    if not token:
        return "anonymous"
    if token in _user_cache:
        _user_cache.move_to_end(token)
        return _user_cache[token]
    try:
        payload_b64 = token.split(".")[1]
        payload_b64 += "=" * (4 - len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        username = payload.get("sub") or ANONYMOUS_USER
        if not str(username).strip():
            username = ANONYMOUS_USER
        _user_cache[token] = username
        if len(_user_cache) > _MAX_CACHE_SIZE:
            _user_cache.popitem(last=False)
        return username
    except Exception as exc:
        logger.warning("Failed to decode user identity from token: %s", exc)
        return ANONYMOUS_USER


def is_apps_runtime() -> bool:
    """True when Databricks Apps injects a runtime app name or host binding."""
    return bool(
        (os.getenv("DATABRICKS_APP_NAME") or "").strip()
        or (os.getenv("DATABRICKS_APP_PORT") or "").strip()
        or (os.getenv("DATABRICKS_APP_URL") or "").strip()
    )


def anonymous_identity_allowed() -> bool:
    """Local/test bypass must be an explicit env flag — never implied by missing headers."""
    raw = (os.getenv(IDENTITY_BYPASS_ENV) or "").strip().lower()
    return raw in {"1", "true", "yes"}


def _valid_identity(user: str) -> bool:
    value = (user or "").strip()
    return bool(value) and value != ANONYMOUS_USER


async def require_identity(request: Request) -> str:
    """Reject missing/invalid forwarded identity in Databricks Apps runtimes."""
    user = await get_current_user(request)
    if not is_apps_runtime() or anonymous_identity_allowed():
        return user
    if not _valid_identity(user):
        raise HTTPException(status_code=401, detail="identity required")
    return user
