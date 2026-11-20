"""Tests for iot_twin.core.auth — JWT-based user identity extraction."""

from __future__ import annotations

import base64
import json
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from iot_twin.core import auth
from iot_twin.core.auth import get_current_user, require_identity


@pytest.fixture(autouse=True)
def _clear_user_cache() -> None:
    """Ensure each test starts with a fresh user cache."""
    auth._user_cache.clear()


def _make_jwt(payload: dict) -> str:
    """Build a minimal unsigned JWT (header.payload.signature)."""
    header = base64.urlsafe_b64encode(json.dumps({"alg": "none"}).encode()).rstrip(b"=").decode()
    body = base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode()
    return f"{header}.{body}.fakesig"


def _request_with_token(token: str | None) -> MagicMock:
    """Create a mock FastAPI Request with the given access token header."""
    request = MagicMock()
    if token is None:
        request.headers.get.return_value = None
    else:
        request.headers.get.return_value = token
    return request


class TestGetCurrentUserNoToken:
    """When no token header is present, return 'anonymous'."""

    @pytest.mark.asyncio
    async def test_returns_anonymous_when_header_missing(self) -> None:
        request = _request_with_token(None)
        result = await get_current_user(request)
        assert result == "anonymous"

    @pytest.mark.asyncio
    async def test_returns_anonymous_when_header_empty_string(self) -> None:
        request = MagicMock()
        request.headers.get.return_value = ""
        result = await get_current_user(request)
        assert result == "anonymous"


class TestGetCurrentUserValidToken:
    """When a valid JWT is provided, extract the 'sub' claim."""

    @pytest.mark.asyncio
    async def test_decodes_sub_claim(self) -> None:
        token = _make_jwt({"sub": "alice@company.com"})
        request = _request_with_token(token)
        result = await get_current_user(request)
        assert result == "alice@company.com"

    @pytest.mark.asyncio
    async def test_returns_anonymous_when_sub_missing(self) -> None:
        token = _make_jwt({"name": "Alice"})
        request = _request_with_token(token)
        result = await get_current_user(request)
        assert result == "anonymous"

    @pytest.mark.asyncio
    async def test_returns_anonymous_when_sub_is_empty(self) -> None:
        token = _make_jwt({"sub": ""})
        request = _request_with_token(token)
        result = await get_current_user(request)
        assert result == "anonymous"


class TestGetCurrentUserMalformedToken:
    """When the token cannot be decoded, return 'anonymous'."""

    @pytest.mark.asyncio
    async def test_returns_anonymous_for_garbage_token(self) -> None:
        request = _request_with_token("not.a.valid.jwt")
        result = await get_current_user(request)
        assert result == "anonymous"

    @pytest.mark.asyncio
    async def test_returns_anonymous_for_single_segment_token(self) -> None:
        request = _request_with_token("single-segment")
        result = await get_current_user(request)
        assert result == "anonymous"


class TestGetCurrentUserCaching:
    """Decoded tokens are cached for performance."""

    @pytest.mark.asyncio
    async def test_caches_decoded_token(self) -> None:
        token = _make_jwt({"sub": "bob@company.com"})
        request = _request_with_token(token)

        first = await get_current_user(request)
        second = await get_current_user(request)

        assert first == "bob@company.com"
        assert second == "bob@company.com"
        assert token in auth._user_cache

    @pytest.mark.asyncio
    async def test_different_tokens_cached_separately(self) -> None:
        token_a = _make_jwt({"sub": "alice@co.com"})
        token_b = _make_jwt({"sub": "bob@co.com"})

        result_a = await get_current_user(_request_with_token(token_a))
        result_b = await get_current_user(_request_with_token(token_b))

        assert result_a == "alice@co.com"
        assert result_b == "bob@co.com"
        assert len(auth._user_cache) == 2


class TestRequireIdentity:
    @pytest.mark.asyncio
    async def test_apps_runtime_rejects_anonymous_c7601_defect(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7601][defect] Databricks Apps runtime without identity returns 401."""
        monkeypatch.setenv("DATABRICKS_APP_NAME", "iot-digital-twin")
        monkeypatch.delenv("IOT_ALLOW_ANONYMOUS_IDENTITY", raising=False)
        with pytest.raises(HTTPException) as exc:
            await require_identity(_request_with_token(None))
        assert exc.value.status_code == 401

    @pytest.mark.asyncio
    async def test_explicit_bypass_allows_local_c7601_smoke(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7601][smoke] Explicit local bypass remains available."""
        monkeypatch.setenv("DATABRICKS_APP_NAME", "iot-digital-twin")
        monkeypatch.setenv("IOT_ALLOW_ANONYMOUS_IDENTITY", "1")
        result = await require_identity(_request_with_token(None))
        assert result == "anonymous"

    @pytest.mark.asyncio
    async def test_valid_token_accepted_in_runtime_c7601_regression(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """[C7601][regression] Forwarded identity is accepted in Apps runtime."""
        monkeypatch.setenv("DATABRICKS_APP_NAME", "iot-digital-twin")
        monkeypatch.delenv("IOT_ALLOW_ANONYMOUS_IDENTITY", raising=False)
        token = _make_jwt({"sub": "ops@example.com"})
        result = await require_identity(_request_with_token(token))
        assert result == "ops@example.com"
