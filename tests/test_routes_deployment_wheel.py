"""Upload-wheel endpoint uses the runtime-wheel augmenter [qa]."""

from __future__ import annotations

import ast
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from iot_twin.core.state import AppState
from iot_twin.routes import deployment as deployment_routes


def _build_app() -> FastAPI:
    app = FastAPI()
    app.state.app_state = AppState()
    app.include_router(deployment_routes.router)
    return app


class TestUploadWheel:
    @pytest.mark.asyncio
    async def test_upload_sends_augmented_bytes_c7315_regression(self, tmp_path: Path) -> None:
        """[C7315][regression] Wheel upload posts augmented bytes, same workspace filename."""
        app = _build_app()
        transport = ASGITransport(app=app)
        user = MagicMock(user_name="tester@databricks.com")
        workspace = MagicMock()
        workspace.current_user.me.return_value = user
        augmented = b"AUGMENTED-WHEEL-BYTES"
        with (
            patch.object(deployment_routes, "WorkspaceClient", return_value=workspace),
            patch.object(
                deployment_routes,
                "augment_runtime_wheel",
                return_value=augmented,
            ) as augment,
            patch.object(
                deployment_routes,
                "bundled_wheel_path",
                return_value=tmp_path / deployment_routes.WHEEL_FILENAME,
            ),
        ):
            (tmp_path / deployment_routes.WHEEL_FILENAME).write_bytes(b"base-wheel")
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post("/api/test-connectivity/upload-wheel")
        assert resp.status_code == 200
        body = resp.json()
        assert body["wheel_path"].endswith(f"/{deployment_routes.WHEEL_FILENAME}")
        augment.assert_called_once()
        imported = workspace.workspace.import_.call_args.kwargs
        assert imported["overwrite"] is True
        assert imported["path"].endswith(deployment_routes.WHEEL_FILENAME)

    @pytest.mark.asyncio
    async def test_upload_errors_are_generic_c7315_defect(self, tmp_path: Path) -> None:
        """[C7315][defect] Upload failures return a generic error and log detail server-side."""
        app = _build_app()
        transport = ASGITransport(app=app)
        with (
            patch.object(
                deployment_routes,
                "bundled_wheel_path",
                return_value=tmp_path / deployment_routes.WHEEL_FILENAME,
            ),
            patch.object(
                deployment_routes,
                "augment_runtime_wheel",
                side_effect=RuntimeError("secret token xyz"),
            ),
            patch.object(deployment_routes, "WorkspaceClient"),
        ):
            (tmp_path / deployment_routes.WHEEL_FILENAME).write_bytes(b"base-wheel")
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post("/api/test-connectivity/upload-wheel")
        assert resp.status_code == 500
        assert "secret" not in str(resp.json())
        assert resp.json()["error"] == "Wheel upload failed"

    def test_upload_wheel_has_no_inline_imports_c7315_defect(self) -> None:
        """[C7315][defect] upload_wheel does not introduce inline imports."""
        src = Path(deployment_routes.__file__).read_text(encoding="utf-8")
        tree = ast.parse(src)
        for node in tree.body:
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "upload_wheel":
                for child in ast.walk(node):
                    if child is node:
                        continue
                    if isinstance(child, (ast.Import, ast.ImportFrom)):
                        pytest.fail("upload_wheel contains an inline import")
                return
        pytest.fail("upload_wheel not found")
