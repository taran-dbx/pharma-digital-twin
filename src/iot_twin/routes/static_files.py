"""Static file serving for the React SPA."""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import FileResponse

from iot_twin.core.paths import SPA_INDEX

router = APIRouter()


@router.get("/")
async def root():
    return FileResponse(SPA_INDEX)
