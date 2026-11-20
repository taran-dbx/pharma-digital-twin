"""Pipeline helper functions — row conversion and resource tracking."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from iot_twin.core.lakebase import LakebaseConnectionManager

logger = logging.getLogger("iot_digital_twin.services.pipeline")


def pipeline_row_to_dict(row) -> dict:
    """Convert an asyncpg Record to a JSON-safe dict."""
    if row is None:
        return {}
    d = dict(row)
    for key in ("id", "created_at", "updated_at"):
        if key in d:
            d[key] = str(d[key])
    for key in ("sensor_config", "source_config", "sink_config", "resources"):
        if key in d and isinstance(d[key], str):
            try:
                d[key] = json.loads(d[key])
            except (json.JSONDecodeError, TypeError):
                pass
    return d


async def track_resource(
    db: LakebaseConnectionManager | None,
    db_available: bool,
    pipeline_id: str | None,
    key: str,
    value: str,
) -> None:
    """Merge a key into the pipeline's resources JSONB column."""
    if not pipeline_id or not db_available or not db:
        return
    try:
        await db.execute(
            """
            UPDATE pipelines
            SET resources = resources || $2::jsonb,
                updated_at = NOW()
            WHERE id = $1::uuid
            """,
            pipeline_id,
            json.dumps({key: value}),
        )
    except Exception as exc:
        logger.warning("Failed to track resource %s for pipeline %s: %s", key, pipeline_id, exc)
