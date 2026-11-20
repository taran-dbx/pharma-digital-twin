"""Source registry service — prevent duplicate pipeline source bindings."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("iot_digital_twin.services.source_registry")


# ---------------------------------------------------------------------------
# Key derivation
# ---------------------------------------------------------------------------


def derive_source_key(source_type: str, source_config: dict[str, Any]) -> str:
    """Return a normalized, lowercased key for the given source.

    Raises:
        ValueError: If source_type is not recognized.
        KeyError: If a required field is missing from source_config.
    """
    st = source_type.lower()

    if st == "kafka":
        servers_raw: str = source_config["bootstrap_servers"]
        servers = sorted(s.strip() for s in servers_raw.split(","))
        topic: str = source_config["topic"]
        return f"{','.join(servers)}::{topic}".lower()

    if st == "kinesis":
        region: str = source_config["region"]
        stream_name: str = source_config["stream_name"]
        return f"{region}::{stream_name}".lower()

    if st == "eventhub":
        namespace: str = source_config["namespace"]
        eventhub_name: str = source_config["eventhub_name"]
        consumer_group: str = source_config["consumer_group"]
        return f"{namespace}::{eventhub_name}::{consumer_group}".lower()

    if st == "rabbitmq":
        host: str = source_config["host"]
        port: str = str(source_config["port"])
        queue_name: str = source_config["queue_name"]
        return f"{host}:{port}::{queue_name}".lower()

    if st in ("pubsub", "pub/sub"):
        project_id: str = source_config["project_id"]
        subscription_id: str = source_config["subscription_id"]
        return f"{project_id}::{subscription_id}".lower()

    if st == "zerobus":
        catalog: str = source_config["catalog"]
        schema: str = source_config["schema"]
        table_name: str = source_config["table_name"]
        return f"{catalog}::{schema}::{table_name}".lower()

    raise ValueError(f"Unknown source_type: {source_type!r}")


# ---------------------------------------------------------------------------
# Availability check
# ---------------------------------------------------------------------------


async def check_source_available(
    db: Any,
    source_type: str,
    source_config: dict[str, Any],
) -> dict[str, Any]:
    """Check whether a source is available (not claimed by any active pipeline).

    Returns:
        {"available": True} if unclaimed, or
        {"available": False, "claimed_by_pipeline": str, "claimed_by": str}
    """
    source_key = derive_source_key(source_type, source_config)
    row = await db.fetchrow(
        """
        SELECT pipeline_id, claimed_by
        FROM source_bindings
        WHERE source_type = $1
          AND source_key = $2
          AND status = 'active'
        """,
        source_type.lower(),
        source_key,
    )
    if row is None:
        return {"available": True}
    return {
        "available": False,
        "claimed_by_pipeline": str(row["pipeline_id"]),
        "claimed_by": row["claimed_by"],
    }


# ---------------------------------------------------------------------------
# Claim
# ---------------------------------------------------------------------------


async def claim_source(
    db: Any,
    pipeline_id: str,
    source_type: str,
    source_config: dict[str, Any],
    user_email: str,
    partition_count: int | None = None,
) -> dict[str, Any]:
    """Claim a source for the given pipeline.

    Returns:
        {"claimed": True} on success, or
        {"claimed": False, "claimed_by_pipeline": str} if already claimed.
    """
    source_key = derive_source_key(source_type, source_config)
    existing = await db.fetchrow(
        """
        SELECT pipeline_id
        FROM source_bindings
        WHERE source_type = $1
          AND source_key = $2
          AND status = 'active'
        """,
        source_type.lower(),
        source_key,
    )
    if existing is not None:
        return {"claimed": False, "claimed_by_pipeline": str(existing["pipeline_id"])}

    try:
        await db.execute(
            """
            INSERT INTO source_bindings
                (pipeline_id, source_type, source_key, partition_count, claimed_by, status)
            VALUES ($1::uuid, $2, $3, $4, $5, 'active')
            """,
            pipeline_id,
            source_type.lower(),
            source_key,
            partition_count,
            user_email,
        )
    except Exception as e:
        error_name = type(e).__name__
        if "UniqueViolation" in error_name or "unique" in str(e).lower():
            # Race condition: another request claimed it between our check and insert
            logger.info("Source claim race detected for pipeline=%s, re-checking", pipeline_id)
            existing = await db.fetchrow(
                """
                SELECT pipeline_id, claimed_by
                FROM source_bindings
                WHERE source_type = $1
                  AND source_key = $2
                  AND status = 'active'
                """,
                source_type.lower(),
                source_key,
            )
            if existing:
                return {
                    "claimed": False,
                    "claimed_by_pipeline": str(existing["pipeline_id"]),
                    "claimed_by": existing["claimed_by"],
                }
        raise
    return {"claimed": True}


# ---------------------------------------------------------------------------
# Release
# ---------------------------------------------------------------------------


async def release_source(db: Any, pipeline_id: str) -> None:
    """Soft-delete all active source bindings for the given pipeline.

    Sets status to 'released' rather than deleting rows.
    """
    await db.execute(
        """
        UPDATE source_bindings
        SET status = 'released'
        WHERE pipeline_id = $1::uuid
          AND status = 'active'
        """,
        pipeline_id,
    )
