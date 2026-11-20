"""Stop and resume a production cell for real.

Stopping writes a small control file next to the industry's deployed
notebooks. The Kafka producer polls it once a second and stops emitting the
cell machines' sensors, so Lakebase, SSE and the alert engine all see the cell
go quiet; shop utilities keep streaming. Resuming rewrites the file and the
machines' readings flow again within a couple of seconds — the streaming job
itself is never paused.

The control file is written before the state is recorded, so the app never
reports a stop the producer was not told about.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat

logger = logging.getLogger(__name__)

PRODUCTION_STATE_EVENT = "production_state"
# matches PRODUCTION_CONTROL_PLACEHOLDER in client/src/lib/manufacturing-scenario.ts
PRODUCTION_CONTROL_PLACEHOLDER = "__IOT_PRODUCTION_CONTROL_FILE__"
PRODUCTION_STATES = ("running", "stopped")
CONTROL_DIR = "production_control"
WORKSPACE_SUBDIR = ".iot-digital-twin"

# industry -> sensor-name prefixes of the machines a stop halts
CELL_ASSETS: dict[str, tuple[str, ...]] = {
    "manufacturing": ("cnc_1", "assy_line_1"),
}


class ProductionControlError(RuntimeError):
    """The producer could not be told about a stop or resume."""


def control_file_path(workspace_dir: str, industry: str) -> str:
    return f"{workspace_dir.rstrip('/')}/{CONTROL_DIR}/{industry}.json"


def _as_utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def _assets(raw: Any) -> tuple[str, ...]:
    if isinstance(raw, str):
        raw = json.loads(raw or "[]")
    return tuple(str(item) for item in raw or ())


@dataclass(frozen=True)
class ProductionChange:
    """One stop or resume. The latest change per industry is its current state."""

    id: str
    industry: str
    state: str
    assets: tuple[str, ...]
    reason: str | None
    alert_id: str | None
    changed_by: str
    changed_at: datetime

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "industry": self.industry,
            "state": self.state,
            "assets": list(self.assets),
            "reason": self.reason,
            "alert_id": self.alert_id,
            "changed_by": self.changed_by,
            "changed_at": self.changed_at.isoformat(),
        }


def _running_since_boot(industry: str) -> dict[str, Any]:
    return {
        "industry": industry,
        "state": "running",
        "assets": list(CELL_ASSETS.get(industry, ())),
        "reason": None,
        "alert_id": None,
        "changed_by": None,
        "changed_at": None,
    }


def _row_to_change(row: Any, *, id_key: str = "id", at_key: str = "created_at") -> ProductionChange:
    return ProductionChange(
        id=str(row[id_key]) if id_key in row else "",
        industry=str(row["industry"]),
        state=str(row["state"]),
        assets=_assets(row["assets"]),
        reason=row["reason"],
        alert_id=str(row["alert_id"]) if row["alert_id"] else None,
        changed_by=str(row["changed_by"]),
        changed_at=_as_utc(row[at_key]),
    )


def _upload_control(client: Any, path: str, body: bytes) -> None:
    try:
        client.workspace.mkdirs(path.rsplit("/", 1)[0])
    except Exception as exc:  # noqa: BLE001 - the upload below reports real failures
        logger.debug("mkdirs for %s skipped: %s", path, exc)
    client.workspace.upload(path, io.BytesIO(body), format=ImportFormat.AUTO, overwrite=True)


class ProductionControl:
    """Lakebase-backed production state with an in-memory fallback."""

    def __init__(
        self,
        workspace_factory: Callable[[], Any] = WorkspaceClient,
        now_fn: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._workspace_factory = workspace_factory
        self._now = now_fn
        self._client: Any = None
        self._memory: list[ProductionChange] = []
        self._lock = asyncio.Lock()

    def _workspace(self) -> Any:
        if self._client is None:
            self._client = self._workspace_factory()
        return self._client

    async def current(self, industry: str, db: Any | None = None) -> dict[str, Any]:
        latest = await self._latest(industry, db)
        if latest is None:
            return _running_since_boot(industry)
        return latest.to_dict()

    async def _latest(self, industry: str, db: Any | None) -> ProductionChange | None:
        if db is not None:
            try:
                row = await db.fetchrow(
                    "SELECT * FROM production_state WHERE industry = $1", industry
                )
                return _row_to_change(row, id_key="id", at_key="changed_at") if row else None
            except Exception:
                logger.exception("production state read from Lakebase failed; using memory")
        found = [c for c in self._memory if c.industry == industry]
        return found[-1] if found else None

    async def history(
        self, industry: str, since: datetime, db: Any | None = None
    ) -> list[ProductionChange]:
        if db is not None:
            try:
                rows = await db.fetch(
                    """
                    SELECT * FROM production_event
                    WHERE industry = $1 AND created_at >= $2
                    ORDER BY created_at
                    """,
                    industry,
                    since,
                )
                return [_row_to_change(row) for row in rows]
            except Exception:
                logger.exception("production history read from Lakebase failed; using memory")
        return [c for c in self._memory if c.industry == industry and c.changed_at >= since]

    async def set_state(
        self,
        industry: str,
        state: str,
        *,
        actor: str,
        reason: str | None = None,
        alert_id: str | None = None,
        db: Any | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """Stop or resume ``industry``'s cell; returns (state, changed)."""
        if industry not in CELL_ASSETS:
            raise KeyError(industry)
        if state not in PRODUCTION_STATES:
            raise ValueError(f"state must be one of: {', '.join(PRODUCTION_STATES)}")
        async with self._lock:
            latest = await self._latest(industry, db)
            if (latest.state if latest else "running") == state:
                return (latest.to_dict() if latest else _running_since_boot(industry)), False
            change = ProductionChange(
                id=str(uuid.uuid4()),
                industry=industry,
                state=state,
                assets=CELL_ASSETS[industry],
                reason=reason,
                alert_id=alert_id,
                changed_by=actor,
                changed_at=self._now(),
            )
            await self._write_control(change, db)
            await self._persist(change, db)
            return change.to_dict(), True

    async def _control_paths(self, industry: str, db: Any | None) -> list[str]:
        dirs: set[str] = set()
        if db is not None:
            try:
                rows = await db.fetch(
                    """
                    SELECT DISTINCT d.notebook_path
                    FROM pipeline_deployment d
                    JOIN pipeline_setup s ON s.id = d.setup_id
                    WHERE s.industry = $1 AND d.notebook_path IS NOT NULL
                    """,
                    industry,
                )
                dirs = {str(row["notebook_path"]).rsplit("/", 1)[0] for row in rows}
            except Exception:
                logger.exception("could not list %s deployments for production control", industry)
        if not dirs:
            me = await asyncio.to_thread(lambda: self._workspace().current_user.me())
            dirs = {f"/Workspace/Users/{me.user_name}/{WORKSPACE_SUBDIR}"}
        return sorted(control_file_path(d, industry) for d in dirs)

    async def _write_control(self, change: ProductionChange, db: Any | None) -> None:
        body = json.dumps(
            {
                "state": change.state,
                "assets": list(change.assets) if change.state == "stopped" else [],
                "reason": change.reason,
                "changed_by": change.changed_by,
                "changed_at": change.changed_at.isoformat(),
            }
        ).encode()
        try:
            paths = await self._control_paths(change.industry, db)
            for path in paths:
                await asyncio.to_thread(_upload_control, self._workspace(), path, body)
        except Exception as exc:
            raise ProductionControlError(
                f"could not reach the {change.industry} producer: {exc}"
            ) from exc
        logger.info("production %s for %s via %s", change.state, change.industry, ", ".join(paths))

    async def _persist(self, change: ProductionChange, db: Any | None) -> None:
        if db is not None:
            try:
                assets = json.dumps(list(change.assets))
                await db.execute(
                    """
                    INSERT INTO production_state
                        (industry, state, assets, reason, alert_id, changed_by, changed_at)
                    VALUES ($1, $2, $3::jsonb, $4, $5::uuid, $6, $7)
                    ON CONFLICT (industry) DO UPDATE SET
                        state = EXCLUDED.state,
                        assets = EXCLUDED.assets,
                        reason = EXCLUDED.reason,
                        alert_id = EXCLUDED.alert_id,
                        changed_by = EXCLUDED.changed_by,
                        changed_at = EXCLUDED.changed_at
                    """,
                    change.industry,
                    change.state,
                    assets,
                    change.reason,
                    change.alert_id,
                    change.changed_by,
                    change.changed_at,
                )
                await db.execute(
                    """
                    INSERT INTO production_event
                        (id, industry, state, assets, reason, alert_id, changed_by, created_at)
                    VALUES ($1::uuid, $2, $3, $4::jsonb, $5, $6::uuid, $7, $8)
                    """,
                    change.id,
                    change.industry,
                    change.state,
                    assets,
                    change.reason,
                    change.alert_id,
                    change.changed_by,
                    change.changed_at,
                )
                return
            except Exception:
                logger.exception("production state persist failed; recording in memory")
        self._memory.append(change)


def production_control_for(state: Any) -> ProductionControl:
    control = getattr(state, "production_control", None)
    if control is None:
        control = ProductionControl()
        state.production_control = control
    return control


def production_event(change: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": PRODUCTION_STATE_EVENT,
        "industry": change["industry"],
        "production": change,
    }
