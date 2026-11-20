"""Tests for LakeTS setup on an industry Lakebase branch (plan 2.3 / spec §1.9–1.10)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from iot_twin.services.industry import industry_setup_service as iss


@pytest.fixture(autouse=True)
def _mock_shadow_reconciliation(monkeypatch):
    reconcile = AsyncMock(return_value={"status": "already_exists"})
    monkeypatch.setattr(iss.lakets_service, "reconcile_chronotable_shadow", reconcile)
    return reconcile


def _make_conn():
    conn = MagicMock()
    conn.execute = AsyncMock(return_value="OK")
    # fetchval: first the chronotable-id probe, then partitions-created count.
    conn.fetchval = AsyncMock(side_effect=[42, 96])
    return conn


def steps_of(result: dict) -> dict[str, str]:
    """Map the result's step list to {step_name: status}."""
    return {s["step"]: s["status"] for s in result["steps"]}


@pytest.mark.asyncio
async def test_setup_installs_creates_chronotable_and_partitions(tmp_path: Path) -> None:
    conn = _make_conn()
    ws = MagicMock()

    enable_sync = AsyncMock(return_value={"status": "created"})
    with (
        patch.object(
            iss.lakets_service, "resolve_lakets_source", return_value=tmp_path / "lakets.sql"
        ),
        patch.object(iss.lakets_service, "install_lakets", new=AsyncMock(return_value="1.2.3")),
        patch.object(iss.lakets_service, "create_chronotable", new=AsyncMock(return_value=False)),
        patch.object(
            iss.lakets_service, "create_rollup", new=AsyncMock(return_value={"status": "created"})
        ),
        patch.object(
            iss.lakets_service,
            "enable_rollup_invalidation",
            new=AsyncMock(return_value={"status": "created"}),
        ),
        patch.object(
            iss.lakets_service, "enable_lvc", new=AsyncMock(return_value={"status": "created"})
        ),
        patch.object(iss.lakets_service, "reconcile_chronotable_shadow", new=enable_sync),
        patch.object(
            iss.lakets_deploy_service,
            "deploy_lakets_ops_jobs",
            return_value=[{"job": "PM", "job_id": 1, "status": "created"}],
        ),
    ):
        result = await iss.setup_lakets_on_branch(
            "manufacturing",
            conn=conn,
            ws=ws,
            deploy_ops_jobs=True,
        )

    # CDF shadow prepared: enable_sync called on the sensor_readings ChronoTable
    # (public schema) so the managed Lakebase->Delta sync has a shadow to sync.
    enable_sync.assert_awaited_once_with(
        conn,
        shadow_table="_shadow_manufacturing_sensor_readings",
        table="sensor_readings",
        schema="public",
    )
    assert result["cdf_shadow"] == "created"
    assert steps_of(result)["enable_sync"] == "created"

    # sensor_readings DDL executed from the SSOT.
    ddl_arg = conn.execute.call_args_list[0].args[0]
    assert "CREATE TABLE IF NOT EXISTS sensor_readings" in ddl_arg
    assert "nextval('sensor_readings_id_seq')" in ddl_arg

    # partitions pre-created with the 96-chunk (~24h) forward window.
    # fetchval(_ENSURE_PARTITIONS_SQL, chrono_id, future_count) → args (sql, 42, 96).
    part_call = next(
        call
        for call in conn.fetchval.call_args_list
        if call.args and call.args[0] == iss._ENSURE_PARTITIONS_SQL
    )
    assert part_call.args[1] == 42  # chronotable id
    assert part_call.args[2] == iss.DEFAULT_FUTURE_PARTITIONS == 96

    assert result["lakets_version"] == "1.2.3"
    assert result["partitions_created"] == 96
    steps = {s["step"]: s["status"] for s in result["steps"]}
    assert steps["create_chronotable"] == "created"
    assert steps["precreate_partitions"] == "ok"


@pytest.mark.asyncio
async def test_setup_configures_sensor_rollup_invalidation_and_lvc_c7224_regression(
    tmp_path: Path,
) -> None:
    """[C7224][regression] Idempotently configure LakeTS observability resources."""
    conn = _make_conn()
    create_rollup = AsyncMock(
        return_value={"resource": "LakeTS rollup sensor_1m", "status": "already_exists"}
    )
    enable_invalidation = AsyncMock(
        return_value={
            "resource": "LakeTS invalidation sensor_1m",
            "status": "already_exists",
        }
    )
    enable_lvc = AsyncMock(
        return_value={
            "resource": "LakeTS LVC public.sensor_readings",
            "status": "already_exists",
        }
    )

    with (
        patch.object(
            iss.lakets_service,
            "resolve_lakets_source",
            return_value=tmp_path / "lakets.sql",
        ),
        patch.object(
            iss.lakets_service,
            "install_lakets",
            new=AsyncMock(return_value="1.2.3"),
        ),
        patch.object(
            iss.lakets_service,
            "create_chronotable",
            new=AsyncMock(return_value=True),
        ),
        patch.object(
            iss.lakets_service,
            "enable_sync",
            new=AsyncMock(return_value={"status": "already_exists"}),
        ),
        patch.object(
            iss.lakets_service,
            "create_rollup",
            new=create_rollup,
        ),
        patch.object(
            iss.lakets_service,
            "enable_rollup_invalidation",
            new=enable_invalidation,
            create=True,
        ),
        patch.object(
            iss.lakets_service,
            "enable_lvc",
            new=enable_lvc,
            create=True,
        ),
    ):
        result = await iss.setup_lakets_on_branch(
            "manufacturing",
            conn=conn,
            ws=MagicMock(),
            deploy_ops_jobs=False,
        )

    create_rollup.assert_awaited_once()
    rollup_kwargs = create_rollup.await_args.kwargs
    assert rollup_kwargs["table"] == iss.SENSOR_TABLE
    assert rollup_kwargs["name"] == "sensor_1m"
    assert rollup_kwargs["bucket"] == "1 minute"
    assert rollup_kwargs["group_columns"] == ["sensor_name"]

    enable_invalidation.assert_awaited_once_with(conn, "sensor_1m")
    enable_lvc.assert_awaited_once_with(
        conn,
        table=iss.SENSOR_TABLE,
        key_columns=["sensor_name"],
        value_columns=[
            "event_time",
            "industry",
            "sensor_type",
            "unit",
            "value",
            "fault_active",
        ],
        schema="public",
    )
    steps = steps_of(result)
    assert steps["create_rollup"] == "already_exists"
    assert steps["enable_rollup_invalidation"] == "already_exists"
    assert steps["enable_lvc"] == "already_exists"


@pytest.mark.asyncio
async def test_failed_rollup_skips_invalidation_but_keeps_lvc_c7224_regression(
    tmp_path: Path,
) -> None:
    """[C7224][regression] Keep LVC independent from a mismatched RollUp."""
    conn = _make_conn()
    invalidation = AsyncMock()
    enable_lvc = AsyncMock(return_value={"status": "already_exists"})

    with (
        patch.object(
            iss.lakets_service,
            "resolve_lakets_source",
            return_value=tmp_path / "lakets.sql",
        ),
        patch.object(
            iss.lakets_service,
            "install_lakets",
            new=AsyncMock(return_value="1.2.3"),
        ),
        patch.object(
            iss.lakets_service,
            "create_chronotable",
            new=AsyncMock(return_value=True),
        ),
        patch.object(
            iss.lakets_service,
            "create_rollup",
            new=AsyncMock(return_value={"status": "failed", "error": "definition mismatch"}),
        ),
        patch.object(
            iss.lakets_service,
            "enable_rollup_invalidation",
            new=invalidation,
        ),
        patch.object(
            iss.lakets_service,
            "enable_lvc",
            new=enable_lvc,
        ),
        patch.object(
            iss.lakets_service,
            "enable_sync",
            new=AsyncMock(return_value={"status": "already_exists"}),
        ),
    ):
        result = await iss.setup_lakets_on_branch(
            "manufacturing",
            conn=conn,
            ws=MagicMock(),
            deploy_ops_jobs=False,
        )

    invalidation.assert_not_awaited()
    enable_lvc.assert_awaited_once()
    assert steps_of(result)["enable_rollup_invalidation"] == "skipped"
    assert steps_of(result)["enable_lvc"] == "already_exists"


@pytest.mark.asyncio
async def test_setup_records_jobs_and_chronotable_in_ledger(tmp_path: Path) -> None:
    conn = _make_conn()
    ws = MagicMock()
    recorded: list[dict] = []

    async def _rec(_db, **kw):
        recorded.append(kw)

    with (
        patch.object(
            iss.lakets_service, "resolve_lakets_source", return_value=tmp_path / "lakets.sql"
        ),
        patch.object(iss.lakets_service, "install_lakets", new=AsyncMock(return_value="1.0")),
        patch.object(iss.lakets_service, "create_chronotable", new=AsyncMock(return_value=False)),
        patch.object(
            iss.lakets_deploy_service,
            "deploy_lakets_ops_jobs",
            return_value=[
                {"job": "PM", "job_id": 1, "status": "created"},
                {"job": "Rollup", "job_id": 2, "status": "created"},
            ],
        ),
        patch.object(iss, "record_resource", side_effect=_rec),
    ):
        await iss.setup_lakets_on_branch(
            "manufacturing",
            conn=conn,
            ws=ws,
            db=MagicMock(),
            setup_id="s1",
        )

    types = [r["resource_type"] for r in recorded]
    assert types.count("job") == 2
    assert "uc_table" in types  # the ChronoTable row
    # Jobs are torn down before the branch (higher teardown_order).
    job_orders = [r["teardown_order"] for r in recorded if r["resource_type"] == "job"]
    assert all(o == iss.TEARDOWN_ORDER_JOB for o in job_orders)


@pytest.mark.asyncio
async def test_setup_skips_jobs_when_disabled(tmp_path: Path) -> None:
    conn = _make_conn()
    ws = MagicMock()
    with (
        patch.object(
            iss.lakets_service, "resolve_lakets_source", return_value=tmp_path / "lakets.sql"
        ),
        patch.object(iss.lakets_service, "install_lakets", new=AsyncMock(return_value="1.0")),
        patch.object(iss.lakets_service, "create_chronotable", new=AsyncMock(return_value=True)),
        patch.object(iss.lakets_deploy_service, "deploy_lakets_ops_jobs") as deploy,
    ):
        result = await iss.setup_lakets_on_branch(
            "manufacturing",
            conn=conn,
            ws=ws,
            deploy_ops_jobs=False,
        )
    deploy.assert_not_called()
    assert "ops_jobs" not in result
