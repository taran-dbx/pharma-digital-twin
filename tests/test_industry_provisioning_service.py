"""Tests for the industry provisioning orchestrator (Task 3 — single app SP)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from iot_twin.services.industry import industry_provisioning_service as ips

CATALOG = "example_eu_catalog"
SCHEMA = "rtm_lakets_zb"


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("UC_CATALOG", CATALOG)
    monkeypatch.setenv("UC_SCHEMA", SCHEMA)
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "app-xyz")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "oauth-secret")


def _patch_all(*, preflight_pass=True):
    """Patch every collaborator of the orchestrator; yields the patches dict."""
    checks = [{"check": "x", "status": "pass" if preflight_pass else "fail", "detail": "d"}]
    write_mgr = MagicMock()
    write_mgr.close = AsyncMock()

    patches = {
        "preflight_checks": patch.object(ips.cat, "preflight_checks", return_value=checks),
        "provision_industry_catalog": patch.object(
            ips.cat,
            "provision_industry_catalog",
            new=AsyncMock(
                return_value={
                    "catalog": CATALOG,
                    "schema": SCHEMA,
                    "results": [],
                    "tables": {"zerobus": f"{CATALOG}.{SCHEMA}.spiketest_zerobus"},
                    "metrics_table": f"{CATALOG}.{SCHEMA}.spiketest_metrics",
                }
            ),
        ),
        "put_secret": patch.object(
            ips.uc_secrets,
            "put_secret",
            new=AsyncMock(return_value={"resource": "secret", "status": "created"}),
        ),
        "provision_industry_branch": patch.object(
            ips.pps,
            "provision_industry_branch",
            new=AsyncMock(
                return_value={
                    "project": "iot-pipelines",
                    "branch": "spiketest",
                    "primary_host": "p",
                    "replica_host": "r",
                }
            ),
        ),
        "provision_lakebase_role": patch.object(
            ips.provisioning_service,
            "provision_lakebase_role",
            new=AsyncMock(return_value=[]),
        ),
        "get_write_conn": patch.object(
            ips.pps,
            "get_write_conn",
            new=AsyncMock(return_value=write_mgr),
        ),
        "setup_lakets_on_branch": patch.object(
            ips.setup,
            "setup_lakets_on_branch",
            new=AsyncMock(
                return_value={
                    "branch": "spiketest",
                    "cdf_shadow": "created",
                    "steps": [],
                }
            ),
        ),
        "ensure_cdf_config": patch.object(
            ips.cdf_config_service,
            "ensure_cdf_config",
            return_value={
                "name": (
                    "projects/iot-pipelines/branches/spiketest/databases/"
                    "databricks_postgres/cdf-configs/lakets_cdf"
                ),
                "cdf_config_id": "lakets_cdf",
                "postgres_schema": "lakets_cdf",
                "status": "created",
            },
        ),
        "list_resources": patch.object(
            ips.resource_ledger, "list_resources", new=AsyncMock(return_value=[])
        ),
        "grant_app_sp": patch.object(
            ips.cat,
            "grant_app_sp",
            new=AsyncMock(return_value={"catalog": CATALOG, "principal": "app-xyz", "results": []}),
        ),
    }
    return patches, write_mgr


@pytest.mark.asyncio
async def test_orchestrator_runs_all_phases_and_records_sp() -> None:
    patches, write_mgr = _patch_all()
    recorded: list[dict] = []

    async def _rec(_db, **kw):
        recorded.append(kw)

    with (
        patches["preflight_checks"],
        patches["provision_industry_catalog"],
        patches["put_secret"],
        patches["provision_industry_branch"],
        patches["provision_lakebase_role"],
        patches["get_write_conn"],
        patches["setup_lakets_on_branch"],
        patches["ensure_cdf_config"],
        patches["list_resources"],
        patches["grant_app_sp"],
        patch.object(ips, "record_resource", side_effect=_rec),
    ):
        summary = await ips.provision_industry(
            "manufacturing",
            "setup-1",
            db=MagicMock(),
            ws=MagicMock(),
        )

    phases = [s["phase"] for s in summary["sections"]]
    for expected in (
        "preflight",
        "app_sp",
        "uc_catalog",
        "uc_secret",
        "lakebase_branch",
        "lakets_setup",
        "cdf_sync",
        "grants",
    ):
        assert expected in phases, f"missing phase {expected}"

    assert summary["catalog"] == CATALOG
    assert summary["schema"] == SCHEMA
    assert summary["metrics_table"] == f"{CATALOG}.{SCHEMA}.spiketest_metrics"
    assert summary["branch"] == "spiketest"
    assert summary["sp_principal"] == "app-xyz"
    # The single app SP is recorded (no separate shared/zerobus SP is minted).
    sp_rows = [r for r in recorded if r["resource_type"] == "service_principal"]
    assert sp_rows and sp_rows[0]["metadata"]["application_id"] == "app-xyz"
    cdf_rows = [r for r in recorded if r["resource_type"] == "cdf_sync"]
    assert cdf_rows and cdf_rows[0]["metadata"]["cdf_config_id"] == "lakets_cdf"
    write_mgr.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_orchestrator_writes_uc_secret_from_env() -> None:
    patches, _ = _patch_all()
    with (
        patches["preflight_checks"],
        patches["provision_industry_catalog"],
        patches["put_secret"] as put_mock,
        patches["grant_app_sp"],
    ):
        await ips.provision_industry(
            "spiketest",
            None,
            ws=MagicMock(),
            enable_realtime=False,
        )
    put_mock.assert_awaited_once()
    args = put_mock.await_args.args
    assert args[0] == CATALOG and args[1] == SCHEMA
    assert args[2] == ips.APP_SP_OAUTH_SECRET
    assert args[3] == "oauth-secret"


@pytest.mark.asyncio
async def test_orchestrator_raises_on_failed_preflight() -> None:
    patches, _ = _patch_all(preflight_pass=False)
    with patches["preflight_checks"]:
        with pytest.raises(RuntimeError, match="Pre-flight"):
            await ips.provision_industry("spiketest", "s1", ws=MagicMock())


@pytest.mark.asyncio
async def test_realtime_branch_failure_is_recorded_not_propagated() -> None:
    # On a workspace with no realtime Lakebase project, provision_industry_branch
    # raises — provisioning must stay best-effort: record a failed lakebase_branch
    # section and still reach the grants phase, never a 500.
    patches, _ = _patch_all()
    with (
        patches["preflight_checks"],
        patches["provision_industry_catalog"],
        patches["put_secret"],
        patches["grant_app_sp"],
        patch.object(
            ips.pps,
            "provision_industry_branch",
            new=AsyncMock(side_effect=RuntimeError("Project 'iot-pipelines' not found")),
        ),
    ):
        summary = await ips.provision_industry(
            "spiketest",
            "s1",
            ws=MagicMock(),
            enable_realtime=True,
        )
    sections = {s["phase"]: s["result"] for s in summary["sections"]}
    assert sections["lakebase_branch"]["status"] == "failed"
    assert "iot-pipelines" in sections["lakebase_branch"]["error"]
    # Grants still ran despite the realtime failure.
    assert "grants" in sections
    assert "branch" not in summary


@pytest.mark.asyncio
async def test_orchestrator_skips_realtime_when_disabled() -> None:
    patches, _ = _patch_all()
    with (
        patches["preflight_checks"],
        patches["provision_industry_catalog"],
        patches["put_secret"],
        patches["provision_industry_branch"] as branch_mock,
        patches["grant_app_sp"],
    ):
        summary = await ips.provision_industry(
            "spiketest",
            "s1",
            ws=MagicMock(),
            enable_realtime=False,
        )
    branch_mock.assert_not_called()
    phases = [s["phase"] for s in summary["sections"]]
    assert "lakebase_branch" not in phases
    assert "grants" in phases
    assert "uc_secret" in phases


@pytest.mark.asyncio
async def test_realtime_provisioning_surfaces_shadow_failure_c7246() -> None:
    """Realtime provisioning cannot hide CDF preparation failure [C7246][defect][regression]."""
    patches, _ = _patch_all()
    with (
        patches["preflight_checks"],
        patches["provision_industry_catalog"],
        patches["put_secret"],
        patches["provision_industry_branch"],
        patches["provision_lakebase_role"],
        patches["get_write_conn"],
        patches["setup_lakets_on_branch"] as setup_lakets,
        patches["ensure_cdf_config"] as ensure_cdf,
        patches["grant_app_sp"],
        patch.object(ips, "record_resource", new=AsyncMock()),
    ):
        setup_lakets.return_value = {
            "branch": "manufacturing",
            "cdf_shadow": "failed",
            "steps": [
                {
                    "step": "enable_sync",
                    "status": "failed",
                    "error": "shadow migration failed",
                }
            ],
        }
        with pytest.raises(RuntimeError, match="shadow migration failed"):
            await ips.provision_industry(
                "manufacturing",
                "setup-1",
                db=MagicMock(),
                ws=MagicMock(),
            )

    ensure_cdf.assert_not_called()
