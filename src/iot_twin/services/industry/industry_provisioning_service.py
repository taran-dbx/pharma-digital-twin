"""Industry provisioning orchestrator (Task 3 — one schema, single app SP).

``provision_industry(industry, setup_id)`` runs the full additive provisioning
sequence for one industry pipeline and records every resource in the
``provisioned_resource`` ledger:

1. Pre-flight checks (external location + warehouse).
2. Resolve the SINGLE app service principal (``DATABRICKS_CLIENT_ID``) — no
   second SP is minted; the app SP is used for every auth path.
3. Provision into the PRE-EXISTING ``{UC_CATALOG}.{UC_SCHEMA}``: ``CREATE SCHEMA
   IF NOT EXISTS`` (never ``CREATE CATALOG``) + the managed, industry-prefixed
   Delta tables (``{industry}_zerobus`` / ``_realtime`` / ``_microbatch`` /
   ``_metrics``).
4. Store the app SP's own OAuth client-secret as the UC secret
   ``{catalog}.{schema}.app_sp_oauth`` (read from ``DATABRICKS_CLIENT_SECRET``)
   so the Zerobus producer notebooks can authenticate at runtime.
5. (real-time) never-expiring Lakebase branch + endpoints, then LakeTS setup on
   the branch, registering the SAME app SP as the branch Postgres role.
6. Grants for the single app SP on the schema (USE CATALOG / USE SCHEMA /
   CREATE TABLE / MODIFY / SELECT + READ SECRET on the UC secret).

Additive only — does not touch ``app.yaml`` or ``core/lakebase.py`` live wiring.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from iot_twin.services import (
    cdf_config_service,
    cdf_reconciliation_service,
    provisioning_service,
    resource_ledger,
    uc_secrets,
)
from iot_twin.services.industry import industry_catalog_service as cat
from iot_twin.services.industry import industry_setup_service as setup
from iot_twin.services.industry.industry_catalog_service import (
    APP_SP_OAUTH_SECRET,
    TEARDOWN_ORDER_SP,
)
from iot_twin.services.pipelines import pipeline_project_service as pps
from iot_twin.services.resource_ledger import record_resource

logger = logging.getLogger("iot_digital_twin.services.industry_provisioning")


class _CdfProvisioningError(RuntimeError):
    pass


def _new_ws() -> Any:
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


async def provision_industry(
    industry: str,
    setup_id: Any,
    *,
    db: Any | None = None,
    ws: Any | None = None,
    warehouse_id: str | None = None,
    enable_realtime: bool = True,
    deploy_ops_jobs: bool = True,
    lakets_sql_path: str | None = None,
    run_preflight: bool = True,
) -> dict:
    """Provision all resources for one industry pipeline; record them in the ledger.

    Returns a summary dict with a ``sections`` list (one per phase) and the key
    identifiers (``catalog``, ``schema``, ``metrics_table``, ``sp_principal``).
    Raises ``RuntimeError`` on failed pre-flight; otherwise best-effort per phase.
    """
    ws = ws or _new_ws()
    summary: dict[str, Any] = {"industry": industry, "setup_id": setup_id, "sections": []}

    def _section(name: str, payload: Any) -> None:
        summary["sections"].append({"phase": name, "result": payload})

    # 1. Pre-flight.
    if run_preflight:
        checks = cat.preflight_checks(ws=ws, warehouse_id=warehouse_id)
        _section("preflight", checks)
        cat.raise_on_failed_preflight(checks)

    # 2. Resolve the SINGLE app SP (no minting) — used for every auth path.
    principal = cat.resolve_app_principal(ws)
    summary["sp_principal"] = principal
    _section("app_sp", {"principal": principal})
    if db is not None and setup_id is not None:
        await record_resource(
            db,
            setup_id=setup_id,
            resource_type="service_principal",
            identifier=principal or "app-sp",
            teardown_order=TEARDOWN_ORDER_SP,
            metadata={"shared": True, "application_id": principal},
        )

    # 3. Schema + managed industry-prefixed tables in the pre-existing catalog.
    catalog_result = await cat.provision_industry_catalog(
        industry,
        ws=ws,
        warehouse_id=warehouse_id,
        db=db,
        setup_id=setup_id,
    )
    _section("uc_catalog", catalog_result)
    summary["catalog"] = catalog_result.get("catalog")
    summary["schema"] = catalog_result.get("schema")
    summary["metrics_table"] = catalog_result.get("metrics_table")

    # 4. Store the app SP's OAuth client-secret as a UC secret for the notebooks.
    catalog = catalog_result.get("catalog")
    schema = catalog_result.get("schema")
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET", "")
    if catalog and schema and client_secret:
        secret_result = await uc_secrets.put_secret(
            catalog,
            schema,
            APP_SP_OAUTH_SECRET,
            client_secret,
            ws=ws,
        )
    else:
        secret_result = {
            "resource": f"UC secret '{catalog}.{schema}.{APP_SP_OAUTH_SECRET}'",
            "status": "skipped",
            "error": "DATABRICKS_CLIENT_SECRET not set — cannot store app SP OAuth secret",
        }
    _section("uc_secret", secret_result)

    # 5. Real-time Lakebase branch + LakeTS setup on the branch.
    # The whole realtime Lakebase block is best-effort: a zerobus/micro-batch-only
    # industry never enters it (``enable_realtime=False``), and even when enabled a
    # missing realtime Lakebase project (e.g. ``PIPELINES_LAKEBASE_PROJECT`` blank on
    # this workspace) must NOT hard-fail provisioning — it is recorded as a failed
    # section like every other phase.
    if enable_realtime:
        try:
            branch_result = await pps.provision_industry_branch(
                industry,
                ws=ws,
                db=db,
                setup_id=setup_id,
            )
            _section("lakebase_branch", branch_result)
            branch = (branch_result or {}).get("branch")
            if not branch:
                raise RuntimeError(
                    f"realtime branch provisioning returned no branch: {branch_result!r}"
                )
            summary["branch"] = branch

            # 5b. Register the SINGLE app SP as an OAuth Postgres role on THIS
            #     industry branch so the realtime consumer's LakebaseWriter can
            #     authenticate + write as that SP (one role, not a separate SP).
            try:
                sp_role_result = await provisioning_service.provision_lakebase_role(
                    sp_client_id=principal,
                    project_id=branch_result.get("project"),
                    branch_id=branch,
                    ws=ws,
                )
                _section("lakebase_sp_role", sp_role_result)
            except Exception as exc:  # pragma: no cover - best-effort
                logger.warning(
                    "App SP Lakebase role registration on branch '%s' failed (non-fatal): %s",
                    branch,
                    exc,
                )
                _section("lakebase_sp_role", {"status": "failed", "error": str(exc)[:200]})

            write_mgr = None
            try:
                write_mgr = await pps.get_write_conn(branch, ws=ws)
                lakets_result = await setup.setup_lakets_on_branch(
                    branch,
                    conn=write_mgr,
                    ws=ws,
                    industry=industry,
                    project_id=branch_result.get("project"),
                    deploy_ops_jobs=deploy_ops_jobs,
                    lakets_sql_path=lakets_sql_path,
                    db=db,
                    setup_id=setup_id,
                )
                _section("lakets_setup", lakets_result)
            finally:
                if write_mgr is not None:
                    try:
                        await write_mgr.close()
                    except Exception:  # pragma: no cover - best-effort
                        pass

            # 5c. Enable the managed Lakebase -> Delta CDF sync through the
            # Postgres API as soon as LakeTS has prepared the lakets_cdf shadow.
            # This is independent of the slow interactive validation cluster.
            if (lakets_result or {}).get("cdf_shadow") not in {
                "created",
                "already_exists",
                "migrated",
            }:
                failure = {
                    "status": "failed",
                    "error": (
                        (lakets_result or {}).get("steps", [{}])[-1].get("error")
                        or "LakeTS CDF shadow was not prepared"
                    ),
                }
                _section("cdf_sync", failure)
                raise _CdfProvisioningError(failure["error"])
            else:
                parent = cdf_config_service.database_parent(
                    branch,
                    project_id=branch_result.get("project"),
                )
                try:
                    cdf_result = await asyncio.to_thread(
                        cdf_config_service.ensure_cdf_config,
                        parent,
                        catalog=catalog,
                        schema=schema,
                        ws=ws,
                    )
                    _section("cdf_sync", cdf_result)
                    if db is not None and setup_id is not None:
                        cdf_config_id = cdf_result["cdf_config_id"]
                        resources = await resource_ledger.list_resources(db, setup_id)
                        if not any(
                            resource.get("resource_type") == "cdf_sync"
                            and resource.get("identifier") == cdf_result["name"]
                            and resource.get("status") == "active"
                            for resource in resources
                        ):
                            await record_resource(
                                db,
                                setup_id=setup_id,
                                resource_type="cdf_sync",
                                identifier=cdf_result["name"],
                                parent_ref=pps.branch_resource_name(
                                    branch, branch_result.get("project")
                                ),
                                teardown_order=cdf_config_service.TEARDOWN_ORDER,
                                metadata={
                                    "parent": parent,
                                    "cdf_config_id": cdf_config_id,
                                    "catalog": catalog,
                                    "schema": schema,
                                    "postgres_schema": cdf_result["postgres_schema"],
                                    "database": pps.PG_DATABASE,
                                    "source_table": (
                                        "lakets_cdf."
                                        f"{cdf_reconciliation_service.shadow_table_name(industry)}"
                                    ),
                                    "delta_table": cdf_reconciliation_service.delta_table_fqn(
                                        industry,
                                        catalog=catalog,
                                        schema=schema,
                                    ),
                                    "force_delete_tables": False,
                                },
                            )
                except Exception as exc:
                    logger.warning(
                        "CDF config enablement for industry '%s' failed: %s",
                        industry,
                        exc,
                    )
                    _section(
                        "cdf_sync",
                        {"status": "failed", "error": str(exc)[:200]},
                    )
                    raise _CdfProvisioningError(
                        f"CDF configuration failed for {industry}: {exc}"
                    ) from exc
        except Exception as exc:
            logger.warning(
                "Realtime Lakebase provisioning for industry '%s' failed (non-fatal): %s",
                industry,
                exc,
            )
            _section("lakebase_branch", {"status": "failed", "error": str(exc)[:200]})
            if isinstance(exc, _CdfProvisioningError):
                raise RuntimeError(str(exc)) from exc

    # 6. Grants for the SINGLE app SP on the schema (incl. READ SECRET).
    grants_result = await cat.grant_app_sp(
        industry,
        ws=ws,
        warehouse_id=warehouse_id,
        principal=principal,
        db=db,
        setup_id=setup_id,
    )
    _section("grants", grants_result)

    return summary
