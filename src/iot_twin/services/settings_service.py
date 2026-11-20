"""Settings validation and status helper functions.

These are used by the settings route handlers to validate and probe
workspace resources (UC, Lakebase, secrets, cluster, warehouses).
"""

from __future__ import annotations

import logging

logger = logging.getLogger("iot_digital_twin.services.settings")

# Deliberately bogus login for the Lakebase reachability probe: the connect only
# has to reach the host, an auth rejection still proves it is connectable.
_PROBE_LOGIN = "probe"


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _check(name: str, status: str, detail: str) -> dict:
    return {"name": name, "status": status, "detail": detail}


def _section_result(checks: list[dict]) -> dict:
    statuses = [c["status"] for c in checks]
    if "fail" in statuses:
        overall = "fail"
    elif "warn" in statuses:
        overall = "warn"
    else:
        overall = "pass"
    return {"status": overall, "checks": checks}


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


async def validate_uc(body: dict) -> dict:
    """Validate Unity Catalog settings."""
    uc = body.get("uc", {})
    checks: list[dict] = []

    pattern = uc.get("catalog_pattern", "")
    if "{name}" in pattern:
        checks.append(_check("Catalog pattern valid", "pass", "Contains {name}"))
    elif pattern:
        checks.append(
            _check(
                "Catalog pattern valid",
                "warn",
                "Pattern missing {name} placeholder — fixed name will be used",
            )
        )
    else:
        checks.append(_check("Catalog pattern valid", "fail", "Catalog pattern is empty"))

    storage_root = uc.get("storage_root", "")
    if not storage_root:
        checks.append(_check("Storage root format", "skip", "Not configured"))
    elif storage_root.startswith(("s3://", "abfss://", "gs://")):
        checks.append(
            _check("Storage root format", "pass", f"Valid cloud path: {storage_root[:30]}...")
        )
    else:
        checks.append(
            _check("Storage root format", "warn", "Should start with s3://, abfss://, or gs://")
        )

    return _section_result(checks)


async def validate_lakebase(body: dict) -> dict:
    """Validate Lakebase connection settings."""
    lb = body.get("lakebase", {})
    mode = lb.get("mode", "autoscale")
    checks: list[dict] = []

    try:
        import asyncpg

        if mode == "autoscale":
            project_id = lb.get("project_id", "")
            branch_id = lb.get("branch_id", "production")
            if not project_id:
                checks.append(_check("Lakebase endpoint reachable", "fail", "project_id is empty"))
                checks.append(_check("Database connectable", "skip", "No project_id"))
                checks.append(_check("Table exists", "skip", "No connection"))
                return _section_result(checks)

            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            parent = f"projects/{project_id}/branches/{branch_id}"
            try:
                endpoints = list(w.postgres.list_endpoints(parent=parent))
                if not endpoints:
                    checks.append(
                        _check("Lakebase endpoint reachable", "fail", f"No endpoints for {parent}")
                    )
                    checks.append(_check("Database connectable", "skip", "No endpoint"))
                    checks.append(_check("Table exists", "skip", "No connection"))
                    return _section_result(checks)
                checks.append(
                    _check("Lakebase endpoint reachable", "pass", f"Found endpoint in {parent}")
                )
            except Exception as exc:
                checks.append(_check("Lakebase endpoint reachable", "fail", str(exc)[:120]))
                checks.append(_check("Database connectable", "skip", "Endpoint unreachable"))
                checks.append(_check("Table exists", "skip", "No connection"))
                return _section_result(checks)

            # Connect
            try:
                endpoint = w.postgres.get_endpoint(name=endpoints[0].name)
                pg_host = endpoint.status.hosts.host
                pg_user = w.current_user.me().user_name
                cred = w.postgres.generate_database_credential(endpoint=endpoints[0].name)
                database = lb.get("database", "databricks_postgres")
                conn = await asyncpg.connect(
                    host=pg_host,
                    database=database,
                    user=pg_user,
                    password=cred.token,
                    port=5432,
                    ssl="require",
                )
                checks.append(_check("Database connectable", "pass", f"Connected to {database}"))
            except Exception as exc:
                checks.append(_check("Database connectable", "fail", str(exc)[:120]))
                checks.append(_check("Table exists", "skip", "Connection failed"))
                return _section_result(checks)

        else:  # direct mode
            host = lb.get("host", "")
            if not host:
                checks.append(_check("Host connectable", "fail", "Host is empty"))
                checks.append(_check("Table exists", "skip", "No host"))
                return _section_result(checks)
            try:
                port = int(lb.get("port", 5432))
                database = lb.get("database", "databricks_postgres")
                conn = await asyncpg.connect(
                    host=host,
                    database=database,
                    user=_PROBE_LOGIN,
                    password=_PROBE_LOGIN,
                    port=port,
                    ssl="require",
                    timeout=5,
                )
                checks.append(_check("Host connectable", "pass", f"Connected to {host}:{port}"))
            except Exception as exc:
                err = str(exc)[:120]
                if "authentication" in err.lower() or "password" in err.lower():
                    checks.append(
                        _check(
                            "Host connectable", "pass", "Host reachable (auth needed at runtime)"
                        )
                    )
                else:
                    checks.append(_check("Host connectable", "warn", f"Could not connect: {err}"))
                checks.append(_check("Table exists", "skip", "Connection not fully established"))
                return _section_result(checks)

        # Check table exists
        try:
            table_name = lb.get("pg_table", "sensor_readings")
            row = await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = $1)",
                table_name,
            )
            if row:
                checks.append(_check("Table exists", "pass", f"Table '{table_name}' found"))
            else:
                checks.append(_check("Table exists", "warn", f"Table '{table_name}' not found"))
        except Exception as exc:
            checks.append(_check("Table exists", "warn", f"Could not check: {str(exc)[:80]}"))
        finally:
            await conn.close()

    except ImportError:
        checks.append(_check("asyncpg available", "fail", "asyncpg not installed"))

    return _section_result(checks)


async def validate_secrets(body: dict) -> dict:
    """Validate secrets scope settings."""
    sec = body.get("secrets", {})
    scope_name = sec.get("scope", "")
    checks: list[dict] = []

    if not scope_name:
        checks.append(_check("Secret scope exists", "skip", "Scope name is empty"))
        checks.append(_check("Expected keys present", "skip", "No scope configured"))
        return _section_result(checks)

    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        try:
            secret_list = list(w.secrets.list_secrets(scope=scope_name))
            checks.append(_check("Secret scope exists", "pass", f"Scope '{scope_name}' found"))

            expected_keys = [v for k, v in sec.items() if k.endswith("_key") and v]
            existing_keys = {s.key for s in secret_list if s.key}
            missing = [k for k in expected_keys if k not in existing_keys]
            if not expected_keys:
                checks.append(_check("Expected keys present", "skip", "No key names configured"))
            elif missing:
                checks.append(
                    _check("Expected keys present", "warn", f"Missing keys: {', '.join(missing)}")
                )
            else:
                checks.append(
                    _check("Expected keys present", "pass", f"All {len(expected_keys)} keys found")
                )
        except Exception as exc:
            err = str(exc)
            if "RESOURCE_DOES_NOT_EXIST" in err or "does not exist" in err.lower():
                checks.append(
                    _check("Secret scope exists", "fail", f"Scope '{scope_name}' not found")
                )
                checks.append(_check("Expected keys present", "skip", "Scope does not exist"))
            else:
                checks.append(_check("Secret scope exists", "fail", str(exc)[:120]))
                checks.append(_check("Expected keys present", "skip", "Scope check failed"))
    except ImportError:
        checks.append(_check("Databricks SDK available", "fail", "SDK not installed"))

    return _section_result(checks)


async def validate_cluster(body: dict) -> dict:
    """Validate cluster settings."""
    cl = body.get("cluster", {})
    checks: list[dict] = []

    node_type = cl.get("worker_node_type", "")
    dbr_version = cl.get("dbr_version", "")

    if not node_type:
        checks.append(_check("Node type valid", "skip", "Not configured"))
    else:
        try:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            resp = w.clusters.list_node_types()
            valid_ids = {nt.node_type_id for nt in (resp.node_types or [])}
            if node_type in valid_ids:
                checks.append(_check("Node type valid", "pass", f"{node_type} is available"))
            else:
                checks.append(
                    _check("Node type valid", "fail", f"{node_type} not found in workspace")
                )
        except Exception as exc:
            checks.append(_check("Node type valid", "warn", f"Could not verify: {str(exc)[:80]}"))

    if not dbr_version:
        checks.append(_check("DBR version valid", "skip", "Not configured"))
    else:
        try:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            resp = w.clusters.spark_versions()
            valid_keys = {sv.key for sv in (resp.versions or [])}
            if dbr_version in valid_keys:
                checks.append(_check("DBR version valid", "pass", f"{dbr_version} is available"))
            else:
                checks.append(
                    _check("DBR version valid", "fail", f"{dbr_version} not found in workspace")
                )
        except Exception as exc:
            checks.append(_check("DBR version valid", "warn", f"Could not verify: {str(exc)[:80]}"))

    if not checks:
        checks.append(_check("Cluster config", "skip", "No cluster settings configured"))

    return _section_result(checks)


# ---------------------------------------------------------------------------
# Status helpers
# ---------------------------------------------------------------------------


async def status_uc(settings: dict) -> dict:
    """Probe UC catalogs matching the user's pattern."""
    result: dict = {"catalogs": []}
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        pattern = settings.get("uc", {}).get("catalog_pattern", "iot_{name}")
        prefix = pattern.split("{")[0] if "{" in pattern else pattern
        for cat in w.catalogs.list():
            if cat.name and cat.name.startswith(prefix):
                schemas = [
                    s.name
                    for s in w.schemas.list(catalog_name=cat.name)
                    if s.name and s.name != "information_schema" and s.name != "default"
                ]
                result["catalogs"].append({"name": cat.name, "schemas": schemas})
    except Exception as exc:
        result["error"] = str(exc)[:200]
    return result


async def status_lakebase(settings: dict) -> dict:
    """Probe Lakebase project, branch, endpoint, role, and tables."""
    from iot_twin.models.sql_ddl import LAKEBASE_ROLE_ID, LAKEBASE_ROLE_NAME

    lb = settings.get("lakebase", {})
    result: dict = {
        "project_id": lb.get("project_id"),
        "branch_id": lb.get("branch_id"),
        "endpoint_host": None,
        "tables": [],
        "role_exists": False,
        "role_name": None,
    }
    mode = lb.get("mode", "autoscale")
    if mode != "autoscale":
        result["project_id"] = None
        result["endpoint_host"] = lb.get("host")
        return result

    project_id = lb.get("project_id", "")
    branch_id = lb.get("branch_id", "production")
    if not project_id:
        return result

    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        parent = f"projects/{project_id}/branches/{branch_id}"

        # Check endpoint
        endpoints = []
        try:
            endpoints = list(w.postgres.list_endpoints(parent=parent))
            if endpoints:
                ep = w.postgres.get_endpoint(name=endpoints[0].name)
                result["endpoint_host"] = (
                    ep.status.hosts.host if ep.status and ep.status.hosts else None
                )
        except Exception:
            pass

        # Check role
        try:
            w.postgres.get_role(name=f"{parent}/roles/{LAKEBASE_ROLE_ID}")
            result["role_exists"] = True
            result["role_name"] = LAKEBASE_ROLE_NAME
        except Exception:
            pass

        # Check tables
        try:
            if result["endpoint_host"]:
                import asyncpg

                pg_user = w.current_user.me().user_name
                cred = w.postgres.generate_database_credential(endpoint=endpoints[0].name)
                database = lb.get("database", "databricks_postgres")
                conn = await asyncpg.connect(
                    host=result["endpoint_host"],
                    database=database,
                    user=pg_user,
                    password=cred.token,
                    port=5432,
                    ssl="require",
                )
                try:
                    rows = await conn.fetch(
                        "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
                    )
                    result["tables"] = [r["tablename"] for r in rows]
                finally:
                    await conn.close()
        except Exception:
            pass
    except Exception as exc:
        result["error"] = str(exc)[:200]
    return result


async def status_secrets(settings: dict) -> dict:
    """Probe UC secrets scope and list keys."""
    sec = settings.get("secrets", {})
    scope_name = sec.get("scope", "")
    result: dict = {"scope_exists": False, "scope_name": scope_name, "keys": []}
    if not scope_name:
        return result
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        secret_list = list(w.secrets.list_secrets(scope=scope_name))
        result["scope_exists"] = True
        result["keys"] = [s.key for s in secret_list if s.key]
    except Exception as exc:
        err = str(exc)
        if "RESOURCE_DOES_NOT_EXIST" not in err and "does not exist" not in err.lower():
            result["error"] = err[:200]
    return result


async def status_warehouse() -> dict:
    """List SQL warehouses in the workspace."""
    result: dict = {"warehouses": []}
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        for wh in w.warehouses.list():
            result["warehouses"].append(
                {
                    "id": wh.id,
                    "name": wh.name,
                    "cluster_size": wh.cluster_size,
                    "state": str(wh.state.value) if wh.state else "UNKNOWN",
                    "serverless": bool(wh.enable_serverless_compute),
                    "auto_stop_mins": wh.auto_stop_mins or 0,
                }
            )
    except Exception as exc:
        result["error"] = str(exc)[:200]
    return result
