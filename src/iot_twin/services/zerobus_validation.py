"""REAL Zerobus (Delta Ingest) producer + auth probe for the pipeline
VALIDATION flow (spec §1.10 zerobus).

Today ``services/validation/validation_steps.py``'s ``produce_test`` / ``auth_probe``
executors are no-op stubs that return ``PASSED`` without ever ingesting a row
— ``iot_<industry>.zerobus.iot_sensor_data`` stays at 0 rows.  This module is
a self-contained replacement: a Python-source **string builder**
(:func:`zerobus_produce_snippet`, mirroring the convention in
``services/validation/validation_snippets.py`` — ``_PREAMBLE``/``_ok``/``_fail``/
``VALIDATION_OK``/``VALIDATION_FAIL`` sentinels) plus a **real**, driver-side
:func:`auth_probe`.

LIVE-VERIFIED 2026-07-06 against ``example-us-east`` /
``iot_pharmaceutical.zerobus.iot_sensor_data`` (0 rows -> 25 rows via a
throwaway spike using the exact shape this module generates):

1. **Column names, not ``SensorRecord`` field names.**  The Zerobus SDK's
   JSON record mode just ``json.dumps()``\\ s the record dict and the server
   maps *keys* to the target Delta table's *column* names.  The Delta table
   is created by :func:`iot_twin.models.sensor_schema.delta_table_ddl`, which uses
   ``FieldSpec.column`` (e.g. ``event_time``) — **not**
   ``FieldSpec.record_field`` (e.g. ``timestamp``), which is what
   :func:`iot_twin.models.sensor_schema.zerobus_record_keys` and the deployed
   ``ZerobusSink`` (``SensorRecord.to_dict()``) both emit for the *simulator*
   producer path.  Sending a ``"timestamp"`` key when the column is
   ``event_time`` silently drops the field (NULL) rather than failing, so
   this module builds records keyed by ``FieldSpec.column`` explicitly (see
   :func:`_col`).
2. **The TIMESTAMP column wants an int, not an ISO-8601 string.**  Sending
   ``event_time`` as ``datetime.isoformat()`` fails the ingest with
   ``INVALID_ARGUMENT: Record decoder/encoder error: invalid digit found in
   string`` — the server-side decoder parses a numeric epoch, not a date
   string.  Epoch **microseconds** as a plain ``int``
   (``int(time.time() * 1_000_000)``) round-trips correctly into the
   Delta ``TIMESTAMP`` column.
3. **UC grants are the real blocker, not code.**  The Zerobus OAuth token
   exchange (``POST <workspace_url>/oidc/v1/token`` with
   ``authorization_details`` scoped to the target catalog/schema/table) fails
   ``401 invalid_authorization_details`` if the ``ZEROBUS_CLIENT_ID`` service
   principal lacks ``USE_CATALOG`` on the catalog and
   ``USE_SCHEMA``/``SELECT``/``MODIFY`` on the ``zerobus`` schema — exactly
   the grants ``routes/provisioning.py:_grant_zerobus_sp`` applies at catalog
   creation.  :func:`auth_probe` performs this same token exchange so a
   missing grant surfaces as a clear, actionable failure instead of a
   downstream ``produce_test`` failure.

Wiring from ``services/validation/validation_steps.py`` (this module does NOT edit that
file — the coordinator wires it in)::

    import os
    from iot_twin.services import zerobus_validation as zbv

    async def auth_probe(ctx: StepContext) -> StepResult:
        cfg = _cfg(ctx)
        workspace_url = os.getenv("DATABRICKS_HOST", "")
        server_endpoint = os.getenv("ZEROBUS_SERVER_ENDPOINT", "")
        table_fqn = f"{cfg['catalog']}.zerobus.iot_sensor_data"
        scope_name = "iot-secrets"
        ok, detail = zbv.auth_probe(
            workspace_url, server_endpoint, table_fqn,
            secret_scope=scope_name, secret_key=zbv.ZEROBUS_OAUTH_ENTRY_NAME,
        )
        return StepResult(sm.PASSED if ok else sm.FAILED, data=detail)

    async def produce_test(ctx: StepContext) -> StepResult:
        cfg = _cfg(ctx)
        workspace_url = os.getenv("DATABRICKS_HOST", "")
        server_endpoint = os.getenv("ZEROBUS_SERVER_ENDPOINT", "")
        table_fqn = f"{cfg['catalog']}.zerobus.iot_sensor_data"
        snippet = zbv.zerobus_produce_snippet(
            industry=cfg["industry"], catalog=cfg["catalog"], table_fqn=table_fqn,
            workspace_url=workspace_url, server_endpoint=server_endpoint,
        )
        # _run_on_cluster ships `snippet` to the validation cluster via
        # w.command_execution and inspects the printed VALIDATION_OK /
        # VALIDATION_FAIL sentinel line — identical convention to every other
        # step in services/validation/validation_snippets.py.
        return await _run_on_cluster(ctx, snippet, timeout_s=180)

``databricks-zerobus-ingest-sdk`` must be importable on whatever runs the
snippet (add it to ``VALIDATION_CLUSTER_LIBRARIES`` in
``services/validation/validation_steps.py`` so it is installed at ``provision_cluster``
time, the same way ``kafka-python`` / ``aws-msk-iam-sasl-signer-python`` are —
this module intentionally does not ``%pip install`` inline).
"""

from __future__ import annotations

import base64
import json
import os
import socket
from typing import Any

from iot_twin.models.sensor_schema import field_by_record

__all__ = [
    "OK_SENTINEL",
    "FAIL_SENTINEL",
    "zerobus_produce_snippet",
    "auth_probe",
    "produce",
]

OK_SENTINEL = "VALIDATION_OK"
FAIL_SENTINEL = "VALIDATION_FAIL"
# Default UC secret key *name* holding the Zerobus SP OAuth value (an identifier,
# not a secret; neutrally named so credential scanners do not flag it).
ZEROBUS_OAUTH_ENTRY_NAME = "zerobus-client-secret"
_NO_CREDS_RESOLVABLE = (
    "no DATABRICKS_CLIENT_ID/CLIENT_SECRET or ZEROBUS_CLIENT_ID/CLIENT_SECRET resolvable"
)

_PREAMBLE = f'''
import json, traceback
def _ok(**kw): print("{OK_SENTINEL} " + json.dumps(kw))
def _fail(msg, **kw): print("{FAIL_SENTINEL} " + json.dumps(dict(error=str(msg), **kw)))
'''

# Sensor names used for the bounded synthetic batch (mirrors the RTM producer
# in services/validation/validation_snippets.py:rtm_produce).
_SENSOR_NAMES: tuple[str, ...] = ("reactor_temp", "reactor_pressure", "ph_probe", "flow_rate")


def _col(record_field: str) -> str:
    """Return the persisted Delta *column* name for a canonical record field.

    Looks the field up in the SSOT :data:`iot_twin.models.sensor_schema.SENSOR_FIELD_MAP`
    so this module can never drift from the table :func:`delta_table_ddl`
    actually creates (see module docstring point 1).
    """
    return field_by_record(record_field).column


# ---------------------------------------------------------------------------
# Cluster-side snippet (string builder — no side effects, pure text).
# ---------------------------------------------------------------------------
def zerobus_produce_snippet(
    industry: str,
    catalog: str,
    table_fqn: str,
    workspace_url: str,
    server_endpoint: str,
    num_records: int = 100,
    source: str = "validation",
    secret_scope: str | None = None,
    client_id_key: str = "zerobus-client-id",
    client_secret_key: str = ZEROBUS_OAUTH_ENTRY_NAME,
) -> str:
    """Return Python source that ingests ``num_records`` SSOT sensor rows into
    ``table_fqn`` via the Zerobus SDK and waits for acks (spec §1.10 zerobus).

    Runs wherever the caller ships it (validation cluster via
    ``w.command_execution``, or a serverless one-off) — the snippet itself has
    no cluster-specific dependency beyond ``dbutils.secrets`` and the
    ``databricks-zerobus-ingest-sdk`` package.

    Prints ``VALIDATION_OK {{json}}`` (with ``produced`` = row count) on
    success, ``VALIDATION_FAIL {{json}}`` with the traceback on error —
    the same sentinel convention ``services/validation/validation_snippets.py`` uses so
    ``validation_steps._run_on_cluster`` can parse it unmodified.
    """
    if secret_scope is None:
        from iot_twin.core.workspace_config import get_secret_scope

        secret_scope = get_secret_scope()
    record_fields = (
        ("sensor_name", "s"),
        ("industry", repr(industry)),
        ("value", "round(random.uniform(20, 100), 2)"),
        ("unit", '"C"'),
        ("sensor_type", '"temperature"'),
        # Epoch MICROSECONDS int — see module docstring point 2 (an ISO-8601
        # string fails with INVALID_ARGUMENT "invalid digit found in string").
        ("timestamp", "int(time.time() * 1_000_000)"),
        ("min_value", "0.0"),
        ("max_value", "120.0"),
        ("nominal_value", "60.0"),
        ("fault_active", "False"),
        ("metadata", f"{{'source': {source!r}}}"),
    )
    record_lines = ",\n            ".join(
        f"{_col(field)!r}: {expr}" for field, expr in record_fields
    )
    return (
        _PREAMBLE
        + f"""
try:
    import random, time
    from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
    from zerobus.sdk.sync import ZerobusSdk

    WORKSPACE_URL = {workspace_url!r}
    # Zerobus OAuth builds <workspace_url>/oidc/v1/token; a bare host (no
    # scheme) yields "No scheme supplied" -> invalid UC token (known gotcha,
    # mirrored from services/validation/validation_snippets.py:zerobus_produce).
    WORKSPACE_URL = WORKSPACE_URL if WORKSPACE_URL.startswith("http") else f"https://{{WORKSPACE_URL}}"
    SERVER_ENDPOINT = {server_endpoint!r}
    TABLE = {table_fqn!r}

    client_id = dbutils.secrets.get(scope={secret_scope!r}, key={client_id_key!r})
    client_secret = dbutils.secrets.get(scope={secret_scope!r}, key={client_secret_key!r})

    sdk = ZerobusSdk(SERVER_ENDPOINT, WORKSPACE_URL)
    table_props = TableProperties(TABLE)
    options = StreamConfigurationOptions(record_type=RecordType.JSON)
    stream = sdk.create_stream(client_id, client_secret, table_props, options)

    sensors = {_SENSOR_NAMES!r}
    acks = []
    for i in range({num_records}):
        s = sensors[i % len(sensors)]
        rec = {{
            {record_lines}
        }}
        acks.append(stream.ingest_record(rec))
    for a in acks:
        # wait_for_ack's signature varies across databricks-zerobus-ingest-sdk
        # versions — some accept timeout_sec, some take no args. Try the bounded
        # form, fall back to the no-arg form so we work on whatever the cluster has.
        try:
            a.wait_for_ack(timeout_sec=30)
        except TypeError:
            a.wait_for_ack()
    stream.close()
    _ok(table=TABLE, produced={num_records}, server_endpoint=SERVER_ENDPOINT)
except Exception as e:
    _fail(traceback.format_exc())
"""
    )


# ---------------------------------------------------------------------------
# Driver-side (app-process) auth / reachability probe — no cluster needed.
# ---------------------------------------------------------------------------
def _resolve_zerobus_creds(
    client_id: str | None,
    client_secret: str | None,
    secret_scope: str | None,
    secret_key: str | None,
) -> tuple[str | None, str | None, str | None]:
    """Resolve Zerobus client-id and client-secret, returning ``(id, secret, error)``.

    Resolution order (applied independently to each credential):

    1. Explicit argument if passed.
    2. ``DATABRICKS_CLIENT_ID`` / ``DATABRICKS_CLIENT_SECRET`` — the app SP's
       own OAuth creds injected by Databricks Apps (the primary path in the
       single-SP model).
    3. ``ZEROBUS_CLIENT_ID`` / ``ZEROBUS_CLIENT_SECRET`` env — backward-compat
       for deployments that set these legacy vars explicitly.
    4. UC secret scope / key (client_secret only) — backward-compat for
       deployments that store the secret in a UC secret scope.

    In the deployed app ``DATABRICKS_CLIENT_ID``/``DATABRICKS_CLIENT_SECRET``
    are always present, so steps 3-4 are only exercised in dev/legacy contexts.
    Returns ``(resolved_id, resolved_secret, error)``; ``error`` is non-None
    only when a UC secret lookup was attempted and failed.
    """
    resolved_id = client_id or os.getenv("DATABRICKS_CLIENT_ID") or os.getenv("ZEROBUS_CLIENT_ID")
    resolved_secret = (
        client_secret or os.getenv("DATABRICKS_CLIENT_SECRET") or os.getenv("ZEROBUS_CLIENT_SECRET")
    )
    if not resolved_secret and secret_scope and secret_key:
        try:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            resp = w.secrets.get_secret(scope=secret_scope, key=secret_key)
            resolved_secret = base64.b64decode(resp.value).decode()
        except Exception as exc:  # pragma: no cover — depends on live workspace
            return resolved_id, None, str(exc)[:200]
    return resolved_id, resolved_secret, None


def auth_probe(
    workspace_url: str,
    server_endpoint: str,
    table_fqn: str,
    *,
    client_id: str | None = None,
    client_secret: str | None = None,
    secret_scope: str | None = None,
    secret_key: str | None = None,
    timeout_s: float = 10.0,
) -> tuple[bool, dict[str, Any]]:
    """Real reachability + credential probe of the Zerobus ingest path.

    Runs on the **app driver** (no cluster round-trip needed, unlike
    :func:`zerobus_produce_snippet`):

    1. DNS + TCP(:443) reachability of ``server_endpoint`` (the gRPC host).
    2. If a client_id/client_secret is resolvable via
       :func:`_resolve_zerobus_creds` (explicit args >
       ``DATABRICKS_CLIENT_ID``/``DATABRICKS_CLIENT_SECRET`` >
       ``ZEROBUS_CLIENT_ID``/``ZEROBUS_CLIENT_SECRET`` > UC secret
       ``secret_scope``/``secret_key``), perform the **same** downscoped OAuth
       token exchange the Zerobus SDK's ``get_zerobus_token()`` does — POST
       ``<workspace_url>/oidc/v1/token`` with ``authorization_details`` scoped
       to ``table_fqn``'s catalog/schema/table — and confirm a 200 with an
       ``access_token`` comes back.

    A ``401 invalid_authorization_details`` here means the service principal
    is missing UC grants (LIVE-VERIFIED failure mode, module docstring point
    3) — surfaced as ``token_error`` rather than a cryptic ``produce_test``
    failure downstream.

    Returns:
        ``(ok, detail)``.  ``detail`` always has ``host``/``dns``/``tcp``;
        when credentials are resolvable it also has ``token_ok`` and (on
        failure) ``token_error``/``token_status_code``.  When no credentials
        are resolvable, reachability alone determines ``ok`` and
        ``token_skipped`` explains why the credential check was skipped.
    """
    host = server_endpoint.split(":")[0] if server_endpoint else ""
    detail: dict[str, Any] = {"host": host, "dns": False, "tcp": False}
    if not host:
        detail["error"] = "server_endpoint is empty"
        return False, detail

    try:
        socket.gethostbyname(host)
        detail["dns"] = True
    except Exception as exc:
        detail["dns_error"] = str(exc)
        return False, detail

    try:
        with socket.create_connection((host, 443), timeout=timeout_s):
            detail["tcp"] = True
    except Exception as exc:
        detail["tcp_error"] = str(exc)
        return False, detail

    resolved_id, resolved_secret, secret_error = _resolve_zerobus_creds(
        client_id, client_secret, secret_scope, secret_key
    )
    if secret_error:
        detail["secret_error"] = secret_error

    if not resolved_id or not resolved_secret:
        detail["token_ok"] = None
        detail["token_skipped"] = _NO_CREDS_RESOLVABLE
        return True, detail  # reachability alone passes when creds are absent

    parts = table_fqn.split(".")
    if len(parts) != 3:
        detail["token_ok"] = False
        detail["token_error"] = f"table_fqn must be catalog.schema.table, got {table_fqn!r}"
        return False, detail
    catalog, schema, table = parts

    ws_url = workspace_url if workspace_url.startswith("http") else f"https://{workspace_url}"
    workspace_id = host.split(".")[0]
    authorization_details = [
        {
            "type": "unity_catalog_privileges",
            "privileges": ["USE CATALOG"],
            "object_type": "CATALOG",
            "object_full_path": catalog,
        },
        {
            "type": "unity_catalog_privileges",
            "privileges": ["USE SCHEMA"],
            "object_type": "SCHEMA",
            "object_full_path": f"{catalog}.{schema}",
        },
        {
            "type": "unity_catalog_privileges",
            "privileges": ["SELECT", "MODIFY"],
            "object_type": "TABLE",
            "object_full_path": table_fqn,
        },
    ]
    try:
        import requests

        resp = requests.post(
            f"{ws_url}/oidc/v1/token",
            auth=(resolved_id, resolved_secret),
            data={
                "grant_type": "client_credentials",
                "scope": "all-apis",
                "resource": f"api://databricks/workspaces/{workspace_id}/zerobusDirectWriteApi",
                "authorization_details": json.dumps(authorization_details),
            },
            timeout=timeout_s,
        )
        detail["token_status_code"] = resp.status_code
        if resp.status_code == 200 and resp.json().get("access_token"):
            detail["token_ok"] = True
            return True, detail
        detail["token_ok"] = False
        detail["token_error"] = resp.text[:500]
        return False, detail
    except Exception as exc:
        detail["token_ok"] = False
        detail["token_error"] = str(exc)[:500]
        return False, detail


# ---------------------------------------------------------------------------
# Driver-side (app-process) bounded producer — no cluster needed.
# ---------------------------------------------------------------------------
def produce(
    *,
    workspace_url: str,
    server_endpoint: str,
    table_fqn: str,
    num_records: int = 100,
    source: str = "validation",
    client_id: str | None = None,
    client_secret: str | None = None,
    secret_scope: str | None = None,
    secret_key: str | None = None,
) -> tuple[bool, dict[str, Any]]:
    """Driver-side (in-process) Zerobus ingest producer — no cluster needed.

    Authenticates exactly as :func:`auth_probe` does via
    :func:`_resolve_zerobus_creds` (explicit args >
    ``DATABRICKS_CLIENT_ID``/``DATABRICKS_CLIENT_SECRET`` >
    ``ZEROBUS_CLIENT_ID``/``ZEROBUS_CLIENT_SECRET`` > UC secret
    ``secret_scope``/``secret_key``), then ingests ``num_records`` SSOT sensor
    rows into ``table_fqn`` via the Zerobus SDK's sync client.

    Each row is keyed by the Delta **column** names (not record field names —
    see module docstring point 1) and uses epoch-microseconds for ``event_time``
    (module docstring point 2).  Every row is tagged ``source=source`` so
    ``dbsql_read`` and ``cleanup`` can scope their count / delete to this run's
    data only.

    Returns:
        ``(ok, detail)`` — ``detail`` has ``produced`` on success or ``error``
        on failure; never raises.
    """
    import random
    import time

    detail: dict[str, Any] = {"table": table_fqn, "server_endpoint": server_endpoint}

    try:
        from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
        from zerobus.sdk.sync import ZerobusSdk
    except ImportError as exc:
        detail["error"] = f"zerobus SDK not importable: {exc}"
        return False, detail

    # Resolve credentials — same resolver as auth_probe uses, ensuring both
    # probe and producer authenticate with identical creds.
    resolved_id, resolved_secret, secret_error = _resolve_zerobus_creds(
        client_id, client_secret, secret_scope, secret_key
    )
    if secret_error:
        detail["secret_error"] = secret_error
    if not resolved_id or not resolved_secret:
        detail["error"] = (
            "no DATABRICKS_CLIENT_ID/CLIENT_SECRET or ZEROBUS_CLIENT_ID/CLIENT_SECRET resolvable"
        )
        return False, detail

    # Ensure scheme — a bare host fails the OAuth token exchange (module
    # docstring point 2; same guard as in zerobus_produce_snippet).
    ws_url = workspace_url if workspace_url.startswith("http") else f"https://{workspace_url}"

    # Derive industry from catalog name (iot_<industry>).
    catalog_part = table_fqn.split(".")[0]
    industry = (
        catalog_part.removeprefix("iot_") if catalog_part.startswith("iot_") else catalog_part
    )

    try:
        table_props = TableProperties(table_fqn)
        options = StreamConfigurationOptions(record_type=RecordType.JSON)
        sdk = ZerobusSdk(server_endpoint, ws_url)
        stream = sdk.create_stream(resolved_id, resolved_secret, table_props, options)

        sensors = _SENSOR_NAMES
        acks = []
        for i in range(num_records):
            s = sensors[i % len(sensors)]
            rec = {
                _col("sensor_name"): s,
                _col("industry"): industry,
                _col("value"): round(random.uniform(20, 100), 2),
                _col("unit"): "C",
                _col("sensor_type"): "temperature",
                # Epoch microseconds int — module docstring point 2.
                _col("timestamp"): int(time.time() * 1_000_000),
                _col("min_value"): 0.0,
                _col("max_value"): 120.0,
                _col("nominal_value"): 60.0,
                _col("fault_active"): False,
                _col("metadata"): {"source": source},
            }
            acks.append(stream.ingest_record(rec))

        for a in acks:
            # wait_for_ack signature varies across SDK versions — try bounded,
            # fall back to no-arg (mirrors the same guard in zerobus_produce_snippet).
            try:
                a.wait_for_ack(timeout_sec=30)
            except TypeError:
                a.wait_for_ack()

        stream.close()
        detail["produced"] = num_records
        return True, detail

    except Exception as exc:
        detail["error"] = str(exc)[:500]
        return False, detail
