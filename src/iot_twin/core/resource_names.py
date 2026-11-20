"""Single source of truth for every resource name the app builds.

All builders derive from :func:`iot_twin.core.config.get_resource_prefix`
(``RESOURCE_PREFIX``, default ``pharma-twin``) and read it on every call, so an
override takes effect without a re-import.  Never format a resource name with a
literal namespace anywhere else: ``tests/test_resource_isolation.py`` greps the
tree for the upstream ``iot`` namespace and fails on new occurrences.

Hyphenated form (``pharma-twin-...``): jobs, Lakebase projects, secret scopes,
clusters, run names, service principals, workspace dirs, tags.
Underscore form (``pharma_twin_...``): Unity Catalog catalogs.
Dotted form (``pharma-twin.<industry>.<mode>``): Kafka topics.
"""

from __future__ import annotations

from iot_twin.core.config import get_resource_prefix, get_resource_prefix_ident


def prefixed(*parts: str) -> str:
    """Return ``<prefix>-<part>-...`` (the hyphenated resource-name form)."""
    return "-".join([get_resource_prefix(), *parts])


def prefixed_ident(*parts: str) -> str:
    """Return ``<prefix_ident>_<part>_...`` (the UC / Postgres identifier form)."""
    return "_".join([get_resource_prefix_ident(), *parts])


# --- Streaming pipelines ------------------------------------------------------
def pipeline_name(industry: str, mode: str) -> str:
    """Per-mode pipeline / job / per-pipeline Lakebase project name."""
    return prefixed(industry, mode)


def topic_name(industry: str, mode: str) -> str:
    """Kafka topic ``<prefix>.<industry>.<mode>``."""
    return f"{get_resource_prefix()}.{industry}.{mode}"


def consumer_group(industry: str) -> str:
    """Kafka consumer group for an industry's producer/consumer pair."""
    return prefixed(industry)


# --- Unity Catalog ------------------------------------------------------------
def catalog_name(industry: str) -> str:
    """Per-industry catalog ``<prefix_ident>_<industry>`` (``industry`` must be UC-safe)."""
    return prefixed_ident(industry)


def catalog_pattern() -> str:
    """Default ``{name}`` catalog pattern used by the settings / provisioning flows."""
    return prefixed_ident("{name}")


def shared_catalog_name() -> str:
    """Catalog used when all pipelines share one catalog."""
    return prefixed_ident("shared")


def default_catalog_name() -> str:
    """Catalog used when the configured pattern resolves to nothing."""
    return prefixed_ident("default")


def pipeline_catalog_name(safe_name: str, short_id: str) -> str:
    """Per-pipeline catalog ``<prefix_ident>_<name>_<short_id>``."""
    return prefixed_ident(safe_name, short_id)


# --- Secrets / identities -----------------------------------------------------
def shared_secret_scope() -> str:
    """Shared app secret scope (``<prefix>-secrets``)."""
    return prefixed("secrets")


def pipeline_secret_scope(short_id: str) -> str:
    """Per-pipeline secret scope (``<prefix>-pipeline-<short_id>``)."""
    return prefixed("pipeline", short_id)


def streaming_sp_name(scope_name: str) -> str:
    """Per-scope streaming service-principal display name."""
    return prefixed("sss", scope_name)


def shared_streaming_sp_name() -> str:
    """Shared streaming service principal display name (and its Lakebase role id)."""
    return prefixed("sss-industries")


def app_lakebase_role_id() -> str:
    """Lakebase role id the app-backend grants are written for."""
    return prefixed("app-user")


def admin_group() -> str:
    """Default workspace group whose members are app admins."""
    return prefixed("admins")


def system_owner_email() -> str:
    """Owner recorded on app-wide (curated industry) pipeline rows."""
    return f"industries@{get_resource_prefix()}.system"


# --- App / Lakebase projects ---------------------------------------------------
def app_name() -> str:
    """Default Databricks App name (prod) and ``app`` tag value."""
    return get_resource_prefix()


def app_backend_project() -> str:
    """Default app-backend Lakebase project."""
    return prefixed("backend")


def pipelines_project() -> str:
    """Default shared streaming-sink Lakebase project."""
    return prefixed("pipelines")


def lakets_demo_project() -> str:
    """Default Lakebase project for the LakeTS demo / smoke scripts."""
    return prefixed("lakets-demo")


def warehouse_name() -> str:
    """Default name for a SQL warehouse created from the settings page."""
    return get_resource_prefix()


# --- Workspace paths / compute / tags -------------------------------------------
def workspace_subdir() -> str:
    """Hidden per-user workspace directory (``.<prefix>``) for uploaded notebooks."""
    return f".{get_resource_prefix()}"


def user_workspace_dir(username: str) -> str:
    """``/Workspace/Users/<username>/.<prefix>`` — the app's per-user upload root."""
    return f"/Workspace/Users/{username}/{workspace_subdir()}"


def lakets_workspace_subdir() -> str:
    """Per-user workspace sub-path for the LakeTS retention notebooks."""
    return f"{get_resource_prefix()}/lakets"


def metrics_storage_segment() -> str:
    """Path segment isolating this app's external metrics tables under the storage base."""
    return get_resource_prefix()


def cluster_name(*parts: str) -> str:
    """Cluster name ``<prefix>-<part>-...``."""
    return prefixed(*parts)


def run_name(*parts: str) -> str:
    """One-time job run name ``<prefix>-<part>-...``."""
    return prefixed(*parts)


def lakets_job_name(kind: str, project_id: str, branch_id: str) -> str:
    """LakeTS ops job display name, e.g. ``pharma-twin LakeTS Retention — <project>/<branch>``.

    Prefixed so the by-name lookup can never match an upstream LakeTS job.
    """
    return f"{get_resource_prefix()} LakeTS {kind} — {project_id}/{branch_id}"


def resource_tags(component: str, **extra: str) -> dict[str, str]:
    """Tags stamped on every job the app creates (``app`` = :func:`app_name`)."""
    return {"app": app_name(), "component": component, **extra}
