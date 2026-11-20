"""Resource-name isolation from the upstream ``iot`` deployment.

This app is deployed side by side with the upstream IoT Digital Twin in the same
workspace. Two guards keep it from ever addressing an upstream resource:

1. every name builder derives from ``RESOURCE_PREFIX`` (default ``pharma-twin``)
   and honours an override;
2. the tracked tree carries no literal in the upstream namespace outside a small,
   justified allowlist.
"""

from __future__ import annotations

import re
import subprocess
from collections.abc import Callable
from pathlib import Path

import pytest

from iot_twin.core import config, resource_names
from iot_twin.services.deployment import deployment_service
from iot_twin.services.industry import industry_catalog_service, industry_service
from iot_twin.services.validation import validation_config

ROOT = Path(__file__).resolve().parents[1]

# --- 1. Builders --------------------------------------------------------------

# (builder, expected with default prefix, expected with RESOURCE_PREFIX=acme-lab)
BUILDERS: list[tuple[str, Callable[[], object], object, object]] = [
    (
        "pipeline_name",
        lambda: resource_names.pipeline_name("pharmaceutical", "realtime"),
        "pharma-twin-pharmaceutical-realtime",
        "acme-lab-pharmaceutical-realtime",
    ),
    (
        "industry_service.pipeline_name",
        lambda: industry_service.pipeline_name("pharmaceutical", "microbatch"),
        "pharma-twin-pharmaceutical-microbatch",
        "acme-lab-pharmaceutical-microbatch",
    ),
    (
        "deployment_service.job_display_name",
        lambda: deployment_service.job_display_name("manufacturing", "realtime"),
        "pharma-twin-manufacturing-realtime",
        "acme-lab-manufacturing-realtime",
    ),
    (
        "industry_service.topic_name",
        lambda: industry_service.topic_name("pharmaceutical", "realtime"),
        "pharma-twin.pharmaceutical.realtime",
        "acme-lab.pharmaceutical.realtime",
    ),
    (
        "validation_config.topic_name",
        lambda: validation_config.topic_name("Oil & Gas", "microbatch"),
        "pharma-twin.oil_gas.microbatch",
        "acme-lab.oil_gas.microbatch",
    ),
    (
        "industry_service.catalog_name",
        lambda: industry_service.catalog_name("pharmaceutical"),
        "pharma_twin_pharmaceutical",
        "acme_lab_pharmaceutical",
    ),
    (
        "industry_catalog_service.catalog_name",
        lambda: industry_catalog_service.catalog_name("Oil & Gas"),
        "pharma_twin_oil_gas",
        "acme_lab_oil_gas",
    ),
    (
        "catalog_pattern",
        resource_names.catalog_pattern,
        "pharma_twin_{name}",
        "acme_lab_{name}",
    ),
    (
        "shared_catalog_name",
        resource_names.shared_catalog_name,
        "pharma_twin_shared",
        "acme_lab_shared",
    ),
    (
        "default_catalog_name",
        resource_names.default_catalog_name,
        "pharma_twin_default",
        "acme_lab_default",
    ),
    (
        "pipeline_catalog_name",
        lambda: resource_names.pipeline_catalog_name("pipe", "abc123"),
        "pharma_twin_pipe_abc123",
        "acme_lab_pipe_abc123",
    ),
    (
        "shared_secret_scope",
        resource_names.shared_secret_scope,
        "pharma-twin-secrets",
        "acme-lab-secrets",
    ),
    (
        "pipeline_secret_scope",
        lambda: resource_names.pipeline_secret_scope("abcd1234"),
        "pharma-twin-pipeline-abcd1234",
        "acme-lab-pipeline-abcd1234",
    ),
    (
        "streaming_sp_name",
        lambda: resource_names.streaming_sp_name("scope1"),
        "pharma-twin-sss-scope1",
        "acme-lab-sss-scope1",
    ),
    (
        "shared_streaming_sp_name",
        resource_names.shared_streaming_sp_name,
        "pharma-twin-sss-industries",
        "acme-lab-sss-industries",
    ),
    (
        "app_lakebase_role_id",
        resource_names.app_lakebase_role_id,
        "pharma-twin-app-user",
        "acme-lab-app-user",
    ),
    ("admin_group", resource_names.admin_group, "pharma-twin-admins", "acme-lab-admins"),
    (
        "system_owner_email",
        resource_names.system_owner_email,
        "industries@pharma-twin.system",
        "industries@acme-lab.system",
    ),
    ("app_name", resource_names.app_name, "pharma-twin", "acme-lab"),
    (
        "app_backend_project",
        resource_names.app_backend_project,
        "pharma-twin-backend",
        "acme-lab-backend",
    ),
    (
        "pipelines_project",
        resource_names.pipelines_project,
        "pharma-twin-pipelines",
        "acme-lab-pipelines",
    ),
    (
        "lakets_demo_project",
        resource_names.lakets_demo_project,
        "pharma-twin-lakets-demo",
        "acme-lab-lakets-demo",
    ),
    ("warehouse_name", resource_names.warehouse_name, "pharma-twin", "acme-lab"),
    ("workspace_subdir", resource_names.workspace_subdir, ".pharma-twin", ".acme-lab"),
    (
        "user_workspace_dir",
        lambda: resource_names.user_workspace_dir("u@example.com"),
        "/Workspace/Users/u@example.com/.pharma-twin",
        "/Workspace/Users/u@example.com/.acme-lab",
    ),
    (
        "lakets_workspace_subdir",
        resource_names.lakets_workspace_subdir,
        "pharma-twin/lakets",
        "acme-lab/lakets",
    ),
    ("metrics_storage_segment", resource_names.metrics_storage_segment, "pharma-twin", "acme-lab"),
    (
        "validation cluster default",
        lambda: validation_config.build_validation_cluster_spec(
            node_type_id="i3.xlarge", num_workers=1, single_user_name="u", spark_version="18.2"
        )["cluster_name"],
        "pharma-twin-validation-harness",
        "acme-lab-validation-harness",
    ),
    (
        "consumer_group",
        lambda: resource_names.consumer_group("pharmaceutical"),
        "pharma-twin-pharmaceutical",
        "acme-lab-pharmaceutical",
    ),
    (
        "run_name",
        lambda: resource_names.run_name("producer", "kafka", "1"),
        "pharma-twin-producer-kafka-1",
        "acme-lab-producer-kafka-1",
    ),
    (
        "resource_tags",
        lambda: resource_names.resource_tags("pipeline", job="j"),
        {"app": "pharma-twin", "component": "pipeline", "job": "j"},
        {"app": "acme-lab", "component": "pipeline", "job": "j"},
    ),
]


@pytest.mark.parametrize(
    ("builder", "expected_default"),
    [(b, d) for _, b, d, _ in BUILDERS],
    ids=[n for n, *_ in BUILDERS],
)
def test_builder_uses_default_prefix(monkeypatch, builder, expected_default) -> None:
    monkeypatch.delenv(config.RESOURCE_PREFIX_ENV, raising=False)
    assert builder() == expected_default


@pytest.mark.parametrize(
    ("builder", "expected_override"),
    [(b, o) for _, b, _, o in BUILDERS],
    ids=[n for n, *_ in BUILDERS],
)
def test_builder_honours_override(monkeypatch, builder, expected_override) -> None:
    monkeypatch.setenv(config.RESOURCE_PREFIX_ENV, "acme-lab")
    assert builder() == expected_override


def test_prefix_ident_form(monkeypatch) -> None:
    monkeypatch.delenv(config.RESOURCE_PREFIX_ENV, raising=False)
    assert config.get_resource_prefix() == config.DEFAULT_RESOURCE_PREFIX == "pharma-twin"
    assert config.get_resource_prefix_ident() == "pharma_twin"


@pytest.mark.parametrize(
    "bad", ["iot", "iot-twin", "iot_x", "IOT", "Pharma Twin", "a", "x--y", "-x", "x-"]
)
def test_rejects_legacy_or_malformed_prefix(monkeypatch, bad: str) -> None:
    monkeypatch.setenv(config.RESOURCE_PREFIX_ENV, bad)
    with pytest.raises(ValueError):
        config.get_resource_prefix()


def test_blank_prefix_falls_back_to_default(monkeypatch) -> None:
    monkeypatch.setenv(config.RESOURCE_PREFIX_ENV, "  ")
    assert config.get_resource_prefix() == "pharma-twin"


def test_shared_service_credential_is_configurable(monkeypatch) -> None:
    monkeypatch.delenv("UC_SERVICE_CREDENTIAL", raising=False)
    assert config.get_uc_service_credential() == config.SHARED_MSK_IAM_UC_NAME
    monkeypatch.setenv("UC_SERVICE_CREDENTIAL", "other-sc")
    assert config.get_uc_service_credential() == "other-sc"


# --- 2. Legacy-literal grep ----------------------------------------------------

SCAN_PATHS = (
    "src",
    "scripts",
    "deploy",
    "resources",
    "databricks.yml",
    "app.yaml",
    "tests",
    "client/src",
    "ops",
    "usecases",
)

LEGACY_PATTERNS = re.compile(
    r"iot-\{|iot\.\{|iot_\{"  # f-string name builders in the upstream namespace
    r"|iot[-._]<"  # documented templates such as iot-<industry>-<mode>
    r"|iot-secrets|iot-digital-twin|iot-twin-dev|iot-app-backend|iot-pipelines"
    r"|iot-sss|iot-app-user|iot_app_user|iot-admins|iot-lakets"
    r"|iot[-._](?:pharmaceutical|manufacturing)"  # literal upstream jobs/topics/catalogs
    r"|iot_[\"']"  # a bare "iot_" prefix literal (e.g. removeprefix("iot_"))
)

# path -> justification. Whole files, so keep entries to files whose purpose is
# to describe the upstream namespace; prefer fixing code over growing this.
ALLOWLIST: dict[str, str] = {
    # Describes the upstream namespace it rejects (docstring + validation regex comment).
    "src/iot_twin/core/config.py": "documents the rejected upstream namespace",
    # Isolation-guard comment naming what the bundle must never touch.
    "databricks.yml": "isolation guard comment lists the upstream names",
    # This file: the pattern itself and the known-legacy fixtures that prove it matches.
    "tests/test_resource_isolation.py": "guard pattern + legacy-form fixtures",
    # Redacted LIVE-RUN snapshots captured from the upstream deployment before the
    # fork (immutable records, regenerated only by re-running the notebook here).
    "ops/notebooks/runs/README.md": "describes the upstream live-run snapshots",
    "ops/notebooks/runs/pharmaceutical_realtime_2026-11-20.html": "upstream live-run snapshot",
    "ops/notebooks/runs/pharmaceutical_realtime_2026-11-20.ipynb": "upstream live-run snapshot",
}


def _tracked_files() -> list[str]:
    out = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "--", *SCAN_PATHS],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return [line for line in out.splitlines() if line]


def _scan() -> list[str]:
    hits: list[str] = []
    for rel in _tracked_files():
        path = ROOT / rel
        if not path.is_file() or path.suffix in {".whl", ".png", ".ico"}:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for lineno, line in enumerate(text.splitlines(), 1):
            if LEGACY_PATTERNS.search(line) and rel not in ALLOWLIST:
                hits.append(f"{rel}:{lineno}: {line.strip()}")
    return hits


def test_scan_covers_the_tree() -> None:
    files = _tracked_files()
    assert any(f.startswith("src/") for f in files)
    assert "databricks.yml" in files and "app.yaml" in files


def test_no_legacy_resource_literals() -> None:
    hits = _scan()
    assert hits == [], (
        "upstream 'iot' resource names found; build them via iot_twin.core.resource_names:\n"
        + "\n".join(hits)
    )


def test_allowlist_entries_are_still_needed() -> None:
    for rel in ALLOWLIST:
        text = (ROOT / rel).read_text(encoding="utf-8")
        assert LEGACY_PATTERNS.search(text), f"{rel} no longer needs its allowlist entry"


@pytest.mark.parametrize(
    "line",
    [
        'f"iot-{industry}-{mode}"',
        'f"iot.{industry}.{mode}"',
        'f"iot_{safe}"',
        'scope = "iot-secrets"',
        "app_name: iot-digital-twin",
        'sp = "iot-sss-industries"',
        'ROLE = "iot_app_user"',
        'jn = "iot-pharmaceutical-realtime"',
        'industry = name.removeprefix("iot_")',
    ],
)
def test_pattern_catches_known_legacy_forms(line: str) -> None:
    assert LEGACY_PATTERNS.search(line)


@pytest.mark.parametrize(
    "line",
    [
        "from iot_twin.core import config",
        "import iot_simulator",
        'table = "iot_sensor_data"',
        'sc_name = "iot-rtm-sc"',
    ],
)
def test_pattern_ignores_product_strings(line: str) -> None:
    assert not LEGACY_PATTERNS.search(line)


def test_scan_covers_tests_client_ops_and_usecases() -> None:
    files = _tracked_files()
    for head in ("tests/", "client/src/", "ops/", "usecases/"):
        assert any(f.startswith(head) for f in files), head


# --- 3. Ownership guard ----------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    [
        "pharma-twin",
        "pharma-twin-backend",
        "pharma-twin.pharmaceutical.realtime",
        "pharma_twin_pharmaceutical",
        "projects/pharma-twin-pipelines/branches/pharmaceutical",
        "projects/pharma-twin-backend/branches/production/endpoints/primary",
    ],
)
def test_owned_names_accepted(monkeypatch, name: str) -> None:
    monkeypatch.delenv(config.RESOURCE_PREFIX_ENV, raising=False)
    assert config.assert_owned_name(name) == name


@pytest.mark.parametrize(
    "name",
    [
        "",
        "iot-pipelines",
        "iot-app-backend",
        "iot-lakets-demo",
        "iot_pharmaceutical",
        "projects/iot-pipelines/branches/pharmaceutical",
        "projects/iot-app-backend/branches/production/endpoints/primary",
        "pharma-twinx",
        "other-project",
        "projects/other/branches/pharma-twin",
    ],
)
def test_foreign_names_rejected(monkeypatch, name: str) -> None:
    monkeypatch.delenv(config.RESOURCE_PREFIX_ENV, raising=False)
    with pytest.raises(ValueError):
        config.assert_owned_name(name)


def test_ownership_follows_prefix_override(monkeypatch) -> None:
    monkeypatch.setenv(config.RESOURCE_PREFIX_ENV, "acme-lab")
    assert config.is_owned_name("acme-lab-pipelines")
    assert not config.is_owned_name("pharma-twin-pipelines")


@pytest.mark.parametrize(
    ("var", "getter"),
    [
        ("PIPELINES_LAKEBASE_PROJECT", "get_pipelines_project"),
        ("SECRET_SCOPE", "get_secret_scope"),
        ("ZEROBUS_SECRET_SCOPE", "get_secret_scope"),
        ("LAKEBASE_PROJECT", "get_lakebase_project"),
        ("LAKEBASE_ENDPOINT", "get_lakebase_endpoint"),
    ],
)
def test_workspace_getters_reject_foreign_overrides(monkeypatch, var: str, getter: str) -> None:
    from iot_twin.core import workspace_config

    for other in ("SECRET_SCOPE", "ZEROBUS_SECRET_SCOPE"):
        monkeypatch.delenv(other, raising=False)
    monkeypatch.setenv(var, "projects/iot-app-backend/x" if "ENDPOINT" in var else "iot-secrets")
    with pytest.raises(ValueError):
        getattr(workspace_config, getter)()


def test_admin_group_rejects_foreign_override(monkeypatch) -> None:
    from iot_twin.services import app_permissions

    monkeypatch.setenv("ADMIN_GROUP", "iot-admins")
    with pytest.raises(ValueError):
        app_permissions._admin_group()


@pytest.mark.parametrize("project", ["iot-lakets-demo", "other", ""])
def test_lakets_deploy_rejects_foreign_project(project: str) -> None:
    from unittest.mock import MagicMock

    from iot_twin.services.lakets import lakets_deploy_service as lds

    w = MagicMock()
    with pytest.raises(ValueError):
        lds.deploy_lakets_ops_jobs(w, project)
    with pytest.raises(ValueError):
        lds.deploy_retention_job(project)
    w.jobs.list.assert_not_called()


def test_lakets_job_names_are_prefixed(monkeypatch) -> None:
    from iot_twin.services.lakets import lakets_deploy_service as lds

    monkeypatch.delenv(config.RESOURCE_PREFIX_ENV, raising=False)
    name = lds._job_display_name("pharma-twin-lakets-demo", "production")
    assert name == "pharma-twin LakeTS Retention — pharma-twin-lakets-demo/production"


# --- 4. Live-test gate / bundle / scripts ----------------------------------------


def test_live_workspace_client_is_blocked_by_default() -> None:
    from tests.conftest import LiveWorkspaceBlocked, live_tests_enabled

    if live_tests_enabled():
        pytest.skip("live tests opted in")
    sdk = pytest.importorskip("databricks.sdk")
    if not isinstance(sdk.WorkspaceClient, type):
        pytest.skip("databricks.sdk is stubbed")
    with pytest.raises(LiveWorkspaceBlocked):
        sdk.WorkspaceClient(profile="any-real-profile")


def test_app_backend_test_endpoint_is_owned(monkeypatch) -> None:
    from tests.conftest import app_backend_endpoint

    monkeypatch.delenv("APP_BACKEND_LAKEBASE_ENDPOINT", raising=False)
    monkeypatch.delenv(config.RESOURCE_PREFIX_ENV, raising=False)
    assert app_backend_endpoint().startswith("projects/pharma-twin-backend/")
    monkeypatch.setenv(
        "APP_BACKEND_LAKEBASE_ENDPOINT",
        "projects/iot-app-backend/branches/production/endpoints/primary",
    )
    with pytest.raises(ValueError):
        app_backend_endpoint()


def _bundle_variables() -> dict:
    import yaml

    return yaml.safe_load((ROOT / "databricks.yml").read_text())["variables"]


def test_bundle_defaults_derive_from_resource_prefix() -> None:
    variables = _bundle_variables()
    prefix = variables["resource_prefix"]["default"]
    assert prefix == config.DEFAULT_RESOURCE_PREFIX
    assert variables["resource_prefix_ident"]["default"] == prefix.replace("-", "_")
    assert variables["app_name"]["default"] == "${var.resource_prefix}-dev"
    assert variables["lakebase_project"]["default"] == "${var.resource_prefix}-backend"
    assert variables["pipelines_lakebase_project"]["default"] == "${var.resource_prefix}-pipelines"
    assert variables["admin_group"]["default"] == "${var.resource_prefix}-admins"
    assert variables["uc_schema"]["default"] == "${var.resource_prefix_ident}"


def test_e2e_script_guards_destructive_stages() -> None:
    source = (ROOT / "scripts" / "e2e" / "pharma_e2e.py").read_text()
    assert "pharma_e2e_state.json" not in source
    assert 'f"/tmp/{get_resource_prefix()}_e2e_state.json"' in source
    teardown = source[source.index("async def stage_teardown") :]
    teardown = teardown[: teardown.index("async def stage_cleanup_cloud")]
    assert teardown.index("_assert_owned_setup(") < teardown.index("teardown_setup(")
    assert "= build_app_backend_manager(" not in source  # env-sourced host is not trusted
