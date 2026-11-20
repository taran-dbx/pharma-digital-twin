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

SCAN_PATHS = ("src", "scripts", "deploy", "resources", "databricks.yml", "app.yaml")

LEGACY_PATTERNS = re.compile(
    r"iot-\{|iot\.\{|iot_\{"  # f-string name builders in the upstream namespace
    r"|iot[-._]<"  # documented templates such as iot-<industry>-<mode>
    r"|iot-secrets|iot-digital-twin|iot-twin-dev|iot-app-backend|iot-pipelines"
    r"|iot-sss|iot-app-user|iot_app_user|iot-admins|iot-lakets"
    r"|iot[-._](?:pharmaceutical|manufacturing)"  # literal upstream jobs/topics/catalogs
)

# path -> justification. Whole files, so keep entries to files whose purpose is
# to describe the upstream namespace; prefer fixing code over growing this.
ALLOWLIST: dict[str, str] = {
    # Describes the upstream namespace it rejects (docstring + validation regex comment).
    "src/iot_twin/core/config.py": "documents the rejected upstream namespace",
    # Isolation-guard comment naming what the bundle must never touch.
    "databricks.yml": "isolation guard comment lists the upstream names",
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
        'UC_SERVICE_CREDENTIAL = "iot-rtm-sc"',
    ],
)
def test_pattern_ignores_product_strings(line: str) -> None:
    assert not LEGACY_PATTERNS.search(line)
