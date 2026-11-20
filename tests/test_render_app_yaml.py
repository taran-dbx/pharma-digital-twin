"""Tests for scripts/render_app_yaml.py — workspace deployment profile renderer."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "render_app_yaml.py"
PROFILES_DIR = ROOT / "deploy" / "workspaces"

_EXPECTED_ENV_NAMES = (
    "UC_CATALOG",
    "UC_SCHEMA",
    "PGHOST",
    "PGDATABASE",
    "PGPORT",
    "LAKEBASE_PROJECT",
    "LAKEBASE_ENDPOINT",
    "DATABRICKS_SQL_WAREHOUSE_ID",
    "ZEROBUS_SERVER_ENDPOINT",
    "ZEROBUS_TABLE_NAME",
    "ZEROBUS_CLIENT_ID",
    "ZEROBUS_SECRET_SCOPE",
    "ZEROBUS_SECRET_KEY",
    "UC_STORAGE_BASE",
    "UC_EXTERNAL_LOCATION",
    "UC_SERVICE_CREDENTIAL",
    "PIPELINES_LAKEBASE_PROJECT",
    "MSK_BOOTSTRAP_SERVERS",
    "INDUSTRIES",
    "ADMIN_GROUP",
    "GENIE_SPACE_ID",
    "USAGE_TRACKING_ENABLED",
)

_MINIMAL_PROFILE = {
    "UC_CATALOG": "test-catalog",
    "UC_SCHEMA": "test_schema",
    "PGHOST": "",
    "LAKEBASE_PROJECT": "iot-app-backend",
    "LAKEBASE_ENDPOINT": "projects/iot-app-backend/branches/production/endpoints/primary",
    "DATABRICKS_SQL_WAREHOUSE_ID": "warehouse-id",
    "ZEROBUS_SERVER_ENDPOINT": "123.zerobus.us-east-1.cloud.databricks.com",
    "ZEROBUS_CLIENT_ID": "",
    "ZEROBUS_SECRET_SCOPE": "",
    "ZEROBUS_SECRET_KEY": "",
    "UC_STORAGE_BASE": "",
    "UC_EXTERNAL_LOCATION": "",
    "UC_SERVICE_CREDENTIAL": "",
    "PIPELINES_LAKEBASE_PROJECT": "",
    "MSK_BOOTSTRAP_SERVERS": "",
    "INDUSTRIES": "pharmaceutical",
    "ADMIN_GROUP": "iot-admins",
}


def _load_renderer():
    spec = importlib.util.spec_from_file_location("render_app_yaml", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["render_app_yaml"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def renderer():
    return _load_renderer()


@pytest.fixture()
def profiles_dir(tmp_path: Path) -> Path:
    workspace_dir = tmp_path / "workspaces"
    workspace_dir.mkdir()
    (workspace_dir / "sample.profile").write_text(
        json.dumps(_MINIMAL_PROFILE, indent=2) + "\n",
        encoding="utf-8",
    )
    return workspace_dir


def test_unknown_profile_raises(renderer, profiles_dir: Path) -> None:
    with pytest.raises(renderer.ProfileError, match="unknown profile"):
        renderer.load_profile("no-such-profile", profiles_dir)


def test_missing_required_key_raises(renderer, profiles_dir: Path) -> None:
    incomplete = dict(_MINIMAL_PROFILE)
    del incomplete["UC_CATALOG"]
    (profiles_dir / "broken.profile").write_text(json.dumps(incomplete), encoding="utf-8")

    with pytest.raises(renderer.ProfileError, match="UC_CATALOG"):
        renderer.load_profile("broken", profiles_dir)


def test_render_includes_runtime_command_and_all_env_names(renderer) -> None:
    rendered = renderer.render_app_yaml(_MINIMAL_PROFILE)

    assert (
        "uvicorn --app-dir src iot_twin.app:app --host 0.0.0.0 --port ${DATABRICKS_APP_PORT:-8000}"
        in rendered
    )
    for env_name in _EXPECTED_ENV_NAMES:
        assert f"name: {env_name}" in rendered
    assert "valueFrom: genie-space" in rendered
    assert "GENIE_SPACE_ID" in rendered
    assert "name: DATABRICKS_SQL_WAREHOUSE_ID\n    valueFrom: sql-warehouse" in rendered
    assert "warehouse-id" not in rendered
    assert "LAKETS_SQL_PATH" not in rendered


def test_render_emits_lakets_path_only_when_overridden(renderer) -> None:
    rendered = renderer.render_app_yaml({**_MINIMAL_PROFILE, "LAKETS_SQL_PATH": "/x/lakets.sql"})
    assert 'name: LAKETS_SQL_PATH\n    value: "/x/lakets.sql"' in rendered


def test_render_quotes_values(renderer) -> None:
    rendered = renderer.render_app_yaml(_MINIMAL_PROFILE)
    assert 'value: "test-catalog"' in rendered
    assert 'value: ""' in rendered


def test_eu_example_profile_values(renderer) -> None:
    profile = renderer.load_profile("example-eu-central", PROFILES_DIR, use_example=True)

    assert profile["UC_CATALOG"] == "example_eu_catalog"
    assert profile["DATABRICKS_SQL_WAREHOUSE_ID"] == "0123456789abcdef"
    assert profile["ZEROBUS_CLIENT_ID"] == "00000000-0000-0000-0000-00000000000a"
    assert profile["UC_SERVICE_CREDENTIAL"] == "iot-rtm-sc"
    assert profile["PIPELINES_LAKEBASE_PROJECT"] == "iot-pipelines"
    assert profile["MSK_BOOTSTRAP_SERVERS"] == (
        "boot-public-9dm.example.abc123.c1.kafka.eu-central-1.amazonaws.com:9198,"
        "boot-public-hak.example.abc123.c1.kafka.eu-central-1.amazonaws.com:9198,"
        "boot-public-mnx.example.abc123.c1.kafka.eu-central-1.amazonaws.com:9198"
    )
    assert profile["ZEROBUS_SECRET_SCOPE"] == ""
    assert profile["ZEROBUS_SECRET_KEY"] == ""


def test_us_example_profile_values(renderer) -> None:
    profile = renderer.load_profile("example-us-east", PROFILES_DIR, use_example=True)

    assert profile["UC_CATALOG"] == "example-catalog"
    assert profile["UC_SCHEMA"] == "rtm_lakets_zb"
    assert profile["DATABRICKS_SQL_WAREHOUSE_ID"] == "0123456789abcdef"
    assert (
        profile["ZEROBUS_SERVER_ENDPOINT"]
        == "111111111111111.zerobus.us-east-1.cloud.databricks.com"
    )
    assert profile["UC_SERVICE_CREDENTIAL"] == "iot-rtm-sc"
    assert profile["PIPELINES_LAKEBASE_PROJECT"] == "iot-pipelines-example"
    assert profile["PGHOST"] == "example-lakebase.database.us-east-1.cloud.databricks.com"
    assert profile["LAKEBASE_PROJECT"] == "iot-app-backend-example"
    assert (
        profile["LAKEBASE_ENDPOINT"]
        == "projects/iot-app-backend-example/branches/production/endpoints/primary"
    )
    assert profile["ZEROBUS_CLIENT_ID"] == "00000000-0000-0000-0000-00000000000b"
    assert profile["ZEROBUS_SECRET_SCOPE"] == ""
    assert profile["ZEROBUS_SECRET_KEY"] == ""
    assert profile["MSK_BOOTSTRAP_SERVERS"] == (
        "b-1-public.example.abc123.c1.kafka.us-east-1.amazonaws.com:9198,"
        "b-2-public.example.abc123.c1.kafka.us-east-1.amazonaws.com:9198,"
        "b-3-public.example.abc123.c1.kafka.us-east-1.amazonaws.com:9198"
    )


def test_us_example_render_uses_blank_zerobus_secret_env(renderer) -> None:
    profile = renderer.load_profile("example-us-east", PROFILES_DIR, use_example=True)
    rendered = renderer.render_app_yaml(profile)

    assert 'name: ZEROBUS_SECRET_SCOPE\n    value: ""' in rendered
    assert 'name: ZEROBUS_SECRET_KEY\n    value: ""' in rendered
    assert "iot-secrets" not in rendered


def test_eu_example_render_uses_blank_zerobus_secret_env(renderer) -> None:
    profile = renderer.load_profile("example-eu-central", PROFILES_DIR, use_example=True)
    rendered = renderer.render_app_yaml(profile)

    assert 'name: ZEROBUS_SECRET_SCOPE\n    value: ""' in rendered
    assert 'name: ZEROBUS_SECRET_KEY\n    value: ""' in rendered
    assert "iot-secrets" not in rendered


def test_committed_app_yaml_matches_example_profile(renderer) -> None:
    profile = renderer.load_profile("example-us-east", PROFILES_DIR, use_example=True)
    app_yaml = ROOT / "app.yaml"

    assert app_yaml.is_file(), "app.yaml must exist at repo root"
    assert renderer.check_app_yaml(app_yaml, profile), (
        "app.yaml drifted from example-us-east profile; "
        "run: python scripts/render_app_yaml.py example-us-east --example"
    )


def test_check_matches_generated_content(renderer, tmp_path: Path) -> None:
    app_yaml = tmp_path / "app.yaml"
    app_yaml.write_text(renderer.render_app_yaml(_MINIMAL_PROFILE), encoding="utf-8")

    assert renderer.check_app_yaml(app_yaml, _MINIMAL_PROFILE) is True


def test_check_detects_mismatch(renderer, tmp_path: Path) -> None:
    app_yaml = tmp_path / "app.yaml"
    app_yaml.write_text("command: []\nenv: []\n", encoding="utf-8")

    assert renderer.check_app_yaml(app_yaml, _MINIMAL_PROFILE) is False


def test_cli_check_exit_code(renderer, profiles_dir: Path, tmp_path: Path) -> None:
    app_yaml = tmp_path / "app.yaml"
    app_yaml.write_text(renderer.render_app_yaml(_MINIMAL_PROFILE), encoding="utf-8")

    ok = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "sample",
            "--profiles-dir",
            str(profiles_dir),
            "--output",
            str(app_yaml),
            "--check",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert ok.returncode == 0

    bad = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "sample",
            "--profiles-dir",
            str(profiles_dir),
            "--output",
            str(tmp_path / "missing.yaml"),
            "--check",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert bad.returncode != 0


def test_cli_writes_output(renderer, profiles_dir: Path, tmp_path: Path) -> None:
    out = tmp_path / "app.yaml"
    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "sample",
            "--profiles-dir",
            str(profiles_dir),
            "--output",
            str(out),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0
    assert out.exists()
    assert renderer.check_app_yaml(out, _MINIMAL_PROFILE)


def test_admin_emails_is_optional_and_omitted_when_unset(renderer) -> None:
    rendered = renderer.render_app_yaml(_MINIMAL_PROFILE)
    assert "ADMIN_EMAILS" not in rendered
    assert "name: ADMIN_GROUP" in rendered


def test_admin_emails_emitted_when_profile_sets_it(renderer) -> None:
    profile = {**_MINIMAL_PROFILE, "ADMIN_EMAILS": "ops@example.com"}
    rendered = renderer.render_app_yaml(profile)
    assert 'name: ADMIN_EMAILS\n    value: "ops@example.com"' in rendered


def test_usage_tracking_defaults_to_enabled(renderer) -> None:
    rendered = renderer.render_app_yaml(_MINIMAL_PROFILE)
    assert 'name: USAGE_TRACKING_ENABLED\n    value: "true"' in rendered


def test_usage_tracking_opt_out_from_profile(renderer, tmp_path: Path) -> None:
    profile = {**_MINIMAL_PROFILE, "USAGE_TRACKING_ENABLED": "false"}
    (tmp_path / "optout.profile").write_text(json.dumps(profile), encoding="utf-8")
    loaded = renderer.load_profile("optout", tmp_path)
    rendered = renderer.render_app_yaml(loaded)
    assert 'name: USAGE_TRACKING_ENABLED\n    value: "false"' in rendered


def test_real_profile_lookup_ignores_example_templates(renderer, tmp_path: Path) -> None:
    (tmp_path / "only.profile.example").write_text(json.dumps(_MINIMAL_PROFILE), encoding="utf-8")
    with pytest.raises(renderer.ProfileError, match="profile.example"):
        renderer.load_profile("only", tmp_path)
    assert renderer.load_profile("only", tmp_path, use_example=True)["UC_SCHEMA"] == "test_schema"


def _bundle_env_names() -> set[str]:
    names: set[str] = set()
    for line in (ROOT / "resources" / "app.yml").read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("- name: ") and stripped[8:].isupper():
            names.add(stripped[8:])
    return names


def _app_yaml_env_names() -> set[str]:
    return {
        line.split("name:", 1)[1].strip()
        for line in (ROOT / "app.yaml").read_text(encoding="utf-8").splitlines()
        if line.strip().startswith("- name:")
    }


def test_bundle_env_matches_app_yaml() -> None:
    """resources/app.yml must carry the same env names as app.yaml (no silent drift)."""
    bundle, app_yaml = _bundle_env_names(), _app_yaml_env_names()
    # ADMIN_EMAILS is optional and absent from both (the bundle never sets it).
    # The Apps API rejects empty env values for bundle `config`, so the entries that
    # app.yaml sets to "" (unset-equivalent at runtime) cannot be expressed in the
    # bundle; they are listed explicitly so any other drift still fails.
    empty_only = {
        "ZEROBUS_TABLE_NAME",
        "ZEROBUS_SECRET_SCOPE",
        "UC_STORAGE_BASE",
        "UC_EXTERNAL_LOCATION",
    }
    assert bundle - {"ADMIN_EMAILS"} == app_yaml - {"ADMIN_EMAILS"} - empty_only, (
        f"drift: only in bundle={bundle - app_yaml}, only in app.yaml={app_yaml - bundle}"
    )


def test_bundle_example_overrides_cover_required_variables() -> None:
    import json
    import re

    text = (ROOT / "databricks.yml").read_text(encoding="utf-8")
    block = text.split("variables:", 1)[1].split("\nsync:", 1)[0]
    required = {
        m.group(1)
        for m in re.finditer(r"^  (\w+):\n    description: .*\n(?!    default)", block, re.M)
    }
    example = json.loads(
        (ROOT / "deploy" / "variable-overrides.json.example").read_text(encoding="utf-8")
    )
    assert required - set(example) == set(), "example overrides missing required variables"
