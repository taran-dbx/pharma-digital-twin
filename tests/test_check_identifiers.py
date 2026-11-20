"""Tests for scripts/check_identifiers.py and scripts/_profile.py."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def checker():
    return _load("check_identifiers")


@pytest.fixture(scope="module")
def profile_helper():
    return _load("_profile")


def _rules(checker, text: str) -> list[str]:
    return [hit.rule for hit in checker.scan_text(text)]


# Offending values are assembled at runtime so this file itself stays clean.
_LAKEBASE_HOST = "ep-" + "quiet-river-" + "a1b2c3d4" + ".database.us-east-1.cloud.databricks.com"
_FAKE_ACCOUNT = "1111" + "2222" + "3333"
_FAKE_WORKSPACE_ID = "9876" + "5432" + "1098" + "7654"
_MSK_REAL = "b-1." + "demo." + "q7x2zk" + ".c3.kafka.us-east-1.amazonaws.com"
_NPM_PROXY = "https://npm-" + "proxy.corp.example.net/pkg/-/pkg-1.0.0.tgz"
_ARN = "arn:aws:kafka:us-east-1:" + _FAKE_ACCOUNT + ":cluster/x"
_OWNER_EMAIL = "tar" + "anfake@" + "example.com"


@pytest.mark.parametrize(
    ("text", "rule"),
    [
        (_LAKEBASE_HOST, "lakebase-endpoint-host"),
        ("https://my-app-1234567890123456.aws." + "databricksapps.com", "databricksapps-host"),
        ("host = real-ws-1" + ".cloud.databricks.com", "databricks-cloud-host"),
        (
            "zerobus: " + "1357924680135792" + ".zerobus.eu-central-1.cloud.databricks.com",
            "databricks-cloud-host",
        ),
        (_ARN, "aws-account-id-in-arn"),
        (_OWNER_EMAIL, "owner-identity"),
        ("profile fe-" + "vm-fakeworkspace", "real-workspace-name"),
        ("aws_account_id = " + _FAKE_ACCOUNT, "aws-account-id-in-context"),
        ("owner 111 " + _FAKE_ACCOUNT + " end", "aws-account-id-bare"),
        ("account=" + _FAKE_ACCOUNT, "aws-account-id-bare"),
        ('"resolved": "' + _NPM_PROXY + '"', "private-npm-registry"),
        ("registry=https://NPM-" + "PROXY.dev.example.net/", "private-npm-registry"),
        (_MSK_REAL + ":9098", "msk-broker-host"),
        (
            "b-2-public." + "demo." + "q7x2zk" + ".c12.kafka.eu-west-1.amazonaws.com",
            "msk-broker-host",
        ),
        ("zerobus_id = " + _FAKE_WORKSPACE_ID, "workspace-id"),
        ("workspace_id: " + _FAKE_WORKSPACE_ID, "workspace-id"),
        ("https://x.example.net/?o=" + _FAKE_WORKSPACE_ID, "workspace-id"),
        ("https://" + _FAKE_WORKSPACE_ID + ".host.example.net/", "workspace-id"),
        (
            "https://" + _FAKE_WORKSPACE_ID + ".zerobus.us-east-1.cloud.databricks.com",
            "databricks-cloud-host",
        ),
    ],
)
def test_flags_real_identifiers(checker, text: str, rule: str) -> None:
    assert rule in _rules(checker, text)


@pytest.mark.parametrize(
    "text",
    [
        "example-lakebase.database.us-east-1.cloud.databricks.com",
        "https://<workspace-host>",
        "https://<app>.databricksapps.com and .databricksapps.com",
        "https://workspace.cloud.databricks.com/api",
        "123.zerobus.us-east-1.cloud.databricks.com",
        "1234567890123456.zerobus.us-east-1.cloud.databricks.com",
        "arn:aws:iam::123456789012:role/example",
        "arn:aws:iam::000000000000:role/example",
        "aws_account_id = 123456789012",
        "owner 123456789012 end",
        "uuid 11111111-1111-4111-8111-111111111111 tail",
        "ts 1700000000000 ms and ratio 0.123456789012",
        "registry https://registry.npmjs.org/pkg/-/pkg-1.0.0.tgz",
        "b-1.example.abc123.c9.kafka.us-east-1.amazonaws.com:9098",
        "b-1-public.example.xxxxxx.c1.kafka.us-east-1.amazonaws.com:9198",
        "b-1.x.y.kafka.eu-west-2.amazonaws.com:9198",
        "zerobus 1357924680135792 and workspace_id = 1234567890123456",
        "111111111111111.zerobus.us-east-1.cloud.databricks.com",
        "WORKSPACE_ID = 1234567890123456",
        "step-by-step-runbook.png and validation-step-failure-reason.png",
        "admin@example.com",
    ],
)
def test_allows_placeholders(checker, text: str) -> None:
    assert _rules(checker, text) == []


def test_overlapping_rules_report_one_hit_per_value(checker) -> None:
    assert _rules(checker, _ARN) == ["aws-account-id-in-arn"]


def test_npm_proxy_is_flagged_in_lockfile_paths(checker, tmp_path: Path) -> None:
    lock = tmp_path / "package-lock.json"
    lock.write_text('{"resolved": "' + _NPM_PROXY + '"}\n', encoding="utf-8")
    assert [h.rule for h in checker.scan_file(lock, "package-lock.json")] == [
        "private-npm-registry"
    ]


def test_inline_allow_marker_suppresses_a_line(checker) -> None:
    assert _rules(checker, f"{_ARN}  # check-identifiers: allow") == []


def test_repo_tree_is_clean(checker) -> None:
    hits = checker.run(checker.tracked_files())
    assert hits == [], "\n".join(f"{h.path}:{h.line} [{h.rule}] {h.text}" for h in hits)


def test_cli_exit_codes(tmp_path: Path) -> None:
    bad = tmp_path / "bad.txt"
    bad.write_text(_ARN + "\n", encoding="utf-8")
    good = tmp_path / "good.txt"
    good.write_text("arn:aws:iam::123456789012:role/x\n", encoding="utf-8")
    script = str(ROOT / "scripts" / "check_identifiers.py")

    failing = subprocess.run(
        [sys.executable, script, str(bad)], capture_output=True, text=True, check=False
    )
    passing = subprocess.run(
        [sys.executable, script, str(good)], capture_output=True, text=True, check=False
    )
    assert failing.returncode == 1
    assert "aws-account-id-in-arn" in failing.stdout
    assert passing.returncode == 0


def test_resolve_profile_prefers_flag_and_strips_it(profile_helper) -> None:
    profile, rest = profile_helper.resolve_profile(
        ["stage", "--profile", "cli-a", "--x"], {"DATABRICKS_CONFIG_PROFILE": "env-b"}
    )
    assert profile == "cli-a"
    assert rest == ["stage", "--x"]


def test_resolve_profile_equals_form_and_env_fallback(profile_helper) -> None:
    assert profile_helper.resolve_profile(["--profile=cli-a"], {})[0] == "cli-a"
    assert profile_helper.resolve_profile([], {"DATABRICKS_CONFIG_PROFILE": "env-b"})[0] == "env-b"
    assert profile_helper.resolve_profile([], {})[0] is None


def test_require_profile_exits_with_clear_error(
    profile_helper, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)
    monkeypatch.setattr(sys, "argv", ["script"])
    with pytest.raises(SystemExit) as exc:
        profile_helper.require_profile()
    assert exc.value.code == 2
    assert "--profile" in capsys.readouterr().err


def test_require_profile_exports_env_and_strips_argv(
    profile_helper, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "")  # registers restore on teardown
    monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE")
    monkeypatch.setattr(sys, "argv", ["script", "run", "--profile", "my-cli"])
    assert profile_helper.require_profile() == "my-cli"
    assert sys.argv == ["script", "run"]
    import os

    assert os.environ["DATABRICKS_CONFIG_PROFILE"] == "my-cli"
