"""Tests for scripts/check_credential_literals.py and tests/_fakes.py."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

from tests._fakes import fake_credential

ROOT = Path(__file__).resolve().parents[1]

# Offending snippets are assembled at runtime so this file itself stays clean.
_EQ, _COLON, _Q = "=", ": ", '"'


def _assign(name: str, value: str, sep: str = _EQ, key_quote: str = "") -> str:
    return f"{key_quote}{name}{key_quote}{sep}{_Q}{value}{_Q}"


@pytest.fixture(scope="module")
def checker():
    spec = importlib.util.spec_from_file_location(
        "check_credential_literals", ROOT / "scripts" / "check_credential_literals.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_credential_literals"] = module
    spec.loader.exec_module(module)
    return module


def _names(checker, text: str, path: str = "<text>") -> list[str]:
    return [hit.name for hit in checker.scan_text(text, path)]


@pytest.mark.parametrize(
    "line, name",
    [
        ("connect(" + _assign("password", "hunter2") + ")", "password"),
        ("x = Mock(" + _assign("token", "abc") + ")", "token"),
        (_assign("client_secret", "s", _COLON, _Q), "client_secret"),
        (_assign("ZEROBUS_SECRET_KEY", "zerobus-client-secret"), "ZEROBUS_SECRET_KEY"),
        (_assign("apiKey", "k-1", _COLON), "apiKey"),
        (_assign("private_key", "pem", _COLON, "'"), "private_key"),
        (_assign("serviceCredential", "cred-name", _COLON), "serviceCredential"),
        (_assign("db_pwd", "pw"), "db_pwd"),
    ],
)
def test_flags_literal_credential_assignments(checker, line, name):
    assert _names(checker, line) == [name]


@pytest.mark.parametrize(
    "line",
    [
        "connect(password=fake_credential('db'))",
        "token = resp.json()['access_token']",
        _assign("SERVICE_CREDENTIAL", "${cred}"),
        _assign("client_secret", "{secret!r}"),
        _assign("credentials", "include", _COLON),
        _assign("credentials", "same-origin"),
        _assign("password", ""),
        "SCOPE_NAME = " + _Q + "iot-secrets" + _Q,
    ],
)
def test_ignores_non_literals_and_allowlisted_shapes(checker, line):
    assert _names(checker, line) == []


def test_fetch_allowlist_is_limited_to_request_modes(checker):
    assert _names(checker, _assign("credentials", "hunter2", _COLON)) == ["credentials"]


def test_deploy_template_allowlist_is_path_scoped(checker):
    line = _assign("UC_SERVICE_CREDENTIAL", "iot-rtm-sc", _COLON, _Q)
    assert _names(checker, line, "deploy/workspaces/x.profile.example") == []
    assert _names(checker, line, "deploy/workspaces/x.profile") == ["UC_SERVICE_CREDENTIAL"]
    assert _names(checker, line, "docs/setup.md") == ["UC_SERVICE_CREDENTIAL"]
    other = _assign("client_secret", "s", _COLON, _Q)
    assert _names(checker, other, "deploy/variable-overrides.json.example") == ["client_secret"]


def test_scans_notebook_sources_and_saved_outputs(checker, tmp_path):
    notebook = {
        "cells": [
            {"cell_type": "code", "source": ["x = 1\n", _assign("token", "t") + "\n"]},
            {
                "cell_type": "code",
                "source": "print(1)",
                "outputs": [
                    {"output_type": "stream", "text": [_assign("password", "p")]},
                    {"output_type": "execute_result", "data": {"text/plain": "ok"}},
                ],
            },
        ]
    }
    path = tmp_path / "nb.ipynb"
    path.write_text(json.dumps(notebook), encoding="utf-8")
    hits = checker.scan_file(path, "nb.ipynb")
    assert [(h.name, h.path) for h in hits] == [
        ("token", "nb.ipynb [cell 0 source]"),
        ("password", "nb.ipynb [cell 1 output]"),
    ]


def test_main_reports_hits_without_values(checker, tmp_path, capsys):
    bad = tmp_path / "bad.py"
    bad.write_text(_assign("password", "hunter2") + "\n", encoding="utf-8")
    assert checker.main([str(bad)]) == 1
    out = capsys.readouterr().out
    assert "password" in out and "hunter2" not in out


def test_tracked_tree_has_no_literal_credential_assignments(checker):
    hits = checker.run(checker.tracked_files())
    assert hits == [], "\n".join(str(hit) for hit in hits)


def test_fake_credential_is_deterministic_and_obviously_fake():
    assert fake_credential("sp") == fake_credential("sp") == "fake-sp-for-tests"
    assert fake_credential("sp") != fake_credential("db")
    with pytest.raises(ValueError):
        fake_credential("")
    with pytest.raises(ValueError):
        fake_credential("has space")
