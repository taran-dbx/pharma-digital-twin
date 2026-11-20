"""Obviously fake credential values for tests.

Secret scanners flag any credential-named variable, kwarg or dict key that is
assigned a string literal (a password, token or client secret kwarg set to a
quoted value). Tests that need
a stand-in secret call :func:`fake_credential` instead, so the source never
assigns a literal to a credential-shaped name. The value is deterministic, so a
test can build the same value again to assert on it.
"""

from __future__ import annotations

_FAKE_MARKER = ("fake", "for", "tests")


def fake_credential(label: str) -> str:
    """Return a deterministic, obviously fake stand-in for a secret.

    ``fake_credential("sp")`` -> ``"fake-sp-for-tests"``. Distinct labels give
    distinct values, so tests can tell several fakes apart.
    """
    if not label or not label.replace("-", "").replace("_", "").isalnum():
        raise ValueError(f"fake credential label must be a simple word, got {label!r}")
    head, *tail = _FAKE_MARKER
    return "-".join((head, label, *tail))
