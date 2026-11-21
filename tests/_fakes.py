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


def owned_job_settings() -> object:
    """A ``jobs.get()`` result that passes ``assert_owned_job`` (prefix name + app tag)."""
    from types import SimpleNamespace

    from iot_twin.core import resource_names

    return SimpleNamespace(
        settings=SimpleNamespace(
            name=resource_names.pipeline_name("test", "realtime"),
            tags=resource_names.resource_tags("pipeline"),
        )
    )


def owned_cluster_info() -> object:
    """A ``clusters.get()`` result that passes ``assert_owned_cluster``."""
    from types import SimpleNamespace

    from iot_twin.core import resource_names

    return SimpleNamespace(
        cluster_name=resource_names.cluster_name("test"),
        custom_tags={"app": resource_names.app_name()},
    )


def own_jobs_and_clusters(w):
    """Make a mock WorkspaceClient's jobs / clusters look owned by this app; returns ``w``."""
    w.jobs.get.return_value = owned_job_settings()
    w.clusters.get.return_value = owned_cluster_info()
    return w
