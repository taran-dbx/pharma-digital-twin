"""Environment-based configuration constants.

Resource-name isolation
-----------------------
Every Databricks / Kafka / Lakebase resource this app creates or looks up by
name is built from ONE configurable prefix, ``RESOURCE_PREFIX`` (default
``pharma-twin``).  Hyphenated names (jobs, Lakebase projects, secret scopes,
clusters, workspace dirs, tags) use the prefix as-is; Unity Catalog and Postgres
identifiers use the underscore form (``pharma_twin``).

The prefix lets this app deploy side by side with the upstream app it was
forked from, which uses the ``iot`` namespace (``iot-<industry>-<mode>`` jobs,
``iot.<industry>.<mode>`` topics, ``iot_<industry>`` catalogs, ``iot-secrets``
...).  A prefix in that namespace is therefore rejected outright.

Read the prefix with :func:`get_resource_prefix` /
:func:`get_resource_prefix_ident` inside functions (they read the environment
on every call, so tests can monkeypatch ``RESOURCE_PREFIX``).  The name builders
themselves live in :mod:`iot_twin.core.resource_names`.
"""

from __future__ import annotations

import os
import re

# Medical-device manufacturing (internal id ``manufacturing``) is the lead story;
# pharmaceutical stays available as a secondary industry.
INDUSTRIES: str = os.getenv("INDUSTRIES", "manufacturing,pharmaceutical")
UPDATE_RATE_HZ: float = float(os.getenv("UPDATE_RATE_HZ", "2.0"))

RESOURCE_PREFIX_ENV = "RESOURCE_PREFIX"
DEFAULT_RESOURCE_PREFIX = "pharma-twin"

# The upstream app's namespace. A prefix inside it could resolve to a live
# upstream resource (e.g. ``iot`` -> ``iot-pharmaceutical-realtime``), so it is
# refused rather than silently accepted.
_LEGACY_NAMESPACE_RE = re.compile(r"^iot(?:$|[-_.])")

# Lower-case letters, digits and single hyphens; starts with a letter, ends with
# a letter/digit. Short enough that derived names (app name <= 30 chars, UC
# identifiers) stay within platform limits.
_PREFIX_RE = re.compile(r"^[a-z](?:[a-z0-9]|-(?!-)){0,18}[a-z0-9]$")

# Platform-required: Lakehouse Sync only supports this Lakebase database name,
# so it is deliberately NOT derived from the prefix.
LAKEBASE_SYNC_DATABASE = "databricks_postgres"

# Shared UC service credential (MSK IAM) that is deliberately reused rather
# than duplicated. Overridable through ``UC_SERVICE_CREDENTIAL``.
SHARED_MSK_IAM_UC_NAME = "iot-rtm-sc"


def validate_resource_prefix(prefix: str) -> str:
    """Return ``prefix`` normalised, or raise ``ValueError`` when it is unusable."""
    value = (prefix or "").strip().lower()
    if not _PREFIX_RE.match(value):
        raise ValueError(
            f"{RESOURCE_PREFIX_ENV}={prefix!r} is invalid: use 2-20 lower-case letters, "
            "digits and single hyphens, starting with a letter"
        )
    if _LEGACY_NAMESPACE_RE.match(value):
        raise ValueError(
            f"{RESOURCE_PREFIX_ENV}={prefix!r} is inside the upstream 'iot' namespace "
            "and could collide with the live upstream deployment"
        )
    return value


def get_resource_prefix() -> str:
    """Return the hyphenated resource prefix (``RESOURCE_PREFIX``), read on every call."""
    raw = os.getenv(RESOURCE_PREFIX_ENV, "").strip()
    return validate_resource_prefix(raw or DEFAULT_RESOURCE_PREFIX)


def get_resource_prefix_ident() -> str:
    """Return the prefix as a UC / Postgres identifier fragment (``pharma_twin``)."""
    return get_resource_prefix().replace("-", "_")


def get_uc_service_credential() -> str:
    """Return the UC service credential name, defaulting to the shared credential."""
    return os.getenv("UC_SERVICE_CREDENTIAL", "").strip() or SHARED_MSK_IAM_UC_NAME
