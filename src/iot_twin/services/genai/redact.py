"""What leaves the app for the model (or for UC): no emails, no hidden tags.

* :func:`pseudonym` replaces a person's login with a stable
  ``person-<16 hex>`` (HMAC-SHA256). The same person maps to the same
  pseudonym, so the model can still say "the same reviewer signed both". The
  Lakebase audit keeps the real ``requested_by`` / ``applied_by`` (the
  access-controlled system of record); grounding rows and the UC mirror carry
  pseudonyms only.

  The HMAC key comes from, in order:

  1. ``GENAI_PSEUDONYM_SALT`` — injected from the ``genai-pseudonym-salt``
     secret app resource (scope ``pharma-twin-secrets``); source ``secret``;
  2. a random 32-byte key generated once and persisted in the Lakebase
     ``genai_settings`` table (:func:`init_key` at boot); source ``lakebase``.
     Only local and non-bundle runs reach this: the bundle always declares the
     secret resource, and its deploy fails without the secret;
  3. nothing: pseudonyms are not computed and every person becomes
     ``person``; source ``redacted``. There is never a built-in constant key.

* :func:`clean_text` normalises untrusted free text before it is stored or sent:
  NFKC (so full-width ``［`` becomes ``[``), zero-width and bidi control characters
  removed, delimiter tags defanged, and email addresses redacted.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import re
import secrets
import unicodedata
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

SALT_ENV = "GENAI_PSEUDONYM_SALT"
MIN_SECRET_CHARS = 16
PSEUDONYM_HEX = 16
REDACTED_PERSON = "person"
KEY_SETTING = "pseudonym_key"

_EMAIL = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
_INVISIBLE = re.compile("[­؜᠎​-‏‪-‮⁠-⁤﻿]")
TAG_PATTERN = re.compile(
    r"\[\s*/?\s*(TRUSTED_CONTEXT|UNTRUSTED_GROUNDING|UNTRUSTED_OBSERVATIONS)\s*\]", re.I
)

_LOAD_SQL = "SELECT value FROM genai_settings WHERE name = $1"
_CREATE_SQL = (
    "INSERT INTO genai_settings (name, value) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING"
)


@dataclass(frozen=True)
class _KeyState:
    key: bytes | None
    source: str  # secret | lakebase | redacted | unset
    detail: str = ""


_state = _KeyState(None, "unset")


def _secret_key() -> bytes | None:
    raw = os.getenv(SALT_ENV, "").strip()
    if not raw:
        return None
    if len(raw) < MIN_SECRET_CHARS:
        logger.warning("%s is shorter than %d characters; ignoring it", SALT_ENV, MIN_SECRET_CHARS)
        return None
    return raw.encode("utf-8")


def set_key(key: bytes | None, source: str, detail: str = "") -> None:
    """Install the pseudonym key (boot or tests)."""
    global _state
    _state = _KeyState(key, source if key else "redacted", detail)


def reset_key() -> None:
    """Back to the unresolved state (tests)."""
    global _state
    _state = _KeyState(None, "unset")


async def init_key(db: Any | None) -> dict[str, str]:
    """Resolve the key at boot: secret, else a persisted Lakebase key, else redact."""
    secret = _secret_key()
    if secret is not None:
        set_key(secret, "secret")
        return key_status()
    if db is None:
        set_key(None, "redacted", "no GENAI_PSEUDONYM_SALT secret and no Lakebase")
        logger.warning("Pseudonym key unavailable: people are redacted to %r", REDACTED_PERSON)
        return key_status()
    try:
        await db.execute(_CREATE_SQL, KEY_SETTING, secrets.token_hex(32))
        stored = await db.fetchval(_LOAD_SQL, KEY_SETTING)
        set_key(bytes.fromhex(str(stored)), "lakebase")
        logger.warning(
            "GENAI_PSEUDONYM_SALT secret not configured; using the key persisted in Lakebase "
            "genai_settings (bind the genai-pseudonym-salt secret resource to rotate it)"
        )
    except Exception as exc:  # noqa: BLE001 - boot must continue; fail closed to redaction
        set_key(None, "redacted", f"Lakebase key unavailable: {exc}")
        logger.warning("Pseudonym key unavailable (%s): people are redacted", exc)
    return key_status()


def key_status() -> dict[str, str]:
    state = _state
    if state.source == "unset":
        secret = _secret_key()
        state = (
            _KeyState(secret, "secret")
            if secret
            else _KeyState(None, "redacted", "not initialised")
        )
    return {
        "source": state.source,
        "pseudonyms": "keyed" if state.key else "redacted",
        "detail": state.detail,
    }


def _key() -> bytes | None:
    if _state.source == "unset":
        return _secret_key()
    return _state.key


def pseudonym(identity: object) -> str | None:
    """``person-<16 hex>`` for a login (case-insensitive); ``person`` without a key."""
    if identity is None or str(identity).strip() == "":
        return None
    key = _key()
    if key is None:
        return REDACTED_PERSON
    raw = str(identity).strip().lower().encode("utf-8")
    return "person-" + hmac.new(key, raw, hashlib.sha256).hexdigest()[:PSEUDONYM_HEX]


def normalize(text: str) -> str:
    """NFKC and remove zero-width / bidi control characters."""
    return _INVISIBLE.sub("", unicodedata.normalize("NFKC", text))


def defang_tags(text: str) -> str:
    """Neutralise delimiter tags as they are (no normalisation)."""
    return TAG_PATTERN.sub(lambda m: f"(tag {m.group(1).lower()} removed)", text)


def defang(text: str) -> str:
    """Normalise, then neutralise delimiter tags (look-alikes are caught too)."""
    return defang_tags(normalize(text))


def redact_emails(text: str) -> str:
    return _EMAIL.sub("[email]", text)


def clean_text(text: str) -> str:
    return redact_emails(defang(text))
