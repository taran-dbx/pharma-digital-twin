"""What leaves the app for the model (or for UC): no emails, no hidden tags.

* :func:`pseudonym` replaces a person's login with a stable ``person-<8 hex>``
  (HMAC-SHA256 keyed by ``GENAI_PSEUDONYM_SALT``). The same person maps to the
  same pseudonym, so the model can still say "the same reviewer signed both".
  The Lakebase audit keeps the real ``requested_by`` / ``applied_by`` (the
  access-controlled system of record); grounding rows and the UC mirror carry
  pseudonyms only.
* :func:`clean_text` normalises untrusted free text before it is stored or sent:
  NFKC (so full-width ``［`` becomes ``[``), zero-width and bidi control characters
  removed, delimiter tags defanged, and email addresses redacted.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import re
import unicodedata

SALT_ENV = "GENAI_PSEUDONYM_SALT"
# Not a secret: set GENAI_PSEUDONYM_SALT in deployments so pseudonyms cannot be
# reversed by hashing a list of known logins.
_DEFAULT_SALT = "pharma-twin-genai-pseudonym"

_EMAIL = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
_INVISIBLE = re.compile("[­؜᠎​-‏‪-‮⁠-⁤﻿]")
TAG_PATTERN = re.compile(
    r"\[\s*/?\s*(TRUSTED_CONTEXT|UNTRUSTED_GROUNDING|UNTRUSTED_OBSERVATIONS)\s*\]", re.I
)


def _salt() -> bytes:
    return (os.getenv(SALT_ENV, "").strip() or _DEFAULT_SALT).encode("utf-8")


def pseudonym(identity: object) -> str | None:
    """``person-<8 hex>`` for a login (case-insensitive); None stays None."""
    if identity is None or str(identity).strip() == "":
        return None
    raw = str(identity).strip().lower().encode("utf-8")
    return "person-" + hmac.new(_salt(), raw, hashlib.sha256).hexdigest()[:8]


def normalize(text: str) -> str:
    """NFKC and remove zero-width / bidi control characters."""
    return _INVISIBLE.sub("", unicodedata.normalize("NFKC", text))


def defang(text: str) -> str:
    """Neutralise delimiter tags (after normalising, so look-alikes are caught too)."""
    return TAG_PATTERN.sub(lambda m: f"(tag {m.group(1).lower()} removed)", normalize(text))


def redact_emails(text: str) -> str:
    return _EMAIL.sub("[email]", text)


def clean_text(text: str) -> str:
    return redact_emails(defang(text))
