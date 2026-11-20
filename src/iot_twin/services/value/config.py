"""Loads ``value_defaults.json``: cost assumptions and measurement definitions."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

DEFAULTS_PATH = Path(__file__).with_name("value_defaults.json")


@lru_cache(maxsize=1)
def defaults_payload() -> dict[str, Any]:
    with DEFAULTS_PATH.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise RuntimeError(f"{DEFAULTS_PATH.name} must hold a JSON object")
    return payload


@dataclass(frozen=True)
class Definitions:
    """How the views measure, as opposed to what things cost.

    ``caught_in_time_grace_s``: the containment deadline is never earlier than
    ``opened_at + grace``, so an onset at the very end of a lot cycle still
    leaves the operator time to act. ``max_open_excursion_s``: an excursion
    still open is counted up to at most this long after it opened.
    """

    caught_in_time_grace_s: float = 120.0
    max_open_excursion_s: float = 86_400.0


def _positive(raw: Any, name: str) -> float:
    if (
        isinstance(raw, bool)
        or not isinstance(raw, int | float)
        or not math.isfinite(raw)
        or raw < 0
    ):
        raise RuntimeError(f"{DEFAULTS_PATH.name}: definitions.{name} must be a number >= 0")
    return float(raw)


@lru_cache(maxsize=1)
def definitions() -> Definitions:
    raw = defaults_payload().get("definitions") or {}
    if not isinstance(raw, dict):
        raise RuntimeError(f"{DEFAULTS_PATH.name}: definitions must be an object")
    base = Definitions()
    return Definitions(
        caught_in_time_grace_s=_positive(
            raw.get("caught_in_time_grace_s", base.caught_in_time_grace_s), "caught_in_time_grace_s"
        ),
        max_open_excursion_s=_positive(
            raw.get("max_open_excursion_s", base.max_open_excursion_s), "max_open_excursion_s"
        ),
    )
