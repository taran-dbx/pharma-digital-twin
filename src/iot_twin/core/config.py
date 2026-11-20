"""Environment-based configuration constants."""

from __future__ import annotations

import os

INDUSTRIES: str = os.getenv("INDUSTRIES", "pharmaceutical")
UPDATE_RATE_HZ: float = float(os.getenv("UPDATE_RATE_HZ", "2.0"))
