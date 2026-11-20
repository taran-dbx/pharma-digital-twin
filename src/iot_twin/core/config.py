"""Environment-based configuration constants."""

from __future__ import annotations

import os

# Medical-device manufacturing (internal id ``manufacturing``) is the lead story;
# pharmaceutical stays available as a secondary industry.
INDUSTRIES: str = os.getenv("INDUSTRIES", "manufacturing,pharmaceutical")
UPDATE_RATE_HZ: float = float(os.getenv("UPDATE_RATE_HZ", "2.0"))
