"""Per-sensor cadence helpers for notebook templates."""

from __future__ import annotations

from iot_twin.domain.catalog import get_domain, sensor_by_name


def cadence_due(
    sensor_name: str,
    now: float,
    last_emitted_at: float | None,
    hz: float,
) -> bool:
    """Return True when ``sensor_name`` should emit at ``now``."""
    if hz <= 0:
        return True
    if last_emitted_at is None:
        return True
    return (now - last_emitted_at) >= (1.0 / hz) - 1e-9


class CadenceClock:
    """Tracks last-emit times and reads hz from the domain catalog."""

    def __init__(self, industry: str) -> None:
        get_domain(industry)
        self.industry = industry
        self._last: dict[str, float] = {}

    def interval_s(self, sensor_name: str) -> float:
        sensor = sensor_by_name(self.industry, sensor_name)
        hz = sensor.cadence_hz if sensor is not None else 1.0
        if hz <= 0:
            return 0.0
        return 1.0 / hz

    def due(self, sensor_name: str, now: float) -> bool:
        sensor = sensor_by_name(self.industry, sensor_name)
        hz = sensor.cadence_hz if sensor is not None else 1.0
        last = self._last.get(sensor_name)
        if not cadence_due(sensor_name, now, last, hz):
            return False
        self._last[sensor_name] = now
        return True
