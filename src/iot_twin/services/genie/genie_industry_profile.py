"""Industry profile for per-pipeline Genie spaces, derived from the catalogs.

Every wording that used to be hard-coded for manufacturing (industry label,
health-index term, example sensors/units) is resolved here from the curated
industry template and the domain catalog, so one content generator serves every
industry the app offers.  Manufacturing keeps its historical wording (OEE).
"""

from __future__ import annotations

from dataclasses import dataclass

from iot_twin.domain.catalog import get_domain, to_record_industry
from iot_twin.services.industry.industry_templates import get_template, mode_config

_MAX_EXAMPLES = 3

# Manufacturing's historical wording is preserved verbatim.
_HEALTH_TERM_OVERRIDES: dict[str, tuple[str, str]] = {
    "manufacturing": ("OEE", "oee_pct"),
}
_TYPE_EXAMPLE_OVERRIDES: dict[str, str] = {
    "manufacturing": "temperature, vibration, pressure",
}
_DEFAULT_HEALTH = ("operating health index", "health_index_pct")
# Wording that is manufacturing-specific in the historical output.
_FAULT_SUBJECT_OVERRIDES: dict[str, str] = {"manufacturing": "machine/sensor"}
_OUTPUT_NOTE_OVERRIDES: dict[str, str] = {"manufacturing": "production counts"}
_DEFAULT_FAULT_SUBJECT = "equipment/sensor"
_DEFAULT_OUTPUT_NOTE = "recorded output counts"


class UnknownIndustry(ValueError):
    """The industry is not one the app offers (no curated template)."""


@dataclass(frozen=True)
class IndustryProfile:
    industry: str  # record industry id as stored in readings / intelligence tables
    name: str  # display name, e.g. "Pharmaceutical"
    health_term: str  # "OEE" or "operating health index"
    health_alias: str  # SQL alias for the health metric
    type_examples: str  # sensor_type column examples
    sensor_examples: tuple[str, ...]
    units: tuple[str, ...]
    fault_subject: str  # who reports an active fault
    output_note: str  # what the health index is NOT derived from
    derived: bool  # True when wording is derived (not the historical manufacturing text)

    @property
    def label(self) -> str:
        return self.name.lower()


def is_known_industry(industry: str | None) -> bool:
    return get_template((industry or "").strip().lower()) is not None


def _units(industry: str) -> tuple[str, ...]:
    seen: list[str] = []
    for binding in get_domain(industry).kpi_bindings:
        unit = (binding.unit or "").strip()
        if unit and unit not in seen:
            seen.append(unit)
    return tuple(seen)


def profile_for(industry: str | None) -> IndustryProfile:
    """Build the profile for ``industry``; raises :class:`UnknownIndustry`."""
    key = (industry or "").strip().lower()
    template = get_template(key)
    if template is None:
        raise UnknownIndustry(f"unknown industry {industry!r}")
    term, alias = _HEALTH_TERM_OVERRIDES.get(key, _DEFAULT_HEALTH)
    samples = tuple(template.sensors[:_MAX_EXAMPLES])
    return IndustryProfile(
        industry=to_record_industry(key),
        name=template.name,
        health_term=term,
        health_alias=alias,
        type_examples=_TYPE_EXAMPLE_OVERRIDES.get(key, ", ".join(samples)),
        sensor_examples=samples,
        units=_units(key),
        fault_subject=_FAULT_SUBJECT_OVERRIDES.get(key, _DEFAULT_FAULT_SUBJECT),
        output_note=_OUTPUT_NOTE_OVERRIDES.get(key, _DEFAULT_OUTPUT_NOTE),
        derived=key not in _HEALTH_TERM_OVERRIDES,
    )


def mode_has_metrics(mode: str) -> bool:
    """True when the mode writes the shared ``{industry}_metrics`` table."""
    return bool(mode_config(mode)["has_metrics"])
