"""Static checks on the DAB (databricks.yml + resources/app.yml) and its docs.

`databricks bundle validate` accepts an env entry that resolves to an empty
string, but `bundle run` then fails with "Must specify environment variable
source using either `value` or `valueFrom`". These tests catch that offline.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
VAR_REF = re.compile(r"\$\{var\.(\w+)\}")
DOC_TABLE = re.compile(
    r"<!-- required-variables:start -->(.*?)<!-- required-variables:end -->", re.S
)


def _bundle_variables() -> dict[str, dict[str, Any]]:
    data = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
    return data["variables"]


def _example_overrides() -> dict[str, str]:
    path = ROOT / "deploy" / "variable-overrides.json.example"
    return json.loads(path.read_text(encoding="utf-8"))


def _env_entries() -> list[dict[str, Any]]:
    data = yaml.safe_load((ROOT / "resources" / "app.yml").read_text(encoding="utf-8"))
    return data["resources"]["apps"]["iot_twin"]["config"]["env"]


def _resolve(raw: str, values: dict[str, str]) -> str:
    return VAR_REF.sub(lambda m: values[m.group(1)], raw)


def _effective_values() -> dict[str, str]:
    """Variable defaults overlaid with the example overrides (a realistic deploy)."""
    defaults = {
        name: str(spec["default"])
        for name, spec in _bundle_variables().items()
        if "default" in spec
    }
    return {**defaults, **_example_overrides()}


def test_every_env_entry_resolves_to_a_non_empty_source() -> None:
    values = _effective_values()
    problems: list[str] = []
    for entry in _env_entries():
        name = entry["name"]
        has_value = "value" in entry
        has_source = "value_from" in entry or "valueFrom" in entry
        if has_value == has_source:
            problems.append(f"{name}: needs exactly one of value / value_from")
        elif has_value and not _resolve(str(entry["value"]), values).strip():
            problems.append(f"{name}: resolves to an empty string")
    assert not problems, "; ".join(problems)


def test_env_references_only_declared_variables() -> None:
    declared = set(_bundle_variables())
    text = (ROOT / "resources" / "app.yml").read_text(encoding="utf-8")
    assert set(VAR_REF.findall(text)) <= declared


def test_empty_string_variable_defaults_are_not_declared() -> None:
    empty = [n for n, spec in _bundle_variables().items() if spec.get("default") == ""]
    assert not empty, f"variables defaulting to '' break `bundle run` when used as env: {empty}"


def test_example_overrides_only_name_declared_variables() -> None:
    assert set(_example_overrides()) <= set(_bundle_variables())


def test_deployment_doc_lists_exactly_the_required_variables() -> None:
    required = {name for name, spec in _bundle_variables().items() if "default" not in spec}
    doc = (ROOT / "docs" / "deployment.md").read_text(encoding="utf-8")
    match = DOC_TABLE.search(doc)
    assert match, "docs/deployment.md is missing the required-variables table markers"
    documented = set(re.findall(r"^\|\s*`(\w+)`", match.group(1), re.M))
    assert documented == required, (
        f"missing from docs: {required - documented}; stale in docs: {documented - required}"
    )


def _targets() -> dict[str, Any]:
    data = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
    return data["targets"]


ENDPOINT_REF = "${resources.model_serving_endpoints.spindle_risk.name}"


def test_dev_target_does_not_prefix_resource_names() -> None:
    """Dev mode decorates names ("[dev <user>] " / "dev_<user>_"), so nothing may
    hardcode the endpoint name: the app's serving resource and the training job's
    --endpoint-name both follow the bundle endpoint resource, whatever its name."""
    assert _targets()["dev"].get("mode") == "development"
    app = yaml.safe_load((ROOT / "resources" / "app.yml").read_text(encoding="utf-8"))
    serving = [
        r for r in app["resources"]["apps"]["iot_twin"]["resources"] if "serving_endpoint" in r
    ]
    assert [r["serving_endpoint"]["name"] for r in serving] == [ENDPOINT_REF]
    env = {e["name"]: e for e in app["resources"]["apps"]["iot_twin"]["config"]["env"]}
    assert env["RISK_ENDPOINT_NAME"].get("value_from") == serving[0]["name"]
    risk = yaml.safe_load((ROOT / "resources" / "risk_model.yml").read_text(encoding="utf-8"))
    params = risk["resources"]["jobs"]["risk_model_train"]["tasks"][0]["spark_python_task"][
        "parameters"
    ]
    assert params[params.index("--endpoint-name") + 1] == ENDPOINT_REF
    entity = risk["resources"]["model_serving_endpoints"]["spindle_risk"]["config"][
        "served_entities"
    ][0]
    # The dev-prefixed endpoint serves the same registered version as any other target.
    assert entity["entity_version"] == "${var.risk_model_version}"


def test_risk_resources_resolve_to_owned_pharma_twin_names() -> None:
    defaults = {name: str(spec.get("default", "")) for name, spec in _bundle_variables().items()}
    risk = yaml.safe_load((ROOT / "resources" / "risk_model.yml").read_text(encoding="utf-8"))
    job = _resolve(risk["resources"]["jobs"]["risk_model_train"]["name"], defaults)
    endpoint = _resolve(
        risk["resources"]["model_serving_endpoints"]["spindle_risk"]["name"], defaults
    )
    assert job == "pharma-twin-risk-model-train"
    assert endpoint == "pharma-twin-spindle-risk"
    for target in _targets().values():
        overrides = {k: str(v) for k, v in (target.get("variables") or {}).items()}
        for name in ("risk_job_name", "risk_endpoint_name"):
            assert overrides.get(name, defaults[name]).startswith("pharma-twin")
    app = yaml.safe_load((ROOT / "resources" / "app.yml").read_text(encoding="utf-8"))
    serving = [
        r["serving_endpoint"]["name"]
        for r in app["resources"]["apps"]["iot_twin"]["resources"]
        if "serving_endpoint" in r
    ]
    # The app queries the bundle's endpoint resource, i.e. pharma-twin-spindle-risk.
    assert serving == ["${resources.model_serving_endpoints.spindle_risk.name}"]
