"""Medical-device quality grounding for the manufacturing Genie space [qa].

The internal industry id stays ``manufacturing``; its Genie space speaks the
Trinity Medical orthopaedic implant story (validated process parameter, lots,
NCR / QA hold, Part 11 sign-off) while every other industry keeps its wording.
"""

from __future__ import annotations

import json

import pytest

from iot_twin.services import genie_service as gs
from iot_twin.services.genie import genie_space_content as content
from iot_twin.services.genie import genie_space_service as gss
from iot_twin.services.genie.genie_industry_profile import profile_for
from iot_twin.services.industry.industry_templates import CURATED_INDUSTRIES, MODES

OTHERS = [i for i in CURATED_INDUSTRIES if i != "manufacturing"]


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UC_CATALOG", "cat")
    monkeypatch.setenv("UC_SCHEMA", "sch")


def _space(industry: str, mode: str) -> dict:
    return json.loads(gss.build_serialized_space(industry, mode))


def _instructions(industry: str, mode: str) -> str:
    return " ".join(_space(industry, mode)["instructions"]["text_instructions"][0]["content"])


class TestProfile:
    def test_manufacturing_health_term_is_first_pass_yield_not_oee(self) -> None:
        profile = profile_for("manufacturing")
        assert profile.medtech is True
        assert "first-pass yield" in profile.health_term
        assert "right-first-time" in profile.health_term
        assert "oee" not in profile.health_term.lower()
        assert profile.health_alias == "fpy_proxy_pct"

    @pytest.mark.parametrize("industry", OTHERS)
    def test_other_industries_unchanged(self, industry: str) -> None:
        profile = profile_for(industry)
        assert profile.medtech is False
        assert profile.health_term == "operating health index"
        assert profile.health_alias == "health_index_pct"


class TestStarters:
    @pytest.mark.parametrize("mode", MODES)
    def test_medtech_starters_present(self, mode: str) -> None:
        questions = gss.starter_questions("manufacturing", mode)
        assert "Which lots were machined while Spindle 3 was outside its validated limit?" in (
            questions
        )
        blob = " ".join(questions).lower()
        for topic in ("ncr", "quarantine", "detect", "first-pass yield"):
            assert topic in blob, (mode, topic)

    @pytest.mark.parametrize("mode", MODES)
    @pytest.mark.parametrize("industry", OTHERS)
    def test_other_industries_get_no_medtech_starters(self, industry: str, mode: str) -> None:
        questions = gss.starter_questions(industry, mode)
        assert not set(questions) & set(content.MEDTECH_STARTERS)
        blob = " ".join(questions).lower()
        assert "spindle" not in blob and "ncr" not in blob and "lot" not in blob


class TestInstructionsAndSql:
    @pytest.mark.parametrize("mode", MODES)
    def test_instructions_ground_validated_process_and_part11(self, mode: str) -> None:
        body = _instructions("manufacturing", mode)
        for phrase in (
            "Trinity Medical",
            "21 CFR 820 (QMSR)",
            "ISO 13485",
            "VALIDATED process-parameter limit",
            "OQ-TM-CNC1-014",
            "PQ-TM-HS12-022",
            "VPP-SPINDLE3-TEMP",
            "dhr_ref",
            "udi_di",
            "decision log",
            "signature meaning",
            "never release or quarantine lots",
        ):
            assert phrase in body, (mode, phrase)
        assert "OEE" not in body

    @pytest.mark.parametrize(
        ("mode", "accessor"),
        [
            ("realtime", "t.metadata:lot_id::string"),
            ("microbatch", "t.metadata['lot_id']"),
            ("zerobus", "t.metadata['lot_id']"),
        ],
    )
    def test_excursion_lot_sql_reads_metadata_for_the_mode(self, mode: str, accessor: str) -> None:
        examples = _space("manufacturing", mode)["instructions"]["example_question_sqls"]
        by_question = {e["question"][0]: e["sql"][0] for e in examples}
        sql = by_question[content.MEDTECH_STARTERS[0]]
        assert accessor in sql
        assert "intelligence_alert_rules" in sql
        assert "rule_id = 'mfg-alert-spindle-temp'" in sql
        assert "t.value > r.threshold" in sql
        assert ("_pg_change_type" in sql) is (mode == "realtime")

    @pytest.mark.parametrize("mode", MODES)
    def test_metadata_column_described_for_medtech_only(self, mode: str) -> None:
        def configs(industry: str) -> dict:
            tables = _space(industry, mode)["data_sources"]["tables"]
            tel = next(t for t in tables if t["identifier"] == gss.telemetry_table(industry, mode))
            return {c["column_name"]: c for c in tel["column_configs"]}

        assert "lot_id" in " ".join(configs("manufacturing")["metadata"]["description"])
        for industry in OTHERS:
            assert "metadata" not in configs(industry)

    @pytest.mark.parametrize("mode", MODES)
    @pytest.mark.parametrize("industry", OTHERS)
    def test_other_industries_have_no_medtech_text(self, industry: str, mode: str) -> None:
        blob = json.dumps(_space(industry, mode)).lower()
        for phrase in ("trinity", "spindle", "iso 13485", "validated", "lot_id", "first-pass"):
            assert phrase not in blob, (industry, mode, phrase)


class TestTrustedPrompt:
    def _prompt(self, industry: str) -> str:
        pack = gs.build_context_pack(
            industry=industry,
            mode="realtime",
            allowed_tables=list(gss.expected_tables(industry, "realtime")),
        )
        return gs.render_trusted_prompt("Which lots are at risk?", pack)

    def test_manufacturing_prompt_is_medical_device_quality_aware(self) -> None:
        prompt = self._prompt("manufacturing")
        for phrase in (
            "medical-device quality investigation copilot",
            "21 CFR 820 (QMSR)",
            "ISO 13485",
            "validated process parameter",
            "device history record (DHR)",
            "NCR",
            "CAPA",
            "never release or quarantine lots",
            "Part 11",
        ):
            assert phrase in prompt, phrase
        assert gs.extract_user_question(prompt) == "Which lots are at risk?"

    @pytest.mark.parametrize("industry", OTHERS)
    def test_other_industries_keep_the_generic_copilot(self, industry: str) -> None:
        prompt = self._prompt(industry)
        assert "industrial investigation copilot" in prompt
        assert "medical-device" not in prompt
