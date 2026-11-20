"""Per-industry Genie space content: instructions, example SQL, snippets, starters.

Pure functions with no I/O — :mod:`services.genie_space_service` assigns the
ids and assembles the ``serialized_space`` payload.  Column references in SQL
snippets are qualified with the telemetry table's short name, as Genie requires.
"""

from __future__ import annotations

from typing import Any

from iot_twin.domain.lot_context import PROJECTED_RFT_DEFINITION
from iot_twin.services.genie.genie_industry_profile import IndustryProfile
from iot_twin.services.uc_ident import sql_fqn

MODE_LABELS: dict[str, str] = {
    "realtime": "real-time (sub-second RTM)",
    "microbatch": "micro-batch",
    "zerobus": "Zerobus direct-ingest",
}


def _common_starters(profile: IndustryProfile) -> tuple[str, ...]:
    return (
        f"What is the approximate {profile.health_term} per sensor over the last 24 hours?",
        "How many minutes of downtime did each sensor have today?",
        "Which sensors had the most anomalies (values outside min/max) in the last hour?",
        "Which fault codes or sensors had active faults most often this week?",
        "Which assets with recent faults are due for maintenance?",
    )


# Medical-device quality questions (Trinity Medical orthopaedic implant cell).
# Every one is answerable from telemetry lot metadata + intelligence_* tables;
# NCR numbers and dispositions live in the app's decision log (see instructions).
MEDTECH_STARTERS: tuple[str, ...] = (
    "Which lots were machined while Spindle 3 was outside its validated limit?",
    "Which part serials should be quarantined for the latest Spindle 3 excursion?",
    "How long did it take to detect each Spindle 3 excursion this week?",
    "What is the projected right-first-time of each lot touched by a Spindle 3 excursion today?",
    "Which Spindle 3 validated-limit excursions this shift need an NCR, and what is each alert's status?",
)

SPINDLE_TEMP_SENSOR = "cnc_1_spindle_temp"
SPINDLE_TEMP_RULE = "mfg-alert-spindle-temp"
LOT_KEYS: tuple[str, ...] = (
    "lot_id",
    "part_serial",
    "work_order",
    "part_number",
    "udi_di",
    "dhr_ref",
)


def _industry_starters(profile: IndustryProfile) -> tuple[str, ...]:
    """Questions about this industry's own sensors (derived industries only)."""
    if profile.medtech:
        return MEDTECH_STARTERS
    if not profile.derived:
        return ()
    return tuple(
        f"What is the average {sensor} reading per hour over the last day?"
        for sensor in profile.sensor_examples[:2]
    )


_MODE_STARTERS: dict[str, tuple[str, ...]] = {
    "realtime": (
        "Which sensors are faulting right now (last 5 minutes)?",
        "What is the end-to-end p99 latency of the real-time stream over the last hour?",
    ),
    "microbatch": (
        "How has the anomaly rate trended per hour over the last day?",
        "What is the average micro-batch trigger duration and throughput over the last hour?",
    ),
    "zerobus": (
        "How many readings per minute were ingested over the last hour?",
        "Which sensors stopped sending data in the last 15 minutes?",
    ),
}


def _short(fqn: str) -> str:
    return fqn.rsplit(".", 1)[-1]


def _quoted(fqn: str) -> str:
    return sql_fqn(*fqn.split("."))


# Same row filter the dashboard applies to the realtime shadow table
# (routes/sensor_data.py): current row images only, scoped to the industry.
CDC_CHANGE_FILTER = "_pg_change_type IN ('insert', 'update_postimage')"
_CDC_HIDDEN_COLUMNS: tuple[str, ...] = ("_pg_lsn", "_pg_xid", "_sort_by", "_timestamp")


def telemetry_row_filter(mode: str, industry: str, qualifier: str = "") -> str | None:
    """Row predicate for ``mode``'s telemetry table, or None when none is needed.

    Realtime reads the Lakehouse Sync shadow table (CDC rows, all industries);
    ``qualifier`` (e.g. ``"tbl."``) prefixes each column for Genie snippets.
    """
    if mode != "realtime":
        return None
    q = qualifier
    return (
        f"{q}_pg_change_type IN ('insert', 'update_postimage') "
        f"AND {q}industry = {_literal(industry)}"
    )


def _cdc_where(row_filter: str | None) -> str:
    """Leading WHERE predicate (empty when the table needs no row filter)."""
    return f"{row_filter} AND " if row_filter else ""


def _filter_sql(condition: str, row_filter: str | None) -> str:
    """Filter snippet SQL; realtime combines the row filter, fully parenthesized."""
    return f"({row_filter} AND {condition})" if row_filter else condition


def _guard(expr: str, row_filter: str | None) -> str:
    """Restrict an aggregate's input rows: non-matching rows become NULL (ignored)."""
    return f"CASE WHEN {row_filter} THEN {expr} END" if row_filter else expr


def metadata_key(mode: str, key: str, qualifier: str = "") -> str:
    """SQL that reads ``key`` from the telemetry ``metadata`` column.

    Delta tables (microbatch, zerobus) store metadata as MAP<STRING, STRING>;
    the realtime Lakehouse Sync mirror carries the Lakebase JSONB, read with
    the ``:`` path operator (works on JSON STRING and VARIANT alike).
    """
    if mode == "realtime":
        return f"{qualifier}metadata:{key}::string"
    return f"{qualifier}metadata[{_literal(key)}]"


def _literal(value: str) -> str:
    """SQL string literal with single quotes escaped."""
    return "'" + str(value).replace("'", "''") + "'"


def starter_questions(mode: str, profile: IndustryProfile) -> tuple[str, ...]:
    """Return starter questions for ``mode`` (common + industry + mode-specific)."""
    if mode not in _MODE_STARTERS:
        raise ValueError(f"unknown mode {mode!r}")
    return _common_starters(profile) + _industry_starters(profile) + _MODE_STARTERS[mode]


def _cdc_column_configs() -> list[dict[str, Any]]:
    hidden = [{"column_name": name, "exclude": True} for name in _CDC_HIDDEN_COLUMNS]
    return [
        *hidden,
        {
            "column_name": "_pg_change_type",
            "description": ["Lakehouse Sync change type; readings are rows with value 'insert'."],
        },
    ]


def column_configs(profile: IndustryProfile, cdc: bool = False) -> list[dict[str, Any]]:
    """Column guidance for the telemetry table (unsorted; caller sorts).

    ``cdc`` adds the Lakehouse Sync shadow-table bookkeeping columns.
    """
    return [*_sensor_column_configs(profile), *(_cdc_column_configs() if cdc else [])]


def _sensor_column_configs(profile: IndustryProfile) -> list[dict[str, Any]]:
    return [
        {
            "column_name": "sensor_name",
            "enable_format_assistance": True,
            "enable_entity_matching": True,
            "description": ["Sensor identifier; one row per reading."],
            "synonyms": ["sensor", "tag", "signal"],
        },
        {
            "column_name": "sensor_type",
            "enable_format_assistance": True,
            "enable_entity_matching": True,
            "description": [f"Measurement kind, e.g. {profile.type_examples}."],
        },
        {
            "column_name": "event_time",
            "description": ["Reading timestamp (UTC). Use for all time windows."],
        },
        {
            "column_name": "value",
            "description": ["Measured value in `unit`."],
            "synonyms": ["reading", "measurement"],
        },
        {
            "column_name": "nominal_value",
            "description": [
                f"Design/target value. Used for the {profile.health_term} performance factor."
            ],
            "synonyms": ["target", "setpoint"],
        },
        {"column_name": "min_value", "description": ["Lower bound of the normal operating range."]},
        {"column_name": "max_value", "description": ["Upper bound of the normal operating range."]},
        {
            "column_name": "fault_active",
            "description": [
                f"TRUE while the {profile.fault_subject} reports an active fault (downtime)."
            ],
            "synonyms": ["fault", "down", "stopped", "alarm"],
        },
        *(_medtech_column_configs() if profile.medtech else []),
    ]


def _medtech_column_configs() -> list[dict[str, Any]]:
    return [
        {
            "column_name": "metadata",
            "description": [
                "Reading context. On cnc_1_* (CNC-1 5-axis mill) readings it carries the "
                "lot / device history record context: "
                + ", ".join(LOT_KEYS)
                + ". part_serial identifies the hip stem or acetabular cup being cut."
            ],
            "synonyms": ["lot", "batch", "serial", "work order", "UDI", "DHR"],
        },
    ]


def text_instructions(
    telemetry: str, metrics: str | None, mode: str, profile: IndustryProfile
) -> list[str]:
    """The single Genie text-instruction block, one newline-terminated rule per string.

    ``metrics`` is None for modes without a metrics table (zerobus).
    """
    return [f"{line}\n" for line in _instruction_lines(telemetry, metrics, mode, profile)]


def _scope_line(telemetry: str, metrics: str | None, mode: str, profile: IndustryProfile) -> str:
    metrics_part = (
        f"Streaming throughput/latency metrics are in {metrics}. " if metrics is not None else ""
    )
    return (
        f"This space covers ONE {profile.label} {MODE_LABELS[mode]} pipeline. "
        f"Sensor telemetry is in {telemetry} (one row per reading). "
        f"{metrics_part}"
        "Asset, sensor-binding, maintenance, alert-rule and alert-snapshot "
        "reference data is in the intelligence_* tables."
    )


def _medtech_context_lines(t: str, mode: str, lit: str) -> list[str]:
    """Medical-device quality grounding for the orthopaedic implant cell."""
    lot = metadata_key(mode, "lot_id", f"{t}.")
    serial = metadata_key(mode, "part_serial", f"{t}.")
    return [
        "Context: Trinity Medical orthopaedic implants plant (Warsaw, IN), regulated under "
        "21 CFR 820 (QMSR) and ISO 13485. Spindle 3 on CNC-1 (5-axis mill) machines "
        "Ti-6Al-4V femoral hip stems (part_number HS-TI64-12) and acetabular cups. "
        "Line: MILL -> CMM INSPECT -> CLEAN / PASSIVATE -> STERILE PACK.",
        f"Spindle 3 temperature ({t}.sensor_name = '{SPINDLE_TEMP_SENSOR}') has a VALIDATED "
        "process-parameter limit (OQ-TM-CNC1-014, PQ-TM-HS12-022, process spec "
        "PS-TM-MILL-HS-003 rev C): take the threshold from intelligence_alert_rules where "
        f"rule_id = '{SPINDLE_TEMP_RULE}' (fault code VPP-SPINDLE3-TEMP). A reading above it "
        "is an excursion outside the validated state and a dimensional nonconformance risk.",
        f"Lot context: {lot} is the lot, {serial} the part serial; work_order, part_number, "
        "udi_di (UDI device identifier) and dhr_ref (device history record) are in the same "
        f"{t}.metadata column. Parts at risk for an excursion = distinct part_serial values "
        f"on '{SPINDLE_TEMP_SENSOR}' readings above the validated limit; the rest of the lot "
        "is a release candidate pending QA review. The app's QA hold is slightly wider (it "
        "runs from the first over-limit reading until the alert clears below the hysteresis "
        "band), so it can name one more part than this query; the app's list is authoritative.",
        "Time to detect = intelligence_alert_snapshot.opened_at minus the first reading above "
        "the validated limit in the same excursion. MTTR = cleared/updated time minus opened_at.",
        f"First-pass yield / right-first-time means ONLY this: {PROJECTED_RFT_DEFINITION} "
        f"Parts started = distinct {serial} values on cnc_1_* readings in the lot. Never call "
        "the process health index first-pass yield.",
        "NCRs, QA holds (quarantine_lot), CMM inspection requests and their Part 11-style "
        "audit fields (actor, timestamp, reason, comment, signature meaning) are recorded in "
        "the app's decision log, not in these tables. For NCR numbers or dispositions, list the "
        "VPP-SPINDLE3-TEMP alert episodes and lots at risk, and say the NCR record lives in "
        "the app's decision log.",
        "You recommend; you never release or quarantine lots, open NCRs or sign records. "
        "Those require a qualified person's electronic signature in the app.",
    ]


def _industry_context_lines(profile: IndustryProfile) -> list[str]:
    """Extra grounding lines for industries whose wording is derived from the catalog."""
    if not profile.derived:
        return []
    lines = [f"Typical {profile.label} sensors: {', '.join(profile.sensor_examples)}."]
    if profile.units:
        lines.append(f"Units used in {profile.label} readings include: {', '.join(profile.units)}.")
    return lines


def _instruction_lines(
    telemetry: str, metrics: str | None, mode: str, profile: IndustryProfile
) -> list[str]:
    industry = profile.industry
    t = _short(telemetry)
    lit = _literal(industry)
    row_filter = telemetry_row_filter(mode, industry, f"{t}.")
    cdc_note = (
        f"{telemetry} is the Lakehouse Sync mirror of the real-time Lakebase "
        f"stream (all industries): ALWAYS filter {row_filter} (current reading "
        "rows for this industry) and ignore the _pg_* / _timestamp / _sort_by "
        "bookkeeping columns."
        if row_filter
        else None
    )
    if metrics is None:
        metrics_note = (
            f"This pipeline has no streaming metrics table; answer ingest-rate questions from "
            f"COUNT(*) over {telemetry} per minute."
        )
    else:
        metrics_note = (
            f"Filter {metrics} with mode = '{mode}' for this pipeline's streaming metrics."
        )
    return [
        _scope_line(telemetry, metrics, mode, profile),
        *_industry_context_lines(profile),
        *(_medtech_context_lines(t, mode, lit) if profile.medtech else []),
        f"Downtime = time intervals where {t}.fault_active is TRUE. Approximate "
        "downtime minutes as the count of distinct minutes "
        f"(date_trunc('minute', {t}.event_time)) with at least one fault_active reading.",
        "Availability = 1 - (fault minutes / observed minutes), where observed minutes "
        "are distinct minutes with any reading. Report as a percentage.",
        f"Performance = AVG(LEAST({t}.value / {t}.nominal_value, 1)) for readings with "
        "nominal_value > 0 (how close output runs to its nominal rate).",
        f"Quality = share of readings with {t}.value BETWEEN {t}.min_value AND {t}.max_value.",
        f"{profile.health_term} (approximation) = Availability x Performance x Quality. Always say it is "
        f"an approximation derived from sensor telemetry, not {profile.output_note}.",
        f"Anomaly = a reading where {t}.value < {t}.min_value OR {t}.value > "
        f"{t}.max_value. Ignore rows where min_value or max_value is NULL.",
        "The intelligence_* tables hold several industries: ALWAYS filter every "
        f"intelligence_* table with industry = {lit}.",
        f"Bridge from telemetry to reference data: {t}.sensor_name = "
        "intelligence_sensor_bindings.sensor_name, then "
        "intelligence_sensor_bindings.sensor_id = intelligence_alert_rules.sensor_id "
        "or intelligence_alert_snapshot.sensor_id (fault codes, thresholds, severity, "
        "alert status). intelligence_sensor_bindings.asset_id links to "
        "intelligence_assets and intelligence_maintenance_records.",
        "Faults: report fault_active readings and use the bridge above for fault "
        "codes and severity.",
        "Maintenance: use intelligence_maintenance_records joined to "
        "intelligence_sensor_bindings on asset_id to relate faults to assets and "
        "their last maintenance.",
        "Default time window when unspecified: the last 24 hours, using "
        f"{t}.event_time >= current_timestamp() - INTERVAL 24 HOURS.",
        metrics_note,
        *([cdc_note] if cdc_note else []),
        "Never modify data. Answer read-only with SELECT queries.",
    ]


def _downtime_sql(tel: str, row_filter: str | None = None) -> str:
    return (
        "SELECT sensor_name, COUNT(DISTINCT date_trunc('minute', event_time)) "
        f"AS downtime_minutes FROM {tel} WHERE {_cdc_where(row_filter)}fault_active "
        "AND event_time >= current_timestamp() - INTERVAL 24 HOURS "
        "GROUP BY sensor_name ORDER BY downtime_minutes DESC"
    )


def _health_sql(tel: str, alias: str, row_filter: str | None = None) -> str:
    return (
        "WITH m AS (SELECT sensor_name, "
        "COUNT(DISTINCT date_trunc('minute', event_time)) AS observed_min, "
        "COUNT(DISTINCT CASE WHEN fault_active THEN date_trunc('minute', event_time) END) "
        "AS fault_min, "
        "AVG(CASE WHEN nominal_value > 0 THEN LEAST(value / nominal_value, 1) END) "
        "AS performance, "
        "AVG(CASE WHEN min_value IS NULL OR max_value IS NULL THEN NULL "
        "WHEN value BETWEEN min_value AND max_value THEN 1.0 ELSE 0.0 END) "
        f"AS quality FROM {tel} "
        f"WHERE {_cdc_where(row_filter)}event_time >= current_timestamp() - INTERVAL 24 HOURS "
        "GROUP BY sensor_name) "
        "SELECT sensor_name, ROUND(100 * (1 - fault_min / observed_min), 2) AS availability_pct, "
        "ROUND(100 * performance, 2) AS performance_pct, ROUND(100 * quality, 2) AS quality_pct, "
        "ROUND(100 * (1 - fault_min / observed_min) * COALESCE(performance, 1) "
        "* COALESCE(quality, 1), 2) "
        f"AS {alias} FROM m ORDER BY {alias} ASC"
    )


def _anomaly_sql(tel: str, row_filter: str | None = None) -> str:
    return (
        "SELECT sensor_name, COUNT(*) AS anomalies, MIN(value) AS min_seen, "
        f"MAX(value) AS max_seen FROM {tel} "
        f"WHERE {_cdc_where(row_filter)}(value < min_value OR value > max_value) "
        "AND event_time >= current_timestamp() - INTERVAL 1 HOUR "
        "GROUP BY sensor_name ORDER BY anomalies DESC"
    )


def _maintenance_sql(
    tel: str, refs: dict[str, str], industry: str, row_filter: str | None = None
) -> str:
    bindings = _quoted(refs["sensor_bindings"])
    maint = _quoted(refs["maintenance_records"])
    lit = _literal(industry)
    return (
        "WITH faults AS (SELECT sensor_name, COUNT(*) AS fault_readings "
        f"FROM {tel} WHERE {_cdc_where(row_filter)}fault_active "
        "AND event_time >= current_timestamp() - INTERVAL 7 DAYS GROUP BY sensor_name) "
        "SELECT b.asset_id, SUM(f.fault_readings) AS fault_readings, "
        "MAX(r.completed_at) AS last_maintenance "
        f"FROM faults f JOIN {bindings} b ON b.sensor_name = f.sensor_name "
        f"AND b.industry = {lit} "
        f"LEFT JOIN {maint} r ON r.asset_id = b.asset_id AND r.industry = {lit} "
        "GROUP BY b.asset_id ORDER BY fault_readings DESC"
    )


def _projected_rft_sql(tel: str, refs: dict[str, str], industry: str, mode: str) -> str:
    """PROJECTED_RFT_DEFINITION per lot touched by a Spindle 3 excursion (last 24 h)."""
    rules = _quoted(refs["alert_rules"])
    lot = metadata_key(mode, "lot_id", "t.")
    serial = metadata_key(mode, "part_serial", "t.")
    where = _cdc_where(telemetry_row_filter(mode, industry, "t."))
    window = "t.event_time >= current_timestamp() - INTERVAL 24 HOURS"
    return (
        f"WITH started AS (SELECT {lot} AS lot_id, COUNT(DISTINCT {serial}) AS parts_started "
        f"FROM {tel} t WHERE {where}t.sensor_name LIKE 'cnc_1_%' AND {window} "
        f"AND {lot} IS NOT NULL GROUP BY 1), "
        f"at_risk AS (SELECT {lot} AS lot_id, COUNT(DISTINCT {serial}) AS parts_at_risk "
        f"FROM {tel} t JOIN {rules} r ON r.rule_id = '{SPINDLE_TEMP_RULE}' "
        f"AND r.industry = {_literal(industry)} WHERE {where}"
        f"t.sensor_name = '{SPINDLE_TEMP_SENSOR}' AND t.value > r.threshold AND {window} "
        f"AND {lot} IS NOT NULL GROUP BY 1) "
        "SELECT s.lot_id, s.parts_started, a.parts_at_risk, "
        "ROUND(100 * (s.parts_started - a.parts_at_risk) / s.parts_started, 1) "
        "AS projected_rft_pct FROM started s JOIN at_risk a ON a.lot_id = s.lot_id "
        "ORDER BY s.lot_id DESC"
    )


def _excursion_lots_sql(tel: str, refs: dict[str, str], industry: str, mode: str) -> str:
    """Lots (and parts at risk) machined while Spindle 3 exceeded its validated limit."""
    rules = _quoted(refs["alert_rules"])
    lot = metadata_key(mode, "lot_id", "t.")
    serial = metadata_key(mode, "part_serial", "t.")
    wo = metadata_key(mode, "work_order", "t.")
    dhr = metadata_key(mode, "dhr_ref", "t.")
    where = _cdc_where(telemetry_row_filter(mode, industry, "t."))
    return (
        f"SELECT {lot} AS lot_id, MAX({wo}) AS work_order, MAX({dhr}) AS dhr_ref, "
        f"COUNT(DISTINCT {serial}) AS parts_at_risk, MIN(t.event_time) AS first_excursion, "
        "MAX(t.event_time) AS last_excursion, ROUND(MAX(t.value), 1) AS peak_temp_c, "
        f"MAX(r.threshold) AS validated_limit_c FROM {tel} t JOIN {rules} r "
        f"ON r.rule_id = '{SPINDLE_TEMP_RULE}' AND r.industry = {_literal(industry)} "
        f"WHERE {where}t.sensor_name = '{SPINDLE_TEMP_SENSOR}' AND t.value > r.threshold "
        "AND t.event_time >= current_timestamp() - INTERVAL 24 HOURS "
        f"AND {lot} IS NOT NULL GROUP BY 1 ORDER BY first_excursion DESC"
    )


def _mode_sql(tel: str, metrics: str | None, mode: str) -> tuple[str, str]:
    if metrics is None:
        return (
            "How many readings per minute were ingested over the last hour?",
            "SELECT date_trunc('minute', event_time) AS minute, COUNT(*) AS readings "
            f"FROM {tel} WHERE event_time >= current_timestamp() - INTERVAL 1 HOUR "
            "GROUP BY 1 ORDER BY 1",
        )
    return (
        "What is the streaming throughput and p99 latency over the last hour?",
        "SELECT date_trunc('minute', timestamp) AS minute, AVG(processed_rps) AS processed_rps, "
        f"MAX(e2e_p99) AS e2e_p99_ms FROM {metrics} WHERE mode = '{mode}' "
        "AND timestamp >= current_timestamp() - INTERVAL 1 HOUR GROUP BY 1 ORDER BY 1",
    )


def example_sqls(
    telemetry: str,
    metrics: str | None,
    refs: dict[str, str],
    mode: str,
    profile: IndustryProfile,
) -> list[tuple[str, str]]:
    """Return ``(question, sql)`` pairs scoped to this pipeline's tables."""
    tel = _quoted(telemetry)
    industry = profile.industry
    rf = telemetry_row_filter(mode, industry)
    return [
        (
            "How many minutes of downtime did each sensor have in the last 24 hours?",
            _downtime_sql(tel, rf),
        ),
        (
            f"What is the approximate {profile.health_term} per sensor over the last 24 hours?",
            _health_sql(tel, profile.health_alias, rf),
        ),
        ("Which sensors had the most anomalies in the last hour?", _anomaly_sql(tel, rf)),
        (
            "Which assets had the most faults this week and when were they last maintained?",
            _maintenance_sql(tel, refs, industry, rf),
        ),
        *(
            [
                (
                    MEDTECH_STARTERS[0],
                    _excursion_lots_sql(tel, refs, industry, mode),
                ),
                (
                    MEDTECH_STARTERS[3],
                    _projected_rft_sql(tel, refs, industry, mode),
                ),
            ]
            if profile.medtech
            else []
        ),
        _mode_sql(tel, _quoted(metrics) if metrics else None, mode),
    ]


def sql_snippets(
    telemetry: str, mode: str, profile: IndustryProfile
) -> dict[str, list[dict[str, Any]]]:
    """Reusable measures / filters / expressions qualified with the table name.

    For realtime, measures and filters also apply the CDC/industry row filter.
    """
    t = _short(telemetry)
    rf = telemetry_row_filter(mode, profile.industry, f"{t}.")
    fault = f"CASE WHEN {t}.fault_active THEN 1.0 ELSE 0.0 END"
    anomaly = (
        f"CASE WHEN {t}.min_value IS NULL OR {t}.max_value IS NULL THEN NULL "
        f"WHEN {t}.value < {t}.min_value OR {t}.value > {t}.max_value THEN 1.0 ELSE 0.0 END"
    )
    return {
        "measures": [
            {
                "alias": "fault_rate_pct",
                "display_name": "Fault rate %",
                "sql": [f"100 * AVG({_guard(fault, rf)})"],
                "synonyms": ["downtime rate", "fault ratio"],
            },
            {
                "alias": "availability_pct",
                "display_name": "Availability % (reading-based)",
                "sql": [f"100 * (1 - AVG({_guard(fault, rf)}))"],
                "synonyms": ["availability", "uptime"],
            },
            {
                "alias": "anomaly_rate_pct",
                "display_name": "Anomaly rate %",
                "sql": [f"100 * AVG({_guard(f'({anomaly})', rf)})"],
                "synonyms": ["out of range rate"],
            },
        ],
        "filters": [
            {
                "display_name": "Fault active (downtime)",
                "sql": [_filter_sql(f"{t}.fault_active = TRUE", rf)],
                "synonyms": ["down", "faulted", "stopped"],
            },
            {
                "display_name": "Anomalous reading",
                "sql": [
                    _filter_sql(f"({t}.value < {t}.min_value OR {t}.value > {t}.max_value)", rf)
                ],
                "synonyms": ["out of range", "anomaly"],
            },
            {
                "display_name": "Last 24 hours",
                "sql": [
                    _filter_sql(f"{t}.event_time >= current_timestamp() - INTERVAL 24 HOURS", rf)
                ],
            },
        ],
        "expressions": [
            {
                "alias": "deviation_from_nominal",
                "display_name": "Deviation from nominal",
                "sql": [f"{t}.value - {t}.nominal_value"],
            },
            {
                "alias": "reading_minute",
                "display_name": "Reading minute",
                "sql": [f"date_trunc('minute', {t}.event_time)"],
            },
        ],
    }
