# Medical-device domain: glossary and app mapping

The manufacturing industry in this app tells a **life-sciences medical-device manufacturing**
story. Trinity Medical machines orthopaedic implants at its Warsaw, IN plant. Spindle 3 on the
CNC-1 5-axis mill cuts Ti-6Al-4V femoral hip stems (part `HS-TI64-12`) and acetabular cups. The
line runs **MILL → CMM INSPECT → CLEAN / PASSIVATE → STERILE PACK**. When Spindle 3 heats past
its validated temperature limit, the parts cut during the excursion risk a dimensional
nonconformance, so they are quarantined, measured and dispositioned.

> **This is a demo, not a quality system.** Nothing in this app is a validated computerized
> system, a QMS of record, or evidence for a regulatory submission. Protocol numbers, lot
> numbers, the UDI-DI and the DHR references are reference fixtures that show where real
> records would plug in. A production deployment would integrate the MES, the QMS (NCR / CAPA)
> and the device history record system, and validate the result.

## People and KPIs

| Owner | Role | KPIs |
|---|---|---|
| Alex Morgan | VP Quality & Site Head (funding executive) | cost of poor quality, lots scrapped / quarantined, lot-release lead time |
| Shreya | Head of Manufacturing Operations (operational owner) | time-to-detect, NCRs opened, projected right-first-time (first-pass yield proxy), MTTR |

## IDs vs labels

The internal industry id stays **`manufacturing`**, so Kafka topics, Unity Catalog tables
(`manufacturing_realtime`, `manufacturing_metrics`, ...), Lakebase branches, pipeline names and
persisted alert rows do not churn. Only the user-facing name ("Medical Devices") and tag
("Life Sciences · orthopaedic implants · 5-axis machining") changed. The same rule applies to the
catalog: stable ids such as `mfg-site-detroit`, `mfg-asset-cnc-1` and `mfg-alert-spindle-temp`
are kept for compatibility with stored data, while their display names now read
"Trinity Medical · Warsaw, IN", "CNC-1 5-axis mill" and so on. `INDUSTRIES` now defaults to
`manufacturing`; pharmaceutical stays available as a secondary industry.

## Concept mapping

| Generic CNC demo | Medical-device demo | Where in the app |
|---|---|---|
| Detroit Plant | Trinity Medical, Warsaw, IN (orthopaedic implants) | `domain/catalog.py` site name (id `mfg-site-detroit` unchanged) |
| CNC-1 mill, Spindle 3 | CNC-1 5-axis mill, Spindle 3 cutting Ti-6Al-4V hip stems and acetabular cups | catalog asset `mfg-asset-cnc-1` |
| Vision QA gate | CMM-1 metrology lab (temperature-controlled, `hvac_zone_1_temp` / `hvac_zone_1_humidity`) | new catalog asset `mfg-asset-cmm-1`; twin station **CMM** |
| Hydraulic press 1 | Clean & passivation line (`press_1_*` sensors) | catalog asset `mfg-asset-press-1` |
| Assembly / pack line | Sterile pack line (`assy_line_1_*` sensors) | catalog asset `mfg-asset-assy-1`; twin station **PACK** (clean, passivate & sterile pack) |
| Air compressor 1 | Clean dry air compressor (`compressor_1_*`) | catalog asset `mfg-asset-compressor-1` |
| 60 °C operating limit | 60 °C **validated process parameter** (OQ-TM-CNC1-014, PQ-TM-HS12-022, PS-TM-MILL-HS-003 rev C) | alert rule `mfg-alert-spindle-temp`, validation refs in rule and alert `metadata` |
| `M-SPINDLE-OVERTEMP`, `M-SPINDLE-VIBE`, `M-PRESS-LOW` | `VPP-SPINDLE3-TEMP`, `SPC-SPINDLE3-VIBE`, `CLN-PASSIVATION-PRESS-LOW` | alert `fault_code` |
| Work order / batch fixture | Lot `TM-HS12-<YYMMDD>-<NNN>` (cycle of the UTC day, so ids never wrap), part serial `<lot>-NN`, work order `WO-TM-24817`, part `HS-TI64-12`, UDI-DI `00850012345671`, DHR `DHR-<lot>` | `metadata` on `cnc_1_*` readings and on alerts |
| Hold batch | **Quarantine lot** (QA hold) | `alert_response.action = quarantine_lot` |
| Send to inspect | **Request CMM inspection** (stops the mill and the pack line) | `request_cmm_inspection` |
| — | **Open NCR** | `open_ncr` |
| Request maintenance | Request Spindle 3 maintenance | `request_maintenance` |
| Operator click | Part 11-style signed record: actor, time, reason, comment, signature meaning | `alert_response` audit columns |
| OEE tiles | projected right-first-time, cost of poor quality if scrapped, NCRs, lots quarantined, time-to-detect | cell KPIs, Genie, KPI notebook |
| OEE (A × P × Q) | process health index (telemetry approximation); never called first-pass yield | Genie, KPI notebook |

## KPI definitions

- **Projected right-first-time** = (parts started − parts at risk) / parts started, over the lots an excursion touched. Parts at risk are the parts whose cut overlapped the time Spindle 3 was outside its validated limit. *Projected*, because no CMM results exist yet. The UI tile "Projected RFT", Genie's example SQL and this document use exactly this
  definition. The canonical text is `PROJECTED_RFT_DEFINITION` in
  `src/iot_twin/domain/lot_context.py`, mirrored in `client/src/lib/manufacturing-quality.ts`.
- **Parts at risk** start at the first over-limit reading the alert engine saw
  (`alert.metadata.excursion_started_at`), or `opened_at − duration_s` when that is unknown, and
  end when the alert clears. Containment errs inclusive.
- **Cost of poor quality (if scrapped)** = parts at risk × a reference unit cost; nothing has been
  scrapped until QA dispositions the parts.
- **Time to detect** = alert opened − first over-limit reading. **MTTR** = mean opened → cleared
  time of recent excursions.
- **Process health index (telemetry approximation)** = availability × performance × quality from
  telemetry (the OEE product). It is not first-pass yield.

## Glossary

### IQ / OQ / PQ (installation, operational, performance qualification)

Equipment and processes are qualified before they make product. **IQ** shows the equipment is
installed to specification, **OQ** shows it operates within its limits across the operating
range, and **PQ** shows the process consistently makes conforming product under routine
conditions. Limits proven in OQ/PQ become *validated process parameters*: running outside them
means the output is no longer covered by the validation.

Machining that is fully checked by CMM is normally *verified* rather than validated
(ISO 13485 §7.5.6 applies to processes whose output is not or cannot be fully verified). This demo
treats spindle temperature as a validated parameter because it governs a characteristic that is
not 100 % verified downstream, for example sub-surface or heat-affected material properties of
the titanium that CMM cannot measure.

*In the app:* the Spindle 3 temperature limit (60 °C) is modelled as a validated process
parameter. The alert rule carries its OQ/PQ protocol and process-spec references in
`metadata`, the fault code is prefixed `VPP-`, and the UI says "outside its validated limit"
rather than "over temperature". The protocol numbers are fixtures.

### DHR (device history record)

The record of how each lot or unit was made: dates, quantities, the equipment and process
parameters used, acceptance records, labels and the UDI. Under QMSR the DHR/DMR terms are
retired; per-lot production records (ISO 13485 §7.5.1) carry this content. The app keeps the
familiar "DHR" label for the reference.

*In the app:* every `cnc_1_*` reading and every Spindle 3 alert carries `lot_id`,
`part_serial`, `work_order`, `part_number`, `udi_di` and a `dhr_ref` in its `metadata` JSONB,
so an excursion can be traced to the exact parts cut during it. The DHR itself is not stored;
`dhr_ref` is a pointer a DHR system would resolve.

### NCR (nonconformance report)

A record that a product or process did not meet a requirement, with containment, investigation
and a disposition (use as is, rework, scrap, return).

*In the app:* `open_ncr` is a recorded response on the alert episode. It names the at-risk parts
derived from the lot context; disposition and investigation would continue in the QMS.

### CAPA (corrective and preventive action)

The systematic process that investigates the root cause of nonconformances (or potential ones)
and verifies that the fix works. Recurring NCRs on the same parameter typically trigger a CAPA.

*In the app:* not a workflow of its own. The alert history (repeat Spindle 3 excursions, their
peaks and decisions) and Genie questions over it are the evidence a CAPA owner would start from.

### UDI (unique device identification)

The FDA UDI system labels devices with a **UDI-DI** (device identifier: labeller and model or
version) and a **UDI-PI** (production identifier: lot or batch, serial number, manufacturing and
expiry dates).

*In the app:* the hip stem carries a fixture UDI-DI `00850012345671` (a GTIN-14 with a valid
check digit, not a registered device); `lot_id` and `part_serial` play the role of the UDI-PI.

### QMSR (21 CFR 820) and ISO 13485

FDA's Quality Management System Regulation, the amended 21 CFR Part 820, took effect on
**2 February 2026**. It incorporates **ISO 13485:2016** (medical devices — quality management
systems) by reference, replacing the former Quality System Regulation. ISO 13485 requires,
among other things, process validation, control of nonconforming product, traceability and
corrective action.

*In the app:* the workflow mirrors those clauses: a validated parameter is monitored live,
nonconforming product is contained (`quarantine_lot`), measured (`request_cmm_inspection`) and
recorded (`open_ncr`), and every decision is traceable to lot and part. The app is not part of
a QMS.

### 21 CFR Part 11 (electronic records and signatures)

Requirements for trustworthy electronic records and signatures: secure, computer-generated,
time-stamped audit trails, and signatures that show the signer's name, the date and time, and
the **meaning** of the signature (for example review, approval, responsibility or authorship).

*In the app:* each `alert_response` row records the actor (`recorded_by`, the authenticated app
user), the time (`created_at`), a `reason`, an optional `comment`, the `signature_meaning`
(`authored`, `reviewed`, `approved` or `responsible`) and `signed_at`. Rows are only ever
inserted, never updated, and one decision per action and episode is enforced by a unique
constraint, and the `alert_id` foreign key is `ON DELETE RESTRICT`, so deleting an alert
cannot silently delete its signed decisions. The API also returns a printed signature line
(`<signer> · <meaning> · <signed at>`), and the UI requires an explicit "I sign this decision
as …" intent before it submits. This is *Part 11-style*: there is no re-authentication at
signing, no signature/record linking controls and no system validation. Resuming production
after a validated-limit stop is not yet a signed decision (the resume records only who and when);
a validated deployment should require a signed reason there too. Legacy clients that post
`hold_batch` / `send_to_inspect` without signing fields are recorded as `legacy-unsigned`.

## Data model notes

`ensure_alert_response_table` (`src/iot_twin/models/app_schema.py`) upgrades existing app-backend
databases in place, without data loss:

- The `action` CHECK constraint is dropped and recreated inside a single `DO` block, only when
  the current definition lacks the new actions. New values: `quarantine_lot`, `open_ncr`,
  `request_cmm_inspection`, `request_maintenance`; the legacy `hold_batch` and
  `send_to_inspect` stay accepted, so stored rows remain valid. Re-running is a no-op.
- The audit columns are added only when missing (checked in `information_schema.columns`),
  which avoids the Postgres "must be owner of table" error that an unconditional
  `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` raises for a table created by another role.
- A cascading `alert_id` foreign key (older tables) is replaced by `ON DELETE RESTRICT` in the
  same guarded way; new tables are created with RESTRICT.
- If the app identity does not own a table that still needs an upgrade, boot logs a warning
  instead of failing. Signed decisions are then still stored durably: the signature fields go to
  `reference.audit`, and an action the old CHECK rejects is stored under its legacy name
  (`quarantine_lot` → `hold_batch`, `request_cmm_inspection` → `send_to_inspect`) with the real
  action in `reference.audit.action`; reads and history map it back. `open_ncr` has no legacy
  equivalent, so it is refused with `503 alert_response table not upgraded` until the owner runs
  the upgrade. With the app backend configured, a decision is never kept only in memory; any
  database error is a `503`. The in-memory store is used only when no app backend is configured.

See [architecture.md](architecture.md#alerts) for the alert flow and
[genie.md](genie.md) for the quality-aware Genie questions.
