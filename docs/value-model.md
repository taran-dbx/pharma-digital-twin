# Value model and operational metrics

The medical-device cell page has two outcome views next to the live cell, one per owner, each
in the metrics that owner is measured on:

| Tab | For | Shows |
|---|---|---|
| **Executive · Alex Morgan, VP Quality & Site Head** | the funding executive | cost avoided to date, annual exposure if scrapped, exposure remaining if scrapped, lots protected vs quarantined, an editable assumptions panel with the formula, and a one-line "what this means" |
| **Operations · Shreya, Head of Manufacturing Ops** | the operational owner | time-to-detect and time-to-acknowledge distributions, time-to-disposition, NCRs and CMM inspections triggered, projected right-first-time, MTTR, and the recent excursions table |

Both read one pipeline over a window (24 h, 7 d, 30 d or 90 d). Dashboards are per-pipeline,
so every query filters on the pipeline's alert keys.

> **Demo, not a business case of record.** Every cost is an illustrative reference value. The
> model shows *how* the outcome is priced so a site can put in its own figures; it is not
> validated financial or quality data.

## API

| Route | Purpose |
|---|---|
| `GET /api/value/summary` | Savings model: `projection` (annual, from assumptions) and `actuals` (recorded over the window) |
| `GET /api/ops/summary` | Operational metrics over the window |

Query parameters, both routes: `industry` (required, a curated industry id), `pipeline_id`
(required: a deployment UUID or `industry:<industry>`) and `window` (`<n>h` or `<n>d`, 1 h to
365 d, default `30d`). `/api/value/summary` also accepts any assumption below as an override,
e.g. `capture_rate=0.9&cost_per_lot_lost=60000`. Unknown parameters are rejected.

Responses use the envelope `{"success", "data", "error", "meta"}`. `meta` carries the
pipeline, the window bounds, `data_available` (false when the app backend is not connected)
and, on a 422, `details: [{field, message}]` for every invalid input. A failed query is a 503
with a generic message; details are logged server-side only. Identity is enforced the same way
as the alert routes these read from (`require_identity`).

A bound **realtime** deployment also reads alerts stamped `industry:<industry>`, because the
industry-first realtime consumer posts readings with that pipeline id rather than the
deployment id.

## Definitions

**Excursion (episode).** One open → cleared run of an alert. The alert row is reused when a
rule re-opens, so episodes are keyed by alert id plus the `opened_at` carried in every
lifecycle event (`alert_event.payload`), exactly as the alert history is.

**Onset.** `excursion_started_at`, as the alert engine stamps it when the alert opens: the
**first violating reading's device time** (`timestamp` or `event_time`). If that is missing or
falls outside `[opened_at − (rule.duration_s + 120 s), opened_at]`, the engine uses its own
first-violation clock, but never later than `opened_at − rule.duration_s`. The stamp is written
to the alert row's `metadata`, which belongs to the current episode and is replaced on reopen, and
also to the episode's `opened` event, so earlier episodes keep their own onset. The views read
the event copy first and the row for the current episode. They apply the same bound, and fall
back to **`opened_at − rule.duration_s`** (shown as "est.") when the stamp is missing or out of
bounds. This is the onset the live cell uses for its containment window.

**Containment window.** Onset → `cleared_at`, or the `request_cmm_inspection` decision if that
came first (it stops the mill, so no more parts are cut), or the window end while the excursion
is still open and the mill still running. The parts cut in it are the **parts at risk** (`lot_disposition` in
`domain/lot_context.py`).

**Time to detect (TTD).** `opened_at − onset`: pipeline latency plus the rule's sustain
duration. This is the number the three pipeline flavors compete on.

**Time to acknowledge (TTA).** The first of (the `acknowledged` event, the first signed
decision) minus `opened_at`. Signing a decision implies the alert was seen.

**Time to disposition.** The first **containment** decision (`quarantine_lot` or
`request_cmm_inspection`, or the legacy `hold_batch` / `send_to_inspect`) minus `opened_at`.

**Release deadline.** The end of the lot cycle the onset part belongs to. That is when the lot
leaves the mill for CMM and clean / pack, i.e. when the first out-of-limit part would move on.
The demo has no MES, so lots follow the deterministic lot clock (`LOT_CYCLE_S` = 300 s,
15 parts a lot).

**Caught in time.** A containment decision was signed **no later than the release deadline**.
The out-of-limit parts never left the cell unheld, so only the parts at risk go to CMM and
the rest of the lot is released. An excursion contained after the deadline is "late"; one with
no containment decision is "not contained". Neither counts as caught.

**Lots protected.** Distinct lots touched by caught-in-time excursions. **Lots quarantined**
counts distinct lots touched by excursions with a `quarantine_lot` decision; its parts split into
parts held (at risk) and parts released.

**NCRs / CMM inspections triggered.** Excursions with an `open_ncr` / `request_cmm_inspection`
decision. **Cell stops** count `production_event` rows with `state = 'stopped'` linked to the
pipeline's alerts.

**Projected right-first-time.** The app's single first-pass-yield proxy, defined once in
`domain/lot_context.py` (`projected_rft`) and `client/src/lib/manufacturing-quality.ts`:
`(parts started − parts at risk) / parts started` over the lots an excursion touched. The
window figure sums parts started and parts at risk over its excursions. It is *projected*
because no CMM results exist yet. The A×P×Q figure is the **process health index (telemetry
approximation)**, not FPY.

**MTTR.** Mean `cleared_at − opened_at` over cleared excursions: the time to bring Spindle 3
back inside its validated limit.

## Savings model

Defaults live in `src/iot_twin/services/value/value_defaults.json`; any of them can be
overridden per request (and in the assumptions panel). The model itself
(`services/value/value_model.py`) is pure and unit-tested.

```
loss_per_event     = cost_per_lot_lost + lot_release_delay_cost_per_day × release_delay_days_per_event
annual_events      = lines_in_scope × excursion_events_per_line_per_year
annual_exposure    = annual_events × loss_per_event                           (if scrapped)
caught_events      = annual_events × capture_rate
cost_avoided       = caught_events × (loss_per_event − inspection_cost)
exposure_remaining = annual_exposure − cost_avoided
                   = missed_events × loss_per_event + caught_events × inspection_cost
```

Actuals over the window:

```
cost_avoided_to_date = caught_in_time × (cost_per_lot_lost − inspection_cost)
exposure_if_scrapped = not_caught_in_time × cost_per_lot_lost
```

Every response also returns the formula with the numbers substituted, and a per-event
breakdown (what a lost lot costs, what catching it costs instead).

### Assumptions and their sources

| Assumption | Default | Bounds | Basis |
|---|---|---|---|
| `lines_in_scope` | 4 | 1–1,000 (whole) | Illustrative: a site running four 5-axis cells on implant stems |
| `excursion_events_per_line_per_year` | 12 | 0–10,000 | Illustrative: about one validated-parameter excursion per line per month |
| `cost_per_lot_lost` | $49,750 | 0–$1B | Sum of the four components below |
| · scrap | $27,750 | | 15 stems × $1,850, the app's reference cost of one scrapped Ti-6Al-4V hip stem (`LOT_REFERENCE.unitCostUsd`) |
| · rework and MRB / NCR handling | $6,000 | | Illustrative: investigation, MRB review, NCR documentation |
| · re-validation | $12,000 | | Illustrative: engineering time and extra sampling to re-establish the validated state |
| · lost run time | $4,000 | | Illustrative: machine and operator time to re-cut a lot |
| `capture_rate` | 0.8 | 0–1 | Illustrative: share of excursions contained before the lot moves |
| `inspection_cost` | $1,500 | 0–$1B | Illustrative: CMM measurement of the parts at risk plus the NCR it replaces the scrap with |
| `lot_release_delay_cost_per_day` | $0 | 0–$1B | Optional; off by default |
| `release_delay_days_per_event` | 5 | 0–365 | Used only when the delay cost is set |

Overriding `cost_per_lot_lost` replaces the component breakdown with one line, so the breakdown
always sums to the cost used.

## Limitations

* **Costs are references, not ledger data.** No ERP or QMS is connected. Replace the defaults
  with site figures before quoting a number.
* **Caught-in-time uses the demo lot clock.** Real lot movement comes from the MES; the
  release deadline here is the end of the 300 s lot cycle.
* **Priced "if scrapped".** Exposure assumes a missed excursion scraps the lot. In practice
  MRB may disposition some parts use-as-is, which lowers the true exposure.
* **Projected RFT is not FPY.** It counts parts at risk, not measured nonconformances; CMM
  results would replace it.
* **TTD depends on onset quality.** With a device-time onset, TTD includes the pipeline's
  delivery latency and the rule's sustain time. Excursions with a rule-duration onset (marked
  "est.") show only the configured debounce.
* **Window caps.** At most 500 excursions are read per request; TTD/TTA samples are capped at 200.
* **No persistence of overrides.** Edited assumptions live in the browser session only.
