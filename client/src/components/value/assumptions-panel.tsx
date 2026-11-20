/**
 * Editable savings-model assumptions with the formula they feed. Edits are
 * validated against the server's field bounds before they are applied; the
 * server validates again and answers 422 on anything out of range.
 */
import { useState, type FormEvent } from "react";

import type { AssumptionField, AssumptionName, Assumptions, Projection } from "@/lib/value-api";
import { costLineLabel, fullUsd } from "@/lib/value-format";

type Draft = Partial<Record<AssumptionName, string>>;

/** Capture rate is edited as a percentage; everything else as-is. */
const PERCENT_FIELDS: ReadonlySet<AssumptionName> = new Set(["capture_rate"]);

export function toDisplay(name: AssumptionName, value: number): string {
  return PERCENT_FIELDS.has(name) ? String(Math.round(value * 1000) / 10) : String(value);
}

/** Validate one edited field; returns the model value or an error message. */
export function parseField(field: AssumptionField, raw: string): { value?: number; error?: string } {
  const text = raw.trim();
  if (text === "") return { error: "Enter a number" };
  const shown = Number(text);
  if (!Number.isFinite(shown)) return { error: "Enter a number" };
  const value = PERCENT_FIELDS.has(field.name) ? shown / 100 : shown;
  if (field.integer && !Number.isInteger(value)) return { error: "Whole numbers only" };
  if (value < field.min || value > field.max) {
    const lo = PERCENT_FIELDS.has(field.name) ? `${field.min * 100}%` : field.min.toLocaleString("en-US");
    const hi = PERCENT_FIELDS.has(field.name) ? `${field.max * 100}%` : field.max.toLocaleString("en-US");
    return { error: `Between ${lo} and ${hi}` };
  }
  return { value };
}

export function AssumptionsPanel({
  fields,
  assumptions,
  defaults,
  overridden,
  projection,
  onApply,
}: {
  fields: AssumptionField[];
  assumptions: Assumptions;
  defaults: Assumptions;
  overridden: AssumptionName[];
  projection: Projection;
  onApply: (overrides: Partial<Record<AssumptionName, number>>) => void;
}) {
  const [draft, setDraft] = useState<Draft>({});
  const errors = Object.fromEntries(
    fields.flatMap((f) => (draft[f.name] === undefined ? [] : [[f.name, parseField(f, draft[f.name] ?? "").error]])),
  ) as Partial<Record<AssumptionName, string | undefined>>;
  const invalid = Object.values(errors).some(Boolean);

  const submit = (event: FormEvent) => {
    event.preventDefault();
    if (invalid) return;
    const next: Partial<Record<AssumptionName, number>> = {};
    for (const f of fields) {
      const raw = draft[f.name];
      const value = raw === undefined ? assumptions[f.name] : parseField(f, raw).value;
      if (value !== undefined && value !== defaults[f.name]) {
        next[f.name] = value;
      }
    }
    setDraft({});
    onApply(next);
  };

  const reset = () => {
    setDraft({});
    onApply({});
  };

  return (
    <section className="value-assumptions" aria-labelledby="value-assumptions-title">
      <header className="value-section-head">
        <h3 id="value-assumptions-title">Assumptions</h3>
        <span className="value-note">Illustrative defaults · edit to model your site</span>
      </header>
      <form onSubmit={submit} className="value-assumptions__form" noValidate>
        {fields.map((f) => {
          const id = `assumption-${f.name}`;
          const error = errors[f.name];
          return (
            <label key={f.name} htmlFor={id} className={`value-field${error ? " has-error" : ""}`}>
              <span className="value-field__label">
                {f.label}
                {PERCENT_FIELDS.has(f.name) ? " (%)" : ""}
                {overridden.includes(f.name) && <em> · edited</em>}
              </span>
              <input
                id={id}
                name={f.name}
                inputMode="decimal"
                value={draft[f.name] ?? toDisplay(f.name, assumptions[f.name])}
                onChange={(e) => setDraft((d) => ({ ...d, [f.name]: e.target.value }))}
                aria-invalid={Boolean(error)}
                aria-describedby={error ? `${id}-error` : undefined}
              />
              {error && (
                <span id={`${id}-error`} className="value-field__error">
                  {error}
                </span>
              )}
            </label>
          );
        })}
        <div className="value-assumptions__actions">
          <button type="submit" className="btn primary" disabled={invalid}>
            Recalculate
          </button>
          <button type="button" className="btn" onClick={reset} disabled={overridden.length === 0 && Object.keys(draft).length === 0}>
            Reset to defaults
          </button>
        </div>
      </form>

      <div className="value-breakdown">
        <h4>One excursion, priced</h4>
        <table className="value-table value-table--compact">
          <tbody>
            {projection.per_event.if_missed.map((line) => (
              <tr key={line.item}>
                <td>{costLineLabel(line.item)}</td>
                <td className="tnum">{fullUsd(line.usd)}</td>
              </tr>
            ))}
            <tr className="is-total">
              <td>Lot lost if scrapped</td>
              <td className="tnum">{fullUsd(projection.per_event.loss_per_event)}</td>
            </tr>
            {projection.per_event.if_caught.map((line) => (
              <tr key={line.item}>
                <td>Instead, if caught: {costLineLabel(line.item)}</td>
                <td className="tnum">{fullUsd(line.usd)}</td>
              </tr>
            ))}
            <tr className="is-total">
              <td>Avoided per caught excursion</td>
              <td className="tnum">{fullUsd(projection.per_event.net_avoided_per_caught_event)}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div className="value-formula" data-testid="value-formula">
        <h4>Formula</h4>
        <ul>
          {Object.entries(projection.formula).map(([name, text]) => (
            <li key={name}>
              <code>{text}</code>
            </li>
          ))}
        </ul>
        {projection.warnings.map((w) => (
          <p key={w} className="value-warning" role="note">
            {w}
          </p>
        ))}
      </div>
    </section>
  );
}
