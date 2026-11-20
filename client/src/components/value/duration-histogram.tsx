/**
 * Single-series duration histogram: one sequential hue, columns ≤ 24px wide
 * with a 4px rounded cap and a square baseline, a 2px surface gap, hairline
 * baseline, a per-column hover/focus tooltip and a table view. One series,
 * so the title names it and there is no legend.
 */
import { useState } from "react";

import { binDurations, type DurationBin } from "@/lib/value-format";

export function DurationHistogram({ title, samples, caption }: { title: string; samples: number[]; caption: string }) {
  const bins = binDurations(samples);
  const [active, setActive] = useState<number | null>(null);
  const [table, setTable] = useState(false);
  const peak = Math.max(1, ...bins.map((b) => b.count));

  return (
    <figure className="value-chart" data-testid="duration-histogram">
      <figcaption className="value-chart__head">
        <span className="value-chart__title">{title}</span>
        {bins.length > 0 && (
          <button type="button" className="value-chart__toggle" onClick={() => setTable((t) => !t)} aria-pressed={table}>
            {table ? "Chart" : "Table"}
          </button>
        )}
      </figcaption>
      {bins.length === 0 ? (
        <p className="value-empty">No excursions in this window yet.</p>
      ) : table ? (
        <BinTable bins={bins} title={title} />
      ) : (
        <div className="value-chart__plot">
          <div className="value-chart__cols" role="list" aria-label={`${title}, ${samples.length} excursions`}>
            {bins.map((bin, i) => (
              <div
                key={bin.start}
                role="listitem"
                tabIndex={0}
                className={`value-chart__col${active === i ? " is-active" : ""}`}
                aria-label={`${bin.label}: ${bin.count}`}
                onPointerEnter={() => setActive(i)}
                onPointerLeave={() => setActive(null)}
                onFocus={() => setActive(i)}
                onBlur={() => setActive(null)}
              >
                <span className="value-chart__bar" style={{ height: bin.count ? `${(bin.count / peak) * 100}%` : 0 }} />
                {active === i && (
                  <span className="value-chart__tip" role="status">
                    <strong>{bin.count}</strong>
                    <span>{bin.label}</span>
                  </span>
                )}
              </div>
            ))}
          </div>
          <div className="value-chart__axis" aria-hidden="true">
            <span>{bins[0].label.split("–")[0]}</span>
            <span>{bins[bins.length - 1].label.split("–")[1]}</span>
          </div>
        </div>
      )}
      <p className="value-chart__caption">{caption}</p>
    </figure>
  );
}

function BinTable({ bins, title }: { bins: DurationBin[]; title: string }) {
  return (
    <table className="value-table value-table--compact">
      <caption className="sr-only">{title}</caption>
      <thead>
        <tr>
          <th scope="col">Range</th>
          <th scope="col">Excursions</th>
        </tr>
      </thead>
      <tbody>
        {bins.map((bin) => (
          <tr key={bin.start}>
            <td>{bin.label}</td>
            <td className="tnum">{bin.count}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
