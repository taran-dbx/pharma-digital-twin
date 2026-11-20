import type { TileView } from "@/lib/value-format";

/** A KPI row of stat tiles: label · value · one-line foot. Values are never colored. */
export function StatTiles({ tiles, label, dim }: { tiles: TileView[]; label: string; dim?: boolean }) {
  return (
    <div className={`value-tiles${dim ? " is-refreshing" : ""}`} role="list" aria-label={label}>
      {tiles.map((tile) => (
        <article className="value-tile" role="listitem" key={tile.id} data-testid={`value-tile-${tile.id}`}>
          <span className="value-tile__label">{tile.label}</span>
          <span className="value-tile__value">{tile.value}</span>
          <span className="value-tile__foot">{tile.foot}</span>
        </article>
      ))}
    </div>
  );
}

export function TilesSkeleton({ count, label }: { count: number; label: string }) {
  return (
    <div className="value-tiles is-loading" aria-busy="true" aria-label={label}>
      {Array.from({ length: count }, (_, i) => (
        <article className="value-tile" key={i}>
          <span className="value-tile__label">Loading…</span>
          <span className="value-tile__value">&nbsp;</span>
          <span className="value-tile__foot">&nbsp;</span>
        </article>
      ))}
    </div>
  );
}
