import type { ReactNode } from "react";

/** Color-coded accent palette for the 4 dashboard tiers. */
export const SECTION_ACCENTS = {
  sse: "#3fb950", // Live / SSE — green
  lakebase: "#3b82f6", // Lakebase warm — blue
  dbsql: "#a855f7", // DBSQL cold — purple
  cdf: "#f59e0b", // CDF / amber
  zerobus: "#ec4899", // Zerobus Delta ingest — pink
} as const;

export type SectionAccent = keyof typeof SECTION_ACCENTS;

interface DashboardSectionProps {
  /** Big section number (1-4). */
  index: number;
  title: string;
  subtitle?: string;
  accent: SectionAccent;
  badge?: ReactNode;
  children: ReactNode;
  /**
   * Visual emphasis only.  ``"dimmed"`` lowers opacity to de-emphasise a
   * section that isn't the focus of the current Live/Historical view.  It does
   * NOT change data fetching — every plane stays fixed and explicit.
   */
  emphasis?: "normal" | "dimmed";
}

/**
 * A clearly-labeled, color-coded dashboard section wrapper. The left accent
 * bar + header dot are driven by the section's tier color so each of the four
 * sections is visually distinct.
 */
export function DashboardSection({
  index,
  title,
  subtitle,
  accent,
  badge,
  children,
  emphasis = "normal",
}: DashboardSectionProps) {
  const color = SECTION_ACCENTS[accent];
  return (
    <section
      className={`rounded-lg border bg-surface-low/40 transition-opacity ${
        emphasis === "dimmed" ? "opacity-60" : "opacity-100"
      }`}
      style={{ borderColor: `${color}33`, borderLeft: `3px solid ${color}` }}
    >
      <header className="flex items-center gap-3 px-4 py-2.5 border-b border-border/40">
        <span
          className="flex h-6 w-6 shrink-0 items-center justify-center rounded-full font-mono text-[11px] font-bold"
          style={{ background: `${color}22`, color }}
        >
          {index}
        </span>
        <div className="min-w-0">
          <h2
            className="font-display text-sm font-semibold leading-tight truncate"
            style={{ color }}
          >
            {title}
          </h2>
          {subtitle && (
            <p className="font-ui text-[10px] text-muted-foreground truncate">
              {subtitle}
            </p>
          )}
        </div>
        {badge && <div className="ml-auto shrink-0">{badge}</div>}
      </header>
      <div className="p-4 space-y-4">{children}</div>
    </section>
  );
}

interface SectionBadgeProps {
  color: string;
  label: string;
  dot?: boolean;
}

/** Pill badge with a leading dot, color-coded to a tier. */
export function SectionBadge({ color, label, dot = true }: SectionBadgeProps) {
  return (
    <span
      className="inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 font-mono text-[10px] font-medium ring-1"
      style={{
        background: `${color}1f`,
        color,
        boxShadow: `inset 0 0 0 1px ${color}55`,
      }}
    >
      {dot && (
        <span
          className="h-1.5 w-1.5 rounded-full"
          style={{ background: color }}
        />
      )}
      {label}
    </span>
  );
}
