/**
 * Presentational primitives for the pipeline resource-health page, styled to
 * match the setup-validation flow's status-pill / step-row visual (glass
 * cards, uppercase eyebrow labels, success/warning/destructive semantics) so
 * this new page reads as part of the same app in both light and dark themes.
 */
import type { ReactNode } from "react";
import {
  CheckCircle2,
  AlertTriangle,
  XCircle,
  MinusCircle,
} from "lucide-react";
import { cn } from "@/lib/utils";
import type { HealthStatus, ResourceGroup, ResourceItem } from "./types";

const HEALTH_META: Record<
  HealthStatus,
  { label: string; text: string; pillBg: string; dot: string; icon: typeof CheckCircle2 }
> = {
  healthy: {
    label: "Healthy",
    text: "text-success",
    pillBg: "bg-success/15",
    dot: "bg-success",
    icon: CheckCircle2,
  },
  warning: {
    label: "Warning",
    text: "text-warning",
    pillBg: "bg-warning/15",
    dot: "bg-warning",
    icon: AlertTriangle,
  },
  down: {
    label: "Down",
    text: "text-destructive",
    pillBg: "bg-destructive/15",
    dot: "bg-destructive",
    icon: XCircle,
  },
  idle: {
    label: "Idle",
    text: "text-muted-foreground",
    pillBg: "bg-surface-highest",
    dot: "bg-muted-foreground/60",
    icon: MinusCircle,
  },
};

/** Uppercase, letter-spaced eyebrow label — matches the wizard's field labels. */
export function Eyebrow({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <span
      className={cn(
        "block text-xs font-ui font-medium uppercase tracking-wider text-muted-foreground",
        className,
      )}
    >
      {children}
    </span>
  );
}

/** Brand mark + label row item (uses the Databricks product SVGs). */
export function BrandMark({ src, label }: { src: string; label: string }) {
  return (
    <span className="inline-flex items-center gap-2 text-sm font-medium">
      <img src={src} alt="" className="h-5 w-5 shrink-0" />
      {label}
    </span>
  );
}

/** "BUILT ON" style product strip. */
export function BuiltOnRow({ items }: { items: { src: string; label: string }[] }) {
  return (
    <div className="flex flex-wrap items-center gap-x-5 gap-y-2">
      <span className="border-r border-border pr-4 text-[10px] font-ui font-semibold uppercase tracking-[0.08em] text-muted-foreground">
        Built on
      </span>
      {items.map((it) => (
        <BrandMark key={it.label} src={it.src} label={it.label} />
      ))}
    </div>
  );
}

/** A small colored status dot. */
export function HealthDot({ status, className }: { status: HealthStatus; className?: string }) {
  const meta = HEALTH_META[status];
  return (
    <span
      className={cn(
        "h-2.5 w-2.5 shrink-0 rounded-full",
        meta.dot,
        status === "healthy" && "animate-pulse",
        className,
      )}
      aria-hidden="true"
    />
  );
}

/** Icon + label status pill (green / amber / red / grey). */
export function HealthPill({ status }: { status: HealthStatus }) {
  const meta = HEALTH_META[status];
  const Icon = meta.icon;
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 rounded-md px-2 py-0.5 text-[11px] font-ui font-semibold",
        meta.pillBg,
        meta.text,
      )}
    >
      <Icon className="h-3 w-3" />
      {meta.label}
    </span>
  );
}

/** One resource row: colored dot + name + detail + optional timestamp + pill. */
export function ResourceRow({ item }: { item: ResourceItem }) {
  const isDegraded = item.status === "down" || item.status === "warning";
  return (
    <div
      className={cn(
        "flex items-start gap-3 rounded-md px-2 py-2 transition-colors",
        isDegraded ? "bg-destructive/[0.04]" : "hover:bg-surface-high/60",
      )}
    >
      <HealthDot status={item.status} className="mt-1.5" />
      <div className="min-w-0 flex-1">
        <div className="flex flex-wrap items-center justify-between gap-x-3 gap-y-1">
          <span
            title={item.label}
            className={cn(
              "break-all text-sm font-medium text-foreground",
              item.mono && "font-mono",
            )}
          >
            {item.label}
          </span>
          <div className="flex items-center gap-2">
            {item.timestamp && (
              <span
                className={cn(
                  "text-[11px] font-ui tabular-nums",
                  isDegraded ? "text-destructive/80" : "text-muted-foreground",
                )}
              >
                {item.timestamp}
              </span>
            )}
            <HealthPill status={item.status} />
          </div>
        </div>
        {item.detail && (
          <p className="mt-0.5 break-all text-xs text-muted-foreground">{item.detail}</p>
        )}
      </div>
    </div>
  );
}

/** A grouped resource-health card: title (+ brand), rollup pill, rows, note. */
export function ResourceGroupCard({
  group,
  rollupStatus,
}: {
  group: ResourceGroup;
  rollupStatus: HealthStatus;
}) {
  return (
    <div className="glass min-w-0 overflow-hidden rounded-lg p-5">
      <div className="mb-3 flex items-center justify-between gap-3">
        <div className="flex min-w-0 items-center gap-2.5">
          {group.brand ? (
            <img src={group.brand.src} alt="" className="h-5 w-5 shrink-0" />
          ) : (
            <HealthDot status={rollupStatus} />
          )}
          <h3 className="truncate font-display text-sm font-semibold">{group.title}</h3>
        </div>
        <HealthPill status={rollupStatus} />
      </div>
      <div className="divide-y divide-border/30">
        {group.items.map((item) => (
          <ResourceRow key={item.label} item={item} />
        ))}
      </div>
      {group.note && (
        <p className="mt-3 rounded-md border border-border/50 bg-surface-high px-3 py-2 text-[11px] leading-relaxed text-muted-foreground">
          {group.note}
        </p>
      )}
    </div>
  );
}
