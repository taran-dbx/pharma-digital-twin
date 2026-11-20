/**
 * Shared presentational primitives for the setup-validation flow, styled to
 * match the existing pipeline wizard (glass cards, uppercase eyebrow labels,
 * red-primary accents, success/warning semantics) so the new page is a pixel
 * match for the rest of the app in both light and dark themes.
 */
import type { ReactNode } from "react";
import type { LucideIcon } from "lucide-react";
import {
  CheckCircle2,
  Circle,
  Loader2,
  Lock,
  AlertCircle,
  Clock,
  Lightbulb,
  RotateCcw,
} from "lucide-react";
import { cn } from "@/lib/utils";
import type { StepStatus } from "./types";

/**
 * Selectable / disabled option card — the shared primitive behind both the
 * Step 1 flavor cards (Classic / Serverless) and the Step 2 source cards
 * (Kafka / Kinesis), so a "coming soon" option looks identical everywhere:
 * a "SOON" pill top-right, an uppercase sub, dimmed + non-interactive.
 */
export function OptionCard({
  title,
  sub,
  desc,
  icon: Icon,
  selected,
  disabled,
  badge,
  onSelect,
}: {
  title: string;
  sub: string;
  desc: string;
  icon: LucideIcon;
  selected: boolean;
  disabled?: boolean;
  badge?: string;
  onSelect: () => void;
}) {
  return (
    <button
      type="button"
      disabled={disabled}
      onClick={onSelect}
      className={cn(
        "flex flex-col rounded-lg border p-4 text-left transition-all",
        selected
          ? "border-primary bg-primary/[0.04] shadow-[0_0_0_1px_hsl(var(--primary))]"
          : "border-border/50 bg-surface-high hover:border-primary/40",
        disabled && "cursor-not-allowed opacity-55 hover:border-border/50",
      )}
    >
      <div className="mb-2 flex items-start justify-between">
        <Icon className={cn("h-5 w-5", selected ? "text-primary" : "text-muted-foreground")} />
        {selected && <CheckCircle2 className="h-4 w-4 text-primary" />}
        {badge && (
          <span className="rounded-full bg-surface-highest px-2 py-0.5 text-[10px] font-ui font-semibold uppercase tracking-wider text-muted-foreground">
            {badge}
          </span>
        )}
      </div>
      <span className="text-sm font-semibold">{title}</span>
      <span className="mt-0.5 text-[11px] font-ui font-medium uppercase tracking-wider text-muted-foreground">
        {sub}
      </span>
      <p className="mt-2 text-xs leading-relaxed text-muted-foreground">{desc}</p>
    </button>
  );
}

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

/** A locked, auto-derived chip (read-only source facts). */
export function LockedChip({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between gap-3 rounded-md border border-border/50 bg-surface-high px-3 py-2">
      <div className="min-w-0">
        <span className="block text-[10px] font-ui font-medium uppercase tracking-wider text-muted-foreground">
          {label}
        </span>
        <span className="block truncate font-mono text-xs text-foreground">{value}</span>
      </div>
      <Lock className="h-3.5 w-3.5 shrink-0 text-muted-foreground/60" />
    </div>
  );
}

/** Brand mark + label row item (uses the Databricks product SVGs). */
export function BrandMark({ src, label, className }: { src: string; label: string; className?: string }) {
  return (
    <span className={cn("inline-flex items-center gap-2 text-sm font-medium", className)}>
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

const STATUS_META: Record<
  StepStatus,
  { label: string; className: string; icon: typeof Circle; spin?: boolean }
> = {
  pending: { label: "Pending", className: "text-muted-foreground/60", icon: Circle },
  running: { label: "Running", className: "text-primary", icon: Loader2, spin: true },
  passed: { label: "Passed", className: "text-success", icon: CheckCircle2 },
  failed: { label: "Failed", className: "text-destructive", icon: AlertCircle },
  waiting_admin: { label: "Waiting", className: "text-warning", icon: Clock },
};

/** Small status pill for a validation step. */
export function StatusPill({ status }: { status: StepStatus }) {
  const meta = STATUS_META[status];
  const bg =
    status === "passed"
      ? "bg-success/15"
      : status === "waiting_admin"
        ? "bg-warning/15"
        : status === "failed"
          ? "bg-destructive/15"
          : status === "running"
            ? "bg-primary/10"
            : "bg-surface-high";
  const Icon = meta.icon;
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 rounded-md px-2 py-0.5 text-[11px] font-ui font-semibold",
        bg,
        meta.className,
      )}
    >
      <Icon className={cn("h-3 w-3", meta.spin && "animate-spin")} />
      {meta.label}
    </span>
  );
}

/** A single validation-lane row: icon + label + detail + status pill.
 * A `failed` row surfaces a Retry affordance that re-runs the step. */
export function ValidationRow({
  stepId,
  label,
  detail,
  failureReason,
  status,
  active,
  onRetry,
}: {
  stepId: string;
  label: string;
  detail?: string;
  failureReason?: string;
  status: StepStatus;
  /** Highlight this row as the currently-executing step. */
  active?: boolean;
  onRetry?: (id: string) => void;
}) {
  const meta = STATUS_META[status];
  const Icon = meta.icon;
  return (
    <div
      className={cn(
        "flex items-start gap-3 py-2",
        active && "-mx-2 rounded-md bg-primary/[0.06] px-2 ring-1 ring-primary/25",
      )}
    >
      <Icon className={cn("mt-0.5 h-4 w-4 shrink-0", meta.className, meta.spin && "animate-spin")} />
      <div className="min-w-0 flex-1">
        <div className="flex items-center justify-between gap-2">
          <span
            className={cn(
              "text-sm font-medium",
              status === "pending" ? "text-muted-foreground/70" : "text-foreground",
            )}
          >
            {label}
          </span>
          <div className="flex items-center gap-2">
            {status === "failed" && onRetry && (
              <button
                type="button"
                onClick={() => onRetry(stepId)}
                aria-label={`Retry ${label}`}
                className="inline-flex items-center gap-1 rounded-md border border-destructive/40 bg-destructive/10 px-2 py-0.5 text-[11px] font-ui font-semibold text-destructive transition-colors hover:bg-destructive/20"
              >
                <RotateCcw className="h-3 w-3" />
                Retry
              </button>
            )}
            <StatusPill status={status} />
          </div>
        </div>
        {detail && <p className="mt-0.5 text-xs text-muted-foreground">{detail}</p>}
        {status === "failed" && failureReason && (
          <p
            role="alert"
            title={failureReason}
            className="mt-2 max-h-24 overflow-auto break-words rounded-md border border-destructive/30 bg-destructive/[0.06] px-2.5 py-2 text-xs leading-relaxed text-destructive"
          >
            <span className="font-semibold">Failure reason:</span> {failureReason}
          </p>
        )}
      </div>
    </div>
  );
}

/** The red-bordered "Architect's Tip" callout used across wizard steps. */
export function ArchitectTip({ children }: { children: ReactNode }) {
  return (
    <div className="glass space-y-1.5 rounded-lg border border-primary/30 p-3">
      <div className="flex items-center gap-1.5">
        <Lightbulb className="h-3.5 w-3.5 shrink-0 text-primary" />
        <span className="text-[10px] font-ui font-semibold uppercase tracking-wider text-primary">
          Architect&apos;s Tip
        </span>
      </div>
      <p className="text-xs leading-relaxed text-muted-foreground">{children}</p>
    </div>
  );
}

/** Key/value row for the summary + preview panels. */
export function KvRow({ k, v, mono }: { k: string; v: ReactNode; mono?: boolean }) {
  return (
    <div className="flex items-center justify-between gap-3">
      <span className="text-xs font-ui text-muted-foreground">{k}</span>
      <span className={cn("text-xs font-medium", mono ? "font-mono" : "capitalize")}>{v}</span>
    </div>
  );
}
