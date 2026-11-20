import { useState } from "react";
import {
  Loader2,
  CheckCircle2,
  XCircle,
  AlertTriangle,
  MinusCircle,
  ChevronDown,
  ChevronRight,
} from "lucide-react";
import { cn } from "@/lib/utils";
import type {
  SettingsSectionResult,
  SettingsCheckResult,
  ProvisionedResource,
} from "@/lib/types";

// ---------------------------------------------------------------------------
// Form field components
// ---------------------------------------------------------------------------
export function InputField({
  label,
  placeholder,
  value,
  onChange,
  disabled,
  description,
}: {
  label: string;
  placeholder: string;
  value: string;
  onChange: (v: string) => void;
  disabled?: boolean;
  description?: string;
}) {
  return (
    <div>
      <label className="mb-1.5 block font-ui text-xs font-medium uppercase tracking-wider text-muted-foreground">
        {label}
      </label>
      <input
        type="text"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        disabled={disabled}
        className="w-full rounded-md border border-border/50 bg-input px-3 py-2 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-primary focus:border-primary disabled:cursor-not-allowed disabled:opacity-50"
      />
      {description && (
        <p className="mt-1 text-xs text-muted-foreground">{description}</p>
      )}
    </div>
  );
}

export function NumberField({
  label,
  value,
  onChange,
  min,
  max,
}: {
  label: string;
  value: number;
  onChange: (v: number) => void;
  min?: number;
  max?: number;
}) {
  return (
    <div>
      <label className="mb-1.5 block font-ui text-xs font-medium uppercase tracking-wider text-muted-foreground">
        {label}
      </label>
      <input
        type="number"
        value={value}
        onChange={(e) => onChange(Number(e.target.value))}
        min={min}
        max={max}
        className="w-full rounded-md border border-border/50 bg-input px-3 py-2 text-sm focus:outline-none focus:ring-1 focus:ring-primary focus:border-primary"
      />
    </div>
  );
}

export function SelectField({
  label,
  value,
  onChange,
  options,
  loading,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  options: Array<{ value: string; label: string }>;
  loading?: boolean;
}) {
  return (
    <div>
      <label className="mb-1.5 block font-ui text-xs font-medium uppercase tracking-wider text-muted-foreground">
        {label}
      </label>
      {loading ? (
        <div className="flex h-[38px] items-center gap-2 rounded-md border border-border/50 bg-input px-3 text-sm text-muted-foreground">
          <Loader2 className="h-3.5 w-3.5 animate-spin" />
          Loading...
        </div>
      ) : (
        <select
          value={value}
          onChange={(e) => onChange(e.target.value)}
          className="w-full rounded-md border border-border/50 bg-input px-3 py-2 text-sm focus:outline-none focus:ring-1 focus:ring-primary focus:border-primary"
        >
          <option value="">— Select —</option>
          {options.map((o) => (
            <option key={o.value} value={o.value}>
              {o.label}
            </option>
          ))}
        </select>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Status icons
// ---------------------------------------------------------------------------
export const STATUS_ICONS = {
  pass: <CheckCircle2 className="h-4 w-4 text-success" />,
  fail: <XCircle className="h-4 w-4 text-destructive" />,
  warn: <AlertTriangle className="h-4 w-4 text-yellow-500" />,
  skip: <MinusCircle className="h-4 w-4 text-muted-foreground" />,
} as const;

// ---------------------------------------------------------------------------
// Validation section panel
// ---------------------------------------------------------------------------
export function ValidationSection({
  label,
  icon: Icon,
  result,
}: {
  label: string;
  icon: React.ComponentType<{ className?: string }>;
  result: SettingsSectionResult;
}) {
  const [expanded, setExpanded] = useState(result.status !== "pass");

  return (
    <div className="rounded-md border border-border/50 glass">
      <button
        onClick={() => setExpanded((p) => !p)}
        className="flex w-full items-center gap-3 px-4 py-2.5 text-sm font-ui font-medium hover:bg-muted/40 transition-colors"
      >
        {STATUS_ICONS[result.status]}
        <Icon className="h-4 w-4 text-muted-foreground" />
        <span className="flex-1 text-left font-display text-sm font-semibold">{label}</span>
        {expanded ? (
          <ChevronDown className="h-4 w-4 text-muted-foreground" />
        ) : (
          <ChevronRight className="h-4 w-4 text-muted-foreground" />
        )}
      </button>
      {expanded && (
        <div className="border-t border-border px-4 py-2 space-y-1.5">
          {result.checks.map((check: SettingsCheckResult) => (
            <div key={check.name} className="flex items-start gap-2 text-xs">
              {STATUS_ICONS[check.status]}
              <span className="font-medium min-w-[140px]">{check.name}</span>
              <span className="text-muted-foreground">{check.detail}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Provision result badges
// ---------------------------------------------------------------------------
const PROVISION_BADGE_STYLES: Record<string, string> = {
  created: "bg-success/15 text-success border-success/20",
  already_exists: "bg-blue-500/10 text-blue-600 border-blue-500/20",
  skipped: "bg-yellow-500/10 text-yellow-600 border-yellow-500/20",
  failed: "bg-destructive/15 text-destructive border-destructive/20",
};

export function ProvisionResultList({ items }: { items: ProvisionedResource[] }) {
  return (
    <div className="space-y-1.5">
      {items.map((item, i) => (
        <div key={i} className="flex items-center gap-2 text-xs">
          <span
            className={cn(
              "rounded-full border px-2 py-0.5 text-[10px] font-medium",
              PROVISION_BADGE_STYLES[item.status],
            )}
          >
            {item.status === "already_exists" ? "exists" : item.status}
          </span>
          <span className="font-medium">{item.resource}</span>
          {item.error && (
            <span className={cn(
              "truncate max-w-[300px]",
              item.status === "skipped" ? "text-yellow-600" : "text-red-500",
            )}>{item.error}</span>
          )}
        </div>
      ))}
    </div>
  );
}
