/**
 * Step 1 — Flavor & industry (spec §1.2).
 * Classic (selected) vs Serverless (disabled "coming soon"); zerobus is always
 * serverless. Industry grid enforces one-per-industry: an industry that already
 * has a setup is greyed "already configured" and is non-selectable for CREATE —
 * clicking it routes to Edit instead. The set of already-configured industries
 * comes from the backend (`GET /api/admin/pipelines`); the static `exists` flag
 * on `INDUSTRIES` is only a fallback when that list hasn't resolved.
 *
 * Also hosts the pipeline-mode multi-select (realtime / micro-batch / zerobus).
 * When only zerobus is selected the flavor toggle is irrelevant and is
 * visually greyed out with a note.
 */
import { Check, CircleCheck, Cpu, Cloud } from "lucide-react";
import { cn } from "@/lib/utils";
import { Eyebrow, OptionCard } from "./components";
import { INDUSTRIES, type Flavor, type IndustryOption } from "./types";
import type { ModeConfigs } from "@/lib/admin-api";
import type { PipelineMode } from "@/lib/industries-api";

interface IndustryTileProps {
  industry: IndustryOption;
  /** Already has a setup — non-selectable for CREATE, routes to Edit. */
  exists: boolean;
  selected: boolean;
  onSelect: () => void;
  onEdit: () => void;
}

function IndustryTile({ industry, exists, selected, onSelect, onEdit }: IndustryTileProps) {
  const { icon: Icon } = industry;
  return (
    <button
      type="button"
      onClick={exists ? onEdit : onSelect}
      title={exists ? `${industry.name} already has a setup — click to edit it` : undefined}
      className={cn(
        "relative flex items-start gap-2.5 rounded-lg border p-3 text-left transition-all",
        exists
          ? "cursor-pointer border-border/50 bg-surface-high opacity-50 hover:opacity-80"
          : selected
            ? "border-primary bg-primary/[0.04] shadow-[0_0_0_1px_hsl(var(--primary))]"
            : "border-border/50 bg-surface-high hover:border-primary/40",
      )}
    >
      <Icon
        className={cn(
          "mt-0.5 h-4 w-4 shrink-0",
          selected && !exists ? "text-primary" : "text-muted-foreground",
        )}
      />
      <div className="min-w-0">
        <span className="block truncate text-sm font-medium">{industry.name}</span>
        <span className="block text-[11px] text-muted-foreground">
          {exists ? "already configured · edit" : `${industry.sensors} sensors`}
        </span>
      </div>
      {selected && !exists && (
        <CircleCheck className="absolute right-2 top-2 h-4 w-4 text-primary" fill="currentColor" stroke="hsl(var(--card))" />
      )}
    </button>
  );
}

const MODE_OPTIONS: { value: PipelineMode; label: string; sub: string }[] = [
  { value: "realtime", label: "Real-time", sub: "Spark consumer · Lakebase + LakeTS" },
  { value: "microbatch", label: "Micro-batch", sub: "Bounded consumer · Delta (UC)" },
  { value: "zerobus", label: "Zerobus", sub: "Serverless producer · Delta Ingest" },
];

interface StepProps {
  flavor: Flavor;
  onFlavorChange: (f: Flavor) => void;
  industryId: string;
  onIndustryChange: (id: string) => void;
  /** Selected pipeline modes — at least one must be checked. */
  enabledModes: PipelineMode[];
  onEnabledModesChange: (modes: PipelineMode[]) => void;
  modeConfigs: ModeConfigs;
  onModeRateChange: (mode: PipelineMode, rateHz: number) => void;
  lockedFrequencyModes: Set<PipelineMode>;
  /** Industries that already have a setup (from the backend). `null` = not yet
   * resolved, in which case the static `exists` flag is used as a fallback. */
  existingIds: Set<string> | null;
  /** Navigate to edit an already-configured industry. */
  onEditExisting: (id: string) => void;
}

export function StepFlavorIndustry({
  flavor,
  onFlavorChange,
  industryId,
  onIndustryChange,
  enabledModes,
  onEnabledModesChange,
  modeConfigs,
  onModeRateChange,
  lockedFrequencyModes,
  existingIds,
  onEditExisting,
}: StepProps) {
  const industryExists = (ind: IndustryOption): boolean =>
    existingIds ? existingIds.has(ind.id) : !!ind.exists;

  const zerobusOnly = enabledModes.length === 1 && enabledModes[0] === "zerobus";

  function toggleMode(mode: PipelineMode) {
    const next = enabledModes.includes(mode)
      ? enabledModes.filter((m) => m !== mode)
      : [...enabledModes, mode];
    // Require at least one mode.
    if (next.length > 0) onEnabledModesChange(next as PipelineMode[]);
  }

  return (
    <div className="space-y-6">
      {/* ── Pipeline modes ─────────────────────────────────────────── */}
      <div className="glass space-y-3 rounded-lg p-5">
        <Eyebrow>Pipeline Modes</Eyebrow>
        <div className="grid gap-2.5 sm:grid-cols-3">
          {MODE_OPTIONS.map(({ value, label, sub }) => {
            const checked = enabledModes.includes(value);
            const isLast = checked && enabledModes.length === 1;
            return (
              <button
                key={value}
                type="button"
                onClick={() => toggleMode(value)}
                disabled={isLast}
                title={isLast ? "At least one mode must be selected" : undefined}
                className={cn(
                  "flex items-center gap-3 rounded-lg border px-3 py-2.5 text-left transition-all",
                  checked
                    ? "border-primary bg-primary/[0.04] shadow-[0_0_0_1px_hsl(var(--primary))]"
                    : "border-border/50 bg-surface-high hover:border-primary/40",
                  isLast && "cursor-not-allowed opacity-60",
                )}
              >
                <span
                  className={cn(
                    "flex h-4 w-4 shrink-0 items-center justify-center rounded border transition-colors",
                    checked
                      ? "border-primary bg-primary"
                      : "border-border bg-transparent",
                  )}
                >
                  {checked && (
                    <Check className="h-2.5 w-2.5 text-primary-foreground" />
                  )}
                </span>
                <span className="min-w-0">
                  <span className="block text-sm font-medium">{label}</span>
                  <span className="block truncate text-[11px] text-muted-foreground">{sub}</span>
                </span>
              </button>
            );
          })}
        </div>
        <p className="text-xs text-muted-foreground">
          Select the streaming modes to enable for this pipeline. At least one mode is required.
        </p>
        <div className="grid gap-3 border-t border-border/50 pt-3 sm:grid-cols-3">
          {MODE_OPTIONS.filter(({ value }) => enabledModes.includes(value)).map(
            ({ value, label }) => {
              const rateHz = modeConfigs[value]?.rate_hz ?? 2000;
              const locked = lockedFrequencyModes.has(value);
              return (
                <label key={value} className="space-y-1.5 text-xs text-muted-foreground">
                  <span className="flex items-center justify-between gap-2">
                    <span>{label} data frequency</span>
                    <strong className="font-mono text-foreground">
                      {rateHz.toLocaleString()} rec/s
                    </strong>
                  </span>
                  <input
                    type="range"
                    aria-label={`${label} data frequency`}
                    min={100}
                    max={5000}
                    step={100}
                    value={rateHz}
                    disabled={locked}
                    onChange={(event) => onModeRateChange(value, Number(event.target.value))}
                    className="w-full accent-primary disabled:cursor-not-allowed disabled:opacity-50"
                  />
                  {locked && (
                    <span className="block text-[10px] text-muted-foreground">
                      Stop this mode before changing its frequency
                    </span>
                  )}
                </label>
              );
            },
          )}
        </div>
      </div>

      {/* ── Flavor ─────────────────────────────────────────────────── */}
      <div className={cn("glass space-y-3 rounded-lg p-5", zerobusOnly && "opacity-50")}>
        <Eyebrow>Pipeline Flavor</Eyebrow>
        {zerobusOnly && (
          <p className="text-xs text-muted-foreground">
            Flavor only applies when <b className="font-medium text-foreground">realtime</b> or{" "}
            <b className="font-medium text-foreground">microbatch</b> is selected. Zerobus is always
            serverless.
          </p>
        )}
        <div className={cn("grid gap-3 sm:grid-cols-2", zerobusOnly && "pointer-events-none")}>
          <OptionCard
            title="Classic"
            sub="Interactive / job cluster"
            desc="Spark consumer on a cluster. Validated on an interactive harness that proxies the deploy-time job cluster."
            icon={Cpu}
            selected={flavor === "classic"}
            onSelect={() => !zerobusOnly && onFlavorChange("classic")}
          />
          <OptionCard
            title="Serverless"
            sub="Coming soon"
            desc="Serverless Spark consumer. Deferred — Classic only is built today."
            icon={Cloud}
            selected={false}
            disabled
            badge="Soon"
            onSelect={() => {}}
          />
        </div>
        {!zerobusOnly && (
          <p className="text-xs text-muted-foreground">
            Flavor applies to <b className="font-medium text-foreground">realtime</b> +{" "}
            <b className="font-medium text-foreground">microbatch</b>. Zerobus is always serverless
            (producer-only) — no flavor toggle.
          </p>
        )}
      </div>

      <div className="glass space-y-4 rounded-lg p-5">
        <div className="flex items-center gap-2">
          <Eyebrow className="inline">Select Industry</Eyebrow>
          <Check className="h-3.5 w-3.5 text-success" />
        </div>
        <div className="grid gap-2.5 sm:grid-cols-2 lg:grid-cols-4">
          {INDUSTRIES.map((ind) => (
            <IndustryTile
              key={ind.id}
              industry={ind}
              exists={industryExists(ind)}
              selected={industryId === ind.id}
              onSelect={() => onIndustryChange(ind.id)}
              onEdit={() => onEditExisting(ind.id)}
            />
          ))}
        </div>
        <p className="text-xs text-muted-foreground">
          One pipeline setup per industry. Industries with an existing setup are locked for new
          pipelines — click them to edit their configuration instead.
        </p>
      </div>
    </div>
  );
}
