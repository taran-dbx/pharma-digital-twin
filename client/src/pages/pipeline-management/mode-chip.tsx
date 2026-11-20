/**
 * Small enabled-mode chip (Real-time / Micro-batch / Zerobus). Matches the
 * monospace mode chips on the home industry cards and the wizard's derived
 * chips — neutral surface, uppercase-adjacent mono label.
 */
import { cn } from "@/lib/utils";
import { MODE_LABEL, type PipelineMode } from "./types";

export function ModeChip({ mode }: { mode: PipelineMode }) {
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-md border border-border/50 bg-surface-high",
        "px-2 py-0.5 font-mono text-[11px] font-medium text-muted-foreground",
      )}
    >
      {MODE_LABEL[mode]}
    </span>
  );
}
