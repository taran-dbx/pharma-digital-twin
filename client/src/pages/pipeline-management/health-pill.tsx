/**
 * Overall-health pill for a deployed pipeline. Colour-coded but always
 * icon + text (never colour alone) so state reads in any theme and for
 * colour-blind users. Uses the same success/warning/destructive tokens as
 * the setup-validation StatusPill for a pixel match across the feature.
 */
import { CheckCircle2, AlertTriangle, OctagonX, PencilRuler } from "lucide-react";
import { cn } from "@/lib/utils";
import type { Health } from "./types";

const HEALTH_META: Record<
  Health,
  { label: string; icon: typeof CheckCircle2; className: string }
> = {
  healthy: {
    label: "Healthy",
    icon: CheckCircle2,
    className: "bg-success/15 text-success",
  },
  degraded: {
    label: "Degraded",
    icon: AlertTriangle,
    className: "bg-warning/15 text-warning",
  },
  down: {
    label: "Down",
    icon: OctagonX,
    className: "bg-destructive/15 text-destructive",
  },
  draft: {
    label: "Draft",
    icon: PencilRuler,
    className: "bg-muted/60 text-muted-foreground",
  },
};

export function HealthPill({ health }: { health: Health }) {
  const meta = HEALTH_META[health];
  const Icon = meta.icon;
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 rounded-md px-2.5 py-1 text-xs font-ui font-semibold",
        meta.className,
      )}
    >
      <Icon className="h-3.5 w-3.5" />
      {meta.label}
    </span>
  );
}
