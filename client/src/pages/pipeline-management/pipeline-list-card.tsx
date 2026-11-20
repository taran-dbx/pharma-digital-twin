/**
 * One deployed-pipeline row on the management list. Left: industry identity
 * (icon + name + tagline + enabled-mode chips). Right: overall health pill.
 * Middle: quick stats (jobs / resources / last updated) with colour-coded but
 * always-labelled status dots. Footer: View health / Edit config / Start·Stop
 * / Delete actions. Styled with the feature's glass-card + surface tokens.
 */
import { Link } from "react-router-dom";
import {
  Activity,
  Boxes,
  Clock,
  Play,
  Square,
  Trash2,
  ArrowRight,
  SlidersHorizontal,
} from "lucide-react";

import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

import { HealthPill } from "./health-pill";
import { ModeChip } from "./mode-chip";
import type { DeployedPipeline, Health } from "./types";

/** Dot colour keyed off health so stats echo the roll-up state. */
const DOT_CLASS: Record<Health, string> = {
  healthy: "bg-success",
  degraded: "bg-warning",
  down: "bg-muted-foreground/50",
  draft: "bg-muted-foreground/40",
};

function Stat({
  icon: Icon,
  value,
  label,
  dot,
}: {
  icon: typeof Activity;
  value: string;
  label: string;
  dot?: string;
}) {
  return (
    <div className="flex items-center gap-2.5">
      <Icon className="h-4 w-4 shrink-0 text-muted-foreground" />
      <div className="min-w-0">
        <div className="flex items-center gap-1.5">
          {dot && <span className={cn("h-2 w-2 shrink-0 rounded-full", dot)} />}
          <span className="text-sm font-semibold tabular-nums text-foreground">{value}</span>
        </div>
        <span className="block text-[10px] font-ui font-medium uppercase tracking-wider text-muted-foreground">
          {label}
        </span>
      </div>
    </div>
  );
}

interface PipelineListCardProps {
  pipeline: DeployedPipeline;
  onToggleRunning: (id: string) => void;
  onDelete: (id: string) => void;
}

export function PipelineListCard({ pipeline, onToggleRunning, onDelete }: PipelineListCardProps) {
  const { icon: Icon } = pipeline;
  const isRunning = pipeline.jobsTotal > 0 && pipeline.jobsRunning === pipeline.jobsTotal;
  const dot = DOT_CLASS[pipeline.health];
  // A draft / not-yet-provisioned pipeline can't be started — only edited.
  const startDisabled = !isRunning && !pipeline.startable;

  return (
    <div
      className={cn(
        "glass rounded-lg border p-5 transition-all",
        pipeline.health === "down"
          ? "border-border/50"
          : pipeline.health === "degraded"
            ? "border-warning/30"
            : "border-border/50 hover:border-primary/30",
      )}
    >
      {/* Identity + health */}
      <div className="flex items-start justify-between gap-4">
        <div className="flex min-w-0 items-start gap-3">
          <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-lg bg-surface-high">
            <Icon className="h-5 w-5 text-primary" />
          </div>
          <div className="min-w-0">
            <h3 className="truncate font-display text-base font-semibold tracking-tight">
              {pipeline.industry}
            </h3>
            <p className="mt-0.5 truncate text-xs text-muted-foreground">{pipeline.tagline}</p>
            <div className="mt-2 flex flex-wrap gap-1.5">
              {pipeline.modes.map((mode) => (
                <ModeChip key={mode} mode={mode} />
              ))}
            </div>
          </div>
        </div>
        <HealthPill health={pipeline.health} />
      </div>

      {/* Optional degraded / down note */}
      {pipeline.note && (
        <p
          className={cn(
            "mt-4 rounded-md border px-3 py-2 text-xs",
            pipeline.health === "down"
              ? "border-destructive/30 bg-destructive/10 text-destructive"
              : "border-warning/30 bg-warning/10 text-warning",
          )}
        >
          {pipeline.note}
        </p>
      )}

      {/* Quick stats */}
      <div className="mt-4 grid grid-cols-1 gap-3 border-t border-border/50 pt-4 sm:grid-cols-3">
        <Stat
          icon={Activity}
          value={`${pipeline.jobsRunning}/${pipeline.jobsTotal} running`}
          label="Jobs"
          dot={dot}
        />
        <Stat
          icon={Boxes}
          value={`${pipeline.resourcesHealthy}/${pipeline.resourcesTotal} healthy`}
          label="Resources"
          dot={dot}
        />
        <Stat icon={Clock} value={pipeline.lastUpdated} label="Last updated" />
      </div>

      {/* Actions */}
      <div className="mt-4 flex flex-wrap items-center gap-2">
        <Button asChild size="sm" className="gap-1.5 hover:glow-primary transition-all">
          <Link to={`/pipelines/${pipeline.id}/health`}>
            <Activity className="h-4 w-4" />
            View health
            <ArrowRight className="h-4 w-4" />
          </Link>
        </Button>
        <Button asChild size="sm" variant="outline" className="gap-1.5">
          <Link
            to={
              pipeline.industryId
                ? `/setup-validate?industry=${pipeline.industryId}`
                : "/setup-validate"
            }
          >
            <SlidersHorizontal className="h-4 w-4" />
            Edit config
          </Link>
        </Button>
        <Button
          size="sm"
          variant="outline"
          className="gap-1.5"
          disabled={startDisabled}
          title={
            startDisabled ? "Complete setup & validation before this pipeline can start" : undefined
          }
          onClick={() => onToggleRunning(pipeline.id)}
        >
          {isRunning ? (
            <>
              <Square className="h-4 w-4" />
              Stop
            </>
          ) : (
            <>
              <Play className="h-4 w-4" />
              Start
            </>
          )}
        </Button>
        <Button
          size="sm"
          variant="ghost"
          className="ml-auto gap-1.5 text-muted-foreground hover:text-destructive"
          onClick={() => onDelete(pipeline.id)}
        >
          <Trash2 className="h-4 w-4" />
          Delete
        </Button>
      </div>
    </div>
  );
}
