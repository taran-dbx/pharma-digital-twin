import { Link } from "react-router-dom";
import { formatDistanceToNow } from "date-fns";
import { ArrowRight, Trash2 } from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import type { Pipeline, PipelineStatus } from "@/lib/types";

export interface PipelineCardProps {
  pipeline: Pipeline;
  onDelete: (id: string) => void;
}

const statusVariant: Record<PipelineStatus, "default" | "secondary" | "success" | "warning" | "destructive"> = {
  draft: "secondary",
  step1_complete: "warning",
  step2_complete: "warning",
  step3_complete: "warning",
  active: "success",
  error: "destructive",
  deleted: "secondary",
};

const statusLabel: Record<PipelineStatus, string> = {
  draft: "Draft",
  step1_complete: "Step 1 Done",
  step2_complete: "Step 2 Done",
  step3_complete: "Step 3 Done",
  active: "Active",
  error: "Error",
  deleted: "Deleted",
};

function pipelineHref(p: Pipeline): string {
  // Resume at the NEXT unconfigured step, not the one just completed.
  // The 4-step wizard is Data Hub(0) → Pipeline Config(1) → Sink Config(2) → Deploy & Monitor(3).
  // Once a pipeline is active, the dashboard is the natural landing spot.
  switch (p.status) {
    case "active":
      return `/pipelines/${p.id}/dashboard`;
    case "step3_complete":
      return `/pipelines/new?id=${p.id}&step=3`;
    case "step2_complete":
      return `/pipelines/new?id=${p.id}&step=3`;
    case "step1_complete":
      return `/pipelines/new?id=${p.id}&step=2`;
    case "draft":
      return `/pipelines/new?id=${p.id}&step=0`;
    default:
      return `/pipelines/${p.id}`;
  }
}

/** Deterministic sparkline points derived from the pipeline name string. */
function buildSparklinePath(name: string): string {
  // Hash the name into a seed for stable pseudo-random heights
  let seed = 0;
  for (let i = 0; i < name.length; i++) {
    seed = (seed * 31 + name.charCodeAt(i)) & 0xffffffff;
  }

  const WIDTH = 240;
  const HEIGHT = 40;
  const POINTS = 8;
  const heights: number[] = [];

  for (let i = 0; i < POINTS; i++) {
    // LCG-style step so each point differs
    seed = (seed * 1664525 + 1013904223) & 0xffffffff;
    const normalized = (seed >>> 0) / 0xffffffff; // 0..1
    heights.push(normalized);
  }

  // Build smooth bezier path using cardinal spline control points
  const xs = Array.from({ length: POINTS }, (_, i) => (i / (POINTS - 1)) * WIDTH);
  const ys = heights.map((h) => HEIGHT - 4 - h * (HEIGHT - 10));

  let d = `M${xs[0]},${ys[0]}`;
  for (let i = 0; i < POINTS - 1; i++) {
    const cpx = (xs[i] + xs[i + 1]) / 2;
    d += ` C${cpx},${ys[i]} ${cpx},${ys[i + 1]} ${xs[i + 1]},${ys[i + 1]}`;
  }

  // Close area path
  const area = `${d} L${WIDTH},${HEIGHT} L0,${HEIGHT} Z`;
  return area;
}

export function PipelineCard({ pipeline, onDelete }: PipelineCardProps) {
  const created = pipeline.created_at
    ? formatDistanceToNow(new Date(pipeline.created_at), { addSuffix: true })
    : "";
  const href = pipelineHref(pipeline);
  const sparklineArea = buildSparklinePath(pipeline.name);
  // Stroke line is the area path without the closing legs — rebuild just the curve
  const sparklineStroke = ((): string => {
    let seed = 0;
    for (let i = 0; i < pipeline.name.length; i++) {
      seed = (seed * 31 + pipeline.name.charCodeAt(i)) & 0xffffffff;
    }
    const WIDTH = 240;
    const HEIGHT = 40;
    const POINTS = 8;
    const heights: number[] = [];
    for (let i = 0; i < POINTS; i++) {
      seed = (seed * 1664525 + 1013904223) & 0xffffffff;
      heights.push((seed >>> 0) / 0xffffffff);
    }
    const xs = Array.from({ length: POINTS }, (_, i) => (i / (POINTS - 1)) * WIDTH);
    const ys = heights.map((h) => HEIGHT - 4 - h * (HEIGHT - 10));
    let d = `M${xs[0]},${ys[0]}`;
    for (let i = 0; i < POINTS - 1; i++) {
      const cpx = (xs[i] + xs[i + 1]) / 2;
      d += ` C${cpx},${ys[i]} ${cpx},${ys[i + 1]} ${xs[i + 1]},${ys[i + 1]}`;
    }
    return d;
  })();

  return (
    <Card className="group glass transition-all duration-300 hover:border-primary/30 hover:shadow-[0_0_20px_rgba(255,54,33,0.1)]">
      <CardContent className="p-5">
        {/* Top row: name + badge + actions */}
        <div className="flex items-center gap-4">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2">
              <Link
                to={href}
                className="truncate text-sm font-display font-semibold hover:text-primary transition-colors"
              >
                {pipeline.name}
              </Link>
              <Badge
                variant={statusVariant[pipeline.status]}
                className="shrink-0 text-[10px]"
              >
                {statusLabel[pipeline.status]}
              </Badge>
            </div>
            <div className="mt-1 flex items-center gap-3 font-ui text-xs text-muted-foreground">
              {pipeline.industry && (
                <span className={cn("capitalize")}>
                  {pipeline.industry.replace(/_/g, " ")}
                </span>
              )}
              {created && <span>{created}</span>}
            </div>
          </div>

          <div className="flex items-center gap-1">
            <Button
              variant="ghost"
              size="icon"
              className="h-8 w-8 text-muted-foreground hover:text-destructive"
              onClick={(e) => {
                e.preventDefault();
                onDelete(pipeline.id);
              }}
              aria-label={`Delete pipeline ${pipeline.name}`}
            >
              <Trash2 className="h-4 w-4" />
            </Button>
            <Button asChild variant="ghost" size="icon" className="h-8 w-8">
              <Link
                to={href}
                aria-label={`View pipeline ${pipeline.name}`}
              >
                <ArrowRight className="h-4 w-4" />
              </Link>
            </Button>
          </div>
        </div>

        {/* Sparkline chart */}
        <div className="mt-3 overflow-hidden rounded-md bg-surface-low px-1 pt-1">
          <svg
            viewBox="0 0 240 40"
            xmlns="http://www.w3.org/2000/svg"
            className="w-full"
            aria-hidden="true"
          >
            <defs>
              <linearGradient id={`spark-fill-${pipeline.id}`} x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="hsl(var(--primary))" stopOpacity="0.15" />
                <stop offset="100%" stopColor="hsl(var(--primary))" stopOpacity="0.01" />
              </linearGradient>
            </defs>
            <path
              d={sparklineArea}
              fill={`url(#spark-fill-${pipeline.id})`}
            />
            <path
              d={sparklineStroke}
              fill="none"
              stroke="hsl(var(--success) / 0.6)"
              strokeWidth="1.2"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
        </div>
      </CardContent>
    </Card>
  );
}
