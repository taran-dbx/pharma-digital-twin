import { CirclePause, CirclePlay, LoaderCircle, Power, Sparkles } from "lucide-react";

import { clockOf } from "@/components/cell/cell-kpis";
import { PipelineChain, type PipelineNode } from "@/components/cell/platform-health";
import { RefreshIntervalSelect } from "@/components/dashboard/refresh-interval-select";
import type { CellProduction, ProductionPhase } from "@/hooks/use-production";
import { formatCountdown } from "@/lib/manufacturing-story";

const PHASE_LABEL: Record<ProductionPhase, string> = {
  running: "Running",
  stopped: "Stopped",
  restarting: "Restarting",
};

function PhaseIcon({ phase }: { phase: ProductionPhase }) {
  switch (phase) {
    case "running":
      return <CirclePlay aria-hidden="true" />;
    case "stopped":
      return <CirclePause aria-hidden="true" />;
    case "restarting":
      return <LoaderCircle aria-hidden="true" className="cellops-spin" />;
    default: {
      const _exhaustive: never = phase;
      return _exhaustive;
    }
  }
}

export interface PipelineToggle {
  label: string;
  busy: boolean;
  onToggle: () => void;
}

export function CellHeader({
  modeLabel,
  production,
  canControl,
  now,
  nodes,
  refreshSec,
  onRefreshSec,
  onRefreshNow,
  pipeline,
  onOpenGenie,
}: {
  modeLabel: string;
  production: CellProduction;
  canControl: boolean;
  now: number;
  nodes: PipelineNode[];
  refreshSec: number;
  onRefreshSec: (sec: number) => void;
  onRefreshNow: () => void;
  pipeline: PipelineToggle;
  onOpenGenie: () => void;
}) {
  const { phase, state } = production;
  const since = production.since;
  return (
    <header className={`cellops-hero is-${phase}`} data-testid="cell-header">
      <div className="cellops-hero__id">
        <span className="cellops-eyebrow">Manufacturing twin · {modeLabel}</span>
        <h1>
          CNC cell
          <span className="cellops-hero__route" aria-label="Mill, inspect, pack">
            MILL <i>→</i> INSPECT <i>→</i> PACK
          </span>
        </h1>
        <PipelineChain nodes={nodes} />
      </div>

      <div className="cellops-hero__side">
        <div className={`cellops-state is-${phase}`} data-testid="production-state" role="status">
          <PhaseIcon phase={phase} />
          <div>
            <b>Production {PHASE_LABEL[phase].toLowerCase()}</b>
            <span>
              {since === undefined
                ? "since app start"
                : `since ${clockOf(state?.changed_at)} · ${formatCountdown(now - since)}`}
              {phase === "stopped" && state?.reason ? ` · ${state.reason}` : ""}
            </span>
          </div>
        </div>
        <div className="cellops-hero__controls">
          {canControl &&
            (phase === "stopped" ? (
              <button
                type="button"
                className="btn primary"
                onClick={() => void production.resume()}
                disabled={production.pending !== null}
                data-testid="header-resume"
              >
                <CirclePlay aria-hidden="true" />
                {production.pending === "running" ? "Resuming…" : "Resume production"}
              </button>
            ) : (
              <button
                type="button"
                className="btn cellops-stop-btn"
                onClick={() => void production.stop("manual stop")}
                disabled={production.pending !== null || phase === "restarting"}
                data-testid="header-stop"
              >
                <CirclePause aria-hidden="true" />
                {production.pending === "stopped" ? "Stopping…" : "Stop production"}
              </button>
            ))}
          <button type="button" className="btn" onClick={onOpenGenie}>
            <Sparkles aria-hidden="true" />
            Ask Genie
          </button>
        </div>
        <div className="cellops-hero__ops">
          <RefreshIntervalSelect value={refreshSec} onChange={onRefreshSec} onRefreshNow={onRefreshNow} />
          <button
            type="button"
            className="btn sm ghost"
            onClick={pipeline.onToggle}
            disabled={pipeline.busy}
            title="Starts or stops the Databricks streaming job"
          >
            <Power aria-hidden="true" />
            {pipeline.label}
          </button>
        </div>
      </div>
    </header>
  );
}
