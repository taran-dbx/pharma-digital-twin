/**
 * Manufacturing operator page for the CNC cell (MILL → INSPECT → PACK).
 * Reads top to bottom the way a shift lead works an incident: is the cell
 * producing, what is wrong, what was decided, and is the data platform
 * keeping up. Pipeline internals live in a collapsed deep dive.
 */
import { useCallback, useRef, useState, type CSSProperties, type ReactNode } from "react";
import { Link } from "react-router-dom";

import { AlertTimeline } from "@/components/cell/alert-timeline";
import { CellHeader, type PipelineToggle } from "@/components/cell/cell-header";
import { CellKpis } from "@/components/cell/cell-kpis";
import { CellStage } from "@/components/cell/cell-stage";
import { IncidentPanel } from "@/components/cell/incident-panel";
import { PlatformHealth, pipelineNodes, type PlatformInputs } from "@/components/cell/platform-health";
import { useMachiningCellModel, type MachiningCellModel } from "@/components/cell/use-cell-model";
import { CellStationCard } from "@/components/dashboard/cell-station-card";
import { GenieCopilot } from "@/components/dashboard/genie-copilot";
import type { CellCue } from "@/components/twin/machining-cell";
import { useAlertHistory } from "@/hooks/use-alert-history";
import { usePlatformMetrics } from "@/hooks/use-platform-metrics";
import type {
  Industry,
  IndustryAlertRule,
  IndustryAssets,
  IndustryMaintenanceEvent,
  PipelineMode,
} from "@/lib/industries-api";
import { RESPONSE_STEPS, type CellStationId } from "@/lib/manufacturing-story";
import type { AlertRecord, AlertResponseAction, ZerobusMetricsResponse } from "@/lib/types";

import "@/components/cell/cell-page.css";

function StationCard({ model, selected, stacked }: { model: MachiningCellModel; selected: CellStationId; stacked?: boolean }) {
  return (
    <CellStationCard
      station={selected}
      spindle={model.spindle}
      limit={model.limit}
      alert={model.spindleAlert}
      heatEvent={model.heatEvent}
      lastService={model.lastService}
      pack={model.pack}
      tiles={model.tiles}
      decisions={model.episode.decisions}
      batchHeld={model.batchHeld}
      stacked={stacked}
    />
  );
}

function DeepDive({ children }: { children: ReactNode }) {
  const [open, setOpen] = useState(false);
  return (
    <details
      className="cellops-deepdive"
      data-testid="platform-internals"
      onToggle={(event) => setOpen(event.currentTarget.open)}
    >
      <summary>
        Deep dive
        <span>Lakebase write performance · insertion health · LakeTS · streaming metrics · DBSQL history</span>
      </summary>
      {open && <div className="cellops-deepdive__body">{children}</div>}
    </details>
  );
}

export function ManufacturingCellPage({
  industry,
  mode,
  modeLabel,
  showKpis,
  rules,
  maintenance,
  assets,
  zerobus,
  wsConnected,
  powered,
  isAdmin,
  pipelineId,
  setupId,
  refreshSec,
  onRefreshSec,
  nonce,
  onRefreshNow,
  pipeline,
  ingestPanel,
  deepDive,
  style,
}: {
  industry: Industry;
  mode: PipelineMode;
  modeLabel: string;
  showKpis: boolean;
  rules: IndustryAlertRule[];
  maintenance: IndustryMaintenanceEvent[];
  assets: IndustryAssets | null;
  zerobus: ZerobusMetricsResponse | null;
  wsConnected: boolean;
  powered: boolean;
  isAdmin: boolean;
  pipelineId: string;
  /** Pipeline setup id for the per-pipeline Genie space; falls back to pipelineId. */
  setupId?: string | null;
  refreshSec: number;
  onRefreshSec: (sec: number) => void;
  nonce: number;
  onRefreshNow: () => void;
  pipeline: PipelineToggle;
  /** Panels that feed the twin and must stay mounted (Zerobus latest rows). */
  ingestPanel?: ReactNode;
  deepDive: ReactNode;
  style: CSSProperties;
}) {
  const model = useMachiningCellModel({ mode, rules, maintenance, zerobusReadings: zerobus?.latest_readings });
  const metrics = usePlatformMetrics({ pipelineId, industry: industry.id, mode, refreshSec, nonce });
  const { production, episode } = model;
  const changeKey = [
    model.alerts.map((a) => `${a.id}:${a.status}`).join(","),
    Object.keys(episode.decisions).length,
    production.state?.changed_at ?? "",
  ].join("|");
  const { history, error: historyError } = useAlertHistory(industry.id, changeKey);

  const [selected, setSelected] = useState<CellStationId>("mill");
  const [cue, setCue] = useState<CellCue | null>(null);
  const [genieOpen, setGenieOpen] = useState(false);
  const [explainAlert, setExplainAlert] = useState<AlertRecord | null>(null);
  const stageRef = useRef<HTMLDivElement>(null);

  const onRecorded = useCallback((action: AlertResponseAction) => {
    const step = RESPONSE_STEPS.find((s) => s.action === action);
    if (!step) return;
    const box = stageRef.current?.getBoundingClientRect();
    if (box && box.top < 0) {
      const reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
      stageRef.current?.scrollIntoView({ behavior: reduced ? "auto" : "smooth", block: "start" });
    }
    setSelected(step.station);
    setCue({ station: step.station, view: action === "send_to_inspect" ? "cell" : step.station, at: Date.now() });
  }, []);
  const onExplain = useCallback((alert: AlertRecord) => {
    setExplainAlert(alert);
    setGenieOpen(true);
  }, []);

  const controllable = mode !== "zerobus";
  const platform: PlatformInputs = {
    mode,
    powered,
    wsConnected,
    phase: production.phase,
    feed: model.feed,
    metrics,
    zerobus,
    now: model.now,
  };
  const statusLabel = `${industry.name} · ${
    mode === "realtime" ? (wsConnected ? "live" : "SSE offline") : mode === "zerobus" && zerobus?.latest_readings?.length ? "live" : "preview"
  }`;
  const incident = (
    <IncidentPanel
      model={model}
      episodes={history?.episodes ?? []}
      changes={history?.production ?? []}
      controllable={controllable}
      onRecorded={onRecorded}
      onExplain={onExplain}
    />
  );

  return (
    <section id="view-dashboard" className={`cellops is-${production.phase}`} style={style}>
      <div className="crumbs">
        <Link to="/industries">Industries</Link>
        <span className="sep">/</span>
        <Link to={`/i/${industry.id}`}>{industry.name}</Link>
        <span className="sep">/</span>
        <span>Dashboard</span>
      </div>

      <CellHeader
        modeLabel={modeLabel}
        production={production}
        canControl={controllable && isAdmin}
        now={model.now}
        nodes={pipelineNodes(platform)}
        refreshSec={refreshSec}
        onRefreshSec={onRefreshSec}
        onRefreshNow={onRefreshNow}
        pipeline={pipeline}
        onOpenGenie={() => setGenieOpen(true)}
      />

      {showKpis && <CellKpis kpis={industry.kpis} model={model} powered={powered} />}

      <div className="cellops-main">
        <div className="cellops-main__stage" ref={stageRef}>
          <CellStage
            model={model}
            selected={selected}
            onSelect={setSelected}
            animate={powered && (mode !== "realtime" || wsConnected)}
            statusLabel={statusLabel}
            cue={cue}
            side={
              <>
                {incident}
                <StationCard model={model} selected={selected} stacked />
              </>
            }
          />
          <StationCard model={model} selected={selected} />
        </div>
        <div className="cellops-main__side">
          {incident}
          <AlertTimeline history={history} current={production.state} now={model.now} error={historyError} />
        </div>
      </div>

      <PlatformHealth input={platform}>
        {ingestPanel}
        <DeepDive>{deepDive}</DeepDive>
      </PlatformHealth>

      <div className={`cellops-genie${genieOpen ? " is-open" : ""}`}>
        <GenieCopilot
          industry={industry.id}
          mode={mode}
          setupId={setupId ?? pipelineId}
          isAdmin={isAdmin}
          assets={assets}
          alert={explainAlert}
          open={genieOpen}
          onOpenChange={setGenieOpen}
        />
      </div>

      <footer>Manufacturing cell · alert decisions and production stops are recorded in Lakebase.</footer>
    </section>
  );
}
