import { useCallback, useEffect, useState, type ReactNode } from "react";
import { createPortal } from "react-dom";

import type { MachiningCellModel } from "@/components/cell/use-cell-model";
import { MachiningCell, type CellCue, type CellStopView } from "@/components/twin/machining-cell";
import type { CellStationId } from "@/lib/manufacturing-story";

function stopView(model: MachiningCellModel): CellStopView | null {
  const { production } = model;
  if (production.phase === "running") return null;
  return {
    phase: production.phase,
    since: production.since,
    lastReading: model.lastMachineTs,
    reason: production.state?.reason ?? "operator stop",
    resuming: production.pending === "running",
    onResume: () => void production.resume(),
  };
}

/** The 3D cell, with a full-view mode that carries `side` (incident + station detail) along. */
export function CellStage({
  model,
  selected,
  onSelect,
  animate,
  statusLabel,
  cue,
  side,
}: {
  model: MachiningCellModel;
  selected: CellStationId;
  onSelect: (station: CellStationId) => void;
  animate: boolean;
  statusLabel: string;
  cue: CellCue | null;
  side: ReactNode;
}) {
  const [expanded, setExpanded] = useState(false);
  const toggle = useCallback(() => setExpanded((open) => !open), []);

  useEffect(() => {
    if (!expanded) return;
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") setExpanded(false);
    };
    window.addEventListener("keydown", onKey);
    const previous = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      window.removeEventListener("keydown", onKey);
      document.body.style.overflow = previous;
    };
  }, [expanded]);

  const twin = (
    <MachiningCell
      stations={model.stations}
      sensors={model.sensors}
      selected={selected}
      onSelect={onSelect}
      decisions={model.episode.decisions}
      batchHeld={model.batchHeld}
      spindleHeat={model.spindleHeat}
      spindleLimit={model.limit?.threshold}
      spindleRpm={model.spindleRpm}
      lineSpeed={model.production.phase === "stopped" ? 0 : model.pack.speed.value}
      animate={animate}
      statusLabel={statusLabel}
      expanded={expanded}
      onToggleExpanded={toggle}
      stop={stopView(model)}
      cue={cue}
    />
  );

  return (
    <>
      <div className="twin twin--cell cellops__twin">
        {expanded ? <div className="cell-layout__placeholder">Showing in full view · press Esc to return</div> : twin}
      </div>
      {expanded &&
        createPortal(
          <div className="cell-immersive" role="dialog" aria-modal="true" aria-label="Machining cell full view">
            <div className="cell-immersive__stage">{twin}</div>
            <aside className="cell-immersive__side">{side}</aside>
          </div>,
          document.body,
        )}
    </>
  );
}
