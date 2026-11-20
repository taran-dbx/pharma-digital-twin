/**
 * Tab switch between the live cell and the two outcome views on the
 * medical-device cell page. The live cell stays mounted (it feeds the twin);
 * the page hides it with `data-view` while an outcome view is open.
 */
import { useId, useState, type KeyboardEvent } from "react";

import { ExecutiveView } from "@/components/value/executive-view";
import { OperationsView } from "@/components/value/operations-view";
import { VALUE_WINDOWS, type ValueWindow } from "@/lib/value-api";

import "@/components/value/value-views.css";

export type CellView = "cell" | "exec" | "ops";

export const CELL_VIEWS: { id: CellView; label: string; persona?: string }[] = [
  { id: "cell", label: "Live cell" },
  { id: "exec", label: "Executive", persona: "Alex Morgan, VP Quality & Site Head" },
  { id: "ops", label: "Operations", persona: "Shreya, Head of Manufacturing Ops" },
];

export function CellViewTabs({ value, onChange }: { value: CellView; onChange: (view: CellView) => void }) {
  const onKey = (event: KeyboardEvent<HTMLDivElement>) => {
    const step = event.key === "ArrowRight" ? 1 : event.key === "ArrowLeft" ? -1 : 0;
    if (!step) return;
    event.preventDefault();
    const index = CELL_VIEWS.findIndex((v) => v.id === value);
    const next = CELL_VIEWS[(index + step + CELL_VIEWS.length) % CELL_VIEWS.length];
    onChange(next.id);
    document.getElementById(`cellview-tab-${next.id}`)?.focus();
  };
  return (
    <div className="cellview-tabs" role="tablist" aria-label="Cell views" onKeyDown={onKey} data-testid="cell-view-tabs">
      {CELL_VIEWS.map((view) => (
        <button
          key={view.id}
          id={`cellview-tab-${view.id}`}
          type="button"
          role="tab"
          aria-label={view.persona ? `${view.label} · ${view.persona}` : view.label}
          aria-selected={value === view.id}
          tabIndex={value === view.id ? 0 : -1}
          className={`cellview-tab${value === view.id ? " is-active" : ""}`}
          onClick={() => onChange(view.id)}
        >
          <span className="cellview-tab__label">{view.label}</span>
          {view.persona && <span className="cellview-tab__persona">{view.persona}</span>}
        </button>
      ))}
    </div>
  );
}

export function CellValuePanel({
  view,
  industry,
  pipelineId,
  nonce,
}: {
  view: CellView;
  industry: string;
  pipelineId: string;
  nonce: number;
}) {
  const [windowId, setWindowId] = useState<ValueWindow>("30d");
  const selectId = useId();
  if (view === "cell") return null;
  const windowLabel = VALUE_WINDOWS.find((w) => w.id === windowId)?.label ?? windowId;
  const scope = { industry, pipelineId, window: windowId };
  const persona = CELL_VIEWS.find((v) => v.id === view);
  return (
    <section
      className="cellview-panel"
      role="tabpanel"
      aria-labelledby={`cellview-tab-${view}`}
      data-testid={`cell-view-${view}`}
    >
      <div className="cellview-filters">
        <label htmlFor={selectId}>Window</label>
        <select id={selectId} value={windowId} onChange={(e) => setWindowId(e.target.value as ValueWindow)}>
          {VALUE_WINDOWS.map((w) => (
            <option key={w.id} value={w.id}>
              {w.label}
            </option>
          ))}
        </select>
        <span className="value-note">
          {persona?.label} · {persona?.persona} · this pipeline only
        </span>
      </div>
      {view === "exec" ? (
        <ExecutiveView key={`exec-${pipelineId}`} scope={scope} windowLabel={windowLabel} nonce={nonce} />
      ) : (
        <OperationsView key={`ops-${pipelineId}`} scope={scope} nonce={nonce} />
      )}
    </section>
  );
}
