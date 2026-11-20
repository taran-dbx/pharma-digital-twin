/**
 * "How your data flows" panel body — one `.arch-flow` row per deployed mode.
 *
 * Ports `archFlow`/`renderArch` from the design reference
 * (the original design reference), swapping its
 * `IND[cur].flavors` lookup for the real `Industry.modes[].status` the
 * backend catalog returns. Node lists come from the shared `archNodes()` /
 * `<NodeIcon>` in `./arch-nodes` (also used by the "View data flow" modal).
 */
import { Fragment } from "react";

import type { ModeView } from "@/lib/admin-api";
import type { Industry } from "@/lib/industries-api";
import { archNodes, MODE_LABEL, NodeIcon } from "./arch-nodes";

export interface ArchDiagramProps {
  industry: Industry;
  modes: ModeView[];
}

export function ArchDiagram({ industry, modes }: ArchDiagramProps) {
  const deployed = industry.modes
    .map((mode) => ({
      mode,
      runtime: modes.find((candidate) => candidate.mode === mode.id),
    }))
    .filter(
      (entry): entry is typeof entry & { runtime: ModeView } =>
        entry.runtime !== undefined,
    );

  if (deployed.length === 0) {
    return <div className="arch-empty">Deploy a pipeline mode to see its data flow.</div>;
  }

  return (
    <>
      {deployed.map(({ mode, runtime }) => (
        <div
          aria-label={`${MODE_LABEL[mode.id]} data flow, ${runtime.status}`}
          className={`arch-flow is-${runtime.status}`}
          data-mode={mode.id}
          data-state={runtime.status}
          key={mode.id}
        >
          <span className="arch-flabel">
            {MODE_LABEL[mode.id]}
            <small>{runtime.status}</small>
          </span>
          <div className="arch">
            {archNodes(industry, mode.id).map((node, i) => (
              <Fragment key={`${node.t}-${i}`}>
                {i > 0 && (
                  <div className="aarrow" aria-hidden="true">
                    <span>→</span>
                    {runtime.status === "running" && <i className="flow-pulse" />}
                  </div>
                )}
                <div className={`anode ${node.sink ? "sink" : ""}`}>
                  <div className="an-ic">
                    <NodeIcon ic={node.ic} />
                  </div>
                  <div className="an-t">{node.t}</div>
                  <div className="an-s">{node.s}</div>
                </div>
              </Fragment>
            ))}
          </div>
        </div>
      ))}
    </>
  );
}
