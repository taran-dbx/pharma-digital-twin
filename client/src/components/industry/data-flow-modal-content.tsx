/**
 * "View data flow" modal body — ports `dataflowHtml()` from the design
 * reference (the original design reference), opened from a
 * pipeline card's `.fc-info` button. Uses the real `IndustryMode.desc`/`.to`
 * text the backend already returns (`services/industry_templates.py`'s
 * `MODES`, which mirrors the design's own `MODES[].desc`/`.to` verbatim)
 * instead of re-hardcoding the copy.
 */
import { Fragment } from "react";

import type { Industry, IndustryMode } from "@/lib/industries-api";
import { archNodes, NodeIcon } from "./arch-nodes";

export interface DataFlowModalContentProps {
  industry: Industry;
  mode: IndustryMode;
}

export function DataFlowModalContent({ industry, mode }: DataFlowModalContentProps) {
  return (
    <>
      <div className="af-desc">
        {mode.desc} <b>Primary sink:</b> {mode.to}.
      </div>
      <div className="af-flow">
        {archNodes(industry, mode.id).map((node, i) => (
          <Fragment key={`${node.t}-${i}`}>
            {i > 0 && <div className="af-arrow">→</div>}
            <div className={`af-node ${node.sink ? "sink" : ""}`}>
              <div className="af-ic">
                <NodeIcon ic={node.ic} />
              </div>
              <div className="af-t">{node.t}</div>
              <div className="af-s">{node.s}</div>
            </div>
          </Fragment>
        ))}
      </div>
      <div className="af-legend">
        <span>
          <span className="tierdot" style={{ background: "var(--sse)" }} />
          Kafka / live
        </span>
        <span>
          <span className="tierdot" style={{ background: "var(--lakebase)" }} />
          Lakebase (hot)
        </span>
        <span>
          <span className="tierdot" style={{ background: "var(--dbsql)" }} />
          Delta / DBSQL (cold)
        </span>
        <span>
          <span className="tierdot" style={{ background: "var(--ind)" }} />
          primary sink
        </span>
      </div>
    </>
  );
}
