import { BrainCircuit } from "lucide-react";

import { riskPercent, type SpindleRisk } from "@/lib/manufacturing-risk";
import { formatNumber } from "@/lib/utils";

/**
 * Thermal-risk model output for Spindle 3: a "predicted" badge, distinct from
 * the threshold rule's "breached" state, with the score, cut-off, model version
 * and the features that drove the score.
 */
export function RiskBadge({ risk, compact = false }: { risk: SpindleRisk; compact?: boolean }) {
  const tone = risk.active ? "is-predicted" : "is-quiet";
  const horizon = risk.horizonS ? `${Math.round(risk.horizonS / 60)} min` : "the horizon";
  return (
    <div className={`cellops-risk ${tone}`} data-testid="risk-badge" data-active={risk.active}>
      <div className="cellops-risk__head">
        <span className="cellops-risk__kind">
          <BrainCircuit aria-hidden="true" />
          {risk.active ? "Predicted excursion" : "Model: risk back down"}
        </span>
        <b className="tnum" data-testid="risk-score">
          {riskPercent(risk.score)}
        </b>
      </div>
      {!compact && (
        <p className="cellops-risk__detail">
          Risk Spindle 3 exceeds its validated limit within {horizon}
          {risk.threshold !== null ? ` · alert at ≥ ${riskPercent(risk.threshold)}` : ""}
        </p>
      )}
      {risk.topFeatures.length > 0 && (
        <ul className="cellops-risk__drivers" aria-label="Top risk drivers">
          {risk.topFeatures.map((f) => (
            <li key={f.feature}>
              <span>{f.label}</span>
              <em className={f.contribution >= 0 ? "is-up" : "is-down"}>
                {f.contribution >= 0 ? "▲" : "▼"} {f.value === null ? "" : formatNumber(f.value, 1)}
              </em>
            </li>
          ))}
        </ul>
      )}
      {risk.leadBeforeOnsetS !== null && (
        <p className="cellops-risk__lead" data-testid="risk-lead">
          Warned {Math.round(risk.leadBeforeOnsetS)} s before the excursion onset
        </p>
      )}
      <small className="cellops-risk__model">
        {risk.modelName ?? "thermal-risk model"}
        {risk.modelVersion ? ` v${risk.modelVersion}` : ""}
      </small>
    </div>
  );
}
