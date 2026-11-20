/**
 * Executive view for Alex Morgan, VP Quality & Site Head: what the cell's
 * excursion detection is worth, in cost of poor quality and lots protected.
 */
import { useState } from "react";

import { AssumptionsPanel } from "@/components/value/assumptions-panel";
import { StatTiles, TilesSkeleton } from "@/components/value/stat-tiles";
import { useValueSummary } from "@/components/value/use-value-data";
import type { AssumptionName, SummaryScope } from "@/lib/value-api";
import { execTiles, whatThisMeans } from "@/lib/value-format";

export function ExecutiveView({ scope, windowLabel, nonce }: { scope: SummaryScope; windowLabel: string; nonce: number }) {
  const [overrides, setOverrides] = useState<Partial<Record<AssumptionName, number>>>({});
  const { result, error, loading, refreshing } = useValueSummary(scope, overrides, nonce);

  if (loading && !result) return <TilesSkeleton count={4} label="Loading the savings model" />;
  if (!result) {
    return (
      <p className="value-error" role="alert">
        {error ?? "The savings model is unavailable."}
      </p>
    );
  }
  const { data, meta } = result;
  return (
    <div className="value-view" data-testid="executive-view">
      <p className="value-means" data-testid="value-means">
        <strong>What this means · </strong>
        {whatThisMeans(data.projection, data.actuals, data.assumptions.lines_in_scope, windowLabel)}
      </p>
      {!meta.data_available && (
        <p className="value-empty" role="note">
          The app backend is not connected, so recorded excursions can&rsquo;t be counted yet. The annual model below
          uses the assumptions only.
        </p>
      )}
      {error && (
        <p className="value-error" role="alert">
          {error}
        </p>
      )}
      <StatTiles tiles={execTiles(data, windowLabel)} label="Savings model headline figures" dim={refreshing} />
      <AssumptionsPanel
        fields={data.fields}
        assumptions={data.assumptions}
        defaults={data.defaults}
        overridden={data.overridden}
        projection={data.projection}
        onApply={setOverrides}
      />
      <p className="value-note">
        Annual figures are a model from the assumptions; &ldquo;to date&rdquo; figures count the excursions this pipeline
        recorded. Costs are reference values, priced as if the lot were scrapped. See docs/value-model.md.
      </p>
    </div>
  );
}
