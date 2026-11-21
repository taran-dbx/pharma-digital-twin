/**
 * Operations view for Shreya, Head of Manufacturing Operations: how fast the
 * cell detects, acknowledges and contains a Spindle 3 excursion.
 */
import { DurationHistogram } from "@/components/value/duration-histogram";
import { StatTiles, TilesSkeleton } from "@/components/value/stat-tiles";
import { useOpsSummary } from "@/components/value/use-value-data";
import type { RecentEvent, SummaryScope } from "@/lib/value-api";
import { ackChart, decisionsText, formatDuration, formatEventTime, opsTiles, samplingNote } from "@/lib/value-format";

export function OperationsView({ scope, nonce }: { scope: SummaryScope; nonce: number }) {
  const { result, error, loading, refreshing } = useOpsSummary(scope, nonce);

  if (loading && !result) return <TilesSkeleton count={6} label="Loading operations metrics" />;
  if (!result) {
    return (
      <p className="value-error" role="alert">
        {error ?? "Operations metrics are unavailable."}
      </p>
    );
  }
  const { data, meta } = result;
  return (
    <div className="value-view" data-testid="operations-view">
      {!meta.data_available ? (
        <p className="value-empty" role="note">
          The app backend is not connected, so there are no recorded excursions to measure yet.
        </p>
      ) : (
        data.episodes === 0 && (
          <p className="value-empty" role="note">
            No Spindle 3 excursions in this window. Detection and response times appear after the first alert.
          </p>
        )
      )}
      {error && (
        <p className="value-error" role="alert">
          {error}
        </p>
      )}
      {samplingNote(meta) && (
        <p className="value-sampled" role="note" data-testid="value-sampled">
          {samplingNote(meta)}
        </p>
      )}
      <StatTiles tiles={opsTiles(data)} label="Operations metrics" dim={refreshing} />
      <div className="value-charts">
        <DurationHistogram
          title="Time to detect"
          samples={data.time_to_detect.samples}
          caption="Onset (first over-limit reading, else alert open − rule duration) to alert open."
        />
        <DurationHistogram
          title="Time to acknowledge"
          samples={data.time_to_acknowledge.samples}
          caption={ackChart(data, meta.samples_shown).caption}
          emptyText={ackChart(data, meta.samples_shown).empty}
        />
      </div>
      <RecentEvents events={data.recent_events} />
    </div>
  );
}

function RecentEvents({ events }: { events: RecentEvent[] }) {
  return (
    <section className="value-events" aria-labelledby="value-events-title">
      <header className="value-section-head">
        <h3 id="value-events-title">Recent excursions</h3>
        <span className="value-note">newest first</span>
      </header>
      {events.length === 0 ? (
        <p className="value-empty">No excursions recorded in this window.</p>
      ) : (
        <div className="value-table-wrap">
          <table className="value-table" data-testid="recent-events">
            <thead>
              <tr>
                <th scope="col">Opened</th>
                <th scope="col">Lot</th>
                <th scope="col">Detect</th>
                <th scope="col">Acknowledge</th>
                <th scope="col">Contain</th>
                <th scope="col">Parts at risk</th>
                <th scope="col">In time</th>
                <th scope="col">Decisions</th>
              </tr>
            </thead>
            <tbody>
              {events.map((e) => (
                <tr key={`${e.alert_id}-${e.opened_at}`}>
                  <td className="tnum">{formatEventTime(e.opened_at)}</td>
                  <td className="tnum">{e.lot_id}</td>
                  <td className="tnum" title={e.onset_source === "first_reading" ? "From the first over-limit reading" : "Estimated: alert open − rule duration"}>
                    {formatDuration(e.time_to_detect_s)}
                    {e.onset_source !== "first_reading" && <span className="value-est"> est.</span>}
                  </td>
                  <td className="tnum">{formatDuration(e.time_to_acknowledge_s)}</td>
                  <td className="tnum">{formatDuration(e.time_to_disposition_s)}</td>
                  <td className="tnum">{e.parts_at_risk}</td>
                  <td>
                    <span className={`value-flag ${e.caught_in_time ? "is-ok" : "is-late"}`}>
                      <span aria-hidden="true">{e.caught_in_time ? "✓" : "!"}</span>
                      {e.caught_in_time ? "Caught" : e.decisions.length ? "Late" : "Not contained"}
                    </span>
                  </td>
                  <td>{decisionsText(e.decisions)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
