/**
 * One Genie query attachment: description, a collapsible "Show SQL" block,
 * and the fetched result rendered by {@link GenieResultChart}. Attachments
 * without an id show their SQL but never fetch a result.
 */
import { useCallback, useEffect, useState } from "react";

import { GenieResultChart } from "@/components/dashboard/genie-result-chart";
import { api } from "@/lib/api";
import type { PipelineMode } from "@/lib/industries-api";
import type { GenieAttachmentQuery, GenieQueryResult } from "@/lib/types";

interface ResultTarget {
  setupId: string;
  mode: PipelineMode;
  conversationId: string;
  messageId: string;
  attachmentId: string;
}

interface GenieQueryAttachmentProps extends Omit<ResultTarget, "attachmentId"> {
  attachmentId: string | null;
  query: GenieAttachmentQuery;
}

interface ResultState {
  result: GenieQueryResult | null;
  loading: boolean;
  error: string | null;
}

const INITIAL: ResultState = { result: null, loading: true, error: null };

function FetchedResult({ setupId, mode, conversationId, messageId, attachmentId }: ResultTarget) {
  const [state, setState] = useState<ResultState>(INITIAL);
  const [attempt, setAttempt] = useState(0);

  useEffect(() => {
    let cancelled = false;
    setState(INITIAL);
    api
      .getPipelineGenieAttachmentResult(setupId, mode, conversationId, messageId, attachmentId)
      .then((result) => {
        if (!cancelled) setState({ result, loading: false, error: null });
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setState({ result: null, loading: false, error: err instanceof Error ? err.message : "Result unavailable" });
        }
      });
    return () => {
      cancelled = true;
    };
  }, [setupId, mode, conversationId, messageId, attachmentId, attempt]);

  const retry = useCallback(() => setAttempt((n) => n + 1), []);

  return <GenieResultChart result={state.result} loading={state.loading} error={state.error} onRetry={retry} />;
}

export function GenieQueryAttachment({ attachmentId, query, ...target }: GenieQueryAttachmentProps) {
  return (
    <section className="genie-attachment" data-testid="genie-query-attachment" aria-label="Genie query result">
      {query.description && <p className="genie-attachment__desc">{query.description}</p>}
      <details className="genie-attachment__sql" data-testid="genie-sql">
        <summary>Show SQL</summary>
        <pre>
          <code>{query.sql}</code>
        </pre>
      </details>
      {attachmentId ? (
        <FetchedResult {...target} attachmentId={attachmentId} />
      ) : (
        <p className="genie-drawer__hint" data-testid="genie-result-no-id">
          Genie did not return a result reference for this query.
        </p>
      )}
    </section>
  );
}
