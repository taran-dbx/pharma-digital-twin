/**
 * Shared Genie message helpers used by both the workspace-wide and the
 * per-pipeline copilots: status classification for polling, the answer text
 * to show (never `message.content`, which is the wrapped prompt), and
 * defensive normalisation of attachments (external data).
 */
import type { GenieAttachmentQuery, GenieMessageStatus } from "@/lib/types";

const KNOWN_STATUSES: ReadonlySet<GenieMessageStatus> = new Set<GenieMessageStatus>([
  "SUBMITTED",
  "FETCHING_METADATA",
  "FILTERING_CONTEXT",
  "ASKING_AI",
  "PENDING_WAREHOUSE",
  "EXECUTING_QUERY",
  "IN_PROGRESS",
  "COMPLETED",
  "FAILED",
  "CANCELLED",
  "QUERY_RESULT_EXPIRED",
  "UNKNOWN",
]);

export function classifyStatus(status: unknown): GenieMessageStatus {
  const value = typeof status === "string" ? status.trim().toUpperCase() : "";
  return KNOWN_STATUSES.has(value as GenieMessageStatus) ? (value as GenieMessageStatus) : "UNKNOWN";
}

/** Terminal = COMPLETED, FAILED, CANCELLED, QUERY_RESULT_EXPIRED. Everything else keeps polling. */
export function isTerminal(status: GenieMessageStatus): boolean {
  switch (status) {
    case "COMPLETED":
    case "FAILED":
    case "CANCELLED":
    case "QUERY_RESULT_EXPIRED":
      return true;
    case "SUBMITTED":
    case "FETCHING_METADATA":
    case "FILTERING_CONTEXT":
    case "ASKING_AI":
    case "PENDING_WAREHOUSE":
    case "EXECUTING_QUERY":
    case "IN_PROGRESS":
    case "UNKNOWN":
      return false;
    default: {
      const exhaustive: never = status;
      return exhaustive;
    }
  }
}

export function isPending(status: GenieMessageStatus): boolean {
  return !isTerminal(status);
}

export function statusLabel(status: GenieMessageStatus): string {
  switch (status) {
    case "EXECUTING_QUERY":
      return "Querying data";
    case "SUBMITTED":
    case "FETCHING_METADATA":
    case "FILTERING_CONTEXT":
    case "ASKING_AI":
    case "PENDING_WAREHOUSE":
    case "IN_PROGRESS":
    case "UNKNOWN":
      return "Working";
    case "COMPLETED":
      return "Complete";
    case "FAILED":
      return "Failed";
    case "CANCELLED":
      return "Cancelled";
    case "QUERY_RESULT_EXPIRED":
      return "Expired";
    default: {
      const exhaustive: never = status;
      return exhaustive;
    }
  }
}

/** Fixed copy when Genie returns neither an error nor any answer attachment. */
export function fallbackAnswer(status: GenieMessageStatus): string {
  switch (status) {
    case "FAILED":
      return "Genie could not complete this investigation.";
    case "CANCELLED":
      return "Cancelled";
    case "QUERY_RESULT_EXPIRED":
      return "The query result expired — ask again.";
    default:
      return "No answer";
  }
}

/** Internal, render-ready view of one attachment. */
export interface GenieAttachmentView {
  /** Stable React key (falls back to the index when the id is missing). */
  key: string;
  /** Null when Genie sent no id — the result cannot be fetched. */
  attachmentId: string | null;
  text: string | null;
  query: GenieAttachmentQuery | null;
  suggestedQuestions: string[];
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function nonEmpty(value: unknown): string | null {
  return typeof value === "string" && value.trim() !== "" ? value : null;
}

/** Text is a string (per-pipeline API) or `{content}` (raw SDK shape on the static API). */
function parseText(value: unknown): string | null {
  return nonEmpty(value) ?? (isRecord(value) ? nonEmpty(value.content) : null);
}

function parseQuery(value: unknown): GenieAttachmentQuery | null {
  if (!isRecord(value)) return null;
  const sql = nonEmpty(value.sql) ?? nonEmpty(value.query);
  if (!sql) return null;
  return { sql, description: nonEmpty(value.description) };
}

function parseQuestions(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((v): v is string => nonEmpty(v) !== null) : [];
}

/** Drop malformed / empty attachments; never trust the wire shape. */
export function normalizeAttachments(raw: unknown): GenieAttachmentView[] {
  if (!Array.isArray(raw)) return [];
  return raw.flatMap((item, index): GenieAttachmentView[] => {
    if (!isRecord(item)) return [];
    const attachmentId = nonEmpty(item.attachment_id);
    const view: GenieAttachmentView = {
      key: attachmentId ?? `att-${index}`,
      attachmentId,
      text: parseText(item.text),
      query: parseQuery(item.query),
      suggestedQuestions: parseQuestions(item.suggested_questions),
    };
    const empty = !view.text && !view.query && view.suggestedQuestions.length === 0;
    return empty ? [] : [view];
  });
}

/**
 * Lead text for an assistant turn: the error, else nothing when attachments
 * carry the answer, else a fixed per-status message. Never `message.content`.
 */
export function answerText(
  status: GenieMessageStatus,
  error: string | null | undefined,
  attachments: readonly GenieAttachmentView[],
): string | null {
  const err = nonEmpty(error);
  if (err) return err;
  const answered = attachments.some((a) => a.text) || (status === "COMPLETED" && attachments.some((a) => a.query));
  return answered ? null : fallbackAnswer(status);
}

/** Flattened single-string answer for the text-only (static) transcript. */
export function plainAnswer(
  status: GenieMessageStatus,
  error: string | null | undefined,
  rawAttachments: unknown,
): string {
  const attachments = normalizeAttachments(rawAttachments);
  const lead = answerText(status, error, attachments);
  if (lead) return lead;
  const texts = attachments.map((a) => a.text).filter((t): t is string => t !== null);
  return texts.length > 0 ? texts.join("\n\n") : "Genie ran a query for this question.";
}

/** Suggested follow-ups from a message's attachments, de-duplicated, in order. */
export function suggestedQuestions(attachments: readonly GenieAttachmentView[]): string[] {
  return [...new Set(attachments.flatMap((att) => att.suggestedQuestions))];
}
