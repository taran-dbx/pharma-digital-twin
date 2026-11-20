import type { Page } from "@playwright/test";

import { manufacturingCatalog } from "./dashboard-apis";

export const PIPELINE_SETUP_ID = "setup-mfg-1";
export const PIPELINE_DEPLOYMENT_ID = "deploy-mfg-rt";

export interface GenieIndustryFixture {
  id: string;
  name: string;
  setupId: string;
  deploymentId: string;
  starters: string[];
  heading: RegExp;
}

export const MANUFACTURING: GenieIndustryFixture = {
  id: "manufacturing",
  name: "Manufacturing",
  setupId: PIPELINE_SETUP_ID,
  deploymentId: PIPELINE_DEPLOYMENT_ID,
  starters: ["How has spindle temperature trended this shift?", "Which station raised the most alerts?"],
  heading: /Manufacturing twin/,
};

export const PHARMACEUTICAL: GenieIndustryFixture = {
  id: "pharmaceutical",
  name: "Pharmaceutical",
  setupId: "setup-pharma-1",
  deploymentId: "deploy-pharma-rt",
  starters: ["How has bioreactor pH trended this batch?", "Which WFI loop raised the most alerts?"],
  heading: /Pharma/i,
};

/** Catalog whose realtime mode is a running industry-first deployment of `fixture`. */
export function deployedCatalog(fixture: GenieIndustryFixture, isAdmin = false) {
  const base = manufacturingCatalog.industries[0];
  return {
    ...manufacturingCatalog,
    is_admin: isAdmin,
    industries: [
      {
        ...base,
        id: fixture.id,
        name: fixture.name,
        modes: base.modes.map((mode) => ({
          ...mode,
          status: "running",
          deployment_id: fixture.deploymentId,
          setup_id: fixture.setupId,
        })),
      },
    ],
  };
}

export const deployedManufacturingCatalog = deployedCatalog(MANUFACTURING);

/** Starters served by the backend `/starters` endpoint for manufacturing (kept for existing specs). */
export const starterQuestions = MANUFACTURING.starters;

const lineResult = {
  columns: [
    { name: "window_start", type: "TIMESTAMP" },
    { name: "avg_spindle_temp", type: "DOUBLE" },
  ],
  rows: [
    ["2026-11-18T08:00:00Z", "61.2"],
    ["2026-11-18T08:05:00Z", "63.9"],
    ["2026-11-18T08:10:00Z", "67.4"],
  ],
  truncated: true,
};

const barResult = {
  columns: [
    { name: "station", type: "STRING" },
    { name: "alert_count", type: "BIGINT" },
  ],
  rows: [
    ["MILL", "7"],
    ["INSPECT", "2"],
    ["PACK", "1"],
  ],
  truncated: false,
};

const tableResult = {
  columns: [
    { name: "station", type: "STRING" },
    { name: "fault_code", type: "STRING" },
    { name: "operator", type: "STRING" },
  ],
  rows: [["MILL", "M-SPINDLE-HOT", "shift-a"]],
  truncated: false,
};

/** What the backend echoes as `content`: the trusted prompt wrapper. Must never render. */
export const WRAPPED_PROMPT = "[TRUSTED CONTEXT] pipeline=setup-mfg-1 [UNTRUSTED QUESTION] …";

const messages: Record<string, Record<string, unknown>> = {
  "msg-1": {
    conversation_id: "conv-1",
    message_id: "msg-1",
    status: "COMPLETED",
    content: WRAPPED_PROMPT,
    error: null,
    attachments: [
      {
        attachment_id: "att-text-1",
        text: "Spindle temperature climbed 6°C over the window.",
        query: null,
        suggested_questions: [],
      },
      {
        attachment_id: "att-line",
        text: null,
        query: { sql: "SELECT window_start, avg(temp) FROM spindle GROUP BY 1", description: "Average spindle temperature per 5 minutes" },
        suggested_questions: [],
      },
      {
        attachment_id: "att-suggest",
        text: null,
        query: null,
        suggested_questions: ["Which station raised the most alerts?"],
      },
    ],
  },
  "msg-2": {
    conversation_id: "conv-1",
    message_id: "msg-2",
    status: "COMPLETED",
    content: WRAPPED_PROMPT,
    error: null,
    attachments: [
      {
        attachment_id: "att-bar",
        text: null,
        query: { sql: "SELECT station, count(*) FROM alerts GROUP BY 1", description: null },
        suggested_questions: [],
      },
      {
        attachment_id: "att-table",
        text: null,
        query: { sql: "SELECT station, fault_code, operator FROM alerts", description: "Raw alerts" },
        suggested_questions: [],
      },
    ],
  },
};

const results: Record<string, unknown> = {
  "att-line": lineResult,
  "att-bar": barResult,
  "att-table": tableResult,
};

export interface PipelineGenieMockOptions {
  /** Industry the dashboard belongs to (default manufacturing). */
  industry?: GenieIndustryFixture;
  /** Catalog `is_admin` — admins see the "Create Genie space" action. */
  isAdmin?: boolean;
  /** /status answers 200 with supported:false (industry has no per-pipeline Genie). */
  unsupported?: boolean;
  /** The first POST /conversations fails with a 500 (retry path). */
  failFirstSend?: boolean;
  /** `/starters` answers an error so the empty state has no chips. */
  startersFail?: boolean;
  /** Admin backfill POST fails with this detail. */
  provisionError?: string;
  available?: boolean;
  spaceId?: string | null;
  reason?: string;
  /** /status answers 404 (unknown setup / non-UUID id). */
  statusNotFound?: boolean;
  failResults?: boolean;
  /** Polls for the first message answer 404 (conversation lost after an app restart). */
  expireFirstMessage?: boolean;
  /** Polls never reach a terminal status. */
  neverFinish?: boolean;
}

/** Non-terminal statuses served before COMPLETED on every poll sequence. */
export const PENDING_SEQUENCE = ["SUBMITTED", "ASKING_AI"] as const;

/** Speed up polling and shorten the 3-minute deadline (read by lib/genie-poll.ts). */
export async function setGeniePollHooks(page: Page, hooks: { pollIntervalMs?: number; pollDeadlineMs?: number }) {
  await page.addInitScript((value) => {
    (window as unknown as { __genieTestHooks?: unknown }).__genieTestHooks = value;
  }, hooks);
}

export async function mockPipelineGenie(page: Page, options: PipelineGenieMockOptions = {}) {
  const fixture = options.industry ?? MANUFACTURING;
  const genieBase = `/api/genie/pipelines/${fixture.setupId}/realtime`;
  const provisionPath = `/api/admin/pipelines/${fixture.setupId}/genie/realtime`;
  let provisioned = options.available ?? true;
  const provisionCalls: string[] = [];
  let sendFailures = options.failFirstSend ? 1 : 0;
  const currentSpaceId = () =>
    options.spaceId !== undefined && !provisioned ? options.spaceId : provisioned ? "space-1" : null;
  let nextMessage = 1;
  const pollCounts = new Map<string, number>();
  const polledStatuses: string[] = [];

  await page.route("**/api/industries/catalog", async (route) => {
    await route.fulfill({ json: deployedCatalog(fixture, options.isAdmin ?? false) });
  });
  if (fixture.id !== MANUFACTURING.id) {
    await page.route(`**/api/industries/${fixture.id}/assets`, async (route) => {
      await route.fulfill({ json: { industry: fixture.id, sites: [] } });
    });
    await page.route(`**/api/industries/${fixture.id}/reference`, async (route) => {
      await route.fulfill({ json: { industry: fixture.id, kpis: [], maintenance_history: [] } });
    });
  }

  await page.route(`**${provisionPath}`, async (route) => {
    provisionCalls.push(route.request().method());
    if (options.provisionError) {
      await route.fulfill({ status: 500, json: { detail: options.provisionError } });
      return;
    }
    provisioned = true;
    await route.fulfill({ json: { space_id: "space-1", status: "created" } });
  });

  await page.route(`**${genieBase}/**`, async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname.slice(genieBase.length);

    if (path === "/status") {
      if (options.statusNotFound) {
        await route.fulfill({ status: 404, json: { detail: "unknown pipeline" } });
        return;
      }
      if (options.unsupported) {
        await route.fulfill({
          json: {
            available: false,
            space_id: null,
            supported: false,
            industry: fixture.id,
            reason: `Per-pipeline Genie is not available for industry '${fixture.id}'.`,
          },
        });
        return;
      }
      await route.fulfill({
        json: { available: provisioned, space_id: currentSpaceId(), reason: provisioned ? undefined : options.reason },
      });
      return;
    }
    if (options.expireFirstMessage && path === "/conversations/conv-1/messages/msg-1") {
      await route.fulfill({ status: 404, json: { detail: "unknown conversation" } });
      return;
    }
    if (path === "/starters") {
      if (options.startersFail) {
        await route.fulfill({ status: 500, json: { detail: "starters unavailable" } });
        return;
      }
      await route.fulfill({ json: { questions: fixture.starters } });
      return;
    }
    if (request.method() === "POST" && /^\/conversations(\/conv-1\/messages)?$/.test(path)) {
      if (sendFailures > 0) {
        sendFailures -= 1;
        await route.fulfill({ status: 500, json: { detail: "Genie temporarily unavailable" } });
        return;
      }
      const messageId = `msg-${nextMessage}`;
      nextMessage += 1;
      await route.fulfill({ json: { conversation_id: "conv-1", message_id: messageId } });
      return;
    }
    const resultMatch = path.match(/^\/conversations\/conv-1\/messages\/(msg-\d)\/attachments\/([^/]+)\/result$/);
    if (resultMatch) {
      if (options.failResults) {
        await route.fulfill({ status: 502, json: { detail: "warehouse unavailable" } });
        return;
      }
      await route.fulfill({ json: results[resultMatch[2]] });
      return;
    }
    const messageMatch = path.match(/^\/conversations\/conv-1\/messages\/(msg-\d)$/);
    if (messageMatch && messages[messageMatch[1]]) {
      const id = messageMatch[1];
      const count = pollCounts.get(id) ?? 0;
      pollCounts.set(id, count + 1);
      const status = options.neverFinish ? "ASKING_AI" : (PENDING_SEQUENCE[count] ?? "COMPLETED");
      polledStatuses.push(status);
      const body = status === "COMPLETED" ? messages[id] : { ...messages[id], status, attachments: [] };
      await route.fulfill({ json: body });
      return;
    }
    await route.fulfill({ status: 404, json: { detail: `Unexpected Genie request: ${path}` } });
  });
  return { polledStatuses, provisionCalls, starters: fixture.starters };
}
