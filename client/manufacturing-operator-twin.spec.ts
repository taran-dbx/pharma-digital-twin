import { expect, test, type Page } from "@playwright/test";

import {
  DEVICE_T0,
  HOT_SPINDLE,
  NORMAL_SPINDLE,
  adminCatalog,
  assets,
  catalog,
  cellUpdate,
  clearedEpisode,
  eventStream,
  lakebaseHealth,
  lakebaseHealthAt,
  lakebasePerf,
  productionEvent,
  productionState,
  reference,
  responseRecorded,
  sensorUpdates,
  spindleAlertOpened,
  streamingLatency,
  zerobusMetrics,
  type MockProduction,
  type ProductionStateValue,
} from "./tests/fixtures/manufacturing-operator";

const EVIDENCE_DIR = "./test-results/evidence";
/** Specs that pin the browser clock start it just after the last fixture reading. */
const CLOCK_T0 = DEVICE_T0 + 5;
/** After a resume, the mock producer stays silent this long before the first new reading. */
const RESTART_DELAY_MS = 4000;

type ResponseAction = "hold_batch" | "send_to_inspect" | "request_maintenance";

interface MockCell {
  posts: string[];
  productionPosts: ProductionStateValue[];
  production: MockProduction;
  changes: MockProduction[];
  resumedWallMs?: number;
}

interface MockOptions {
  /** SSE body EventSource replays on every reconnect. */
  sse: unknown[];
  admin?: boolean;
  /** Use time-relative Lakebase / Spark metrics and history (needs a pinned clock). */
  live?: boolean;
  episodes?: unknown[];
  changes?: MockProduction[];
  /** Temperature of the first reading the producer sends after a resume. */
  resumeTemp?: number;
}

async function mockCell(page: Page, options: MockOptions): Promise<MockCell> {
  const startedMs = Date.now();
  const serverNow = () => CLOCK_T0 + (Date.now() - startedMs) / 1000;
  const cell: MockCell = {
    posts: [],
    productionPosts: [],
    production: options.changes?.at(-1) ?? productionState("running", DEVICE_T0 - 3600, { changed_by: null }),
    changes: [...(options.changes ?? [])],
  };
  const setProduction = (state: ProductionStateValue, extra: Partial<MockProduction>) => {
    cell.production = productionState(state, serverNow(), extra);
    cell.changes.push(cell.production);
    if (state === "running") cell.resumedWallMs = Date.now();
    return cell.production;
  };
  const stream = () => {
    const messages = [...options.sse];
    if (cell.changes.length > 0) messages.push(productionEvent(cell.production));
    if (
      cell.production.state === "running" &&
      cell.resumedWallMs !== undefined &&
      Date.now() - cell.resumedWallMs > RESTART_DELAY_MS
    ) {
      const resumedAt = Date.parse(cell.production.changed_at) / 1000;
      messages.push(cellUpdate(options.resumeTemp ?? 58.2, resumedAt + 1));
    }
    return messages;
  };

  await page.route("**/api/**", (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    const json = (body: unknown) =>
      route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(body) });

    if (path === "/api/events") {
      return route.fulfill({
        status: 200,
        contentType: "text/event-stream",
        headers: { "Cache-Control": "no-cache" },
        body: eventStream(stream()),
      });
    }
    if (path === "/api/industries/catalog") return json(options.admin ? adminCatalog : catalog);
    if (path === "/api/industries/manufacturing/assets") return json(assets);
    if (path === "/api/industries/manufacturing/reference") return json(reference);
    if (path === "/api/industries/manufacturing/production") {
      if (request.method() === "POST") {
        const body = request.postDataJSON() as { state: ProductionStateValue; reason?: string };
        cell.productionPosts.push(body.state);
        return json({ ...setProduction(body.state, { reason: body.reason ?? null }), changed: true });
      }
      return json(cell.production);
    }
    if (path === "/api/alerts/history") {
      return json({
        industry: "manufacturing",
        since: new Date((serverNow() - 3600) * 1000).toISOString(),
        episodes: options.episodes ?? [],
        production: cell.changes,
      });
    }
    if (path === "/api/alerts") return json({ alerts: [] });
    const ack = path.match(/^\/api\/alerts\/([^/]+)\/acknowledge$/);
    if (ack) {
      const hot = spindleAlertOpened(HOT_SPINDLE[HOT_SPINDLE.length - 1]);
      return json({
        ...hot,
        type: "alert_acknowledged",
        status: "acknowledged",
        acknowledged_at: new Date(serverNow() * 1000).toISOString(),
        acknowledged_by: "operator@example.com",
      });
    }
    const responses = path.match(/^\/api\/alerts\/([^/]+)\/responses$/);
    if (responses && request.method() === "POST") {
      const action = (request.postDataJSON() as { action: ResponseAction }).action;
      cell.posts.push(action);
      const recorded = responseRecorded(action);
      if (action !== "send_to_inspect") return json(recorded);
      return json({
        ...recorded,
        production: setProduction("stopped", { reason: "inspection", alert_id: responses[1] }),
      });
    }
    if (responses) return json({ alert_id: responses[1], episode_opened_at: null, responses: [] });
    if (path === "/api/pipelines") return json([]);
    if (path.endsWith("/zerobus-metrics")) return json(zerobusMetrics);
    if (path.endsWith("/lakebase-health")) return json(options.live ? lakebaseHealthAt(serverNow()) : lakebaseHealth);
    if (path.endsWith("/lakebase-perf") && options.live) return json(lakebasePerf(serverNow()));
    if (path === "/api/latency/streaming" && options.live) return json(streamingLatency(serverNow()));
    return json({});
  });
  return cell;
}

async function pinClock(page: Page) {
  await page.clock.install({ time: new Date(CLOCK_T0 * 1000) });
}

function hotStream() {
  return [...sensorUpdates(HOT_SPINDLE), spindleAlertOpened(HOT_SPINDLE[HOT_SPINDLE.length - 1])];
}

function kpi(page: Page, label: string) {
  return page.getByTestId("manufacturing-kpis").locator(".kpi").filter({ hasText: label }).locator(".v");
}

function twin(page: Page) {
  return page.getByRole("region", { name: "Machining cell twin: mill, inspect, pack" });
}

test.describe("[Windsor] Manufacturing operator twin", () => {
  test.use({ timezoneId: "UTC", viewport: { width: 1680, height: 1050 } });
  test.setTimeout(90_000);

  test.describe("line overview", () => {
    test("shows the MILL · INSPECT · PACK cell with Spindle 3 selected [C7310][smoke]", async ({ page }) => {
      await mockCell(page, { sse: sensorUpdates(NORMAL_SPINDLE) });
      await page.goto("/i/manufacturing/realtime/dashboard");

      const cell = twin(page);
      await expect(cell).toBeVisible();
      await expect(cell.locator("canvas")).toBeVisible();
      for (const name of ["MILL · Spindle 3", "INSPECT · Vision QA", "PACK · Pack line"]) {
        await expect(cell.getByRole("button", { name: `Select ${name}` })).toBeVisible();
      }
      await expect(cell.getByRole("button", { name: "Select MILL · Spindle 3" })).toHaveAttribute(
        "aria-pressed",
        "true",
      );

      await expect(page.getByTestId("cell-header")).toContainText("CNC cell");
      await expect(page.getByTestId("production-state")).toContainText("Production running");
      await expect(page.getByTestId("pipeline-chain")).toContainText("Spark RTM");

      const kpis = page.getByTestId("manufacturing-kpis");
      await expect(kpis).not.toContainText(/OEE|Utilization|Production rate/);
      await expect(kpi(page, "Cell state")).toHaveText("Running");
      const foot = (label: string) =>
        kpis.locator(".kpi").filter({ hasText: label }).locator(".cell-kpi__foot");
      await expect(foot("Spindle 3 temperature")).toHaveText("13.8 °C below the limit");
      await expect(foot("Operating limit")).toHaveText("Clears below 55 °C");
      await expect(foot("Spindle 3 status")).toContainText("Prepared heat event");
      await expect(foot("Open alerts")).toHaveText("None this session");
      await expect(foot("Last device event")).toContainText(/ago|just now/);

      await expect(page.getByTestId("incident-panel")).toHaveAttribute("data-state", "clear");
      await expect(page.getByTestId("platform-internals")).not.toHaveAttribute("open", "");
      await expect(page.getByRole("complementary", { name: "Operational alerts" })).toHaveCount(0);

      await page.screenshot({ path: `${EVIDENCE_DIR}/C7310-cell-overview.png`, fullPage: true });
    });
  });

  test.describe("Spindle 3 twin card", () => {
    test("normal readings show temperature, limit, status and device time [C7311][regression]", async ({
      page,
    }) => {
      await mockCell(page, { sse: sensorUpdates(NORMAL_SPINDLE) });
      await page.goto("/i/manufacturing/realtime/dashboard");

      await expect(page.getByTestId("spindle-temperature")).toContainText("46.2");
      await expect(page.getByTestId("operating-limit")).toContainText("60");
      await expect(page.getByTestId("station-status")).toHaveText("Normal");
      await expect(page.getByTestId("device-event-time")).toContainText("09:41:04.000");
      await expect(page.getByTestId("spindle-trend")).toBeVisible();
      await expect(page.getByTestId("heat-event")).toContainText(/next in \d+:\d{2}|in progress/);
      await expect(page.getByText("Reference · MES not connected")).toBeVisible();
      await expect(page.getByText("Spindle bearing inspection · 2026-09-01")).toBeVisible();

      await expect(kpi(page, "Spindle 3 status")).toHaveText("Normal");
      await expect(kpi(page, "Open alerts")).toHaveText("0");
      await expect(kpi(page, "Last device event")).toContainText("09:41:04.000");
      await expect(page.getByTestId("spindle-action")).toHaveCount(0);

      await page.screenshot({ path: `${EVIDENCE_DIR}/C7311-spindle-normal.png`, fullPage: true });
    });

    test("crossing the limit opens an incident with Inspection required [C7312][defect][regression]", async ({
      page,
    }) => {
      await mockCell(page, { sse: hotStream() });
      await page.goto("/i/manufacturing/realtime/dashboard");

      await expect(page.getByTestId("spindle-temperature")).toContainText("66.3");
      await expect(page.getByTestId("station-status")).toHaveText("Over limit");
      await expect(page.getByTestId("spindle-action")).toContainText("Inspection required");
      await expect(kpi(page, "Spindle 3 status")).toHaveText("Over limit");
      await expect(kpi(page, "Open alerts")).toHaveText("1");

      const incident = page.getByTestId("incident-panel");
      await expect(incident).toHaveAttribute("data-state", "alert");
      await expect(incident).toContainText("Action required");
      await expect(incident).toContainText("Spindle 3 over limit");
      await expect(incident).toContainText("M-SPINDLE-OVERTEMP");
      await expect(incident).toContainText("+6.3");
      await expect(incident.getByTestId("incident-action")).toContainText("Inspection required");
      await expect(incident.getByTestId("lifecycle-opened")).toContainText("09:41:04");

      await incident.getByTestId("lifecycle-acknowledged").getByRole("button", { name: "Acknowledge" }).click();
      await expect(incident.getByTestId("lifecycle-acknowledged").locator("time")).toBeVisible();

      await page.screenshot({ path: `${EVIDENCE_DIR}/C7312-spindle-incident.png`, fullPage: true });
    });
  });

  test.describe("other stations", () => {
    test("INSPECT and PACK show only what is streamed [C7313][regression]", async ({ page }) => {
      await mockCell(page, { sse: sensorUpdates(NORMAL_SPINDLE) });
      await page.goto("/i/manufacturing/realtime/dashboard");
      const cell = twin(page);

      await cell.getByRole("button", { name: "Select INSPECT · Vision QA" }).click();
      await expect(page.getByLabel("Vision QA twin card")).toContainText("No sensor feed");

      await cell.getByRole("button", { name: "Select PACK · Pack line" }).click();
      const pack = page.getByLabel("Pack line twin card");
      await expect(pack).toContainText("8.1 m/min");
      await expect(pack).toContainText("12.3 A");

      const deepDive = page.getByTestId("platform-internals");
      await deepDive.locator("summary").click();
      await expect(deepDive.locator(".ptitle").filter({ hasText: "Lakebase write performance" })).toBeVisible();
      await expect(deepDive.getByRole("heading", { name: "Historical · DBSQL" })).toBeVisible();

      await page.screenshot({ path: `${EVIDENCE_DIR}/C7313-pack-and-deep-dive.png`, fullPage: true });
    });
  });

  test.describe("cell sensors and theme", () => {
    test("Spindle 3 lists its other live sensors without invented limits [C7314][regression]", async ({ page }) => {
      await mockCell(page, { sse: sensorUpdates(NORMAL_SPINDLE) });
      await page.goto("/i/manufacturing/realtime/dashboard");

      const tiles = page.getByTestId("station-sensors");
      await expect(tiles).toContainText("Spindle speed");
      await expect(tiles).toContainText("3,548");
      await expect(tiles.locator(".cell-tile").filter({ hasText: "Vibration" })).toContainText("Limit ≤ 8 mm/s");
      await expect(tiles.locator(".cell-tile").filter({ hasText: "Coolant flow" })).toContainText("No approved limit");

      const cell = twin(page);
      await expect(cell.locator(".cell-pin--hero")).toContainText("46.2");
      await expect(cell.getByRole("region", { name: "Spindle 3 sensors" })).toContainText("Spindle speed");
      await expect(page.getByTestId("thermal-scale")).toContainText("limit 60");

      await cell.getByRole("button", { name: "Whole cell" }).click();
      await expect(cell.getByRole("region", { name: "Shop utilities" })).toContainText(/Shop air\s*109/);
      await page.waitForTimeout(1500);
      await page.screenshot({ path: `${EVIDENCE_DIR}/C7314-cell-sensors-dark.png`, fullPage: true });
    });

    test("the twin follows the light page theme [C7315][regression]", async ({ page }) => {
      await page.addInitScript(() => localStorage.setItem("iot-twin-theme", "light"));
      await mockCell(page, { sse: sensorUpdates(NORMAL_SPINDLE) });
      await page.goto("/i/manufacturing/realtime/dashboard");

      const cell = twin(page);
      await expect(cell).toHaveClass(/machining-cell--light/);
      await expect(cell.locator(".cell-pin--hero")).toContainText("46.2");
      await page.screenshot({ path: `${EVIDENCE_DIR}/C7315-cell-light.png`, fullPage: true });

      await page.getByRole("button", { name: "Toggle light / dark" }).click();
      await expect(cell).toHaveClass(/machining-cell--dark/);
    });
  });

  test.describe("operator response", () => {
    test("holding the batch marks PACK on hold and keeps the cell running [C7316][defect][regression]", async ({
      page,
    }) => {
      const mock = await mockCell(page, { sse: hotStream() });
      await page.goto("/i/manufacturing/realtime/dashboard");

      const incident = page.getByTestId("incident-panel");
      await expect(incident).toContainText("Hold the batch at PACK");
      await incident.getByRole("button", { name: "Hold batch" }).click();
      await expect(page.getByTestId("decision-hold_batch")).toContainText("Recorded 09:41:06 by operator");
      expect(mock.posts).toEqual(["hold_batch"]);
      expect(mock.productionPosts).toEqual([]);
      await expect(page.getByTestId("production-state")).toContainText("Production running");
      await expect(incident).toHaveAttribute("data-state", "alert");

      const cell = twin(page);
      await expect(cell.locator(".cell-tag").filter({ hasText: "Pack line" })).toContainText("Batch on hold");
      await cell.getByRole("button", { name: "Select PACK · Pack line" }).click();
      await expect(page.getByTestId("station-decisions")).toContainText("Batch on hold");
      await page.screenshot({ path: `${EVIDENCE_DIR}/C7316-batch-on-hold.png`, fullPage: true });
    });

    test("inspection and maintenance decisions carry their references [C7317][regression]", async ({ page }) => {
      const mock = await mockCell(page, { sse: hotStream() });
      await page.goto("/i/manufacturing/realtime/dashboard");

      const incident = page.getByTestId("incident-panel");
      await incident.getByRole("button", { name: "Stop & inspect" }).click();
      await expect(page.getByTestId("decision-send_to_inspect")).toContainText("Recorded");
      await incident.getByRole("button", { name: "Request maintenance" }).click();
      await expect(page.getByTestId("decision-request_maintenance")).toContainText(
        "last service: Spindle bearing inspection",
      );
      expect(mock.posts).toEqual(["send_to_inspect", "request_maintenance"]);

      const cell = twin(page);
      await expect(cell.locator(".cell-tag").filter({ hasText: "Vision QA" })).toContainText("Inspection requested");
      await cell.getByRole("button", { name: "Select INSPECT · Vision QA" }).click();
      await expect(page.getByLabel("Vision QA twin card")).toContainText("Parts machined since 09:41:04");
      await page.screenshot({ path: `${EVIDENCE_DIR}/C7317-inspect-and-maintenance.png`, fullPage: true });
    });

    test("full view opens the cell immersively and Escape returns [C7318][smoke]", async ({ page }) => {
      await mockCell(page, { sse: hotStream() });
      await page.goto("/i/manufacturing/realtime/dashboard");

      await page.getByRole("button", { name: "Full view" }).click();
      const dialog = page.getByRole("dialog", { name: "Machining cell full view" });
      await expect(dialog).toBeVisible();
      await expect(dialog.locator("canvas")).toBeVisible();
      await expect(dialog.getByTestId("incident-panel")).toContainText("Inspection required");
      await page.waitForTimeout(1500);
      await page.screenshot({ path: `${EVIDENCE_DIR}/C7318-full-view.png` });

      await page.keyboard.press("Escape");
      await expect(dialog).toHaveCount(0);
      await expect(twin(page)).toBeVisible();
    });
  });

  test.describe("production stop and resume", () => {
    test("sending parts to inspection stops production until it is resumed [C7330][defect][regression]", async ({
      page,
    }) => {
      await pinClock(page);
      const mock = await mockCell(page, { sse: hotStream(), admin: true, live: true });
      await page.goto("/i/manufacturing/realtime/dashboard");
      await expect(page.getByTestId("spindle-temperature")).toContainText("66.3");

      const incident = page.getByTestId("incident-panel");
      await incident.getByRole("button", { name: "Stop & inspect" }).click();

      await expect(page.getByTestId("production-state")).toContainText("Production stopped");
      await expect(page.getByTestId("production-state")).toContainText("inspection");
      await expect(kpi(page, "Cell state")).toHaveText("Stopped");
      await expect(incident).toHaveAttribute("data-state", "stopped");
      await expect(incident.getByTestId("production-stop")).toContainText("CNC-1 and the pack line send no readings");
      await expect(incident.getByTestId("lifecycle-stopped").locator("time")).toBeVisible();
      const cell = twin(page);
      const banner = cell.getByTestId("cell-hold");
      await expect(banner).toContainText("Production stopped · inspection");
      await expect(banner).toContainText("Last machine reading 09:41:04");
      await expect(cell).toHaveClass(/is-held/);
      await expect(page.getByTestId("metric-cell-feed")).toHaveClass(/tone-held/);

      // The producer is silent: reconnects replay only the readings from before the stop.
      await page.waitForTimeout(5000);
      await expect(kpi(page, "Spindle 3 temperature")).toContainText("66.3");
      await expect(kpi(page, "Last device event")).toContainText("09:41:04");
      await page.screenshot({ path: `${EVIDENCE_DIR}/C7330-production-stopped.png`, fullPage: true });

      await incident.getByTestId("production-stop").getByRole("button", { name: "Resume production" }).click();
      expect(mock.productionPosts).toEqual(["running"]);
      await expect(page.getByTestId("production-state")).toContainText("Production restarting");
      await expect(banner).toContainText("waiting for the first CNC-1 reading");

      await expect(page.getByTestId("production-state")).toContainText("Production running", { timeout: 20_000 });
      await expect(kpi(page, "Spindle 3 temperature")).toContainText("58.2");
      await expect(kpi(page, "Cell state")).toHaveText("Running");
      await expect(banner).toHaveCount(0);
      await expect(incident.getByTestId("lifecycle-resumed").locator("time")).toBeVisible();
      await expect(page.getByTestId("metric-cell-feed").locator(".cellops-metric__value")).toHaveText(/^8\s*of 8$/);
      expect(mock.posts).toEqual(["send_to_inspect"]);
      await page.screenshot({ path: `${EVIDENCE_DIR}/C7330-production-resumed.png`, fullPage: true });
    });

    test("resuming after hold and inspection releases the batch so parts move again [C7335][defect][regression]", async ({
      page,
    }) => {
      await pinClock(page);
      const mock = await mockCell(page, { sse: hotStream(), admin: true, live: true });
      await page.goto("/i/manufacturing/realtime/dashboard");
      await expect(page.getByTestId("spindle-temperature")).toContainText("66.3");

      const incident = page.getByTestId("incident-panel");
      const cell = twin(page);
      const packTag = cell.locator(".cell-tag").filter({ hasText: "Pack line" });
      await incident.getByRole("button", { name: "Hold batch" }).click();
      await expect(packTag).toContainText("Batch on hold");
      await expect(cell).toHaveAttribute("data-line", "held-at-pack");

      await incident.getByRole("button", { name: "Stop & inspect" }).click();
      await expect(page.getByTestId("production-state")).toContainText("Production stopped");
      await expect(cell).toHaveAttribute("data-line", "stopped");
      await expect(packTag).toContainText("Batch on hold");

      await incident.getByTestId("production-stop").getByRole("button", { name: "Resume production" }).click();
      await expect(page.getByTestId("production-state")).toContainText("Production running", { timeout: 20_000 });
      await expect(cell).toHaveAttribute("data-line", "moving");
      await expect(packTag).toContainText("Batch released");
      await expect(packTag).not.toContainText("Batch on hold");
      await expect(page.getByTestId("decision-hold_batch")).toContainText("released on resume");
      await expect(page.getByTestId("decision-hold_batch")).toContainText("released on resume");
      await cell.getByRole("button", { name: "Select PACK · Pack line" }).click();
      await expect(page.getByTestId("station-decisions")).toContainText("Batch released");
      expect(mock.posts).toEqual(["hold_batch", "send_to_inspect"]);
      expect(mock.productionPosts).toEqual(["running"]);
      await page.screenshot({ path: `${EVIDENCE_DIR}/C7335-batch-released-on-resume.png`, fullPage: true });
    });

    test("an admin can stop and resume the cell from the header [C7331][regression]", async ({ page }) => {
      await pinClock(page);
      const mock = await mockCell(page, { sse: sensorUpdates(NORMAL_SPINDLE), admin: true, live: true });
      await page.goto("/i/manufacturing/realtime/dashboard");

      await page.getByTestId("header-stop").click();
      await expect(page.getByTestId("production-state")).toContainText("Production stopped");
      await expect(page.getByTestId("production-state")).toContainText("manual stop");
      await expect(page.getByTestId("incident-panel")).toHaveAttribute("data-state", "stopped");
      await expect(page.getByTestId("pipeline-chain")).toContainText("cell stopped");

      await page.getByTestId("header-resume").click();
      await expect(page.getByTestId("production-state")).toContainText(/Production (restarting|running)/);
      await expect(page.getByTestId("production-state")).toContainText("Production running", { timeout: 20_000 });
      expect(mock.productionPosts).toEqual(["stopped", "running"]);
      expect(mock.posts).toEqual([]);
    });

    test("operators without admin rights see the state but no stop control [C7332][regression]", async ({
      page,
    }) => {
      await mockCell(page, { sse: sensorUpdates(NORMAL_SPINDLE) });
      await page.goto("/i/manufacturing/realtime/dashboard");

      await expect(page.getByTestId("production-state")).toContainText("Production running");
      await expect(page.getByTestId("header-stop")).toHaveCount(0);
      await expect(page.getByTestId("header-resume")).toHaveCount(0);
    });
  });

  test.describe("alert history and data platform", () => {
    test("the last hour shows past episodes, decisions and stops [C7333][regression]", async ({ page }) => {
      await pinClock(page);
      const stoppedAt = CLOCK_T0 - 1500;
      await mockCell(page, {
        sse: sensorUpdates(NORMAL_SPINDLE),
        live: true,
        episodes: [clearedEpisode(CLOCK_T0 - 1560, CLOCK_T0 - 1200)],
        changes: [
          productionState("stopped", stoppedAt, { reason: "inspection" }),
          productionState("running", stoppedAt + 240),
        ],
      });
      await page.goto("/i/manufacturing/realtime/dashboard");

      const timeline = page.getByTestId("alert-timeline");
      await expect(timeline).toContainText("1 episode · 1 stop");
      const row = timeline.locator(".cellops-timeline__list li").first();
      await expect(row).toContainText("Spindle 3 temp");
      await expect(row).toContainText("M-SPINDLE-OVERTEMP");
      await expect(row).toContainText("peak 68.4 / 60");
      await expect(row).toContainText("6 min 0 s");
      await expect(row).toContainText("Inspection requested");
      await expect(row).toContainText("cleared");
      await expect(timeline.locator(".cellops-timeline__bar")).toHaveCount(1);
      await expect(timeline.locator(".cellops-timeline__stop")).toHaveCount(1);
      await expect(timeline.locator(".cellops-timeline__decision")).toHaveCount(1);

      await expect(page.getByTestId("last-incident")).toContainText("peak 68.4 °C");
      await timeline.scrollIntoViewIfNeeded();
      await timeline.screenshot({ path: `${EVIDENCE_DIR}/C7333-alert-history.png` });
    });

    test("pipeline health shows Lakebase writes, freshness and Spark RTM latency [C7334][regression]", async ({
      page,
    }) => {
      await pinClock(page);
      await page.addInitScript(() => localStorage.setItem("iot-twin-theme", "light"));
      await mockCell(page, { sse: sensorUpdates(NORMAL_SPINDLE), live: true });
      await page.goto("/i/manufacturing/realtime/dashboard");

      const platform = page.getByTestId("platform-health");
      await expect(platform.getByTestId("metric-lakebase-writes")).toContainText("296");
      await expect(platform.getByTestId("metric-lakebase-writes")).toContainText("17,760 rows/min");
      await expect(platform.getByTestId("metric-lakebase-writes").locator("svg.cellops-metric__trend")).toBeVisible();
      await expect(platform.getByTestId("metric-freshness")).toContainText("88,800 rows in the last 5 min");
      await expect(platform.getByTestId("metric-spark-latency")).toContainText("7 / 13");
      await expect(platform.getByTestId("metric-spark-latency")).toContainText("batch #412");
      await expect(platform.getByTestId("metric-cell-feed")).toContainText("of 8");

      const chain = page.getByTestId("pipeline-chain");
      await expect(chain.locator("li").filter({ hasText: "Lakebase" })).toContainText("296 rows/s");
      await expect(chain.locator("li").filter({ hasText: "Spark RTM" })).toContainText("p50 7 ms");

      await page.screenshot({ path: `${EVIDENCE_DIR}/C7334-platform-light.png`, fullPage: true });
    });
  });
});
