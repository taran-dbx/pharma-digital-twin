import { mockModeDashboardApis } from "../fixtures/dashboard-apis";
import {
  MANUFACTURING,
  PHARMACEUTICAL,
  WRAPPED_PROMPT,
  mockPipelineGenie,
  setGeniePollHooks,
  starterQuestions,
  type PipelineGenieMockOptions,
} from "../fixtures/pipeline-genie";
import { expect as baseExpect, test } from "../fixtures/test";
import { ModeDashboardPage } from "../pages/mode-dashboard.page";
import type { Page } from "@playwright/test";

// Polling + result fetches race the WebGL twin for the main thread; allow slack.
const expect = baseExpect.configure({ timeout: 45_000 });

type PollHooks = { pollIntervalMs?: number; pollDeadlineMs?: number };

async function openCell(page: Page, options: PipelineGenieMockOptions = {}, hooks: PollHooks = { pollIntervalMs: 200 }) {
  await setGeniePollHooks(page, hooks);
  await mockModeDashboardApis(page);
  const mock = await mockPipelineGenie(page, options);
  const dashboard = new ModeDashboardPage(page);
  await dashboard.openIndustryDashboard((options.industry ?? MANUFACTURING).id);
  await dashboard.ensureGenieOpen();
  return { dashboard, mock };
}

test.describe("[Windsor] Per-pipeline Genie on the manufacturing cell", () => {
  // The cell page mounts the WebGL twin, which is slow under headless Chromium;
  // run these serially so parallel twins do not starve each other.
  test.describe.configure({ mode: "serial", timeout: 300_000 });

  test("gates on space status and explains why it is not provisioned [defect]", async ({ page }) => {
    const { dashboard } = await openCell(page, {
      available: false,
      reason: "Space is provisioned after the first deploy.",
    });

    await expect(dashboard.genieStatus).toHaveText("Not provisioned");
    await expect(dashboard.genieDisabled).toContainText("Genie space not provisioned yet");
    await expect(dashboard.genieDisabled).toContainText("Space is provisioned after the first deploy.");
    await expect(dashboard.genieInput).toBeDisabled();
    await expect(page.getByTestId("genie-starters")).toHaveCount(0);
  });

  test("distinguishes an existing but unusable space [defect]", async ({ page }) => {
    const { dashboard } = await openCell(page, {
      available: false,
      spaceId: "space-1",
      reason: "Genie space tables do not match the pipeline allowlist.",
    });

    await expect(dashboard.genieStatus).toHaveText("Unavailable");
    await expect(dashboard.genieDisabled).toContainText("Genie space unavailable");
    await expect(dashboard.genieDisabled).not.toContainText("not provisioned");
    await expect(dashboard.genieDisabled).toContainText("allowlist");
    await expect(dashboard.genieInput).toBeDisabled();
  });

  test("shows an unresolved pipeline (404) without a create action [defect]", async ({ page }) => {
    const { dashboard } = await openCell(page, { statusNotFound: true, isAdmin: true });

    await expect(dashboard.genieStatus).toHaveText("Not found");
    await expect(dashboard.genieDisabled).toContainText("This pipeline could not be resolved");
    await expect(dashboard.genieDisabled).not.toContainText("not provisioned");
    await expect(page.getByTestId("genie-create-space")).toHaveCount(0);
    await expect(dashboard.genieInput).toBeDisabled();
  });

  test("Escape closes the drawer and returns focus to the toggle [a11y]", async ({ page }) => {
    const { dashboard } = await openCell(page);
    await expect(dashboard.genieStatus).toHaveText("Ready");
    await expect(dashboard.genieInput).toBeFocused();

    await page.keyboard.press("Escape");
    await expect(dashboard.genieToggle).toHaveAttribute("aria-expanded", "false");
    await expect(dashboard.genieDrawer).toBeHidden();
    await expect(dashboard.genieToggle).toBeFocused();
  });

  test("a failed send keeps the turn and Retry resends it once [defect]", async ({ page }) => {
    const { dashboard } = await openCell(page, { failFirstSend: true });

    await dashboard.genieInput.fill(starterQuestions[0]);
    await dashboard.genieSend.click();
    await expect(page.getByTestId("genie-error")).toContainText("temporarily unavailable");
    await expect(dashboard.genieTranscript.getByText(starterQuestions[0])).toHaveCount(1);

    await page.getByTestId("genie-retry").click();
    await expect(dashboard.genieTranscript.getByTestId("genie-answer").first()).toContainText("climbed");
    await expect(dashboard.genieTranscript.getByText(starterQuestions[0])).toHaveCount(1);
    await expect(page.getByTestId("genie-error")).toHaveCount(0);
  });

  test("polls through pending statuses; starter → line, follow-up → bar + table [smoke]", async ({ page }) => {
    const { dashboard, mock } = await openCell(page);
    await expect(dashboard.genieStatus).toHaveText("Ready");

    const starters = page.getByRole("group", { name: "Starter questions" });
    await expect(starters.getByRole("button")).toHaveCount(starterQuestions.length);
    await starters.getByRole("button", { name: starterQuestions[0] }).click();

    const first = dashboard.genieTranscript.getByTestId("genie-answer").first();
    await expect(first).toContainText("Spindle temperature climbed");
    expect(mock.polledStatuses.slice(0, 3)).toEqual(["SUBMITTED", "ASKING_AI", "COMPLETED"]);
    await expect(dashboard.genieTranscript).not.toContainText(WRAPPED_PROMPT);

    const lineResult = first.getByTestId("genie-result");
    await expect(lineResult).toHaveAttribute("data-chart-kind", "line");
    await expect(lineResult.getByTestId("genie-result-line")).toBeVisible();
    await expect(lineResult.getByTestId("genie-result-truncated")).toHaveText(/truncated to 3 rows/);
    await expect(first).toContainText("Average spindle temperature per 5 minutes");

    const sql = first.getByTestId("genie-sql");
    await sql.getByText("Show SQL").click();
    await expect(sql.locator("code")).toContainText("SELECT window_start");

    await lineResult.getByTestId("genie-result-view-table").click();
    await expect(lineResult.getByRole("region", { name: "Query result table" })).toContainText("avg_spindle_temp");
    await expect(lineResult.getByTestId("genie-result-view-table")).toHaveAttribute("aria-pressed", "true");

    const followUps = page.getByRole("group", { name: "Suggested follow-up questions" });
    await followUps.getByRole("button", { name: "Which station raised the most alerts?" }).click();

    const second = dashboard.genieTranscript.getByTestId("genie-answer").nth(1);
    const bar = second.getByTestId("genie-result").nth(0);
    const table = second.getByTestId("genie-result").nth(1);
    await expect(bar).toHaveAttribute("data-chart-kind", "bar");
    await expect(bar.getByTestId("genie-result-bar")).toBeVisible();
    await expect(table).toHaveAttribute("data-chart-kind", "table");
    await expect(table.getByTestId("genie-result-table")).toContainText("M-SPINDLE-HOT");
    await expect(table.getByTestId("genie-result-view-chart")).toHaveCount(0);
  });

  test("recovers from an expired conversation (404) by asking again [defect]", async ({ page }) => {
    const { dashboard } = await openCell(page, { expireFirstMessage: true });

    await dashboard.genieInput.fill(starterQuestions[0]);
    await dashboard.genieSend.click();
    await expect(page.getByTestId("genie-error")).toHaveText("This conversation expired - ask again.");
    await expect(dashboard.genieInput).toBeEnabled();

    await dashboard.genieInput.fill("Which station raised the most alerts?");
    await dashboard.genieSend.click();
    await expect(dashboard.genieTranscript.getByTestId("genie-answer").first().getByTestId("genie-result").first()).toHaveAttribute(
      "data-chart-kind",
      "bar",
    );
    await expect(page.getByTestId("genie-error")).toHaveCount(0);
  });

  test("stops polling at the deadline with an explicit timeout [defect]", async ({ page }) => {
    const { dashboard } = await openCell(page, { neverFinish: true }, { pollIntervalMs: 200, pollDeadlineMs: 1500 });

    await dashboard.genieInput.fill(starterQuestions[0]);
    await dashboard.genieSend.click();
    await expect(page.getByTestId("genie-error")).toContainText("taking too long");
    await expect(dashboard.genieStatus).toHaveText("Ready");
    await expect(dashboard.genieInput).toBeEnabled();
  });

  test("shows an explicit, retryable error when a result fails to load [defect]", async ({ page }) => {
    const { dashboard } = await openCell(page, { failResults: true });

    await dashboard.genieInput.fill(starterQuestions[0]);
    await dashboard.genieSend.click();
    const error = dashboard.genieTranscript.getByTestId("genie-result-error");
    await expect(error).toContainText("warehouse unavailable");
    await expect(error.getByRole("button", { name: "Retry" })).toBeVisible();
  });
});

test.describe("[Windsor] Per-pipeline Genie is available for every industry", () => {
  test.describe.configure({ mode: "serial", timeout: 300_000 });

  test("pharmaceutical dashboard uses its own pipeline space and backend starters [smoke]", async ({ page }) => {
    const { dashboard, mock } = await openCell(page, { industry: PHARMACEUTICAL });
    await expect(dashboard.genieStatus).toHaveText("Ready");

    const starters = page.getByRole("group", { name: "Starter questions" });
    await expect(starters.getByRole("button")).toHaveCount(PHARMACEUTICAL.starters.length);
    await expect(starters.getByRole("button", { name: PHARMACEUTICAL.starters[0] })).toBeVisible();
    await expect(starters).not.toContainText(MANUFACTURING.starters[0]);

    await starters.getByRole("button", { name: PHARMACEUTICAL.starters[0] }).click();
    await expect(dashboard.genieTranscript.getByTestId("genie-answer").first()).toContainText("climbed");
    expect(mock.polledStatuses.slice(0, 3)).toEqual(["SUBMITTED", "ASKING_AI", "COMPLETED"]);
  });

  test("pharmaceutical: no space yet tells non-admins to ask an admin [defect]", async ({ page }) => {
    const { dashboard } = await openCell(page, {
      industry: PHARMACEUTICAL,
      available: false,
      reason: "No Genie space is provisioned for this pipeline yet.",
    });
    await expect(dashboard.genieStatus).toHaveText("Not provisioned");
    await expect(dashboard.genieDisabled).toContainText("Genie space not provisioned yet");
    await expect(dashboard.genieDisabled).toContainText("Ask an admin");
    await expect(page.getByTestId("genie-create-space")).toHaveCount(0);
    await expect(dashboard.genieInput).toBeDisabled();
  });

  test("pharmaceutical: admin creates the missing space and chat unlocks [smoke]", async ({ page }) => {
    const { dashboard, mock } = await openCell(page, {
      industry: PHARMACEUTICAL,
      available: false,
      isAdmin: true,
      reason: "No Genie space is provisioned for this pipeline yet.",
    });
    await expect(dashboard.genieDisabled).toContainText("Genie space not provisioned yet");
    await page.getByTestId("genie-create-space").click();

    await expect(dashboard.genieStatus).toHaveText("Ready");
    expect(mock.provisionCalls).toEqual(["POST"]);
    await expect(dashboard.genieDisabled).toHaveCount(0);
    await expect(dashboard.genieInput).toBeEnabled();
  });

  test("admin sees an explicit error when creating the space fails [defect]", async ({ page }) => {
    const { dashboard } = await openCell(page, {
      industry: PHARMACEUTICAL,
      available: false,
      isAdmin: true,
      provisionError: "Genie provisioning is not supported for this industry.",
    });
    await page.getByTestId("genie-create-space").click();
    await expect(page.getByTestId("genie-create-error")).toContainText("not supported");
    await expect(dashboard.genieStatus).toHaveText("Not provisioned");
    await expect(page.getByTestId("genie-create-space")).toBeEnabled();
  });

  test("pharmaceutical: unknown pipeline (404) is an informative empty state [defect]", async ({ page }) => {
    const { dashboard } = await openCell(page, { industry: PHARMACEUTICAL, statusNotFound: true, isAdmin: true });
    await expect(dashboard.genieStatus).toHaveText("Not found");
    await expect(dashboard.genieDisabled).toContainText("This pipeline could not be resolved");
    await expect(page.getByTestId("genie-create-space")).toHaveCount(0);
    await expect(dashboard.genieInput).toBeDisabled();
  });

  test("pharmaceutical: unsupported industry has no create action [defect]", async ({ page }) => {
    const { dashboard } = await openCell(page, { industry: PHARMACEUTICAL, unsupported: true, isAdmin: true });
    await expect(dashboard.genieStatus).toHaveText("Not available");
    await expect(dashboard.genieDisabled).toContainText("not available for this industry");
    await expect(page.getByTestId("genie-create-space")).toHaveCount(0);
    await expect(dashboard.genieInput).toBeDisabled();
  });

  test("starter question failure degrades to a hint, chat still works [defect]", async ({ page }) => {
    const { dashboard } = await openCell(page, { industry: PHARMACEUTICAL, startersFail: true });
    await expect(dashboard.genieStatus).toHaveText("Ready");
    await expect(page.getByText("Starter questions unavailable.")).toBeVisible();
    await expect(dashboard.genieInput).toBeEnabled();
  });
});
