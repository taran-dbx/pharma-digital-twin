import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  mockModeDashboardApis,
  sampleClearedAlert,
} from "../fixtures/dashboard-apis";
import { expect, test } from "../fixtures/test";
import { ModeDashboardPage } from "../pages/mode-dashboard.page";

const evidenceDir = fileURLToPath(
  new URL("../../../../qa-reports/evidence/", import.meta.url),
);

test.describe("[Windsor] Mode dashboard alert rail", () => {
  test.beforeEach(async ({ page }) => {
    await mockModeDashboardApis(page);
  });

  test.describe("realtime dashboard", () => {
    test("renders the mode dashboard heading and empty alert rail [C7412][smoke]", async ({
      page,
    }, testInfo) => {
      const dashboard = new ModeDashboardPage(page);
      await dashboard.openRealtime();

      await expect(dashboard.heading).toBeVisible();
      await expect(page.getByText("Real-time mode")).toBeVisible();
      await expect(dashboard.alertRail).toBeVisible();
      await expect(dashboard.emptyAlerts).toBeVisible();
      await expect(dashboard.alertRail.getByText("Alerts", { exact: true })).toBeVisible();
      await expect(
        dashboard.alertRail.getByRole("button", { name: "Acknowledge" }),
      ).toHaveCount(0);

      await mkdir(evidenceDir, { recursive: true });
      const fileName = "C7412-alert-rail-empty.png";
      await page.screenshot({ path: testInfo.outputPath(fileName), fullPage: true });
      await page.screenshot({ path: join(evidenceDir, fileName), fullPage: true });
      await testInfo.attach("alert-rail-empty-log", {
        body: JSON.stringify(
          {
            url: page.url(),
            heading: await dashboard.heading.innerText(),
            emptyCopy: await dashboard.emptyAlerts.innerText(),
          },
          null,
          2,
        ),
        contentType: "application/json",
      });
    });
  });

  test.describe("mobile dashboard chrome", () => {
    test.use({ viewport: { width: 390, height: 844 } });

    test("keeps the stacked alert rail inside the mobile viewport [C7413][regression]", async ({
      page,
    }, testInfo) => {
      const dashboard = new ModeDashboardPage(page);
      await dashboard.openRealtime();
      await expect(dashboard.alertRail).toBeVisible();
      await expect(dashboard.emptyAlerts).toBeVisible();

      const metrics = await dashboard.viewportOverflow();
      expect(metrics.documentWidth).toBeLessThanOrEqual(metrics.viewportWidth);
      expect(metrics.dashWidth).toBeLessThanOrEqual(metrics.viewportWidth);

      await mkdir(evidenceDir, { recursive: true });
      const fileName = "C7413-alert-rail-mobile.png";
      await page.screenshot({ path: testInfo.outputPath(fileName), fullPage: true });
      await page.screenshot({ path: join(evidenceDir, fileName), fullPage: true });
      await testInfo.attach("alert-rail-mobile-layout", {
        body: JSON.stringify(metrics, null, 2),
        contentType: "application/json",
      });
    });
  });

  test("keeps Explain this alert inside the rail [C7414][defect][regression]", async ({
    page,
  }, testInfo) => {
    await mockModeDashboardApis(page, { alerts: [sampleClearedAlert] });
    const dashboard = new ModeDashboardPage(page);
    await dashboard.openRealtime();

    const explain = dashboard.alertRail.getByRole("button", { name: /Explain this alert/ });
    await expect(explain).toBeVisible();

    const overflow = await page.evaluate(() => {
      const rail = document.querySelector(".alert-rail");
      const actions = document.querySelector(".alert-rail__actions");
      if (!rail || !actions) return { ok: false };
      const railBox = rail.getBoundingClientRect();
      const actionBox = actions.getBoundingClientRect();
      return {
        ok: true,
        actionRight: actionBox.right,
        railRight: railBox.right,
      };
    });
    expect(overflow.ok).toBe(true);
    expect(overflow.actionRight).toBeLessThanOrEqual((overflow.railRight ?? 0) + 1);

    await mkdir(evidenceDir, { recursive: true });
    const fileName = "C7414-alert-actions-wrap.png";
    await page.screenshot({ path: testInfo.outputPath(fileName), fullPage: true });
    await page.screenshot({ path: join(evidenceDir, fileName), fullPage: true });
  });
});
