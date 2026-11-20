import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  disabledGenieStatus,
  mockModeDashboardApis,
} from "../fixtures/dashboard-apis";
import { expect, test } from "../fixtures/test";
import { ModeDashboardPage } from "../pages/mode-dashboard.page";

const evidenceDir = fileURLToPath(
  new URL("../../../../qa-reports/evidence/", import.meta.url),
);

test.describe("[Windsor] Genie investigation copilot", () => {
  test.describe("mode dashboard drawer", () => {
    test("docks a right-side investigation panel with polling selectors [C7506][smoke]", async ({
      page,
    }, testInfo) => {
      await mockModeDashboardApis(page);
      const dashboard = new ModeDashboardPage(page);
      await dashboard.openRealtime();
      await dashboard.ensureGenieOpen();

      await expect(dashboard.genie).toBeVisible();
      await expect(dashboard.genieDrawer).toBeVisible();
      await expect(dashboard.genieTranscript).toBeVisible();
      await expect(dashboard.genieInput).toBeVisible();
      await expect(dashboard.genieInput).toBeEnabled();
      await expect(dashboard.genieSend).toBeVisible();
      await expect(dashboard.genieStatus).toHaveText("Ready");
      await expect(dashboard.genieDisabled).toHaveCount(0);

      await mkdir(evidenceDir, { recursive: true });
      const fileName = "C7506-genie-copilot-drawer.png";
      await page.screenshot({ path: testInfo.outputPath(fileName), fullPage: true });
      await page.screenshot({ path: join(evidenceDir, fileName), fullPage: true });
      await testInfo.attach("genie-enabled-log", {
        body: JSON.stringify(
          {
            url: page.url(),
            status: await dashboard.genieStatus.innerText(),
            expanded: await dashboard.genieToggle.getAttribute("aria-expanded"),
          },
          null,
          2,
        ),
        contentType: "application/json",
      });
    });

    test("shows disabled copy when Genie is unbound [C7507][defect]", async ({
      page,
    }, testInfo) => {
      await mockModeDashboardApis(page, { genieStatus: disabledGenieStatus });
      const dashboard = new ModeDashboardPage(page);
      await dashboard.openRealtime();
      await dashboard.ensureGenieOpen();

      await expect(dashboard.genieStatus).toHaveText("Disabled");
      await expect(dashboard.genieDisabled).toContainText("Genie isn’t connected");
      await expect(dashboard.genieDisabled).not.toContainText("GENIE_SPACE_ID");
      await expect(dashboard.genieDisabled).not.toContainText("valueFrom");
      await expect(dashboard.genieInput).toBeDisabled();
      await expect(dashboard.genieSend).toBeDisabled();

      await mkdir(evidenceDir, { recursive: true });
      const fileName = "C7507-genie-disabled.png";
      await page.screenshot({ path: testInfo.outputPath(fileName), fullPage: true });
      await page.screenshot({ path: join(evidenceDir, fileName), fullPage: true });
      await testInfo.attach("genie-disabled-log", {
        body: JSON.stringify(
          {
            url: page.url(),
            status: await dashboard.genieStatus.innerText(),
            reason: await dashboard.genieDisabled.innerText(),
          },
          null,
          2,
        ),
        contentType: "application/json",
      });
    });
  });

  test.describe("mobile drawer", () => {
    test.use({ viewport: { width: 390, height: 844 } });

    test("keeps the investigation drawer stacked without overflow [C7508][regression]", async ({
      page,
    }, testInfo) => {
      await mockModeDashboardApis(page);
      const dashboard = new ModeDashboardPage(page);
      await dashboard.openRealtime();
      await dashboard.ensureGenieOpen();

      await expect(dashboard.genieDrawer).toBeVisible();
      const metrics = await dashboard.viewportOverflow();
      expect(metrics.documentWidth).toBeLessThanOrEqual(metrics.viewportWidth);

      await mkdir(evidenceDir, { recursive: true });
      const fileName = "C7508-genie-drawer-mobile.png";
      await page.screenshot({ path: testInfo.outputPath(fileName), fullPage: true });
      await page.screenshot({ path: join(evidenceDir, fileName), fullPage: true });
      await testInfo.attach("genie-mobile-layout", {
        body: JSON.stringify(metrics, null, 2),
        contentType: "application/json",
      });
    });
  });
});
