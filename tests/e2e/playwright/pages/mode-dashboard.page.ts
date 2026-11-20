import { expect, type Page } from "@playwright/test";

export class ModeDashboardPage {
  constructor(private readonly page: Page) {}

  readonly heading = this.page.getByRole("heading", {
    level: 1,
    name: /Manufacturing twin/,
  });
  readonly alertRail = this.page.getByRole("complementary", {
    name: "Operational alerts",
  });
  readonly emptyAlerts = this.alertRail.getByText("No open alerts");
  readonly genie = this.page.getByTestId("genie-copilot");
  readonly genieToggle = this.page.getByTestId("genie-copilot-toggle");
  readonly genieDrawer = this.page.getByTestId("genie-copilot-drawer");
  readonly genieTranscript = this.page.getByTestId("genie-copilot-transcript");
  readonly genieInput = this.page.getByTestId("genie-copilot-input");
  readonly genieSend = this.page.getByTestId("genie-copilot-send");
  readonly genieStatus = this.page.getByTestId("genie-status");
  readonly genieDisabled = this.page.getByTestId("genie-disabled");

  async openRealtime() {
    await this.page.goto("/i/manufacturing/realtime/dashboard");
    await expect(this.heading).toBeVisible();
  }

  /** Manufacturing cell page (requires a deployed realtime mode in the catalog). */
  async openManufacturingCell() {
    await this.page.goto("/i/manufacturing/realtime/dashboard");
    await expect(this.genieToggle).toBeVisible();
  }

  /** Industry dashboard with a deployed realtime mode (any industry). */
  async openIndustryDashboard(industryId: string) {
    await this.page.goto(`/i/${industryId}/realtime/dashboard`);
    await expect(this.genieToggle).toBeVisible();
  }

  async ensureGenieOpen() {
    await expect(this.genieToggle).toBeVisible();
    await expect(this.genieToggle).toHaveAttribute("aria-expanded", /true|false/);
    if ((await this.genieToggle.getAttribute("aria-expanded")) !== "true") {
      await this.genieToggle.click();
    }
    await expect(this.genieDrawer).toBeVisible();
  }

  async viewportOverflow() {
    return this.page.evaluate(() => ({
      viewportWidth: window.innerWidth,
      documentWidth: document.documentElement.scrollWidth,
      alertRailWidth: document.querySelector(".alert-rail")?.scrollWidth ?? 0,
      dashWidth: document.querySelector(".dash-with-alerts")?.scrollWidth ?? 0,
    }));
  }
}
