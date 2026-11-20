import type { Page } from "@playwright/test";

export class PlatformPage {
  constructor(private readonly page: Page) {}

  readonly allPipelines = this.page.getByRole("link", { name: "All pipelines" });
  readonly themeToggle = this.page.getByRole("button", { name: "Toggle light / dark" });

  async open() {
    await this.page.goto("/");
  }

  async viewportMetrics() {
    return this.page.evaluate(() => {
      const actions = document.querySelector(".actions")?.getBoundingClientRect();
      return {
        viewportWidth: window.innerWidth,
        documentWidth: document.documentElement.scrollWidth,
        actionsRight: actions?.right ?? Number.POSITIVE_INFINITY,
      };
    });
  }
}
