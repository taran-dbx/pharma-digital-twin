import { test as base, expect } from "@playwright/test";

import { adminCatalog, adminUser } from "./catalog";

type Fixtures = {
  mockAdminApi: void;
};

export const test = base.extend<Fixtures>({
  mockAdminApi: [
    async ({ page }, use) => {
      await page.route("**/api/**", async (route) => {
        const pathname = new URL(route.request().url()).pathname;
        if (pathname === "/api/me") {
          await route.fulfill({ json: adminUser });
          return;
        }
        if (pathname === "/api/industries/catalog") {
          await route.fulfill({ json: adminCatalog });
          return;
        }
        if (pathname === "/api/industries/runtime") {
          await route.fulfill({ json: { pipelines: [] } });
          return;
        }
        await route.fulfill({
          status: 503,
          contentType: "application/json",
          body: JSON.stringify({ detail: `Unexpected test request: ${pathname}` }),
        });
      });
      await use();
    },
    { auto: true },
  ],
});

export { expect };
