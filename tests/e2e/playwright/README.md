# Playwright UI QA

This suite tests the committed production SPA in `static/dist`. It serves the
bundle with SPA fallback behavior and mocks only the APIs required by each
scenario.

```bash
cd tests/e2e/playwright
npm ci
npx playwright install chromium
npm test
```

Set `PLAYWRIGHT_BASE_URL` to run the same tests against a deployed app instead
of the local static server.

Reports are written to `qa-reports/` (repo root, gitignored):

- `results/playwright.xml` — JUnit for CI/TestQuality ingestion
- `results/playwright.json` — input for `qa-shadow-report`
- `playwright-report/` — HTML report
- `playwright-artifacts/` — screenshots, traces, and videos

Optional local setup: to drive the browser from an editor's MCP client (e.g.
Cursor), add a Playwright MCP server to your local, untracked `.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "playwright": {
      "command": "npx",
      "args": ["--yes", "@playwright/mcp@0.0.81"]
    }
  }
}
```

`.cursor/` is gitignored, so this config stays per-developer. Reload the editor
after changing it so its browser tools are rediscovered. The test suite itself
does not depend on it.
