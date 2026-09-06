const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

async function prepare(page, cards = {}) {
  await loadApp(page);
  await page.evaluate((overrides) => {
    window.__savedScan = { cards: {
      channels: 4, videos: 12, physical_copies: 13, size_label: "4 GB",
      scan_complete: false, scanned_channels: 1, total_channels: 4,
      ...overrides,
    } };
    window.__catalogStats = {
      total_videos: 100, transcribed_videos: 75, segments: 1000,
      hours: 50, index_db_size_label: "8 MB",
    };
    window.__setBridgeHandler("get_index_summary", () => window.__savedScan);
    window.__setBridgeHandler("get_index_db_stats", () => window.__catalogStats);
    window.__setBridgeHandler("index_summary", () => ({ videos: 100, segments: 1000, channels: 5 }));
  }, cards);
}

test("partial saved scans never stand in for the full catalog count", async ({ page }) => {
  await prepare(page);
  await page.evaluate(() => window._refreshIndexStats());
  const stats = page.locator("#index-stats-text");
  await expect(stats).toContainText("Videos in catalog: 100");
  await expect(stats).not.toContainText("Videos in catalog: 12");
  await expect(stats).toContainText("Saved channel scan: 1 of 4 current channels (incomplete)");
  await expect(stats).toContainText("Videos in saved scan: 12");
  await expect(stats).toContainText("Files in saved scan: 13");
  await expect(stats).toContainText("Size in saved scan: 4 GB");
  await page.evaluate(() => window._refreshHealthOverview());
  await expect(page.locator("#health-overview-archive-value")).toHaveText("Incomplete saved scan");
  await expect(page.locator("#health-overview-archive-detail"))
    .toHaveText("Saved channel scan · 1 of 4 current channels · 12 videos · 13 files · 4 GB");
  await expect(page.locator("#health-overview-index-value")).toHaveText("100 available videos");
});

test("catalog count stays loading while the real database aggregate is pending", async ({ page }) => {
  await prepare(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("get_index_db_stats", () => new Promise(resolve => {
      window.__finishCatalogStats = resolve;
    }));
    window.__statsRead = window._refreshIndexStats();
  });
  const stats = page.locator("#index-stats-text");
  await expect(stats).toContainText("Videos in catalog: loading…");
  await expect(stats).toContainText("Videos in saved scan: 12");
  await page.evaluate(async () => {
    window.__finishCatalogStats(window.__catalogStats);
    await window.__statsRead;
  });
  await expect(stats).toContainText("Videos in catalog: 100");
});

for (const failure of ["error response", "rejected request", "missing response"]) {
  test(`catalog ${failure} shows unavailable values instead of saved scan totals`, async ({ page }) => {
    await prepare(page);
    await page.evaluate((kind) => {
      window.__setBridgeHandler("get_index_db_stats", () => {
        if (kind === "rejected request") throw new Error("Fixture unavailable");
        if (kind === "missing response") return null;
        return { total_videos: 0, segments: 0, hours: 0, error: "Fixture unavailable" };
      });
    }, failure);
    await page.evaluate(() => window._refreshIndexStats());
    const stats = page.locator("#index-stats-text");
    await expect(stats).toContainText("Videos in catalog: —");
    await expect(stats).toContainText("Segments: —");
    await expect(stats).toContainText("Detailed statistics could not be loaded. Try again later.");
    await expect(stats).toContainText("Videos in saved scan: 12");
    await expect(stats).not.toContainText("Videos in catalog: 0");
  });
}

test("a failed saved scan does not prevent loading the real catalog count", async ({ page }) => {
  await prepare(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("get_index_summary", () => { throw new Error("Fixture unavailable"); });
  });
  await page.evaluate(() => window._refreshIndexStats());
  await expect(page.locator("#index-stats-text")).toContainText("Videos in catalog: 100");
  await expect(page.locator("#index-stats-text")).toContainText("Saved channel scan: Unavailable");
  await expect(page.locator("#index-stats-text")).toContainText("Files in saved scan: —");
  await page.evaluate(() => window._refreshHealthOverview());
  await expect(page.locator("#health-overview-archive-value")).toHaveText("Unavailable");
  await expect(page.locator("#health-overview-index-value")).toHaveText("100 available videos");
});

test("complete scans still describe current subscriptions separately from the full catalog", async ({ page }) => {
  await prepare(page, { scan_complete: true, scanned_channels: 4 });
  await page.evaluate(() => window._refreshIndexStats());
  await expect(page.locator("#index-stats-text")).toContainText("Videos in catalog: 100");
  await expect(page.locator("#index-stats-text"))
    .toContainText("Saved channel scan: 4 of 4 current channels (complete)");
  await page.evaluate(() => window._refreshHealthOverview());
  await expect(page.locator("#health-overview-archive-value")).toHaveText("12 videos");
  await expect(page.locator("#health-overview-archive-detail"))
    .toHaveText("Saved channel scan · 4 of 4 current channels · 13 files · 4 GB");
  await expect(page.locator("#health-overview-archive-value").locator("..")).not.toHaveClass(/is-warn/);
});

test("legacy scan payloads do not imply complete coverage and empty subscriptions stay honest", async ({ page }) => {
  await prepare(page);
  await page.evaluate(() => {
    delete window.__savedScan.cards.scan_complete;
    delete window.__savedScan.cards.scanned_channels;
    delete window.__savedScan.cards.total_channels;
  });
  await page.evaluate(() => window._refreshIndexStats());
  await expect(page.locator("#index-stats-text")).toContainText("Saved channel scan: Coverage unknown");
  await page.evaluate(() => window._refreshHealthOverview());
  await expect(page.locator("#health-overview-archive-value")).toHaveText("Scan coverage unknown");
  await page.evaluate(() => {
    window.__savedScan.cards = { channels: 0, videos: 0, physical_copies: 0,
      size_label: "0 B", scan_complete: true, scanned_channels: 0, total_channels: 0 };
    window.__catalogStats.total_videos = 0;
  });
  await page.evaluate(() => window._refreshIndexStats());
  await expect(page.locator("#index-stats-text")).toContainText("Videos in catalog: 0");
  await expect(page.locator("#index-stats-text")).toContainText("Saved channel scan: 0 of 0 current channels (complete)");
  await page.evaluate(() => window._refreshHealthOverview());
  await expect(page.locator("#health-overview-archive-value")).toHaveText("No subscribed channels");
});
