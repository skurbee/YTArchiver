const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

test.describe("plain-language UI states", () => {
  test("setup copy works for both first run and later review", async ({ page }) => {
    await loadApp(page);

    await expect(page.locator("#onboarding-overlay .onb-logo-sub"))
      .toHaveText("setup");
    await expect(page.locator("#btn-run-setup"))
      .toHaveAttribute("title", "Review or change setup");

    await page.evaluate(() => window._startOnboarding({ force: true }));
    await expect(page.locator("#onboarding-overlay")).toBeVisible();
    await expect(page.locator("#onb-intro-title"))
      .toHaveText("Review YTArchiver setup");
    await expect(page.locator("#onb-intro-text"))
      .toContainText("Your existing choices stay in place unless you change them.");
    await expect(page.locator("#onb-next")).toHaveText("Review setup");

    const finalCopy = page.locator('.onb-step[data-step="5"] .onb-text');
    await expect(finalCopy).toContainText("YTArchiver is ready.");
    await expect(finalCopy).toContainText(
      "Browse > Channels to manage subscriptions or add a channel");
    await expect(page.locator("#onboarding-overlay"))
      .not.toContainText("first-time setup");

    await page.locator("#onb-close").click();
    await page.evaluate(() => window._startOnboarding());
    await expect(page.locator("#onb-intro-title"))
      .toHaveText("Welcome to YTArchiver");
    await expect(page.locator("#onb-intro-text"))
      .toContainText("We'll choose your archive folder, traffic limits, and download tools.");
    await expect(page.locator("#onb-next")).toHaveText("Get started");
  });

  test("global pause button always names its current action", async ({ page }) => {
    await loadApp(page);
    const pause = page.locator("#btn-pause");

    await page.evaluate(() => window.setQueueState({
      sync: { running: true, paused: false, pausedActive: false },
      gpu: { running: false, paused: false, pausedActive: false },
    }));
    await expect(pause).toHaveAttribute(
      "aria-label", "Pause all queues (current jobs finish first)");

    await page.evaluate(() => window.setQueueState({
      sync: { running: true, paused: true, pausedActive: true },
      gpu: { running: false, paused: false, pausedActive: false },
    }));
    await expect(pause).toHaveAttribute("aria-label", "Resume all queues");
    await expect(pause.locator("svg")).toHaveAttribute("data-icon", "play");
  });

  test("Bookmarks shows loading before a real empty result", async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => {
      window.__setBridgeHandler("bookmark_list", () => new Promise((resolve) => {
        window.__resolveBookmarkList = resolve;
      }));
      window.__bookmarkRefresh = window.refreshBookmarks();
    });

    const list = page.locator("#bookmarks-list");
    const exportBtn = page.locator("#btn-bookmarks-export");
    await expect(list).toHaveText("Loading bookmarks…");
    await expect(list).toHaveAttribute("aria-busy", "true");
    await expect(exportBtn).toBeDisabled();

    await page.evaluate(() => window.__resolveBookmarkList({ ok: true, rows: [] }));
    await page.evaluate(() => window.__bookmarkRefresh);
    await expect(list).toContainText(
      "No bookmarks yet. Open a video in Watch and click Bookmark, "
      + "or right-click a transcript segment.");
    await expect(list).toHaveAttribute("aria-busy", "false");
    await expect(exportBtn).toBeEnabled();
  });

  test("a bookmark load failure is not presented as an empty library", async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => {
      window.__setBridgeHandler("bookmark_list", () => ({
        ok: false,
        error: "Fixture catalog unavailable",
      }));
    });

    await page.evaluate(() => window.refreshBookmarks());
    const list = page.locator("#bookmarks-list");
    await expect(list).toHaveText("Couldn't load bookmarks. Try again.");
    await expect(list).not.toContainText("No bookmarks yet");
    await expect(page.locator("#btn-bookmarks-export")).toBeDisabled();
  });
});
