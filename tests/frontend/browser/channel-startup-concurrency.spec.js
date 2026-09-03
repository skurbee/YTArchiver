const { test, expect } = require("@playwright/test");
const { installBridgeStub, loadApp } = require("./fixtures");

test.describe("Channels startup concurrency", () => {
  test("Health statistics stay lazy and cannot hold up Channels", async ({ page }) => {
    await loadApp(page);

    // initIndexControls used to start this hidden, multi-second aggregate on
    // every cold launch (and once more from its delayed bridge fallback).
    // Give both former eager paths time to fire before asserting they are gone.
    await page.waitForTimeout(950);
    expect(await page.evaluate(() =>
      window.__bridgeCallsFor("get_index_db_stats").length)).toBe(0);

    await page.evaluate(() => {
      window.__setBridgeHandler("get_index_summary", () => ({
        cards: { channels: 1, videos: 7, physical_copies: 7 },
      }));
      window.__indexStatsReleased = false;
      window.__setBridgeHandler("get_index_db_stats", () =>
        new Promise((resolve) => {
          window.__resolveIndexStats = (value) => {
            window.__indexStatsReleased = true;
            resolve(value);
          };
        }));
    });

    await page.locator('.tab[data-tab="health"]').click();
    expect(await page.evaluate(() =>
      window.__bridgeCallsFor("get_index_db_stats").length)).toBe(0);

    await page.locator(
      '#panel-health .settings-subnav-btn[data-settings-view="library"]'
    ).click();
    await expect.poll(() => page.evaluate(() =>
      typeof window.__resolveIndexStats)).toBe("function");
    expect(await page.evaluate(() =>
      window.__bridgeCallsFor("get_index_db_stats").length)).toBe(1);

    // Once the user leaves Health, that explicit diagnostic read is still
    // allowed to finish. It uses a separate backend SQLite connection and
    // therefore must not keep the foreground Channels hydration queued.
    await page.evaluate(() => {
      window.__channelHydrationStarted = false;
      window.__setBridgeHandler("browse_list_channels", () => {
        window.__channelHydrationStarted = true;
        return [{
          folder: "Lane Channel",
          name: "Lane Channel",
          n_vids: 7,
          size: "700 MB",
        }];
      });
    });
    await page.locator('.tab[data-tab="browse"]').click();
    await page.evaluate(() => {
      window.__channelHydrationDone = false;
      window.__channelHydration = window._primeBrowse([{
        folder: "Lane Channel",
        name: "Lane Channel",
      }]).then(() => {
        window.__channelHydrationDone = true;
      });
    });

    await expect.poll(() => page.evaluate(() =>
      window.__channelHydrationStarted)).toBe(true);
    await expect.poll(() => page.evaluate(() =>
      window.__channelHydrationDone)).toBe(true);
    await expect(page.locator(
      '#channel-grid .channel-card[data-channel-name="Lane Channel"] '
      + ".channel-card-meta"
    )).toContainText("7 videos");
    expect(await page.evaluate(() => window.__indexStatsReleased)).toBe(false);

    await page.evaluate(() => window.__resolveIndexStats({
      segments: 99,
      hours: 1.5,
      total_videos: 7,
      transcribed_videos: 6,
      index_db_size_label: "5 MB",
    }));
    await expect.poll(() => page.evaluate(() =>
      window.YT.bridge.catalogReadBusy("index-stats"))).toBe(false);
    await expect(page.locator("#stat-segments")).toHaveText("99");
    await expect(page.locator("#search-stat-segments")).toHaveText("99");
  });

  test("a visible Library request retries once when the bridge becomes ready", async ({ page }) => {
    await loadApp(page, { bridgeDelayed: true });

    await page.locator('.tab[data-tab="health"]').click();
    await page.locator(
      '#panel-health .settings-subnav-btn[data-settings-view="library"]'
    ).click();
    await expect(page.locator("#index-stats-text")).toContainText("loading");

    await page.evaluate(installBridgeStub);
    await page.evaluate(() => {
      window.__setBridgeHandler("get_index_summary", () => ({
        cards: { channels: 2, videos: 8, physical_copies: 8 },
      }));
      window.__setBridgeHandler("get_index_db_stats", () =>
        new Promise((resolve) => {
          window.__resolveDelayedIndexStats = resolve;
        }));
      // A repeated ready signal and the 800ms safety fallback must not turn
      // the one pending user request into duplicate archive-wide aggregates.
      window.dispatchEvent(new Event("pywebviewready"));
      window.dispatchEvent(new Event("pywebviewready"));
    });

    await expect.poll(() => page.evaluate(() =>
      window.__bridgeCallsFor("get_index_db_stats").length)).toBe(1);
    await page.waitForTimeout(900);
    expect(await page.evaluate(() =>
      window.__bridgeCallsFor("get_index_db_stats").length)).toBe(1);

    await page.evaluate(() => window.__resolveDelayedIndexStats({
      segments: 123,
      hours: 2,
      total_videos: 8,
      transcribed_videos: 6,
      index_db_size_label: "6 MB",
    }));
    await expect(page.locator("#index-stats-text"))
      .toContainText("Segments: 123");
  });

  test("a pre-ready Library request remains lazy while Health is hidden", async ({ page }) => {
    await loadApp(page, { bridgeDelayed: true });

    await page.locator('.tab[data-tab="health"]').click();
    await page.locator(
      '#panel-health .settings-subnav-btn[data-settings-view="library"]'
    ).click();
    await page.locator('.tab[data-tab="browse"]').click();

    await page.evaluate(installBridgeStub);
    await page.evaluate(() => {
      window.dispatchEvent(new Event("pywebviewready"));
    });
    await page.waitForTimeout(900);
    expect(await page.evaluate(() =>
      window.__bridgeCallsFor("get_index_db_stats").length)).toBe(0);

    // Library is still the selected Health page. Returning to Health is the
    // first point where the deferred request becomes visible and may run.
    await page.locator('.tab[data-tab="health"]').click();
    await expect.poll(() => page.evaluate(() =>
      window.__bridgeCallsFor("get_index_db_stats").length)).toBe(1);
  });
});
