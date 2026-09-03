const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

test.describe("Browse post-sync summaries", () => {
  test("a lightweight completion refresh updates landing cards and library totals", async ({ page }) => {
    await loadApp(page);

    await page.evaluate(async () => {
      window.__weekSummary = {
        ok: true,
        new_videos: 0,
        new_channels: 0,
        total_channels: 2,
        channel_list: [],
      };
      window.__setBridgeHandler(
        "browse_week_summary", () => ({ ...window.__weekSummary }));
      window.__setBridgeHandler("browse_list_channels", () => [{
        folder: "Existing Channel",
        name: "Existing Channel",
        n_vids: 2,
        size: "10 MB",
      }, {
        folder: "Disposable Test Channel",
        name: "Disposable Test Channel",
        n_vids: 0,
        size: "—",
      }]);
      window.__freshSubsRows = [{
        folder: "Existing Channel",
        n_vids: "2",
        size: "10 MB",
        size_bytes: 10485760,
      }, {
        folder: "Disposable Test Channel",
        // get_subs_channels deliberately returns display-formatted strings.
        n_vids: "5",
        size: "44 MB",
        size_bytes: 46137344,
      }];
      window.__setBridgeHandler("get_subs_channels", () => [
        window.__freshSubsRows.map((row) => ({ ...row })),
        "Total: 54 MB",
      ]);

      await window._primeBrowse([{
        folder: "Existing Channel",
        n_vids: "2",
        size: "10 MB",
      }, {
        folder: "Disposable Test Channel",
        n_vids: "—",
        size: "—",
      }]);
      window._browseState.currentChannel = null;
      document.getElementById("stat-channels").textContent = "1";
      document.getElementById("stat-videos").textContent = "2";
      window.__catalogReadsBeforeCompletion =
        window.__bridgeCallsFor("browse_list_channels").length;
    });

    const testCard = page.locator(
      '#channel-grid .channel-card[data-channel-name="Disposable Test Channel"]');
    await expect(testCard.locator(".channel-card-meta")).toContainText("0 videos");

    // Ordinary navigation only redraws the cached channel state. It must not
    // be the mechanism required to discover a completed sync's fresh count.
    await page.evaluate(() => {
      window.showView("watch");
      window.showView("channels");
    });
    await expect(testCard.locator(".channel-card-meta")).toContainText("0 videos");

    await page.evaluate(async () => {
      window.__weekSummary = {
        ok: true,
        new_videos: 5,
        new_channels: 1,
        total_channels: 2,
        channel_list: ["Disposable Test Channel"],
      };
      await window.refreshSubsTable({ primeBrowse: false });
    });

    await expect(testCard.locator(".channel-card-meta")).toContainText("5 videos");
    await expect(testCard.locator(".channel-card-meta")).toContainText("44 MB");
    await expect(page.locator("#stat-channels")).toHaveText("2");
    await expect(page.locator("#stat-videos")).toHaveText("7");
    await expect(page.locator("#bsb-new-videos")).toHaveText("5");
    await expect(page.locator("#bsb-new-channels")).toHaveText("1");
    await expect(page.locator("#bsb-total-channels")).toHaveText("2");

    const state = await page.evaluate(() => ({
      catalogReads: window.__bridgeCallsFor("browse_list_channels").length,
      catalogReadsBeforeCompletion: window.__catalogReadsBeforeCompletion,
      countType: typeof window._browseState.channels.find(
        (row) => row.folder === "Disposable Test Channel")?.n_vids,
      count: window._browseState.channels.find(
        (row) => row.folder === "Disposable Test Channel")?.n_vids,
    }));
    expect(state.catalogReads).toBe(state.catalogReadsBeforeCompletion);
    expect(state.countType).toBe("number");
    expect(state.count).toBe(5);
  });
});
