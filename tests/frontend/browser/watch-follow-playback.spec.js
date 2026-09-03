const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

test("Watch always follows the active transcript segment without a setting", async ({ page }) => {
  await loadApp(page);
  await page.locator('.tab[data-tab="browse"]').click();

  await page.evaluate(() => {
    window.showView("watch");

    const video = document.getElementById("watch-video");
    window.__fixturePlaybackTime = 0.25;
    Object.defineProperty(video, "currentTime", {
      configurable: true,
      get: () => window.__fixturePlaybackTime,
    });

    window.__followedSegmentStarts = [];
    window._scrollTranscriptTo = (_container, target) => {
      window.__followedSegmentStarts.push(Number(target.dataset.s));
    };

    window.renderWatchView({
      video_id: "follow-fixture",
      title: "Follow fixture",
    }, [
      { s: 0, e: 0.9, t: "First segment", w: [] },
      { s: 1, e: 1.9, t: "Second segment", w: [] },
    ], null, { skipVideoReload: true });
  });

  await expect(page.locator("#watch-autofollow")).toHaveCount(0);
  await expect(page.getByText("Follow playback", { exact: true })).toHaveCount(0);
  await expect.poll(() => page.evaluate(() =>
    window.__followedSegmentStarts.slice())).toEqual([0]);

  await page.evaluate(() => { window.__fixturePlaybackTime = 1.25; });
  await expect.poll(() => page.evaluate(() =>
    window.__followedSegmentStarts.slice())).toEqual([0, 1]);
});
