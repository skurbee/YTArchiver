const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

for (const state of ["loading", "error", "empty"]) {
  test(`caption preferences cannot revive the previous video's words in ${state} transcript state`, async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => {
      document.querySelector('.tab[data-tab="browse"]').click();
      window.renderWatchView({ title: "First", channel: "Example", video_id: "abcDEF12345" }, [
        { s: 0, e: 100, text: "Earlier caption", words: [
          { w: "Earlier", s: 0, e: 50 }, { w: "caption", s: 50, e: 100 },
        ] },
      ], null, { skipVideoReload: true });
      window.setCaptionPref("size", "medium");
    });
    await expect.poll(() => page.evaluate(() =>
      document.getElementById("watch-video")._capOverlay.classList.contains("show"))).toBe(true);
    await page.evaluate(state => {
      const options = { skipVideoReload: true };
      if (state === "loading") options.transcriptLoading = true;
      if (state === "error") options.transcriptError = "Temporary read failure";
      window.renderWatchView({ title: "Second", channel: "Example", video_id: "zyx987wvu65" }, [], null, options);
      window.setCaptionPref("bg", "opaque");
    }, state);
    expect(await page.evaluate(() =>
      document.getElementById("watch-video")._capOverlay.classList.contains("show"))).toBe(false);
    await expect(page.locator("#watch-title")).toHaveText("Second");
  });
}
