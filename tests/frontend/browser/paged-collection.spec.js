const { test, expect, loadApp } = require("./fixtures");

const views = [
  { name: "Videos", mode: "recent", grid: "recent-grid", view: "view-recent",
    method: "list_all_videos", sort: "videos-sort", refresh: "_refreshVideosViewIfActive" },
  { name: "Manual", mode: "manual", grid: "manual-grid", view: "view-manual",
    method: "list_manual_videos", sort: "manual-sort", refresh: "_refreshManualViewIfActive" },
  { name: "Channel", mode: "videos", grid: "video-grid", view: "view-videos",
    method: "browse_list_videos_page", sort: "browse-sort", refresh: "_refreshChannelVideosIfLoaded" },
];

async function prepare(page, view, scenario) {
  await loadApp(page);
  await page.locator('.tab[data-tab="browse"]').click();
  // Finish the tab-entry refresh before installing this scenario's endpoint;
  // that refresh has its own animation-frame callback in the actual app.
  await page.evaluate(() => new Promise((resolve) =>
    requestAnimationFrame(() => requestAnimationFrame(resolve))));
  await page.evaluate(({ view, scenario }) => {
    window.__pageRequests = [];
    window.__pageReleases = [];
    window.__pageData = (names, offset = 0, hasMore = false) => ({
      rows: names.map((name) => ({
        title: name, video_id: name, filepath: `C:\\FixtureArchive\\${name}.mp4`,
        channel: "Paged Fixture", duration: "1:00", uploaded: "2020",
      })), next_offset: offset + names.length, has_more: hasMore,
      folder: "C:\\FixtureArchive",
    });
    window.__setBridgeHandler(view.method, (...args) => {
      const offset = args[view.mode === "videos" ? 3 : 2];
      window.__pageRequests.push(offset);
      const call = window.__pageRequests.length;
      if (scenario === "late" || (scenario === "refresh" && call === 2)) {
        return new Promise((resolve) => { window.__pageReleases.push(resolve); });
      }
      if (scenario === "retry") {
        if (call === 1) return window.__pageData(["First"], 0, true);
        if (call === 2) return { error: "Temporarily unavailable" };
        return window.__pageData([], offset, false);
      }
      if (call === 1) return window.__pageData(["First", "Second"], 0, true);
      return window.__pageData(["New arrival", "First", "Second", "Third", "Fourth"]);
    });
    // Exercise scroll request ownership independently of the number of columns
    // available in this test viewport. The real listeners and page loaders run.
    window.YT.util.nearScrollBottom = () => true;
    if (view.mode === "videos") {
      const channel = { name: "Paged Fixture", folder: "Paged Fixture", n_vids: 8 };
      window._browseState.currentChannel = channel;
      window.showView("videos");
      window.loadVideosFor(channel);
    } else {
      document.querySelector(`[data-submode="${view.mode}"]`).click();
    }
  }, { view, scenario });
}

async function scroll(page, view) {
  await page.evaluate(async (id) => {
    document.getElementById(id).dispatchEvent(new Event("scroll"));
    await new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)));
  }, view.view);
}

for (const view of views) {
  test(`${view.name} coalesces refreshes received while a later page is loading`, async ({ page }) => {
    await prepare(page, view, "refresh");
    const titles = page.locator(`#${view.grid} .video-card-title`);
    await expect(titles).toHaveCount(2);
    await scroll(page, view);
    await expect.poll(() => page.evaluate(() => window.__pageReleases.length)).toBe(1);
    await page.evaluate((refresh) => {
      window[refresh]();
      window[refresh]();
    }, view.refresh);
    // No refresh can supersede the page's native read or clear its loader.
    expect(await page.evaluate(() => window.__pageRequests)).toEqual([0, 2]);
    await expect(titles).toHaveCount(2);
    await page.evaluate(() => {
      window.__pageReleases[0](window.__pageData(["Third", "Fourth"], 2, true));
    });
    await expect(titles).toHaveCount(5);
    expect(new Set(await titles.allTextContents())).toEqual(
      new Set(["New arrival", "First", "Second", "Third", "Fourth"]));
    expect(await page.evaluate(() => window.__pageRequests)).toEqual([0, 2, 0]);
  });

  test(`${view.name} retries the same failed offset and stops after an empty final page`, async ({ page }) => {
    await prepare(page, view, "retry");
    const titles = page.locator(`#${view.grid} .video-card-title`);
    await expect(titles).toHaveText("First");
    await scroll(page, view);
    await expect.poll(() => page.evaluate(() => window.__pageRequests.length)).toBe(2);
    await expect(titles).toHaveText("First");
    await scroll(page, view);
    await expect.poll(() => page.evaluate(() => window.__pageRequests.length)).toBe(3);
    await scroll(page, view);
    await scroll(page, view);
    expect(await page.evaluate(() => window.__pageRequests)).toEqual([0, 1, 1]);
    await expect(titles).toHaveText("First");
  });

  test(`${view.name} ignores the old response and its completion after a sort reset`, async ({ page }) => {
    await prepare(page, view, "late");
    await expect.poll(() => page.evaluate(() => window.__pageReleases.length)).toBe(1);
    await page.locator(`#${view.sort}`).selectOption("oldest");
    await page.evaluate(() => window.__pageReleases[0](window.__pageData(["Old response"])));
    await expect.poll(() => page.evaluate(() => window.__pageReleases.length)).toBe(2);
    await expect(page.locator(`#${view.grid}`)).not.toContainText("Old response");
    await page.evaluate(() => window.__pageReleases[1](window.__pageData(["Current response"])));
    await expect(page.locator(`#${view.grid} .video-card-title`)).toHaveText("Current response");
    expect(await page.evaluate(() => window.__pageRequests)).toEqual([0, 0]);
  });
}
