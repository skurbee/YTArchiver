const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

test.beforeEach(async ({ page }) => { await loadApp(page); });

async function mediaFixture(page) {
  await page.evaluate(() => {
    const video = document.getElementById("watch-video");
    window.mediaState = { source: "", position: 0, ready: 0, plays: 0 };
    const state = window.mediaState;
    Object.defineProperties(video, {
      src: { configurable: true, get: () => state.source, set: value => { state.source = value; } },
      currentTime: { configurable: true, get: () => state.position, set: value => { state.position = value; } },
      readyState: { configurable: true, get: () => state.ready },
      duration: { configurable: true, get: () => state.ready ? 600 : NaN },
    });
    const remove = video.removeAttribute.bind(video);
    video.removeAttribute = name => { if (name === "src") state.source = ""; remove(name); };
    video.load = () => { state.ready = 0; };
    video.pause = () => {};
    video.play = () => { state.plays++; return Promise.resolve(); };
    window.__setBridgeHandler("browse_get_transcript", () => ({ segments: [] }));
    window.__setBridgeHandler("browse_video_url", filepath => ({ ok: true, url: filepath }));
  });
}

test("a background sign-in alert cannot approve the confirmation beneath it", async ({ page }) => {
  await page.evaluate(() => {
    window.dangerResult = null;
    window.askDanger("Delete fixture", "Delete this fixture?", "Delete")
      .then(value => { window.dangerResult = value; });
  });
  await expect(page.getByRole("button", { name: "Cancel", exact: true })).toBeFocused();
  await page.evaluate(() => window.dispatchEvent(new CustomEvent("yt-control", { detail: { kind: "cookie_alert" } })));
  await expect(page.getByRole("button", { name: "Got it", exact: true })).toBeFocused();
  await page.keyboard.press("Enter");
  await expect.poll(() => page.evaluate(() => window.dangerResult)).toBeNull();
  await expect(page.getByRole("button", { name: "Cancel", exact: true })).toBeFocused();
  await page.keyboard.press("Enter");
  await expect.poll(() => page.evaluate(() => window.dangerResult)).toBe(false);
});

test("stale media responses cannot clear newer media or autoplay after a tab change", async ({ page }) => {
  await mediaFixture(page);
  const result = await page.evaluate(async () => {
    let oldResponse;
    window.__setBridgeHandler("browse_video_url", path => path.includes("Old")
      ? new Promise(resolve => { oldResponse = resolve; }) : { ok: true, url: path });
    await window._openVideoInWatch({ title: "Old", video_id: "OLD00000001", filepath: "C:\\FixtureArchive\\Old.mp4" });
    await window._openVideoInWatch({ title: "New", video_id: "NEW00000001", filepath: "C:\\FixtureArchive\\New.mp4" });
    await new Promise(resolve => setTimeout(resolve, 20));
    oldResponse({ ok: true, url: "C:\\FixtureArchive\\Old.mp4" });
    await new Promise(resolve => setTimeout(resolve, 20));
    const selected = window.mediaState.source;
    let hiddenResponse;
    window.__setBridgeHandler("browse_video_url", () => new Promise(resolve => { hiddenResponse = resolve; }));
    await window._openVideoInWatch({ title: "Hidden", video_id: "HIDDEN00001", filepath: "C:\\FixtureArchive\\Hidden.mp4" });
    document.querySelector('.tab[data-tab="download"]').click();
    const plays = window.mediaState.plays;
    document.querySelector('.tab[data-tab="browse"]').click();
    hiddenResponse({ ok: true, url: "C:\\FixtureArchive\\Hidden.mp4" });
    await new Promise(resolve => setTimeout(resolve, 20));
    return { selected, newPlays: window.mediaState.plays - plays };
  });
  expect(result.selected).toContain("New.mp4");
  expect(result.newPlays).toBe(0);
});

test("bookmark seeks belong to one video and normal current-video seeks still work", async ({ page }) => {
  await mediaFixture(page);
  const result = await page.evaluate(async () => {
    await window._openVideoInWatch({ title: "Old", video_id: "OLD00000001", filepath: "C:\\FixtureArchive\\Old.mp4", _seek_to: 120 });
    await window._openVideoInWatch({ title: "New", video_id: "NEW00000001", filepath: "C:\\FixtureArchive\\New.mp4" });
    await new Promise(resolve => setTimeout(resolve, 20));
    window.mediaState.ready = 1;
    document.getElementById("watch-video").dispatchEvent(new Event("loadedmetadata"));
    const stalePosition = window.mediaState.position;
    window._seekWatchTo(42);
    return { stalePosition, currentPosition: window.mediaState.position };
  });
  expect(result).toEqual({ stalePosition: 0, currentPosition: 42 });
});

test("Ctrl+F after Watch opens the visible Search field", async ({ page }) => {
  await mediaFixture(page);
  await page.evaluate(() => window._openVideoInWatch({ title: "Example", filepath: "C:\\FixtureArchive\\Example.mp4" }));
  await page.locator('.tab[data-tab="download"]').click();
  await page.keyboard.press("Control+f");
  await expect(page.locator("#search-query")).toBeFocused();
  await expect(page.locator("#view-search")).toBeVisible();
});

test("Watch redownload passes the exact selected physical copy", async ({ page }) => {
  await mediaFixture(page);
  await page.evaluate(async () => {
    window.redownloadArguments = null;
    window.askChoice = async () => "360";
    window.__setBridgeHandler("video_redownload", (...args) => {
      window.redownloadArguments = args;
      return { ok: true };
    });
    await window._openVideoInWatch({ title: "Two copies", video_id: "TWOCOPIES01", filepath: "D:\\OtherRoot\\Two copies.mp4", tracked: true });
    document.getElementById("btn-watch-redownload").click();
  });
  await expect.poll(() => page.evaluate(() => window.redownloadArguments))
    .toEqual(["TWOCOPIES01", "Two copies", "360", "D:\\OtherRoot\\Two copies.mp4"]);
});

test("channel filter survives Watch and the header menu works without a channel card", async ({ page }) => {
  await mediaFixture(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("browse_list_videos_page", (_channel, _sort, _limit, _offset, query) => ({
      rows: [{ title: "Apple", video_id: "APPLE000001", filepath: "C:\\FixtureArchive\\Apple.mp4" },
        { title: "Banana", video_id: "BANANA00001", filepath: "C:\\FixtureArchive\\Banana.mp4" }].filter(row => !query || row.title.includes(query)), has_more: false,
    }));
    document.querySelector('.tab[data-tab="browse"]').click();
    window._browseState.currentChannel = { folder: "Example", name: "Example" };
    window.showView("videos");
    return window.loadVideosFor(window._browseState.currentChannel);
  });
  await page.locator("#browse-filter").fill("Apple");
  await expect(page.locator("#video-grid .video-card")).toHaveCount(1);
  await page.evaluate(() => window._openVideoInWatch(window._browseState.videos[0]));
  await page.locator("#browse-back-btn").click();
  await expect(page.locator("#browse-filter")).toHaveValue("Apple");
  await expect(page.locator("#video-grid .video-card")).toHaveCount(1);
  await page.evaluate(() => document.getElementById("channel-grid").replaceChildren());
  await page.locator("#cph-more").click();
  await expect(page.locator("#ctx-menu-root")).toContainText("Fix file dates only");
});

test("channel pages and scroll survive an unchanged top-level tab roundtrip", async ({ page }) => {
  await page.evaluate(() => {
    const rows = Array.from({ length: 400 }, (_, i) => ({ title: `Example ${i}`, video_id: `VID${String(i).padStart(8, "0")}`, filepath: `C:\\FixtureArchive\\${i}.mp4` }));
    window.__setBridgeHandler("browse_list_videos_page", (_channel, _sort, limit, offset) => ({ rows: rows.slice(offset, offset + limit), next_offset: Math.min(rows.length, offset + limit), has_more: offset + limit < rows.length }));
    document.querySelector('.tab[data-tab="browse"]').click();
    document.getElementById("browse-group-year").checked = false;
    document.getElementById("browse-group-month").checked = false;
    window._browseState.currentChannel = { folder: "Example", name: "Example" };
    window.showView("videos");
    return window.loadVideosFor(window._browseState.currentChannel);
  });
  await page.evaluate(() => { const view = document.getElementById("view-videos"); view.scrollTop = view.scrollHeight; });
  await expect(page.locator("#video-grid .video-card")).toHaveCount(240);
  const before = await page.locator("#view-videos").evaluate(el => el.scrollTop);
  await page.locator('.tab[data-tab="download"]').click();
  await page.locator('.tab[data-tab="browse"]').click();
  await page.waitForTimeout(200);
  await expect(page.locator("#video-grid .video-card")).toHaveCount(240);
  expect(await page.locator("#view-videos").evaluate(el => el.scrollTop)).toBe(before);
});

test("Graph drill replaces channel scope and includes both calendar years of an ISO week", async ({ page }) => {
  await page.evaluate(() => {
    window._setSearchSelectedChannels(["Old channel"]);
    window.__setBridgeHandler("browse_search", () => []);
    window._drillIntoSearch("example", "2026-W01", "week", null);
  });
  await expect(page.locator("#search-channel-label")).toHaveText("All channels");
  await expect(page.locator("#search-year-from")).toHaveValue("2025");
  await expect(page.locator("#search-year-to")).toHaveValue("2026");
  expect(await page.evaluate(() => window._searchSelectedChannels())).toEqual([]);
});

test("bookmark buttons retain keyboard actions and date-only dates retain their day", async ({ page }) => {
  await page.evaluate(() => {
    window.bookmarkJumps = [];
    window.__setBridgeHandler("bookmark_list", () => [{ id: 1, title: "Example", channel: "Example", video_id: "EXAMPLE0001", filepath: "C:\\FixtureArchive\\Example.mp4", uploaded: "2026-09-05", start_time: 10, text: "Example segment" }]);
    window._openVideoInWatch = item => { window.bookmarkJumps.push(item); };
    document.querySelector('.tab[data-tab="browse"]').click();
    document.querySelector('[data-submode="bookmarks"]').click();
  });
  await expect(page.locator(".bookmark-card-meta")).toContainText("Sep 5");
  await page.locator(".bookmark-remove").focus();
  await page.keyboard.press("Enter");
  await expect(page.getByRole("dialog", { name: "Delete bookmark" })).toBeVisible();
  expect(await page.evaluate(() => window.bookmarkJumps)).toEqual([]);
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  await page.locator(".bookmark-note-btn").focus();
  await page.keyboard.press("Space");
  await expect(page.getByRole("dialog")).toContainText("note");
  expect(await page.evaluate(() => window.bookmarkJumps)).toEqual([]);
});

test("a small scroll does not fetch pages and a new upload does not duplicate the next page", async ({ page }) => {
  await page.evaluate(() => {
    window.rows = Array.from({ length: 300 }, (_, i) => ({ title: `Video ${i}`, video_id: `VIDEO${String(i).padStart(6, "0")}`, filepath: `C:\\FixtureArchive\\${i}.mp4` }));
    window.__setBridgeHandler("list_all_videos", (_sort, limit, offset) => ({ rows: window.rows.slice(offset, offset + limit), has_more: offset + limit < window.rows.length }));
    document.querySelector('.tab[data-tab="browse"]').click();
    document.querySelector('[data-submode="recent"]').click();
  });
  await expect(page.locator("#recent-grid .video-card")).toHaveCount(60);
  await page.locator("#view-recent").evaluate(el => { el.scrollTop = 5; });
  await page.waitForTimeout(100);
  await expect(page.locator("#recent-grid .video-card")).toHaveCount(60);
  await page.evaluate(async () => {
    window.rows.unshift({ title: "New", video_id: "NEWUPLOAD01", filepath: "C:\\FixtureArchive\\New.mp4" });
    await window._refreshVideosViewIfActive();
  });
  await expect(page.locator("#recent-grid .video-card")).toHaveCount(61);
  await page.locator("#view-recent").evaluate(el => { el.scrollTop = el.scrollHeight; });
  await expect(page.locator("#recent-grid .video-card")).toHaveCount(121);
  const ids = await page.locator("#recent-grid .video-card").evaluateAll(cards => cards.map(card => card.dataset.videoId));
  expect(new Set(ids).size).toBe(ids.length);
});
