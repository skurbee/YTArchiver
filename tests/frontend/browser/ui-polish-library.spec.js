const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

test.beforeEach(async ({ page }) => { await loadApp(page); });

test("media and metadata start before a delayed transcript and are not restarted", async ({ page }) => {
  await page.evaluate(() => {
    window.__setBridgeHandler("browse_get_transcript", () => new Promise(resolve => { window.finishTranscript = resolve; }));
    window.__setBridgeHandler("browse_video_url", () => ({ ok: false, error: "No fixture media" }));
    window.__setBridgeHandler("browse_get_video_metadata", () => ({ ok: true, meta: {
      duration: 120, upload_date: "20250101", view_count: 1200, description: "Saved description",
    } }));
    window.opening = window._openVideoInWatch({ title: "Fixture", channel: "Example",
      video_id: "EXAMPLE0001", filepath: "C:\\FixtureArchive\\Fixture.mp4" });
  });
  await expect.poll(() => page.evaluate(() => window.__bridgeCallsFor("browse_video_url").length)).toBe(1);
  await expect(page.locator("#watch-meta-description")).toContainText("Saved description");
  await expect(page.locator("#watch-meta")).toContainText("2:00");
  await expect(page.locator("#watch-meta")).toContainText("views");
  await expect(page.locator("#watch-transcript")).toContainText("You can play");
  await expect(page.locator("#btn-bookmark-now")).toBeDisabled();
  await page.evaluate(async () => {
    window.finishTranscript({ segments: [{ s: 0, e: 2, t: "New transcript" }] });
    await window.opening;
  });
  await expect(page.locator("#watch-transcript")).toContainText("New transcript");
  await expect(page.locator("#btn-bookmark-now")).toBeEnabled();
  expect(await page.evaluate(() => window.__bridgeCallsFor("browse_video_url").length)).toBe(1);
  expect(await page.evaluate(() => window.__bridgeCallsFor("browse_get_video_metadata").length)).toBe(1);
});

test("a late transcript cannot replace a newer video or its error state", async ({ page }) => {
  await page.evaluate(() => {
    window.__setBridgeHandler("browse_video_url", () => ({ ok: false }));
    window.__setBridgeHandler("browse_get_transcript", payload => payload.title === "Old"
      ? new Promise(resolve => { window.finishOld = resolve; })
      : { ok: false, error: "Transcript check timed out. Try again." });
    window.oldOpen = window._openVideoInWatch({ title: "Old", video_id: "OLD00000001", filepath: "C:\\FixtureArchive\\Old.mp4" });
  });
  await page.evaluate(() => window._openVideoInWatch({ title: "New", video_id: "NEW00000001", filepath: "C:\\FixtureArchive\\New.mp4" }));
  await page.evaluate(async () => { window.finishOld({ segments: [{ s: 0, e: 2, t: "Stale text" }] }); await window.oldOpen; });
  await expect(page.locator("#watch-title")).toHaveText("New");
  await expect(page.locator("#watch-transcript")).toContainText("timed out");
  await expect(page.locator("#watch-transcript")).not.toContainText("Stale text");
});

test("Search reader has an explicit timestamped Play action with saved summary fields", async ({ page }) => {
  await page.evaluate(() => {
    window.__setBridgeHandler("browse_search", () => [{ segment_id: 1, title: "Result", channel: "Example",
      video_id: "RESULT00001", jsonl_path: "fixture.jsonl", start_time: 22, text: "matching example" }]);
    window.__setBridgeHandler("browse_search_titles", () => []);
    window.__setBridgeHandler("browse_search_context", () => ({ ok: true, segments: [{ id: 1, s: 22, t: "matching example" }] }));
    window.__setBridgeHandler("browse_resolve_segment", () => ({ ok: true, filepath: "C:\\FixtureArchive\\Result.mp4",
      video_id: "RESULT00001", duration: "4:00", views: "2.0K", upload_ts: 1735689600 }));
    window._openVideoInWatch = video => { window.playedResult = video; };
    document.querySelector('.tab[data-tab="browse"]').click();
    document.querySelector('[data-submode="search"]').click();
  });
  await page.locator("#search-query").fill("example");
  await page.locator("#btn-search-run").click();
  await page.locator(".search-result").first().click();
  await page.getByRole("button", { name: "Play at 0:22", exact: true }).click();
  await expect.poll(() => page.evaluate(() => window.playedResult?._seek_to)).toBe(22);
  expect(await page.evaluate(() => ({ duration: window.playedResult.duration, views: window.playedResult.views })))
    .toEqual({ duration: "4:00", views: "2.0K" });
});

test("Graph date filter stays visible during loading and can be cleared", async ({ page }) => {
  await page.evaluate(() => {
    window.__setBridgeHandler("browse_search", () => new Promise(resolve => { window.finishSearch = resolve; }));
    window.__setBridgeHandler("browse_search_titles", () => []);
    window._drillIntoSearch("example", "2025-06", "month", null);
  });
  await expect(page.locator("#search-date-filter-label")).toHaveText("June 2025");
  await expect(page.locator("#search-count")).not.toContainText("matches");
  await expect.poll(() => page.evaluate(() => typeof window.finishSearch)).toBe("function");
  await page.evaluate(() => { window.__setBridgeHandler("browse_search", () => []); window.finishSearch([]); });
  await page.locator("#search-date-filter-clear").click();
  await expect(page.locator("#search-date-filter")).toBeHidden();
  await expect(page.locator("#search-query")).toHaveValue("example");
  await expect.poll(() => page.evaluate(() => window.__bridgeCallsFor("browse_search").length)).toBe(2);
  expect(await page.evaluate(() => window.__bridgeCallsFor("browse_search").at(-1).args.slice(-2))).toEqual([null, null]);
});

test("channel filtering reports its scope and clear restores the full collection", async ({ page }) => {
  await page.evaluate(() => {
    const rows = ["Apple", "Banana"].map(title => ({ title, video_id: title.padEnd(11, "0"), filepath: `C:\\FixtureArchive\\${title}.mp4` }));
    window.__setBridgeHandler("browse_list_videos_page", (_channel, _sort, _limit, _offset, query) => ({
      rows: rows.filter(row => !query || row.title.toLowerCase().includes(query.toLowerCase())), has_more: false,
    }));
    document.querySelector('.tab[data-tab="browse"]').click();
    window._browseState.currentChannel = { folder: "Example", n_vids: 2, size: "10 MB" };
    window.showView("videos");
    return window.loadVideosFor(window._browseState.currentChannel);
  });
  await page.locator("#browse-filter").fill("Apple");
  await expect(page.locator("#cph-info")).toContainText("1 of 2 videos");
  await expect(page.locator("#cph-info")).toContainText("10 MB total in channel");
  await page.locator("#browse-filter-clear").click();
  await expect(page.locator("#video-grid .video-card")).toHaveCount(2);
  await expect(page.locator("#browse-filter")).toBeFocused();
});

test("bookmark filters cover notes and multiline notes save only on explicit confirmation", async ({ page }) => {
  await page.evaluate(() => {
    window.__setBridgeHandler("bookmark_list", query => ({ ok: true, rows: query === "absent" ? [] : [
      { id: 1, title: "Example", channel: "Fixture", video_id: "EXAMPLE0001", start_time: -1, note: "Research note" },
    ] }));
    window.__setBridgeHandler("bookmark_update_note", () => ({ ok: true }));
    document.querySelector('.tab[data-tab="browse"]').click();
    document.querySelector('[data-submode="bookmarks"]').click();
  });
  await page.locator("#bookmarks-filter").fill("Research");
  await expect.poll(() => page.evaluate(() => window.__bridgeCallsFor("bookmark_list").at(-1).args[0])).toBe("Research");
  await page.locator(".bookmark-note-btn").click();
  const field = page.getByRole("textbox", { name: "Edit note" });
  await field.fill("First line");
  await field.press("End");
  await field.press("Enter");
  await field.pressSequentially("Second line");
  await expect(field).toHaveValue("First line\nSecond line");
  expect(await page.evaluate(() => window.__bridgeCallsFor("bookmark_update_note").length)).toBe(0);
  const width = await field.evaluate(el => el.getBoundingClientRect().width / el.closest(".askq-dialog").getBoundingClientRect().width);
  expect(width).toBeGreaterThan(0.85);
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await expect.poll(() => page.evaluate(() => window.__bridgeCallsFor("bookmark_update_note").length)).toBe(1);
  expect(await page.evaluate(() => window.__bridgeCallsFor("bookmark_update_note")[0].args[1])).toBe("First line\nSecond line");
  await page.locator("#bookmarks-filter").fill("absent");
  await expect(page.locator("#bookmarks-list")).toContainText('No bookmarks match "absent"');
});

test("bookmark saving shows pending feedback and coalesces repeated clicks", async ({ page }) => {
  await page.evaluate(() => {
    window.__setBridgeHandler("bookmark_add", () => new Promise(resolve => { window.finishBookmark = resolve; }));
    window.payload = { video_id: "EXAMPLE0001", start_time: -1, title: "Example" };
    window.saving = window._saveBookmark(window.payload);
  });
  await expect(page.locator("#toast-root")).toContainText("Saving bookmark");
  expect(await page.evaluate(() => window._saveBookmark(window.payload))).toEqual({ pending: true });
  expect(await page.evaluate(() => window.__bridgeCallsFor("bookmark_add").length)).toBe(1);
  await page.evaluate(async () => { window.finishBookmark({ ok: false, error: "Library busy; try again." }); window.saved = await window.saving; });
  await expect(page.locator("#toast-root")).not.toContainText("Saving bookmark");
  expect(await page.evaluate(() => window.saved.ok)).toBe(false);
});

test("Manual filter is sent before pagination", async ({ page }) => {
  await page.evaluate(() => {
    window.__setBridgeHandler("list_manual_videos", (_sort, _limit, _offset, query) => ({
      rows: query ? [{ title: "Older match", channel: "Example", filepath: "C:\\FixtureArchive\\Old.mp4" }] : [],
      total: query ? 1 : 147, unfiltered_total: 147, folder: "C:\\FixtureArchive", has_more: false,
    }));
    document.querySelector('.tab[data-tab="browse"]').click();
    document.querySelector('[data-submode="manual"]').click();
  });
  await page.locator("#manual-filter").fill("Older");
  await expect(page.locator("#manual-grid")).toContainText("Older match");
  await expect(page.locator("#manual-folder-label")).toContainText("1 of 147");
  expect(await page.evaluate(() => window.__bridgeCallsFor("list_manual_videos").at(-1).args.slice(2))).toEqual([0, "Older"]);
});

test("Graph export retains rectangular data with units and scope", async ({ page }) => {
  await page.evaluate(() => {
    window.__setBridgeHandler("browse_graph", () => ({ labels: ["2025-06", "2025-07"], values: [4, 6] }));
    window.__setBridgeHandler("save_text_to_file", (_name, csv) => { window.exportedCsv = csv; return { ok: true }; });
    document.querySelector('.tab[data-tab="browse"]').click();
    document.querySelector('[data-submode="graph"]').click();
  });
  await page.locator("#graph-word").fill("example");
  await page.locator("#btn-graph-run").click();
  await expect(page.locator("#graph-empty")).toHaveText("");
  await page.locator("#btn-graph-export-csv").click();
  const csv = await page.evaluate(() => window.exportedCsv);
  expect(csv).toContain("units,channel_scope,normalization,bucket_size,counting_rule");
  expect(csv).toContain("Matching transcript segments,All channels,Off,month");
  expect(csv.split("\n")).toHaveLength(3);
});
