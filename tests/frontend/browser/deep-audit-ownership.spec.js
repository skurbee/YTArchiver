const { test, expect, loadApp } = require("./fixtures");

test("a prompt waits for its consumer and duplicate delivery does not reopen it", async ({ page }) => {
  await loadApp(page);
  const result = await page.evaluate(() => {
    const event = { channel: "test", key: "prompt", revision: 1, topic: "control",
      payload: { kind: "test_prompt" } };
    const before = window._appEventBatch([event]);
    let displayed = 0;
    const stop = window.YT.eventState.onControl("test_prompt", () => { displayed++; });
    const first = window._appEventBatch([event]);
    const retry = window._appEventBatch([event]);
    stop();
    return { before, first, retry, displayed };
  });
  expect(result).toEqual({ before: [], first: [1], retry: [1], displayed: 1 });
});

test("sample confirmation works without logs and a stale open cannot undo its close", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__sampleEvent = { channel: "test", key: "sample:1", revision: 1, topic: "control",
      payload: { kind: "redownload_sample", sample_id: "1", deadline_ts: Date.now() / 1000 + 60 } };
    window._appEventBatch([window.__sampleEvent]);
  });
  await expect(page.locator("#redwnl-sample-modal")).toBeVisible();
  await page.evaluate(() => {
    window._appEventBatch([{ ...window.__sampleEvent, revision: 2,
      payload: { kind: "redownload_sample_closed", sample_id: "1" } }]);
    window._appEventBatch([window.__sampleEvent]);
  });
  await expect(page.locator("#redwnl-sample-modal")).toBeHidden();
});

test("late telemetry and old request completions cannot change a newer paused job", async ({ page }) => {
  await loadApp(page);
  const result = await page.evaluate(() => {
    window._inflightRetranscribes.set("video", { request_id: "new", pct: 20,
      phase: "transcribing", started_at: Date.now(), phase_started_at: Date.now() });
    const push = (revision, state, more = {}) => window._appEventBatch([{
      channel: "test", key: "processing:new", topic: "processing", revision,
      payload: { request_id: "new", video_id: "video", state, ...more },
    }]);
    push(3, "paused", { message: "Paused by user" });
    const paused = { ...window._inflightRetranscribes.get("video") };
    push(2, "transcribing", { pct: 30 });
    push(4, "transcribing", { pct: 35 });
    window._appEventBatch([{ channel: "test", key: "processing:old", topic: "processing",
      revision: 5, payload: { request_id: "old", video_id: "video", kind: "complete" } }]);
    const held = { ...window._inflightRetranscribes.get("video") };
    push(6, "resuming");
    push(7, "transcribing", { pct: 36 });
    return { paused, held, resumed: window._inflightRetranscribes.get("video") };
  });
  expect(result.held.phase).toBe("paused");
  expect(result.held.message).toBe("Paused by user");
  expect(result.held.phase_started_at).toBe(result.paused.phase_started_at);
  expect(result.held.pct).toBe(35);
  expect(result.resumed.phase).toBe("transcribing");
  expect(result.resumed.pct).toBe(36);
});

test("root removal captures its target and remains one command when selection changes", async ({ page }) => {
  await loadApp(page, { bridge: { settings: { tp_archive_roots: ["D:\\One", "E:\\Two"] } } });
  await page.evaluate(() => {
    window.__setBridgeHandler("archive_root_remove", root => new Promise(resolve => {
      window.__removedRoot = root;
      window.__finishRootRemove = resolve;
    }));
  });
  await page.locator('.tab[data-tab="settings"]').click();
  await page.locator("#settings-roots-list .root-entry").filter({ hasText: "D:\\One" }).click();
  const remove = page.locator("#btn-settings-remove-root");
  await remove.click();
  await expect.poll(() => page.evaluate(() => window.__removedRoot)).toBe("D:\\One");
  await page.locator("#settings-roots-list .root-entry").filter({ hasText: "E:\\Two" }).click();
  await expect(remove).toBeDisabled();
  await page.evaluate(() => window.__finishRootRemove({ ok: false, error: "Fixture cleanup failed" }));
  await expect(remove).toBeEnabled();
  expect(await page.evaluate(() => window.__bridgeCallsFor("archive_root_remove").map(c => c.args)))
    .toEqual([["D:\\One"]]);
  expect(await page.evaluate(() => window.__bridgeCallsFor("settings_save").filter(c =>
    "tp_archive_roots" in c.args[0]))).toEqual([]);
});

for (const navigate of ["new video", "different view", "round trip"]) {
  test(`a late legacy bookmark lookup cannot replace a ${navigate}`, async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => {
      window.__setBridgeHandler("bookmark_list", () => [{ id: 1, title: "Legacy bookmark",
        channel: "Fixture", video_id: "", filepath: "" }]);
      window.__setBridgeHandler("recent_resolve", () => new Promise(resolve => {
        window.__finishBookmarkResolve = resolve;
      }));
      document.querySelector('.tab[data-tab="browse"]').click();
      document.querySelector('[data-submode="bookmarks"]').click();
    });
    await page.locator(".bookmark-card").click();
    await expect.poll(() => page.evaluate(() => typeof window.__finishBookmarkResolve)).toBe("function");
    await page.evaluate(async navigate => {
      if (navigate === "new video") await window._openVideoInWatch({ video_id: "new",
        title: "New selection", filepath: "C:\\FixtureArchive\\new.mp4" });
      else {
        document.querySelector('[data-submode="channels"]').click();
        if (navigate === "round trip") document.querySelector('[data-submode="bookmarks"]').click();
      }
      window.__finishBookmarkResolve({ ok: true, filepath: "C:\\FixtureArchive\\old.mp4", video_id: "old" });
    }, navigate);
    if (navigate === "new video") await expect(page.locator("#watch-title")).toHaveText("New selection");
    else await expect(page.locator(navigate === "round trip" ? "#view-bookmarks" : "#view-channels")).toBeVisible();
    expect(await page.evaluate(() => window._watchCurrentVideo?.video_id)).not.toBe("old");
  });
}

test("metadata and pause revisions remain independent of a Watch open", async ({ page }) => {
  await loadApp(page);
  const result = await page.evaluate(() => {
    const session = window.YT.watchSession;
    const video = { video_id: "one", title: "One" };
    const opening = session.begin(video);
    session.render(video);
    const playing = session.ticket();
    const oldMetadata = session.beginMetadata(video);
    const freshMetadata = session.beginMetadata(video);
    session.cancelPlayback();
    const beforeSwitch = { opening: session.isCurrent(opening), rendered: session.isRendered(opening),
      oldMetadata: session.metadataCurrent(oldMetadata), freshMetadata: session.metadataCurrent(freshMetadata),
      playing: session.playbackCurrent(playing), immutable: Object.isFrozen(opening) };
    session.begin({ video_id: "two" });
    return { beforeSwitch, stale: session.isCurrent(opening), action: session.actionVideo() };
  });
  expect(result).toEqual({ beforeSwitch: { opening: true, rendered: true, oldMetadata: false,
    freshMetadata: true, playing: false, immutable: true }, stale: false, action: null });
});
