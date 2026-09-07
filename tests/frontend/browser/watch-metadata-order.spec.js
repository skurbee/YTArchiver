const { test, expect, loadApp } = require("./fixtures");

test("late metadata cannot replace a newer video or a newer refresh of that video", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(async () => {
    window.__setBridgeHandler("browse_get_video_metadata", request => request.video_id === "old"
      ? new Promise(resolve => { window.__oldMetadata = resolve; })
      : { ok: true, meta: { description: "New video's description" } });
    await window._openVideoInWatch({ video_id: "old", title: "Old video" });
    await window._openVideoInWatch({ video_id: "new", title: "New video" });
  });
  await expect(page.locator("#watch-meta-description")).toHaveText("New video's description");
  await page.evaluate(() => window.__oldMetadata({ ok: true, meta: { description: "Stale old description" } }));
  await expect(page.locator("#watch-meta-description")).toHaveText("New video's description");
  await page.evaluate(async () => {
    window.__setBridgeHandler("browse_get_video_metadata", () => new Promise(resolve => {
      window.__oldRefresh = resolve;
    }));
    window.__pendingRefresh = window.loadWatchMetadataDrawer(window._watchCurrentVideo);
    window.__setBridgeHandler("browse_get_video_metadata", () => ({ ok: true,
      meta: { description: "Newest saved description" } }));
    await window.loadWatchMetadataDrawer(window._watchCurrentVideo);
    window.__oldRefresh({ ok: true, meta: { description: "Stale refresh description" } });
    await window.__pendingRefresh;
  });
  await expect(page.locator("#watch-meta-description")).toHaveText("Newest saved description");
  await expect(page.locator("#watch-title")).toHaveText("New video");
});
