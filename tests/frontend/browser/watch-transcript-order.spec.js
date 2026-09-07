const { test, expect, loadApp } = require("./fixtures");

for (const initial of ["success", "failure"]) {
  test(`late initial transcript ${initial} cannot overwrite a completed refresh`, async ({ page }) => {
    await loadApp(page);
    const result = await page.evaluate(async initial => {
      let calls = 0;
      let releaseInitial;
      let rejectInitial;
      const toasts = [];
      window._showToast = message => toasts.push(message);
      window.__setBridgeHandler("browse_get_transcript", () => {
        if (++calls === 1) return new Promise((resolve, reject) => {
          releaseInitial = resolve;
          rejectInitial = reject;
        });
        return { ok: true, segments: [{ s: 0, e: 1, t: "Fresh completed transcript", w: [] }] };
      });
      const opening = window._openVideoInWatch({ video_id: "fixture", title: "Fixture",
        filepath: "C:\\FixtureArchive\\fixture.mp4" });
      const request = window.YT.watchSession.ticket();
      await window._onRetranscribeComplete({ video_id: "fixture" });
      const fresh = document.getElementById("watch-transcript").textContent;
      if (initial === "success") releaseInitial({ ok: true,
        segments: [{ s: 0, e: 1, t: "Old initial transcript", w: [] }] });
      else rejectInitial(new Error("Stale initial read failed"));
      await opening;
      return { calls, fresh, final: document.getElementById("watch-transcript").textContent,
        toasts, stillSameOpen: window.YT.watchSession.isRendered(request) };
    }, initial);
    expect(result.calls).toBe(2);
    expect(result.fresh).toContain("Fresh completed transcript");
    expect(result.final).toBe(result.fresh);
    expect(result.toasts).toEqual(["Re-transcription complete — transcript updated."]);
    expect(result.stillSameOpen).toBe(true);
  });
}

test("a newer same-video completion owns transcript paint without invalidating metadata or playback", async ({ page }) => {
  await loadApp(page);
  const result = await page.evaluate(async () => {
    await window._openVideoInWatch({ video_id: "fixture", title: "Fixture",
      filepath: "C:\\FixtureArchive\\fixture.mp4" });
    const session = window.YT.watchSession;
    const metadata = session.beginMetadata(window._watchCurrentVideo);
    const playback = session.ticket();
    let calls = 0;
    let releaseOlder;
    window.__setBridgeHandler("browse_get_transcript", () => {
      if (++calls === 1) return new Promise(resolve => { releaseOlder = resolve; });
      return { ok: true, segments: [{ s: 0, e: 1, t: "Newest completion", w: [] }] };
    });
    const older = window._onRetranscribeComplete({ video_id: "fixture" });
    await window._onRetranscribeComplete({ video_id: "fixture" });
    releaseOlder({ ok: true, segments: [{ s: 0, e: 1, t: "Older completion", w: [] }] });
    await older;
    return { text: document.getElementById("watch-transcript").textContent,
      metadataCurrent: session.metadataCurrent(metadata), playbackCurrent: session.playbackCurrent(playback) };
  });
  expect(result.text).toContain("Newest completion");
  expect(result.metadataCurrent).toBe(true);
  expect(result.playbackCurrent).toBe(true);
});
