const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");


const VIDEO_ID = "fixture-finalizing-video";


async function openCaptionedVideo(page) {
  await page.evaluate(() => {
    window.__setBridgeHandler("browse_get_transcript", () => ({
      segments: [{
        s: 0,
        e: 2,
        t: "A fixture transcript sentence.",
        w: [],
      }],
      source: { source: "yt_captions_raw", raw: "YT CAPTIONS" },
    }));
  });
  await page.evaluate((videoId) => window._openVideoInWatch({
    video_id: videoId,
    title: "Fixture finalization",
    channel: "Fixture Channel",
    filepath: "C:\\FixtureArchive\\Fixture finalization.mp4",
  }), VIDEO_ID);
  await expect(page.locator(".watch-retranscribe-control")).toBeVisible();
}


async function retranscribePaint(page) {
  return page.locator(".watch-retranscribe-control").evaluate((control) => {
    const progress = control.querySelector(".watch-retranscribe-progress");
    const text = control.querySelector(".watch-retranscribe-progress-text");
    const fill = control.querySelector(".watch-retranscribe-fill");
    const button = document.getElementById("btn-watch-retranscribe");
    const banner = control.closest(".watch-src-banner");
    const fillStyle = getComputedStyle(fill);
    return {
      controlClass: control.className,
      progressClass: progress.className,
      bannerClass: banner.className,
      progressHidden: progress.hidden,
      linkHidden: control.querySelector(".watch-retranscribe-link").hidden,
      text: text.textContent,
      title: text.title,
      fillClass: fill.className,
      fillWidth: fill.style.width,
      animationName: fillStyle.animationName,
      animationIterationCount: fillStyle.animationIterationCount,
      buttonText: button.textContent,
      buttonDisabled: button.disabled,
      buttonBusy: button.dataset.busy,
    };
  });
}


test("Watch changes a completed 99% pass into a truthful finalizing state", async ({ page }) => {
  await loadApp(page);
  await openCaptionedVideo(page);

  const timestamps = await page.evaluate((videoId) => {
    const now = Date.now();
    const startedAt = now - 90_000;
    window._inflightRetranscribes.set(videoId, {
      pct: 99,
      phase: "transcribing",
      started_at: startedAt,
      phase_started_at: startedAt,
    });
    window._syncWatchRetranscribeButton();
    return { now, startedAt };
  }, VIDEO_ID);

  const percent = await retranscribePaint(page);
  expect(percent.text).toBe("Whisper 99%");
  expect(percent.fillWidth).toBe("99%");
  expect(percent.fillClass).not.toContain("is-indeterminate");

  await page.evaluate((videoId) => {
    window._logBatch({
      main: [[
        [" — ", ["tx_done_" + videoId, "whisper_bracket"]],
        ["Finalizing transcript", [
          "tx_done_" + videoId,
          "whisper_job_fixture",
          "whisper_finalizing",
        ]],
        [" \"Fixture finalization\"...\n", [
          "tx_done_" + videoId,
          "whisper_job_fixture",
        ]],
      ]],
      activity: [],
    });
  }, VIDEO_ID);

  await expect.poll(() => page.evaluate((videoId) =>
    window._inflightRetranscribes.get(videoId)?.phase, VIDEO_ID))
    .toBe("finalizing");
  const state = await page.evaluate((videoId) =>
    window._inflightRetranscribes.get(videoId), VIDEO_ID);
  expect(state.pct).toBe(99);
  expect(state.started_at).toBe(timestamps.startedAt);
  expect(state.phase_started_at).toBeGreaterThanOrEqual(timestamps.now);

  const finalizing = await retranscribePaint(page);
  expect(finalizing.text).toMatch(/^Finishing transcript…/);
  expect(`${finalizing.text} ${finalizing.title}`).not.toContain("99%");
  expect(`${finalizing.text} ${finalizing.title}`).toMatch(/\b0s\b|\bless than a second\b/i);
  expect(finalizing.fillClass).toContain("is-indeterminate");
  expect(finalizing.animationName).toBe("watch-retranscribe-indeterminate");
  expect(finalizing.animationIterationCount).toBe("infinite");
  expect(finalizing.buttonText).toMatch(/^Finishing transcript…/);
  expect(finalizing.buttonDisabled).toBe(true);
  expect(finalizing.buttonBusy).toBe("1");
});


test("an old finalizing phase says Still finishing without error styling", async ({ page }) => {
  await loadApp(page);
  await openCaptionedVideo(page);

  await page.evaluate((videoId) => {
    const now = Date.now();
    window._inflightRetranscribes.set(videoId, {
      pct: 99,
      phase: "finalizing",
      started_at: now - 240_000,
      phase_started_at: now - 121_000,
    });
    window._syncWatchRetranscribeButton();
  }, VIDEO_ID);

  const stalled = await retranscribePaint(page);
  expect(`${stalled.text} ${stalled.title}`).toMatch(/Still finishing…/i);
  expect(`${stalled.text} ${stalled.title}`).toMatch(/2m|121s/i);
  expect(stalled.fillClass).toContain("is-indeterminate");
  expect(stalled.animationIterationCount).toBe("infinite");
  expect(stalled.buttonDisabled).toBe(true);
  expect(stalled.buttonBusy).toBe("1");
  expect([
    stalled.controlClass,
    stalled.progressClass,
    stalled.bannerClass,
    stalled.fillClass,
  ].join(" ")).not.toMatch(/error|warn|danger|failed|stalled/i);
  expect(`${stalled.text} ${stalled.title} ${stalled.buttonText}`)
    .not.toMatch(/failed|error|stalled|longer than usual/i);
});


async function seedFinalizingState(page) {
  await page.evaluate((videoId) => {
    const now = Date.now();
    window._inflightRetranscribes.set(videoId, {
      pct: 99,
      phase: "finalizing",
      started_at: now - 90_000,
      phase_started_at: now - 10_000,
    });
    window._syncWatchRetranscribeButton();
  }, VIDEO_ID);
}


test("a failed finalization becomes Needs attention instead of spinning forever", async ({ page }) => {
  await loadApp(page);
  await openCaptionedVideo(page);
  await seedFinalizingState(page);
  const transcriptCallsBefore = await page.evaluate(() =>
    window.__bridgeCallsFor("browse_get_transcript").length);

  expect(await page.evaluate(() => typeof window._onRetranscribeState))
    .toBe("function");
  await page.evaluate((videoId) => window._onRetranscribeState({
    state: "needs_attention",
    video_id: videoId,
    filepath: "C:\\FixtureArchive\\Fixture finalization.mp4",
    message: "Needs attention — retry from Processing",
  }), VIDEO_ID);

  const state = await page.evaluate((videoId) =>
    window._inflightRetranscribes.get(videoId), VIDEO_ID);
  expect(state.phase).toBe("needs_attention");
  const paint = await retranscribePaint(page);
  // The banner text is the visible control. The hidden proxy button also
  // carries state, but it cannot mask stale "Whisper 99%" copy on screen.
  expect(`${paint.text} ${paint.title}`)
    .toMatch(/Needs attention.*Processing/i);
  expect(paint.buttonText).toMatch(/Needs attention.*Processing/i);
  expect(paint.buttonDisabled).toBe(true);
  expect(paint.buttonBusy).toBe("1");
  expect(paint.fillClass).not.toContain("is-indeterminate");
  expect(paint.animationIterationCount).not.toBe("infinite");
  expect(await page.evaluate(() =>
    window.__bridgeCallsFor("browse_get_transcript").length))
    .toBe(transcriptCallsBefore);
});


test("cancelled finalization clears only that video's busy state", async ({ page }) => {
  await loadApp(page);
  await openCaptionedVideo(page);
  await seedFinalizingState(page);
  await page.evaluate((videoId) => {
    window._inflightRetranscribes.set("unrelated-video", {
      pct: 50,
      phase: "transcribing",
      started_at: Date.now(),
      phase_started_at: Date.now(),
    });
  }, VIDEO_ID);
  const transcriptCallsBefore = await page.evaluate(() =>
    window.__bridgeCallsFor("browse_get_transcript").length);

  expect(await page.evaluate(() => typeof window._onRetranscribeState))
    .toBe("function");
  await page.evaluate((videoId) => window._onRetranscribeState({
    state: "cancelled",
    video_id: videoId,
    filepath: "C:\\FixtureArchive\\Fixture finalization.mp4",
    message: "Re-transcription cancelled",
  }), VIDEO_ID);

  const mapState = await page.evaluate((videoId) => ({
    target: window._inflightRetranscribes.has(videoId),
    unrelated: window._inflightRetranscribes.has("unrelated-video"),
  }), VIDEO_ID);
  expect(mapState).toEqual({ target: false, unrelated: true });
  const paint = await retranscribePaint(page);
  expect(paint.buttonText).toBe("Re-transcribe…");
  expect(paint.buttonDisabled).toBe(false);
  expect(paint.buttonBusy).toBe("");
  expect(paint.progressHidden).toBe(true);
  expect(paint.linkHidden).toBe(false);
  expect(await page.evaluate(() =>
    window.__bridgeCallsFor("browse_get_transcript").length))
    .toBe(transcriptCallsBefore);
});


test("paused and resuming finalization states use truthful Watch copy", async ({ page }) => {
  await loadApp(page);
  await openCaptionedVideo(page);
  await seedFinalizingState(page);

  expect(await page.evaluate(() => typeof window._onRetranscribeState))
    .toBe("function");
  await page.evaluate((videoId) => window._onRetranscribeState({
    state: "paused",
    video_id: videoId,
    filepath: "C:\\FixtureArchive\\Fixture finalization.mp4",
    message: "Paused — resume from Processing",
  }), VIDEO_ID);

  let state = await page.evaluate((videoId) =>
    window._inflightRetranscribes.get(videoId), VIDEO_ID);
  expect(state.phase).toBe("paused");
  let paint = await retranscribePaint(page);
  expect(`${paint.text} ${paint.title}`).toMatch(/Paused.*Processing/i);
  expect(paint.buttonText).toMatch(/paused/i);
  expect(paint.buttonDisabled).toBe(true);
  expect(paint.fillClass).not.toContain("is-indeterminate");

  await page.evaluate((videoId) => window._onRetranscribeState({
    state: "resuming",
    video_id: videoId,
    filepath: "C:\\FixtureArchive\\Fixture finalization.mp4",
    message: "Resuming transcription…",
  }), VIDEO_ID);

  state = await page.evaluate((videoId) =>
    window._inflightRetranscribes.get(videoId), VIDEO_ID);
  expect(state.phase).toBe("resuming");
  paint = await retranscribePaint(page);
  expect(`${paint.text} ${paint.title}`).toMatch(/Resuming transcription/i);
  expect(paint.buttonText).toMatch(/Resuming/i);
  expect(paint.buttonDisabled).toBe(true);
  expect(paint.buttonBusy).toBe("1");
});


test("a no-speech retranscribe says the existing transcript was kept", async ({ page }) => {
  await loadApp(page);
  await openCaptionedVideo(page);
  await page.evaluate((videoId) => {
    window._inflightRetranscribes.set(videoId, {
      pct: 99,
      phase: "finalizing",
      started_at: Date.now() - 60_000,
      phase_started_at: Date.now() - 5_000,
    });
    window._inflightRetranscribes.set("unrelated-video", {
      pct: 42,
      phase: "transcribing",
      started_at: Date.now(),
      phase_started_at: Date.now(),
    });
  }, VIDEO_ID);

  await page.evaluate((videoId) => window._onRetranscribeComplete({
    video_id: videoId,
    filepath: "C:\\FixtureArchive\\Fixture finalization.mp4",
    existing_transcript_kept: true,
  }), VIDEO_ID);

  const toast = page.locator("#toast-root .toast").last();
  await expect(toast).toContainText("Whisper found no speech");
  await expect(toast).toContainText("existing transcript was kept");
  await expect(toast).not.toContainText("transcript updated");
  await expect(page.locator("#watch-transcript"))
    .toContainText("A fixture transcript sentence.");

  const mapState = await page.evaluate((videoId) => ({
    target: window._inflightRetranscribes.has(videoId),
    unrelated: window._inflightRetranscribes.has("unrelated-video"),
  }), VIDEO_ID);
  expect(mapState).toEqual({ target: false, unrelated: true });
});


test("a no-ID orphan completion preserves unrelated Watch progress", async ({ page }) => {
  await loadApp(page);
  await openCaptionedVideo(page);
  await page.evaluate((videoId) => {
    const now = Date.now();
    window._inflightRetranscribes.set(videoId, {
      pct: 37,
      phase: "transcribing",
      started_at: now - 30_000,
      phase_started_at: now - 30_000,
      filepath: "C:\\FixtureArchive\\Fixture finalization.mp4",
    });
    window._syncWatchRetranscribeButton();
  }, VIDEO_ID);

  await page.evaluate(() => window._onRetranscribeComplete({
    video_id: "",
    filepath: "C:\\FixtureArchive\\No ID orphan.mp4",
  }));

  expect(await page.evaluate((videoId) =>
    window._inflightRetranscribes.has(videoId), VIDEO_ID)).toBe(true);
  const paint = await retranscribePaint(page);
  expect(paint.text).toBe("Whisper 37%");
  expect(paint.buttonText).toBe("Re-transcribing… 37%");
  expect(paint.buttonDisabled).toBe(true);
  expect(paint.buttonBusy).toBe("1");
});
