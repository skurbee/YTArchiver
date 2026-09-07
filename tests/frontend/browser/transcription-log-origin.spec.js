const { test, expect, loadApp } = require("./fixtures");

const videoA = "abcDEF12345";
const videoB = "zyxWVU98765";

const headerA = [["[1/2] Channel A\n", "simpleline"]];
const downloadA = [[" — ✓ Video A\n", "simpleline"]];
const queuedA = [["      — ⏳ Transcription queued in Processing…\n",
  ["whisper_bracket", `tx_done_${videoA}`]]];
const headerB = [["[2/2] Channel B\n", "simpleline"]];
const downloadB = [[" — ✓ Video B\n", "simpleline"]];
const queuedB = [["      — ⏳ Transcription queued in Processing…\n",
  ["whisper_bracket", `tx_done_${videoB}`]]];

// Job identity intentionally precedes the video marker in some segments:
// delayed worker output must still find the slot reserved by the download.
const waitingA = [["      — Waiting for download to finish\n",
  ["yellow", "whisper_job_101", `tx_done_${videoA}`]]];
const progressA = [["      — Transcribing Video A 45%\n",
  ["whisper_progress", "whisper_job_101", `tx_done_${videoA}`]]];
const keptA = [["      — ⚠ No speech detected; existing transcript kept.\n",
  ["yellow", "whisper_job_101", `tx_done_${videoA}`]]];

async function startLog(page) {
  await loadApp(page);
  await page.evaluate(() => {
    window.clearLog("main-log");
    window._anySyncRunning = () => true;
  });
}

async function emit(page, lines) {
  await page.evaluate(main => window._logBatch({ main }), lines);
}

async function rowTexts(page) {
  return page.locator("#main-log > .log-line").allTextContents();
}

test("late waiting, progress and kept-transcript warning stay with their originating channel", async ({ page }) => {
  await startLog(page);
  await emit(page, [headerA, downloadA, queuedA, headerB, downloadB, queuedB]);
  const rowsBefore = await rowTexts(page);
  await emit(page, [waitingA]);
  let rows = await rowTexts(page);
  expect(rows).toHaveLength(6);
  expect(rows[2]).toContain("Waiting for download");
  expect(rows.slice(3)).toEqual(rowsBefore.slice(3));

  // Sync can finish before the worker. Its next progress tick must not move
  // this video's existing slot to the unrelated channel at the log tail.
  await page.evaluate(() => { window._anySyncRunning = () => false; });
  await emit(page, [progressA]);
  rows = await rowTexts(page);
  expect(rows[2]).toContain("Transcribing Video A 45%");
  expect(rows.slice(3)).toEqual(rowsBefore.slice(3));
  await emit(page, [keptA]);
  rows = await rowTexts(page);
  expect(rows).toHaveLength(6);
  expect(rows[2]).toContain("No speech detected; existing transcript kept.");
  expect(rows.slice(3)).toEqual(rowsBefore.slice(3));
  const ownSlot = page.locator(`#main-log [data-inplace="tx_done_${videoA}"]`);
  await expect(ownSlot).toHaveCount(1);
  await expect(ownSlot.locator(".t-yellow")).toHaveCount(1);
  await expect(ownSlot).not.toHaveAttribute("data-pin-bottom", "1");
  await expect(ownSlot).not.toContainText("Waiting");
  await expect(ownSlot).not.toContainText("✓ Transcription");
  await expect(ownSlot).not.toContainText("45%");
});

test("same-batch late warning replaces the original slot without retaining stale success or wait rows", async ({ page }) => {
  await startLog(page);
  const oldSuccess = [["      — ✓ Transcription (Whisper small)\n",
    ["simpleline_blue", `tx_done_${videoA}`]]];
  await emit(page, [headerA, downloadA, oldSuccess, queuedA,
    headerB, downloadB, queuedB, waitingA, progressA, keptA]);
  const rows = await rowTexts(page);
  expect(rows).toEqual([
    headerA[0][0], downloadA[0][0], keptA[0][0],
    headerB[0][0], downloadB[0][0], queuedB[0][0],
  ]);
  await expect(page.locator(`#main-log [data-inplace="tx_done_${videoA}"]`)).toHaveCount(1);
  await expect(page.locator("#main-log")).not.toContainText("Whisper small");
  await expect(page.locator("#main-log")).not.toContainText("Waiting for download");

  // A later completion for B must replace only B's row, even though the
  // newest event in the preceding batch belonged to A.
  const doneB = [["      — ✓ Transcription (Whisper medium)\n",
    ["simpleline_blue", "whisper_job_102", `tx_done_${videoB}`]]];
  await emit(page, [doneB]);
  expect(await rowTexts(page)).toEqual([
    headerA[0][0], downloadA[0][0], keptA[0][0],
    headerB[0][0], downloadB[0][0], doneB[0][0],
  ]);
});

test("late terminal update preserves the reader's scroll position and channel grouping", async ({ page }) => {
  await startLog(page);
  await page.locator('.tab[data-tab="download"]').click();
  const filler = Array.from({ length: 120 }, (_, index) =>
    [[`Later channel ${index + 1}\n`, "simpleline"]]);
  await emit(page, [headerA, downloadA, queuedA, headerB, downloadB, queuedB, ...filler]);
  const scrollBefore = await page.locator("#main-log").evaluate(el => {
    el.scrollTop = 0;
    el.dispatchEvent(new Event("scroll"));
    return { top: el.scrollTop, scrollHeight: el.scrollHeight, height: el.clientHeight };
  });
  expect(scrollBefore.scrollHeight).toBeGreaterThan(scrollBefore.height);
  await emit(page, [keptA]);
  expect(await page.locator("#main-log").evaluate(el => el.scrollTop)).toBe(scrollBefore.top);
  const rows = await rowTexts(page);
  expect(rows[2]).toEqual(keptA[0][0]);
  expect(rows[3]).toEqual(headerB[0][0]);
  expect(rows.at(-1)).toEqual(filler.at(-1)[0][0]);
});
