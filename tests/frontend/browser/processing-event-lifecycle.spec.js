const { test, expect, loadApp } = require("./fixtures");

test("coalesced resume and progress adopts the newer lifecycle epoch", async ({ page }) => {
  await loadApp(page);
  const result = await page.evaluate(() => {
    window._inflightRetranscribes.set("fixture", { request_id: "job", pct: 10,
      phase: "queued", phase_revision: 0, started_at: Date.now(), phase_started_at: Date.now() });
    const push = (revision, payload) => window._appEventBatch([{
      channel: "lifecycle", key: "job", topic: "processing", revision,
      payload: { request_id: "job", video_id: "fixture", ...payload },
    }]);
    push(1, { state: "paused", phase_revision: 1, message: "Paused by user" });
    push(2, { state: "transcribing", phase_revision: 1, pct: 20 });
    const held = { ...window._inflightRetranscribes.get("fixture") };
    // The backend coalesced resuming(epoch2) and transcribing(epoch3).
    push(4, { state: "transcribing", phase_revision: 3, pct: 21 });
    const resumed = { ...window._inflightRetranscribes.get("fixture") };
    push(5, { state: "paused", phase_revision: 4 });
    window._retranscribeWatchUpdateProgress(22, "fixture");
    push(6, { state: "transcribing", phase_revision: 3, pct: 25 });
    return { held, resumed, last: window._inflightRetranscribes.get("fixture") };
  });
  expect(result.held.phase).toBe("paused");
  expect(result.resumed.phase).toBe("transcribing");
  expect(result.resumed.phase_revision).toBe(3);
  expect(result.last.phase).toBe("paused");
  expect(result.last.pct).toBe(22);
});

test("async control receipt retries rejection without duplicate pending consumers", async ({ page }) => {
  await loadApp(page);
  const result = await page.evaluate(async () => {
    let attempts = 0;
    let rejectFirst;
    window.YT.eventState.onControl("async-fixture", () => {
      attempts++;
      if (attempts === 1) return new Promise((resolve, reject) => { rejectFirst = reject; });
      return Promise.resolve(true);
    });
    const event = { channel: "receipt", key: "prompt", revision: 1, topic: "control",
      payload: { kind: "async-fixture" } };
    const first = window._appEventBatch([event]);
    const pending = window._appEventBatch([event]);
    const pendingAttempts = attempts;
    rejectFirst(new Error("injected failed presentation"));
    await new Promise(resolve => setTimeout(resolve, 0));
    const retry = window._appEventBatch([event]);
    await new Promise(resolve => setTimeout(resolve, 0));
    const accepted = window._appEventBatch([event]);
    return { first, pending, pendingAttempts, retry, accepted, attempts };
  });
  expect(result).toEqual({ first: [], pending: [], pendingAttempts: 1, retry: [], accepted: [1], attempts: 2 });
});

test("newer close supersedes a pending async open without an old receipt undoing it", async ({ page }) => {
  await loadApp(page);
  const result = await page.evaluate(async () => {
    let finishOpen;
    const seen = [];
    window.YT.eventState.onControl(["async-open", "async-close"], event => {
      seen.push(event.detail.kind);
      if (event.detail.kind === "async-open") return new Promise(resolve => { finishOpen = resolve; });
    });
    const open = { channel: "supersede", key: "sample", revision: 1, topic: "control",
      payload: { kind: "async-open" } };
    window._appEventBatch([open]);
    const close = window._appEventBatch([{ ...open, revision: 2, payload: { kind: "async-close" } }]);
    finishOpen(true);
    await new Promise(resolve => setTimeout(resolve, 0));
    const stale = window._appEventBatch([open]);
    return { close, stale, seen };
  });
  expect(result).toEqual({ close: [2], stale: [1], seen: ["async-open", "async-close"] });
});
