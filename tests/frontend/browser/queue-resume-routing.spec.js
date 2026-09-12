const { test, expect, loadApp } = require("./fixtures");

async function loadResume(page, options = {}) {
  await loadApp(page, { args: options, configure: options => {
    window.__resumeOptions = options;
    window.__actualSyncRunning = !!options.actualRunning;
    const kinds = options.kinds || ["download"];
    window.__resumeQueues = {
      sync: kinds.map((kind, i) => ({ task_id: `sync-${i}`, kind,
        name: `Fixture channel ${i}`, status: "queued" })),
      gpu: [{ task_id: "gpu-0", kind: "transcribe", name: "Fixture video", status: "queued" }],
      sync_paused: true, sync_paused_active: true,
      gpu_paused: true, gpu_paused_active: true, identity_ids_durable: true,
    };
    window.__publishResumeQueues = (running, waiting = false) => {
      window.renderQueues(window.__resumeQueues);
      window.setQueueState({
        sync: { running, paused: window.__resumeQueues.sync_paused,
          pausedActive: window.__resumeQueues.sync_paused,
          count: kinds.length, trafficWaiting: waiting,
          trafficWait: { active: waiting, queue: "sync", reason: "daily_limit",
            until: Date.now() / 1000 + 3600 }, sessionLimited: false },
        gpu: { running: !!options.gpuRunning, paused: true,
          pausedActive: true, count: 1, trafficWaiting: false },
      });
    };
    window.__setBridgeHandler("get_queues", () => window.__resumeQueues);
    window.__setBridgeHandler("sync_is_running", () => window.__actualSyncRunning);
    window.__setBridgeHandler("queue_is_paused", () => {
      if (options.countRace) window._blinkState.sync.count = 0;
      return { sync: window.__resumeQueues.sync_paused, gpu: true };
    });
    window.__setBridgeHandler("sync_start_all", add => {
      if (add !== false) throw new Error("Resume must not enqueue the full library");
      window.__actualSyncRunning = true;
      window.__resumeQueues.sync_paused = false;
      window.__resumeQueues.sync_paused_active = false;
      window.__resumeQueues.sync[0].status = "running";
      window.__publishResumeQueues(true, true);
      return { ok: true, started: true };
    });
    window.__setBridgeHandler("queue_resume", () => ({ ok: true, paused: false }));
    window.__setBridgeHandler("queue_pause", () => ({ ok: true }));
    window.__setBridgeHandler("gpu_start", () => ({ ok: true }));
    window.__setBridgeHandler("resume_pending_redownloads", () => ({
      ok: true, resumed: kinds.filter(k => k === "redownload").length,
      regular_pending: kinds.filter(k => k !== "redownload").length,
    }));
    window.__setBridgeHandler("youtube_traffic_override", () => ({ ok: true }));
  } });
  await page.evaluate(() => window.__publishResumeQueues(!!window.__resumeOptions.paintedRunning));
}

async function resumeButton(page, control = "sync") {
  if (control === "sync") await page.locator("#btn-sync-tasks").click();
  return page.locator(control === "sync" ? "#btn-pause-sync-queue" : "#btn-pause");
}

async function mutations(page) {
  return page.evaluate(() => window.__bridgeCalls.filter(call =>
    ["sync_start_all", "resume_pending_redownloads", "queue_resume", "queue_pause", "gpu_start"]
      .includes(call.name)));
}

for (const control of ["sync", "global"]) {
  test(`${control} Resume starts the existing queue when painted activity outlives its worker`, async ({ page }) => {
    await loadResume(page, { paintedRunning: true, actualRunning: false });
    await (await resumeButton(page, control)).click();
    await expect.poll(() => page.evaluate(() => window.__bridgeCallsFor("sync_start_all").length)).toBe(1);
    expect(await page.evaluate(() => window.__bridgeCallsFor("sync_start_all")[0].args)).toEqual([false]);
    expect(await page.evaluate(() => window.__bridgeCallsFor("queue_resume"))).toEqual([]);
    expect(await page.evaluate(() => window.__bridgeCallsFor("gpu_start").length)).toBe(control === "global" ? 1 : 0);
    await expect(page.locator("#btn-pause")).not.toHaveAttribute("aria-busy", "true");
  });

  test(`${control} Resume uses the existing live worker even if the painted queue is idle`, async ({ page }) => {
    await loadResume(page, { paintedRunning: false, actualRunning: true });
    await (await resumeButton(page, control)).click();
    await expect.poll(() => mutations(page)).toEqual([
      { name: "queue_resume", args: [control === "global" ? "both" : "sync"] },
    ]);
  });

  for (const mixed of [false, true]) {
    test(`${control} Resume preserves ${mixed ? "mixed" : "redownload-only"} routing with stale activity`, async ({ page }) => {
      await loadResume(page, { paintedRunning: true, actualRunning: false,
        kinds: mixed ? ["redownload", "download"] : ["redownload"] });
      await (await resumeButton(page, control)).click();
      const expected = [{ name: "resume_pending_redownloads", args: [] }];
      if (mixed) expected.push({ name: "sync_start_all", args: [false] });
      if (control === "global") expected.push({ name: "gpu_start", args: ["gpu"] });
      await expect.poll(() => mutations(page)).toEqual(expected);
    });
  }

  test(`${control} Resume clears pending feedback after an authoritative state read rejects`, async ({ page }) => {
    await loadResume(page, { paintedRunning: true, actualRunning: false });
    await page.evaluate(() => window.__setBridgeHandler("sync_is_running", () =>
      Promise.reject(new Error("Worker state unavailable"))));
    const button = await resumeButton(page, control);
    await button.click();
    await expect(page.getByText(/Worker state unavailable/)).toBeVisible();
    await expect(button).toBeEnabled();
    await expect(button).not.toHaveAttribute("aria-busy", "true");
    expect(await mutations(page)).toEqual([]);
  });

  test(`${control} Resume does not fall through to a regular sync when redownload restoration rejects`, async ({ page }) => {
    await loadResume(page, { paintedRunning: true, actualRunning: false, kinds: ["redownload"] });
    await page.evaluate(() => window.__setBridgeHandler("resume_pending_redownloads", () =>
      Promise.reject(new Error("Saved task unavailable"))));
    const button = await resumeButton(page, control);
    await button.click();
    await expect(page.getByText(/Saved task unavailable/)).toBeVisible();
    await expect(button).toBeEnabled();
    expect(await mutations(page)).toEqual([{ name: "resume_pending_redownloads", args: [] }]);
  });

  for (const mixed of [false, true]) {
    test(`${control} Resume reports a resolved ${mixed ? "mixed" : "redownload-only"} restoration failure`, async ({ page }) => {
      await loadResume(page, { paintedRunning: true, actualRunning: false,
        kinds: mixed ? ["redownload", "download"] : ["redownload"] });
      await page.evaluate(mixed => window.__setBridgeHandler("resume_pending_redownloads", () => ({
        ok: false, resumed: 0, regular_pending: mixed ? 1 : 0,
        error: "Saved redownload could not be restored",
      })), mixed);
      const button = await resumeButton(page, control);
      await button.click();
      await expect(page.getByText("Saved redownload could not be restored", { exact: true })).toBeVisible();
      await expect(button).toBeEnabled();
      expect(await mutations(page)).toEqual([{ name: "resume_pending_redownloads", args: [] }]);
    });
  }

  test(`${control} Resume can drain regular work after a successful zero-redownload refresh`, async ({ page }) => {
    await loadResume(page, { paintedRunning: true, actualRunning: false,
      kinds: ["redownload", "download"] });
    await page.evaluate(() => window.__setBridgeHandler("resume_pending_redownloads", () => ({
      ok: true, resumed: 0, regular_pending: 1,
    })));
    await (await resumeButton(page, control)).click();
    const expected = [
      { name: "resume_pending_redownloads", args: [] }, { name: "sync_start_all", args: [false] },
    ];
    if (control === "global") expected.push({ name: "gpu_start", args: ["gpu"] });
    await expect.poll(() => mutations(page)).toEqual(expected);
  });
}

test("a queue payload arriving during the pause-state read cannot erase restored work", async ({ page }) => {
  await loadResume(page, { paintedRunning: false, actualRunning: false, countRace: true });
  await (await resumeButton(page)).click();
  await expect.poll(() => mutations(page)).toEqual([{ name: "sync_start_all", args: [false] }]);
});

test("global Resume starts missing Sync while resuming an already live paused Processing worker", async ({ page }) => {
  await loadResume(page, { paintedRunning: true, actualRunning: false, gpuRunning: true });
  await (await resumeButton(page, "global")).click();
  await expect.poll(() => mutations(page)).toEqual([
    { name: "sync_start_all", args: [false] }, { name: "queue_resume", args: ["gpu"] },
  ]);
});

test("a resumed queue's budget wait exposes the existing confirmed override action", async ({ page }) => {
  await loadResume(page, { paintedRunning: true, actualRunning: false });
  await (await resumeButton(page)).click();
  await expect(page.locator("#gsb-sync-text")).toContainText("waiting for 24-hour slot");
  const globalButton = page.locator("#btn-pause");
  await expect(globalButton).toBeEnabled();
  await expect(globalButton).toHaveAttribute("aria-label", /click to override/);
  await globalButton.click();
  const dialog = page.getByRole("dialog", { name: "Override YouTube traffic limit?" });
  await expect(dialog).toBeVisible();
  expect(await page.evaluate(() => window.__bridgeCallsFor("youtube_traffic_override"))).toEqual([]);
  await dialog.getByRole("button", { name: "Override and continue", exact: true }).click();
  await expect.poll(() => page.evaluate(() => window.__bridgeCallsFor("youtube_traffic_override"))).toEqual([
    { name: "youtube_traffic_override", args: [] },
  ]);
  expect(await page.evaluate(() => window.__bridgeCallsFor("sync_start_all").length)).toBe(1);
});

test("older bridges without worker-state reads retain the painted-state fallback", async ({ page }) => {
  await loadResume(page, { paintedRunning: false, actualRunning: false });
  await page.evaluate(() => {
    const original = window.pywebview.api;
    window.pywebview.api = new Proxy(original, { get(target, key) {
      return key === "sync_is_running" ? undefined : target[key];
    } });
  });
  await (await resumeButton(page)).click();
  await expect.poll(() => mutations(page)).toEqual([{ name: "sync_start_all", args: [false] }]);
});

test("an empty lane's stale pause flag cannot change a global Pause click into Resume", async ({ page }) => {
  await loadResume(page, { kinds: [], gpuRunning: true });
  await page.evaluate(() => window.setQueueState({
    sync: { running: false, paused: true, pausedActive: false, count: 0 },
    gpu: { running: true, paused: false, pausedActive: false, count: 1 },
  }));
  const button = page.locator("#btn-pause");
  await expect(button).toHaveAttribute("data-pause-state", "running");
  const readsBefore = await page.evaluate(() => window.__bridgeCallsFor("get_queues").length);
  await button.click();
  await expect.poll(() => mutations(page)).toEqual([{ name: "queue_pause", args: ["both"] }]);
  expect(await page.evaluate(() => window.__bridgeCallsFor("sync_is_running"))).toEqual([]);
  expect(await page.evaluate(() => window.__bridgeCallsFor("get_queues").length)).toBe(readsBefore);
});
