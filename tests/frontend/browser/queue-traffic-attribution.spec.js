const { test, expect, loadApp } = require("./fixtures");

async function loadQueues(page) {
  await loadApp(page, { configure: () => {
    window.__testQueues = { sync: [], gpu: [], identity_ids_durable: true };
    window.__setBridgeHandler("get_queues", () => window.__testQueues);
    window.__setBridgeHandler("youtube_traffic_override", queue => ({ ok: true, queue }));
  } });
}

async function showQueues(page, { sync = false, gpu = true, waiting = "gpu",
  reason = "daily_limit", paused = false } = {}) {
  await page.evaluate(({ sync, gpu, waiting, reason, paused }) => {
    const task = (queue, title) => ({ task_id: `${queue}-current`, status: "running", name: title });
    window.__testQueues = {
      sync: sync ? [task("sync", "Syncing Fixture channel")] : [],
      gpu: gpu ? [task("gpu", "Transcribing Fixture video"),
        { task_id: "gpu-next", status: "queued", name: "Next fixture video" }] : [],
      identity_ids_durable: true,
    };
    window.__testQueueState = {};
    for (const queue of ["sync", "gpu"]) {
      const isWaiting = waiting === queue || waiting === "both";
      window.__testQueueState[queue] = {
        running: window.__testQueues[queue].length > 0,
        count: window.__testQueues[queue].length,
        paused: paused && isWaiting, pausedActive: paused && isWaiting,
        trafficWaiting: isWaiting, sessionLimited: false, resumePending: false,
        trafficWait: { active: isWaiting, queue, task_id: `${queue}-current`,
          reason, until: Date.now() / 1000 + 600 },
      };
    }
    window.renderQueues(window.__testQueues);
    window.setQueueState(window.__testQueueState);
  }, { sync, gpu, waiting, reason, paused });
}

async function currentState(page, queue, patch) {
  await page.evaluate(({ queue, patch }) => {
    Object.assign(window.__testQueueState[queue], patch);
    window.setQueueState(window.__testQueueState);
  }, { queue, patch });
}

test("a Processing caption hold parks only Processing and keeps running-task actions safe", async ({ page }) => {
  await loadQueues(page);
  await showQueues(page);
  await expect(page.locator("#gsb-sync-text")).toHaveText("Sync idle");
  await expect(page.locator("#gsb-gpu-text")).toContainText("Processing waiting for 24-hour slot");
  await expect(page.locator("#gsb-gpu .gsb-dot")).toHaveClass(/paused/);
  await expect(page.locator("#gsb-gpu .gsb-dot")).not.toHaveClass(/\bon\b/);
  await expect(page.locator("#btn-gpu-tasks")).toHaveAttribute("data-blink-state", "paused");
  await page.waitForTimeout(760);
  await expect(page.locator("#btn-gpu-tasks")).toHaveAttribute("data-blink-state", "paused");
  expect(await page.evaluate(() => window._blinkState.timer)).toBeNull();

  await page.locator("#gsb-gpu").click();
  const row = page.locator('#gpu-tasks-body [data-task-id="gpu-current"]');
  await expect(row).toHaveAttribute("data-traffic-waiting", "true");
  await expect(row).toHaveClass(/paused/);
  await expect(row).toContainText("Waiting for YouTube 24-hour limit");
  await expect(row.locator(".queue-task-dots")).toBeHidden();
  await expect(row).toHaveAttribute("draggable", "false");
  await expect(row.locator(".queue-task-close")).toHaveCount(0);
  await expect(page.locator('#gpu-tasks-body [data-task-id="gpu-next"] .queue-task-close')).toBeVisible();
  await row.click({ button: "right" });
  await expect(page.getByRole("menuitem", { name: "Cancel task", exact: true })).toBeVisible();
  await expect(page.getByRole("menuitem", { name: "Remove from queue", exact: true })).toHaveCount(0);
});

test("a queue-state-only release restores the running row and keeps later payload refreshes correct", async ({ page }) => {
  await loadQueues(page);
  await showQueues(page);
  await page.locator("#gsb-gpu").click();
  const row = page.locator('#gpu-tasks-body [data-task-id="gpu-current"]');
  await page.evaluate(() => window.renderQueues(window.__testQueues));
  await expect(row).toContainText("Waiting for YouTube 24-hour limit");
  await currentState(page, "gpu", { trafficWaiting: false, trafficWait: { active: false } });
  await expect(row).toHaveClass(/running/);
  await expect(row.locator(".queue-task-wait")).toHaveCount(0);
  await expect(row.locator(".queue-task-dots")).toBeVisible();
  await expect(page.locator("#gsb-gpu-text")).toContainText("Processing Transcribing Fixture video");
  await expect(page.locator("#gsb-gpu .gsb-dot")).toHaveClass(/\bon\b/);
  await expect(page.locator("#gsb-sync-text")).toHaveText("Sync idle");
});

for (const waiting of ["sync", "gpu"]) {
  test(`${waiting} traffic hold leaves the other queue visibly active`, async ({ page }) => {
    await loadQueues(page);
    await showQueues(page, { sync: true, gpu: true, waiting, reason: "hourly_limit" });
    const active = waiting === "sync" ? "gpu" : "sync";
    await expect(page.locator(`#gsb-${waiting}-text`)).toContainText("waiting for hourly slot");
    await expect(page.locator(`#gsb-${waiting} .gsb-dot`)).not.toHaveClass(/\bon\b/);
    await expect(page.locator(`#gsb-${active} .gsb-dot`)).toHaveClass(/\bon\b/);
    await expect(page.locator(`#gsb-${active}-text`)).not.toContainText("waiting");
    await page.locator(`#gsb-${active}`).click();
    await expect(page.locator(`#${active}-tasks-body .queue-task-row`).first()).toHaveClass(/running/);
  });
}

test("manual Processing pause takes precedence over a stale traffic wait", async ({ page }) => {
  await loadQueues(page);
  await showQueues(page, { paused: true });
  await expect(page.locator("#gsb-gpu-text")).toHaveText("Processing paused (2)");
  await expect(page.locator("#btn-pause")).toHaveAttribute("aria-label", "Resume all queues");
  await page.locator("#gsb-gpu").click();
  await expect(page.locator("#gpu-tasks-body .queue-task-wait")).toHaveCount(0);
  await expect(page.locator("#btn-pause-gpu-queue")).toHaveAttribute("data-tooltip", "Resume processing queue");
  await expect(page.locator("#btn-pause-gpu-queue")).not.toHaveAttribute("title");
});

for (const queue of ["sync", "gpu"]) {
  test(`global override explains and targets only the ${queue} wait`, async ({ page }) => {
    await loadQueues(page);
    await showQueues(page, { sync: queue === "sync", gpu: queue === "gpu", waiting: queue });
    const label = queue === "gpu" ? "Processing" : "Sync";
    const button = page.locator("#btn-pause");
    await expect(button).toHaveAttribute("aria-label", `${label} is waiting for a YouTube traffic slot — click to override`);
    await button.click();
    const dialog = page.getByRole("dialog", { name: "Override YouTube traffic limit?" });
    await expect(dialog).toContainText(`${label} is waiting for a 24-hour rolling-window slot`);
    await expect(dialog).toContainText(queue === "gpu"
      ? "this task and its remaining queued caption follow-ups"
      : "the rest of this sync pass and its remaining Processing follow-ups");
    await expect(dialog).toContainText("launch spacing remains active");
    await dialog.getByRole("button", { name: "Override and continue", exact: true }).click();
    await expect.poll(() => page.evaluate(() => window.__bridgeCallsFor("youtube_traffic_override").length)).toBe(1);
    expect(await page.evaluate(() => window.__bridgeCallsFor("youtube_traffic_override")[0].args))
      .toEqual(queue === "gpu" ? ["gpu", "gpu-current"] : []);
    const unrelated = await page.evaluate(() => window.__bridgeCalls.filter(call =>
      /^(sync|gpu|queues)_(pause|resume|start)/.test(call.name)));
    expect(unrelated).toEqual([]);
  });
}

test("cancelling the Processing override dialog never changes a queue", async ({ page }) => {
  await loadQueues(page);
  await showQueues(page);
  await page.locator("#btn-pause").click();
  const dialog = page.getByRole("dialog", { name: "Override YouTube traffic limit?" });
  await dialog.getByRole("button", { name: "Keep waiting", exact: true }).click();
  await expect(dialog).toBeHidden();
  expect(await page.evaluate(() => window.__bridgeCallsFor("youtube_traffic_override"))).toEqual([]);
  await expect(page.locator("#gsb-gpu-text")).toContainText("Processing waiting for 24-hour slot");
});

test("an expired Processing override request surfaces the backend response without starting Sync", async ({ page }) => {
  await loadQueues(page);
  await showQueues(page);
  await page.evaluate(() => window.__setBridgeHandler("youtube_traffic_override", () =>
    ({ ok: false, error: "Processing is no longer waiting for a traffic slot." })));
  await page.locator("#btn-pause").click();
  await page.getByRole("dialog", { name: "Override YouTube traffic limit?" })
    .getByRole("button", { name: "Override and continue", exact: true }).click();
  await expect(page.getByText("Processing is no longer waiting for a traffic slot.", { exact: true })).toBeVisible();
  await expect(page.locator("#gsb-sync-text")).toHaveText("Sync idle");
});

test("a Processing confirmation keeps the captured task ID when another task takes its place", async ({ page }) => {
  await loadQueues(page);
  await showQueues(page);
  await page.locator("#btn-pause").click();
  await currentState(page, "gpu", { trafficWait: { active: true, queue: "gpu",
    task_id: "replacement-task", reason: "daily_limit", until: Date.now() / 1000 + 600 } });
  await page.getByRole("dialog", { name: "Override YouTube traffic limit?" })
    .getByRole("button", { name: "Override and continue", exact: true }).click();
  await expect.poll(() => page.evaluate(() => window.__bridgeCallsFor("youtube_traffic_override").length)).toBe(1);
  expect(await page.evaluate(() => window.__bridgeCallsFor("youtube_traffic_override")[0].args))
    .toEqual(["gpu", "gpu-current"]);
});

test("a sync that finishes during confirmation reports only its remaining Processing grant", async ({ page }) => {
  await loadQueues(page);
  await showQueues(page, { sync: true, gpu: false, waiting: "sync" });
  await page.evaluate(() => window.__setBridgeHandler("youtube_traffic_override", () =>
    ({ ok: true, queue: "sync", followups_only: true })));
  await page.locator("#btn-pause").click();
  await page.getByRole("dialog", { name: "Override YouTube traffic limit?" })
    .getByRole("button", { name: "Override and continue", exact: true }).click();
  await expect(page.getByText(
    "The sync pass finished. Traffic ceilings overridden for its remaining Processing follow-ups.",
    { exact: true })).toBeVisible();
});

test("a Processing override remains accurately labelled while Sync has a pending manual pause", async ({ page }) => {
  await loadQueues(page);
  await showQueues(page, { sync: true });
  await currentState(page, "sync", { paused: true, pausedActive: false });
  await expect(page.locator("#btn-pause")).toHaveAttribute("data-pause-state", "traffic-wait");
  await expect(page.locator("#btn-pause")).toHaveAttribute("aria-label", /Processing is waiting.*override/);
  await expect(page.locator("#gsb-sync-text")).toHaveText("Sync paused (1)");
});
