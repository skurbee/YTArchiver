const { test, expect, loadApp } = require("./fixtures");

const VIDEO_URL = "https://www.youtube.com/watch?v=fixture12345";

async function expectUrlAlignment(page) {
  await expect.poll(() => page.evaluate(() => {
    const input = document.getElementById("url-input").getBoundingClientRect();
    const interval = document.querySelector(".download-schedule > .yt-dd").getBoundingClientRect();
    return Math.abs(input.right - interval.right);
  })).toBeLessThanOrEqual(1);
}

async function controlGeometry(page) {
  return page.evaluate(() => {
    const rect = id => {
      const bounds = document.getElementById(id).getBoundingClientRect();
      return { left: bounds.left, right: bounds.right, top: bounds.top,
        bottom: bounds.bottom, width: bounds.width, height: bounds.height };
    };
    return {
      input: rect("url-input"), download: rect("btn-download-single"),
      sync: rect("btn-sync-subbed"), pause: rect("btn-pause"),
      lastSync: rect("last-full-sync"),
      log: rect("main-log"), footer: rect("global-status-bar"),
      syncQueue: rect("gsb-sync"), processingQueue: rect("gsb-gpu"),
      viewport: { width: innerWidth, height: innerHeight },
      documentWidth: document.documentElement.scrollWidth,
    };
  });
}

test("Download controls remain compact and usable from the minimum window to ultrawide", async ({ page }) => {
  await loadApp(page, { configure: () => {
    let autorun = window.__fixtureDefaultResult("autorun_state");
    window.__setBridgeHandler("autorun_state", () => autorun);
    window.__setBridgeHandler("autorun_set", label => {
      autorun = { ...autorun, label, budget_mode: label === "When budget allows" };
      return { ok: true };
    });
  } });
  for (const viewport of [
    { width: 640, height: 480 },
    { width: 900, height: 720 },
    { width: 901, height: 720 },
    { width: 1440, height: 900 },
    { width: 2560, height: 1440 },
  ]) {
    await page.setViewportSize(viewport);
    await page.locator("#url-input").fill("");
    await expect(page.locator("#btn-download-single")).toBeHidden();
    await expect(page.locator("#video-opts-panel")).toBeHidden();
    await expectUrlAlignment(page);
    await page.locator("#url-input").fill("not a URL");
    await expect(page.locator("#btn-download-single")).toBeVisible();
    await expect(page.locator("#btn-download-single")).toBeDisabled();
    await expectUrlAlignment(page);
    const geometry = await controlGeometry(page);
    expect(geometry.documentWidth).toBeLessThanOrEqual(viewport.width);
    for (const control of [geometry.input, geometry.download, geometry.sync,
      geometry.pause, geometry.syncQueue, geometry.processingQueue]) {
      expect(control.left).toBeGreaterThanOrEqual(0);
      expect(control.right).toBeLessThanOrEqual(viewport.width);
      expect(control.bottom).toBeLessThanOrEqual(viewport.height);
      expect(control.height).toBeGreaterThanOrEqual(28);
    }
    expect(geometry.input.width).toBeLessThanOrEqual(600);
    expect(geometry.input.bottom).toBeLessThanOrEqual(geometry.sync.top);
    expect(geometry.lastSync.right).toBeLessThanOrEqual(viewport.width);
    if (viewport.width <= 900) {
      expect(geometry.lastSync.bottom).toBeLessThanOrEqual(geometry.input.top);
    } else {
      expect(geometry.lastSync.left).toBeGreaterThanOrEqual(geometry.download.right);
      expect(Math.abs(geometry.lastSync.top - geometry.input.top)).toBeLessThanOrEqual(1);
    }
    expect(geometry.download.left).toBeGreaterThanOrEqual(geometry.input.right);
    expect(Math.abs(geometry.download.top - geometry.input.top)).toBeLessThanOrEqual(1);
    expect(Math.abs(geometry.download.height - geometry.sync.height)).toBeLessThanOrEqual(1);
    expect(Math.abs(geometry.pause.height - geometry.sync.height)).toBeLessThanOrEqual(1);
    expect(geometry.log.height).toBeGreaterThanOrEqual(120);
    expect(geometry.log.bottom).toBeLessThanOrEqual(geometry.footer.top);
    expect(geometry.footer.bottom).toBeLessThanOrEqual(viewport.height);
  }
  await page.getByRole("combobox", { name: "Auto-sync:", exact: true }).click();
  await page.getByRole("option", { name: /^When budget allows\b/ }).click();
  await expect(page.locator("#auto-sync-select")).toHaveValue("When budget allows");
  await expectUrlAlignment(page);
  await page.setViewportSize({ width: 640, height: 480 });
  await expectUrlAlignment(page);
});

test("video options fit a narrow window and Enter submits their selected values", async ({ page }) => {
  await page.setViewportSize({ width: 640, height: 480 });
  await loadApp(page);
  const input = page.locator("#url-input");
  const download = page.locator("#btn-download-single");
  await input.fill(VIDEO_URL);
  await expect(download).toBeEnabled();
  await expect(page.locator("#video-opts-panel")).toBeVisible();
  await page.locator("#vo-save-to").fill("C:\\FixtureArchive\\Manual");
  await page.locator("#vo-resolution").selectOption("720");
  await page.locator("#vo-use-yt-title").uncheck();
  await page.locator("#vo-custom-name").fill("Fixture download");
  await page.locator("#vo-grab-metadata").check();
  await page.locator("#vo-transcribe").check();
  await page.locator("#vo-transcribe").scrollIntoViewIfNeeded();
  await expect(page.locator("#vo-transcribe")).toBeInViewport();
  const overflow = await page.locator("#video-opts-panel").evaluate(panel => ({
    right: panel.getBoundingClientRect().right,
    width: panel.clientWidth, scrollWidth: panel.scrollWidth,
  }));
  expect(overflow.right).toBeLessThanOrEqual(640);
  expect(overflow.scrollWidth).toBeLessThanOrEqual(overflow.width);
  await input.press("Enter");
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("archive_single_video").length)).toBe(1);
  const call = await page.evaluate(() => window.__bridgeCallsFor("archive_single_video")[0]);
  expect(call.args).toEqual([VIDEO_URL, {
    save_to: "C:\\FixtureArchive\\Manual", resolution: "720", date_file: true,
    add_date: false, use_yt_title: false, custom_name: "Fixture download", grab_metadata: true,
    transcribe: true,
  }]);
  await expect(input).toHaveValue("");
  await expect(download).toBeHidden();
  await expect(download).toBeDisabled();
  await expect(page.locator("#video-opts-panel")).toBeHidden();
});

test("manual processing defaults on and saved choices reach click submissions", async ({ page }) => {
  await loadApp(page);
  const input = page.locator("#url-input");
  const transcribe = page.locator("#vo-transcribe");
  await input.fill(VIDEO_URL);
  await expect(transcribe).toBeChecked();
  await expect(page.locator("#vo-grab-metadata")).toBeChecked();
  await page.locator("#vo-grab-metadata").uncheck();
  for (const enabled of [false, true]) {
    await input.fill(VIDEO_URL);
    await transcribe.setChecked(enabled);
    await page.reload({ waitUntil: "load" });
    await page.evaluate(() => window.YT.settingsReady);
    await input.fill(VIDEO_URL);
    await expect(transcribe).toBeChecked({ checked: enabled });
    await expect(page.locator("#vo-grab-metadata")).not.toBeChecked();
    await page.locator("#btn-download-single").click();
    await expect.poll(() => page.evaluate(() =>
      window.__bridgeCallsFor("archive_single_video").length)).toBe(1);
    const call = await page.evaluate(() => window.__bridgeCallsFor("archive_single_video")[0]);
    expect(call.args[0]).toBe(VIDEO_URL);
    expect(call.args[1]).toMatchObject({ transcribe: enabled, grab_metadata: false });
    await expect(input).toHaveValue("");
    await expect(page.locator("#btn-download-single")).toBeHidden();
  }
});

test("dropped videos keep the saved transcription choice while video options are collapsed", async ({ page }) => {
  await loadApp(page);
  const input = page.locator("#url-input");
  for (const enabled of [false, true]) {
    await input.fill(VIDEO_URL);
    await page.locator("#vo-transcribe").setChecked(enabled);
    await page.reload({ waitUntil: "load" });
    await page.evaluate(() => window.YT.settingsReady);
    await expect(page.locator("#video-opts-panel")).toBeHidden();
    await page.evaluate(url => {
      const transfer = new DataTransfer();
      transfer.setData("text/uri-list", url);
      document.getElementById("panel-download").dispatchEvent(new DragEvent("drop", {
        bubbles: true, cancelable: true, dataTransfer: transfer,
      }));
    }, VIDEO_URL);
    await expect.poll(() => page.evaluate(() =>
      window.__bridgeCallsFor("archive_single_video").length)).toBe(1);
    const call = await page.evaluate(() => window.__bridgeCallsFor("archive_single_video")[0]);
    expect(call.args[0]).toBe(VIDEO_URL);
    expect(call.args[1]).toMatchObject({ transcribe: enabled });
    await expect(input).toHaveValue("");
    await expect(page.locator("#btn-download-single")).toBeHidden();
    await expect(page.locator("#video-opts-panel")).toBeHidden();
  }
});

test("Escape clears URL feedback and a channel URL still opens its prefilled editor", async ({ page }) => {
  await loadApp(page);
  const input = page.locator("#url-input");
  const download = page.locator("#btn-download-single");
  await input.fill("   ");
  await expect(download).toBeHidden();
  await expect(page.locator("#video-opts-panel")).toBeHidden();
  await expect(page.locator("#channel-nudge-panel")).toBeHidden();
  await input.fill("https://example.invalid/video");
  await expect(download).toBeVisible();
  await expect(page.locator("#url-error-row")).toBeVisible();
  await expect(download).toBeDisabled();
  await input.press("Escape");
  await expect(input).toHaveValue("");
  await expect(page.locator("#url-error-row")).toBeHidden();
  await expect(download).toBeHidden();
  await expect(download).toBeDisabled();

  const channel = "https://www.youtube.com/@FixtureChannel";
  await input.fill(channel);
  const notice = page.locator("#channel-nudge-panel");
  await expect(download).toBeVisible();
  await expect(notice).toBeVisible();
  await expect(notice).toContainText("Channel link detected");
  await expect(page.locator("#video-opts-panel")).toBeHidden();
  await expect(download).toBeDisabled();
  for (const width of [640, 1440]) {
    await page.setViewportSize({ width, height: width === 640 ? 480 : 900 });
    const noticeBounds = await notice.boundingBox();
    const geometry = await controlGeometry(page);
    expect(noticeBounds.width).toBeLessThanOrEqual(580);
    expect(noticeBounds.x + noticeBounds.width).toBeLessThanOrEqual(width);
    expect(Math.abs(noticeBounds.x - geometry.input.left)).toBeLessThanOrEqual(1);
    expect(noticeBounds.y).toBeGreaterThanOrEqual(geometry.input.bottom);
    expect(noticeBounds.y + noticeBounds.height).toBeLessThanOrEqual(geometry.sync.top);
    await expect(notice.getByRole("button", { name: "Add this channel", exact: true })).toBeInViewport();
  }
  await input.press("Enter");
  expect(await page.evaluate(() => window.__bridgeCallsFor("archive_single_video"))).toEqual([]);
  await page.locator("#btn-channel-nudge-add").click();
  await expect(page.locator("#channel-editor-backdrop")).toBeVisible();
  await expect(page.locator("#edit-url")).toHaveValue(channel);
  await expect(input).toHaveValue("");
  await expect(download).toBeHidden();
  await expect(page.locator("#channel-nudge-panel")).toBeHidden();
});

test("editing a URL while submission is pending cannot reenable or duplicate the download", async ({ page }) => {
  await loadApp(page, { configure: () => {
    window.__setBridgeHandler("archive_single_video", () => new Promise(resolve => {
      window.__finishDownload = resolve;
    }));
  } });
  const input = page.locator("#url-input");
  const download = page.locator("#btn-download-single");
  await input.fill(VIDEO_URL);
  await download.click();
  await expect.poll(() => page.evaluate(() => typeof window.__finishDownload)).toBe("function");
  await expect(download).toBeDisabled();
  const nextUrl = "https://youtu.be/fixture67890";
  await input.fill(nextUrl);
  await expect(download).toBeDisabled();
  await input.press("Enter");
  expect(await page.evaluate(() => window.__bridgeCallsFor("archive_single_video").length)).toBe(1);
  await page.evaluate(() => window.__finishDownload({ ok: true, queued: 1 }));
  await expect(input).toHaveValue(nextUrl);
  await expect(download).toBeEnabled();
  await input.press("Escape");
  await expect(download).toBeHidden();
  await expect(download).toBeDisabled();
});

test("the global pause control visibly describes running, pending, resumed and queued states", async ({ page }) => {
  await loadApp(page);
  const label = page.locator("#btn-pause .pause-label");
  const setSync = patch => page.evaluate(patch => window.setQueueState({
    sync: { running: true, paused: false, pausedActive: false, count: 1,
      trafficWaiting: false, sessionLimited: false, resumePending: false, ...patch },
    gpu: { running: false, paused: false, pausedActive: false, count: 0,
      trafficWaiting: false, resumePending: false },
  }), patch);
  await setSync({});
  await expect(label).toHaveText("Pause all");
  await expectUrlAlignment(page);
  await setSync({ paused: true });
  await expect(label).toHaveText("Pausing…");
  await setSync({ paused: true, pausedActive: true });
  await expect(label).toHaveText("Resume all");
  await expectUrlAlignment(page);
  await page.evaluate(() => window._setQueueResumePending("sync", true));
  await expect(label).toHaveText("Resuming…");
  await expect(page.locator("#btn-pause")).toBeDisabled();
  await page.evaluate(() => window._setQueueResumePending("sync", false));
  await setSync({ running: false });
  await expect(label).toHaveText("Start queued");
  await expect(page.locator("#btn-pause")).toBeEnabled();
});

test("each footer queue has one keyboard-accessible opener that works from other tabs", async ({ page }) => {
  await page.setViewportSize({ width: 640, height: 480 });
  await loadApp(page);
  for (const tab of ["browse", "health", "settings"]) {
    await page.locator(`.tab[data-tab="${tab}"]`).click();
    for (const queue of ["sync", "gpu"]) {
      const trigger = page.locator(`#gsb-${queue}`);
      const popover = page.locator(`#popover-${queue}-tasks`);
      await expect(page.locator(`button[aria-controls="popover-${queue}-tasks"]`)).toHaveCount(1);
      expect(await trigger.evaluate(el => el.tagName)).toBe("BUTTON");
      await trigger.focus();
      await trigger.press(queue === "sync" ? "Enter" : "Space");
      await expect(popover).toHaveClass(/\bopen\b/);
      await expect(trigger).toHaveAttribute("aria-expanded", "true");
      const bounds = await popover.boundingBox();
      expect(bounds.x).toBeGreaterThanOrEqual(0);
      expect(bounds.y).toBeGreaterThanOrEqual(0);
      expect(bounds.x + bounds.width).toBeLessThanOrEqual(640);
      expect(bounds.y + bounds.height).toBeLessThanOrEqual(480);
      await page.keyboard.press("Escape");
      await expect(popover).not.toHaveClass(/\bopen\b/);
      await expect(trigger).toBeFocused();
      await expect(trigger).toHaveAttribute("aria-expanded", "false");
    }
  }
});

test("active queue names and counts remain readable beside request limits at intermediate widths", async ({ page }) => {
  await page.clock.install();
  await loadApp(page, { bridge: { responses: {
    youtube_traffic_status: { ok: true, mode: "custom", paused: false,
      hourly_used: 975, hourly_limit: 1000, daily_used: 5975, daily_limit: 6000 },
  } } });
  // Session errors ignore the historical log seed during initial startup.
  await page.clock.fastForward(3000);
  await page.evaluate(() => {
    window.renderQueues({
      sync: [{ task_id: "sync-current", kind: "download", status: "running",
        name: "Syncing a fixture channel with a very long descriptive title" }],
      gpu: [{ task_id: "gpu-current", kind: "transcribe", status: "running",
        name: "Transcribing a fixture video with a very long descriptive title" }],
      sync_count: 1234, gpu_count: 5678, identity_ids_durable: true,
    });
    window.setQueueState({
      sync: { running: true, paused: false, pausedActive: false, count: 1234 },
      gpu: { running: true, paused: false, pausedActive: false, count: 5678 },
    });
    window._setIndicator("sweep", "Index scan waiting for active work to finish before checking archive files");
    window.appendMainLog([["Fixture operation failed", "error_detail"]]);
  });
  await expect(page.locator("#gsb-traffic")).toBeVisible();
  await expect(page.locator("#gsb-index")).toBeVisible();
  await expect(page.locator("#gsb-errors")).toBeVisible();
  for (const width of [640, 820, 900, 961, 1024, 1440]) {
    await page.setViewportSize({ width, height: 720 });
    const controls = await page.locator("#gsb-sync, #gsb-gpu, #gsb-traffic")
      .evaluateAll(elements => elements.map(el => {
        const bounds = el.getBoundingClientRect();
        return { id: el.id, left: bounds.left, right: bounds.right,
          width: el.clientWidth, scrollWidth: el.scrollWidth,
          statusWidth: el.querySelector(".gsb-text")?.clientWidth,
          children: [...el.querySelectorAll(".gsb-queue-name, .gsb-queue-count, .gsb-chevron")]
            .map(child => ({ right: child.getBoundingClientRect().right,
              width: child.clientWidth, scrollWidth: child.scrollWidth })) };
      }));
    for (const control of controls) {
      expect(control.left, `${control.id} left at ${width}px`).toBeGreaterThanOrEqual(0);
      expect(control.right, `${control.id} right at ${width}px`).toBeLessThanOrEqual(width);
      expect(control.scrollWidth, `${control.id} content at ${width}px`)
        .toBeLessThanOrEqual(control.width);
      if (control.statusWidth !== undefined) {
        expect(control.statusWidth, `${control.id} status at ${width}px`).toBeGreaterThanOrEqual(24);
      }
      for (const child of control.children) {
        expect(child.right, `${control.id} label/count at ${width}px`)
          .toBeLessThanOrEqual(control.right);
        expect(child.scrollWidth).toBeLessThanOrEqual(child.width);
      }
    }
  }
});

test("a session rate limit retains its accessible description without queue hover tooltips", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.renderQueues({ sync: [{ task_id: "sync-limited", status: "running",
      name: "Syncing Fixture channel" }], gpu: [], identity_ids_durable: true });
    window.setQueueState({
      sync: { running: true, paused: false, pausedActive: false, count: 1,
        trafficWaiting: false, sessionLimited: true },
      gpu: { running: false, paused: false, pausedActive: false, count: 0 },
    });
  });
  await expect(page.locator("#gsb-sync-text")).toHaveText("Paused by YouTube");
  await expect(page.locator("#gsb-sync")).toHaveAttribute("aria-label", /paused by YouTube's session rate limit/);
  for (const queue of ["sync", "gpu"]) {
    await expect(page.locator(`#gsb-${queue}`)).not.toHaveAttribute("data-tooltip");
    await expect(page.locator(`#gsb-${queue}`)).not.toHaveAttribute("title");
    await expect(page.locator(`#gsb-${queue}`)).toHaveAttribute("aria-label", /queue: .+\. Open queue/);
  }
});

test("the surrounding controls preserve log content and typography until Clear is confirmed", async ({ page }) => {
  await loadApp(page, { bridge: { settings: { show_activity_log: false } } });
  await page.evaluate(() => window.renderMainLog([
    [["Fixture completed download", "success"]],
    [["Fixture transcript complete", "info"]],
  ]));
  const log = page.locator("#main-log");
  const snapshot = () => log.evaluate(el => {
    const style = getComputedStyle(el);
    return { html: el.innerHTML, className: el.className, fontFamily: style.fontFamily,
      fontSize: style.fontSize, lineHeight: style.lineHeight, color: style.color,
      background: style.backgroundColor, padding: style.padding, whiteSpace: style.whiteSpace };
  });
  const before = await snapshot();
  await page.locator("#url-input").fill(VIDEO_URL);
  await page.locator("#url-input").press("Escape");
  await page.locator("#gsb-sync").click();
  await page.keyboard.press("Escape");
  expect(await snapshot()).toEqual(before);

  const clear = page.locator("#btn-clear-menu");
  await expect(clear).toBeVisible();
  const clearBounds = await clear.boundingBox();
  const logBounds = await log.boundingBox();
  expect(clearBounds.y + clearBounds.height).toBeLessThanOrEqual(logBounds.y);
  expect(clearBounds.x).toBeGreaterThan(logBounds.x + logBounds.width / 2);
  await clear.click();
  const dialog = page.getByRole("dialog", { name: "Clear log", exact: true });
  await expect(dialog).toBeVisible();
  await dialog.getByRole("button", { name: "Cancel", exact: true }).click();
  expect(await snapshot()).toEqual(before);
  await clear.click();
  await dialog.getByRole("button", { name: "Clear", exact: true }).click();
  await expect(log).toBeEmpty();
  await expect(clear).toBeHidden();
});
