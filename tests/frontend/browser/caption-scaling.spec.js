const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

const BASE_FONT = { xsmall: 6, small: 13, medium: 20, large: 26 };
const MODES = ["single", "phrase3", "default"];

async function settleFrames(page) {
  await page.evaluate(() => new Promise(resolve => {
    requestAnimationFrame(() => requestAnimationFrame(resolve));
  }));
}

async function renderFixture(page, options = {}) {
  await page.addStyleTag({ content: `
    #watch-video-stage:not(.cssfs) {
      width: var(--fixture-player-width, 640px);
      height: var(--fixture-player-height, 360px);
      flex: none;
    }
    #watch-video-stage:not(.cssfs) > #watch-video { width: 100%; height: 100%; }
  ` });
  await page.locator('.tab[data-tab="browse"]').click();
  await page.evaluate(options => {
    window.showView("watch");
    const stage = document.getElementById("watch-video-stage");
    stage.style.setProperty("--fixture-player-width", `${options.width ?? 640}px`);
    stage.style.setProperty("--fixture-player-height", `${options.height ?? 360}px`);
    const video = document.getElementById("watch-video");
    // Geometry is real browser layout; only media playback is synthetic.
    Object.defineProperty(video, "currentTime", { configurable: true, get: () => 1.2 });
    Object.defineProperty(video, "paused", { configurable: true, get: () => true });
    video.hidden = false;
    document.getElementById("watch-video-placeholder").hidden = true;
    if (!options.keepPreferences) {
      for (const [id, value] of [
        ["watch-cap-size", options.size || "medium"],
        ["watch-cap-mode", options.mode || "default"],
        ["watch-cap-bg", "translucent"],
      ]) {
        const select = document.getElementById(id);
        select.value = value;
        select.dispatchEvent(new Event("change", { bubbles: true }));
      }
    }
    window.renderWatchView({
      video_id: "scaling-fixture", title: "Caption sizing fixture", channel: "Example",
    }, [{
      s: 0, e: 101, text: "Earlier current later",
      words: [
        { w: "Earlier", s: 0, e: 0.9 },
        { w: "current", s: 1, e: 99 },
        { w: "later", s: 100, e: 101 },
      ],
    }], null, { skipVideoReload: true });
  }, options);
  await settleFrames(page);
}

async function openFixture(page, options = {}) {
  await loadApp(page);
  await renderFixture(page, options);
}

async function setDimensions(page, width, height, event = null) {
  await page.evaluate(({ width, height, event }) => {
    const stage = document.getElementById("watch-video-stage");
    stage.style.setProperty("--fixture-player-width", `${width}px`);
    stage.style.setProperty("--fixture-player-height", `${height}px`);
    if (event) document.getElementById("watch-video").dispatchEvent(new Event(event));
  }, { width, height, event });
  await settleFrames(page);
}

async function metrics(page) {
  return page.locator("#watch-cap-ovl").evaluate(overlay => {
    const video = document.getElementById("watch-video");
    const current = overlay.querySelector(".cap-ovl-cur");
    const rows = Array.from(overlay.querySelectorAll(".cap-ovl-line"));
    const text = current.querySelector(".cap-ovl-text") || current;
    const style = getComputedStyle(text);
    const rect = text.getBoundingClientRect();
    const player = video.getBoundingClientRect();
    return {
      shown: overlay.classList.contains("show"),
      playerWidth: player.width, playerHeight: player.height,
      font: Number.parseFloat(style.fontSize),
      paddingX: Number.parseFloat(style.paddingLeft),
      paddingY: Number.parseFloat(style.paddingTop),
      paintedWidth: rect.width, paintedHeight: rect.height,
      overlayHeight: overlay.getBoundingClientRect().height,
      rowHeights: rows.map(row => row.getBoundingClientRect().height),
      rowGap: rows.length === 2
        ? rows[1].getBoundingClientRect().top - rows[0].getBoundingClientRect().bottom : null,
      lines: rows.map(row => row.textContent.trim()),
      cells: Array.from(overlay.children, cell => cell.textContent.trim()),
      time: video.currentTime,
    };
  });
}

async function expectFont(page, value) {
  await expect.poll(async () => (await metrics(page)).font).toBeCloseTo(value, 1);
}

function expectRatio(actual, original, ratio) {
  expect(Math.abs(actual - original * ratio)).toBeLessThanOrEqual(0.8);
}

for (const mode of MODES) {
  for (const [size, baseline] of Object.entries(BASE_FONT)) {
    test(`${mode} ${size} captions scale every rendered metric with the player`, async ({ page }) => {
      await openFixture(page, { mode, size });
      await expectFont(page, baseline);
      const initial = await metrics(page);
      expect(initial.shown).toBe(true);
      expect(initial.playerWidth).toBe(640);
      expect(initial.playerHeight).toBe(360);

      // No window/metadata/time event: a paused player-only resize must work.
      await setDimensions(page, 1280, 720);
      await expectFont(page, baseline * 2);
      const doubled = await metrics(page);
      expect(doubled.playerWidth).toBe(1280);
      expect(doubled.playerHeight).toBe(720);
      for (const property of ["paddingX", "paddingY", "paintedWidth", "paintedHeight", "overlayHeight"]) {
        expectRatio(doubled[property], initial[property], 2);
      }
      expect(doubled.lines).toEqual(initial.lines);
      expect(doubled.cells).toEqual(initial.cells);
      if (mode === "default") {
        expect(doubled.rowHeights).toHaveLength(2);
        initial.rowHeights.forEach((height, i) => expectRatio(doubled.rowHeights[i], height, 2));
        expectRatio(doubled.rowGap, initial.rowGap, 2);
      }
      expect(doubled.time).toBe(initial.time);
    });
  }

  test(`${mode} sizing uses the limiting player dimension without a fixed font floor`, async ({ page }) => {
    await openFixture(page, { mode, size: "medium" });
    await expectFont(page, BASE_FONT.medium);
    await setDimensions(page, 1280, 180);
    await expectFont(page, BASE_FONT.medium / 2);
    const short = await metrics(page);
    expect(short.playerWidth).toBe(1280);
    expect(short.playerHeight).toBe(180);
    await setDimensions(page, 160, 720);
    await expectFont(page, BASE_FONT.medium / 4);
    expect((await metrics(page)).shown).toBe(true);
  });

  test(`${mode} size follows the paused player rather than the application window`, async ({ page }) => {
    await openFixture(page, { mode, size: "medium" });
    const initial = await metrics(page);
    await page.setViewportSize({ width: 1700, height: 1100 });
    await settleFrames(page);
    const wide = await metrics(page);
    await page.setViewportSize({ width: 1200, height: 850 });
    await settleFrames(page);
    const narrow = await metrics(page);
    for (const current of [wide, narrow]) {
      expect(current.playerWidth).toBe(initial.playerWidth);
      expect(current.playerHeight).toBe(initial.playerHeight);
      expect(current.font).toBe(initial.font);
      expect(current.paintedHeight).toBe(initial.paintedHeight);
      expect(current.lines).toEqual(initial.lines);
    }
  });

  test(`${mode} scales into window fullscreen and restores its paused size`, async ({ page }) => {
    await openFixture(page, { mode, size: "medium" });
    const initial = await metrics(page);
    await page.locator("#watch-video-stage").hover();
    await page.locator("#watch-fs-btn").click();
    await expect(page.locator("#watch-video-stage")).toHaveClass(/cssfs/);
    await settleFrames(page);
    const dimensions = await metrics(page);
    const factor = Math.min(dimensions.playerWidth / 640, dimensions.playerHeight / 360);
    expect(factor).toBeGreaterThan(1);
    await expectFont(page, BASE_FONT.medium * factor);
    expect((await metrics(page)).paintedHeight).toBeGreaterThan(initial.paintedHeight);
    await page.keyboard.press("Escape");
    await expect(page.locator("#watch-video-stage")).not.toHaveClass(/cssfs/);
    await expectFont(page, initial.font);
    const restored = await metrics(page);
    expect(restored.lines).toEqual(initial.lines);
    expect(restored.paintedHeight).toBe(initial.paintedHeight);
    expect(restored.time).toBe(initial.time);
  });

  test(`${mode} initializes at player size and keeps the last good scale through zero size`, async ({ page }) => {
    await openFixture(page, { mode, size: "medium", width: 320, height: 180 });
    await expectFont(page, BASE_FONT.medium / 2);
    await setDimensions(page, 0, 0, "loadedmetadata");
    expect((await metrics(page)).playerWidth).toBe(0);
    expect((await metrics(page)).playerHeight).toBe(0);
    await expectFont(page, BASE_FONT.medium / 2);
    await setDimensions(page, 960, 540, "loadedmetadata");
    await expectFont(page, BASE_FONT.medium * 1.5);
    await page.evaluate(() => {
      document.getElementById("watch-video-stage").style.display = "none";
      document.getElementById("watch-video").dispatchEvent(new Event("resize"));
    });
    await settleFrames(page);
    await expectFont(page, BASE_FONT.medium * 1.5);
    await page.evaluate(() => {
      document.getElementById("watch-video-stage").style.removeProperty("display");
      document.getElementById("watch-video").dispatchEvent(new Event("loadedmetadata"));
    });
    await expectFont(page, BASE_FONT.medium * 1.5);
    expect((await metrics(page)).shown).toBe(true);
  });
}

test("X-small remains a distinct persisted caption size after reload", async ({ page }) => {
  await openFixture(page, { mode: "default", size: "small" });
  await expectFont(page, BASE_FONT.small);
  await expect(page.locator("#watch-cap-mode option")).toHaveText(["YT Style", "3 Words", "1 Word"]);
  expect(await page.locator("#watch-cap-mode option").evaluateAll(options => options.map(option => option.value)))
    .toEqual(["default", "phrase3", "single"]);
  await expect(page.locator('#watch-cap-size option[value="xsmall"]')).toHaveText("X-small");
  await page.locator("#watch-cap-size").selectOption("xsmall");
  await expectFont(page, BASE_FONT.xsmall);
  expect(await page.evaluate(() => localStorage.getItem("ytarchiver_caption_size"))).toBe("xsmall");
  await expect.poll(() => page.evaluate(() => window.__bridgeCallsFor("settings_save")
    .some(call => call.args[0]?.caption_overlay_size === "xsmall"))).toBe(true);
  await page.addInitScript(() => {
    // Confirm backend preferences can restore X-small independently of the
    // local browser cache. Install when the fixture bridge becomes available,
    // regardless of which addInitScript runs first.
    localStorage.removeItem("ytarchiver_caption_size");
    const configure = setHandler => setHandler("settings_load", () => Promise.resolve({
      output_dir: "C:\\FixtureArchive", video_out_dir: "C:\\FixtureArchive",
      default_resolution: "1080", caption_overlay_size: "xsmall",
      caption_overlay_mode: "default", caption_overlay_bg: "translucent",
    }));
    if (typeof window.__setBridgeHandler === "function") configure(window.__setBridgeHandler);
    else Object.defineProperty(window, "__setBridgeHandler", {
      configurable: true,
      set(handler) {
        Object.defineProperty(window, "__setBridgeHandler", {
          configurable: true, writable: true, value: handler,
        });
        configure(handler);
      },
    });
  });
  await page.reload({ waitUntil: "load" });
  await page.waitForFunction(() => window._watchActionsInited === true);
  await renderFixture(page, { keepPreferences: true });
  await expect(page.locator("#watch-cap-size")).toHaveValue("xsmall");
  await expectFont(page, BASE_FONT.xsmall);
  expect((await metrics(page)).shown).toBe(true);
});
