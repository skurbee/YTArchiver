const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

async function bootWithPreferences(page, { local = {}, saved = {}, delayed = false } = {}) {
  await page.addInitScript(({ local, saved, delayed }) => {
    for (const [key, value] of Object.entries(local)) {
      localStorage.setItem(`ytarchiver_caption_${key}`, value);
    }
    const settings = {
      output_dir: "C:\\FixtureArchive", video_out_dir: "C:\\FixtureArchive",
      default_resolution: "1080", ...saved,
    };
    window.__captionSettingsCalls = 0;
    const pending = delayed ? new Promise(resolve => {
      window.__releaseCaptionSettings = () => resolve(settings);
    }) : null;
    const configure = setHandler => setHandler("settings_load", () => {
      window.__captionSettingsCalls++;
      return pending || Promise.resolve(settings);
    });
    // Fixture init scripts may execute in either order. Attach the handler
    // before application boot without relying on a runtime-private helper.
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
  }, { local, saved, delayed });
  await loadApp(page);
}

async function settleFrames(page) {
  await page.evaluate(() => new Promise(resolve => {
    requestAnimationFrame(() => requestAnimationFrame(resolve));
  }));
}

async function renderFixture(page, words = ["Earlier", "current", "later"]) {
  await page.locator('.tab[data-tab="browse"]').click();
  await page.evaluate(words => {
    window.showView("watch");
    const video = document.getElementById("watch-video");
    Object.defineProperty(video, "currentTime", { configurable: true, get: () => 1.2 });
    Object.defineProperty(video, "paused", { configurable: true, get: () => true });
    video.hidden = false;
    video.style.aspectRatio = "16 / 9";
    document.getElementById("watch-video-placeholder").hidden = true;
    const wrap = video.closest(".watch-video-wrap");
    wrap.style.width = "640px";
    wrap.style.maxWidth = "100%";
    window.renderWatchView({
      video_id: `${words[0]}-fixture`, title: `${words[0]} fixture`, channel: "Example",
    }, [{
      s: 0, e: 101, text: words.join(" "),
      words: [
        { w: words[0], s: 0, e: 0.9 },
        { w: words[1], s: 1, e: 99 },
        { w: words[2], s: 100, e: 101 },
      ],
    }], null, { skipVideoReload: true });
  }, words);
  await settleFrames(page);
}

async function expectMode(page, mode, words = ["Earlier", "current", "later"]) {
  await expect(page.locator("#watch-cap-mode")).toHaveValue(mode);
  await expect(page.locator("#watch-cap-ovl")).toBeVisible();
  await expect(page.locator("#watch-cap-ovl")).toHaveAttribute("data-cap-mode", mode);
  const output = await page.locator("#watch-cap-ovl").evaluate(overlay => ({
    cells: Array.from(overlay.children, cell => cell.textContent.trim()),
    lines: Array.from(overlay.querySelectorAll(".cap-ovl-line"), row => row.textContent.trim()),
  }));
  if (mode === "default") expect(output.lines).toEqual(["", words.slice(0, 2).join(" ")]);
  if (mode === "single") expect(output.cells).toEqual(["", words[1], ""]);
  if (mode === "phrase3") expect(output.cells).toEqual(words);
}

test("fresh startup selects YT Style while leaving the overlay Off until enabled", async ({ page }) => {
  await bootWithPreferences(page);
  await expect(page.locator("#watch-cap-mode")).toHaveValue("default");
  await expect(page.locator("#watch-cap-size")).toHaveValue("off");
  await renderFixture(page);
  await expect(page.locator("#watch-cap-ovl")).not.toBeVisible();
  await page.locator("#watch-cap-size").selectOption("medium");
  await expectMode(page, "default");
});

for (const source of ["local", "backend"]) {
  for (const mode of ["single", "phrase3"]) {
    test(`startup ignores the old ${mode} choice from ${source} preferences`, async ({ page }) => {
      await bootWithPreferences(page, source === "local"
        ? { local: { size: "medium", mode } }
        : { saved: { caption_overlay_size: "medium", caption_overlay_mode: mode } });
      await expect(page.locator("#watch-cap-size")).toHaveValue("medium");
      await renderFixture(page);
      await expectMode(page, "default");
    });
  }
}

test("every Off-to-enabled transition starts in YT Style for all four sizes", async ({ page }) => {
  await bootWithPreferences(page, { local: { size: "medium" } });
  await renderFixture(page);
  for (const previousMode of ["single", "phrase3"]) {
    for (const size of ["xsmall", "small", "medium", "large"]) {
      await page.locator("#watch-cap-mode").selectOption(previousMode);
      await expectMode(page, previousMode);
      await page.locator("#watch-cap-size").selectOption("off");
      await expect(page.locator("#watch-cap-ovl")).not.toBeVisible();
      await page.locator("#watch-cap-size").selectOption(size);
      await expectMode(page, "default");
    }
  }
});

for (const mode of ["single", "phrase3"]) {
  test(`an explicit ${mode} choice survives enabled size changes, resizing, and video changes`, async ({ page }) => {
    await bootWithPreferences(page, { local: { size: "medium" } });
    await renderFixture(page);
    await page.locator("#watch-cap-mode").selectOption(mode);
    await expectMode(page, mode);
    for (const size of ["xsmall", "small", "medium", "large"]) {
      await page.locator("#watch-cap-size").selectOption(size);
      await expectMode(page, mode);
    }
    await page.evaluate(() => {
      document.querySelector(".watch-video-wrap").style.width = "360px";
      window.dispatchEvent(new Event("resize"));
    });
    await settleFrames(page);
    await expectMode(page, mode);
    await page.locator("#watch-video-stage").hover();
    await page.locator("#watch-fs-btn").click();
    await expect(page.locator("#watch-video-stage")).toHaveClass(/cssfs/);
    await expectMode(page, mode);
    await page.keyboard.press("Escape");
    await expect(page.locator("#watch-video-stage")).not.toHaveClass(/cssfs/);
    const replacement = ["Another", "present", "future"];
    await renderFixture(page, replacement);
    await expectMode(page, mode, replacement);
  });
}

test("late saved settings cannot undo a newer enabled size, word mode, or background choice", async ({ page }) => {
  await bootWithPreferences(page, {
    delayed: true,
    saved: { caption_overlay_size: "off", caption_overlay_mode: "single", caption_overlay_bg: "translucent" },
  });
  await renderFixture(page);
  expect(await page.evaluate(() => window.__captionSettingsCalls)).toBeGreaterThan(0);
  await page.locator("#watch-cap-size").selectOption("large");
  await page.locator("#watch-cap-mode").selectOption("phrase3");
  await page.locator("#watch-cap-bg").selectOption("outline");
  await expectMode(page, "phrase3");
  await page.evaluate(() => window.__releaseCaptionSettings());
  await settleFrames(page);
  await expect(page.locator("#watch-cap-size")).toHaveValue("large");
  await expect(page.locator("#watch-cap-bg")).toHaveValue("outline");
  await expectMode(page, "phrase3");
});

test("late saved settings cannot re-enable captions after the user turns them Off", async ({ page }) => {
  await bootWithPreferences(page, {
    local: { size: "medium" }, delayed: true,
    saved: { caption_overlay_size: "large", caption_overlay_mode: "phrase3" },
  });
  await renderFixture(page);
  await page.locator("#watch-cap-mode").selectOption("single");
  await page.locator("#watch-cap-size").selectOption("off");
  await page.evaluate(() => window.__releaseCaptionSettings());
  await settleFrames(page);
  await expect(page.locator("#watch-cap-size")).toHaveValue("off");
  await expect(page.locator("#watch-cap-ovl")).not.toBeVisible();
  await page.locator("#watch-cap-size").selectOption("small");
  await expectMode(page, "default");
});
