const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

test("active reinstalls stay busy when installed dependencies render", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("onboarding_state", () => ({
      output_dir: "C:\\FixtureArchive",
      installing: { core: true, whisper: true },
      deps: {
        ytdlp: { ok: true },
        ffmpeg: { ok: true },
        ffprobe: { ok: true },
        python311: { ok: true },
        whisper: { checked: true, ok: true },
        gpu: { ok: true, name: "Fixture GPU" },
        cookies: {},
      },
    }));
    return window._startOnboarding({ force: true });
  });

  await expect(page.locator("#onb-install-core")).toBeDisabled();
  await expect(page.locator("#onb-install-core")).toHaveText("Installing…");
  await expect(page.locator("#onb-install-whisper")).toBeDisabled();
  await expect(page.locator("#onb-install-whisper")).toHaveText("Installing…");
});


test("every onboarding step names the dialog and receives heading focus", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("onboarding_state", () => ({
      output_dir: "C:\\FixtureArchive",
      installing: { core: false, whisper: false },
      deps: {
        ytdlp: { ok: true },
        ffmpeg: { ok: true },
        ffprobe: { ok: true },
        python311: { ok: false },
        whisper: { checked: true, ok: false },
        gpu: { ok: false },
        cookies: {},
      },
    }));
    return window._startOnboarding({ force: true });
  });

  const steps = [
    ["Review YTArchiver setup", "#onb-intro-title"],
    ["Pick an archive folder", "#onb-folder-title"],
    ["Choose a traffic safety level", "#onb-traffic-title"],
    ["Install dependencies", "#onb-deps-title"],
    ["You're all set", "#onb-done-title"],
  ];

  for (let index = 0; index < steps.length; index += 1) {
    const [name, headingSelector] = steps[index];
    await expect(page.getByRole("dialog", { name })).toBeVisible();
    await expect(page.locator(headingSelector)).toBeFocused();
    if (index < steps.length - 1) await page.locator("#onb-next").click();
  }

  await page.locator("#onb-back").click();
  await expect(page.getByRole("dialog", { name: "Install dependencies" }))
    .toBeVisible();
  await expect(page.locator("#onb-deps-title")).toBeFocused();
});
