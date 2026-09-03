const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

test("Download URL validation uses the real hostname", async ({ page }) => {
  await loadApp(page);

  const results = await page.evaluate(() => {
    const classify = (url) => ({
      youtube: window._urlLooksLikeYouTube(url),
      video: window._urlLooksLikeVideo(url),
    });
    return {
      valid: [
        "https://www.youtube.com/watch?v=fixture12345",
        "https://music.youtube.com/watch?v=fixture12345",
        "https://m.youtube.com/shorts/fixture12345",
        "https://youtu.be/fixture12345?si=shared",
      ].map(classify),
      invalid: [
        "https://evil.example/youtube.com/watch?v=fixture12345",
        "https://www.youtube.com.evil.example/watch?v=fixture12345",
        "https://www.youtube.com@evil.example/watch?v=fixture12345",
        "https://evil.example@www.youtube.com/watch?v=fixture12345",
      ].map(classify),
    };
  });

  expect(results.valid).toEqual([
    { youtube: true, video: true },
    { youtube: true, video: true },
    { youtube: true, video: true },
    { youtube: true, video: true },
  ]);
  expect(results.invalid).toEqual([
    { youtube: false, video: false },
    { youtube: false, video: false },
    { youtube: false, video: false },
    { youtube: false, video: false },
  ]);

  const input = page.locator("#url-input");
  const button = page.locator("#btn-download-single");
  await input.fill("https://www.youtube.com.evil.example/watch?v=fixture12345");
  await expect(button).toBeHidden();
  await expect(page.locator("#url-error-row")).toBeVisible();
  await input.fill("https://music.youtube.com/watch?v=fixture12345");
  await expect(button).toBeVisible();
  await expect(page.locator("#url-error-row")).toBeHidden();
});

test("single-download input does not show saved URL history", async ({ page }) => {
  await loadApp(page);
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("settings_load").length,
  )).toBeGreaterThan(0);

  const input = page.locator("#url-input");
  await expect(input).toHaveAttribute("autocomplete", "off");
  await expect(input).not.toHaveAttribute("list", /.+/);
  await expect(page.locator("#url-history-list")).toHaveCount(0);
  expect(await page.evaluate(() =>
    window.__bridgeCallsFor("url_history").length,
  )).toBe(0);
});

test("pasting a single-video URL still queues it normally", async ({ page }) => {
  await loadApp(page);
  const input = page.locator("#url-input");
  const button = page.locator("#btn-download-single");
  const url = "https://www.youtube.com/watch?v=fixture12345";

  await input.evaluate((element, pastedUrl) => {
    element.dispatchEvent(new Event("paste", { bubbles: true }));
    element.value = pastedUrl;
  }, url);
  await expect(button).toBeVisible();
  await button.click();

  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("archive_single_video").length,
  )).toBe(1);
  expect(await page.evaluate(() =>
    window.__bridgeCallsFor("archive_single_video")[0].args[0],
  )).toBe(url);
  await expect(input).toHaveValue("");
  await expect(button).toBeHidden();
});

test("dropped foreign URLs never reach the download bridge", async ({ page }) => {
  await loadApp(page);

  await page.evaluate(() => {
    const transfer = new DataTransfer();
    transfer.setData(
      "text/plain",
      "https://www.youtube.com@evil.example/watch?v=fixture12345",
    );
    document.getElementById("panel-download").dispatchEvent(new DragEvent(
      "drop",
      { bubbles: true, cancelable: true, dataTransfer: transfer },
    ));
  });

  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("archive_single_video").length)).toBe(0);
  await expect(page.locator("#toast-root .toast").last())
    .toContainText("Drop a YouTube URL to archive.");
});

test("Escape returns queue-popover focus to its invoking control", async ({ page }) => {
  await loadApp(page);

  const cases = [
    ["#btn-sync-tasks", "#popover-sync-tasks", "#btn-pause-sync-queue"],
    ["#gsb-gpu", "#popover-gpu-tasks", "#btn-pause-gpu-queue"],
  ];
  for (const [triggerSelector, popoverSelector, insideSelector] of cases) {
    const trigger = page.locator(triggerSelector);
    const popover = page.locator(popoverSelector);
    await trigger.click();
    await expect(popover).toHaveClass(/\bopen\b/);
    await page.locator(insideSelector).focus();
    await page.keyboard.press("Escape");
    await expect(popover).not.toHaveClass(/\bopen\b/);
    await expect(trigger).toBeFocused();
    await expect(trigger).toHaveAttribute("aria-expanded", "false");
  }
});
