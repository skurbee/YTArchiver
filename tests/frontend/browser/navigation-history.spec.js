const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

async function mouseHistory(page, button) {
  await page.evaluate((mouseButton) => {
    window.dispatchEvent(new MouseEvent("mousedown", {
      button: mouseButton,
      bubbles: true,
      cancelable: true,
    }));
    window.dispatchEvent(new MouseEvent("mouseup", {
      button: mouseButton,
      bubbles: true,
      cancelable: true,
    }));
    window.dispatchEvent(new MouseEvent("auxclick", {
      button: mouseButton,
      bubbles: true,
      cancelable: true,
    }));
  }, button);
}

async function activeTopTab(page) {
  return page.locator(".tab.active[data-tab]").getAttribute("data-tab");
}

async function activeSubview(page, panel) {
  return page.locator(
    `#panel-${panel} .settings-subnav-btn.active[data-settings-view]`,
  ).getAttribute("data-settings-view");
}

test("mouse Back and Forward traverse Health pages and Settings", async ({ page }) => {
  await loadApp(page);
  await expect.poll(() => page.evaluate(() =>
    window.YT?.navigationHistory?.initialized)).toBe(true);

  await page.evaluate(() => { window.__navigationPageSentinel = "still-here"; });
  const originalUrl = page.url();

  await page.locator('.tab[data-tab="health"]').click();
  const healthViews = await page.locator(
    "#panel-health .settings-subnav-btn[data-settings-view]",
  ).evaluateAll((nodes) => nodes.map((node) => node.dataset.settingsView));
  expect(healthViews).toEqual(["overview", "library", "backups"]);
  await page.locator(
    `#panel-health [data-settings-view="${healthViews[1]}"]`,
  ).click();
  await page.locator(
    `#panel-health [data-settings-view="${healthViews[2]}"]`,
  ).click();

  await page.locator('.tab[data-tab="settings"]').click();
  await expect(page.locator("#settings-view-preferences")).toBeVisible();

  await mouseHistory(page, 3);
  await expect.poll(() => activeTopTab(page)).toBe("health");
  await expect.poll(() => activeSubview(page, "health"))
    .toBe(healthViews[2]);

  await mouseHistory(page, 3);
  await expect.poll(() => activeSubview(page, "health"))
    .toBe(healthViews[1]);
  await expect.poll(() => activeTopTab(page)).toBe("health");

  await mouseHistory(page, 3);
  await expect.poll(() => activeSubview(page, "health"))
    .toBe(healthViews[0]);

  await mouseHistory(page, 4);
  await expect.poll(() => activeSubview(page, "health"))
    .toBe(healthViews[1]);
  await mouseHistory(page, 4);
  await expect.poll(() => activeSubview(page, "health"))
    .toBe(healthViews[2]);
  await mouseHistory(page, 4);
  await expect.poll(() => activeTopTab(page)).toBe("settings");
  await expect(page.locator("#settings-view-preferences")).toBeVisible();

  expect(page.url()).toBe(originalUrl);
  expect(await page.evaluate(() => window.__navigationPageSentinel))
    .toBe("still-here");
  await expect(page.locator("#settings-mini-log")).toBeVisible();
});

test("mouse history crosses top tabs and Browse sections", async ({ page }) => {
  await loadApp(page);
  await expect.poll(() => page.evaluate(() =>
    window.YT?.navigationHistory?.initialized)).toBe(true);

  await page.locator('.tab[data-tab="settings"]').click();
  await page.locator('.tab[data-tab="browse"]').click();
  await page.locator('#panel-browse [data-submode="recent"]').click();
  await page.locator('#panel-browse [data-submode="search"]').click();

  await mouseHistory(page, 3);
  await expect(page.locator(
    '#panel-browse [data-submode="recent"]',
  )).toHaveClass(/active/);
  await mouseHistory(page, 3);
  await expect(page.locator(
    '#panel-browse [data-submode="channels"]',
  )).toHaveClass(/active/);
  await mouseHistory(page, 3);
  await expect.poll(() => activeTopTab(page)).toBe("settings");
  await expect(page.locator("#settings-view-preferences")).toBeVisible();

  await mouseHistory(page, 4);
  await expect.poll(() => activeTopTab(page)).toBe("browse");
  await expect(page.locator(
    '#panel-browse [data-submode="channels"]',
  )).toHaveClass(/active/);
  await mouseHistory(page, 4);
  await expect(page.locator(
    '#panel-browse [data-submode="recent"]',
  )).toHaveClass(/active/);
  await mouseHistory(page, 4);
  await expect(page.locator(
    '#panel-browse [data-submode="search"]',
  )).toHaveClass(/active/);
  await expect(page.locator("#browse-mini-log")).toBeVisible();
});

test("Browse channel and Watch pages participate in shared history", async ({ page }) => {
  await loadApp(page);
  await page.locator('.tab[data-tab="browse"]').click();

  await page.evaluate(() => {
    const channel = { folder: "Fixture Channel", name: "Fixture Channel" };
    window._browseState.currentChannel = channel;
    window.showView("videos");
  });
  await expect(page.locator("#view-videos")).toBeVisible();

  await page.evaluate(() => {
    window._openVideoInWatch({
      video_id: "fixture-video-id",
      title: "Fixture Video",
      channel: "Fixture Channel",
      filepath: "C:\\FixtureArchive\\Fixture Video.mp4",
    });
  });
  await expect(page.locator("#view-watch")).toBeVisible();

  await mouseHistory(page, 3);
  await expect(page.locator("#view-videos")).toBeVisible();
  await mouseHistory(page, 3);
  await expect(page.locator("#view-channels")).toBeVisible();

  await mouseHistory(page, 4);
  await expect(page.locator("#view-videos")).toBeVisible();
  await mouseHistory(page, 4);
  await expect(page.locator("#view-watch")).toBeVisible();
  await expect(page.locator("#watch-title")).toContainText("Fixture Video");
});

test("refresh clicks and repeated records do not create duplicate entries", async ({ page }) => {
  await loadApp(page);
  await page.locator('.tab[data-tab="health"]').click();

  const before = await page.evaluate(() =>
    window.YT.navigationHistory.snapshot());
  await page.locator('.tab[data-tab="health"]').click();
  await page.evaluate(() => {
    window.YT.navigationHistory.record();
    window.YT.navigationHistory.record();
  });
  const after = await page.evaluate(() =>
    window.YT.navigationHistory.snapshot());
  expect(after.length).toBe(before.length);
  expect(after.position).toBe(before.position);

  await mouseHistory(page, 3);
  await expect.poll(() => activeTopTab(page)).toBe("download");
  await expect(page.locator("#main-log")).toBeVisible();
});
