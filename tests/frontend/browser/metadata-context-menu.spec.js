const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

const CHANNEL_NAME = "Metadata Fixture";

async function renderChannel(page, metadataPending = 4, transcriptionPending = 0) {
  await page.evaluate(({ name, metadataCount, transcriptionCount }) => {
    window.renderChannelGrid([{
      name,
      folder: name,
      metadata_pending: metadataCount,
      transcription_pending: transcriptionCount,
    }], () => {});
  }, {
    name: CHANNEL_NAME,
    metadataCount: metadataPending,
    transcriptionCount: transcriptionPending,
  });
}

async function openChannelMenu(page) {
  await page.locator(
    `#channel-grid [data-channel-name="${CHANNEL_NAME}"]`,
  ).evaluate((card) => {
    card.dispatchEvent(new MouseEvent("contextmenu", {
      bubbles: true,
      cancelable: true,
      clientX: 180,
      clientY: 180,
    }));
  });
  const menu = page.locator("#ctx-menu-root > .ctx-menu");
  await expect(menu).toBeVisible();
  return menu;
}

function directItem(menu, label) {
  const escaped = label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return menu.locator(":scope > .ctx-menu-item").filter({
    hasText: new RegExp(`^${escaped}`),
  }).first();
}

async function directLabels(menu) {
  return menu.evaluate((element) => [...element.children]
    .filter((child) => child.classList.contains("ctx-menu-item"))
    .map((child) => [...child.childNodes]
      .find((node) => node.nodeType === Node.TEXT_NODE)?.textContent.trim())
    .filter(Boolean));
}

async function openMetadataFlyout(page, rootMenu) {
  const trigger = directItem(rootMenu, "Metadata");
  await trigger.hover();
  const menu = trigger.locator(":scope > .ctx-submenu");
  await expect(menu).toBeVisible();
  return { trigger, menu };
}

test("channel maintenance groups every metadata action in one submenu", async ({ page }) => {
  await loadApp(page);
  await renderChannel(page);
  const rootMenu = await openChannelMenu(page);

  const rootLabels = await directLabels(rootMenu);
  expect(rootLabels).toContain("Metadata");
  expect(rootLabels).not.toContain("Recheck metadata (4 pending)");
  expect(rootLabels).not.toContain("Refetch missing thumbnails");
  expect(rootLabels).not.toContain("Refresh views/likes…");
  expect(rootLabels).not.toContain("Refresh comments…");

  const { trigger, menu } = await openMetadataFlyout(page, rootMenu);
  await expect(trigger).toHaveAttribute("aria-haspopup", "menu");
  await expect(trigger).toHaveAttribute("aria-expanded", "true");
  expect(await directLabels(menu)).toEqual([
    "Fix missing information (4 pending)",
    "Repair missing thumbnails",
    "Refresh views & likes",
    "Refresh comments",
  ]);

  const statistics = directItem(menu, "Refresh views & likes");
  await statistics.hover();
  const scopes = statistics.locator(":scope > .ctx-submenu");
  await expect(scopes).toBeVisible();
  expect(await directLabels(scopes)).toEqual([
    "Last 7 days",
    "Last 30 days",
    "Last 90 days",
    "All videos (slow)",
  ]);
});

test("missing information is a direct missing-only action with no old prompt", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__metadataChoiceCalls = 0;
    window.askMetadataAlreadyDownloaded = () => {
      window.__metadataChoiceCalls += 1;
      return Promise.resolve("refresh");
    };
  });
  await renderChannel(page, 6);
  const rootMenu = await openChannelMenu(page);
  const { menu } = await openMetadataFlyout(page, rootMenu);
  await directItem(menu, "Fix missing information (6 pending)").click();

  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("metadata_fill_missing_channel").length,
  )).toBe(1);
  expect(await page.evaluate(() =>
    window.__bridgeCallsFor("metadata_fill_missing_channel")[0].args,
  )).toEqual([{ name: CHANNEL_NAME }]);
  expect(await page.evaluate(() => ({
    oldRecheck: window.__bridgeCallsFor("metadata_recheck_channel").length,
    statistics: window.__bridgeCallsFor("metadata_refresh_views_channel").length,
    prompts: window.__metadataChoiceCalls,
  }))).toEqual({ oldRecheck: 0, statistics: 0, prompts: 0 });
});

test("nested metadata actions pass the selected channel and time scope", async ({ page }) => {
  await loadApp(page);
  await renderChannel(page);

  let rootMenu = await openChannelMenu(page);
  let metadata = await openMetadataFlyout(page, rootMenu);
  await directItem(metadata.menu, "Repair missing thumbnails").click();
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("refetch_thumbnails").length,
  )).toBe(1);
  expect(await page.evaluate(() =>
    window.__bridgeCallsFor("refetch_thumbnails")[0].args,
  )).toEqual([{ name: CHANNEL_NAME }]);

  rootMenu = await openChannelMenu(page);
  metadata = await openMetadataFlyout(page, rootMenu);
  const statistics = directItem(metadata.menu, "Refresh views & likes");
  await statistics.hover();
  await directItem(
    statistics.locator(":scope > .ctx-submenu"), "Last 30 days",
  ).click();
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("metadata_refresh_views_channel").length,
  )).toBe(1);
  expect(await page.evaluate(() =>
    window.__bridgeCallsFor("metadata_refresh_views_channel")[0].args,
  )).toEqual([{ name: CHANNEL_NAME }, 30]);

  rootMenu = await openChannelMenu(page);
  metadata = await openMetadataFlyout(page, rootMenu);
  const comments = directItem(metadata.menu, "Refresh comments");
  await comments.hover();
  await directItem(
    comments.locator(":scope > .ctx-submenu"), "All videos (slow)",
  ).click();
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("metadata_refresh_comments_channel").length,
  )).toBe(1);
  expect(await page.evaluate(() =>
    window.__bridgeCallsFor("metadata_refresh_comments_channel")[0].args,
  )).toEqual([{ name: CHANNEL_NAME }, null]);
});

test("keyboard navigation enters and backs out of both metadata flyout levels", async ({ page }) => {
  await loadApp(page);
  await renderChannel(page);
  const rootMenu = await openChannelMenu(page);
  const metadata = directItem(rootMenu, "Metadata");
  await metadata.focus();

  await page.keyboard.press("ArrowRight");
  const metadataMenu = metadata.locator(":scope > .ctx-submenu");
  const fixMissing = directItem(metadataMenu, "Fix missing information");
  await expect(fixMissing).toBeFocused();
  await expect(metadata).toHaveAttribute("aria-expanded", "true");

  await page.keyboard.press("ArrowDown");
  await expect(directItem(metadataMenu, "Repair missing thumbnails"))
    .toBeFocused();
  await page.keyboard.press("ArrowDown");
  const statistics = directItem(metadataMenu, "Refresh views & likes");
  await expect(statistics).toBeFocused();

  await page.keyboard.press("ArrowRight");
  const statisticsMenu = statistics.locator(":scope > .ctx-submenu");
  await expect(directItem(statisticsMenu, "Last 7 days")).toBeFocused();
  await expect(statistics).toHaveAttribute("aria-expanded", "true");

  await page.keyboard.press("ArrowLeft");
  await expect(statistics).toBeFocused();
  await expect(statistics).toHaveAttribute("aria-expanded", "false");
  await page.keyboard.press("ArrowLeft");
  await expect(metadata).toBeFocused();
  await expect(metadata).toHaveAttribute("aria-expanded", "false");
});

test("nested metadata flyouts show only the hovered leaf tooltip", async ({ page }) => {
  await loadApp(page);
  await renderChannel(page);
  const rootMenu = await openChannelMenu(page);
  const { trigger: metadata, menu } = await openMetadataFlyout(page, rootMenu);

  // A submenu trigger remains hovered while the pointer is inside its flyout.
  // Keeping a native title on that ancestor produces the doubled tooltips seen
  // in the app, so submenu triggers expose their explanation without `title`.
  await expect(metadata).not.toHaveAttribute("title");
  await expect(metadata).toHaveAttribute(
    "aria-description",
    "Repair missing information or update saved YouTube details",
  );

  const views = directItem(menu, "Refresh views & likes");
  await expect(views).not.toHaveAttribute("title");
  await expect(views).toHaveAttribute(
    "aria-description",
    "Refresh views, likes, comment totals, and YouTube availability",
  );

  const leaf = directItem(menu, "Fix missing information (4 pending)");
  await leaf.hover();
  await expect(leaf).not.toHaveAttribute("title");
  await expect(leaf).toHaveAttribute(
    "data-tooltip",
    "Fetch information and thumbnails only where they are missing",
  );
  await expect(page.locator(".custom-tooltip")).toHaveCount(1);
  await expect(page.locator(".custom-tooltip")).toHaveText(
    "Fetch information and thumbnails only where they are missing",
  );
});

for (const { pending, dim } of [
  { pending: 0, dim: true },
  { pending: 7, dim: false },
]) {
  test(`transcribe-missing row shows a ${dim ? "dim zero" : "bright pending"} count`, async ({ page }) => {
    await loadApp(page);
    await renderChannel(page, 0, pending);
    const rootMenu = await openChannelMenu(page);
    const row = directItem(rootMenu, "Transcribe all missing");
    const count = row.locator(":scope > .ctx-menu-count");

    await expect(row).toBeVisible();
    await expect(count).toHaveText(String(pending));
    if (dim) await expect(count).toHaveClass(/\bdim\b/);
    else await expect(count).not.toHaveClass(/\bdim\b/);

    // Keep the value visually separate from the action label and aligned at
    // the far side of the menu row instead of embedding it in the sentence.
    const positions = await Promise.all([
      row.boundingBox(),
      count.boundingBox(),
    ]);
    expect(positions[0]).not.toBeNull();
    expect(positions[1]).not.toBeNull();
    expect(positions[1].x).toBeGreaterThan(
      positions[0].x + (positions[0].width / 2),
    );
    expect(
      positions[0].x + positions[0].width
        - (positions[1].x + positions[1].width),
    ).toBeLessThanOrEqual(32);
  });
}
