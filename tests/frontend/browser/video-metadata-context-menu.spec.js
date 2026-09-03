const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

const CHANNEL = "Video metadata fixture";
const TRACKED_METADATA_LABELS = [
  "Refresh views & likes",
  "Refresh comments",
  "Refresh thumbnail",
  "Refresh all",
];

async function appendVideoCard(page, gridId, video, options = {}) {
  const fixtureId = options.fixtureId || `${gridId}-metadata`;
  await page.evaluate(({ gridId, video, fixtureId, options }) => {
    const grid = document.getElementById(gridId);
    if (!grid) throw new Error(`Missing fixture grid: ${gridId}`);

    const card = window._buildVideoCard(video, () => {});
    card.dataset.videoMetadataFixture = fixtureId;
    if (typeof options.tracked === "boolean") {
      card.dataset.tracked = options.tracked ? "1" : "0";
    }
    grid.appendChild(card);
  }, { gridId, video, fixtureId, options });
  return page.locator(`[data-video-metadata-fixture="${fixtureId}"]`);
}

async function openCardMenu(page, card) {
  await card.evaluate((element) => {
    element.dispatchEvent(new MouseEvent("contextmenu", {
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

async function openMetadataFlyout(rootMenu) {
  const trigger = directItem(rootMenu, "Metadata");
  await trigger.hover();
  const menu = trigger.locator(":scope > .ctx-submenu");
  await expect(menu).toBeVisible();
  return { trigger, menu };
}

test("a legacy no-ID card can refresh its missing thumbnail from Metadata", async ({ page }) => {
  await loadApp(page);
  const filepath = "C:\\FixtureArchive\\Video metadata fixture\\Legacy local video.mp4";
  const card = await appendVideoCard(page, "video-grid", {
    video_id: "",
    title: "Legacy local video",
    channel: CHANNEL,
    filepath,
    thumbnail_url: "",
  }, {
    fixtureId: "legacy-no-id",
  });

  // A NULL catalog video_id reaches the shared builder as an empty value.
  // The card remains actionable because its local filepath is usable.
  await expect(card).toHaveAttribute("data-video-id", "");
  await expect(card.locator(".video-thumb-img")).toHaveCount(0);

  const rootMenu = await openCardMenu(page, card);
  const rootLabels = await directLabels(rootMenu);
  expect(rootLabels).toContain("Metadata");
  expect(rootLabels).not.toContain("Refresh metadata");
  const { trigger, menu } = await openMetadataFlyout(rootMenu);
  await expect(trigger).toHaveAttribute("aria-haspopup", "menu");
  await expect(trigger).toHaveAttribute("aria-expanded", "true");
  expect(await directLabels(menu)).toEqual(TRACKED_METADATA_LABELS);

  await directItem(menu, "Refresh thumbnail").click();
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("browse_repair_video_thumbnail").length,
  )).toBe(1);
  expect(await page.evaluate(() =>
    window.__bridgeCallsFor("browse_repair_video_thumbnail")[0].args,
  )).toEqual([{
    filepath,
    video_id: "",
    title: "Legacy local video",
    channel: CHANNEL,
    force: true,
  }]);
  expect(await page.evaluate(() =>
    window.__bridgeCallsFor("browse_refresh_video_metadata").length,
  )).toBe(0);
});

test("tracked Metadata actions send exact mode and force payloads", async ({ page }) => {
  await loadApp(page);
  const payload = {
    filepath: "C:\\FixtureArchive\\Video metadata fixture\\Known video [abcdefghijk].mp4",
    video_id: "abcdefghijk",
    title: "Known video",
    channel: CHANNEL,
  };
  const card = await appendVideoCard(page, "video-grid", payload, {
    fixtureId: "known-id",
  });

  const actions = [
    ["Refresh views & likes", "browse_refresh_video_metadata"],
    ["Refresh comments", "browse_refresh_video_metadata"],
    ["Refresh thumbnail", "browse_repair_video_thumbnail"],
    ["Refresh all", "browse_refresh_video_metadata"],
  ];
  const expectedCalls = {
    browse_refresh_video_metadata: 0,
    browse_repair_video_thumbnail: 0,
  };

  for (const [label, method] of actions) {
    const rootMenu = await openCardMenu(page, card);
    const { menu } = await openMetadataFlyout(rootMenu);
    expect(await directLabels(menu)).toEqual(TRACKED_METADATA_LABELS);
    await directItem(menu, label).click();
    expectedCalls[method] += 1;
    await expect.poll(() => page.evaluate((bridgeMethod) =>
      window.__bridgeCallsFor(bridgeMethod).length, method,
    )).toBe(expectedCalls[method]);
  }

  expect(await page.evaluate(() => ({
    metadata: window.__bridgeCallsFor("browse_refresh_video_metadata")
      .map((call) => call.args),
    thumbnail: window.__bridgeCallsFor("browse_repair_video_thumbnail")
      .map((call) => call.args),
  }))).toEqual({
    metadata: [
      [{ ...payload, mode: "stats" }],
      [{ ...payload, mode: "comments" }],
      [{ ...payload, mode: "all" }],
    ],
    thumbnail: [[{ ...payload, force: true }]],
  });
});

test("Metadata requires a usable filepath, not a YouTube video ID", async ({ page }) => {
  await loadApp(page);
  const noPath = await appendVideoCard(page, "video-grid", {
    video_id: "abcdefghijk",
    title: "Catalog row without a local file",
    channel: CHANNEL,
    filepath: "",
  }, {
    fixtureId: "no-filepath",
  });

  await expect(noPath).toHaveAttribute("data-filepath", "");
  const rootMenu = await openCardMenu(page, noPath);
  const rootLabels = await directLabels(rootMenu);
  expect(rootLabels).not.toContain("Metadata");
  expect(rootLabels).not.toContain("Refresh metadata");
  await expect(page.getByRole("menuitem", {
    name: "Refresh thumbnail",
    exact: true,
  })).toHaveCount(0);
});

test("a tracked Videos card gets the same Metadata refresh menu", async ({ page }) => {
  await loadApp(page);
  const filepath = "C:\\FixtureArchive\\Tracked legacy video.mp4";
  const card = await appendVideoCard(page, "recent-grid", {
    video_id: "",
    title: "Tracked legacy video",
    channel: CHANNEL,
    filepath,
  }, {
    fixtureId: "tracked-recent-no-id",
    tracked: true,
  });

  const rootMenu = await openCardMenu(page, card);
  const { menu } = await openMetadataFlyout(rootMenu);
  expect(await directLabels(menu)).toEqual(TRACKED_METADATA_LABELS);
  await directItem(menu, "Refresh thumbnail").click();
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("browse_repair_video_thumbnail").length,
  )).toBe(1);
  expect(await page.evaluate(() =>
    window.__bridgeCallsFor("browse_repair_video_thumbnail")[0].args,
  )).toEqual([{
    filepath,
    video_id: "",
    title: "Tracked legacy video",
    channel: CHANNEL,
    force: true,
  }]);
});

test("untracked Recent and Manual cards keep their local metadata routing", async ({ page }) => {
  await loadApp(page);
  const cases = [
    {
      gridId: "recent-grid", tracked: false,
      fixtureId: "untracked-recent", videoId: "recent00001",
    },
    {
      gridId: "manual-grid", tracked: false,
      fixtureId: "manual", videoId: "manual00001",
    },
  ];
  const expectedPayloads = [];

  for (const [index, fixture] of cases.entries()) {
    const payload = {
      video_id: fixture.videoId,
      title: `${fixture.fixtureId} video`,
      channel: CHANNEL,
      filepath: `C:\\FixtureArchive\\${fixture.fixtureId}.mp4`,
    };
    expectedPayloads.push(payload);
    const card = await appendVideoCard(page, fixture.gridId, payload, fixture);
    const rootMenu = await openCardMenu(page, card);

    // These grids have different sidecar semantics from a tracked channel.
    // Preserve their existing flat action and manual bridge instead of
    // accidentally routing them through the tracked-channel endpoint.
    const labels = await directLabels(rootMenu);
    expect(labels).toContain("Refresh metadata");
    expect(labels).not.toContain("Metadata");
    expect(labels).not.toContain("Redownload…");
    await page.getByRole("menuitem", {
      name: "Refresh metadata",
      exact: true,
    }).click();
    await expect.poll(() => page.evaluate(() =>
      window.__bridgeCallsFor("manual_refresh_metadata").length,
    )).toBe(index + 1);
  }

  expect(await page.evaluate(() => ({
    manual: window.__bridgeCallsFor("manual_refresh_metadata")
      .map((call) => call.args),
    tracked: window.__bridgeCallsFor("browse_refresh_video_metadata").length,
    repairs: window.__bridgeCallsFor("browse_repair_video_thumbnail").length,
  }))).toEqual({
    manual: expectedPayloads.map((payload) => [payload]),
    tracked: 0,
    repairs: 0,
  });
});

test("thumbnail refresh preserves card actions and fallback until the image loads", async ({ page }) => {
  const thumbnailUrl = "http://thumbnail.fixture/repaired.gif";
  let releaseThumbnail;
  await page.route((url) => url.href.startsWith(thumbnailUrl), async (route) => {
    await new Promise((resolve) => { releaseThumbnail = resolve; });
    await route.fulfill({
      status: 200,
      contentType: "image/gif",
      body: Buffer.from(
        "R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw==",
        "base64",
      ),
    });
  });
  await loadApp(page);
  await page.evaluate((url) => {
    window.__setBridgeHandler("browse_repair_video_thumbnail", () =>
      Promise.resolve({
        ok: true,
        source: "local",
        thumbnail_url: url,
      }));
  }, thumbnailUrl);

  const card = await appendVideoCard(page, "video-grid", {
    video_id: "",
    title: "Delayed repaired thumbnail",
    channel: CHANNEL,
    filepath: "C:\\FixtureArchive\\delayed-thumbnail.mp4",
    thumbnail_url: "",
  }, {
    fixtureId: "delayed-thumbnail",
  });
  const placeholder = card.locator(
    ".video-thumb > span:not(.video-duration-badge):not(.video-removed-badge)",
  );
  const kebab = card.locator(".card-kebab");
  await card.evaluate((element) => element.dispatchEvent(new MouseEvent(
    "mouseover", { bubbles: true },
  )));
  await expect(kebab).toHaveCount(1);
  await expect(placeholder).toHaveCount(1);

  const rootMenu = await openCardMenu(page, card);
  const { menu } = await openMetadataFlyout(rootMenu);
  await directItem(menu, "Refresh thumbnail").click();

  const replacement = card.locator(".video-thumb-img");
  await expect.poll(() => typeof releaseThumbnail).toBe("function");
  await expect(replacement).toHaveCount(1);
  await expect(replacement).toHaveCSS("visibility", "hidden");
  await expect(placeholder).toHaveCount(1);
  await expect(kebab).toHaveCount(1);

  releaseThumbnail();
  await expect(replacement).toHaveCSS("visibility", "visible");
  await expect(placeholder).toHaveCount(0);
  await expect(kebab).toHaveCount(1);
});

test("refreshing a populated same-URL thumbnail cache-busts without dropping its token", async ({ page }) => {
  const thumbnailBase = "http://thumbnail.fixture/populated.gif";
  const thumbnailUrl = `${thumbnailBase}?t=keep-this-token`;
  const imageBody = Buffer.from(
    "R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw==",
    "base64",
  );
  const requestedUrls = [];
  let releaseRefresh;
  await page.route((url) => url.href.startsWith(thumbnailBase), async (route) => {
    requestedUrls.push(route.request().url());
    if (requestedUrls.length === 2) {
      await new Promise((resolve) => { releaseRefresh = resolve; });
    }
    await route.fulfill({
      status: 200,
      contentType: "image/gif",
      body: imageBody,
    });
  });
  await loadApp(page);
  await page.evaluate((url) => {
    window.__setBridgeHandler("browse_repair_video_thumbnail", () =>
      Promise.resolve({
        ok: true,
        source: "youtube",
        thumbnail_url: url,
      }));
  }, thumbnailUrl);

  const payload = {
    video_id: "cachebust01",
    title: "Populated thumbnail",
    channel: CHANNEL,
    filepath: "C:\\FixtureArchive\\populated-thumbnail.mp4",
  };
  const card = await appendVideoCard(page, "video-grid", {
    ...payload,
    thumbnail_url: thumbnailUrl,
    eager_thumbnail: true,
  }, {
    fixtureId: "populated-thumbnail",
  });
  await expect.poll(() => card.locator(".video-thumb-img").evaluate((image) =>
    image.complete && image.naturalWidth > 0,
  )).toBe(true);
  await expect(card.locator(".video-thumb-img")).toHaveCount(1);

  const rootMenu = await openCardMenu(page, card);
  const { menu } = await openMetadataFlyout(rootMenu);
  await directItem(menu, "Refresh thumbnail").click();

  await expect.poll(() => typeof releaseRefresh).toBe("function");
  expect(requestedUrls).toHaveLength(2);
  const refreshedUrl = new URL(requestedUrls[1]);
  expect(refreshedUrl.searchParams.get("t")).toBe("keep-this-token");
  expect(refreshedUrl.searchParams.get("refresh")).toMatch(/^\d+$/);
  expect(refreshedUrl.href).not.toBe(thumbnailUrl);

  const images = card.locator(".video-thumb-img");
  await expect(images).toHaveCount(2);
  await expect(images.nth(0)).toHaveCSS("visibility", "hidden");
  await expect(images.nth(1)).toHaveAttribute("src", thumbnailUrl);
  await expect(images.nth(1)).toHaveCSS("visibility", "visible");
  expect(await page.evaluate(() =>
    window.__bridgeCallsFor("browse_repair_video_thumbnail")[0].args,
  )).toEqual([{ ...payload, force: true }]);

  releaseRefresh();
  await expect(images).toHaveCount(1);
  await expect(images).toHaveAttribute("src", refreshedUrl.href);
  await expect(images).toHaveCSS("visibility", "visible");
});

test("failed thumbnail refresh restores the existing card fallback", async ({ page }) => {
  const thumbnailUrl = "http://thumbnail.fixture/broken.gif";
  let requestSeen = false;
  await page.route((url) => url.href.startsWith(thumbnailUrl), async (route) => {
    requestSeen = true;
    await route.abort("failed");
  });
  await loadApp(page);
  await page.evaluate((url) => {
    window.__setBridgeHandler("browse_repair_video_thumbnail", () =>
      Promise.resolve({
        ok: true,
        source: "local",
        thumbnail_url: url,
      }));
  }, thumbnailUrl);

  const card = await appendVideoCard(page, "video-grid", {
    video_id: "",
    title: "Failed repaired thumbnail",
    channel: CHANNEL,
    filepath: "C:\\FixtureArchive\\failed-thumbnail.mp4",
    thumbnail_url: "",
  }, {
    fixtureId: "failed-thumbnail",
  });
  const wrap = card.locator(".video-thumb");
  const placeholder = card.locator(
    ".video-thumb > span:not(.video-duration-badge):not(.video-removed-badge)",
  );
  await card.evaluate((element) => element.dispatchEvent(new MouseEvent(
    "mouseover", { bubbles: true },
  )));
  const backgroundBefore = await wrap.evaluate((element) => element.style.background);

  const rootMenu = await openCardMenu(page, card);
  const { menu } = await openMetadataFlyout(rootMenu);
  await directItem(menu, "Refresh thumbnail").click();

  await expect.poll(() => requestSeen).toBe(true);
  await expect(card.locator(".video-thumb-img")).toHaveCount(0);
  await expect(placeholder).toHaveCount(1);
  await expect(card.locator(".card-kebab")).toHaveCount(1);
  expect(await wrap.evaluate((element) => element.style.background))
    .toBe(backgroundBefore);
});

test("tracked Watch metadata refresh requests all fields and surfaces a partial warning", async ({ page }) => {
  const warning = "Video information was refreshed, but the thumbnail could not be refreshed.";
  await loadApp(page);
  await page.evaluate(({ message, channel }) => {
    window.__setBridgeHandler("browse_refresh_video_metadata", () =>
      Promise.resolve({ ok: true, warning: message }));
    window.renderWatchView({
      video_id: "watchabc123",
      title: "Tracked Watch video",
      channel,
      filepath: "C:\\FixtureArchive\\Tracked Watch video [watchabc123].mp4",
      tracked: true,
    }, [], null, { skipVideoReload: true });
    document.getElementById("btn-watch-refresh-meta").click();
  }, { message: warning, channel: CHANNEL });

  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("browse_refresh_video_metadata").length,
  )).toBe(1);
  expect(await page.evaluate(() =>
    window.__bridgeCallsFor("browse_refresh_video_metadata")[0].args,
  )).toEqual([{
    filepath: "C:\\FixtureArchive\\Tracked Watch video [watchabc123].mp4",
    video_id: "watchabc123",
    title: "Tracked Watch video",
    channel: CHANNEL,
    mode: "all",
  }]);

  const toast = page.locator("#toast-root .toast").last();
  await expect(toast).toHaveClass(/\bwarn\b/);
  await expect(toast).toContainText(warning);
  await expect(toast).not.toHaveClass(/\bok\b/);
});

test("untracked Watch metadata refresh keeps the manual endpoint and payload", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("manual_refresh_metadata", () =>
      Promise.resolve({ ok: true }));
    window.renderWatchView({
      video_id: "manualab123",
      title: "Manual Watch video",
      channel: "Manual imports",
      filepath: "D:\\Loose videos\\Manual Watch video [manualab123].mp4",
      tracked: false,
    }, [], null, { skipVideoReload: true });
    document.getElementById("btn-watch-refresh-meta").click();
  });

  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("manual_refresh_metadata").length,
  )).toBe(1);
  expect(await page.evaluate(() =>
    window.__bridgeCallsFor("manual_refresh_metadata")[0].args,
  )).toEqual([{
    filepath: "D:\\Loose videos\\Manual Watch video [manualab123].mp4",
    video_id: "manualab123",
    title: "Manual Watch video",
    channel: "Manual imports",
  }]);
  expect(await page.evaluate(() =>
    window.__bridgeCallsFor("browse_refresh_video_metadata").length,
  )).toBe(0);
  await expect(page.locator("#toast-root .toast.ok").last())
    .toContainText("Metadata refreshed.");
});
