const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

async function openContextMenu(page, selector) {
  await page.locator(selector).evaluate((element) => {
    element.dispatchEvent(new MouseEvent("contextmenu", {
      bubbles: true,
      clientX: 180,
      clientY: 180,
    }));
  });
  await expect(page.getByRole("menu")).toBeVisible();
}

test("channel-page More keeps the current channel after a sorted render", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(async () => {
    const alpha = {
      name: "Shared display name", folder: "Alpha Folder", metadata_pending: 3,
    };
    const beta = {
      name: "Shared display name", folder: "Beta Folder", metadata_pending: 7,
    };
    window.__editedChannel = "";
    window._editChannelFromBrowse = (name) => { window.__editedChannel = name; };
    window.__setBridgeHandler("browse_list_videos_page", async () => ({
      rows: [], has_more: false, next_offset: 0,
    }));
    document.querySelector('.tab[data-tab="browse"]')?.click();
    window._browseState.channels = [alpha, beta];
    window.renderChannelGrid([beta, alpha], () => {});
    window._browseState.currentChannel = alpha;
    window.showView("videos");
    await window.loadVideosFor(alpha);
  });

  const more = page.locator("#cph-more");
  await more.click();
  await expect(more).toHaveAttribute("aria-expanded", "true");
  await expect(page.locator(
    "#ctx-menu-root > .ctx-menu > .ctx-menu-item",
  ).filter({ hasText: /^Metadata/ }).first()).toBeVisible();
  await page.getByRole("menuitem", { name: "Edit settings", exact: true }).click();
  await expect(more).toHaveAttribute("aria-expanded", "false");
  await expect.poll(() => page.evaluate(() => window.__editedChannel))
    .toBe("Alpha Folder");
});

test("pending metadata uses the direct missing-only operation", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.renderChannelGrid([{
      name: "Metadata Fixture",
      folder: "Metadata Fixture",
      metadata_pending: 4,
    }], () => {});
  });
  await openContextMenu(page, '#channel-grid [data-channel-name="Metadata Fixture"]');
  const metadata = page.locator(
    "#ctx-menu-root > .ctx-menu > .ctx-menu-item",
  ).filter({ hasText: /^Metadata/ }).first();
  await metadata.hover();
  await metadata.locator(":scope > .ctx-submenu > .ctx-menu-item")
    .filter({ hasText: /^Fix missing information \(4 pending\)/ })
    .click();

  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("metadata_fill_missing_channel").length)).toBe(1);
  expect(await page.evaluate(() =>
    window.__bridgeCallsFor("metadata_recheck_channel").length)).toBe(0);
  expect(await page.evaluate(() =>
    window.__bridgeCallsFor("metadata_refresh_views_channel").length)).toBe(0);
});

test("channel transcription keeps the full channel identity", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(async () => {
    window.__setBridgeHandler("chan_transcribe_all", async () => ({
      ok: true,
      queued: 2,
    }));
    await window._askTranscribeChannel({
      name: "Duplicate display name",
      folder: "Unique Folder",
      url: "https://www.youtube.com/@unique-fixture",
    }, false);
  });

  const calls = await page.evaluate(() =>
    window.__bridgeCallsFor("chan_transcribe_all"));
  expect(calls).toHaveLength(1);
  expect(calls[0].args).toEqual([{
    name: "Duplicate display name",
    folder: "Unique Folder",
    url: "https://www.youtube.com/@unique-fixture",
  }, false]);
});

test("untracked videos use local metadata refresh and hide redownload", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    const card = window._buildVideoCard({
      video_id: "untracked001",
      title: "Former subscription video",
      channel: "Former Channel",
      filepath: "C:\\FixtureArchive\\former.mp4",
      tracked: false,
    }, () => {});
    card.dataset.tracked = "0";
    card.dataset.auditUntracked = "1";
    document.getElementById("recent-grid").appendChild(card);
  });
  await openContextMenu(page, '[data-audit-untracked="1"]');
  await expect(page.getByRole("menuitem", { name: "Redownload…", exact: true }))
    .toHaveCount(0);
  await page.getByRole("menuitem", { name: "Refresh metadata", exact: true }).click();
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("manual_refresh_metadata").length)).toBe(1);
  expect(await page.evaluate(() =>
    window.__bridgeCallsFor("browse_refresh_video_metadata").length)).toBe(0);
});

test("resolved rescan refusal is reported immediately", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("archive_rescan", async () => ({
      ok: false,
      started: false,
      error: "Another library task is running.",
    }));
    const button = document.getElementById("search-rescan-btn");
    button.hidden = false;
    button.click();
  });
  await expect(page.locator("#toast-root .toast.error").last())
    .toHaveText("Another library task is running.");
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("archive_rescan").length)).toBe(1);
});

for (const [label, bucket] of [
  ["2024-03", "month"],
  ["2025-W01", "week"],
]) {
  test(`graph drill keeps the exact ${bucket} range`, async ({ page }) => {
    await loadApp(page);
    await page.evaluate(({ label, bucket }) => {
      window.__setBridgeHandler("browse_search", async () => []);
      window.YT.graph.drillIntoSearch("fixture", label, bucket, null);
    }, { label, bucket });
    await expect.poll(() => page.evaluate(() =>
      window.__bridgeCallsFor("browse_search").length)).toBe(1);
    const args = await page.evaluate(() =>
      window.__bridgeCallsFor("browse_search")[0].args);
    expect(args[6]).toEqual(expect.any(Number));
    expect(args[7]).toEqual(expect.any(Number));
    expect(args[7]).toBeGreaterThan(args[6]);
    const spanDays = (args[7] - args[6]) / 86400;
    if (bucket === "week") expect(spanDays).toBe(7);
    else expect(spanDays).toBeGreaterThanOrEqual(28);
  });
}

test("failed graph query clears the old word cloud and export snapshot", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    document.querySelector('.tab[data-tab="browse"]')?.click();
    document.querySelector('.submode-btn[data-submode="graph"]')?.click();
    window.__setBridgeHandler("browse_word_cloud", async () => ({
      ok: true,
      words: [{ word: "fixture", count: 10 }],
    }));
  });
  await page.locator('.chart-type-btn[data-type="wordcloud"]').click();
  await expect(page.locator("#graph-wordcloud")).toBeVisible();

  await page.evaluate(() => {
    document.getElementById("graph-word").value = "fixture";
    window.__setBridgeHandler("browse_graph", async () => ({
      error: "Graph query failed.",
    }));
  });
  await page.locator('.chart-type-btn[data-type="line"]').click();
  await expect(page.locator("#graph-empty")).toHaveText("Graph query failed.");
  await expect(page.locator("#graph-wordcloud")).toBeHidden();
  await page.locator("#btn-graph-export-csv").click();
  await expect(page.locator("#toast-root .toast.warn").last())
    .toHaveText("Plot something first.");
});

test("double-click playback reports a resolved backend failure", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    document.querySelector('.tab[data-tab="browse"]')?.click();
    document.querySelector('.submode-btn[data-submode="recent"]')?.click();
    window.__setBridgeHandler("browse_open_video", async () => ({
      ok: false,
      error: "The fixture file is unavailable.",
    }));
    const card = window._buildVideoCard({
      video_id: "openfail001",
      title: "Open failure",
      filepath: "C:\\FixtureArchive\\missing.mp4",
    }, () => {});
    card.dataset.auditOpenFailure = "1";
    document.getElementById("recent-grid").appendChild(card);
    card.dispatchEvent(new MouseEvent("dblclick", { bubbles: true }));
  });
  await expect(page.locator("#toast-root .toast.error").last())
    .toHaveText("The fixture file is unavailable.");
});

test("Videos renders a catalog failure instead of an empty archive", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("list_all_videos", async () => ({
      rows: [], has_more: false, error: "Fixture catalog is unavailable.",
    }));
  });
  await page.locator('.tab[data-tab="browse"]').click();
  await page.locator('.submode-btn[data-submode="recent"]').click();
  await expect(page.locator("#recent-grid"))
    .toContainText("Fixture catalog is unavailable.");
  await expect(page.locator("#recent-grid")).not.toContainText(
    "No videos in the archive yet.");
});

test("focused cards open their actions with Shift+F10", async ({ page }) => {
  await loadApp(page);
  await page.locator('.tab[data-tab="browse"]').click();
  await page.evaluate(() => {
    window.renderChannelGrid([{
      name: "Keyboard Fixture",
      folder: "Keyboard Fixture Folder",
    }], () => {});
  });
  const card = page.locator('[data-channel-folder="Keyboard Fixture Folder"]');
  await card.focus();
  await page.keyboard.press("Shift+F10");
  await expect(page.getByRole("menu")).toBeVisible();
  await expect(card).toHaveAttribute("aria-expanded", "true");
  await page.keyboard.press("Escape");
  await expect(page.getByRole("menu")).toHaveCount(0);
  await expect(card).toHaveAttribute("aria-expanded", "false");
});

test("transcript context actions are keyboard reachable and hide invalid bookmarks", async ({ page }) => {
  await loadApp(page);
  await page.locator('.tab[data-tab="browse"]').click();
  await page.evaluate(() => {
    window.showView("watch");
    window._browseState.currentVideo = {
      title: "Local file without an ID",
      filepath: "C:\\FixtureArchive\\local.mp4",
      video_id: "",
    };
    const transcript = document.getElementById("watch-transcript");
    transcript.innerHTML = `
      <div class="transcript-para">
        <button class="para-ts" data-s="12" aria-haspopup="menu"
                aria-expanded="false">0:12</button>
        <span class="seg" data-s="12"><span class="word" data-s="12">Fixture</span></span>
      </div>`;
  });
  const timestamp = page.locator("#watch-transcript .para-ts");
  await timestamp.focus();
  await page.keyboard.press("Shift+F10");
  await expect(page.getByRole("menuitem", { name: "Copy segment" })).toBeVisible();
  await expect(page.getByRole("menuitem", { name: "Bookmark this moment…" }))
    .toHaveCount(0);
});

test("Search channel popup reports its real open state", async ({ page }) => {
  await loadApp(page);
  await page.locator('.tab[data-tab="browse"]').click();
  await page.locator('.submode-btn[data-submode="search"]').click();
  const trigger = page.locator("#search-channel-trigger");
  await expect(trigger).toHaveAttribute("aria-haspopup", "dialog");
  await expect(trigger).toHaveAttribute("aria-expanded", "false");
  await trigger.click();
  await expect(page.getByRole("dialog", { name: "Choose search channels" }))
    .toBeVisible();
  await expect(trigger).toHaveAttribute("aria-expanded", "true");
  await page.keyboard.press("Escape");
  await expect(trigger).toHaveAttribute("aria-expanded", "false");
});

test("Watch transcript splitter supports keyboard resizing", async ({ page }) => {
  await loadApp(page);
  await page.locator('.tab[data-tab="browse"]').click();
  await page.evaluate(() => window.showView("watch"));
  const splitter = page.locator("#watch-splitter");
  await page.evaluate(() => {
    document.documentElement.style.setProperty("--watch-tx-width", "420px");
    document.getElementById("watch-splitter")
      .setAttribute("aria-valuenow", "420");
  });
  await splitter.focus();
  await page.keyboard.press("ArrowLeft");
  await expect(splitter).toHaveAttribute("aria-valuenow", "440");
  await page.keyboard.press("ArrowRight");
  await expect(splitter).toHaveAttribute("aria-valuenow", "420");
});

test("Graph exposes chart type state and a keyboard drill control", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("browse_graph", async () => ({
      labels: ["2024-03", "2024-04"],
      values: [4, 6],
    }));
    window.__setBridgeHandler("browse_search", async () => []);
  });
  await page.locator('.tab[data-tab="browse"]').click();
  await page.locator('.submode-btn[data-submode="graph"]').click();
  await page.locator("#graph-word").fill("fixture");
  await page.locator("#btn-graph-run").click();
  await expect(page.locator("#graph-drill-controls")).toBeVisible();
  await expect(page.locator("#graph-drill-select option")).toHaveCount(2);
  await expect(page.locator('.chart-type-btn[data-type="line"]'))
    .toHaveAttribute("aria-pressed", "true");
  await page.locator("#graph-drill-select").selectOption("0");
  await page.locator("#btn-graph-drill").click();
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("browse_search").length)).toBe(1);
  const args = await page.evaluate(() =>
    window.__bridgeCallsFor("browse_search")[0].args);
  expect(args[6]).toEqual(expect.any(Number));
  expect(args[7]).toBeGreaterThan(args[6]);
});
