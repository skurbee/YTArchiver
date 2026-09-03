const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

test("Manual bulk actions show feedback while their summary is being counted", async ({ page }) => {
  await loadApp(page);
  await page.locator('.tab[data-tab="browse"]').click();
  await page.locator('[data-submode="manual"]').click();

  await page.evaluate(() => {
    window.__setBridgeHandler("manual_bulk_action_summary", () =>
      new Promise((resolve) => { window.__resolveManualSummary = resolve; }));
  });

  const button = page.locator("#manual-recover-ids");
  await button.click();
  await expect(button).toHaveText("Checking…");
  await expect(button).toBeDisabled();
  await expect(button).toHaveAttribute("aria-busy", "true");

  await page.evaluate(() => window.__resolveManualSummary({
    ok: true,
    total: 2,
    with_id: 1,
    percent_with_id: 50,
    recover_eligible: 1,
    recover_excluded: 0,
    recover_tried: 0,
  }));

  const dialog = page.getByRole("dialog", { name: "Recover manual video IDs" });
  await expect(dialog).toBeVisible();
  await expect(button).toHaveText("Recover IDs");
  await expect(button).toBeEnabled();
  await expect(button).not.toHaveAttribute("aria-busy", "true");
  await dialog.getByRole("button", { name: "Cancel" }).click();
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("manual_backfill_ids").length)).toBe(0);
});

test("command palette can find every Browse destination", async ({ page }) => {
  await loadApp(page);

  const expected = [
    ["bookmark", "Bookmarks — saved moments"],
    ["graph", "Graph — transcript trends"],
    ["manual", "Manual downloads"],
  ];
  for (const [query, label] of expected) {
    await page.evaluate(() => window.openCommandPalette());
    await page.locator("#cmdp-input").fill(query);
    await expect(page.locator(".cmdp-item .cmdp-label")).toHaveText([label]);
    await page.keyboard.press("Escape");
  }

  await page.evaluate(() => window.openCommandPalette());
  await page.locator("#cmdp-input").fill("bookmark");
  await page.keyboard.press("Enter");
  await expect(page.locator("#view-bookmarks")).toBeVisible();
});

test("Settings dropdowns support arrows, Enter, Escape, and ARIA state", async ({ page }) => {
  await loadApp(page);
  await page.locator('.tab[data-tab="settings"]').click();

  const trigger = page.locator(
    '.yt-dd:has(+ #settings-log-mode) .yt-dd-trigger');
  const menu = page.locator(
    '.yt-dd:has(+ #settings-log-mode) .yt-dd-menu');
  await trigger.focus();
  await page.keyboard.press("Enter");
  await expect(trigger).toHaveAttribute("aria-expanded", "true");
  await expect(menu).toBeVisible();

  const before = await trigger.getAttribute("aria-activedescendant");
  await page.keyboard.press("ArrowDown");
  const after = await trigger.getAttribute("aria-activedescendant");
  expect(after).not.toBe(before);
  await expect(page.locator(`#${after}`)).toHaveClass(/\bactive\b/);

  await page.keyboard.press("Enter");
  await expect(trigger).toHaveAttribute("aria-expanded", "false");
  await expect(menu).toBeHidden();
  await expect.poll(() => page.locator("#settings-log-mode").inputValue())
    .toBe(await page.locator(`#${after}`).getAttribute("data-idx") === "0"
      ? "Simple"
      : "Verbose");
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("settings_save")
      .filter((call) => Object.hasOwn(call.args[0] || {}, "log_mode")).length))
    .toBe(1);
  expect(await page.evaluate(() =>
    window.__bridgeCallsFor("set_log_mode").length)).toBe(0);

  await page.keyboard.press("Enter");
  await page.keyboard.press("Escape");
  await expect(trigger).toHaveAttribute("aria-expanded", "false");
  await expect(menu).toBeHidden();
});

test("Add Channel hides archive-only actions and shows Folder organization", async ({ page }) => {
  await page.setViewportSize({ width: 980, height: 720 });
  await loadApp(page);
  await page.locator('.tab[data-tab="browse"]').click();
  await page.evaluate(() => window._openAddChannelEditor());

  await expect(page.locator("#channel-editor-backdrop")).toBeVisible();
  await expect(page.locator("#edit-res-recheck")).toBeHidden();
  const width = await page.locator("#edit-folder-org")
    .evaluate((element) => element.getBoundingClientRect().width);
  expect(width).toBeGreaterThanOrEqual(125);
});

test("Add Channel explains invalid and incomplete input before enabling Add", async ({ page }) => {
  await loadApp(page);
  await page.locator('.tab[data-tab="browse"]').click();
  await page.evaluate(() => window._openAddChannelEditor());

  const url = page.locator("#edit-url");
  const folder = page.locator("#edit-folder");
  const validation = page.locator("#edit-channel-validation");
  const add = page.locator("#btn-edit-update");

  await expect(validation).toBeVisible();
  await expect(validation).toContainText("YouTube channel URL or @handle");
  await expect(add).toBeDisabled();

  await url.fill("not a channel!");
  await expect(validation).toHaveClass(/\bis-error\b/);
  await expect(validation).toHaveText("Enter a YouTube channel URL or @handle.");
  await expect(url).toHaveAttribute("aria-invalid", "true");
  await expect(add).toBeDisabled();

  // The backend accepts a bare @handle. The editor should mirror that grammar,
  // derive the folder, and clear the explanatory error.
  await url.fill("@valid_handle");
  await expect(folder).toHaveValue("valid_handle");
  await expect(validation).toBeHidden();
  await expect(url).not.toHaveAttribute("aria-invalid", "true");
  await expect(add).toBeEnabled();

  // A manually supplied folder must not make a spoofed host valid.
  await url.fill("https://youtube.com.evil.example/@bad");
  await expect(validation).toHaveText("Use a youtube.com channel link, not another site.");
  await expect(add).toBeDisabled();

  // Channel-ID URLs are structurally valid but cannot yield a friendly folder
  // name locally, so the still-required field gets a precise explanation.
  await url.fill("https://www.youtube.com/channel/UC1234567890");
  await folder.fill("");
  await expect(url).not.toHaveAttribute("aria-invalid", "true");
  await expect(folder).toHaveAttribute("aria-invalid", "true");
  await expect(validation).toContainText("Enter a folder name");
  await expect(add).toBeDisabled();

  await folder.fill("Bad:Folder");
  await expect(validation).toContainText("Folder name can’t contain");
  await expect(add).toBeDisabled();
  await folder.fill("Channel ID Fixture");
  await expect(validation).toBeHidden();
  await expect(add).toBeEnabled();
});

test("resolution reset is accessible and marks an existing channel dirty", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("subs_get_channel", async () => ({
      ok: true,
      channel: {
        name: "Resolution Fixture",
        folder: "Resolution Fixture",
        url: "https://www.youtube.com/@resolution_fixture",
        resolution: "720",
        mode: "full",
      },
    }));
    window.__setBridgeHandler("subs_update_channel", async (_identity, payload) => ({
      ok: true,
      channel: { name: payload.folder, folder: payload.folder },
    }));
    window.__setBridgeHandler("subs_get_defaults", async () => ({
      resolution: "1080",
    }));
    window._editChannelFromBrowse("Resolution Fixture");
  });

  await expect(page.locator("#edit-resolution")).toHaveValue("720");
  await expect(page.locator("#btn-edit-update")).toBeDisabled();
  const reset = page.getByRole("button", {
    name: "Reset channel resolution to your configured default",
  });
  await expect(reset).toHaveAttribute("type", "button");
  await reset.focus();
  await page.keyboard.press("Enter");

  await expect(page.locator("#edit-resolution")).toHaveValue("1080");
  await expect(page.locator("#btn-edit-update")).toBeEnabled();
  await page.locator("#btn-edit-update").click();
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("subs_update_channel").length)).toBe(1);
  const payload = await page.evaluate(() =>
    window.__bridgeCallsFor("subs_update_channel")[0].args[1]);
  expect(payload.resolution).toBe("1080");
});

test("Remove Channel explains when downloaded files are considered", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__removeResult = "pending";
    window._removeChannelWithPrompt("Fixture Channel")
      .then((result) => { window.__removeResult = result; });
  });

  const dialog = page.getByRole("dialog", { name: "Remove channel" });
  await expect(dialog).toContainText("This stops future syncs");
  await expect(dialog).toContainText(
    "will stay where they are unless you choose to move them on the next screen");
  await dialog.getByRole("button", { name: "Cancel" }).click();
  await expect.poll(() => page.evaluate(() => window.__removeResult)).toBe(null);
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("subs_remove_channel").length)).toBe(0);
});

test("Remove Channel truthfully offers archive files or YTArchiver Trash", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__removeResult = "pending";
    window._removeChannelWithPrompt("Fixture Channel")
      .then((result) => { window.__removeResult = result; });
  });

  const first = page.getByRole("dialog", { name: "Remove channel" });
  await first.getByRole("button", { name: "Remove" }).click();
  const second = page.getByRole("dialog", {
    name: "What should happen to downloaded files?",
  });
  const moveFiles = second.getByRole("button", { name: "Move files to Trash" });
  const cancelRemoval = second.getByRole("button", { name: "Cancel removal" });
  await expect(second).toContainText("move the channel folder to YTArchiver Trash");
  await expect(moveFiles).toHaveClass(/\bbtn-danger\b/);
  await expect(cancelRemoval).toHaveClass(/\bbtn-ghost\b/);
  await expect(cancelRemoval).not.toHaveClass(/\bbtn-danger\b/);
  await cancelRemoval.click();

  await expect.poll(() => page.evaluate(() => window.__removeResult)).toBe(null);
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("subs_remove_channel").length)).toBe(0);
});

test("Browse channel menu keeps the shared removal flow available during another sync", async ({ page }) => {
  await loadApp(page);
  await page.locator('.tab[data-tab="browse"]').click();
  await page.evaluate(() => {
    window._anySyncRunning = () => true;
    window.renderChannelGrid([{
      folder: "Context Fixture",
      name: "Context Fixture",
      n_vids: 2,
    }], () => {});
  });

  await page.locator(
    '#channel-grid .channel-card[data-channel-name="Context Fixture"]')
    .click({ button: "right" });
  const remove = page.getByRole("menuitem", {
    name: "Remove channel…",
    exact: true,
  });
  await expect(remove).toBeVisible();
  await expect(remove).toHaveClass(/\bdanger\b/);
  await expect(remove).not.toHaveAttribute("aria-disabled", "true");
  await remove.click();
  await expect(page.getByRole("dialog", { name: "Remove channel" }))
    .toBeVisible();
  await page.getByRole("button", { name: "Cancel" }).click();
});

test("channel removal shows progress, blocks a retry, and refreshes Browse", async ({ page }) => {
  await loadApp(page);
  await page.locator('.tab[data-tab="browse"]').click();
  await page.evaluate(() => {
    window._anySyncRunning = () => false;
    window.__setBridgeHandler("subs_remove_channel", () =>
      new Promise((resolve) => { window.__resolveChannelRemoval = resolve; }));
    window.__setBridgeHandler("get_subs_channels", () => [[], []]);
    window.renderChannelGrid([{
      folder: "Slow Remove Fixture",
      name: "Slow Remove Fixture",
      n_vids: 3,
    }], () => {});
  });

  const card = page.locator(
    '#channel-grid .channel-card[data-channel-name="Slow Remove Fixture"]');
  await card.click({ button: "right" });
  await page.getByRole("menuitem", { name: "Remove channel…" }).click();
  await page.getByRole("dialog", { name: "Remove channel" })
    .getByRole("button", { name: "Remove" }).click();
  await page.getByRole("dialog", {
    name: "What should happen to downloaded files?",
  }).getByRole("button", { name: "Move files to Trash" }).click();

  await expect(page.locator("#toast-root .toast", {
    hasText: "Removing \"Slow Remove Fixture\"",
  })).toBeVisible();
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("subs_remove_channel").length)).toBe(1);

  await page.evaluate(async () => {
    window.__duplicateRemoveResult = await window._removeChannelWithPrompt(
      "Slow Remove Fixture");
  });
  await expect(page.locator("#toast-root")).toContainText(
    "A channel removal is already in progress");
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("subs_remove_channel").length)).toBe(1);

  const args = await page.evaluate(() =>
    window.__bridgeCallsFor("subs_remove_channel")[0].args);
  expect(args).toEqual([{ name: "Slow Remove Fixture" }, true]);

  await page.evaluate(() => window.__resolveChannelRemoval({
    ok: true,
    subscription_removed: true,
    files_removed: true,
    deleted_folder: true,
    catalog_cleanup_ok: true,
  }));
  await expect(card).toHaveCount(0);
  await expect(page.locator("#toast-root")).toContainText(
    "downloaded files moved to YTArchiver Trash");
  await expect(page.locator("#toast-root .toast", {
    hasText: "Removing \"Slow Remove Fixture\"",
  })).toHaveCount(0);
});

test("a committed removal with an index warning still refreshes the UI", async ({ page }) => {
  await loadApp(page);
  await page.locator('.tab[data-tab="browse"]').click();
  await page.evaluate(() => {
    window.__setBridgeHandler("subs_remove_channel", () => ({
      ok: false,
      subscription_removed: true,
      files_removed: true,
      deleted_folder: true,
      catalog_cleanup_ok: false,
      catalog_warning: "fixture index cleanup failed.",
    }));
    window.__setBridgeHandler("get_subs_channels", () => [[], []]);
    window.renderChannelGrid([{
      folder: "Partial Fixture",
      name: "Partial Fixture",
      n_vids: 1,
    }], () => {});
    window.__partialRemove = window._removeChannelWithPrompt("Partial Fixture");
  });

  await page.getByRole("dialog", { name: "Remove channel" })
    .getByRole("button", { name: "Remove" }).click();
  await page.getByRole("dialog", {
    name: "What should happen to downloaded files?",
  }).getByRole("button", { name: "Move files to Trash" }).click();

  await expect(page.locator(
    '#channel-grid .channel-card[data-channel-name="Partial Fixture"]'))
    .toHaveCount(0);
  await expect(page.locator("#toast-root")).toContainText(
    "library index could not be fully cleaned up");
  await expect(page.locator("#toast-root")).toContainText(
    "fixture index cleanup failed");
});

test("a moved folder does not masquerade as a removed subscription when config save fails", async ({ page }) => {
  await loadApp(page);
  await page.locator('.tab[data-tab="browse"]').click();
  await page.evaluate(() => {
    window.__setBridgeHandler("subs_remove_channel", () => ({
      ok: false,
      subscription_removed: false,
      files_removed: true,
      deleted_folder: true,
      error: "Fixture config save failed.",
    }));
    window.renderChannelGrid([{
      folder: "Recovery Fixture",
      name: "Recovery Fixture",
      n_vids: 1,
    }], () => {});
    window.__recoveryRemove = window._removeChannelWithPrompt(
      "Recovery Fixture");
  });

  await page.getByRole("dialog", { name: "Remove channel" })
    .getByRole("button", { name: "Remove" }).click();
  await page.getByRole("dialog", {
    name: "What should happen to downloaded files?",
  }).getByRole("button", { name: "Move files to Trash" }).click();

  await expect(page.locator(
    '#channel-grid .channel-card[data-channel-name="Recovery Fixture"]'))
    .toHaveCount(1);
  await expect(page.locator("#toast-root")).toContainText(
    "Fixture config save failed");
  await expect(page.locator("#toast-root")).toContainText(
    "restart YTArchiver so recovery can finish");
  expect(await page.evaluate(() =>
    window.__bridgeCallsFor("get_subs_channels").length)).toBe(1);
});

test("Processing queue gives manual transcription a visible label", async ({ page }) => {
  await page.setViewportSize({ width: 980, height: 720 });
  await loadApp(page);

  await page.evaluate(() => window.toggleQueuePopover(
    "gpu", document.getElementById("gsb-gpu")));

  const button = page.getByRole("button", { name: "Manual transcription…" });
  await expect(button).toBeVisible();
  await expect(button).toHaveAttribute("title", "Open manual transcription");
  await button.click();
  const dialog = page.getByRole("dialog", { name: "Manual Transcribe" });
  await expect(dialog).toBeVisible();
  await expect(dialog.locator("#manual-tx-model")).toHaveCount(0);
  await expect(dialog.getByRole("button", { name: "Browse…" })).toBeFocused();
  await page.getByRole("button", { name: "Cancel" }).click();
  await expect(page.locator("#btn-manual-transcribe")).toBeFocused();
});

test("Metadata uses its tracked-video denominator when the disk cache is missing", async ({ page }) => {
  await loadApp(page);

  await page.evaluate(() => {
    localStorage.removeItem("ytarchiver_meta_rows");
    window.__setBridgeHandler("get_channel_metadata_status", () => [{
      name: "Catalog-only fixture",
      folder: "Catalog-only fixture",
      url: "https://youtube.example/@catalogfixture",
      // Reproduces a missing disk-summary cache record: the old UI showed
      // zero here even though every coverage metric knew about 480 videos.
      video_count: 0,
      id_total: 480,
      id_with_id: 475,
      id_missing: 5,
      tx_total: 480,
      tx_transcribed: 460,
    }]);
    window.__setBridgeHandler("thumbnail_status_bulk", () => ({ rows: {} }));
  });

  await page.locator('.tab[data-tab="health"]').click();
  await page.locator('[data-settings-view="library"]').click();
  await page.locator("#health-library-metadata").evaluate((details) => {
    details.open = true;
  });
  await page.evaluate(() => window._refreshMetadataTab({ force: true }));

  await expect(page.locator("#metadata-tbody .md-col-num")).toHaveText("480");
  await expect(page.locator("#md-tot-videos")).toHaveText("480");
  await expect(page.locator("#metadata-totals .md-total-label"))
    .toContainText(["Channels", "Videos", "Video IDs", "Thumbnails", "Still on YT", "Transcribed"]);
  await expect(page.locator('#metadata-table th[data-sort="videos"]'))
    .toHaveText("Videos");
});

test("legacy possessive corruption is repaired only for display", async ({ page }) => {
  await loadApp(page);
  const rawChannel = "Creator\uFFFDs Workshop";
  const rawTitle = "Maker\uFFFDs Build";

  const helperResults = await page.evaluate(() => [
    window.YT.util.displayText("Creator\uFFFDs Workshop"),
    window.YT.util.displayText("\uFFFD\uFFFD\uFFFDQuoted\uFFFD\uFFFD\uFFFD"),
    window.YT.util.displayText("Unknown \uFFFD marker"),
  ]);
  expect(helperResults).toEqual([
    "Creator’s Workshop",
    "\uFFFD\uFFFD\uFFFDQuoted\uFFFD\uFFFD\uFFFD",
    "Unknown \uFFFD marker",
  ]);

  await page.evaluate(({ rawChannel, rawTitle }) => {
    window.renderChannelGrid([{
      folder: rawChannel,
      name: rawChannel,
      n_vids: 1,
    }], (channel) => { window.__clickedRawChannel = channel.folder; });
    window.renderVideoGrid([{
      title: rawTitle,
      channel: rawChannel,
      filepath: "C:\\FixtureArchive\\video.mp4",
      show_channel: true,
    }], (video) => { window.__clickedRawTitle = video.title; });
  }, { rawChannel, rawTitle });

  const channelCard = page.locator("#channel-grid .channel-card");
  await expect(channelCard.locator(".channel-card-name"))
    .toHaveText("Creator’s Workshop");
  await expect(channelCard).toHaveAttribute("data-channel-name", rawChannel);
  await channelCard.evaluate((element) => element.click());
  await expect.poll(() => page.evaluate(() => window.__clickedRawChannel))
    .toBe(rawChannel);

  const videoCard = page.locator("#video-grid .video-card");
  await expect(videoCard.locator(".video-card-title"))
    .toHaveText("Maker’s Build");
  await expect(videoCard.locator(".video-card-channel"))
    .toHaveText("Creator’s Workshop");
  await expect(videoCard).toHaveAttribute("data-title", rawTitle);
  await expect(videoCard).toHaveAttribute("data-channel", rawChannel);
  await videoCard.evaluate((element) => element.click());
  await expect.poll(() => page.evaluate(() => window.__clickedRawTitle))
    .toBe(rawTitle);
});

test("Search displays repaired labels but resolves with the raw identity", async ({ page }) => {
  await loadApp(page);
  const rawChannel = "Creator\uFFFDs Workshop";
  const rawTitle = "Maker\uFFFDs Build";
  await page.evaluate(({ rawChannel, rawTitle }) => {
    window.__setBridgeHandler("browse_search", () => [{
      segment_id: 17,
      video_id: "fixture-video",
      title: rawTitle,
      channel: rawChannel,
      start_time: 10,
      text: `${rawTitle} transcript`,
      snippet: `${rawTitle} transcript`,
      jsonl_path: "C:/FixtureArchive/.Transcript.jsonl",
    }]);
    window.__setBridgeHandler("browse_search_titles", () => []);
    window.__setBridgeHandler("browse_resolve_segment", () => ({
      ok: false,
      error: "Fixture stops before playback",
    }));
  }, { rawChannel, rawTitle });

  await page.locator('.tab[data-tab="browse"]').click();
  await page.locator('[data-submode="search"]').click();
  await page.locator("#search-query").fill("fixture");
  await page.locator("#btn-search-run").click();

  const result = page.locator("#search-results .search-result");
  await expect(result.locator(".search-result-title")).toHaveText("Maker’s Build");
  await expect(result.locator(".search-result-meta"))
    .toContainText("Creator’s Workshop");
  await expect(result.locator(".search-result-snippet"))
    .toContainText("Maker’s Build transcript");

  await result.dblclick();
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("browse_resolve_segment").at(-1)?.args))
    .toEqual([
      "C:/FixtureArchive/.Transcript.jsonl",
      "fixture-video",
      rawTitle,
    ]);
});
