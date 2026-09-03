const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

async function showDenseSubs(page) {
  // Startup settings may apply the saved dense-tab preference just after the
  // core boot marker. Let that one-time load settle before overriding it for
  // this focused fixture.
  await page.waitForTimeout(250);
  await page.evaluate(() => {
    window._applyLegacySubsMode(true);
    document.querySelector('.tab[data-tab="subs"]')?.click();
  });
  await expect(page.locator("#panel-subs")).toBeVisible();
}


test("Queue Pending keeps force-all available and reports async admission", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window._applyLegacySubsMode(true);
    window.__setBridgeHandler("subs_queue_all", async () => ({
      ok: true,
      started: true,
    }));
  });
  await showDenseSubs(page);

  const button = page.locator("#btn-queue-pending");
  await expect(button).not.toHaveAttribute("hidden", "");
  await button.click({ button: "right" });
  const dialog = page.getByRole("dialog", { name: "Queue all channels" });
  await expect(dialog).toBeVisible();
  await dialog.getByRole("button", { name: "Queue all" }).click();

  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("subs_queue_all").length)).toBe(1);
  await expect(page.locator("#toast-root .toast").last()).toContainText(
    "Checking all channels for transcription work");
});


test("a failed editor load closes instead of exposing fake channel settings", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("subs_get_channel", async () => ({
      ok: false,
      error: "Channel no longer exists.",
    }));
    window._editChannelFromBrowse("Missing Fixture");
  });

  await expect(page.locator("#channel-editor-backdrop")).toBeHidden();
  await expect(page.locator("#toast-root .toast").last()).toContainText(
    "Channel no longer exists");
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("subs_update_channel").length)).toBe(0);
});


test("a blocked save keeps the user's editor and values open", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("subs_get_channel", async () => ({
      ok: true,
      channel: {
        name: "Blocked Fixture",
        folder: "Blocked Fixture",
        url: "https://www.youtube.com/@blocked_fixture",
        resolution: "720",
        mode: "new",
      },
    }));
    window.__setBridgeHandler("subs_update_channel", async () => ({
      ok: false,
      write_blocked: true,
      error: "The channel changes could not be saved.",
    }));
    window._editChannelFromBrowse("Blocked Fixture");
  });

  const folder = page.locator("#edit-folder");
  await expect(folder).toHaveValue("Blocked Fixture");
  await folder.fill("Blocked Fixture Renamed");
  await page.locator("#btn-edit-update").click();

  await expect(page.locator("#channel-editor-backdrop")).toBeVisible();
  await expect(folder).toHaveValue("Blocked Fixture Renamed");
  await expect(page.locator("#toast-root .toast").last()).toContainText(
    "could not be saved");
});


test("invalid duration and From-date values never reach the backend", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("subs_get_channel", async () => ({
      ok: true,
      channel: {
        name: "Validation Fixture",
        folder: "Validation Fixture",
        url: "https://www.youtube.com/@validation_fixture",
        resolution: "720",
        min_duration: 3,
        max_duration: 20,
        mode: "new",
      },
    }));
    window._editChannelFromBrowse("Validation Fixture");
  });

  await page.locator("#edit-min-dur").fill("30");
  await page.locator("#edit-max-dur").fill("10");
  await page.locator("#btn-edit-update").click();
  await expect(page.locator("#toast-root .toast").last()).toContainText(
    "cannot be greater");
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("subs_update_channel").length)).toBe(0);

  await page.locator("#edit-min-dur").fill("3");
  await page.evaluate(() => {
    const radio = document.querySelector(
      'input[name="edit-range"][value="fromdate"]');
    radio.checked = true;
    radio.dispatchEvent(new Event("change", { bubbles: true }));
  });
  await page.locator("#edit-date-year").fill("2026");
  await page.locator("#edit-date-month").fill("02");
  await page.locator("#edit-date-day").fill("30");
  await page.locator("#btn-edit-update").click();
  await expect(page.locator("#toast-root .toast").last()).toContainText(
    "not a valid calendar date");
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("subs_update_channel").length)).toBe(0);
});


test("compression settings participate in dirty tracking and obsolete Batch is removed", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("subs_get_channel", async () => ({
      ok: true,
      channel: {
        name: "Compression Fixture",
        folder: "Compression Fixture",
        url: "https://www.youtube.com/@compression_fixture",
        resolution: "720",
        mode: "new",
        compress_enabled: true,
        compress_level: "Generous",
        compress_output_res: "720",
      },
    }));
    window._editChannelFromBrowse("Compression Fixture");
  });

  await expect(page.locator("#edit-compress-batch")).toHaveCount(0);
  await expect(page.locator("#btn-edit-update")).toBeDisabled();
  await page.locator("#edit-compress-quality").selectOption("Average");
  await expect(page.locator("#btn-edit-update")).toBeEnabled();
});


test("late channel responses cannot replace the newer editor", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__channelResolvers = {};
    window.__setBridgeHandler("subs_get_channel", (identity) =>
      new Promise((resolve) => {
        window.__channelResolvers[identity.name] = resolve;
      }));
    window._editChannelFromBrowse("First Fixture");
    window._editChannelFromBrowse("Second Fixture");
  });
  await page.evaluate(() => {
    window.__channelResolvers["Second Fixture"]({
      ok: true,
      channel: {
        name: "Second Fixture",
        folder: "Second Fixture",
        url: "https://www.youtube.com/@second_fixture",
        resolution: "720",
        mode: "new",
      },
    });
  });
  await expect(page.locator("#edit-folder")).toHaveValue("Second Fixture");
  await page.evaluate(() => {
    window.__channelResolvers["First Fixture"]({
      ok: true,
      channel: {
        name: "First Fixture",
        folder: "First Fixture",
        url: "https://www.youtube.com/@first_fixture",
        resolution: "1080",
        mode: "full",
      },
    });
  });
  await expect(page.locator("#edit-folder")).toHaveValue("Second Fixture");
  await expect(page.locator("#edit-resolution")).toHaveValue("720");
});


test("subscription sorting is keyboard-operable, persistent, and keeps placeholders last", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window._applyLegacySubsMode(true);
    const rows = [
      { folder: "Unknown", n_vids: "—" },
      { folder: "Ten", n_vids: "10" },
      { folder: "Two", n_vids: "2" },
    ].map((row) => ({
      res: "720", min: "—", max: "—", compress: "—",
      transcribe: "—", metadata: "—", last_sync: "Never",
      size: "—", avg_size: "—", ...row,
    }));
    window.renderSubsTable(rows, "3 channels");
  });
  await showDenseSubs(page);

  const header = page.locator('th[data-sort="n_vids"]');
  await header.focus();
  await page.keyboard.press("Enter");
  await expect(header).toHaveAttribute("aria-sort", "ascending");
  await expect(page.locator("#subs-table-body tr .col-folder")).toHaveText([
    "Two", "Ten", "Unknown",
  ]);
  await header.press("Enter");
  await expect(header).toHaveAttribute("aria-sort", "descending");
  await expect(page.locator("#subs-table-body tr .col-folder")).toHaveText([
    "Ten", "Two", "Unknown",
  ]);

  await page.evaluate(() => {
    document.querySelector("#subs-table-body tr")?.classList.add("row-selected");
    const bar = document.getElementById("subs-bulk-bar");
    const count = document.getElementById("subs-bulk-count");
    if (bar) bar.hidden = false;
    if (count) count.textContent = "3 channels selected";
    window.renderSubsTable([...window._subsAllRows].reverse(), "3 channels");
  });
  await expect(page.locator("#subs-bulk-bar")).toBeHidden();
  await expect(page.locator("#subs-bulk-count")).toHaveText(
    "0 channels selected");
  await expect(page.locator("#subs-table-body tr .col-folder")).toHaveText([
    "Ten", "Two", "Unknown",
  ]);
});


test("subscription form labels and total-size rescan work from the keyboard", async ({ page }) => {
  await loadApp(page);
  await showDenseSubs(page);
  await expect(page.getByLabel("Channel URL")).toHaveAttribute("id", "edit-url");
  await expect(page.getByLabel("Folder Name")).toHaveAttribute("id", "edit-folder");
  await expect(page.getByLabel("Minimum video length in minutes"))
    .toHaveAttribute("id", "edit-min-dur");
  await expect(page.getByLabel("From date year"))
    .toHaveAttribute("id", "edit-date-year");

  await page.evaluate(() => {
    window.__setBridgeHandler("archive_rescan", async () => ({
      ok: true,
      started: true,
    }));
  });
  const rescan = page.getByRole("button", {
    name: "Rescan all channel folder sizes",
  });
  await rescan.focus();
  await page.keyboard.press("Enter");
  const dialog = page.getByRole("dialog", { name: "Refresh sizes" });
  await expect(dialog).toBeVisible();
  await dialog.getByRole("button", { name: "Rescan" }).click();
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("archive_rescan").length)).toBe(1);
});


test("Undo calls the exact removal represented by its toast", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("subs_remove_channel", async () => ({
      ok: true,
      subscription_removed: true,
      can_undo: true,
      undo_id: "removal-fixture-1",
    }));
    window.__setBridgeHandler("subs_undo_remove", async () => ({
      ok: true,
    }));
    window._browseState.submode = "channels";
    window._browseState.view = "videos";
    window._browseState.currentChannel = {
      name: "Undo Fixture",
      folder: "Undo Fixture",
    };
    window._browseState.videos = [{ title: "Stale video" }];
    window._removeChannelWithPrompt("Undo Fixture");
  });
  await page.getByRole("dialog", { name: "Remove channel" })
    .getByRole("button", { name: "Remove" }).click();
  await page.getByRole("dialog", {
    name: "What should happen to downloaded files?",
  }).getByRole("button", { name: "Keep files" }).click();
  await page.locator("#toast-root .toast").last()
    .getByRole("button", { name: "Undo" }).click();

  const args = await page.evaluate(() =>
    window.__bridgeCallsFor("subs_undo_remove").at(-1).args);
  expect(args).toEqual(["removal-fixture-1"]);
  const browseState = await page.evaluate(() => ({
    view: window._browseState.view,
    currentChannel: window._browseState.currentChannel,
    videos: window._browseState.videos,
  }));
  expect(browseState).toEqual({
    view: "channels",
    currentChannel: null,
    videos: [],
  });
});


test("a list-refresh failure does not turn a committed removal into a failure", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("subs_remove_channel", async () => ({
      ok: true,
      subscription_removed: true,
      can_undo: true,
      undo_id: "refresh-failure-removal",
    }));
    window.refreshSubsTable = async () => {
      throw new Error("fixture refresh unavailable");
    };
    window.__refreshFailureRemoval =
      window._removeChannelWithPrompt("Refresh Failure Fixture");
  });
  await page.getByRole("dialog", { name: "Remove channel" })
    .getByRole("button", { name: "Remove" }).click();
  await page.getByRole("dialog", {
    name: "What should happen to downloaded files?",
  }).getByRole("button", { name: "Keep files" }).click();

  const result = await page.evaluate(() => window.__refreshFailureRemoval);
  expect(result).toMatchObject({ ok: true, subscription_removed: true });
  const toast = page.locator("#toast-root .toast").last();
  await expect(toast).toContainText("Reopen this tab to refresh the list");
  await expect(toast.getByRole("button", { name: "Undo" })).toBeVisible();
});
