const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

async function openAdd(page, url = "") {
  await page.evaluate(url => {
    window.__setBridgeHandler("subs_get_defaults", async () => ({ resolution: "720" }));
    window._openAddChannelEditor(url);
  }, url);
  await expect(page.locator("#channel-editor-backdrop")).toBeVisible();
  await expect(page.locator("#edit-url")).toBeEnabled();
}

test("canonical channel links clearly require a folder and validation does not move controls", async ({ page }) => {
  await loadApp(page);
  await openAdd(page);
  await expect(page.locator("#edit-folder")).toHaveAttribute("placeholder", "Auto-fills from @handle links");
  await page.locator("#edit-url").fill("https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa");
  await expect(page.locator("#edit-folder")).toHaveValue("");
  await expect(page.locator("#btn-edit-update")).toBeDisabled();
  const before = await page.locator("#edit-folder").boundingBox();
  await page.locator("#edit-folder").fill("Example Archive");
  await expect(page.locator("#btn-edit-update")).toBeEnabled();
  const after = await page.locator("#edit-folder").boundingBox();
  expect(Math.abs(before.y - after.y)).toBeLessThan(1);
  expect(await page.locator("#edit-channel-validation").evaluate(el => getComputedStyle(el).display)).not.toBe("none");
  await expect(page.locator("#edit-channel-validation")).toBeHidden();
});

for (const range of ["all", "fromdate", "subscribe"]) {
  test(`post-add ${range} prompt identifies saved scope and only syncs that channel`, async ({ page }) => {
    await loadApp(page);
    await openAdd(page, "https://www.youtube.com/@ExampleArchive");
    await page.locator("#edit-folder").fill("Example Folder");
    await page.evaluate(range => {
      const input = document.querySelector(`input[name="edit-range"][value="${range}"]`);
      input.checked = true;
      input.dispatchEvent(new Event("change", { bubbles: true }));
      const resolution = document.getElementById("edit-resolution");
      resolution.value = "1080";
      resolution.dispatchEvent(new Event("change", { bubbles: true }));
      window.__setBridgeHandler("subs_add_channel", async payload => ({
        ok: true, channel: { ...payload, name: "Example Channel",
          mode: range === "all" ? "full" : range === "fromdate" ? "fromdate" : "new" },
      }));
      window.__setBridgeHandler("sync_one_channel", async () => ({ ok: true, queued: true, started: true }));
    }, range);
    if (range === "fromdate") {
      await page.locator("#edit-date-year").fill("2024");
      await page.locator("#edit-date-month").fill("06");
      await page.locator("#edit-date-day").fill("03");
    }
    await page.locator("#btn-edit-update").click();
    const dialog = page.getByRole("dialog", { name: "Channel added", exact: true });
    await expect(dialog).toBeVisible();
    await expect(dialog).toContainText("Example Channel was added.");
    await expect(dialog).toContainText("Resolution: 1080p");
    await expect(dialog).toContainText("Archive folder: Example Folder");
    await expect(dialog).toContainText(range === "all" ? "Scope: Entire channel"
      : range === "fromdate" ? "Scope: Videos from 2024-06-03" : "Scope: New videos only");
    expect(await page.evaluate(() => window.__bridgeCallsFor("sync_one_channel").length)).toBe(0);
    await dialog.getByRole("button", { name: "Sync now", exact: true }).click();
    await expect.poll(() => page.evaluate(() => window.__bridgeCallsFor("sync_one_channel").length)).toBe(1);
    expect(await page.evaluate(() => window.__bridgeCallsFor("sync_one_channel")[0].args)).toEqual([{ name: "Example Channel" }]);
    expect(await page.evaluate(() => window.__bridgeCallsFor("sync_start_all").length)).toBe(0);
  });
}

test("Later leaves the channel saved without starting sync", async ({ page }) => {
  await loadApp(page);
  await openAdd(page, "https://www.youtube.com/@ExampleArchive");
  await page.locator("#edit-folder").fill("Example Folder");
  await page.locator("#btn-edit-update").click();
  const dialog = page.getByRole("dialog", { name: "Channel added", exact: true });
  await expect(dialog).toBeVisible();
  await dialog.getByRole("button", { name: "Later", exact: true }).click();
  expect(await page.evaluate(() => window.__bridgeCallsFor("subs_add_channel").length)).toBe(1);
  expect(await page.evaluate(() => window.__bridgeCallsFor("sync_one_channel").length)).toBe(0);
});

test("future auto-sync deadline wins over unrelated waiting state", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("autorun_state", async () => ({
      mins: 60, label: "1 hour", mode: "clock", waiting_for_sync: true,
      busy_reason: "an archive rescan", next_fire_ts: Date.now() / 1000 + 3600,
    }));
    window.dispatchEvent(new Event("autorun-state-changed"));
  });
  await expect(page.locator("#autorun-countdown")).toContainText("Next at");
  await expect(page.locator("#autorun-countdown")).not.toContainText("waiting");
});

test("due auto-sync describes actual blocker or its own running job", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("autorun_state", async () => ({
      mins: 60, label: "1 hour", mode: "clock", waiting_for_sync: true,
      busy_reason: "an archive rescan", next_fire_ts: Date.now() / 1000 - 60,
    }));
    window.dispatchEvent(new Event("autorun-state-changed"));
  });
  await expect(page.locator("#autorun-countdown")).toHaveText("waiting for an archive rescan…");
  await page.evaluate(() => {
    window.__setBridgeHandler("autorun_state", async () => ({
      mins: 60, label: "1 hour", waiting_for_sync: true, scheduled_sync_running: true,
    }));
    window.dispatchEvent(new Event("autorun-state-changed"));
  });
  await expect(page.locator("#autorun-countdown")).toHaveText("scheduled sync running…");
});

test("sample confirmation keeps Cancel focused after both modal focus timers settle", async ({ page }) => {
  await loadApp(page);
  await page.clock.install();
  await page.evaluate(() => {
    window.dispatchEvent(new CustomEvent("yt-control", { detail: {
      kind: "redownload_sample", sample_id: "settled-focus",
      deadline_ts: Date.now() / 1000 + 300,
    } }));
  });
  await page.clock.runFor(160);
  await expect(page.locator("#redwnl-sample-cancel")).toBeFocused();
});
