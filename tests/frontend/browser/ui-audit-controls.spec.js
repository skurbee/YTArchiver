const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

async function openEditor(page, name = "Control Fixture") {
  await page.evaluate(name => {
    window.__setBridgeHandler("subs_get_channel", async identity => ({
      ok: true,
      channel: {
        name: identity.name, folder: identity.name,
        url: "https://www.youtube.com/@ControlFixture",
        resolution: "720", min_duration: 3, max_duration: 20,
        mode: "new",
      },
    }));
    window._editChannelFromBrowse(name);
  }, name);
  await expect(page.locator("#edit-folder")).toHaveValue(name);
}

test("save locks the submitted form and failed save restores its edits", async ({ page }) => {
  await loadApp(page);
  await openEditor(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("subs_update_channel", () => new Promise(resolve => {
      window.__finishControlSave = resolve;
    }));
  });
  await page.locator("#edit-min-dur").fill("4");
  await page.locator("#btn-edit-update").click();
  await expect.poll(() => page.evaluate(() => typeof window.__finishControlSave)).toBe("function");
  await expect(page.locator("#edit-min-dur")).toBeDisabled();
  await expect(page.locator("#btn-edit-cancel")).toBeDisabled();
  await expect(page.locator("#btn-edit-remove")).toBeDisabled();
  await page.evaluate(() => {
    document.getElementById("edit-min-dur").dispatchEvent(new Event("input", { bubbles: true }));
    document.getElementById("btn-edit-update").click();
  });
  await expect(page.locator("#btn-edit-update")).toBeDisabled();
  expect(await page.evaluate(() => window.__bridgeCallsFor("subs_update_channel").length)).toBe(1);
  await page.evaluate(() => window.__finishControlSave({ ok: false, error: "Temporary write failure" }));
  await expect(page.locator("#edit-min-dur")).toBeEnabled();
  await expect(page.locator("#edit-min-dur")).toHaveValue("4");
  await expect(page.locator("#btn-edit-update")).toBeEnabled();
  await expect(page.locator("#channel-editor-backdrop")).toBeVisible();
});

test("late save cannot close or disable a newer channel editor", async ({ page }) => {
  await loadApp(page);
  await openEditor(page, "First Fixture");
  await page.evaluate(() => {
    window.__setBridgeHandler("subs_update_channel", () => new Promise(resolve => {
      window.__finishControlSave = resolve;
    }));
  });
  await page.locator("#edit-min-dur").fill("4");
  await page.locator("#btn-edit-update").click();
  await expect.poll(() => page.evaluate(() => typeof window.__finishControlSave)).toBe("function");
  await openEditor(page, "Second Fixture");
  await expect(page.locator("#edit-min-dur")).toBeEnabled();
  await page.evaluate(() => window.__finishControlSave({ ok: true, channel: { name: "First Fixture" } }));
  await expect(page.locator("#channel-editor-backdrop")).toBeVisible();
  await expect(page.locator("#edit-folder")).toHaveValue("Second Fixture");
  await page.locator("#edit-min-dur").fill("5");
  await expect(page.locator("#btn-edit-update")).toBeEnabled();
});

test("blank duration fields explicitly clear both existing limits", async ({ page }) => {
  await loadApp(page);
  await openEditor(page);
  await page.locator("#edit-min-dur").fill("");
  await page.locator("#edit-max-dur").fill("");
  await page.locator("#btn-edit-update").click();
  await expect.poll(() => page.evaluate(() => window.__bridgeCallsFor("subs_update_channel").length)).toBe(1);
  const payload = await page.evaluate(() => window.__bridgeCallsFor("subs_update_channel")[0].args[1]);
  expect(payload.min_duration).toBe(0);
  expect(payload.max_duration).toBe(0);
});

test("unknown resolution is reported separately and incomplete scan cannot claim a match", async ({ page }) => {
  await loadApp(page);
  await openEditor(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("chan_scan_resolution_mismatch", async () => ({
      ok: true, complete: true, total: 3, scanned: 2, unknown: 1, mismatch: 0,
    }));
  });
  await page.locator("#edit-res-recheck").click();
  await expect(page.locator("#toast-root .toast").last()).toContainText("resolution is unknown");
  await page.evaluate(() => {
    window.__setBridgeHandler("chan_scan_resolution_mismatch", async () => ({
      ok: true, started: true, token: "incomplete",
    }));
    window.__setBridgeHandler("chan_scan_resolution_mismatch_poll", async () => ({ ok: true }));
  });
  await page.locator("#edit-res-recheck").click();
  await expect(page.locator("#toast-root .toast").last()).toContainText("Scan timed out");
});

test("sample answer waits for acknowledgement and failed acknowledgement stays visible", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("redownload_sample_confirm", () => new Promise(resolve => {
      window.__finishSampleAnswer = resolve;
    }));
    window.dispatchEvent(new CustomEvent("yt-control", { detail: {
      kind: "redownload_sample", sample_id: "sample-first",
      deadline_ts: Date.now() / 1000 + 300, sample_n: 10, res_label: "720p",
    } }));
  });
  await expect(page.locator("#redwnl-sample-cancel")).toBeFocused();
  await page.locator("#redwnl-sample-continue").click();
  await expect(page.locator("#redwnl-sample-modal")).toBeVisible();
  await expect(page.locator("#redwnl-sample-continue")).toBeDisabled();
  expect(await page.evaluate(() => window.__bridgeCallsFor("redownload_sample_confirm")[0].args)).toEqual(["continue", "sample-first"]);
  await page.evaluate(() => window.__finishSampleAnswer({ ok: false, error: "Answer not saved" }));
  await expect(page.locator("#redwnl-sample-modal")).toBeVisible();
  await expect(page.locator("#redwnl-sample-cancel")).toBeEnabled();
  await page.locator("#redwnl-sample-cancel").click();
  await page.evaluate(() => window.__finishSampleAnswer({ ok: true, sample_id: "sample-first" }));
  await expect(page.locator("#redwnl-sample-modal")).toBeHidden();
});

test("hover cannot extend a sample deadline and only its backend close event dismisses it", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.dispatchEvent(new CustomEvent("yt-control", { detail: {
      kind: "redownload_sample", sample_id: "sample-timeout",
      deadline_ts: Date.now() / 1000 + 0.3, sample_n: 10, res_label: "720p",
    } }));
    document.getElementById("redwnl-sample-modal").dispatchEvent(new Event("mouseenter"));
    document.getElementById("redwnl-sample-modal").dispatchEvent(new Event("mouseleave"));
  });
  await expect(page.locator("#redwnl-sample-countdown")).toContainText("timed out");
  await expect(page.locator("#redwnl-sample-continue")).toBeDisabled();
  expect(await page.evaluate(() => window.__bridgeCallsFor("redownload_sample_confirm").length)).toBe(0);
  await page.evaluate(() => window.dispatchEvent(new CustomEvent("yt-control", { detail: {
    kind: "redownload_sample_closed", sample_id: "older-sample", reason: "timeout",
  } })));
  await expect(page.locator("#redwnl-sample-modal")).toBeVisible();
  await page.evaluate(() => window.dispatchEvent(new CustomEvent("yt-control", { detail: {
    kind: "redownload_sample_closed", sample_id: "sample-timeout", reason: "timeout",
  } })));
  await expect(page.locator("#redwnl-sample-modal")).toBeHidden();
});

test("compression projection explains unknown media and retains its bytes", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    const row = { name: "Unknown duration", videos: 2, hours: 0,
      unknown_videos: 2, unknown_gb: 2, current_gb: 2,
      generous_gb: 2, average_gb: 2, below_gb: 2 };
    window.__setBridgeHandler("compress_dry_run", async () => ({
      ok: true, output_res: "720", channels: [row], total: row,
    }));
    document.getElementById("btn-compress-dry-run").click();
  });
  await expect(page.locator("#compress-dry-summary")).toContainText("2 with unknown duration (2.0 GB kept at current size)");
  await expect(page.locator("#compress-dry-body")).toContainText("no savings are assumed for them");
  await expect(page.locator("#compress-dry-body")).not.toContainText("LARGER");
});
