const { test, expect, loadApp } = require("./fixtures");

async function openBackups(page) {
  await page.locator('.tab[data-tab="health"]').click();
  await page.locator('#panel-health [data-settings-view="backups"]').click();
}

function keepTrigger(page) {
  return page.locator('.yt-dd:has(+ #settings-auto-backup-keep) .yt-dd-trigger');
}

async function chooseKeep(page, count) {
  await keepTrigger(page).click();
  await page.getByRole("option", { name: String(count), exact: true }).click();
}

function retentionDialog(page) {
  return page.getByRole("dialog", { name: "Keep fewer backups?", exact: true });
}

async function approveKeep(page, count) {
  await retentionDialog(page).getByRole("button", { name: `Keep ${count}`, exact: true }).click();
  await expect(retentionDialog(page)).not.toBeVisible();
}

test("scheduled backup retention loads the saved choice and explains when cleanup occurs", async ({ page }) => {
  await loadApp(page, { bridge: { settings: { auto_backup_keep: 7 } } });
  await openBackups(page);
  await expect(page.locator("#settings-auto-backup-keep")).toHaveValue("7");
  await expect(keepTrigger(page)).toContainText("7");
  await expect(page.locator("#backup-retention-help")).toContainText("only after the next successful automatic backup");
  expect(await page.evaluate(() => window.__bridgeCallsFor("settings_save"))).toEqual([]);
});

test("scheduled backup retention defaults to four for an existing config", async ({ page }) => {
  await loadApp(page);
  await openBackups(page);
  await expect(page.locator("#settings-auto-backup-keep")).toHaveValue("4");
  await expect(keepTrigger(page)).toContainText("4");
});

test("retention saves integer bounds without starting a backup or changing other backup settings", async ({ page }) => {
  await loadApp(page);
  await openBackups(page);
  await page.locator("#settings-backup-include-search-db").uncheck();
  await expect(page.locator("#settings-backup-include-search-db")).toBeEnabled();
  for (const count of [1, 10]) {
    await chooseKeep(page, count);
    if (count === 1) await approveKeep(page, count);
    else await expect(retentionDialog(page)).not.toBeVisible();
    await expect(page.locator("#settings-auto-backup-keep")).toBeEnabled();
    expect(await page.evaluate(() => window.__bridgeCallsFor("settings_save").at(-1).args))
      .toEqual([{ auto_backup_keep: count }]);
  }
  expect(await page.evaluate(() => window.__bridgeCalls.filter(call => /backup/i.test(call.name))))
    .toEqual([]);
  await page.reload();
  await page.evaluate(() => window.YT.settingsReady);
  await openBackups(page);
  await expect(page.locator("#settings-auto-backup-keep")).toHaveValue("10");
  await expect(page.locator("#settings-backup-include-search-db")).not.toBeChecked();
  await expect(page.locator("#settings-auto-backup")).toHaveValue("weekly");
  expect(await page.evaluate(() => window.__bridgeCallsFor("export_full_backup"))).toEqual([]);
  expect(await page.evaluate(() => window.__bridgeCallsFor("import_full_backup"))).toEqual([]);
});

test("a failed retention save restores the saved choice", async ({ page }) => {
  await loadApp(page, { bridge: { settings: { auto_backup_keep: 6 } } });
  await openBackups(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("settings_save", () => new Promise(resolve => {
      window.__resolveRetentionSave = resolve;
    }));
    window.__messages = [];
    window._showToast = message => window.__messages.push(message);
  });
  await chooseKeep(page, 2);
  await expect(page.locator("#settings-auto-backup-keep")).toBeDisabled();
  expect(await page.evaluate(() => window.__bridgeCallsFor("settings_save"))).toEqual([]);
  await approveKeep(page, 2);
  await expect(page.locator("#settings-auto-backup-keep")).toBeDisabled();
  await expect.poll(() => page.evaluate(() => typeof window.__resolveRetentionSave)).toBe("function");
  await page.evaluate(() => {
    window.__resolveRetentionSave({ ok: false, error: "Fixture retention save failed" });
  });
  await expect(page.locator("#settings-auto-backup-keep")).toBeEnabled();
  await expect(page.locator("#settings-auto-backup-keep")).toHaveValue("6");
  await expect(keepTrigger(page)).toContainText("6");
  expect(await page.evaluate(() => window.__messages.join(" "))).toContain("Fixture retention save failed");
  expect(await page.evaluate(() => window.__bridgeCallsFor("export_full_backup"))).toEqual([]);
  // A failed save must not make the rejected value the baseline for later warnings.
  await chooseKeep(page, 4);
  await expect(retentionDialog(page)).toBeVisible();
  await expect(retentionDialog(page).locator(".askq-body")).toContainText(/6.*4/s);
  await retentionDialog(page).getByRole("button", { name: "Cancel", exact: true }).click();
  await expect(page.locator("#settings-auto-backup-keep")).toHaveValue("6");
  expect(await page.evaluate(() => window.__bridgeCallsFor("settings_save").map(call => call.args)))
    .toEqual([[{ auto_backup_keep: 2 }]]);
});

for (const cancelAction of ["Cancel", "Escape", "Enter"]) {
  test(`lowering backup retention can be cancelled with ${cancelAction} without saving`, async ({ page }) => {
    await loadApp(page, { bridge: { settings: { auto_backup_keep: 6 } } });
    await openBackups(page);
    for (const count of [2, 4]) {
      await chooseKeep(page, count);
      const dialog = retentionDialog(page);
      await expect(dialog).toBeVisible();
      await expect(dialog.locator(".askq-body")).toContainText(new RegExp(`6.*${count}`, "s"));
      await expect(page.locator("#settings-auto-backup-keep")).toBeDisabled();
      await expect(dialog.getByRole("button", { name: "Cancel", exact: true })).toBeFocused();
      expect(await page.evaluate(() => window.__bridgeCallsFor("settings_save"))).toEqual([]);
      if (cancelAction === "Cancel") {
        await dialog.getByRole("button", { name: "Cancel", exact: true }).click();
      } else {
        await page.keyboard.press(cancelAction);
      }
      await expect(dialog).not.toBeVisible();
      await expect(page.locator("#settings-auto-backup-keep")).toBeEnabled();
      await expect(page.locator("#settings-auto-backup-keep")).toHaveValue("6");
      await expect(keepTrigger(page)).toContainText("6");
      expect(await page.evaluate(() => window.__bridgeCallsFor("settings_save"))).toEqual([]);
    }
  });
}

test("lower retention warns about deferred deletion and uses the last saved count", async ({ page }) => {
  await loadApp(page, { bridge: { settings: { auto_backup_keep: 4 } } });
  await openBackups(page);
  await chooseKeep(page, 8);
  await expect(retentionDialog(page)).not.toBeVisible();
  await expect(page.locator("#settings-auto-backup-keep")).toBeEnabled();
  await chooseKeep(page, 5);
  const dialog = retentionDialog(page);
  await expect(dialog).toBeVisible();
  const body = dialog.locator(".askq-body");
  await expect(body).toContainText(/8.*5/s);
  await expect(body).toContainText(/no.*deleted.*now|nothing.*deleted.*now|no immediate deletion/i);
  await expect(body).toContainText(/permanently delet/i);
  await expect(body).toContainText(/next successful automatic backup/i);
  expect(await page.evaluate(() => window.__bridgeCallsFor("settings_save").map(call => call.args)))
    .toEqual([[{ auto_backup_keep: 8 }]]);
  await approveKeep(page, 5);
  await expect(page.locator("#settings-auto-backup-keep")).toBeEnabled();
  expect(await page.evaluate(() => window.__bridgeCallsFor("settings_save").map(call => call.args)))
    .toEqual([[{ auto_backup_keep: 8 }], [{ auto_backup_keep: 5 }]]);
  expect(await page.evaluate(() => window.__bridgeCalls.filter(call => /backup/i.test(call.name))))
    .toEqual([]);
  await chooseKeep(page, 3);
  await expect(dialog.locator(".askq-body")).toContainText(/5.*3/s);
  await dialog.getByRole("button", { name: "Cancel", exact: true }).click();
  await expect(page.locator("#settings-auto-backup-keep")).toHaveValue("5");
  expect(await page.evaluate(() => window.__bridgeCallsFor("settings_save").map(call => call.args)))
    .toEqual([[{ auto_backup_keep: 8 }], [{ auto_backup_keep: 5 }]]);
});
