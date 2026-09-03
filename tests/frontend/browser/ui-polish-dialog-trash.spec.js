const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

async function openVideoTrashMenu(page, gridId, filepath) {
  await page.evaluate(({ gridId, filepath }) => {
    const grid = document.getElementById(gridId);
    if (!grid) throw new Error(`Missing fixture grid: ${gridId}`);

    const card = document.createElement("article");
    card.className = "video-card";
    card.dataset.uiTrashFixture = gridId;
    card.dataset.filepath = filepath;
    card.dataset.videoId = `fixture-${gridId}`;
    card.dataset.title = `Fixture ${gridId}`;
    card.dataset.channel = "Fixture Channel";
    card.textContent = `Fixture ${gridId}`;
    grid.appendChild(card);
    card.dispatchEvent(new MouseEvent("contextmenu", {
      bubbles: true,
      clientX: 120,
      clientY: 120,
    }));
  }, { gridId, filepath });

  const item = page.getByRole("menuitem", {
    name: gridId === "manual-grid"
      ? "Remove from YTArchiver…"
      : "Move file to trash…",
    exact: true,
  });
  await expect(item).toBeVisible();
  await expect(item).toHaveClass(/\bdanger\b/);
  return item;
}

test.describe("confirmed UI polish regressions", () => {
  test("choice-dialog Enter honors the focused choice and focused Cancel", async ({ page }) => {
    await loadApp(page);

    await page.evaluate(() => {
      window.__choiceResult = "pending";
      window.askChoice({
        title: "Keyboard choice",
        message: "Choose one action.",
        choices: [
          { label: "Secondary action", value: "secondary", kind: "ghost" },
          { label: "Primary action", value: "primary", kind: "primary" },
        ],
      }).then((value) => { window.__choiceResult = value; });
    });
    const firstDialog = page.getByRole("dialog", { name: "Keyboard choice" });
    await expect(firstDialog).toBeVisible();
    await expect(firstDialog.getByRole("button", { name: "Primary action" }))
      .toBeFocused();
    await firstDialog.getByRole("button", { name: "Secondary action" }).focus();
    await page.keyboard.press("Enter");
    await expect.poll(() => page.evaluate(() => window.__choiceResult))
      .toBe("secondary");
    await expect(firstDialog).toHaveCount(0);

    await page.evaluate(() => {
      window.__choiceResult = "pending";
      window.askChoice({
        title: "Keyboard cancel",
        message: "Choose one action.",
        choices: [
          { label: "Primary action", value: "primary", kind: "primary" },
        ],
      }).then((value) => { window.__choiceResult = value; });
    });
    const secondDialog = page.getByRole("dialog", { name: "Keyboard cancel" });
    await expect(secondDialog).toBeVisible();
    await expect(secondDialog.getByRole("button", { name: "Primary action" }))
      .toBeFocused();
    await secondDialog.getByRole("button", { name: "Cancel" }).focus();
    await page.keyboard.press("Enter");
    await expect.poll(() => page.evaluate(() => window.__choiceResult))
      .toBe(null);
  });

  test("canceling Move file to trash leaves the file and bridge untouched", async ({ page }) => {
    await loadApp(page);
    const filepath = "C:\\FixtureArchive\\cancel-me.mp4";

    const item = await openVideoTrashMenu(page, "video-grid", filepath);
    await item.click();

    const dialog = page.getByRole("dialog", { name: "Move file to trash?" });
    await expect(dialog).toBeVisible();
    await expect(dialog).toContainText("YTArchiver Trash");
    await expect(dialog).toContainText("restore it from Trash");
    await dialog.getByRole("button", { name: "Cancel" }).click();

    await expect.poll(() => page.evaluate(() =>
      window.__bridgeCallsFor("video_delete_file").length)).toBe(0);
    await expect(page.locator('[data-ui-trash-fixture="video-grid"]'))
      .toHaveCount(1);
  });

  test("every video grid confirms exactly one move-to-trash bridge call", async ({ page }) => {
    await loadApp(page);
    const gridIds = ["video-grid", "recent-grid", "manual-grid"];

    for (const [index, gridId] of gridIds.entries()) {
      const filepath = `C:\\FixtureArchive\\${gridId}.mp4`;
      await page.evaluate(() => {
        window.__setBridgeHandler("video_delete_file", async () => ({
          ok: true,
          message: "Moved to fixture trash.",
        }));
      });

      const item = await openVideoTrashMenu(page, gridId, filepath);
      await item.click();
      const manual = gridId === "manual-grid";
      const dialog = page.getByRole("dialog", {
        name: manual ? "Remove downloaded video?" : "Move file to trash?",
      });
      await expect(dialog).toBeVisible();
      await dialog.getByRole("button", {
        name: manual ? "Remove" : "Move to trash",
      }).click();

      await expect.poll(() => page.evaluate(() =>
        window.__bridgeCallsFor("video_delete_file").length)).toBe(index + 1);
      const calls = await page.evaluate(() =>
        window.__bridgeCallsFor("video_delete_file"));
      expect(calls[index].args).toEqual([filepath]);
      await expect(page.locator(`[data-ui-trash-fixture="${gridId}"]`))
        .toHaveCount(0);
    }
  });

  test("catalog-only cleanup truthfully says the external file was preserved", async ({ page }) => {
    await loadApp(page);
    const filepath = "C:\\OutsideFixture\\catalog-only.mp4";
    await page.evaluate(() => {
      window.__trashChangedCalls = 0;
      window._onTrashChanged = () => { window.__trashChangedCalls += 1; };
      window.__setBridgeHandler("video_delete_file", async () => ({
        ok: true,
        catalog_entry_removed: true,
        external_file_preserved: true,
        message: "Removed from YTArchiver. The external file was left in place.",
      }));
    });

    const item = await openVideoTrashMenu(page, "manual-grid", filepath);
    await item.click();
    const dialog = page.getByRole("dialog", { name: "Remove downloaded video?" });
    await expect(dialog).toContainText("outside the archive");
    await dialog.getByRole("button", { name: "Remove" }).click();

    await expect(page.locator('[data-ui-trash-fixture="manual-grid"]'))
      .toHaveCount(0);
    await expect(page.locator(".toast").last())
      .toContainText("external file was left in place");
    await expect.poll(() => page.evaluate(() => window.__trashChangedCalls))
      .toBe(0);
  });
});
