const { test, expect, loadApp } = require("./fixtures");

const backupPath = `C:\\FixtureArchive\\${"Long archive directory\\".repeat(5)}${"backup_segment_".repeat(24)}.zip`;

async function noteLayout(note) {
  return note.evaluate(element => {
    const bounds = element.getBoundingClientRect();
    const parentBounds = element.parentElement.getBoundingClientRect();
    const cardBounds = element.closest(".health-backup-grid").getBoundingClientRect();
    return {
      width: element.clientWidth,
      height: element.clientHeight,
      scrollWidth: element.scrollWidth,
      insideParent: bounds.left >= parentBounds.left - 1 && bounds.right <= parentBounds.right + 1,
      insideCard: bounds.left >= cardBounds.left - 1 && bounds.right <= cardBounds.right + 1,
      pageOverflow: document.documentElement.scrollWidth - document.documentElement.clientWidth,
    };
  });
}

async function expectContained(note) {
  const layout = await noteLayout(note);
  expect(layout.insideParent).toBe(true);
  expect(layout.insideCard).toBe(true);
  expect(layout.pageOverflow).toBeLessThanOrEqual(1);
  return layout;
}

for (const width of [980, 1400]) {
  for (const interaction of ["hover", "keyboard focus"]) {
    test(`backup paths truncate and expand on ${interaction} within a ${width}px window`, async ({ page }) => {
      await page.setViewportSize({ width, height: 1000 });
      await loadApp(page, {
        bridge: {
          settings: {
            last_backup_ts: Math.floor(Date.now() / 1000) - 3600,
            last_backup_path: backupPath,
            last_auto_backup_ts: Math.floor(Date.now() / 1000) - 3600,
            last_auto_backup_path: backupPath,
          },
        },
      });
      await page.locator('.tab[data-tab="health"]').click();
      await page.locator('#panel-health [data-settings-view="backups"]').click();
      await page.mouse.move(0, 0);

      for (const id of ["backup-auto-age-display", "backup-age-display"]) {
        const note = page.locator(`#${id}`);
        await expect(note).toContainText(backupPath);
        await expect(note).toHaveCSS("white-space", "nowrap");
        await expect(note).toHaveCSS("text-overflow", "ellipsis");
        await expect(note).toHaveCSS("overflow-x", "hidden");
        const collapsed = await expectContained(note);
        expect(collapsed.scrollWidth).toBeGreaterThan(collapsed.width);
        const fullText = await note.textContent();

        if (interaction === "hover") {
          await note.hover();
        } else {
          await expect(note).toHaveAttribute("tabindex", "0");
          await note.focus();
          await page.keyboard.press("Shift+Tab");
          await expect(note).not.toBeFocused();
          await page.keyboard.press("Tab");
          await expect(note).toBeFocused();
        }

        await expect(note).toHaveCSS("white-space", "normal");
        await expect(note).toHaveText(fullText);
        const expanded = await expectContained(note);
        expect(expanded.height).toBeGreaterThan(collapsed.height);
        expect(expanded.scrollWidth).toBeLessThanOrEqual(expanded.width + 1);

        if (interaction === "hover") {
          await page.mouse.move(0, 0);
        } else {
          await page.keyboard.press("Tab");
          await expect(note).not.toBeFocused();
        }
        await expect(note).toHaveCSS("white-space", "nowrap");
        const restored = await expectContained(note);
        expect(restored.height).toBe(collapsed.height);
        expect(restored.scrollWidth).toBeGreaterThan(restored.width);
      }
    });
  }
}
