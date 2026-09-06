const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

async function openAbout(page) {
  await loadApp(page);
  await page.locator('.tab[data-tab="settings"]').click();
  await page.locator("#settings-about-troubleshooting").evaluate(details => { details.open = true; });
  await page.locator("#btn-about").click();
  await expect(page.locator("#about-close")).toBeFocused();
}

for (const method of ["Escape", "backdrop"]) {
  test(`static modal ${method} close restores its launch button`, async ({ page }) => {
    await openAbout(page);
    if (method === "Escape") await page.keyboard.press("Escape");
    else await page.locator("#about-backdrop").click({ position: { x: 2, y: 2 } });
    await expect(page.locator("#about-backdrop")).toBeHidden();
    await expect(page.locator("#btn-about")).toBeFocused();
  });
}

test("closing a nested question restores the static dialog before its launch button", async ({ page }) => {
  await openAbout(page);
  await page.evaluate(() => {
    window.__nestedDecision = "pending";
    window.askQuestion({ title: "Nested confirmation", message: "Fixture", danger: true })
      .then(value => { window.__nestedDecision = value; });
  });
  await expect(page.getByRole("dialog", { name: "Nested confirmation" })).toBeVisible();
  await page.keyboard.press("Escape");
  await expect.poll(() => page.evaluate(() => window.__nestedDecision)).toBe(false);
  await expect(page.locator("#about-backdrop")).toBeVisible();
  await expect(page.locator("#about-close")).toBeFocused();
  await page.keyboard.press("Escape");
  await expect(page.locator("#about-backdrop")).toBeHidden();
  await expect(page.locator("#btn-about")).toBeFocused();
});
