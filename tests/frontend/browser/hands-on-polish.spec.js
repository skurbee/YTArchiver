const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");


test("the Subs legend explains behind counts without duplicated symbols", async ({ page }) => {
  await loadApp(page);

  const legend = page.locator(".subs-legend");
  await expect(legend).toContainText("-N behind");
  await expect(legend).not.toContainText("— -N N behind");
});


test("one new session failure uses the singular error label", async ({ page }) => {
  await loadApp(page);
  // Startup history is intentionally ignored for 2.5 seconds. Add a fresh
  // actionable failure after that gate, just as the native log bridge does.
  await page.waitForTimeout(2600);
  await page.locator("#main-log").evaluate((log) => {
    const line = document.createElement("div");
    line.className = "log-line";
    const detail = document.createElement("span");
    detail.className = "t-error_detail";
    detail.textContent = "Fixture failure";
    line.appendChild(detail);
    log.appendChild(line);
  });

  const button = page.locator("#gsb-errors");
  await expect(button).toBeVisible();
  await expect(button).toContainText("1 error");
  await expect(button).not.toContainText("1 errors");
  await expect(button).toHaveAttribute(
    "title", "1 error this session — click for details",
  );
});
