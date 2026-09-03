const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

async function openHealthView(page, view) {
  await page.locator('.tab[data-tab="health"]').click();
  await page.locator(`#panel-health [data-settings-view="${view}"]`).click();
}

test("metadata summary uses all six columns when wide and a 3-by-2 grid at minimum size", async ({ page }) => {
  await page.setViewportSize({ width: 980, height: 720 });
  await loadApp(page);
  await openHealthView(page, "library");
  await page.locator("#health-library-metadata").evaluate((details) => {
    details.open = true;
  });

  const totals = page.locator("#metadata-totals");
  await totals.evaluate((element) => { element.hidden = false; });

  const columnCount = () => totals.evaluate((element) => {
    // The dashboard hides this strip when the fixture has no rows. Reveal it
    // in the same synchronous measurement so async fixture rendering cannot
    // race the responsive-CSS assertion.
    element.hidden = false;
    return getComputedStyle(element).gridTemplateColumns
      .split(/\s+/).filter(Boolean).length;
  });

  await expect.poll(columnCount).toBe(3);

  await page.setViewportSize({ width: 1440, height: 900 });
  await expect.poll(columnCount).toBe(6);
});

test("Health Library repairs stay inside the viewport at the native minimum size", async ({ page }) => {
  await page.setViewportSize({ width: 980, height: 720 });
  await loadApp(page);
  await openHealthView(page, "library");
  await page.locator("#health-library-archive").evaluate((details) => {
    details.open = true;
    details.querySelectorAll("details").forEach((nested) => {
      nested.open = true;
    });
  });

  const geometry = await page.evaluate(() => {
    const area = document.querySelector("#panel-health .settings-area");
    const main = document.querySelector("#panel-health .settings-main");
    const archive = document.getElementById("health-library-archive");
    const areaRect = area.getBoundingClientRect();
    const mainRect = main.getBoundingClientRect();
    const visibleRows = [...archive.querySelectorAll(
      ".edit-grid, .edit-gv-inline, .edit-gv-stack, .settings-inline-note")]
      .filter((element) => element.getClientRects().length > 0);
    const overflowers = visibleRows
      .map((element) => {
        const rect = element.getBoundingClientRect();
        return {
          className: element.className,
          right: rect.right,
          scrollWidth: element.scrollWidth,
          clientWidth: element.clientWidth,
        };
      })
      .filter((item) =>
        item.right > areaRect.right + 1 || item.scrollWidth > item.clientWidth + 1);
    return {
      areaRight: areaRect.right,
      areaScrollWidth: area.scrollWidth,
      areaClientWidth: area.clientWidth,
      mainRight: mainRect.right,
      mainScrollWidth: main.scrollWidth,
      mainClientWidth: main.clientWidth,
      overflowers,
    };
  });

  expect(geometry.mainRight).toBeLessThanOrEqual(geometry.areaRight + 1);
  expect(geometry.areaScrollWidth).toBeLessThanOrEqual(geometry.areaClientWidth + 1);
  expect(geometry.mainScrollWidth).toBeLessThanOrEqual(geometry.mainClientWidth + 1);
  expect(geometry.overflowers).toEqual([]);
});

test("Watch playback controls wrap without clipping", async ({ page }) => {
  await page.setViewportSize({ width: 980, height: 720 });
  await loadApp(page);
  await page.locator('.tab[data-tab="browse"]').click();
  await page.evaluate(() => {
    document.querySelectorAll("#panel-browse .browse-view").forEach((view) => {
      view.hidden = true;
    });
    document.getElementById("view-watch").hidden = false;
    document.querySelector("#view-watch .watch-layout")
      .style.setProperty("--watch-tx-width", "500px");
  });

  const geometry = await page.evaluate(() => {
    const videoColumn = document.querySelector("#view-watch .watch-video-wrap");
    const row = document.querySelector("#view-watch .watch-actions-row2");
    const columnRect = videoColumn.getBoundingClientRect();
    const rowRect = row.getBoundingClientRect();
    return {
      columnRight: columnRect.right,
      rowRight: rowRect.right,
      rowScrollWidth: row.scrollWidth,
      rowClientWidth: row.clientWidth,
    };
  });

  expect(geometry.rowRight).toBeLessThanOrEqual(geometry.columnRight + 1);
  expect(geometry.rowScrollWidth).toBeLessThanOrEqual(geometry.rowClientWidth + 1);

  // Turning the overlay on reveals Style + Mode. At this same narrow pane
  // width, that group used to be 52px wider than its row even though the
  // default (Overlay Off) state above fit correctly.
  await page.locator("#watch-cap-size").selectOption("small");
  await expect(page.locator("#watch-overlay-extras")).not.toHaveClass(/collapsed/);
  const expanded = await page.evaluate(() => {
    const videoColumn = document.querySelector("#view-watch .watch-video-wrap");
    const row = document.querySelector("#view-watch .watch-actions-row2");
    const group = row.querySelector(".watch-overlay-group");
    const columnRect = videoColumn.getBoundingClientRect();
    const rowRect = row.getBoundingClientRect();
    const groupRect = group.getBoundingClientRect();
    return {
      columnRight: columnRect.right,
      rowRight: rowRect.right,
      groupRight: groupRect.right,
      rowScrollWidth: row.scrollWidth,
      rowClientWidth: row.clientWidth,
      groupScrollWidth: group.scrollWidth,
      groupClientWidth: group.clientWidth,
    };
  });

  expect(expanded.groupRight).toBeLessThanOrEqual(expanded.rowRight + 1);
  expect(expanded.rowRight).toBeLessThanOrEqual(expanded.columnRight + 1);
  expect(expanded.rowScrollWidth).toBeLessThanOrEqual(expanded.rowClientWidth + 1);
  expect(expanded.groupScrollWidth).toBeLessThanOrEqual(expanded.groupClientWidth + 1);
});

test("ArrowRight enters a context submenu and ArrowLeft returns to its trigger", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.showContextMenu(40, 40, [
      {
        label: "More actions",
        submenu: [
          { label: "First nested action", action() {} },
          { label: "Second nested action", action() {} },
        ],
      },
      { label: "Regular action", action() {} },
    ]);
  });

  const trigger = page.locator(".ctx-submenu-wrap", { hasText: "More actions" });
  const submenu = trigger.locator(":scope > .ctx-submenu");
  await expect(trigger).toBeFocused();
  // Keep the mouse on the trigger to prove ArrowLeft explicitly closes the
  // flyout instead of leaving the hover-open CSS state behind.
  await trigger.hover();

  await page.keyboard.press("ArrowRight");

  await expect(page.locator(".ctx-submenu .ctx-menu-item").first()).toBeFocused();
  await expect(submenu).toBeVisible();
  await expect(trigger).toHaveAttribute("aria-expanded", "true");

  await page.keyboard.press("ArrowLeft");

  await expect(trigger).toBeFocused();
  await expect(submenu).toBeHidden();
  await expect(trigger).toHaveAttribute("aria-expanded", "false");

  // Vertical navigation remains available and skips the now-hidden flyout.
  await page.keyboard.press("ArrowDown");
  await expect(page.locator(".ctx-menu > .ctx-menu-item").last()).toBeFocused();
  await expect(submenu).toBeHidden();

  await page.keyboard.press("ArrowUp");
  await page.keyboard.press("ArrowRight");
  await page.keyboard.press("ArrowDown");
  await expect(page.locator(".ctx-submenu .ctx-menu-item").nth(1)).toBeFocused();
});

test("context flyouts stay fully visible near the bottom of the window", async ({ page }) => {
  await page.setViewportSize({ width: 980, height: 720 });
  await loadApp(page);
  await page.evaluate(() => {
    window.showContextMenu(940, 700, [
      { label: "First", action() {} },
      { label: "Second", action() {} },
      { label: "Third", action() {} },
      { label: "Fourth", action() {} },
      { label: "Fifth", action() {} },
      { label: "Long flyout", submenu: [
        { label: "Choice one", action() {} },
        { label: "Choice two", action() {} },
        { label: "Choice three", action() {} },
        { label: "Choice four", action() {} },
        { label: "Choice five", action() {} },
        { label: "Choice six", action() {} },
        { label: "Choice seven", action() {} },
      ] },
    ]);
  });

  const trigger = page.locator(".ctx-submenu-wrap", {
    hasText: "Long flyout",
  });
  await trigger.hover();
  const rect = await trigger.locator(":scope > .ctx-submenu")
    .evaluate((element) => {
      const bounds = element.getBoundingClientRect();
      return {
        top: bounds.top,
        right: bounds.right,
        bottom: bounds.bottom,
        left: bounds.left,
      };
    });
  expect(rect.top).toBeGreaterThanOrEqual(3);
  expect(rect.left).toBeGreaterThanOrEqual(3);
  expect(rect.right).toBeLessThanOrEqual(977);
  expect(rect.bottom).toBeLessThanOrEqual(717);
});

test("disabled context actions are announced, skipped, and cannot fire", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__disabledActionCalls = 0;
    window.showContextMenu(40, 40, [
      {
        label: "Unavailable action",
        disabled: true,
        action() { window.__disabledActionCalls += 1; },
      },
      { label: "Available action", action() {} },
    ]);
  });

  const disabled = page.getByRole("menuitem", { name: "Unavailable action" });
  const available = page.getByRole("menuitem", {
    name: "Available action",
    exact: true,
  });
  await expect(disabled).toHaveAttribute("aria-disabled", "true");
  await expect(available).toBeFocused();
  await disabled.evaluate((element) => element.click());
  await expect.poll(() => page.evaluate(() => window.__disabledActionCalls))
    .toBe(0);
});

test("Settings dropdowns flip above a clipped bottom edge", async ({ page }) => {
  await page.setViewportSize({ width: 980, height: 720 });
  await loadApp(page);
  await page.evaluate(() => {
    const host = document.createElement("div");
    host.id = "dropdown-clip-fixture";
    host.className = "settings-main";
    Object.assign(host.style, {
      position: "fixed",
      left: "40px",
      top: "120px",
      width: "360px",
      height: "210px",
      overflow: "hidden",
      background: "var(--c-surface)",
      zIndex: "20000",
    });
    const view = document.createElement("div");
    view.className = "settings-view";
    Object.assign(view.style, {
      position: "absolute",
      left: "12px",
      right: "12px",
      bottom: "8px",
    });
    const select = document.createElement("select");
    select.id = "dropdown-bottom-fixture";
    select.className = "ctl-select";
    for (const label of ["One", "Two", "Three", "Four", "Five", "Six"]) {
      const option = document.createElement("option");
      option.value = label.toLowerCase();
      option.textContent = label;
      select.appendChild(option);
    }
    view.appendChild(select);
    host.appendChild(view);
    document.body.appendChild(host);
    window.enhanceSelect(select);
  });

  const host = page.locator("#dropdown-clip-fixture");
  const trigger = host.locator(".yt-dd-trigger");
  const menu = host.locator(".yt-dd-menu");
  await trigger.click();
  await expect(host.locator(".yt-dd")).toHaveClass(/\bopen-up\b/);
  const bounds = await page.evaluate(() => {
    const clip = document.getElementById("dropdown-clip-fixture")
      .getBoundingClientRect();
    const flyout = document.querySelector(
      "#dropdown-clip-fixture .yt-dd-menu").getBoundingClientRect();
    return {
      clipTop: clip.top,
      clipBottom: clip.bottom,
      menuTop: flyout.top,
      menuBottom: flyout.bottom,
    };
  });
  expect(bounds.menuTop).toBeGreaterThanOrEqual(bounds.clipTop - 1);
  expect(bounds.menuBottom).toBeLessThanOrEqual(bounds.clipBottom + 1);

  await menu.getByRole("option", { name: "Six" }).click();
  await expect(page.locator("#dropdown-bottom-fixture")).toHaveValue("six");

  await trigger.focus();
  await page.keyboard.press("Enter");
  await page.keyboard.press("Home");
  await page.keyboard.press("Enter");
  await expect(page.locator("#dropdown-bottom-fixture")).toHaveValue("one");
});
