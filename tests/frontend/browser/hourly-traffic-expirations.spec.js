const { test, expect, loadApp } = require("./fixtures");

test.use({ locale: "en-US", timezoneId: "America/Chicago" });

const AS_OF = Date.parse("2030-01-15T23:30:30-06:00") / 1000;
const EXPIRATIONS = [
  { expires_at: Date.parse("2030-01-15T23:31:00-06:00") / 1000, units: 13 },
  { expires_at: Date.parse("2030-01-15T23:34:00-06:00") / 1000, units: 7 },
  { expires_at: Date.parse("2030-01-16T00:00:00-06:00") / 1000, units: 23 },
];
const POPUP = "#popover-traffic-expirations";
const BODY = "#gsb-traffic-expirations-body";
const TITLE = "#gsb-traffic-expirations-title";
const HOUR = "#gsb-traffic-hourly";
const DAY = "#gsb-traffic-daily";
const GROUP = "#gsb-traffic-expirations-group";
const CLOSE = "#gsb-traffic-expirations-close";

async function loadTraffic(page, { expirations = EXPIRATIONS } = {}) {
  await loadApp(page, {
    bridge: { responses: {
      youtube_traffic_status: {
        ok: true, mode: "custom", paused: false,
        hourly_used: 43, hourly_limit: 500, daily_used: 87, daily_limit: 6000,
      },
    } },
    args: { asOf: AS_OF, expirations },
    configure: ({ asOf, expirations: rows }) => {
      window.__setBridgeHandler("youtube_traffic_expirations", (mode) => (
        mode === "hourly" ? {
          ok: true, as_of: asOf,
          hourly_used: rows.reduce((sum, row) => sum + row.units, 0),
          expirations: rows,
        } : {
          ok: true, as_of: asOf, daily_used: 87,
          expirations: rows.map((row, index) => ({ ...row, units: [31, 29, 27][index] })),
        }
      ));
    },
  });
  await expect(page.locator(HOUR)).toBeVisible();
}

async function expirationArgs(page) {
  return page.evaluate(() => window.__bridgeCallsFor("youtube_traffic_expirations")
    .map(call => call.args));
}

test("hourly drop-offs show minute detail and charges across midnight without redundant explanations", async ({ page }) => {
  await loadTraffic(page);
  const hour = page.locator(HOUR);
  await expect(hour).toHaveAttribute("aria-haspopup", "dialog");
  await expect(hour).toHaveAttribute("aria-controls", "popover-traffic-expirations");
  await hour.click();

  await expect(page.locator(POPUP)).toBeVisible();
  await expect(page.locator(TITLE)).toHaveText("Hourly drop-offs");
  await expect(page.locator("#gsb-traffic-expirations-summary, #gsb-traffic-expirations-next, #gsb-traffic-expirations-note")).toHaveCount(0);
  await expect(page.locator(POPUP)).not.toContainText(/currently counted|Next drop-off|Local time|Usage falls off|New requests|Other limits/);
  await expect(page.locator(POPUP)).not.toHaveAttribute("aria-describedby");
  await expect(page.getByRole("combobox", { name: "Group by", exact: true })).toBeHidden();
  const rows = page.locator(`${BODY} tbody tr`);
  await expect(rows).toHaveCount(3);
  await expect(rows.locator("td:first-child")).toHaveText([
    "Today · 11:31 PM", "Today · 11:34 PM", "Tomorrow · 12:00 AM",
  ]);
  await expect(rows.locator("td:last-child")).toHaveText(["−13", "−7", "−23"]);
  expect(await expirationArgs(page)).toEqual([["hourly"]]);
});

for (const [label, offset] of [["already started", -30], ["at the current time", 0]]) {
  test(`hourly rows show the local time and charge when a bucket is ${label}`, async ({ page }) => {
    await loadTraffic(page, { expirations: [{ expires_at: AS_OF + offset, units: 12 }] });
    await page.locator(HOUR).click();
    const rows = page.locator(`${BODY} tbody tr`);
    await expect(rows).toHaveCount(1);
    await expect(rows.locator("td:first-child")).toHaveText(["Today · 11:30 PM"]);
    await expect(rows.locator("td:last-child")).toHaveText(["−12"]);
  });
}

test("hourly rows identify tomorrow when the first bucket crosses midnight", async ({ page }) => {
  await loadTraffic(page, { expirations: [EXPIRATIONS[2]] });
  await page.locator(HOUR).click();
  const rows = page.locator(`${BODY} tbody tr`);
  await expect(rows).toHaveCount(1);
  await expect(rows.locator("td:first-child")).toHaveText(["Tomorrow · 12:00 AM"]);
  await expect(rows.locator("td:last-child")).toHaveText(["−23"]);
});

test("switching the open popup preserves daily grouping without applying it to hourly minutes", async ({ page }) => {
  await loadTraffic(page);
  await page.locator(DAY).click();
  const group = page.getByRole("combobox", { name: "Group by", exact: true });
  await group.click();
  await page.locator(POPUP).getByRole("option", { name: "1 hour", exact: true }).click();
  await expect(page.locator(GROUP)).toHaveValue("60");
  await expect(page.locator(`${BODY} tbody tr`)).toHaveCount(2);

  await page.locator(HOUR).click();
  await expect(page.locator(POPUP)).toBeVisible();
  await expect(page.locator(TITLE)).toHaveText("Hourly drop-offs");
  await expect(page.locator(HOUR)).toHaveAttribute("aria-expanded", "true");
  await expect(page.locator(DAY)).toHaveAttribute("aria-expanded", "false");
  await expect(group).toBeHidden();
  await expect(page.locator(`${BODY} tbody tr`)).toHaveCount(3);
  await expect(page.locator(`${BODY} tbody tr`).first()).toContainText("11:31 PM");
  await expect(page.locator(`${BODY} tbody tr td:last-child`)).toHaveText(["−13", "−7", "−23"]);

  await page.locator(DAY).click();
  await expect(page.locator(TITLE)).toHaveText("24-hour drop-offs");
  await expect(page.locator(DAY)).toHaveAttribute("aria-expanded", "true");
  await expect(page.locator(HOUR)).toHaveAttribute("aria-expanded", "false");
  await expect(group).toBeVisible();
  await expect(page.locator(GROUP)).toHaveValue("60");
  await expect(page.locator(`${BODY} tbody tr`)).toHaveCount(2);
  await expect(page.locator(`${BODY} tbody tr td:last-child`)).toHaveText(["−60", "−27"]);
  expect(await expirationArgs(page)).toEqual([[], ["hourly"], []]);
  await page.keyboard.press("Escape");
  await expect(page.locator(DAY)).toBeFocused();
});

for (const firstMode of ["hourly", "daily"]) {
  test(`a pending ${firstMode} result cannot replace the other mode after switching`, async ({ page }) => {
    await loadTraffic(page);
    await page.evaluate(() => {
      window.__expirationResolvers = [];
      window.__setBridgeHandler("youtube_traffic_expirations", (mode) => new Promise(resolve => {
        window.__expirationResolvers.push({ mode: mode || "daily", resolve });
      }));
    });
    const first = firstMode === "hourly" ? HOUR : DAY;
    const second = firstMode === "hourly" ? DAY : HOUR;
    const secondMode = firstMode === "hourly" ? "daily" : "hourly";
    await page.locator(first).click();
    await expect(page.locator(BODY)).toContainText("Loading drop-offs");
    await page.locator(second).click();
    await expect.poll(() => page.evaluate(() => window.__expirationResolvers.length)).toBe(2);
    await page.evaluate(({ asOf, mode }) => window.__expirationResolvers[1].resolve({
      ok: true, as_of: asOf, [`${mode}_used`]: 222,
      expirations: [{ expires_at: asOf + 60, units: 222 }],
    }), { asOf: AS_OF, mode: secondMode });
    await expect(page.locator(`${BODY} tbody tr td:last-child`)).toHaveText(["−222"]);

    await page.evaluate(async ({ asOf, mode }) => {
      window.__expirationResolvers[0].resolve({
        ok: true, as_of: asOf, [`${mode}_used`]: 111,
        expirations: [{ expires_at: asOf + 120, units: 111 }],
      });
      await new Promise(resolve => setTimeout(resolve, 0));
    }, { asOf: AS_OF, mode: firstMode });
    await expect(page.locator(`${BODY} tbody tr td:last-child`)).toHaveText(["−222"]);
    await expect(page.locator(BODY)).not.toContainText("−111");
    await expect(page.locator(TITLE)).toHaveText(secondMode === "hourly" ? "Hourly drop-offs" : "24-hour drop-offs");
    await expect(page.locator(second)).toHaveAttribute("aria-expanded", "true");
    await expect(page.locator(first)).toHaveAttribute("aria-expanded", "false");
  });
}

test("hourly controls restore focus and coordinate with the other status popovers", async ({ page }) => {
  await loadTraffic(page);
  const hour = page.locator(HOUR);
  const popup = page.locator(POPUP);
  await hour.focus();
  await page.keyboard.press("Enter");
  await expect(popup).toBeVisible();
  await expect(page.locator(CLOSE)).toBeFocused();
  await expect(hour).toHaveAttribute("aria-expanded", "true");
  await page.keyboard.press("Escape");
  await expect(popup).toBeHidden();
  await expect(hour).toHaveAttribute("aria-expanded", "false");
  await expect(hour).toBeFocused();

  await page.keyboard.press("Space");
  await expect(popup).toBeVisible();
  await page.locator(CLOSE).click();
  await expect(popup).toBeHidden();
  await expect(hour).toBeFocused();
  await hour.click();
  await expect(popup).toBeVisible();
  await hour.click();
  await expect(popup).toBeHidden();

  await page.locator("#btn-sync-tasks").click();
  await expect(page.locator("#popover-sync-tasks")).toBeVisible();
  await hour.click();
  await expect(popup).toBeVisible();
  await expect(page.locator("#popover-sync-tasks")).toBeHidden();
  await expect(page.locator("#btn-sync-tasks")).toHaveAttribute("aria-expanded", "false");
  await page.locator("#btn-sync-tasks").click();
  await expect(popup).toBeHidden();
  await expect(hour).toHaveAttribute("aria-expanded", "false");
  await hour.click();
  await page.locator('.tab[data-tab="settings"]').click();
  await expect(popup).toBeHidden();
  await expect(hour).toHaveAttribute("aria-expanded", "false");
});

test("hourly reads refresh every five seconds only while open and keep the hourly argument", async ({ page }) => {
  await page.clock.install();
  await loadTraffic(page);
  await page.clock.fastForward(15000);
  expect(await expirationArgs(page)).toEqual([]);
  await page.locator(HOUR).click();
  await expect(page.locator(`${BODY} tbody tr`)).toHaveCount(3);
  expect(await expirationArgs(page)).toEqual([["hourly"]]);
  await page.evaluate((asOf) => {
    window.__setBridgeHandler("youtube_traffic_expirations", (mode) => ({
      ok: true, as_of: asOf, [`${mode || "daily"}_used`]: 9,
      expirations: [{ expires_at: asOf - 30, units: 9 }],
    }));
  }, AS_OF);
  await page.clock.fastForward(5000);
  await expect(page.locator(`${BODY} tbody tr`)).toHaveCount(1);
  await expect(page.locator(`${BODY} tbody tr td:first-child`)).toHaveText(["Today · 11:30 PM"]);
  await expect(page.locator(`${BODY} tbody tr td:last-child`)).toHaveText(["−9"]);
  expect(await expirationArgs(page)).toEqual([["hourly"], ["hourly"]]);
  await page.locator(CLOSE).click();
  await page.clock.fastForward(15000);
  expect(await expirationArgs(page)).toHaveLength(2);
  await page.locator(HOUR).click();
  await expect(page.locator(BODY)).toContainText("−9");
  expect(await expirationArgs(page)).toEqual([["hourly"], ["hourly"], ["hourly"]]);
});

test("hourly loading, failure, and empty history clear old data and retry on reopening", async ({ page }) => {
  await loadTraffic(page);
  await page.locator(DAY).click();
  await expect(page.locator(`${BODY} tbody tr td:last-child`)).toHaveText(["−31", "−29", "−27"]);
  await page.evaluate(() => {
    window.__setBridgeHandler("youtube_traffic_expirations", () => new Promise(resolve => {
      window.__finishHourly = resolve;
    }));
  });
  await page.locator(HOUR).click();
  await expect(page.locator(BODY)).toContainText("Loading drop-offs");
  await expect(page.locator(`${BODY} tbody tr`)).toHaveCount(0);
  await page.evaluate(() => window.__finishHourly({ ok: false, error: "Fixture read failed" }));
  await expect(page.locator(BODY)).toContainText("Could not load drop-offs");
  await expect(page.locator(`${BODY} tbody tr`)).toHaveCount(0);

  await page.locator(CLOSE).click();
  await page.evaluate((asOf) => {
    window.__setBridgeHandler("youtube_traffic_expirations", () => ({
      ok: true, as_of: asOf, hourly_used: 0, expirations: [],
    }));
  }, AS_OF);
  await page.locator(HOUR).click();
  await expect(page.locator(BODY)).toContainText("No requests are counted in the last hour.");
  await expect(page.locator(`${BODY} tbody tr`)).toHaveCount(0);
  await expect(page.getByRole("combobox", { name: "Group by", exact: true })).toBeHidden();
  expect(await expirationArgs(page)).toEqual([[], ["hourly"], ["hourly"]]);
});

for (const viewport of [{ width: 980, height: 720 }, { width: 640, height: 480 }]) {
  test(`a full hour stays inside a ${viewport.width} by ${viewport.height} window with scrollable minute rows`, async ({ page }) => {
    await page.setViewportSize(viewport);
    const expirations = Array.from({ length: 60 }, (_, index) => ({
      expires_at: Math.floor(AS_OF / 60) * 60 + (index + 1) * 60, units: 1,
    }));
    await loadTraffic(page, { expirations });
    await page.locator(HOUR).click();
    const body = page.locator(BODY);
    const rows = body.locator("tbody tr");
    await expect(rows).toHaveCount(60);
    await expect(rows.locator("td:last-child")).toHaveText(Array(60).fill("−1"));
    await expect(page.locator(CLOSE)).toBeInViewport();
    const bounds = await page.locator(POPUP).evaluate((popup) => {
      const rect = popup.getBoundingClientRect();
      const scroll = document.getElementById("gsb-traffic-expirations-body");
      return { top: rect.top, left: rect.left, right: rect.right, bottom: rect.bottom,
        visibleHeight: scroll.clientHeight, scrollHeight: scroll.scrollHeight,
        visibleWidth: scroll.clientWidth, scrollWidth: scroll.scrollWidth };
    });
    expect(bounds.top).toBeGreaterThanOrEqual(0);
    expect(bounds.left).toBeGreaterThanOrEqual(0);
    expect(bounds.right).toBeLessThanOrEqual(viewport.width);
    expect(bounds.bottom).toBeLessThanOrEqual(viewport.height);
    expect(bounds.visibleHeight).toBeGreaterThan(0);
    expect(bounds.scrollHeight).toBeGreaterThan(bounds.visibleHeight);
    expect(bounds.scrollWidth).toBeLessThanOrEqual(bounds.visibleWidth + 1);
    await body.evaluate(element => { element.scrollTop = element.scrollHeight; });
    await expect(rows.last()).toBeInViewport();
    await rows.last().click();
    await expect(page.locator(POPUP)).toBeVisible();
  });
}
