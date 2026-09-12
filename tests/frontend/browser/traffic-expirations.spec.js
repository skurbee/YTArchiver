const { test, expect, loadApp } = require("./fixtures");

test.use({ locale: "en-US", timezoneId: "America/Chicago" });

const AS_OF = Date.parse("2030-01-15T22:30:00-06:00") / 1000;
const EXPIRATIONS = [
  { expires_at: Date.parse("2030-01-15T23:45:00-06:00") / 1000, units: 1350 },
  { expires_at: Date.parse("2030-01-16T00:00:00-06:00") / 1000, units: 223 },
  { expires_at: Date.parse("2030-01-16T07:16:00-06:00") / 1000, units: 285 },
];
const BODY = "#gsb-traffic-expirations-body";
const POPUP = "#popover-traffic-expirations";
const BUTTON = "#gsb-traffic-daily";
const GROUP = "#gsb-traffic-expirations-group";

async function chooseGrouping(page, value) {
  const label = { "1": "1 minute", "10": "10 minutes", "30": "30 minutes", "60": "1 hour" }[value];
  await page.getByRole("combobox", { name: "Group by", exact: true }).click();
  const option = page.locator(POPUP).getByRole("option", { name: label, exact: true });
  await expect(option).toBeInViewport();
  await option.click();
  await expect(page.locator(GROUP)).toHaveValue(value);
}

async function loadTraffic(page, { expirations = EXPIRATIONS, configure, settings, waitFor } = {}) {
  await loadApp(page, {
    bridge: { settings, responses: {
      youtube_traffic_status: {
        ok: true, mode: "custom", paused: false,
        hourly_used: 100, hourly_limit: 500, daily_used: 1858, daily_limit: 6000,
      },
      youtube_traffic_expirations: {
        ok: true, as_of: AS_OF,
        daily_used: expirations.reduce((sum, entry) => sum + entry.units, 0),
        expirations,
      },
    } },
    configure,
    waitFor,
  });
  await expect(page.locator(BUTTON)).toBeVisible();
}

test("daily drop-offs show local times, midnight day labels, and counted units in order", async ({ page }) => {
  await loadTraffic(page);
  await expect(page.locator(BUTTON)).toHaveAttribute("aria-haspopup", "dialog");
  await expect(page.locator(BUTTON)).toHaveAttribute("aria-controls", "popover-traffic-expirations");

  await page.locator(BUTTON).click();

  await expect(page.locator(POPUP)).toBeVisible();
  await expect(page.locator(POPUP)).toHaveAttribute("role", "dialog");
  await expect(page.locator("#gsb-traffic-expirations-summary, #gsb-traffic-expirations-next, #gsb-traffic-expirations-note")).toHaveCount(0);
  await expect(page.locator(POPUP)).not.toContainText(/currently counted|Next drop-off|Local time|Usage falls off|New requests|Other limits/);
  await expect(page.locator(POPUP)).not.toHaveAttribute("aria-describedby");
  const rows = page.locator(`${BODY} tbody tr`);
  await expect(rows).toHaveCount(3);
  await expect(rows.nth(0)).toContainText(/Today\s*·\s*11:45\s*PM/);
  await expect(rows.nth(0)).toContainText("−1,350");
  await expect(rows.nth(1)).toContainText(/Tomorrow\s*·\s*12:00\s*AM/);
  await expect(rows.nth(1)).toContainText("−223");
  await expect(rows.nth(2)).toContainText(/Tomorrow\s*·\s*7:16\s*AM/);
  await expect(rows.nth(2)).toContainText("−285");
});

test("daily drop-offs support keyboard and close controls alongside other status popovers", async ({ page }) => {
  await page.clock.install();
  await loadTraffic(page);
  const button = page.locator(BUTTON);
  const popup = page.locator(POPUP);

  await button.focus();
  await page.keyboard.press("Enter");
  await expect(popup).toBeVisible();
  await expect(button).toHaveAttribute("aria-expanded", "true");
  await page.keyboard.press("Escape");
  await expect(popup).toBeHidden();
  await expect(button).toHaveAttribute("aria-expanded", "false");
  await expect(button).toBeFocused();

  await page.keyboard.press("Space");
  await expect(popup).toBeVisible();
  await page.locator("#gsb-traffic-expirations-close").click();
  await expect(popup).toBeHidden();
  await expect(button).toHaveAttribute("aria-expanded", "false");

  await button.click();
  await expect(popup).toBeVisible();
  await button.click();
  await expect(popup).toBeHidden();

  await button.click();
  await page.locator('.tab[data-tab="settings"]').click();
  await expect(popup).toBeHidden();
  await expect(button).toHaveAttribute("aria-expanded", "false");

  await page.locator('.tab[data-tab="download"]').click();
  await page.locator("#btn-sync-tasks").click();
  await expect(page.locator("#popover-sync-tasks")).toBeVisible();
  await button.click();
  await expect(popup).toBeVisible();
  await expect(page.locator("#popover-sync-tasks")).toBeHidden();
  await expect(page.locator("#btn-sync-tasks")).toHaveAttribute("aria-expanded", "false");
  await page.locator("#btn-sync-tasks").click();
  await expect(popup).toBeHidden();
  await expect(button).toHaveAttribute("aria-expanded", "false");

  // Session errors only count new log lines after the startup history seed.
  await page.clock.fastForward(3000);
  await page.evaluate(() => {
    const line = document.createElement("div");
    line.className = "log-line";
    const message = document.createElement("span");
    message.className = "t-error_detail";
    message.textContent = "Fixture operation failed";
    line.append(message);
    document.getElementById("main-log").append(line);
  });
  await expect(page.locator("#gsb-errors")).toBeVisible();
  await page.locator("#gsb-errors").click();
  await expect(page.locator("#popover-session-errors")).toBeVisible();
  await button.click();
  await expect(popup).toBeVisible();
  await expect(page.locator("#popover-session-errors")).toBeHidden();
  await expect(page.locator("#gsb-errors")).toHaveAttribute("aria-expanded", "false");
  await page.locator("#gsb-errors").click();
  await expect(popup).toBeHidden();
  await expect(button).toHaveAttribute("aria-expanded", "false");
});

test("a full day of minute rows scrolls inside the popup at the minimum window size", async ({ page }) => {
  await page.setViewportSize({ width: 980, height: 720 });
  const expirations = Array.from({ length: 1440 }, (_, index) => ({
    expires_at: AS_OF + (index + 1) * 60, units: 1,
  }));
  await loadTraffic(page, { expirations });
  await page.locator(BUTTON).click();
  const body = page.locator(BODY);
  const rows = body.locator("tbody tr");
  await expect(rows).toHaveCount(1440);

  const bounds = await page.locator(POPUP).evaluate((popup) => {
    const rect = popup.getBoundingClientRect();
    const scroll = document.getElementById("gsb-traffic-expirations-body");
    return {
      top: rect.top, right: rect.right, bottom: rect.bottom, left: rect.left,
      clientHeight: scroll.clientHeight, scrollHeight: scroll.scrollHeight,
      clientWidth: scroll.clientWidth, scrollWidth: scroll.scrollWidth,
    };
  });
  expect(bounds.top).toBeGreaterThanOrEqual(0);
  expect(bounds.left).toBeGreaterThanOrEqual(0);
  expect(bounds.right).toBeLessThanOrEqual(980);
  expect(bounds.bottom).toBeLessThanOrEqual(720);
  expect(bounds.clientHeight).toBeGreaterThan(0);
  expect(bounds.scrollHeight).toBeGreaterThan(bounds.clientHeight);
  expect(bounds.scrollWidth).toBeLessThanOrEqual(bounds.clientWidth + 1);

  await body.evaluate((element) => { element.scrollTop = element.scrollHeight; });
  await expect(rows.last()).toBeInViewport();
  await rows.last().click();
  await expect(page.locator(POPUP)).toBeVisible();
});

test("the open schedule refreshes every five seconds and stops reading when closed", async ({ page }) => {
  await page.clock.install();
  await loadTraffic(page);
  const readCount = () => page.evaluate(() => window.__bridgeCallsFor("youtube_traffic_expirations").length);
  await page.clock.fastForward(15_000);
  expect(await readCount()).toBe(0);

  await page.locator(BUTTON).click();
  await expect(page.locator(`${BODY} tbody tr`)).toHaveCount(3);
  expect(await readCount()).toBe(1);
  await page.evaluate((asOf) => {
    window.__setBridgeHandler("youtube_traffic_expirations", () => ({
      ok: true, as_of: asOf, daily_used: 73,
      expirations: [{ expires_at: asOf + 3600, units: 73 }],
    }));
  }, AS_OF);
  await page.clock.fastForward(5000);
  await expect(page.locator(`${BODY} tbody tr`)).toHaveCount(1);
  await expect(page.locator(BODY)).toContainText("−73");
  expect(await readCount()).toBe(2);

  await page.locator("#gsb-traffic-expirations-close").click();
  await page.clock.fastForward(15_000);
  expect(await readCount()).toBe(2);
  await page.locator(BUTTON).click();
  await expect(page.locator(BODY)).toContainText("−73");
  expect(await readCount()).toBe(3);
});

test("loading, failed reads, and empty history stay clear and reopening retries", async ({ page }) => {
  await loadTraffic(page, { configure: () => {
    window.__setBridgeHandler("youtube_traffic_expirations", () => new Promise(resolve => {
      window.__finishExpirations = resolve;
    }));
  } });
  await page.locator(BUTTON).click();
  await expect(page.locator(BODY)).toContainText("Loading drop-offs");
  await page.evaluate(() => window.__finishExpirations({ ok: false, error: "Fixture read failed" }));
  await expect(page.locator(BODY)).toContainText("Could not load drop-offs");
  await expect(page.locator(`${BODY} tbody tr`)).toHaveCount(0);

  await page.locator("#gsb-traffic-expirations-close").click();
  await page.evaluate((asOf) => {
    window.__setBridgeHandler("youtube_traffic_expirations", () => ({
      ok: true, as_of: asOf, daily_used: 0, expirations: [],
    }));
  }, AS_OF);
  await page.locator(BUTTON).click();
  await expect(page.locator(BODY)).toContainText("No requests are counted in the last 24 hours.");
  await expect(page.locator(`${BODY} tbody tr`)).toHaveCount(0);
  expect(await page.evaluate(() => window.__bridgeCallsFor("youtube_traffic_expirations").length)).toBe(2);

  // The final option must remain clickable even when an empty popup is short.
  await chooseGrouping(page, "60");
  const groupTrigger = page.getByRole("combobox", { name: "Group by", exact: true });
  await groupTrigger.click();
  await expect(groupTrigger).toHaveAttribute("aria-expanded", "true");
  await page.keyboard.press("Escape");
  await expect(groupTrigger).toHaveAttribute("aria-expanded", "false");
  await expect(groupTrigger).toBeFocused();
  await expect(page.locator(POPUP)).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(page.locator(POPUP)).toBeHidden();
  await expect(page.locator(BUTTON)).toBeFocused();
});

test("an old pending response cannot overwrite a newly reopened schedule", async ({ page }) => {
  await loadTraffic(page, { configure: () => {
    window.__expirationResolvers = [];
    window.__setBridgeHandler("youtube_traffic_expirations", () => new Promise(resolve => {
      window.__expirationResolvers.push(resolve);
    }));
  } });
  await page.locator(BUTTON).click();
  await expect(page.locator(BODY)).toContainText("Loading drop-offs");
  await page.locator("#gsb-traffic-expirations-close").click();
  await page.locator(BUTTON).click();
  await expect.poll(() => page.evaluate(() => window.__expirationResolvers.length)).toBe(2);

  await page.evaluate((asOf) => window.__expirationResolvers[1]({
    ok: true, as_of: asOf, daily_used: 222,
    expirations: [{ expires_at: asOf + 3600, units: 222 }],
  }), AS_OF);
  await expect(page.locator(BODY)).toContainText("−222");
  await page.evaluate(async (asOf) => {
    window.__expirationResolvers[0]({
      ok: true, as_of: asOf, daily_used: 111,
      expirations: [{ expires_at: asOf + 3600, units: 111 }],
    });
    await new Promise(resolve => setTimeout(resolve, 0));
  }, AS_OF);
  await expect(page.locator(BODY)).toContainText("−222");
  await expect(page.locator(BODY)).not.toContainText("−111");
});

test("grouping choices combine charges into ordered local clock blocks and restore minute detail", async ({ page }) => {
  const expirations = [
    ["22:31", 3], ["22:39", 7], ["22:40", 11],
    ["22:59", 13], ["23:00", 17], ["23:04", 19],
  ].map(([time, units]) => ({
    expires_at: Date.parse(`2030-01-15T${time}:00-06:00`) / 1000, units,
  }));
  await loadTraffic(page, { expirations });
  await page.locator(BUTTON).click();
  const group = page.locator(GROUP);
  const rows = page.locator(`${BODY} tbody tr`);
  await expect(group).toHaveValue("1");
  await expect(group.locator("option")).toHaveText(["1 minute", "10 minutes", "30 minutes", "1 hour"]);
  await expect(rows).toHaveCount(6);

  const groups = [
    ["10", ["Today · 10:30 PM–10:39 PM", "Today · 10:40 PM–10:49 PM",
      "Today · 10:50 PM–10:59 PM", "Today · 11:00 PM–11:09 PM"], ["−10", "−11", "−13", "−36"]],
    ["30", ["Today · 10:30 PM–10:59 PM", "Today · 11:00 PM–11:29 PM"], ["−34", "−36"]],
    ["60", ["Today · 10:00 PM–10:59 PM", "Today · 11:00 PM–11:59 PM"], ["−34", "−36"]],
    ["1", ["Today · 10:31 PM", "Today · 10:39 PM", "Today · 10:40 PM",
      "Today · 10:59 PM", "Today · 11:00 PM", "Today · 11:04 PM"], ["−3", "−7", "−11", "−13", "−17", "−19"]],
  ];
  for (const [value, times, charges] of groups) {
    await chooseGrouping(page, value);
    await expect(page.locator(POPUP)).toBeVisible();
    await expect(rows).toHaveCount(times.length);
    await expect(rows.locator("td:first-child")).toHaveText(times);
    await expect(rows.locator("td:last-child")).toHaveText(charges);
  }
  expect(await page.evaluate(() => window.__bridgeCallsFor("youtube_traffic_expirations").length)).toBe(1);
});

test("ten-minute, half-hour, and hourly groups keep midnight boundary charges on their own day", async ({ page }) => {
  const expirations = [
    ["15T23:29", 1], ["15T23:30", 2], ["15T23:49", 3],
    ["15T23:50", 5], ["15T23:59", 7], ["16T00:00", 11],
    ["16T00:09", 13], ["16T00:10", 17], ["16T00:29", 19], ["16T00:30", 23],
  ].map(([time, units]) => ({
    expires_at: Date.parse(`2030-01-${time}:00-06:00`) / 1000, units,
  }));
  await loadTraffic(page, { expirations });
  await page.locator(BUTTON).click();
  const rows = page.locator(`${BODY} tbody tr`);
  const groups = [
    ["10", ["Today · 11:20 PM–11:29 PM", "Today · 11:30 PM–11:39 PM",
      "Today · 11:40 PM–11:49 PM", "Today · 11:50 PM–11:59 PM",
      "Tomorrow · 12:00 AM–12:09 AM", "Tomorrow · 12:10 AM–12:19 AM",
      "Tomorrow · 12:20 AM–12:29 AM", "Tomorrow · 12:30 AM–12:39 AM"],
    ["−1", "−2", "−3", "−12", "−24", "−17", "−19", "−23"]],
    ["30", ["Today · 11:00 PM–11:29 PM", "Today · 11:30 PM–11:59 PM",
      "Tomorrow · 12:00 AM–12:29 AM", "Tomorrow · 12:30 AM–12:59 AM"],
    ["−1", "−17", "−60", "−23"]],
    ["60", ["Today · 11:00 PM–11:59 PM", "Tomorrow · 12:00 AM–12:59 AM"], ["−18", "−83"]],
  ];
  for (const [value, times, charges] of groups) {
    await chooseGrouping(page, value);
    await expect(rows).toHaveCount(times.length);
    await expect(rows.locator("td:first-child")).toHaveText(times);
    await expect(rows.locator("td:last-child")).toHaveText(charges);
  }
});

test("grouping preference survives fresh results, reopening, and a fresh browser context", async ({ page, browser }) => {
  await page.clock.install();
  await loadTraffic(page);
  await page.locator(BUTTON).click();
  await chooseGrouping(page, "30");
  await expect(page.locator(`${BODY} tbody tr`).first()).toContainText("Today · 11:30 PM–11:59 PM");
  await page.evaluate((asOf) => {
    window.__setBridgeHandler("youtube_traffic_expirations", () => ({
      ok: true, as_of: asOf, daily_used: 30,
      expirations: [
        { expires_at: asOf + 31 * 60, units: 14 },
        { expires_at: asOf + 39 * 60, units: 16 },
      ],
    }));
  }, AS_OF);
  await page.clock.fastForward(5000);
  await expect(page.locator(GROUP)).toHaveValue("30");
  await expect(page.locator(`${BODY} tbody tr`)).toHaveCount(1);
  await expect(page.locator(BODY)).toContainText("Today · 11:00 PM–11:29 PM");
  await expect(page.locator(BODY)).toContainText("−30");

  await page.locator("#gsb-traffic-expirations-close").click();
  await page.locator(BUTTON).click();
  await expect(page.locator(GROUP)).toHaveValue("30");
  await expect(page.locator(`${BODY} tbody tr`)).toHaveCount(1);
  await expect(page.locator(BODY)).toContainText("−30");

  const saved = await page.evaluate(() => window.__bridgeCallsFor("settings_save")
    .map(call => call.args[0]).find(values => Object.hasOwn(values, "traffic_expiration_group_minutes")));
  expect(saved).toEqual({ traffic_expiration_group_minutes: 30 });
  const restarted = await browser.newContext({ locale: "en-US", timezoneId: "America/Chicago" });
  try {
    // Only the captured native setting crosses this boundary; browser storage does not.
    expect((await restarted.storageState()).origins).toEqual([]);
    const freshPage = await restarted.newPage();
    await loadTraffic(freshPage, { settings: saved });
    await freshPage.locator(BUTTON).click();
    await expect(freshPage.locator(GROUP)).toHaveValue("30");
    await expect(freshPage.locator(`${BODY} tbody tr`)).toHaveCount(3);
    await expect(freshPage.locator(`${BODY} tbody tr`).first()).toContainText("Today · 11:30 PM–11:59 PM");
    expect(await freshPage.evaluate(() => window.__bridgeCallsFor("settings_save"))).toEqual([]);
    expect(await freshPage.evaluate(() => window.__unexpectedBridgeCalls)).toEqual([]);
  } finally {
    await restarted.close();
  }
});

for (const [minutes, firstTime] of [
  [10, "Today · 11:40 PM–11:49 PM"],
  [30, "Today · 11:30 PM–11:59 PM"],
  [60, "Today · 11:00 PM–11:59 PM"],
]) {
  test(`the saved ${minutes}-minute grouping hydrates without writing settings`, async ({ page }) => {
    await loadTraffic(page, { settings: { traffic_expiration_group_minutes: minutes } });
    await page.locator(BUTTON).click();
    await expect(page.locator(GROUP)).toHaveValue(String(minutes));
    const rows = page.locator(`${BODY} tbody tr`);
    await expect(rows.first().locator("td:first-child")).toHaveText(firstTime);
    await expect(rows.locator("td:last-child")).toHaveText(["−1,350", "−223", "−285"]);
    expect(await page.evaluate(() => window.__bridgeCallsFor("settings_save"))).toEqual([]);
  });
}

test("a delayed initial settings read cannot replace a newer grouping choice", async ({ page }) => {
  await loadTraffic(page, {
    waitFor: "handlers",
    settings: { traffic_expiration_group_minutes: 10 },
    configure: () => {
      const oldSettings = window.__fixtureDefaultResult("settings_load");
      const pending = new Promise(resolve => {
        window.__finishTrafficSettings = () => resolve(oldSettings);
      });
      window.__setBridgeHandler("settings_load", () => pending);
    },
  });
  await page.locator(BUTTON).click();
  await chooseGrouping(page, "60");
  await expect.poll(() => page.evaluate(() => window.YT.preferences.isSaving())).toBe(false);
  expect(await page.evaluate(() => window.__bridgeCallsFor("settings_save").map(call => call.args)))
    .toEqual([[{ traffic_expiration_group_minutes: 60 }]]);
  await page.evaluate(async () => {
    window.__finishTrafficSettings();
    await window.YT.settingsReady;
  });
  await expect(page.locator(GROUP)).toHaveValue("60");
  await expect(page.locator(`${BODY} tbody tr`).first()).toContainText("Today · 11:00 PM–11:59 PM");
  expect(await page.evaluate(() => window.YT.preferences.snapshot().traffic_expiration_group_minutes)).toBe(60);
});

test("a failed early grouping save recovers the saved value when the initial read arrives", async ({ page }) => {
  await loadTraffic(page, {
    waitFor: "handlers",
    settings: { traffic_expiration_group_minutes: 30 },
    configure: () => {
      const oldSettings = window.__fixtureDefaultResult("settings_load");
      const pending = new Promise(resolve => {
        window.__finishTrafficSettings = () => resolve(oldSettings);
      });
      window.__setBridgeHandler("settings_load", () => pending);
      window.__setBridgeHandler("settings_save", () => new Promise(resolve => {
        window.__finishGroupingSave = resolve;
      }));
    },
  });
  await page.locator(BUTTON).click();
  await chooseGrouping(page, "60");
  await expect.poll(() => page.evaluate(() => typeof window.__finishGroupingSave)).toBe("function");
  await page.evaluate(() => window.__finishGroupingSave({ ok: false, error: "Fixture early grouping save failed" }));
  await expect(page.locator("#toast-root")).toContainText("Could not save grouping. Please try again.");
  await page.evaluate(async () => {
    window.__finishTrafficSettings();
    await window.YT.settingsReady;
  });
  await expect(page.locator(GROUP)).toHaveValue("30");
  await expect(page.getByRole("combobox", { name: "Group by", exact: true })).toContainText("30 minutes");
  await expect(page.locator(`${BODY} tbody tr`).first()).toContainText("Today · 11:30 PM–11:59 PM");
  expect(await page.evaluate(() => window.__bridgeCallsFor("settings_save").map(call => call.args)))
    .toEqual([[{ traffic_expiration_group_minutes: 60 }]]);
});

test("a failed grouping save restores the saved grouping and schedule and explains the failure", async ({ page }) => {
  await loadTraffic(page, { settings: { traffic_expiration_group_minutes: 30 } });
  await page.locator(BUTTON).click();
  await page.evaluate(() => {
    window.__setBridgeHandler("settings_save", () => new Promise(resolve => {
      window.__finishGroupingSave = resolve;
    }));
  });
  await chooseGrouping(page, "60");
  await expect(page.locator(`${BODY} tbody tr`).first()).toContainText("Today · 11:00 PM–11:59 PM");
  await expect.poll(() => page.evaluate(() => typeof window.__finishGroupingSave)).toBe("function");
  await page.evaluate(() => window.__finishGroupingSave({ ok: false, error: "Fixture grouping save failed" }));
  await expect(page.locator(GROUP)).toHaveValue("30");
  await expect(page.getByRole("combobox", { name: "Group by", exact: true })).toContainText("30 minutes");
  await expect(page.locator(`${BODY} tbody tr`).first()).toContainText("Today · 11:30 PM–11:59 PM");
  await expect(page.locator(`${BODY} tbody tr td:last-child`)).toHaveText(["−1,350", "−223", "−285"]);
  await expect(page.locator("#toast-root")).toContainText("Could not save grouping. Please try again.");
  expect(await page.evaluate(() => window.__bridgeCallsFor("settings_save").map(call => call.args)))
    .toEqual([[{ traffic_expiration_group_minutes: 60 }]]);
});

test("grouping a full day shortens the list without dropping charges", async ({ page }) => {
  await page.setViewportSize({ width: 980, height: 720 });
  const expirations = Array.from({ length: 1440 }, (_, index) => ({
    expires_at: AS_OF + (index + 1) * 60, units: 1,
  }));
  await loadTraffic(page, { expirations });
  await page.locator(BUTTON).click();
  const rows = page.locator(`${BODY} tbody tr`);
  await expect(rows).toHaveCount(1440);

  // A rolling day begins and ends partway through a clock-aligned group.
  for (const [value, count] of [["10", 145], ["30", 49], ["60", 25]]) {
    await chooseGrouping(page, value);
    await expect(rows).toHaveCount(count);
    const total = await rows.locator("td:last-child").evaluateAll((cells) =>
      cells.reduce((sum, cell) => sum + Number(cell.textContent.replace(/[^0-9]/g, "")), 0));
    expect(total).toBe(1440);
    await expect(page.getByRole("combobox", { name: "Group by", exact: true })).toBeInViewport();
    await expect(page.locator("#gsb-traffic-expirations-close")).toBeInViewport();
    const width = await page.locator(BODY).evaluate((element) => ({
      scroll: element.scrollWidth, visible: element.clientWidth,
    }));
    expect(width.scroll).toBeLessThanOrEqual(width.visible + 1);
  }
});
