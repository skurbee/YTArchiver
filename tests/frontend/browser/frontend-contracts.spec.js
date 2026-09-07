const { test, expect, loadApp, APP_URL, installBridgeStub } = require("./fixtures");

test("failed and malformed channel reads stay distinct from an empty subscription list", async ({ page }) => {
  await loadApp(page);
  const result = await page.evaluate(async () => {
    const errors = [];
    for (const reply of [{ ok: false, error: "Channel read failed" }, {}, null]) {
      window.__setBridgeHandler("get_subs_channels", () => reply);
      try { await window.YT.util.loadSubsChannels(); }
      catch (error) { errors.push(error.message); }
    }
    window.__setBridgeHandler("get_subs_channels", () => [[], "No channels"]);
    return { errors, empty: await window.YT.util.loadSubsChannels() };
  });
  expect(result.errors).toHaveLength(3);
  expect(result.errors[0]).toBe("Channel read failed");
  expect(result.empty).toEqual([]);
});

test("failed settings reload preserves saved controls and navigation", async ({ page }) => {
  await loadApp(page, { bridge: { settings: { legacy_subs_tab: true, whisper_model: "medium" } } });
  await page.evaluate(() => window.__setBridgeHandler("settings_load", () => ({
    ok: false, error: "Settings read failed",
  })));
  await page.locator('.tab[data-tab="settings"]').click();
  await expect(page.locator("#settings-autosave-note")).toContainText("Settings unavailable");
  await expect(page.locator("#settings-whisper-model")).toHaveValue("medium");
  await expect(page.locator('.tab[data-tab="subs"]')).toBeVisible();
  await page.evaluate(() => window.__setBridgeHandler("settings_load", () =>
    window.__fixtureDefaultResult("settings_load")));
  await page.locator('.tab[data-tab="settings"]').click();
  await expect.poll(() => page.evaluate(() =>
    window.YT.preferences.snapshot().whisper_model)).toBe("medium");
});

test("a held full snapshot cannot overwrite an acknowledged newer preference", async ({ page }) => {
  await loadApp(page, { bridge: { settings: { transcript_font_size: 14 } } });
  const result = await page.evaluate(async () => {
    const old = window.__fixtureDefaultResult("settings_load");
    let release;
    window.__setBridgeHandler("settings_load", () => new Promise(resolve => { release = resolve; }));
    const read = window.YT.preferences.load({ refresh: true });
    await window.YT.preferences.save({ transcript_font_size: 18 });
    release(old);
    return (await read).transcript_font_size;
  });
  expect(result).toBe(18);
});

test("failed queued saves restore the last acknowledged value, never another failed write", async ({ page }) => {
  await loadApp(page, { bridge: { settings: { transcript_font_size: 14 } } });
  const result = await page.evaluate(async () => {
    let release;
    let started = 0;
    window.__setBridgeHandler("settings_save", () => {
      started++;
      return started === 1
        ? new Promise(resolve => { release = resolve; })
        : { ok: false, error: "Second save failed" };
    });
    const one = window.YT.preferences.save({ transcript_font_size: 18 });
    const two = window.YT.preferences.save({ transcript_font_size: 20 });
    const results = Promise.allSettled([one, two]);
    await Promise.resolve();
    const overlap = started;
    release({ ok: false, error: "First save failed" });
    const outcomes = await results;
    return { overlap, statuses: outcomes.map(value => value.status),
      value: window.YT.preferences.snapshot().transcript_font_size };
  });
  expect(result).toEqual({ overlap: 1, statuses: ["rejected", "rejected"], value: 14 });
});

for (const acknowledged of [true, false]) {
  test(`a read started during a save cannot replace its ${acknowledged ? "acknowledged" : "rolled-back"} result`, async ({ page }) => {
    await loadApp(page, { bridge: { settings: { transcript_font_size: 14 } } });
    const result = await page.evaluate(async acknowledged => {
      const old = window.__fixtureDefaultResult("settings_load");
      let releaseSave;
      let releaseRead;
      window.__setBridgeHandler("settings_save", () => new Promise(resolve => { releaseSave = resolve; }));
      window.__setBridgeHandler("settings_load", () => new Promise(resolve => { releaseRead = resolve; }));
      const save = window.YT.preferences.save({ transcript_font_size: 18 });
      const settled = save.catch(() => {});
      const read = window.YT.preferences.load({ refresh: true });
      await Promise.resolve();
      releaseSave(acknowledged ? { ok: true } : { ok: false, error: "Held save failed" });
      await settled;
      releaseRead(old);
      const reply = await read;
      return { read: reply.transcript_font_size,
        snapshot: window.YT.preferences.snapshot().transcript_font_size };
    }, acknowledged);
    const expected = acknowledged ? 18 : 14;
    expect(result).toEqual({ read: expected, snapshot: expected });
  });
}

test("Watch hydration restores preferences without writing them back", async ({ page }) => {
  await page.clock.install();
  await loadApp(page, {
    bridge: { settings: { transcript_font_size: 19, transcript_pane_width: 610,
      caption_overlay_size: "small", caption_overlay_bg: "none" } },
    configure: () => {
      localStorage.setItem("ytarchiver_tx_font_px", "15");
      localStorage.setItem("ytarchiver_tx_pane_width", "480");
    },
  });
  await page.clock.fastForward(500);
  const result = await page.evaluate(() => ({
    font: document.documentElement.style.getPropertyValue("--watch-transcript-fz"),
    width: document.documentElement.style.getPropertyValue("--watch-tx-width"),
    saved: window.__bridgeCallsFor("settings_save"),
  }));
  expect(result).toEqual({ font: "19.0px", width: "610px", saved: [] });
});

test("a first settings read establishes rollback values for an early failed save", async ({ page }) => {
  await loadApp(page, {
    waitFor: "handlers",
    configure: () => window.__setBridgeHandler("settings_load", () => new Promise(resolve => {
      window.__releaseFirstRead = () => resolve({ output_dir: "C:\\FixtureArchive", transcript_font_size: 14 });
    })),
  });
  const result = await page.evaluate(async () => {
    let releaseSave;
    window.__setBridgeHandler("settings_save", () => new Promise(resolve => { releaseSave = resolve; }));
    const write = window.YT.preferences.save({ transcript_font_size: 18 }).catch(() => {});
    await Promise.resolve();
    window.__releaseFirstRead();
    await window.YT.settingsReady;
    const pending = window.YT.preferences.snapshot().transcript_font_size;
    releaseSave({ ok: false, error: "Early save failed" });
    await write;
    return { pending, restored: window.YT.preferences.snapshot().transcript_font_size };
  });
  expect(result).toEqual({ pending: 18, restored: 14 });
});

test("late Watch hydration preserves a font edit made before its debounce saves", async ({ page }) => {
  await loadApp(page, {
    waitFor: "handlers",
    configure: () => window.__setBridgeHandler("settings_load", () => new Promise(resolve => {
      window.__releasePreferences = () => resolve({
        output_dir: "C:\\FixtureArchive", transcript_font_size: 19,
      });
    })),
  });
  await page.evaluate(() => document.getElementById("btn-tx-font-up").click());
  const edited = await page.evaluate(() =>
    document.documentElement.style.getPropertyValue("--watch-transcript-fz"));
  await page.evaluate(async () => {
    window.__releasePreferences();
    await window.YT.settingsReady;
  });
  expect(await page.evaluate(() =>
    document.documentElement.style.getPropertyValue("--watch-transcript-fz"))).toBe(edited);
});

test("an incomplete initializer retries and disposes listeners from a rejected attempt", async ({ page }) => {
  await loadApp(page);
  const result = await page.evaluate(async () => {
    const button = document.createElement("button");
    const util = window.YT.util;
    let clicks = 0;
    try {
      await util.initialize("retry-fixture", async scope => {
        scope.listen(button, "click", () => clicks++);
        await Promise.resolve();
        throw new Error("Partial setup failed");
      });
    } catch {}
    const failed = util.initializationState("retry-fixture");
    button.click();
    const failedClicks = clicks;
    let bindings = 0;
    const setup = scope => { bindings++; scope.listen(button, "click", () => clicks++); };
    await Promise.all([util.initialize("retry-fixture", setup), util.initialize("retry-fixture", setup)]);
    button.click();
    return { failed, failedClicks, bindings, clicks, ready: util.initializationState("retry-fixture") };
  });
  expect(result).toEqual({ failed: "failed", failedClicks: 0, bindings: 1, clicks: 1, ready: "ready" });
});

test("Watch and Metadata do not claim initialization before prerequisites exist", async ({ page }) => {
  await page.route("**/app.js?*", route => route.fulfill({ contentType: "application/javascript", body: "" }));
  await page.addInitScript(installBridgeStub);
  await page.goto(APP_URL);
  const result = await page.evaluate(async () => {
    const state = window._browseState;
    delete window._browseState;
    let watchError = "";
    try { await window.initWatchActions(); } catch (error) { watchError = error.message; }
    const incompleteWatch = !!window._watchActionsInited;
    window._browseState = state;
    await window.initWatchActions();
    const table = document.getElementById("metadata-table");
    table.id = "held-metadata-table";
    let metadataError = "";
    try { await window.initMetadataTab(); } catch (error) { metadataError = error.message; }
    const incompleteMetadata = !!window._metadataTabInited;
    table.id = "metadata-table";
    await window.initMetadataTab();
    return { watchError, metadataError, incompleteWatch, incompleteMetadata,
      watch: window._watchActionsInited, metadata: window._metadataTabInited };
  });
  expect(result.watchError).toContain("Browse state");
  expect(result.metadataError).toContain("Metadata table");
  expect(result).toMatchObject({ incompleteWatch: false, incompleteMetadata: false, watch: true, metadata: true });
});

test("strict fixtures reject and record an unspecified endpoint", async ({ page }) => {
  await loadApp(page);
  const error = await page.evaluate(async () => {
    let error;
    try { await window.YT.bridge.bridgeCall("unknown_fixture_endpoint"); }
    catch (failure) { error = failure.message; }
    return error;
  });
  await page.reload();
  const calls = await page.evaluate(() => {
    // This test intentionally exercises the audit failure and consumes exactly it.
    sessionStorage.removeItem("yt_fixture_unexpected");
    return window.__unexpectedBridgeCalls.splice(0);
  });
  expect({ error, calls }).toEqual({
    error: "No browser fixture registered for unknown_fixture_endpoint",
    calls: ["unknown_fixture_endpoint"],
  });
});

test("reentrant initialization shares its pending promise and never repeats setup", async ({ page }) => {
  await loadApp(page);
  const result = await page.evaluate(async () => {
    const util = window.YT.util;
    let nested;
    let repeat = 0;
    let release;
    const outer = util.initialize("reentrant-fixture", () => {
      nested = util.initialize("reentrant-fixture", () => repeat++);
      return new Promise(resolve => { release = resolve; });
    });
    const same = nested === outer;
    const pending = util.initializationState("reentrant-fixture");
    release("complete");
    return { same, pending, values: await Promise.all([outer, nested]), repeat,
      ready: util.initializationState("reentrant-fixture") };
  });
  expect(result).toEqual({ same: true, pending: "initializing", values: ["complete", "complete"],
    repeat: 0, ready: "ready" });
});

test("a settings reload during an unacknowledged save preserves the rollback baseline", async ({ page }) => {
  await loadApp(page);
  await page.locator('.tab[data-tab="settings"]').click();
  await page.locator("#settings-background-checks > summary").click();
  const input = page.locator("#settings-disk-staleness");
  await expect(input).toHaveValue("24");
  await page.evaluate(() => window.__setBridgeHandler("settings_save", () => new Promise(resolve => {
    window.__releasePendingSave = resolve;
  })));
  await input.fill("48");
  await input.blur();
  await page.waitForFunction(() => typeof window.__releasePendingSave === "function");
  const reads = await page.evaluate(() => window.__bridgeCallsFor("settings_load").length);
  await page.locator('.tab[data-tab="settings"]').click();
  await expect.poll(() => page.evaluate(() => window.__bridgeCallsFor("settings_load").length)).toBeGreaterThan(reads);
  await expect(input).toHaveAttribute("data-saved-value", "24");
  await page.evaluate(() => window.__releasePendingSave({ ok: false, error: "Held save failed" }));
  await expect(input).toHaveValue("24");
  await expect(input).toHaveAttribute("data-saved-value", "24");
  await page.evaluate(() => window.__setBridgeHandler("settings_save", () => ({ ok: false, error: "Next save failed" })));
  await input.fill("72");
  await input.blur();
  await expect(input).toHaveValue("24");
});

test("a settings snapshot arriving during a draft does not overwrite its value or rollback baseline", async ({ page }) => {
  await loadApp(page);
  await page.locator('.tab[data-tab="settings"]').click();
  await page.locator("#settings-background-checks > summary").click();
  const input = page.locator("#settings-disk-staleness");
  await expect(input).toHaveValue("24");
  await page.evaluate(() => window.__setBridgeHandler("settings_load", () => new Promise(resolve => {
    window.__releaseDraftRead = () => resolve({
      ...window.__fixtureDefaultResult("settings_load"), disk_scan_staleness_hours: 72,
    });
  })));
  await page.locator('.tab[data-tab="settings"]').click();
  await page.waitForFunction(() => typeof window.__releaseDraftRead === "function");
  await input.fill("48");
  await page.evaluate(() => window.__releaseDraftRead());
  await expect.poll(() => page.evaluate(() =>
    window.YT.preferences.snapshot().disk_scan_staleness_hours)).toBe(72);
  await expect(input).toHaveValue("48");
  await expect(input).toHaveAttribute("data-saved-value", "24");
  await page.evaluate(() => window.__setBridgeHandler("settings_save", () => ({
    ok: false, error: "Draft save failed",
  })));
  await input.blur();
  await expect(input).toHaveValue("24");
});

test("editing back to the original value does not block later settings refreshes", async ({ page }) => {
  await loadApp(page);
  await page.locator('.tab[data-tab="settings"]').click();
  await page.locator("#settings-background-checks > summary").click();
  const input = page.locator("#settings-disk-staleness");
  await expect(input).toHaveValue("24");
  await input.fill("48");
  await input.fill("24");
  await input.blur();
  expect(await page.evaluate(() => window.__bridgeCallsFor("settings_save"))).toEqual([]);
  await page.evaluate(() => window.__setBridgeHandler("settings_load", () => ({
    ...window.__fixtureDefaultResult("settings_load"), disk_scan_staleness_hours: 72,
  })));
  await page.locator('.tab[data-tab="settings"]').click();
  await expect(input).toHaveValue("72");
  await expect(input).toHaveAttribute("data-saved-value", "72");
});

test("Watch retries failed initial preferences when native readiness is announced again", async ({ page }) => {
  await loadApp(page, { bridge: { responses: { settings_load: {
    ok: false, code: "NATIVE_BRIDGE_UNAVAILABLE", error: "Settings method still loading",
  } } } });
  await page.evaluate(() => {
    window.__setBridgeHandler("settings_load", () => ({
      output_dir: "C:\\FixtureArchive", transcript_font_size: 19,
      transcript_pane_width: 620, caption_overlay_size: "small",
    }));
    window.dispatchEvent(new Event("pywebviewready"));
  });
  await expect.poll(() => page.evaluate(() =>
    document.documentElement.style.getPropertyValue("--watch-transcript-fz"))).toBe("19.0px");
  await expect(page.locator("#watch-cap-size")).toHaveValue("small");
  expect(await page.evaluate(() => window.__bridgeCallsFor("settings_save"))).toEqual([]);
});

test("Metadata uses shared menu keyboard boundaries and keeps the chosen channel/range", async ({ page }) => {
  await loadApp(page, { bridge: { responses: { get_channel_metadata_status: [{
    name: "Menu fixture", folder: "Menu fixture", url: "https://www.youtube.com/@MenuFixture",
    tx_total: 2, tx_transcribed: 2, id_total: 2, id_with_id: 2, id_missing: 0,
  }] } } });
  await page.locator('.tab[data-tab="health"]').click();
  await page.locator('#panel-health [data-settings-view="library"]').click();
  await page.evaluate(() => {
    document.getElementById("health-library-metadata").open = true;
    return window._refreshMetadataTab({ force: true });
  });
  const row = page.locator("#metadata-tbody .md-row-clickable");
  await row.focus();
  await page.keyboard.press("Shift+F10");
  const menu = page.locator("#ctx-menu-root > .md-context-menu");
  await expect(menu).toBeVisible();
  await page.keyboard.press("End");
  await expect(menu.getByRole("menuitem", { name: "Refetch missing thumbnails" })).toBeFocused();
  await page.keyboard.press("Home");
  await page.keyboard.press("ArrowRight");
  await page.keyboard.press("ArrowDown");
  await expect(menu.getByRole("menuitem", { name: "Last month" })).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(menu).toHaveCount(0);
  await expect(row).toBeFocused();
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("metadata_refresh_views_channel").map(call => call.args)))
    .toEqual([[{ folder: "Menu fixture", url: "https://www.youtube.com/@MenuFixture" }, 30]]);
});
