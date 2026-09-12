const fs = require("node:fs");
const path = require("node:path");
const { test, expect } = require("./fixtures");

const WEB_ROOT = path.resolve(__dirname, "..", "..", "..", "web");
const HEALTH_PARTIAL = fs.readFileSync(
  path.join(WEB_ROOT, "partials", "tab-health.html"), "utf8");
const NOW = new Date("2026-08-01T12:00:00Z");
const READ_ONLY_METHODS = [
  "get_channel_metadata_status",
  "get_index_summary",
  "index_summary",
  "settings_load",
  "ytdlp_version",
];

async function mountOverview(page, { deferred = null, waitForFinished = true } = {}) {
  await page.clock.setFixedTime(NOW);
  await page.setContent(
    '<!doctype html><html><head><meta charset="utf-8"></head><body>' +
      '<button class="tab" data-tab="download" type="button">Download</button>' +
      '<button class="tab" data-tab="health" type="button">Health</button>' +
      HEALTH_PARTIAL + "</body></html>");
  for (const stylesheet of ["styles.css", "styles-settings.css"]) {
    await page.addStyleTag({ path: path.join(WEB_ROOT, stylesheet) });
  }
  await page.evaluate(({ deferred }) => {
    window.__overviewCalls = [];
    window.__overviewFailures = [];
    window.__overviewDeferredMethod = deferred;
    window.__overviewSettingsRevision = 0;
    window.__overviewSettingsSaving = false;
    window.__overviewBridgeUp = true;
    window.__overviewResponses = {
      settings_load: { last_backup_ts: Date.now() / 1000 - 3600 },
      get_index_summary: {
        cards: { channels: 1, videos: 12, physical_copies: 12,
          size_label: "4 GB", scan_complete: true,
          scanned_channels: 1, total_channels: 1 },
      },
      index_summary: { videos: 12, channels: 1, segments: 500 },
      get_channel_metadata_status: [{
        name: "Fixture channel", id_missing: 2,
        last_views_refresh_ts: Date.now() / 1000 - 86400,
        tx_total: 12, tx_transcribed: 11,
      }],
      ytdlp_version: { ok: true, version: "2026.08.01" },
    };
    window.YT = {
      preferences: {
        writeRevision: () => window.__overviewSettingsRevision,
        isSaving: () => window.__overviewSettingsSaving,
      },
      bridge: {
        ready: Promise.resolve({}),
        isUp: () => window.__overviewBridgeUp,
        bridgeCall: async (method) => {
          window.__overviewCalls.push(method);
          if (!(method in window.__overviewResponses)) {
            throw new Error(`Unexpected Overview method: ${method}`);
          }
          const response = structuredClone(window.__overviewResponses[method]);
          if (method === window.__overviewDeferredMethod) {
            await new Promise(resolve => { window.__resolveOverviewRead = resolve; });
          }
          if (window.__overviewFailures.includes(method)) {
            throw new Error("Fixture read failed");
          }
          return response;
        },
      },
    };
    const panel = document.getElementById("panel-health");
    panel.classList.add("active");
    for (const button of document.querySelectorAll(".tab")) {
      button.addEventListener("click", () => {
        panel.classList.toggle("active", button.dataset.tab === "health");
      });
    }
  }, { deferred });
  await page.addScriptTag({ path: path.join(WEB_ROOT, "settingsInfra.js") });
  await page.evaluate(() => window.initSettingsSubTabs());
  await page.addScriptTag({ path: path.join(WEB_ROOT, "healthOverview.js") });
  await page.evaluate(() => window.initHealthOverview());
  if (waitForFinished) await finished(page);
}

async function finished(page) {
  await expect(page.locator("#health-overview-status"))
    .toHaveText("All overview checks finished.");
}

async function expectRounds(page, rounds) {
  await expect.poll(() => page.evaluate(() =>
    [...window.__overviewCalls].sort()))
    .toEqual(Array.from({ length: rounds }, () => READ_ONLY_METHODS).flat().sort());
}

async function returnFromLibrary(page) {
  await page.locator('[data-settings-view="library"]').click();
  await page.locator('[data-settings-view="overview"]').click();
}

async function returnFromDownload(page) {
  await page.locator('.tab[data-tab="download"]').click();
  await page.locator('.tab[data-tab="health"]').click();
  // The Health listener waits one task for the main tab switch to finish.
  await page.evaluate(() => new Promise(resolve => setTimeout(resolve, 0)));
}

async function displayedResults(page) {
  return page.evaluate(() => ({
    cards: [...document.querySelectorAll(".health-summary-card strong")]
      .map(element => element.textContent),
    attention: document.getElementById("health-attention-list").textContent,
  }));
}

test("returning to Overview or Health reuses the recently completed checks", async ({ page }) => {
  await mountOverview(page);
  const displayed = await displayedResults(page);

  await returnFromLibrary(page);
  await returnFromDownload(page);
  await returnFromLibrary(page);

  await expectRounds(page, 1);
  expect(await displayedResults(page)).toEqual(displayed);
  await finished(page);
});

test("Refresh and the maintenance refresh hook bypass a fresh navigation cache", async ({ page }) => {
  await mountOverview(page);
  await page.evaluate(() => { window.__overviewResponses.index_summary.videos = 15; });

  await page.locator("#btn-health-overview-refresh").click();
  await expect(page.locator("#health-overview-index-value")).toHaveText("15 available videos");
  await expectRounds(page, 2);

  await page.evaluate(async () => {
    window.__overviewResponses.index_summary.videos = 18;
    await window._refreshHealthOverview();
  });
  await expect(page.locator("#health-overview-index-value")).toHaveText("18 available videos");
  await expectRounds(page, 3);
});

test("expired checks refresh while the previous cards and attention remain readable", async ({ page }) => {
  await mountOverview(page);
  const displayed = await displayedResults(page);
  await page.clock.setFixedTime(new Date(NOW.getTime() + 61_000));
  await page.evaluate(() => {
    window.__overviewDeferredMethod = "index_summary";
    window.__overviewResponses.index_summary.videos = 20;
  });

  await returnFromLibrary(page);
  await expectRounds(page, 2);
  expect(await displayedResults(page)).toEqual(displayed);

  await page.evaluate(() => {
    window.__overviewDeferredMethod = null;
    window.__resolveOverviewRead();
  });
  await expect(page.locator("#health-overview-index-value")).toHaveText("20 available videos");
  await finished(page);
  await returnFromDownload(page);
  await expectRounds(page, 2);
});

test("navigation and explicit refresh share an already running set of checks", async ({ page }) => {
  await mountOverview(page, { deferred: "index_summary", waitForFinished: false });
  await expectRounds(page, 1);
  await returnFromLibrary(page);
  await returnFromDownload(page);
  await page.locator("#btn-health-overview-refresh").click();
  await page.evaluate(() => { window.__explicitOverviewRefresh = window._refreshHealthOverview(); });
  await expectRounds(page, 1);

  await page.evaluate(async () => {
    window.__overviewDeferredMethod = null;
    window.__resolveOverviewRead();
    await window.__explicitOverviewRefresh;
  });
  await finished(page);
  await returnFromLibrary(page);
  await expectRounds(page, 1);
});

test("a failed refresh reports unavailable data and retries on the next visit", async ({ page }) => {
  await mountOverview(page);
  await page.evaluate(() => { window.__overviewFailures = ["index_summary"]; });
  await page.locator("#btn-health-overview-refresh").click();
  await expect(page.locator("#health-overview-index-value")).toHaveText("Unavailable");
  await expect(page.locator("#health-overview-status"))
    .toContainText("Could not read: search-index status");
  await expect(page.locator("#health-overview-status")).toHaveClass(/is-warn/);
  await expectRounds(page, 2);

  await page.evaluate(() => { window.__overviewFailures = []; });
  await returnFromLibrary(page);
  await finished(page);
  await expectRounds(page, 3);
});

test("archive changes invalidate recent Overview data before the cache expires", async ({ page }) => {
  await mountOverview(page);
  await page.locator('[data-settings-view="library"]').click();
  await page.evaluate(() => {
    window.__overviewResponses.get_index_summary.cards.videos = 21;
    window.dispatchEvent(new Event("archive-roots-changed"));
  });
  await page.locator('[data-settings-view="overview"]').click();
  await expect(page.locator("#health-overview-archive-value")).toHaveText("21 videos");
  await expectRounds(page, 2);
});

test("a saved settings revision invalidates recent Overview data", async ({ page }) => {
  await mountOverview(page);
  await page.locator('[data-settings-view="backups"]').click();
  await page.evaluate(() => {
    window.__overviewResponses.settings_load.last_backup_ts = 0;
    window.__overviewSettingsRevision += 1;
  });
  await page.locator('[data-settings-view="overview"]').click();
  await expect(page.locator("#health-overview-backup-value")).toHaveText("No backup recorded");
  await expectRounds(page, 2);
});

test("an archive change during a refresh replaces the old-root result with one follow-up", async ({ page }) => {
  await mountOverview(page);
  await page.evaluate(() => {
    window.__overviewDeferredMethod = "get_index_summary";
    window.__overviewResponses.get_index_summary.cards.videos = 99;
    document.getElementById("health-overview-archive-value").dataset.sawOldRoot = "false";
    window.__oldRootObserver = new MutationObserver(() => {
      const value = document.getElementById("health-overview-archive-value");
      if (value.textContent === "99 videos") value.dataset.sawOldRoot = "true";
    });
    window.__oldRootObserver.observe(document.getElementById("health-overview-archive-value"),
      { childList: true, subtree: true, characterData: true });
  });
  await page.locator("#btn-health-overview-refresh").click();
  await expectRounds(page, 2);

  await page.evaluate(() => {
    window.__overviewResponses.get_index_summary.cards.videos = 25;
    window.dispatchEvent(new Event("archive-roots-changed"));
    window.dispatchEvent(new Event("archive-roots-changed"));
    window.__overviewDeferredMethod = null;
    window.__resolveOverviewRead();
  });
  await expect(page.locator("#health-overview-archive-value")).toHaveText("25 videos");
  await expect(page.locator("#health-overview-archive-value"))
    .toHaveAttribute("data-saw-old-root", "false");
  await expectRounds(page, 3);
  await returnFromLibrary(page);
  await expectRounds(page, 3);
});

test("a settings save during a read prevents that result from becoming fresh", async ({ page }) => {
  await mountOverview(page);
  await page.evaluate(() => { window.__overviewDeferredMethod = "settings_load"; });
  await page.locator("#btn-health-overview-refresh").click();
  await expectRounds(page, 2);

  await page.evaluate(() => {
    window.__overviewSettingsRevision += 1;
    window.__overviewResponses.settings_load.last_backup_ts = 0;
    window.__overviewDeferredMethod = null;
    window.__resolveOverviewRead();
  });
  await finished(page);
  await returnFromLibrary(page);
  await expect(page.locator("#health-overview-backup-value")).toHaveText("No backup recorded");
  await expectRounds(page, 3);
});

test("a check started while settings are saving is retried after the save finishes", async ({ page }) => {
  await mountOverview(page);
  await page.evaluate(async () => {
    window.__overviewSettingsSaving = true;
    await window._refreshHealthOverview();
  });
  await expectRounds(page, 2);
  await page.evaluate(() => {
    window.__overviewSettingsSaving = false;
    window.__overviewResponses.settings_load.last_backup_ts = 0;
  });

  await returnFromLibrary(page);
  await expect(page.locator("#health-overview-backup-value")).toHaveText("No backup recorded");
  await expectRounds(page, 3);
});
