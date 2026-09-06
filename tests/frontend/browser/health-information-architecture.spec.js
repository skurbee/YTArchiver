const fs = require("node:fs");
const path = require("node:path");
const { test, expect } = require("@playwright/test");

const REPO_ROOT = path.resolve(__dirname, "..", "..", "..");
const WEB_ROOT = path.join(REPO_ROOT, "web");
const HEALTH_PARTIAL = fs.readFileSync(
  path.join(WEB_ROOT, "partials", "tab-health.html"), "utf8");

const LIVE_RESPONSES = {
  settings_load: {
    output_dir: "C:\\FixtureArchive",
    last_backup_ts: Math.floor(Date.now() / 1000) - 3600,
  },
  get_index_summary: {
    cards: {
      channels: 2,
      scan_complete: true,
      scanned_channels: 2,
      total_channels: 2,
      videos: 12,
      physical_copies: 13,
      size_label: "4 GB",
    },
  },
  index_summary: {
    segments: 500,
    videos: 12,
    channels: 2,
    bookmarks: 0,
  },
  get_channel_metadata_status: [
    {
      name: "Fixture One",
      id_missing: 2,
      last_views_refresh_ts: Math.floor(Date.now() / 1000) - 86400,
      tx_total: 8,
      tx_transcribed: 7,
    },
    {
      name: "Fixture Two",
      id_missing: 0,
      last_views_refresh_ts: Math.floor(Date.now() / 1000) - 172800,
      tx_total: 4,
      tx_transcribed: 4,
    },
  ],
  ytdlp_version: {
    ok: true,
    version: "2026.08.01",
  },
};

const READ_ONLY_METHODS = [
  "get_channel_metadata_status",
  "get_index_summary",
  "index_summary",
  "settings_load",
  "ytdlp_version",
];

async function mountHealth(page, {
  failMethods = [],
  responses = LIVE_RESPONSES,
} = {}) {
  await page.setContent(
    "<!doctype html><html><head><meta charset=\"utf-8\"></head><body>" +
      '<button class="tab" data-tab="health" type="button">Health</button>' +
      HEALTH_PARTIAL +
      "</body></html>");
  for (const stylesheet of [
    "styles.css",
    "styles-browse.css",
    "styles-settings.css",
  ]) {
    await page.addStyleTag({ path: path.join(WEB_ROOT, stylesheet) });
  }
  await page.evaluate(({ responses, failed }) => {
    const calls = [];
    window.__healthBridgeCalls = calls;
    window.__healthBridgeArgs = {};
    window.YT = {
      bridge: {
        isUp: () => true,
        ready: Promise.resolve({}),
        bridgeCall: async (method, ...args) => {
          calls.push(method);
          window.__healthBridgeArgs[method] = args;
          if (failed.includes(method)) throw new Error("Fixture unavailable");
          return responses[method];
        },
      },
    };
  }, { responses, failed: failMethods });
  await page.addScriptTag({ path: path.join(WEB_ROOT, "settingsInfra.js") });
  await page.evaluate(() => {
    document.getElementById("panel-health").classList.add("active");
    window.initSettingsSubTabs();
  });
  await page.addScriptTag({ path: path.join(WEB_ROOT, "healthOverview.js") });
  await page.evaluate(() => window.initHealthOverview());
  await page.waitForFunction(() => {
    const status = document.getElementById("health-overview-status")?.textContent || "";
    return status.includes("finished") || status.includes("Could not read");
  });
}

test("Health uses three task-based destinations and keeps Overview as the default", async ({ page }) => {
  await page.setViewportSize({ width: 980, height: 720 });
  await mountHealth(page);

  const navigation = await page.evaluate(() => ({
    label: document.querySelector("#panel-health .settings-subnav-label")
      ?.textContent.trim(),
    items: [...document.querySelectorAll("#panel-health .settings-subnav-btn")]
      .map((button) => button.textContent.trim()),
  }));
  expect(navigation).toEqual({
    label: "HEALTH",
    items: ["Overview", "Library", "Backups"],
  });

  await expect(page.locator("#settings-view-overview")).toBeVisible();
  await expect(page.locator("#panel-health .settings-subnav-btn.active"))
    .toHaveText("Overview");
  for (const view of ["library", "backups"]) {
    await expect(page.locator(`#settings-view-${view}`)).toBeHidden();
  }

  await expect(page.locator("#health-overview-archive-value")).toHaveText("12 videos");
  await expect(page.locator("#health-overview-metadata-value"))
    .toHaveText("2 missing video IDs");
  await expect(page.locator("#health-overview-index-value"))
    .toHaveText("12 available videos");
  await expect(page.locator("#health-overview-transcripts-value"))
    .toHaveText("11 / 12 transcribed");
  await expect(page.locator("#health-overview-system-value"))
    .toHaveText("yt-dlp 2026.08.01");
  await expect(page.locator("#backup-age-display")).toContainText("Last backup:");
  await expect(page.locator("#health-attention-list .health-attention-item"))
    .toHaveCount(1);

  const called = await page.evaluate(() => [...new Set(window.__healthBridgeCalls)].sort());
  expect(called).toEqual(READ_ONLY_METHODS);
  const checkedReadArgs = await page.evaluate(() => ({
    index: window.__healthBridgeArgs.index_summary,
    metadata: window.__healthBridgeArgs.get_channel_metadata_status,
  }));
  expect(checkedReadArgs).toEqual({
    index: [true],
    metadata: [false, true],
  });

  await page.locator('[data-health-anchor="health-library-archive"]').click();
  await expect(page.locator("#settings-view-library")).toBeVisible();
  await expect(page.locator("#settings-view-overview")).toBeHidden();
  await expect(page.locator("#health-library-archive")).toHaveAttribute("open", "");

  const miniLog = page.locator("#health-mini-log");
  await expect(miniLog).toBeVisible();
  await expect(miniLog).toHaveAttribute("class", "main-log mini-log");
  await expect(miniLog).toHaveAttribute("tabindex", "-1");
  const miniLogPlacement = await page.evaluate(() => {
    const area = document.querySelector("#panel-health .settings-area");
    const frame = document.querySelector("#panel-health .mini-log-frame");
    return {
      frameClass: frame?.className,
      followsArea: !!(area.compareDocumentPosition(frame)
        & Node.DOCUMENT_POSITION_FOLLOWING),
    };
  });
  expect(miniLogPlacement).toEqual({
    frameClass: "mini-log-frame",
    followsArea: true,
  });
});

test("Health keeps every status and maintenance control in one intended section", async ({ page }) => {
  await mountHealth(page);

  const expected = {
    "health-library-metadata": [
      "metadata-totals",
      "btn-md-refresh-all-views",
      "btn-md-refresh-all-comments",
      "btn-md-backfill-all-ids",
      "btn-md-refetch-all-thumbs",
      "btn-md-recheck-thumbs",
      "btn-md-reload",
      "metadata-table",
      "metadata-tbody",
    ],
    "health-library-index": [
      "index-stats-text",
      "btn-idx-build",
      "idx-progress",
      "btn-idx-rebuild",
      "index-log",
    ],
    "health-library-archive": [
      "btn-scan-archive",
      "btn-hide-sidecars",
      "btn-provenance",
      "btn-compress-dry-run",
      "btn-thumb-realign",
      "btn-fix-video-lengths",
      "btn-reset-sync-state",
    ],
    "health-library-transcripts": [
      "btn-drift-scan",
      "btn-repair-yt-captions",
      "btn-punct-restore",
    ],
    "settings-view-backups": [
      "settings-auto-backup",
      "backup-auto-age-display",
      "btn-export-channels",
      "btn-import-channels",
      "btn-export-backup",
      "btn-import-backup",
      "backup-age-display",
    ],
  };
  for (const [section, ids] of Object.entries(expected)) {
    for (const id of ids) {
      await expect(page.locator(`#${section} #${id}`)).toHaveCount(1);
      await expect(page.locator(`#panel-health #${id}`)).toHaveCount(1);
    }
  }

  for (const removedPreference of [
    "settings-roots-list",
    "btn-settings-add-root",
    "btn-settings-remove-root",
    "settings-auto-index-enabled",
    "settings-auto-index-threshold",
    "settings-ytdlp-channel",
    "settings-ytdlp-update-mode",
    "settings-ytdlp-update-note",
    "settings-ytdlp-check-days",
    "settings-ytdlp-check-status",
    "settings-ytdlp-version",
    "btn-ytdlp-update",
    "btn-about",
    "btn-diagnostics",
    "btn-run-setup",
  ]) {
    await expect(page.locator(`#panel-health #${removedPreference}`)).toHaveCount(0);
  }

  for (const rareAction of [
    "btn-idx-rebuild",
    "btn-reset-sync-state",
    "btn-repair-yt-captions",
    "btn-punct-restore",
  ]) {
    const details = page.locator(`#${rareAction}`).locator("xpath=ancestor::details[1]");
    await expect(details).toHaveCount(1);
    await expect(details).not.toHaveAttribute("open", "");
  }
  await expect(page.locator("#health-library-metadata"))
    .not.toHaveAttribute("open", "");
  await expect(page.locator("#health-library-index")).not.toHaveAttribute("hidden", "");

  const userFacingCopy = await page.evaluate(() => {
    const panel = document.getElementById("panel-health");
    const titles = [...panel.querySelectorAll("[title]")]
      .map((element) => element.getAttribute("title"));
    return `${panel.innerText}\n${titles.join("\n")}`;
  });
  expect(userFacingCopy).not.toMatch(/video_id|index DB|metadata\.jsonl|sidecar|ffprobe|bootstrap|\.jsonl/i);
});

test("every Health section stays inside the native minimum-width layout", async ({ page }) => {
  await page.setViewportSize({ width: 980, height: 720 });
  await mountHealth(page);

  for (const view of ["overview", "library", "backups"]) {
    await page.locator(`[data-settings-view="${view}"]`).click();
    if (view === "library") {
      await page.evaluate(() => {
        document.querySelectorAll("#settings-view-library details")
          .forEach((details) => { details.open = true; });
      });
    }
    const geometry = await page.evaluate((key) => {
      const area = document.querySelector("#panel-health .settings-area");
      const main = document.querySelector("#panel-health .settings-main");
      const current = document.getElementById(`settings-view-${key}`);
      const areaRect = area.getBoundingClientRect();
      const mainRect = main.getBoundingClientRect();
      const viewRect = current.getBoundingClientRect();
      return {
        areaRight: areaRect.right,
        mainRight: mainRect.right,
        viewRight: viewRect.right,
        areaScrollWidth: area.scrollWidth,
        areaClientWidth: area.clientWidth,
        mainScrollWidth: main.scrollWidth,
        mainClientWidth: main.clientWidth,
      };
    }, view);
    expect(geometry.mainRight, view).toBeLessThanOrEqual(geometry.areaRight + 1);
    expect(geometry.viewRight, view).toBeLessThanOrEqual(geometry.mainRight + 1);
    expect(geometry.areaScrollWidth, view)
      .toBeLessThanOrEqual(geometry.areaClientWidth + 1);
    expect(geometry.mainScrollWidth, view)
      .toBeLessThanOrEqual(geometry.mainClientWidth + 1);
  }
});

test("Health Overview reports unavailable checks honestly", async ({ page }) => {
  await mountHealth(page, { failMethods: READ_ONLY_METHODS });

  await expect(page.locator("#health-overview-status")).toContainText("Could not read:");
  for (const key of ["archive", "metadata", "index", "transcripts", "backup", "system"]) {
    await expect(page.locator(`#health-overview-${key}-value`)).toHaveText("Unavailable");
  }
  await expect(page.locator("#health-attention-list"))
    .toContainText("No confirmed issues in the checks that finished.");
  const called = await page.evaluate(() => [...new Set(window.__healthBridgeCalls)].sort());
  expect(called).toEqual(READ_ONLY_METHODS);
});

test("Health Overview treats backend read-error results as unavailable", async ({ page }) => {
  const responses = structuredClone(LIVE_RESPONSES);
  responses.index_summary = {
    available: false,
    error: "Search index could not be read.",
  };
  responses.get_channel_metadata_status = {
    available: false,
    error: "Metadata status could not be read.",
  };
  await mountHealth(page, { responses });

  await expect(page.locator("#health-overview-index-value")).toHaveText("Unavailable");
  await expect(page.locator("#health-overview-metadata-value")).toHaveText("Unavailable");
  await expect(page.locator("#health-overview-transcripts-value")).toHaveText("Unavailable");
  await expect(page.locator("#health-overview-status"))
    .toContainText("metadata status, transcript coverage, search-index status");
});

test("Health Overview flags a full backup that is 14 or more days old", async ({ page }) => {
  const responses = structuredClone(LIVE_RESPONSES);
  responses.settings_load.last_backup_ts = Math.floor(Date.now() / 1000) - 20 * 86400;
  await mountHealth(page, { responses });

  await expect(page.locator("#health-overview-backup-value"))
    .toContainText("Last backup 20d ago");
  await expect(page.locator('[data-health-target="backups"]'))
    .toHaveClass(/is-warn/);
  await expect(page.locator("#health-attention-list"))
    .toContainText("(20d ago).");
  await expect(page.locator("#backup-age-display"))
    .toContainText("(20d ago)");
});

test("Health Overview distinguishes missing yt-dlp from an unreadable check", async ({ page }) => {
  const responses = structuredClone(LIVE_RESPONSES);
  responses.ytdlp_version = { ok: false, error: "yt-dlp not found" };
  await mountHealth(page, { responses });

  await expect(page.locator("#health-overview-system-value"))
    .toHaveText("yt-dlp needs attention");
  await expect(page.locator('[data-health-target="settings"]')).toHaveClass(/is-bad/);
  await expect(page.locator("#health-attention-list"))
    .toContainText("yt-dlp is not available.");
});

test("Metadata's shortened Videos heading does not overlap the next column", async ({ page }) => {
  await page.setViewportSize({ width: 980, height: 720 });
  await mountHealth(page);
  await page.locator('[data-settings-view="library"]').click();
  await page.locator("#health-library-metadata").evaluate((details) => {
    details.open = true;
  });

  const geometry = await page.evaluate(() => {
    const videos = document.querySelector('#metadata-table th[data-sort="videos"]');
    const ids = document.querySelector('#metadata-table th[data-sort="ids"]');
    const videoRect = videos.getBoundingClientRect();
    const idsRect = ids.getBoundingClientRect();
    const labelRange = document.createRange();
    labelRange.selectNodeContents(videos);
    const labelRect = labelRange.getBoundingClientRect();
    return {
      label: videos.textContent.trim(),
      videosRight: videoRect.right,
      idsLeft: idsRect.left,
      labelRight: labelRect.right,
    };
  });
  expect(geometry.label).toBe("Videos");
  expect(geometry.videosRight).toBeLessThanOrEqual(geometry.idsLeft + 1);
  expect(geometry.labelRight).toBeLessThanOrEqual(geometry.idsLeft + 1);
});

test("command palette routes to the new Health destinations", () => {
  const source = fs.readFileSync(path.join(WEB_ROOT, "commandPalette.js"), "utf8");
  expect(source).not.toContain('_healthSub("tools")');
  expect(source).toContain('_healthSub("overview")');
  expect(source).toContain('_healthSub("backups")');
  for (const anchor of [
    "health-library-metadata",
    "health-library-index",
    "health-library-archive",
    "health-library-transcripts",
  ]) {
    expect(source).toContain(`_healthSub("library", "${anchor}")`);
  }
  expect(source).toContain('_settingsSection("settings-about-troubleshooting")');
});
