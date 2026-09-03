const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

test.describe("catalog-backed screen loading", () => {
  test("catalog reads are globally serialized and keep each screen's newest request", async ({ page }) => {
    await loadApp(page);

    const state = await page.evaluate(async () => {
      let active = 0;
      let maxActive = 0;
      const started = [];
      let signalFirstStart;
      const firstStarted = new Promise((resolve) => {
        signalFirstStart = resolve;
      });
      const task = (label, delay) => async () => {
        started.push(label);
        if (label === "a") signalFirstStart();
        active += 1;
        maxActive = Math.max(maxActive, active);
        await new Promise((resolve) => setTimeout(resolve, delay));
        active -= 1;
        return label;
      };

      const first = window.YT.bridge.catalogRead(
        "screen-a", task("a", 90), { slowAfterMs: 1000 });
      // Startup can have unrelated reads in flight. Wait until A is actually
      // running so the ordering assertion is about the requests below.
      await firstStarted;
      const oldB = window.YT.bridge.catalogRead(
        "screen-b", task("b-old", 10), { slowAfterMs: 1000 });
      const screenC = window.YT.bridge.catalogRead(
        "screen-c", task("c", 10), { slowAfterMs: 1000 });
      const latestB = window.YT.bridge.catalogRead(
        "screen-b", task("b-latest", 10), { slowAfterMs: 1000 });

      const outcomes = await Promise.all([first, oldB, screenC, latestB]);
      return {
        maxActive,
        started,
        skippedOldB: outcomes[1].stale && outcomes[1].skipped,
        values: [outcomes[0].value, outcomes[2].value, outcomes[3].value],
      };
    });

    expect(state).toEqual({
      maxActive: 1,
      started: ["a", "b-latest", "c"],
      skippedOldB: true,
      values: ["a", "c", "b-latest"],
    });
  });

  test("catalog slow timers announce only phase or workload changes", async ({ page }) => {
    await loadApp(page);

    const statuses = await page.evaluate(async () => {
      const seen = [];
      await window.YT.bridge.catalogRead(
        "announcement-contract",
        () => new Promise((resolve) => setTimeout(() => resolve(true), 120)),
        {
          label: "test catalog",
          slowAfterMs: 0,
          tickMs: 25,
          onStatus: (status) => seen.push({
            phase: status.phase,
            announce: status.announce,
            announcement: status.announcement,
          }),
        });
      return seen;
    });

    const slow = statuses.filter((status) => status.phase === "slow");
    expect(slow.length).toBeGreaterThan(2);
    expect(slow.filter((status) => status.announce)).toHaveLength(1);
    expect(new Set(slow.map((status) => status.announcement)).size).toBe(1);
    expect(statuses.find((status) => status.phase === "loading")?.announce)
      .toBe(true);
  });

  test("queued reads say they are waiting, then start a fresh loading timer", async ({ page }) => {
    await loadApp(page);

    const state = await page.evaluate(async () => {
      let releaseBlocker;
      let signalBlockerStart;
      const blockerGate = new Promise((resolve) => {
        releaseBlocker = resolve;
      });
      const blockerStarted = new Promise((resolve) => {
        signalBlockerStart = resolve;
      });
      const blocker = window.YT.bridge.catalogRead(
        "waiting-contract-blocker",
        async () => {
          signalBlockerStart();
          await blockerGate;
          return "blocker";
        },
        { lane: "waiting-contract", slowAfterMs: 1000 });

      await blockerStarted;
      const statuses = [];
      const queued = window.YT.bridge.catalogRead(
        "waiting-contract-manual",
        () => new Promise((resolve) => setTimeout(
          () => resolve("manual"), 90)),
        {
          lane: "waiting-contract",
          label: "manual downloads",
          slowAfterMs: 0,
          tickMs: 25,
          onStatus: (status) => statuses.push({
            phase: status.phase,
            text: status.text,
            announce: status.announce,
            announcement: status.announcement,
            elapsedMs: status.elapsedMs,
          }),
        });

      await new Promise((resolve) => setTimeout(resolve, 80));
      const beforeRelease = statuses.slice();
      releaseBlocker();
      const outcome = await queued;
      await blocker;
      return { beforeRelease, statuses, value: outcome.value };
    });

    expect(state.value).toBe("manual");
    expect(new Set(state.beforeRelease.map((status) => status.phase)))
      .toEqual(new Set(["waiting"]));
    expect(state.beforeRelease[0].text).toContain(
      "Waiting to refresh manual downloads while another library view finishes");
    expect(state.beforeRelease[0].text).not.toContain("loading");
    expect(state.beforeRelease.filter((status) => status.announce))
      .toHaveLength(1);

    const loading = state.statuses.find((status) => status.phase === "loading");
    expect(loading).toMatchObject({
      text: "Loading manual downloads…",
      announce: true,
      announcement: "Loading manual downloads.",
    });
    expect(loading.elapsedMs).toBeLessThan(25);
    expect(state.statuses.some((status) => status.phase === "slow"))
      .toBe(true);
  });

  test("Manual keeps cached cards visible and explains a queued refresh", async ({ page }) => {
    await loadApp(page);

    await page.evaluate(() => {
      localStorage.setItem("ytarchiver_manual_page1", JSON.stringify([{
        title: "Cached Manual Video",
        filepath: "C:\\FixtureArchive\\cached.mp4",
        video_id: "cached-manual",
      }]));
      window.__manualCallBase =
        window.__bridgeCallsFor("list_manual_videos").length;
      window.__setBridgeHandler("list_manual_videos", () =>
        new Promise((resolve) => {
          window.__resolveManualRefresh = resolve;
        }));
      window.__catalogBlockerStarted = false;
      window.__catalogBlocker = window.YT.bridge.catalogRead(
        "manual-ui-blocker",
        () => new Promise((resolve) => {
          window.__catalogBlockerStarted = true;
          window.__releaseCatalogBlocker = resolve;
        }),
        { label: "graph data", slowAfterMs: 10000 });
    });
    await expect.poll(() => page.evaluate(() =>
      window.__catalogBlockerStarted)).toBe(true);

    await page.locator('.tab[data-tab="browse"]').click();
    await page.locator('.submode-btn[data-submode="manual"]').click();

    await expect(page.locator("#manual-grid .video-card-title"))
      .toHaveText("Cached Manual Video");
    const status = page.locator("#manual-catalog-status");
    await expect(status).toContainText(
      "Waiting to refresh manual downloads while another library view finishes");
    await expect(status).not.toContainText("Still loading manual downloads");
    await expect(status).toHaveAttribute("role", "status");
    await expect(status).toHaveAttribute("aria-live", "polite");
    expect(await page.evaluate(() =>
      window.__bridgeCallsFor("list_manual_videos").length
        - window.__manualCallBase)).toBe(0);

    await page.evaluate(() => window.__releaseCatalogBlocker(true));
    await expect.poll(() => page.evaluate(() =>
      window.__bridgeCallsFor("list_manual_videos").length
        - window.__manualCallBase)).toBe(1);
    await expect(status).toHaveText("Loading manual downloads…");

    await page.evaluate(() => window.__resolveManualRefresh({
      rows: [{
        title: "Fresh Manual Video",
        filepath: "C:\\FixtureArchive\\fresh.mp4",
        video_id: "fresh-manual",
      }],
      folder: "C:\\FixtureArchive",
      total: 1,
      has_more: false,
    }));
    await expect(page.locator("#manual-grid .video-card-title"))
      .toHaveText("Fresh Manual Video");
    await expect(status).toHaveCount(0);
    await expect(page.locator("#manual-grid"))
      .not.toHaveClass(/is-refreshing/);
  });

  test("background maintenance does not block ordinary catalog reads", async ({ page }) => {
    await loadApp(page);

    const state = await page.evaluate(async () => {
      let releaseBackground;
      let signalBackgroundStart;
      const backgroundStarted = new Promise((resolve) => {
        signalBackgroundStart = resolve;
      });
      const backgroundGate = new Promise((resolve) => {
        releaseBackground = resolve;
      });
      let backgroundActive = false;

      const background = window.YT.bridge.catalogRead(
        "lane-test-background",
        async () => {
          backgroundActive = true;
          signalBackgroundStart();
          await backgroundGate;
          backgroundActive = false;
          return "background";
        },
        { lane: "background-test", slowAfterMs: 1000 });

      await backgroundStarted;
      const ordinary = window.YT.bridge.catalogRead(
        "lane-test-catalog",
        async () => ({ value: "catalog", overlapped: backgroundActive }),
        { slowAfterMs: 1000 });

      const ordinaryOutcome = await ordinary;
      releaseBackground();
      const backgroundOutcome = await background;
      return {
        ordinary: ordinaryOutcome.value,
        background: backgroundOutcome.value,
      };
    });

    expect(state).toEqual({
      ordinary: { value: "catalog", overlapped: true },
      background: "background",
    });
  });

  test("Channels hides the first partial payload until rich details are ready", async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => window.seedLogs());
    await page.locator('.tab[data-tab="browse"]').click();

    await page.evaluate(() => {
      window._browseState.channels = [];
      window._browseState.channelsReady = false;
      window._browseState.pendingChannels = [];
      window._browseState.channelsError = false;
      window.renderChannelGridLoading();
      window.__channelBlockerStarted = false;
      window.__channelBlocker = window.YT.bridge.catalogRead(
        "channel-hydration-blocker",
        () => new Promise((resolve) => {
          window.__channelBlockerStarted = true;
          window.__releaseChannelBlocker = resolve;
        }),
        { label: "hidden library details", slowAfterMs: 10000 },
      );
    });
    await expect.poll(() => page.evaluate(() =>
      window.__channelBlockerStarted)).toBe(true);

    await page.evaluate(() => {
      window.__setBridgeHandler("browse_list_channels", () =>
        new Promise((resolve) => {
          window.__resolveChannelHydration = resolve;
        }));
      window.__channelsPrime = window._primeBrowse([{
          folder: "Immediate Channel",
          name: "Immediate Channel",
          // These cheap subscription values must never render as a card.
          n_vids: "—",
          size: "—",
      }]);
    });

    const grid = page.locator("#channel-grid");
    await expect(grid.locator(".channel-card")).toHaveCount(0);
    await expect(grid.locator(".grid-loading-label"))
      .toHaveText("Loading channels…");
    await expect(grid).toHaveAttribute("aria-busy", "true");
    await expect(grid).not.toContainText("another library view");

    // Masking partial cards must not erase subscription identity from other
    // UI that only needs a channel name.
    await page.evaluate(() => window.openCommandPalette());
    await expect(page.locator("#cmdp-list")).toContainText("Immediate Channel");
    await page.evaluate(() => window.closeCommandPalette());

    // Sort, filter, and a tab round-trip must not expose the partial cards or
    // replace the loader with the first-run onboarding state.
    await page.locator("#browse-channel-sort").selectOption("size");
    await page.locator("#browse-filter").fill("Immediate");
    await page.waitForTimeout(250);
    await page.locator('.tab[data-tab="settings"]').click();
    await page.locator('.tab[data-tab="browse"]').click();
    await expect(grid.locator(".channel-card")).toHaveCount(0);
    await expect(grid.locator(".browse-first-channel")).toHaveCount(0);
    await expect(grid.locator(".grid-loading-label"))
      .toHaveText("Loading channels…");

    await page.evaluate(() => window.__releaseChannelBlocker(true));
    await expect.poll(() => page.evaluate(() =>
      typeof window.__resolveChannelHydration)).toBe("function");
    await page.evaluate(() => window.__resolveChannelHydration([{
      folder: "Immediate Channel",
      name: "Immediate Channel",
      n_vids: 7,
      size: "700 MB",
      subscriber_count: 1200,
      banner_url: "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=",
    }, {
      folder: "Filtered Out Channel",
      name: "Filtered Out Channel",
      n_vids: 99,
      size: "9 GB",
    }]));

    const card = grid.locator(".channel-card");
    await expect(card).toHaveCount(1);
    await expect(card.locator(".channel-card-name"))
      .toHaveText("Immediate Channel");
    await expect(card.locator(".channel-card-meta"))
      .toContainText("7 videos");
    await expect(card.locator(".channel-card-bg"))
      .toHaveAttribute("src", /data:image\/png/);
    await expect(grid).not.toContainText("Filtered Out Channel");
    expect(await page.evaluate(() => window._browseState.channels.length))
      .toBe(2);
    await expect(grid.locator(".grid-loading")).toHaveCount(0);
    await expect(page.locator("#channel-catalog-status")).toHaveCount(0);
    await expect(grid).toHaveAttribute("aria-busy", "false");
  });

  test("Channels keeps a completed grid visible during a background refresh", async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => window.seedLogs());
    await page.locator('.tab[data-tab="browse"]').click();
    const firstBanner =
      "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=";

    await page.evaluate(async (banner) => {
      window.__setBridgeHandler("browse_list_channels", () => [{
        folder: "Cached Channel",
        name: "Cached Channel",
        n_vids: 4,
        size: "40 MB",
        banner_url: banner,
      }]);
      await window._primeBrowse([{
        folder: "Cached Channel", name: "Cached Channel",
      }]);
      window.__setBridgeHandler("browse_list_channels", () =>
        new Promise((resolve) => {
          window.__resolveChannelRefresh = resolve;
        }));
      window.__cachedChannelRefresh = window._primeBrowse([{
        folder: "Cached Channel",
        name: "Cached Channel",
        n_vids: "—",
      }]);
    }, firstBanner);

    const grid = page.locator("#channel-grid");
    const card = grid.locator(
      '.channel-card[data-channel-name="Cached Channel"]');
    await expect(card).toBeVisible();
    await expect(card.locator(".channel-card-meta")).toContainText("4 videos");
    await expect(card.locator(".channel-card-bg"))
      .toHaveAttribute("src", firstBanner);
    await expect(grid.locator(".grid-loading")).toHaveCount(0);
    await expect(page.locator("#channel-catalog-status"))
      .toHaveText("Showing channels · updating details…");
    await expect(grid).toHaveAttribute("aria-busy", "true");

    await page.evaluate(() => window.__resolveChannelRefresh([{
      folder: "Cached Channel",
      name: "Cached Channel",
      n_vids: 5,
      size: "50 MB",
    }]));
    await expect(card.locator(".channel-card-meta")).toContainText("5 videos");
    await expect(page.locator("#channel-catalog-status")).toHaveCount(0);
    await expect(grid).toHaveAttribute("aria-busy", "false");
  });

  test("Channels preserves cached refresh errors and Retry across rerenders", async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => window.seedLogs());
    await page.locator('.tab[data-tab="browse"]').click();

    await page.evaluate(async () => {
      window.__setBridgeHandler("browse_list_channels", () => [{
        folder: "Cached Error Channel",
        name: "Cached Error Channel",
        n_vids: 2,
        size: "20 MB",
      }]);
      await window._primeBrowse([{
        folder: "Cached Error Channel", name: "Cached Error Channel",
      }]);
      window.__setBridgeHandler("browse_list_channels", () =>
        Promise.reject(new Error("fixture refresh failure")));
      await window._primeBrowse([{
        folder: "Cached Error Channel", name: "Cached Error Channel",
      }]);
    });

    const grid = page.locator("#channel-grid");
    const status = page.locator("#channel-catalog-status");
    await expect(grid.locator(".channel-card-name"))
      .toHaveText("Cached Error Channel");
    await expect(status).toHaveAttribute("role", "alert");
    await expect(status).toContainText("Couldn’t update channel details.");

    await page.locator('.tab[data-tab="settings"]').click();
    await page.locator('.tab[data-tab="browse"]').click();
    await page.locator("#browse-channel-sort").selectOption("videos");
    await expect(status).toHaveAttribute("role", "alert");
    await expect(status.getByRole("button", { name: "Retry" })).toBeVisible();
    await expect(grid).toHaveAttribute("aria-busy", "false");

    await page.evaluate(() => {
      window.__setBridgeHandler("browse_list_channels", () =>
        new Promise((resolve) => {
          window.__resolveCachedErrorRetry = resolve;
        }));
    });
    await status.getByRole("button", { name: "Retry" }).click();
    await expect(status).toHaveAttribute("role", "status");
    await expect(status).toHaveText("Showing channels · updating details…");
    await expect(grid).toHaveAttribute("aria-busy", "true");
    await page.evaluate(() => window.__resolveCachedErrorRetry([{
      folder: "Cached Error Channel",
      name: "Cached Error Channel",
      n_vids: 3,
      size: "30 MB",
    }]));
    await expect(grid.locator(".channel-card-meta")).toContainText("3 videos");
    await expect(status).toHaveCount(0);
    await expect(grid).toHaveAttribute("aria-busy", "false");
  });

  test("a newer channel summary wins if rich hydration started earlier", async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => window.seedLogs());
    await page.locator('.tab[data-tab="browse"]').click();

    await page.evaluate(() => {
      window.__setBridgeHandler("browse_list_channels", () =>
        new Promise((resolve) => {
          window.__resolveStaleChannelHydration = resolve;
        }));
      window.__staleChannelPrime = window._primeBrowse([{
        folder: "Racing Channel",
        name: "Racing Channel",
        n_vids: 4,
        size: "40 MB",
      }]);
    });
    await expect.poll(() => page.evaluate(() =>
      typeof window.__resolveStaleChannelHydration)).toBe("function");

    await page.evaluate(() => {
      window._refreshBrowseChannelSummaries([{
        folder: "Racing Channel",
        name: "Racing Channel",
        n_vids: "5",
        size: "50 MB",
        size_bytes: 52428800,
      }]);
      window.__resolveStaleChannelHydration([{
        folder: "Racing Channel",
        name: "Racing Channel",
        n_vids: 4,
        size: "40 MB",
        size_bytes: 41943040,
      }]);
    });
    await page.evaluate(() => window.__staleChannelPrime);

    const cardMeta = page.locator("#channel-grid .channel-card-meta");
    await expect(cardMeta).toContainText("5 videos");
    await expect(cardMeta).toContainText("50 MB");
    expect(await page.evaluate(() => ({
      count: window._browseState.channels[0]?.n_vids,
      size: window._browseState.channels[0]?.size,
    }))).toEqual({ count: 5, size: "50 MB" });
  });

  test("Channels shows Retry instead of partial cards after invalid detail responses", async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => window.seedLogs());
    await page.locator('.tab[data-tab="browse"]').click();

    await page.evaluate(async () => {
      window.__channelDetailAttempt = 0;
      window.__setBridgeHandler("browse_list_channels", () => {
        window.__channelDetailAttempt += 1;
        if (window.__channelDetailAttempt === 1) return [];
        if (window.__channelDetailAttempt === 2) return { rows: [] };
        return [{
          folder: "Recovered Channel",
          name: "Recovered Channel",
          n_vids: 8,
          size: "80 MB",
        }];
      });
      await window._primeBrowse([{
        folder: "Recovered Channel",
        name: "Recovered Channel",
      }]);
    });

    const grid = page.locator("#channel-grid");
    const retry = grid.getByRole("button", { name: "Retry" });
    await expect(grid).toContainText("Couldn’t load channels.");
    await expect(retry).toBeVisible();
    await expect(grid.locator(".channel-card")).toHaveCount(0);
    await expect(grid.locator(".grid-spinner")).toHaveCount(0);
    await expect(grid.locator(".browse-first-channel")).toHaveCount(0);
    await expect(grid).toHaveAttribute("aria-busy", "false");

    await retry.click();
    await expect.poll(() => page.evaluate(() =>
      window.__channelDetailAttempt)).toBe(2);
    await expect(grid).toContainText("Couldn’t load channels.");
    await expect(retry).toBeVisible();

    await retry.click();
    await expect(grid.locator(".channel-card-name"))
      .toHaveText("Recovered Channel");
    await expect(grid.locator(".channel-card-meta"))
      .toContainText("8 videos");
    await expect(grid).toHaveAttribute("aria-busy", "false");
  });

  test("Channels shows first-run onboarding immediately for a truly empty library", async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => window.seedLogs());
    await page.locator('.tab[data-tab="browse"]').click();

    const calls = await page.evaluate(async () => {
      const before = window.__bridgeCallsFor("browse_list_channels").length;
      await window._primeBrowse([]);
      return window.__bridgeCallsFor("browse_list_channels").length - before;
    });

    await expect(page.locator("#channel-grid .browse-first-channel"))
      .toContainText("Add your first channel");
    await expect(page.locator("#channel-grid"))
      .toHaveAttribute("aria-busy", "false");
    expect(calls).toBe(0);
  });

  test("a no-argument channel repaint cannot wipe a completed grid", async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => window.seedLogs());
    await page.locator('.tab[data-tab="browse"]').click();

    const result = await page.evaluate(async () => {
      window.__setBridgeHandler("browse_list_channels", () => [{
        folder: "Imported Channel",
        name: "Imported Channel",
        n_vids: 3,
        size: "30 MB",
      }]);
      await window._primeBrowse([{
        folder: "Imported Channel", name: "Imported Channel",
      }]);
      const cardBefore = document.querySelector("#channel-grid .channel-card");
      const callsBefore = window.__bridgeCallsFor("browse_list_channels").length;
      await window._primeBrowse();
      return {
        sameCard: cardBefore === document.querySelector(
          "#channel-grid .channel-card"),
        extraCalls: window.__bridgeCallsFor("browse_list_channels").length
          - callsBefore,
      };
    });

    expect(result).toEqual({ sameCard: true, extraCalls: 0 });
    await expect(page.locator("#channel-grid .channel-card-name"))
      .toHaveText("Imported Channel");
    await expect(page.locator("#channel-grid .browse-first-channel"))
      .toHaveCount(0);
  });

  test("a pending channel command opens its videos while cards stay masked", async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => window.seedLogs());

    await page.evaluate(() => {
      window.__setBridgeHandler("browse_list_channels", () =>
        new Promise((resolve) => {
          window.__resolvePendingCommandHydration = resolve;
        }));
      window.__pendingCommandPrime = window._primeBrowse([{
        folder: "Pending Command Channel",
        name: "Pending Command Channel",
      }]);
      window.openCommandPalette();
    });
    await page.locator("#cmdp-input").fill("Pending Command Channel");
    await page.locator("#cmdp-input").press("Enter");

    await expect.poll(() => page.evaluate(() => ({
      channel: window._browseState.currentChannel?.folder,
      view: window._browseState.view,
    }))).toEqual({
      channel: "Pending Command Channel",
      view: "videos",
    });
    await expect(page.locator("#browse-main-title"))
      .toHaveText("Pending Command Channel");

    await page.evaluate(() => window.__resolvePendingCommandHydration([{
      folder: "Pending Command Channel",
      name: "Pending Command Channel",
      n_vids: 1,
      size: "10 MB",
    }]));
    await page.evaluate(() => window.__pendingCommandPrime);
  });

  test("a landed download refreshes hidden channel data without replacing Browse chrome", async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => window.seedLogs());

    await page.evaluate(() => {
      window.__setBridgeHandler("browse_list_channels", () => [{
        folder: "Example Channel",
        name: "Example Channel",
        n_vids: 1,
      }]);
      window.__channelPageReads = 0;
      window.__setBridgeHandler("browse_list_videos_page", () => {
        window.__channelPageReads += 1;
        if (window.__channelPageReads > 1) {
          return new Promise((resolve) => {
            window.__resolveHiddenChannelRefresh = resolve;
          });
        }
        return {
          rows: [{
            video_id: "existing-video",
            title: "Existing video",
            channel: "Example Channel",
            filepath: "C:\\FixtureArchive\\existing.mp4",
          }],
          next_offset: 1,
          has_more: false,
        };
      });
      window.__primeChannelPromise = window._primeBrowse([{
        folder: "Example Channel",
        name: "Example Channel",
        n_vids: 1,
      }]);
    });

    await page.locator('.tab[data-tab="browse"]').click();
    await page.locator("#channel-grid .channel-card").click();
    await expect(page.locator("#browse-main-title"))
      .toHaveText("Example Channel");
    await expect(page.locator("#channel-page-header")).toBeVisible();
    await expect(page.locator("#video-grid .video-card-title"))
      .toHaveText("Existing video");

    // Leave the channel page, then simulate the backend's download-landed
    // push. A hidden refresh must not run the full channel-page loader: that
    // loader also writes the shared title and action header.
    await page.locator('.submode-btn[data-submode="channels"]').click();
    await page.evaluate(() => {
      window._queueHasSyncForChannel = () => "queued";
      window.__hiddenChannelRefresh =
        window._refreshChannelVideosIfLoaded("Example Channel");
    });
    await expect.poll(() => page.evaluate(() =>
      typeof window.__resolveHiddenChannelRefresh)).toBe("function");
    await expect(page.locator("#browse-main-title")).toHaveText("Channels");
    await expect(page.locator("#view-channels")).toBeVisible();
    await expect(page.locator("#channel-page-header")).toBeHidden();
    await expect(page.locator("#cph-info"))
      .not.toContainText("Queued to sync");
    await expect(page.locator("#cph-sync-now")).toBeEnabled();

    // Every Browse destination owns a canonical shared title. Exercise them
    // while the channel refresh is still pending so a late result cannot
    // resurrect the channel name over the destination's header.
    const destinations = [
      ["recent", "Videos"],
      ["search", "Search transcripts"],
      ["graph", "Word frequency"],
      ["bookmarks", "Bookmarks"],
      ["manual", "Manual Downloads"],
      ["channels", "Channels"],
    ];
    for (const [submode, title] of destinations) {
      await page.locator(`.submode-btn[data-submode="${submode}"]`).click();
      await expect(page.locator("#browse-main-title")).toHaveText(title);
    }

    await page.evaluate(() => {
      window.__resolveHiddenChannelRefresh({
        rows: [{
          video_id: "landed-video",
          title: "Newly landed video",
          channel: "Example Channel",
          filepath: "C:\\FixtureArchive\\landed.mp4",
        }],
        next_offset: 1,
        has_more: false,
      });
    });
    await page.evaluate(() => window.__hiddenChannelRefresh);

    await expect(page.locator("#browse-main-title")).toHaveText("Channels");
    await expect(page.locator("#view-channels")).toBeVisible();
    await expect(page.locator("#channel-page-header")).toBeHidden();
    await expect(page.locator("#cph-info"))
      .not.toContainText("Queued to sync");
    await expect(page.locator("#cph-sync-now")).toBeEnabled();
    await expect(page.locator("#video-grid .video-card-title"))
      .toHaveText("Newly landed video");
    await expect.poll(() => page.evaluate(() =>
      window._browseState.videos.map((video) => video.video_id)))
      .toEqual(["landed-video"]);

    // Reproduce the second live symptom directly: the all-archive Videos
    // screen stayed visible, but a download event changed its shared title
    // back to the previously opened channel name.
    await page.locator('.submode-btn[data-submode="recent"]').click();
    await page.evaluate(() => {
      window.__resolveHiddenChannelRefresh = null;
      window.__hiddenChannelRefresh =
        window._refreshChannelVideosIfLoaded("Example Channel");
    });
    await expect.poll(() => page.evaluate(() =>
      typeof window.__resolveHiddenChannelRefresh)).toBe("function");
    await expect(page.locator("#browse-main-title")).toHaveText("Videos");
    await page.evaluate(() => {
      window.__resolveHiddenChannelRefresh({
        rows: [{
          video_id: "second-landed-video",
          title: "Second newly landed video",
          channel: "Example Channel",
          filepath: "C:\\FixtureArchive\\landed-2.mp4",
        }],
        next_offset: 1,
        has_more: false,
      });
    });
    await page.evaluate(() => window.__hiddenChannelRefresh);
    await expect(page.locator("#browse-main-title")).toHaveText("Videos");
    await expect(page.locator("#view-recent")).toBeVisible();
    await expect(page.locator("#channel-page-header")).toBeHidden();
  });

  test("opening another channel wins over a pending hidden refresh", async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => window.seedLogs());

    await page.evaluate(() => {
      window.__setBridgeHandler("browse_list_channels", () => [
        { folder: "Example Channel", name: "Example Channel" },
        { folder: "Another Channel", name: "Another Channel" },
      ]);
      window.__racingPageReads = 0;
      window.__setBridgeHandler("browse_list_videos_page", (name) => {
        window.__racingPageReads += 1;
        if (window.__racingPageReads === 2) {
          return new Promise((resolve) => {
            window.__resolveRacingChannelRefresh = resolve;
          });
        }
        return {
          rows: [{
            video_id: `${name}-current`,
            title: `${name} current video`,
            channel: name,
            filepath: `C:\\FixtureArchive\\${name}.mp4`,
          }],
          next_offset: 1,
          has_more: false,
        };
      });
      window.__primeChannelPromise = window._primeBrowse([
        { folder: "Example Channel", name: "Example Channel" },
        { folder: "Another Channel", name: "Another Channel" },
      ]);
    });

    await page.locator('.tab[data-tab="browse"]').click();
    await page.locator(
      '#channel-grid .channel-card[data-channel-name="Example Channel"]'
    ).click();
    await expect(page.locator("#video-grid .video-card-title"))
      .toHaveText("Example Channel current video");

    await page.locator('.submode-btn[data-submode="channels"]').click();
    await page.evaluate(() => {
      window.__racingChannelRefresh =
        window._refreshChannelVideosIfLoaded("Example Channel");
    });
    await expect.poll(() => page.evaluate(() =>
      typeof window.__resolveRacingChannelRefresh)).toBe("function");

    // This click queues a newer, authoritative channel-page read behind the
    // already-running background read.
    await page.locator(
      '#channel-grid .channel-card[data-channel-name="Another Channel"]'
    ).click();
    await expect(page.locator("#browse-main-title"))
      .toHaveText("Another Channel");

    await page.evaluate(() => {
      window.__resolveRacingChannelRefresh({
        rows: [{
          video_id: "stale-landed-video",
          title: "Stale landed video",
          channel: "Example Channel",
          filepath: "C:\\FixtureArchive\\stale.mp4",
        }],
        next_offset: 1,
        has_more: false,
      });
    });
    await page.evaluate(() => window.__racingChannelRefresh);

    await expect(page.locator("#browse-main-title"))
      .toHaveText("Another Channel");
    await expect(page.locator("#channel-page-header")).toBeVisible();
    await expect(page.locator("#video-grid .video-card-title"))
      .toHaveText("Another Channel current video");
    await expect.poll(() => page.evaluate(() => ({
      channel: window._browseState.currentChannel?.folder,
      videoIds: window._browseState.videos.map((video) => video.video_id),
    }))).toEqual({
      channel: "Another Channel",
      videoIds: ["Another Channel-current"],
    });
  });

  test("a second landed video is refreshed after an in-flight channel read", async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => window.seedLogs());

    await page.evaluate(() => {
      window.__setBridgeHandler("browse_list_channels", () => [{
        folder: "Fixture Channel", name: "Fixture Channel",
      }]);
      window.__landedReadCount = 0;
      window.__setBridgeHandler("browse_list_videos_page", () => {
        window.__landedReadCount += 1;
        if (window.__landedReadCount === 1) {
          return {
            rows: [{
              video_id: "existing",
              title: "Existing video",
              channel: "Fixture Channel",
              filepath: "C:\\FixtureArchive\\existing.mp4",
            }],
            next_offset: 1,
            has_more: false,
          };
        }
        if (window.__landedReadCount === 2) {
          return new Promise((resolve) => {
            window.__resolveFirstLandedRead = resolve;
          });
        }
        return {
          rows: [{
            video_id: "latest",
            title: "Latest landed video",
            channel: "Fixture Channel",
            filepath: "C:\\FixtureArchive\\latest.mp4",
          }],
          next_offset: 1,
          has_more: false,
        };
      });
      window.__primeLandedPromise = window._primeBrowse([{
        folder: "Fixture Channel", name: "Fixture Channel",
      }]);
    });

    await page.locator('.tab[data-tab="browse"]').click();
    await page.locator("#channel-grid .channel-card").click();
    await expect(page.locator("#video-grid .video-card-title"))
      .toHaveText("Existing video");

    await page.evaluate(() => {
      window.__firstLandedRefresh =
        window._refreshChannelVideosIfLoaded("Fixture Channel");
    });
    await expect.poll(() => page.evaluate(() =>
      typeof window.__resolveFirstLandedRead)).toBe("function");

    // This notification arrives while the first refresh owns the shared read.
    // It must schedule one follow-up rather than being silently discarded.
    await page.evaluate(() => {
      window._refreshChannelVideosIfLoaded("Fixture Channel");
      window.__resolveFirstLandedRead({
        rows: [{
          video_id: "first-landed",
          title: "First landed video",
          channel: "Fixture Channel",
          filepath: "C:\\FixtureArchive\\first.mp4",
        }],
        next_offset: 1,
        has_more: false,
      });
    });

    await expect.poll(() => page.evaluate(() => window.__landedReadCount)).toBe(3);
    await expect(page.locator("#video-grid .video-card-title"))
      .toHaveText("Latest landed video");
  });

  test("a complete channel page repairs stale counts without undercounting paginated channels", async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => window.seedLogs());

    await page.evaluate(() => {
      window.__setBridgeHandler("browse_list_channels", () => [
        { folder: "Complete Channel", name: "Complete Channel", n_vids: 0 },
        { folder: "Paginated Channel", name: "Paginated Channel", n_vids: 500 },
        { folder: "Error Channel", name: "Error Channel", n_vids: 9 },
      ]);
      window.__setBridgeHandler("browse_list_videos_page", (name) => {
        if (name === "Error Channel") {
          return {
            rows: [], next_offset: 0, has_more: false,
            error: "fixture catalog failure",
          };
        }
        const complete = name === "Complete Channel";
        const count = complete ? 5 : 2;
        return {
          rows: Array.from({ length: count }, (_unused, index) => ({
            video_id: `${complete ? "complete" : "paged"}-${index}`,
            title: `${name} video ${index + 1}`,
            channel: name,
            filepath: `C:\\FixtureArchive\\${name}-${index}.mp4`,
          })),
          next_offset: count,
          has_more: !complete,
        };
      });
      window.__countPrime = window._primeBrowse([
        {
          folder: "Complete Channel",
          name: "Complete Channel",
          // Physical UI pass reproduced the most confusing form of this:
          // the card said 0 while opening it immediately showed five videos.
          n_vids: 0,
        },
        {
          folder: "Paginated Channel",
          name: "Paginated Channel",
          n_vids: 500,
        },
        {
          folder: "Error Channel",
          name: "Error Channel",
          n_vids: 9,
        },
      ]);
    });

    await page.locator('.tab[data-tab="browse"]').click();
    const completeCard = page.locator(
      '#channel-grid .channel-card[data-channel-name="Complete Channel"]');
    await page.evaluate(() => {
      window.__completeCardBeforeCountRepair = document.querySelector(
        '#channel-grid .channel-card[data-channel-name="Complete Channel"]');
    });
    await completeCard.click();
    await expect(page.locator("#video-grid .video-card-title")).toHaveCount(5);
    await expect(page.locator("#cph-info")).toContainText("5 videos");
    await expect(completeCard.locator(".channel-card-meta"))
      .toContainText("5 videos");
    await expect.poll(() => page.evaluate(() =>
      window.__completeCardBeforeCountRepair?.isConnected
      && window.__completeCardBeforeCountRepair === document.querySelector(
        '#channel-grid .channel-card[data-channel-name="Complete Channel"]')))
      .toBe(true);

    await page.locator('.submode-btn[data-submode="channels"]').click();
    await expect(completeCard.locator(".channel-card-meta"))
      .toContainText("5 videos");

    const paginatedCard = page.locator(
      '#channel-grid .channel-card[data-channel-name="Paginated Channel"]');
    await paginatedCard.click();
    await expect(page.locator("#video-grid .video-card-title")).toHaveCount(2);
    await expect(page.locator("#cph-info")).toContainText("500 videos");
    await expect(paginatedCard.locator(".channel-card-meta"))
      .toContainText("500 videos");

    await page.locator('.submode-btn[data-submode="channels"]').click();
    const errorCard = page.locator(
      '#channel-grid .channel-card[data-channel-name="Error Channel"]');
    await errorCard.click();
    await expect(page.locator("#cph-info")).toContainText("9 videos");
    await expect(errorCard.locator(".channel-card-meta"))
      .toContainText("9 videos");
  });

  test("channel completion clears its header and performs one final catalog read", async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => window.seedLogs());

    await page.evaluate(() => {
      window.__setBridgeHandler("browse_list_channels", () => [{
        folder: "Completion Channel",
        name: "Completion Channel",
        n_vids: 4,
        size: "40 MB",
      }]);
      window.__channelSyncComplete = false;
      window.__channelSyncReads = 0;
      window.__setBridgeHandler("browse_list_videos_page", () => {
        window.__channelSyncReads += 1;
        const count = window.__channelSyncComplete ? 5 : 4;
        return {
          rows: Array.from({ length: count }, (_unused, index) => ({
            video_id: `completion-${index}`,
            title: `Completion video ${index + 1}`,
            channel: "Completion Channel",
            filepath: `C:\\FixtureArchive\\completion-${index}.mp4`,
          })),
          next_offset: count,
          has_more: false,
        };
      });
      window.__setBridgeHandler("sync_one_channel", () => {
        window.renderQueues({
          sync: [{
            task_id: "completion-sync",
            represented_task_ids: ["completion-sync"],
            name: "Downloading Completion Channel",
            channel_name: "Completion Channel",
            status: "running",
          }],
          gpu: [],
          sync_count: 1,
          gpu_count: 0,
          identity_ids_durable: true,
        });
        return {
          ok: true,
          started: true,
          name: "Completion Channel",
        };
      });
      window.__setBridgeHandler("get_subs_channels", () => [[{
        folder: "Completion Channel",
        n_vids: 5,
        size: "50 MB",
      }], "1 channel"]);
      window.__completionPrime = window._primeBrowse([{
        folder: "Completion Channel",
        name: "Completion Channel",
        n_vids: 4,
        size: "40 MB",
      }]);
    });

    await page.locator('.tab[data-tab="browse"]').click();
    await page.locator(
      '#channel-grid .channel-card[data-channel-name="Completion Channel"]'
    ).click();
    await expect(page.locator("#video-grid .video-card-title")).toHaveCount(4);
    await expect.poll(() => page.evaluate(() => window.__channelSyncReads))
      .toBe(1);

    await page.locator("#cph-sync-now").click();
    await expect(page.locator("#cph-info")).toContainText("Syncing now");
    await expect(page.locator("#cph-sync-now")).toBeDisabled();
    await expect.poll(() => page.evaluate(() => window.__channelSyncReads))
      .toBe(1);

    // The normal lightweight post-channel Subs refresh updates count/size
    // without paying for another all-channel art scan.
    await page.evaluate(() =>
      window.refreshSubsTable({ primeBrowse: false }));
    await expect(page.locator("#cph-info")).toContainText("5 videos");
    await expect(page.locator("#cph-info")).toContainText("50 MB");
    await expect(page.locator(
      '#channel-grid .channel-card[data-channel-name="Completion Channel"] '
      + '.channel-card-meta')).toContainText("5 videos");

    // Completion can arrive while Watch (or another Browse destination) is
    // covering the channel page. Returning must reveal fresh hidden chrome.
    await page.evaluate(() => window.showView("watch"));
    await expect(page.locator("#view-watch")).toBeVisible();

    await page.evaluate(() => {
      window.__channelSyncComplete = true;
      window.renderQueues({
        sync: [], gpu: [], sync_count: 0, gpu_count: 0,
        identity_ids_durable: true,
      });
    });

    await expect(page.locator("#cph-info")).not.toContainText("Syncing now");
    await expect(page.locator("#cph-sync-now")).toBeEnabled();
    await expect(page.locator("#video-grid .video-card-title")).toHaveCount(5);
    await expect.poll(() => page.evaluate(() => window.__channelSyncReads))
      .toBe(2);
    await page.evaluate(() => window.showView("videos"));
    await expect(page.locator("#cph-info")).not.toContainText("Syncing now");
    await expect(page.locator("#cph-sync-now")).toBeEnabled();
  });

  test("Videos keeps one Python read active, reports a queued wait, and paints only the latest sort", async ({ page }) => {
    await loadApp(page);

    await page.evaluate(() => {
      window.__videoReadActive = 0;
      window.__videoReadMax = 0;
      window.__videoReadSorts = [];
      window.__setBridgeHandler("list_all_videos", (sort) => {
        window.__videoReadSorts.push(sort);
        window.__videoReadActive += 1;
        window.__videoReadMax = Math.max(
          window.__videoReadMax, window.__videoReadActive);
        const delay = sort === "recent" ? 3600 : 40;
        return new Promise((resolve) => setTimeout(() => {
          window.__videoReadActive -= 1;
          resolve({
            rows: [{
              video_id: `video-${sort}`,
              title: `${sort} result`,
              channel: "Fixture Channel",
              filepath: `C:\\FixtureArchive\\${sort}.mp4`,
            }],
            has_more: false,
          });
        }, delay));
      });
      window.setQueueState({
        sync: { running: true, paused: false },
        gpu: { running: false, paused: false },
      });
    });

    await page.locator('.tab[data-tab="browse"]').click();
    await page.locator('.submode-btn[data-submode="recent"]').click();
    await expect.poll(() => page.evaluate(() =>
      window.__bridgeCallsFor("list_all_videos").length)).toBe(1);

    // Two changes while the first read is still in Python: the middle one is
    // superseded before it starts, and only the final sort is retained.
    await page.locator("#videos-sort").selectOption("newest");
    await page.waitForTimeout(30);
    await page.locator("#videos-sort").selectOption("oldest");

    await expect(page.locator("#recent-grid .grid-loading-label"))
      .toContainText(
        "Waiting to refresh videos while an earlier request for videos finishes");
    await expect(page.locator("#recent-grid .video-card-title"))
      .toHaveText("oldest result");
    await expect(page.locator("#recent-grid"))
      .not.toContainText("recent result");
    await expect(page.locator("#recent-grid"))
      .not.toContainText("newest result");

    const readState = await page.evaluate(() => ({
      maxActive: window.__videoReadMax,
      sorts: window.__videoReadSorts,
    }));
    expect(readState).toEqual({ maxActive: 1, sorts: ["recent", "oldest"] });
  });

  test("combined Search runs its transcript and title reads sequentially", async ({ page }) => {
    await loadApp(page);

    await page.evaluate(() => {
      window.__searchReadActive = 0;
      window.__searchReadMax = 0;
      const responseFor = (kind) => new Promise((resolve) => {
        window.__searchReadActive += 1;
        window.__searchReadMax = Math.max(
          window.__searchReadMax, window.__searchReadActive);
        setTimeout(() => {
          window.__searchReadActive -= 1;
          resolve([{
            video_id: `${kind}-video`,
            title: `${kind} match`,
            channel: "Fixture Channel",
            snippet: `${kind} fixture text`,
            start_time: kind === "transcript" ? 12 : 0,
          }]);
        }, 90);
      });
      window.__setBridgeHandler(
        "browse_search", () => responseFor("transcript"));
      window.__setBridgeHandler(
        "browse_search_titles", () => responseFor("title"));
    });

    await page.locator('.tab[data-tab="browse"]').click();
    await page.locator('.submode-btn[data-submode="search"]').click();
    await page.locator("#search-query").fill("fixture");
    await page.locator("#btn-search-run").click();

    await expect(page.locator("#search-count")).toContainText("2 matches");
    await expect(page.locator("#search-results .search-result"))
      .toHaveCount(2);
    await expect.poll(() => page.evaluate(() =>
      window.__searchReadMax)).toBe(1);
    const calls = await page.evaluate(() => ({
      transcripts: window.__bridgeCallsFor("browse_search").length,
      titles: window.__bridgeCallsFor("browse_search_titles").length,
    }));
    expect(calls).toEqual({ transcripts: 1, titles: 1 });
  });

  test("the latest Search-result double-click wins when file resolution finishes out of order", async ({ page }) => {
    await loadApp(page);
    await page.locator('.tab[data-tab="browse"]').click();
    await page.locator('.submode-btn[data-submode="search"]').click();

    await page.evaluate(() => {
      window.__segmentResolvers = Object.create(null);
      window.__setBridgeHandler(
        "browse_resolve_segment",
        async (_jsonlPath, videoId, title) => new Promise((resolve) => {
          window.__segmentResolvers[videoId] = (filepath) => resolve({
            ok: true,
            filepath,
            video_id: videoId,
            title,
            channel: "Fixture Channel",
          });
        }));
      window.__setBridgeHandler("browse_get_transcript", async (identity) => ({
        segments: [{ s: 0, e: 1, t: `Transcript for ${identity.video_id}` }],
      }));
      window.renderSearchResults(
        document.getElementById("search-results"),
        [
          {
            jsonl_path: "first.jsonl",
            video_id: "first-video",
            title: "First video",
            channel: "Fixture Channel",
            start_time: 12,
            timestamp: "0:12",
            snippet: "first result",
            _search_query: "result",
          },
          {
            jsonl_path: "second.jsonl",
            video_id: "second-video",
            title: "Second video",
            channel: "Fixture Channel",
            start_time: 34,
            timestamp: "0:34",
            snippet: "second result",
            _search_query: "result",
          },
        ],
        "result",
      );
    });

    const hits = page.locator("#search-results .search-result");
    await expect(hits).toHaveCount(2);
    await hits.nth(0).dblclick();
    await hits.nth(1).dblclick();
    await expect.poll(() => page.evaluate(() =>
      Object.keys(window.__segmentResolvers).sort()))
      .toEqual(["first-video", "second-video"]);

    // The newer click resolves first and owns Watch.
    await page.evaluate(() => {
      window.__segmentResolvers["second-video"]("C:\\Fixture\\second.mp4");
    });
    await expect.poll(() => page.evaluate(() =>
      window._browseState.currentVideo?.video_id)).toBe("second-video");

    // The older resolution arrives late and must not replace it.
    await page.evaluate(() => {
      window.__segmentResolvers["first-video"]("C:\\Fixture\\first.mp4");
    });
    await page.waitForTimeout(100);
    await expect.poll(() => page.evaluate(() => ({
      videoId: window._browseState.currentVideo?.video_id,
      title: window._browseState.currentVideo?.title,
    }))).toEqual({ videoId: "second-video", title: "Second video" });
  });

  test("Search channel picker loads independently and reports failures honestly", async ({ page }) => {
    await loadApp(page);

    await page.evaluate(() => {
      window._subsAllRows = [];
      window._browseState.channels = [];
      window.__setBridgeHandler("browse_search", () =>
        new Promise((resolve) => {
          window.__searchPickerBlockerStarted = true;
          window.__releaseSearchPickerBlocker = resolve;
        }));
      window.__setBridgeHandler("browse_search_titles", () => []);
      window.__setBridgeHandler("browse_list_channels", () =>
        new Promise((resolve) => setTimeout(() => resolve([{
          folder: "Picker Channel", name: "Picker Channel",
        }]), 80)));
    });

    await page.locator('.tab[data-tab="browse"]').click();
    await page.locator('.submode-btn[data-submode="search"]').click();
    await page.locator("#search-query").fill("fixture");
    await page.locator("#btn-search-run").click();
    await expect.poll(() => page.evaluate(() =>
      window.__searchPickerBlockerStarted)).toBe(true);
    await page.locator("#search-channel-trigger").click();

    await expect(page.locator("#search-channel-panel")).toBeVisible();
    await expect(page.locator("#search-channel-list"))
      .toContainText(
        "Waiting to refresh search channels while another library view finishes");
    await page.evaluate(() => window.__releaseSearchPickerBlocker([]));
    await expect(page.locator("#search-channel-list .search-channel-opt span"))
      .toHaveText("Picker Channel");
    const calls = await page.evaluate(() => ({
      channels: window.__bridgeCallsFor("browse_list_channels").length,
      searchContext: window.__bridgeCallsFor("browse_search_context").length,
    }));
    expect(calls.channels).toBeGreaterThanOrEqual(1);
    expect(calls.searchContext).toBe(0);

    await page.locator("#search-channel-trigger").click();
    await page.evaluate(() => {
      window.__setBridgeHandler("browse_list_channels", () =>
        Promise.reject(new Error("fixture channel failure")));
    });
    await page.locator("#search-channel-trigger").click();
    await expect(page.locator("#search-channel-list")).toHaveText(
      "Couldn’t load channels. Close this list and try again.");
  });

  test("a title-only Search hit explains why transcript context is unavailable", async ({ page }) => {
    await loadApp(page);

    await page.evaluate(() => {
      window.__setBridgeHandler("browse_search", () => []);
      window.__setBridgeHandler("browse_search_titles", () => [{
        video_id: "desktop-occupant",
        title: "Desktop Occupant",
        channel: "Fixture Channel",
        snippet: "Desktop Occupant",
        start_time: 0,
      }]);
      window.__setBridgeHandler("browse_resolve_segment", () => ({
        ok: true,
        filepath: "C:\\FixtureArchive\\Desktop Occupant.mp4",
        video_id: "desktop-occupant",
        title: "Desktop Occupant",
        channel: "Fixture Channel",
      }));
      window.__openedTitleSearchHit = null;
      window._openVideoInWatch = async (video) => {
        window.__openedTitleSearchHit = video;
      };
    });

    await page.locator('.tab[data-tab="browse"]').click();
    await page.locator('.submode-btn[data-submode="search"]').click();
    await page.locator("#search-query").fill("Desktop Occupant");
    await page.locator("#btn-search-run").click();

    const hit = page.locator("#search-results .search-result");
    await expect(hit).toHaveCount(1);
    await hit.click();
    await expect(page.locator("#search-viewer-body")).toHaveText(
      "No transcript available for this title match. "
      + "Double-click to open the video.");
    await expect.poll(() => page.evaluate(() =>
      window.__bridgeCallsFor("browse_search_context").length)).toBe(0);

    await hit.dblclick();
    await expect.poll(() => page.evaluate(() =>
      window.__bridgeCallsFor("browse_resolve_segment").length)).toBe(1);
    await expect.poll(() => page.evaluate(() =>
      window.__openedTitleSearchHit?.video_id)).toBe("desktop-occupant");
  });

  test("Graph queues only the latest plot while an older query finishes", async ({ page }) => {
    await loadApp(page);

    await page.evaluate(() => {
      window._browseState.channels = [{
        folder: "Fixture Channel", name: "Fixture Channel",
      }];
      window.__graphReadActive = 0;
      window.__graphReadMax = 0;
      window.__graphWords = [];
      window.__setBridgeHandler("browse_graph", (word) => {
        window.__graphWords.push(word);
        window.__graphReadActive += 1;
        window.__graphReadMax = Math.max(
          window.__graphReadMax, window.__graphReadActive);
        return new Promise((resolve) => setTimeout(() => {
          window.__graphReadActive -= 1;
          resolve(word === "second"
            ? { error: "Latest graph query" }
            : { labels: ["2026-01"], values: [1], word });
        }, word === "first" ? 180 : 25));
      });
    });

    await page.locator('.tab[data-tab="browse"]').click();
    await page.locator('.submode-btn[data-submode="graph"]').click();
    await page.locator("#graph-word").fill("first");
    await page.locator("#btn-graph-run").click();
    await page.waitForTimeout(15);
    await page.locator("#graph-word").fill("second");
    await page.locator("#btn-graph-run").click();

    await expect(page.locator("#graph-empty"))
      .toHaveText("Latest graph query");
    const state = await page.evaluate(() => ({
      maxActive: window.__graphReadMax,
      words: window.__graphWords,
    }));
    expect(state).toEqual({ maxActive: 1, words: ["first", "second"] });
  });

  test("Graph channel loading and plotting do not supersede each other", async ({ page }) => {
    await loadApp(page);

    await page.evaluate(() => {
      window._browseState.channels = [];
      window.__graphChannelCallBase =
        window.__bridgeCallsFor("browse_list_channels").length;
      window.__graphPlotCallBase =
        window.__bridgeCallsFor("browse_graph").length;
      window.__setBridgeHandler("browse_list_channels", () =>
        new Promise((resolve) => setTimeout(() => resolve([{
          folder: "Cold Channel", name: "Cold Channel",
        }]), 90)));
      window.__setBridgeHandler("browse_graph", (word) =>
        new Promise((resolve) => setTimeout(() => resolve({
          labels: ["2026-01"], values: [2], word,
        }), 20)));
    });

    await page.locator('.tab[data-tab="browse"]').click();
    await page.locator('.submode-btn[data-submode="graph"]').click();
    await page.locator("#graph-word").fill("fixture");
    await page.locator("#btn-graph-run").click();

    await expect(page.locator("#graph-channel option"))
      .toHaveText(["All", "Cold Channel"]);
    await expect(page.locator("#graph-empty")).toHaveText("");
    const calls = await page.evaluate(() => ({
      channels: window.__bridgeCallsFor("browse_list_channels").length
        - window.__graphChannelCallBase,
      plots: window.__bridgeCallsFor("browse_graph").length
        - window.__graphPlotCallBase,
    }));
    expect(calls).toEqual({ channels: 1, plots: 1 });
  });

  test("Graph gives a single point a centered, forgiving drill target", async ({ page }) => {
    await loadApp(page);

    await page.evaluate(() => {
      window._browseState.channels = [{
        folder: "Fixture Channel", name: "Fixture Channel",
      }];
      window.__graphConfig = null;
      window.Chart = function ChartFixture(_context, config) {
        window.__graphConfig = config;
        this.destroy = () => {};
        this.resize = () => {};
      };
      window.__setBridgeHandler("browse_graph", () => ({
        labels: ["2026-08"], values: [1], word: "Perfios", bucket: "month",
      }));
    });

    await page.locator('.tab[data-tab="browse"]').click();
    await page.locator('.submode-btn[data-submode="graph"]').click();
    await page.locator("#graph-word").fill("Perfios");
    await page.locator("#btn-graph-run").click();
    await expect.poll(() => page.evaluate(() => !!window.__graphConfig)).toBe(true);

    const config = await page.evaluate(() => ({
      pointHitRadius: window.__graphConfig.data.datasets[0].pointHitRadius,
      xOffset: window.__graphConfig.options.scales.x.offset,
      intersect: window.__graphConfig.options.interaction.intersect,
      leftPadding: window.__graphConfig.options.layout.padding.left,
    }));
    expect(config).toEqual({
      pointHitRadius: 14,
      xOffset: true,
      intersect: false,
      leftPadding: 10,
    });
  });

  test("Graph drills into transcript-only Search with the clicked plot word", async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => {
      window._browseState.channels = [{
        folder: "Fixture Channel", name: "Fixture Channel",
      }];
      window.__multiGraphConfig = null;
      window.Chart = function ChartFixture(_context, config) {
        window.__multiGraphConfig = config;
        this.destroy = () => {};
        this.resize = () => {};
      };
      window.__setBridgeHandler("browse_graph", () => ({
        labels: ["2026-08"],
        series: [
          { word: "cat", values: [1] },
          { word: "dog", values: [2] },
        ],
      }));
      window.__setBridgeHandler("browse_search", () => []);
      window.__setBridgeHandler("browse_search_titles", () => []);
    });

    await page.locator('.tab[data-tab="browse"]').click();
    await page.locator('.submode-btn[data-submode="search"]').click();
    // Reproduce a user's previous title-only Search selection. The hidden
    // compatibility scope changes with these checkboxes, so the drill must
    // reset both the visible controls and that legacy state.
    await page.locator("#search-in-transcripts").uncheck();
    await expect(page.locator("#search-in-titles")).toBeChecked();
    await page.locator('.submode-btn[data-submode="graph"]').click();
    await page.locator("#graph-word").fill("cat, dog");
    await page.locator("#btn-graph-run").click();
    await expect.poll(() => page.evaluate(() =>
      !!window.__multiGraphConfig)).toBe(true);

    await page.evaluate(() => {
      window.__multiGraphConfig.options.onClick({}, [{
        datasetIndex: 1,
        index: 0,
      }]);
    });
    await expect(page.locator("#search-query")).toHaveValue("dog");
    await expect(page.locator("#search-in-transcripts")).toBeChecked();
    await expect(page.locator("#search-in-titles")).not.toBeChecked();
    await expect(page.locator("#search-count")).toHaveText("0 matches");
    const calls = await page.evaluate(() => ({
      transcripts: window.__bridgeCallsFor("browse_search").length,
      titles: window.__bridgeCallsFor("browse_search_titles").length,
    }));
    expect(calls).toEqual({ transcripts: 1, titles: 0 });
  });

  test("Manual sort changes keep only the latest queued file listing", async ({ page }) => {
    await loadApp(page);

    await page.evaluate(() => {
      window.__manualReadActive = 0;
      window.__manualReadMax = 0;
      window.__manualSorts = [];
      window.__setBridgeHandler("list_manual_videos", (sort) => {
        window.__manualSorts.push(sort);
        window.__manualReadActive += 1;
        window.__manualReadMax = Math.max(
          window.__manualReadMax, window.__manualReadActive);
        return new Promise((resolve) => setTimeout(() => {
          window.__manualReadActive -= 1;
          resolve({
            rows: [{
              title: `${sort} manual result`,
              filepath: `C:\\FixtureArchive\\${sort}.mp4`,
              video_id: `manual-${sort}`,
            }],
            folder: "C:\\FixtureArchive",
            total: 1,
            has_more: false,
          });
        }, sort === "newest" ? 180 : 25));
      });
    });

    await page.locator('.tab[data-tab="browse"]').click();
    await page.locator('.submode-btn[data-submode="manual"]').click();
    await expect.poll(() => page.evaluate(() =>
      window.__bridgeCallsFor("list_manual_videos").length)).toBe(1);
    await page.locator("#manual-sort").selectOption("oldest");
    await page.waitForTimeout(15);
    await page.locator("#manual-sort").selectOption("largest");

    await expect(page.locator("#manual-grid .video-card-title"))
      .toHaveText("largest manual result");
    const state = await page.evaluate(() => ({
      maxActive: window.__manualReadMax,
      sorts: window.__manualSorts,
    }));
    expect(state).toEqual({ maxActive: 1, sorts: ["newest", "largest"] });
  });

  test("Manual refresh failure clears the cached-card dimming state", async ({ page }) => {
    await loadApp(page);

    await page.evaluate(() => {
      localStorage.setItem("ytarchiver_manual_page1", JSON.stringify([{
        title: "Cached Manual Video",
        filepath: "C:\\FixtureArchive\\cached.mp4",
        video_id: "cached-manual",
      }]));
      window.__setBridgeHandler("list_manual_videos", () =>
        Promise.reject(new Error("fixture manual failure")));
    });

    await page.locator('.tab[data-tab="browse"]').click();
    await page.locator('.submode-btn[data-submode="manual"]').click();
    await expect(page.locator("#manual-grid"))
      .toContainText("Couldn’t load files.");
    await expect(page.locator("#manual-grid")).not.toHaveClass(/is-refreshing/);
  });

  test("Metadata reloads do not overlap and stale rows never replace the latest table", async ({ page }) => {
    await loadApp(page);

    // Metadata can schedule an initial refresh 400 ms after startup when the
    // Library view is active. Keep that unrelated timer out of this
    // explicit two-refresh test, then measure calls from a clean baseline.
    await page.evaluate(() => {
      const view = document.getElementById("settings-view-library");
      if (view) view.hidden = true;
    });
    await page.waitForTimeout(450);

    await page.evaluate(() => {
      window.__metaReadActive = 0;
      window.__metaReadMax = 0;
      window.__metaStatusCalls = 0;
      window.__metaThumbnailBase =
        window.__bridgeCallsFor("thumbnail_status_bulk").length;
      const tracked = (delay, value) => {
        window.__metaReadActive += 1;
        window.__metaReadMax = Math.max(
          window.__metaReadMax, window.__metaReadActive);
        return new Promise((resolve) => setTimeout(() => {
          window.__metaReadActive -= 1;
          resolve(value);
        }, delay));
      };
      window.__setBridgeHandler("get_channel_metadata_status", () => {
        const call = ++window.__metaStatusCalls;
        return tracked(call === 1 ? 180 : 35, [{
          name: call === 1 ? "Stale Metadata Channel" : "Final Metadata Channel",
          folder: call === 1 ? "Stale Metadata Channel" : "Final Metadata Channel",
          video_count: call,
        }]);
      });
      window.__setBridgeHandler("thumbnail_status_bulk", () =>
        tracked(45, { rows: {} }));
    });

    await page.evaluate(async () => {
      const first = window._refreshMetadataTab({ force: true });
      await new Promise((resolve) => setTimeout(resolve, 15));
      const second = window._refreshMetadataTab({ force: true });
      await Promise.all([first, second]);
    });

    await expect(page.locator("#metadata-tbody"))
      .toContainText("Final Metadata Channel");
    await expect(page.locator("#metadata-tbody"))
      .not.toContainText("Stale Metadata Channel");
    await expect.poll(() => page.evaluate(() =>
      window.__metaReadActive)).toBe(0);
    const readState = await page.evaluate(() => ({
      maxActive: window.__metaReadMax,
      statusCalls: window.__metaStatusCalls,
      thumbnailCalls: window.__bridgeCallsFor("thumbnail_status_bulk").length
        - window.__metaThumbnailBase,
    }));
    expect(readState).toEqual({
      maxActive: 1,
      statusCalls: 2,
      thumbnailCalls: 1,
    });
  });

  test("Index refreshes retain only the newest detailed-stat request", async ({ page }) => {
    await loadApp(page);

    await page.evaluate(() => {
      window.__indexReadActive = 0;
      window.__indexReadMax = 0;
      window.__indexSummaryCalls = 0;
      window.__indexDetailBase =
        window.__bridgeCallsFor("get_index_db_stats").length;
      const tracked = (delay, value) => {
        window.__indexReadActive += 1;
        window.__indexReadMax = Math.max(
          window.__indexReadMax, window.__indexReadActive);
        return new Promise((resolve) => setTimeout(() => {
          window.__indexReadActive -= 1;
          resolve(value);
        }, delay));
      };
      window.__setBridgeHandler("get_index_summary", () => {
        const call = ++window.__indexSummaryCalls;
        return tracked(call === 1 ? 160 : 25, {
          cards: {
            channels: call === 1 ? 1 : 9,
            videos: call === 1 ? 2 : 18,
            physical_copies: call === 1 ? 3 : 21,
          },
        });
      });
      window.__setBridgeHandler("get_index_db_stats", () =>
        tracked(35, {
          segments: 99,
          hours: 12.5,
          total_videos: 18,
          transcribed_videos: 9,
          index_db_size_label: "5 MB",
        }));
    });

    await page.evaluate(async () => {
      const first = window._refreshIndexStats();
      await new Promise((resolve) => setTimeout(resolve, 10));
      const middle = window._refreshIndexStats();
      await new Promise((resolve) => setTimeout(resolve, 10));
      const latest = window._refreshIndexStats();
      await Promise.all([first, middle, latest]);
    });

    await expect(page.locator("#index-stats-text"))
      .toContainText("Channels: 9");
    await expect(page.locator("#index-stats-text"))
      .toContainText("Segments: 99");
    const readState = await page.evaluate(() => ({
      maxActive: window.__indexReadMax,
      summaryCalls: window.__indexSummaryCalls,
      detailCalls: window.__bridgeCallsFor("get_index_db_stats").length
        - window.__indexDetailBase,
    }));
    expect(readState).toEqual({
      maxActive: 1,
      summaryCalls: 2,
      detailCalls: 1,
    });
  });

  test("Index detailed-stat error objects are not displayed as real zeros", async ({ page }) => {
    await loadApp(page);

    await page.evaluate(() => {
      window.__setBridgeHandler("get_index_summary", () => ({
        cards: { channels: 4, videos: 8, physical_copies: 8 },
      }));
      window.__setBridgeHandler("get_index_db_stats", () => ({
        segments: 0,
        hours: 0,
        index_db_bytes: 0,
        index_db_size_label: "—",
        error: "fixture database failure",
      }));
    });

    await page.evaluate(() => window._refreshIndexStats());
    await expect(page.locator("#index-stats-text"))
      .toContainText("Segments: —");
    await expect(page.locator("#index-stats-text"))
      .toContainText("Video hours: —");
    await expect(page.locator("#index-stats-text .index-stats-note"))
      .toHaveText("Detailed statistics could not be loaded. Try again later.");
  });
});
