const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

async function appendFixtureVideoCard(page, gridId, video) {
  await page.evaluate(({ gridId, video }) => {
    const grid = document.getElementById(gridId);
    if (!grid) throw new Error(`Missing fixture grid: ${gridId}`);
    const card = window._buildVideoCard(video, () => {});
    card.dataset.uiActionFixture = gridId;
    if (gridId === "recent-grid") card.dataset.tracked = "1";
    grid.appendChild(card);
  }, { gridId, video });
}

async function openCardMenu(page, gridId) {
  const card = page.locator(`[data-ui-action-fixture="${gridId}"]`);
  await card.evaluate((element) => {
    element.dispatchEvent(new MouseEvent("contextmenu", {
      bubbles: true,
      clientX: 160,
      clientY: 160,
    }));
  });
  await expect(page.getByRole("menu")).toBeVisible();
}

test.describe("user actions report backend failures truthfully", () => {
  test("Watch redownload reports a resolved failure and restores its control", async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => {
      const video = {
        video_id: "watch-redownload-fixture",
        title: "Watch redownload fixture",
        channel: "Fixture Channel",
        filepath: "C:\\FixtureArchive\\watch-redownload-fixture.mp4",
      };
      window._browseState.currentVideo = video;
      window._watchCurrentVideo = video;
      window._watchRenderedToken = window._watchOpenToken;
      window.askChoice = async () => "720";
      window.__setBridgeHandler("video_redownload", async () => ({
        ok: false,
        error: "The redownload queue refused this video.",
      }));
      document.getElementById("btn-watch-redownload")?.click();
    });

    await expect.poll(() => page.evaluate(() =>
      window.__bridgeCallsFor("video_redownload").length)).toBe(1);
    expect(await page.evaluate(() =>
      window.__bridgeCallsFor("video_redownload")[0].args))
      .toEqual(["watch-redownload-fixture", "Watch redownload fixture", "720",
        "C:\\FixtureArchive\\watch-redownload-fixture.mp4"]);
    await expect(page.locator("#toast-root .toast.error").last())
      .toHaveText("The redownload queue refused this video.");
    await expect(page.locator("#toast-root")).not.toContainText(
      "Redownload queued at 720.");
    await expect(page.locator("#btn-watch-redownload")).not.toBeDisabled();
    await expect(page.locator("#btn-watch-redownload"))
      .not.toHaveAttribute("aria-busy", "true");
  });

  test("Browse, Recent, and Manual transcription actions show resolved failures", async ({ page }) => {
    await loadApp(page);
    const grids = ["video-grid", "recent-grid", "manual-grid"];
    await page.evaluate(() => {
      window._askWhisperModel = async () => "small";
      window.__setBridgeHandler("transcribe_enqueue", async (filepath) => ({
        ok: false,
        error: `Could not queue ${filepath.split("\\").pop()}.`,
      }));
    });

    for (const gridId of grids) {
      const filepath = `C:\\FixtureArchive\\${gridId}-failure.mp4`;
      await appendFixtureVideoCard(page, gridId, {
        video_id: `${gridId}-failure`,
        title: `${gridId} failure fixture`,
        channel: "Fixture Channel",
        filepath,
      });
      await openCardMenu(page, gridId);
      await page.getByRole("menuitem", {
        name: "Transcribe now",
        exact: true,
      }).click();

      const expectedCallCount = grids.indexOf(gridId) + 1;
      await expect.poll(() => page.evaluate(() =>
        window.__bridgeCallsFor("transcribe_enqueue").length))
        .toBe(expectedCallCount);
      await expect(page.locator("#toast-root .toast.error").last())
        .toHaveText(`Could not queue ${gridId}-failure.mp4.`);
    }

    const calls = await page.evaluate(() =>
      window.__bridgeCallsFor("transcribe_enqueue").map((call) => call.args));
    expect(calls).toEqual(grids.map((gridId) => [
      `C:\\FixtureArchive\\${gridId}-failure.mp4`,
      `${gridId} failure fixture`,
      "small",
    ]));
    await expect(page.locator("#toast-root")).not.toContainText(
      "Queued for Whisper.");
  });

  test("Browse-card redownload does not claim success when the queue rejects it", async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => {
      window._askVideoRedownload = async () => "1080";
      window.__setBridgeHandler("video_redownload", async () => ({
        ok: false,
        error: "That video cannot be redownloaded right now.",
      }));
    });
    await appendFixtureVideoCard(page, "video-grid", {
      video_id: "browse-redownload-failure",
      title: "Browse redownload failure",
      channel: "Fixture Channel",
      filepath: "C:\\FixtureArchive\\browse-redownload-failure.mp4",
    });

    await openCardMenu(page, "video-grid");
    await page.getByRole("menuitem", { name: "Redownload…", exact: true }).click();

    await expect.poll(() => page.evaluate(() =>
      window.__bridgeCallsFor("video_redownload").length)).toBe(1);
    await expect(page.locator("#toast-root .toast.error").last())
      .toHaveText("That video cannot be redownloaded right now.");
    await expect(page.locator("#toast-root")).not.toContainText(
      "Redownload queued at 1080.");
  });
});

test.describe("state-changing controls recover from failures", () => {
  for (const [kind, checkboxId] of [
    ["sync", "sync-auto-checkbox"],
    ["gpu", "gpu-auto-checkbox"],
  ]) {
    test(`Queue Auto ${kind} rolls back when its setting cannot be saved`, async ({ page }) => {
      await loadApp(page);
      await page.evaluate(({ kind }) => {
        window.__setBridgeHandler("queue_auto_set", async () => ({
          ok: false,
          error: `Could not save ${kind} Auto.`,
        }));
      }, { kind });

      const checkbox = page.locator(`#${checkboxId}`);
      await page.locator(kind === "sync" ? "#btn-sync-tasks" : "#btn-gpu-tasks")
        .click();
      // The generic bridge fixture has no autorun label, which autoSync.js
      // conservatively treats as an active schedule and locks Sync Auto on.
      // Put this focused persistence test into the normal "schedule Off"
      // state before exercising the user's toggle.
      await checkbox.evaluate((element) => {
        element.disabled = false;
        element.checked = false;
      });
      await expect(checkbox).not.toBeChecked();
      const callsBefore = await page.evaluate((kind) =>
        window.__bridgeCallsFor("queue_auto_set")
          .filter((call) => call.args[0] === kind).length, kind);
      // click(), rather than check(), is intentional: the correct async
      // result is for the control to roll itself back to unchecked before
      // Playwright's check() postcondition would run.
      await checkbox.click();

      await expect.poll(() => page.evaluate((kind) =>
        window.__bridgeCallsFor("queue_auto_set")
          .filter((call) => call.args[0] === kind).length, kind))
        .toBe(callsBefore + 1);
      expect(await page.evaluate((kind) =>
        window.__bridgeCallsFor("queue_auto_set")
          .filter((call) => call.args[0] === kind).at(-1).args, kind))
        .toEqual([kind, true]);
      await expect(checkbox).not.toBeChecked();
      await expect(checkbox).not.toBeDisabled();
      await expect(checkbox).not.toHaveAttribute("aria-busy", "true");
      await expect(page.locator("#toast-root .toast.error").last())
        .toHaveText(`Could not save ${kind} Auto.`);
    });
  }

  test("a trashed Browse video stays gone after sorting and grouping", async ({ page }) => {
    await loadApp(page);
    await page.locator('.tab[data-tab="browse"]').click();
    const keptRow = {
      video_id: "keep-after-trash",
      title: "Keep after trash",
      filepath: "C:\\FixtureArchive\\keep-after-trash.mp4",
      added_ts: 1700000000,
      view_count: 12,
      tx_status: "pending",
    };
    await page.evaluate((keptRow) => {
      window.__setBridgeHandler("video_delete_file", async () => ({
        ok: true,
        message: "Moved to fixture trash.",
      }));
      window.__setBridgeHandler("browse_list_videos", async () => [keptRow]);
      window.__setBridgeHandler("browse_list_videos_page", async () => ({
        rows: [keptRow],
        next_offset: 1,
        has_more: false,
      }));
      window._browseState.view = "videos";
      window._browseState.currentChannel = {
        folder: "Fixture Channel",
        name: "Fixture Channel",
        split_months: false,
      };
      window._browseState.videos = [
        {
          video_id: "trash-and-stay-gone",
          title: "Trash and stay gone",
          channel: "Fixture Channel",
          filepath: "C:\\FixtureArchive\\trash-and-stay-gone.mp4",
          upload_ts: 1800000000000,
          view_count: 99,
          uploaded: "2027-01-15",
          tx_status: "pending",
        },
        {
          video_id: keptRow.video_id,
          title: keptRow.title,
          channel: "Fixture Channel",
          filepath: keptRow.filepath,
          upload_ts: keptRow.added_ts * 1000,
          view_count: keptRow.view_count,
          uploaded: "2023-11-14",
          tx_status: "pending",
        },
      ];
      window.showView("videos");
      window.sortCurrentVideos("newest");
    }, keptRow);

    const trashedCard = page.locator('[data-video-id="trash-and-stay-gone"]');
    await expect(trashedCard).toHaveCount(1);
    await trashedCard.evaluate((element) => {
      element.dispatchEvent(new MouseEvent("contextmenu", {
        bubbles: true,
        clientX: 180,
        clientY: 180,
      }));
    });
    await page.getByRole("menuitem", {
      name: "Move file to trash…",
      exact: true,
    }).click();
    await page.getByRole("dialog", { name: "Move file to trash?" })
      .getByRole("button", { name: "Move to trash" }).click();

    await expect.poll(() => page.evaluate(() =>
      window._browseState.videos.map((video) => video.video_id)))
      .toEqual(["keep-after-trash"]);
    await expect(trashedCard).toHaveCount(0);

    await page.locator("#browse-sort").selectOption("oldest");
    await expect.poll(() => page.evaluate(() =>
      window.__bridgeCallsFor("browse_list_videos_page").length)).toBeGreaterThan(0);
    await expect.poll(() => page.evaluate(() =>
      window._browseState.videos.map((video) => video.video_id)))
      .toEqual(["keep-after-trash"]);
    await expect(trashedCard).toHaveCount(0);

    await page.locator("#browse-group-year").check();
    await expect.poll(() => page.evaluate(() =>
      window.__bridgeCallsFor("browse_list_videos").length)).toBeGreaterThan(0);
    await expect.poll(() => page.evaluate(() =>
      window._browseState.videos.map((video) => video.video_id)))
      .toEqual(["keep-after-trash"]);
    await expect(trashedCard).toHaveCount(0);
    await expect(page.locator("#video-grid .video-card"))
      .toHaveAttribute("data-video-id", "keep-after-trash");
  });
});
