const { test, expect } = require("@playwright/test");
const { installDelayedBridge, loadApp } = require("./fixtures");

test.describe("YTArchiver real frontend behavior", () => {
  test("single-channel sync feedback is truthful and emits one toast", async ({ page }) => {
    await loadApp(page);

    const feedback = await page.evaluate(() => {
      const toasts = [];
      const originalToast = window._showToast;
      window._showToast = (message, kind) => toasts.push({ message, kind });
      try {
        const report = window.YT.bridge.reportSyncOneResult;
        const rows = [
          report({ ok: true, started: true }, "Fixture Channel"),
          report({ ok: true, queued: true, started: false, paused: true },
            "Fixture Channel"),
          report({ ok: true, queued: false, started: false, paused: true },
            "Fixture Channel"),
          report({ ok: true, queued: true, started: false },
            "Fixture Channel"),
          report({ ok: true, queued: false, started: false },
            "Fixture Channel"),
          report({ ok: false, error: "Channel not found" },
            "Fixture Channel"),
        ];
        return { rows, toasts };
      } finally {
        window._showToast = originalToast;
      }
    });

    expect(feedback.rows).toEqual([
      { message: 'Sync started for "Fixture Channel".', kind: "ok" },
      {
        message: 'Added "Fixture Channel" to the sync queue. The queue is paused.',
        kind: "warn",
      },
      {
        message: '"Fixture Channel" is already in the sync queue. The queue is paused.',
        kind: "warn",
      },
      { message: 'Added "Fixture Channel" to the sync queue.', kind: "ok" },
      { message: '"Fixture Channel" is already in the sync queue.', kind: "warn" },
      { message: "Channel not found", kind: "error" },
    ]);
    expect(feedback.toasts).toEqual(feedback.rows);
  });

  test("every single-channel Sync now entry point uses shared feedback", async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => window.seedLogs());

    await page.evaluate(async () => {
      const original = window.YT.bridge.reportSyncOneResult;
      window.__reportedSyncResults = [];
      window.YT.bridge.reportSyncOneResult = (result, name) => {
        window.__reportedSyncResults.push({ result, name });
        return original(result, name);
      };
      window.__syncResultQueue = [
        { ok: true, started: true, name: "Reporter Channel" },
        {
          ok: true,
          queued: true,
          started: false,
          paused: true,
          name: "Reporter Channel",
        },
        {
          ok: true,
          queued: false,
          started: false,
          name: "Reporter Channel",
        },
      ];
      window.__setBridgeHandler(
        "sync_one_channel", async () => window.__syncResultQueue.shift());
      window.__setBridgeHandler("browse_list_channel_videos", async () => ({
        rows: [], next_offset: 0, has_more: false,
      }));
      window.__setBridgeHandler("browse_list_channels", () => [{
        folder: "Reporter Channel",
        name: "Reporter Channel",
        n_vids: 0,
      }]);
      window._queueHasSyncForChannel = () => null;
      window._anySyncRunning = () => false;
      window.renderSubsTable([{
        folder: "Reporter Channel",
        res: "1080",
        min: "—",
        max: "—",
        compress: "—",
        transcribe: "—",
        metadata: "—",
        last_sync: "never",
        n_vids: "0",
        size: "0 B",
        avg_size: "—",
      }], "0 B");
      await window._primeBrowse([{
        folder: "Reporter Channel",
        name: "Reporter Channel",
        n_vids: 0,
      }]);
    });

    // Legacy Channels table row menu.
    await page.locator("#subs-table-body tr").evaluate((row) => {
      row.dispatchEvent(new MouseEvent("contextmenu", {
        bubbles: true, clientX: 20, clientY: 20,
      }));
    });
    await page.getByRole("menuitem", { name: "Sync now", exact: true }).click();
    await expect.poll(() => page.evaluate(() =>
      window.__reportedSyncResults.length)).toBe(1);

    // Browse channel-card menu.
    await page.locator('.tab[data-tab="browse"]').click();
    await page.locator('.submode-btn[data-submode="channels"]').click();
    const card = page.locator(
      '#channel-grid .channel-card[data-channel-name="Reporter Channel"]');
    await expect(card).toBeVisible();
    await card.click({ button: "right" });
    await page.getByRole("menuitem", { name: "Sync now", exact: true }).click();
    await expect.poll(() => page.evaluate(() =>
      window.__reportedSyncResults.length)).toBe(2);

    // Open-channel header button.
    await card.click();
    await expect(page.locator("#channel-page-header")).toBeVisible();
    await page.locator("#cph-sync-now").click();
    await expect.poll(() => page.evaluate(() =>
      window.__reportedSyncResults.length)).toBe(3);

    const reported = await page.evaluate(() => window.__reportedSyncResults);
    expect(reported.map((entry) => ({
      started: !!entry.result.started,
      queued: !!entry.result.queued,
      paused: !!entry.result.paused,
      name: entry.name,
    }))).toEqual([
      { started: true, queued: false, paused: false, name: "Reporter Channel" },
      { started: false, queued: true, paused: true, name: "Reporter Channel" },
      { started: false, queued: false, paused: false, name: "Reporter Channel" },
    ]);
  });

  test("danger dialog keeps Enter and Escape on the safe path", async ({ page }) => {
    await loadApp(page);

    await page.evaluate(() => {
      window.__dangerResult = "pending";
      window.askDanger("Remove archive entry?", "This cannot be undone.")
        .then((value) => { window.__dangerResult = value; });
    });
    const firstDialog = page.getByRole("dialog", { name: "Remove archive entry?" });
    await expect(firstDialog).toBeVisible();
    await expect(firstDialog.locator('[data-act="cancel"]')).toBeFocused();
    await page.keyboard.press("Enter");
    await expect.poll(() => page.evaluate(() => window.__dangerResult)).toBe(false);

    await page.evaluate(() => {
      window.__dangerResult = "pending";
      window.askConfirm("Danger", "Still destructive.", {
        danger: true,
        confirm: "Delete",
      }).then((value) => { window.__dangerResult = value; });
    });
    const secondDialog = page.getByRole("dialog", { name: "Danger" });
    await expect(secondDialog).toBeVisible();
    await page.keyboard.press("Escape");
    await expect.poll(() => page.evaluate(() => window.__dangerResult)).toBe(false);

    // A no-Cancel danger dialog must also ignore implicit Enter. It may
    // confirm only after keyboard focus deliberately moves to Delete.
    await page.evaluate(() => {
      window.__dangerResult = "pending";
      window.askQuestion({
        title: "Permanent delete",
        message: "There is no undo.",
        confirm: "Delete",
        danger: true,
        noCancel: true,
      }).then((value) => { window.__dangerResult = value; });
    });
    const noCancelDialog = page.getByRole("dialog", { name: "Permanent delete" });
    await expect(noCancelDialog).toBeFocused();
    await page.keyboard.press("Enter");
    await page.waitForTimeout(75);
    await expect.poll(() => page.evaluate(() => window.__dangerResult)).toBe("pending");
    await page.keyboard.press("Tab");
    await expect(noCancelDialog.locator('[data-act="confirm"]')).toBeFocused();
    await page.keyboard.press("Enter");
    await expect.poll(() => page.evaluate(() => window.__dangerResult)).toBe(true);
  });

  test("modal visibility reflects the real DOM, including hidden ancestors", async ({ page }) => {
    await loadApp(page);

    const states = await page.evaluate(() => {
      const modal = document.getElementById("redwnl-sample-modal");
      const originalParent = modal.parentElement;
      const hidden = window.YT.modals.isVisible(modal);
      modal.hidden = false;
      const shown = window.YT.modals.isVisible(modal);
      originalParent.setAttribute("aria-hidden", "true");
      const ancestorHidden = window.YT.modals.isVisible(modal);
      originalParent.removeAttribute("aria-hidden");
      modal.hidden = true;
      return { hidden, shown, ancestorHidden };
    });

    expect(states).toEqual({ hidden: false, shown: true, ancestorHidden: false });
  });

  test("Escape closes every registered static dialog", async ({ page }) => {
    await loadApp(page);
    const backdropIds = [
      "about-backdrop",
      "compress-dry-backdrop",
      "drift-backdrop",
      "repair-yt-backdrop",
      "punct-restore-backdrop",
      "provenance-backdrop",
      "diag-backdrop",
      "manual-tx-backdrop",
      "autorun-history-backdrop",
    ];

    for (const id of backdropIds) {
      await page.evaluate((backdropId) => {
        document.getElementById(backdropId).hidden = false;
      }, id);
      await expect(page.locator(`#${id}`)).toBeVisible();
      await page.keyboard.press("Escape");
      await expect(page.locator(`#${id}`)).toBeHidden();
    }
  });

  test("Escape closes a prompt before the static dialog behind it", async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => {
      document.getElementById("about-backdrop").hidden = false;
      window.__layeredPromptResult = "pending";
      window.askConfirm("Confirm test", "Keep About open behind this prompt.")
        .then((value) => { window.__layeredPromptResult = value; });
    });

    await page.keyboard.press("Escape");
    await expect.poll(() => page.evaluate(() =>
      window.__layeredPromptResult)).toBe(false);
    await expect(page.locator("#about-backdrop")).toBeVisible();
    await expect(page.getByRole("dialog", { name: "Confirm test" })).toBeHidden();

    await page.keyboard.press("Escape");
    await expect(page.locator("#about-backdrop")).toBeHidden();
  });

  test("startup hydrates when the pywebview bridge arrives after 600ms", async ({ page }) => {
    const startedAt = Date.now();
    await loadApp(page, { bridgeDelayed: true });
    const remaining = Math.max(0, 750 - (Date.now() - startedAt));
    await page.waitForTimeout(remaining);
    await installDelayedBridge(page);

    await expect.poll(() => page.evaluate(() =>
      window.__bridgeCallsFor("startup_ready").length)).toBeGreaterThan(0);
    await expect.poll(() => page.evaluate(() =>
      window.__bridgeCallsFor("get_subs_channels").length)).toBe(1);
    await expect.poll(() => page.evaluate(() =>
      window.__bridgeCallsFor("get_queues").length)).toBe(1);
    expect(Date.now() - startedAt).toBeGreaterThan(600);
  });

  test("duplicate-looking queue rows remove the exact task ID", async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => {
      window.renderQueues({
        identity_ids_durable: true,
        sync_count: 2,
        gpu_count: 0,
        sync: [
          { task_id: "task-a", name: "Sync Duplicate", url: "same", status: "queued" },
          { task_id: "task-b", name: "Sync Duplicate", url: "same", status: "queued" },
        ],
        gpu: [],
      });
    });

    await page.locator("#btn-sync-tasks").click();
    await expect(page.locator("#popover-sync-tasks")).toHaveClass(/open/);
    await page.locator('[data-task-id="task-b"] .queue-task-close').click();

    await expect.poll(() => page.evaluate(() =>
      window.__bridgeCallsFor("queues_sync_remove").map((call) => call.args[0])
    )).toEqual(["task-b"]);
    await expect(page.locator('[data-task-id="task-a"]')).toHaveCount(1);
    await expect(page.locator('[data-task-id="task-b"]')).toHaveCount(0);
  });

  test("a late A response cannot replace the newer B Watch selection", async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => {
      window.__setBridgeHandler("browse_get_transcript", (request) =>
        new Promise((resolve) => {
          const isA = request.video_id === "video-a";
          setTimeout(() => resolve({
            segments: [{
              s: 0,
              e: 1,
              t: isA ? "Transcript from A" : "Transcript from B",
              w: [],
            }],
            source: { source: "whisper", raw: "WHISPER tiny" },
          }), isA ? 180 : 15);
        }));
    });

    await page.evaluate(async () => {
      const openA = window._openVideoInWatch({
        video_id: "video-a",
        title: "Video A",
        channel: "Fixture Channel",
      });
      await new Promise((resolve) => setTimeout(resolve, 10));
      const openB = window._openVideoInWatch({
        video_id: "video-b",
        title: "Video B",
        channel: "Fixture Channel",
      });
      await Promise.all([openA, openB]);
    });

    await expect(page.locator("#watch-title")).toHaveText("Video B");
    await expect(page.locator("#watch-transcript")).toContainText("Transcript from B");
    await expect(page.locator("#watch-transcript")).not.toContainText("Transcript from A");
    await expect.poll(() => page.evaluate(() =>
      window._watchCurrentVideo?.video_id)).toBe("video-b");
  });

  test("a new Watch video clears old Find state but keeps Search prefill", async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => {
      window.__setBridgeHandler("browse_get_transcript", (request) => ({
        segments: [{
          s: 0,
          e: 2,
          t: request.video_id === "search-prefill"
            ? "A needle appears in this transcript"
            : `Transcript for ${request.video_id}`,
          w: [],
        }],
      }));
    });

    await page.evaluate(() => window._openVideoInWatch({
      video_id: "first-video",
      title: "First Video",
      channel: "Fixture Channel",
    }));
    await page.locator("#watch-find").fill("Desktop Occupant");
    await expect(page.locator("#watch-find-count")).toHaveText("no matches");

    await page.evaluate(() => window._openVideoInWatch({
      video_id: "second-video",
      title: "Second Video",
      channel: "Fixture Channel",
    }));
    await expect(page.locator("#watch-find")).toHaveValue("");
    await expect(page.locator("#watch-find-count")).toHaveText("");

    await page.evaluate(() => window._openVideoInWatch({
      video_id: "search-prefill",
      title: "Search Result Video",
      channel: "Fixture Channel",
      _search_query: "needle",
    }));
    await expect(page.locator("#watch-find")).toHaveValue("needle");
    await expect(page.locator("#watch-find-count")).toHaveText("1 of 1");
  });

  test("hidden Watch media ignores keyboard playback controls", async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => {
      document.querySelector('.tab[data-tab="browse"]').click();
      window.showView("watch");
      const video = document.getElementById("watch-video");
      Object.defineProperty(video, "currentTime", {
        configurable: true,
        writable: true,
        value: 12,
      });
      Object.defineProperty(video, "paused", {
        configurable: true,
        get: () => true,
      });
      window.__watchPlayCalls = 0;
      video.play = () => {
        window.__watchPlayCalls += 1;
        return Promise.resolve();
      };
      video.muted = false;
      document.querySelector('.tab[data-tab="download"]').click();
      document.body.focus();
    });

    await page.keyboard.press("Space");
    await page.keyboard.press("ArrowRight");
    await page.keyboard.press("m");
    const state = await page.evaluate(() => {
      const video = document.getElementById("watch-video");
      return {
        playCalls: window.__watchPlayCalls,
        currentTime: video.currentTime,
        muted: video.muted,
      };
    });
    expect(state).toEqual({ playCalls: 0, currentTime: 12, muted: false });

    // Watch is visible again, but a text field owns its keystrokes.
    await page.evaluate(() => {
      document.querySelector('.tab[data-tab="browse"]').click();
    });
    await page.locator("#watch-find").focus();
    await page.keyboard.press("Space");
    await page.keyboard.press("ArrowRight");
    await page.keyboard.press("m");
    const typingState = await page.evaluate(() => {
      const video = document.getElementById("watch-video");
      return {
        playCalls: window.__watchPlayCalls,
        currentTime: video.currentTime,
        muted: video.muted,
      };
    });
    expect(typingState).toEqual({ playCalls: 0, currentTime: 12, muted: false });
  });

  test("backend ok:false keeps the URL and shows the backend reason", async ({ page }) => {
    await loadApp(page);
    await page.evaluate(() => {
      window.__setBridgeHandler("archive_single_video", async () => ({
        ok: false,
        error: "The channel requires sign-in.",
      }));
    });

    const input = page.locator("#url-input");
    const url = "https://www.youtube.com/watch?v=fixture12345";
    await input.fill(url);
    await expect(page.locator("#btn-download-single")).toBeVisible();
    await page.locator("#btn-download-single").click();

    await expect(input).toHaveValue(url);
    await expect(page.locator("#url-error-row")).toBeVisible();
    await expect(page.locator("#url-error-text"))
      .toHaveText("The channel requires sign-in.");
  });

  test("channel add keeps blank minimum at zero and reports real sync state", async ({ page }) => {
    await loadApp(page);
    await page.locator('.tab[data-tab="browse"]').click();
    await page.evaluate(() => {
      window.__nextSyncResult = { ok: true, queued: true, started: false };
      window.__setBridgeHandler("subs_check_duplicate", async () => ({ ok: true }));
      window.__setBridgeHandler("subs_add_channel", async (payload) => ({
        ok: true,
        channel: { name: payload.folder, folder: payload.folder },
      }));
      window.__setBridgeHandler(
        "sync_one_channel", async () => window.__nextSyncResult);
    });

    const cases = [
      {
        result: { ok: true, queued: true, started: false },
        toast: "Sync queued.",
      },
      {
        result: { ok: true, queued: true, started: false, paused: true },
        toast: "Sync queued - queue is paused.",
      },
      {
        result: { ok: true, queued: false, started: false, paused: true },
        toast: "Already queued - queue is paused.",
      },
      {
        result: { ok: true, queued: false, started: false },
        toast: "Already queued.",
      },
      {
        result: { ok: true, queued: true, started: true },
        toast: "Sync started.",
      },
    ];

    for (let index = 0; index < cases.length; index += 1) {
      const current = cases[index];
      await page.evaluate((result) => {
        window.__nextSyncResult = result;
      }, current.result);
      await page.locator("#browse-add-channel").click();
      await expect(page.locator("#channel-editor-backdrop")).toBeVisible();
      await page.locator("#edit-url")
        .fill(`https://www.youtube.com/@fixture_add_${index}`);
      await page.locator("#edit-folder").fill(`Fixture Add ${index}`);
      await expect(page.locator("#edit-min-dur")).toHaveValue("");
      await page.locator("#btn-edit-update").click();

      const prompt = page.getByRole("dialog", { name: "Channel added" });
      await expect(prompt).toBeVisible();
      await prompt.getByRole("button", { name: "Sync now" }).click();
      await expect(page.locator("#toast-root .toast").last())
        .toHaveText(current.toast);

      const addPayload = await page.evaluate(() =>
        window.__bridgeCallsFor("subs_add_channel").at(-1).args[0]);
      expect(addPayload.min_duration).toBe(0);
    }

    await page.evaluate(() => {
      window.__setBridgeHandler("subs_get_channel", async () => ({
        ok: true,
        channel: {
          name: "Existing Fixture",
          folder: "Existing Fixture",
          url: "https://www.youtube.com/@existing_fixture",
          resolution: "720",
          min_duration: 3,
          max_duration: 0,
          mode: "full",
        },
      }));
      window.__setBridgeHandler("subs_update_channel", async () => ({
        ok: true,
        channel: { name: "Existing Fixture", folder: "Existing Fixture" },
      }));
      window._editChannelFromBrowse("Existing Fixture");
    });
    await expect(page.locator("#edit-min-dur")).toHaveValue("3");
    await page.locator("#edit-min-dur").fill("");
    await page.locator("#btn-edit-update").click();
    await expect.poll(() => page.evaluate(() =>
      window.__bridgeCallsFor("subs_update_channel").length)).toBe(1);

    const editPayloadState = await page.evaluate(() => {
      const payload = window.__bridgeCallsFor("subs_update_channel")[0].args[1];
      const serialized = JSON.parse(JSON.stringify(payload));
      return {
        valueIsUndefined: payload.min_duration === undefined,
        serializedHasMinimum: Object.prototype.hasOwnProperty.call(
          serialized, "min_duration"),
      };
    });
    expect(editPayloadState).toEqual({
      valueIsUndefined: true,
      serializedHasMinimum: false,
    });
  });
});
