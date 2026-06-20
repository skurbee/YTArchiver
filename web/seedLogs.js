/**
 * web/seedLogs.js — Initial log + queue + activity-log seed from the Python bridge
 */
(function () {
  "use strict";

  const _browseState = window._browseState || {};
  const showContextMenu = window.showContextMenu || (() => {});
  const askConfirm = window.askConfirm;
  const askDanger = window.askDanger;
  const askQuestion = window.askQuestion;
  const askChoice = window.askChoice;
  const askTextInput = window.askTextInput;
  const escapeHtml = window.YT?.util?.escapeHtml || ((s) => String(s ?? ""));
  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }
  function nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
  }

  // ─── Seed logs from the Python bridge ───────────────────────────────
  //
  // pywebview-only — the Phase 0 browser-preview fallback that fetched
  // sample.json was retired once the real backends were wired up.
  async function seedLogs() {
    // Give pywebview a brief window to register its API before falling back.
    const pywebviewReady = () =>
      new Promise((resolve) => {
        if (nativeBridgeUp()) {
          resolve(true);
          return;
        }
        let settled = false;
        const finish = (ok) => {
          if (settled) return;
          settled = true;
          resolve(ok);
        };
        window.addEventListener("pywebviewready",
          () => finish(true), { once: true });
        setTimeout(() => finish(nativeBridgeUp()), 600);
      });

    const ready = await pywebviewReady();

    try {
      if (ready) {
        // Each API call is isolated — one failure does NOT cascade.
        const step = async (name, fn) => {
          try { await fn(); }
          catch (e) { console.error(`[seed] ${name} failed:`, e); }
        };

        // Fire startup_ready FIRST (non-blocking — it just kicks off a
        // daemon thread on the Python side) so Stage 2's disk-walk and
        // the "Scanning disk" indicator can start populating in parallel
        // with the rest of seedLogs's sequential bridge calls. Previously
        // this ran LAST, which meant the disk-scan didn't even begin
        // until all 6 earlier seedLogs steps had completed — visible to
        // the user as a multi-second wait before the indicator appeared.
        // The Python side guards re-entry via `_startup_fired`, so the
        // duplicate call at the end is a harmless no-op.
        step("startup_ready_early", async () => {
          await bridgeCall("startup_ready");
        });

        await step("runtime_info", async () => {
          const info = await bridgeCall("get_runtime_info");
          if (!info) return;
          console.info("[api] runtime_info:", info);
          const sel = document.getElementById("log-mode-select");
          if (sel && info.log_mode) sel.value = info.log_mode;
          document.body.dataset.logMode = info.log_mode || "Simple";
          // Pre-seed the Subs Avg column visibility so the upcoming
          // renderSubsTable step doesn't flash the column in and then
          // hide it. info.show_avg_size defaults true on missing key.
          window._applySubsAvgVisibility?.(info.show_avg_size !== false);
          // First-run onboarding wizard. Driven by the backend-confirmed
          // `onboarded` flag (set once the user finishes/skips the wizard),
          // with missing-output_dir / no-config-file fallbacks so a half-
          // set-up config still triggers it. This replaces the old
          // welcome-modal that could silently no-op (the bug a brand-new
          // machine hit: no folder picker, just dependency errors in the
          // log). The wizard is a full-screen blocking overlay
          // (web/onboarding.js); it owns its own archive-folder picker +
          // dependency installer. Wrapped so a failure here can't sink the
          // rest of seedLogs.
          const _needsOnboarding =
            (info.onboarded === false) ||
            !info.output_dir ||
            info.has_config_file === false;
          if (_needsOnboarding) {
            console.info("[seed] first run detected — launching onboarding wizard",
              { onboarded: info.onboarded, output_dir: info.output_dir,
                has_config_file: info.has_config_file });
            if (typeof window._startOnboarding === "function") {
              try {
                await window._startOnboarding({ firstRun: true });
              } catch (e) {
                console.error("[seed] onboarding wizard failed to start:", e);
              }
            } else {
              console.error("[seed] _startOnboarding missing — onboarding.js "
                + "did not load; cannot show first-run wizard");
            }
          }
        });

        await step("activity_log_history", async () => {
          const history = await bridgeCall("get_activity_log_history");
          window.renderActivityLog(history || []);
          window._syncActivityLogVisibility?.();
        });

        await step("subs_channels", async () => {
          const subsData = await bridgeCall("get_subs_channels");
          if (Array.isArray(subsData) && subsData.length === 2) {
            window.renderSubsTable(subsData[0], subsData[1]);
            window._primeBrowse(subsData[0]);
            window._populateIndexTable?.(subsData[0]);
          }
        });

        // (Recent-downloads boot render removed — the Videos view now
        // self-loads from api.list_all_videos when its submode is opened.)

        await step("index_summary", async () => {
          const idx = await bridgeCall("get_index_summary");
          if (idx) window._applyIndexSummary?.(idx);
        });

        await step("queues", async () => {
          const q = await bridgeCall("get_queues");
          if (q) window.renderQueues(q);
        });

        await step("startup_ready", async () => {
          await bridgeCall("startup_ready");
        });
      } else {
        window._reportBootIssue?.(
          "seedLogs",
          "pywebview bridge not detected; startup data could not load.",
          { level: "error" },
        );
        // pywebview never came up — log and bail. Phase 0's sample.json
        // browser fallback was removed; the app is desktop-only now.
        console.warn("[seed] pywebview bridge not detected — UI will stay empty");
      }
    } catch (e) {
      window._reportBootIssue?.("seedLogs", e, { level: "error" });
      console.error("seedLogs failed:", e);
    }
  }

  window.seedLogs = seedLogs;
})();
