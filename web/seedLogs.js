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

  // ─── Seed logs from the Python bridge ───────────────────────────────
  //
  // pywebview-only — the Phase 0 browser-preview fallback that fetched
  // sample.json was retired once the real backends were wired up.
  async function seedLogs() {
    // Give pywebview a brief window to register its API before falling back.
    const pywebviewReady = () =>
      new Promise((resolve) => {
        if (window.pywebview && window.pywebview.api) {
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
        setTimeout(() => finish(!!(window.pywebview && window.pywebview.api)), 600);
      });

    const ready = await pywebviewReady();

    try {
      if (ready) {
        const api = window.pywebview.api;
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
          await api.startup_ready();
        });

        await step("runtime_info", async () => {
          const info = await api.get_runtime_info();
          if (!info) return;
          console.info("[api] runtime_info:", info);
          const sel = document.getElementById("log-mode-select");
          if (sel && info.log_mode) sel.value = info.log_mode;
          document.body.dataset.logMode = info.log_mode || "Simple";
          // Pre-seed the Subs Avg column visibility so the upcoming
          // renderSubsTable step doesn't flash the column in and then
          // hide it. info.show_avg_size defaults true on missing key.
          window._applySubsAvgVisibility?.(info.show_avg_size !== false);
          // Pre-seed the Recent view mode so the upcoming
          // renderRecentTable step renders into the correct view and
          // the alternate frame is hidden before first paint.
          window._applyRecentViewMode?.(info.recent_view_mode || "list");
          // First-launch archive-root picker. Blocking modal — no
          // Cancel button, no ESC dismissal, no outside-click close.
          // The user MUST pick a folder before the app does anything
          // useful. Replaces the old two-step confirm+picker flow
          // which let users silently leave the app in a half-
          // configured state (default `~/Channel Archives` baked in).
          if (info.has_real_config === false || !info.output_dir) {
            await new Promise((resolve) => {
              const modal = document.getElementById("welcome-modal");
              const pathEl = document.getElementById("welcome-path");
              const browseBtn = document.getElementById("welcome-browse");
              const continueBtn = document.getElementById("welcome-continue");
              if (!modal || !pathEl || !browseBtn || !continueBtn) {
                resolve(); return;
              }
              let pickedPath = "";
              modal.hidden = false;
              // Block ESC from closing while this modal is up. The
              // existing askq/context-menu ESC handlers only fire on
              // elements they own, so this listener just makes sure
              // nothing else hijacks ESC to close the modal.
              const escBlock = (e) => {
                // Skip auto-repeat events (holding Esc) — running
                // the comparison + preventDefault 30+ times/sec is
                // wasteful (audit: seedLogs L132).
                if (e.repeat) return;
                if (e.key === "Escape") { e.preventDefault(); e.stopPropagation(); }
              };
              document.addEventListener("keydown", escBlock, true);

              browseBtn.addEventListener("click", async () => {
                // Guard against double-click spawning two picker
                // dialogs — disable the button for the duration.
                if (browseBtn.disabled) return;
                browseBtn.disabled = true;
                try {
                  const picked = await api.pick_folder("Choose archive root");
                  if (picked?.ok && picked.path) {
                    pickedPath = picked.path;
                    pathEl.value = pickedPath;
                    continueBtn.disabled = false;
                  } else if (picked && picked.ok === false && picked.error) {
                    // Picker refused / failed with an explicit error.
                    window._showToast?.(
                      "Folder picker failed: " + picked.error, "error");
                  }
                  // picked?.ok === false without .error means user
                  // hit Cancel — silent no-op is correct.
                } catch (e) {
                  // pywebview bridge error (process detached, picker
                  // crash, etc.). Pre-fix this was silently swallowed
                  // and the Browse button appeared dead with no hint.
                  window._showToast?.(
                    "Folder picker failed: " + String(e), "error");
                } finally {
                  browseBtn.disabled = false;
                }
              });
              continueBtn.addEventListener("click", async () => {
                if (!pickedPath) return;
                try {
                  const saved = await api.set_parent_folder(pickedPath);
                  if (saved?.ok) {
                    modal.hidden = true;
                    document.removeEventListener("keydown", escBlock, true);
                    window._showToast?.(
                      "Archive root saved: " + pickedPath, "ok");
                    resolve();
                  } else {
                    window._showToast?.(
                      saved?.error || "Could not save folder.", "error");
                  }
                } catch (e) {
                  window._showToast?.(String(e), "error");
                }
              });
            });
          }
        });

        await step("activity_log_history", async () => {
          const history = await api.get_activity_log_history();
          window.renderActivityLog(history || []);
          window._syncActivityLogVisibility?.();
        });

        await step("subs_channels", async () => {
          const subsData = await api.get_subs_channels();
          if (Array.isArray(subsData) && subsData.length === 2) {
            window.renderSubsTable(subsData[0], subsData[1]);
            window._primeBrowse(subsData[0]);
            window._populateIndexTable?.(subsData[0]);
          }
        });

        await step("recent_downloads", async () => {
          const rows = await api.get_recent_downloads();
          if (rows) window.renderRecentTable(rows);
        });

        await step("index_summary", async () => {
          const idx = await api.get_index_summary();
          if (idx) window._applyIndexSummary?.(idx);
        });

        await step("queues", async () => {
          const q = await api.get_queues();
          if (q) window.renderQueues(q);
        });

        await step("startup_ready", async () => {
          await api.startup_ready();
        });
      } else {
        // pywebview never came up — log and bail. Phase 0's sample.json
        // browser fallback was removed; the app is desktop-only now.
        console.warn("[seed] pywebview bridge not detected — UI will stay empty");
      }
    } catch (e) {
      console.error("seedLogs failed:", e);
    }
  }

  window.seedLogs = seedLogs;
})();
