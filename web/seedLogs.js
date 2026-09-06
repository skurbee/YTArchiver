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

  let _seedComplete = false;
  let _seedInFlight = null;
  let _retryOnBridgeReady = false;
  let _lastSeedFailure = "";
  const _seedStepsDone = new Set();

  function clearSeedRetry() {
    document.getElementById("boot-issue-retry-seed")?.remove();
    window._clearBootIssue?.("Startup data");
    window._clearBootIssue?.("App connection");
    window.removeEventListener("pywebviewready", retrySeedAfterBridgeReady);
    _retryOnBridgeReady = false;
    _lastSeedFailure = "";
  }

  function retrySeedAfterBridgeReady() {
    _retryOnBridgeReady = false;
    // If readiness arrives during a failed seed, retry after that attempt
    // settles instead of accidentally returning its in-flight promise.
    Promise.resolve(_seedInFlight).then(() => seedLogs());
  }

  function offerBridgeReadyRetry() {
    if (_retryOnBridgeReady) return;
    _retryOnBridgeReady = true;
    window.addEventListener("pywebviewready", retrySeedAfterBridgeReady, { once: true });
  }

  function requireSuccessfulReply(reply, description) {
    if (reply?.ok === false) {
      throw new Error(String(reply.error || `${description} was not returned`));
    }
    return reply;
  }

  function offerSeedRetry(message) {
    const text = String(message || "Startup data could not load.");
    if (_lastSeedFailure !== text) {
      _lastSeedFailure = text;
      window._reportBootIssue?.("Startup data", text, { level: "error" });
    }
    const actions = document.querySelector("#boot-issue-banner .boot-issue-actions");
    if (!actions || document.getElementById("boot-issue-retry-seed")) return;
    const retry = document.createElement("button");
    retry.type = "button";
    retry.id = "boot-issue-retry-seed";
    retry.className = "btn btn-thin";
    retry.textContent = "Retry startup load";
    retry.addEventListener("click", () => {
      retry.disabled = true;
      Promise.resolve(seedLogs()).finally(() => { retry.disabled = false; });
    });
    actions.prepend(retry);
  }

  // ─── Seed logs from the Python bridge ───────────────────────────────
  //
  // pywebview-only — the Phase 0 browser-preview fallback that fetched
  // sample.json was retired once the real backends were wired up.
  async function runSeedLogs() {
    try {
      // Use the app's canonical readiness gate.  The old private 600 ms race
      // permanently skipped hydration on slower WebView startups.
      if (!nativeBridgeUp()) await window.YT?.bridge?.ready;
      if (nativeBridgeUp()) {
        // Each API call is isolated — one failure does NOT cascade.
        const failedSteps = [];
        const step = async (name, fn) => {
          if (_seedStepsDone.has(name)) return true;
          try {
            await fn();
            _seedStepsDone.add(name);
            return true;
          } catch (e) {
            failedSteps.push(name);
            offerBridgeReadyRetry();
            console.error(`[seed] ${name} failed:`, e);
            return false;
          }
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
        await step("startup_ready_early", async () => {
          requireSuccessfulReply(await bridgeCall("startup_ready"), "startup readiness");
        });

        await step("runtime_info", async () => {
          const info = requireSuccessfulReply(await bridgeCall("get_runtime_info"), "runtime information");
          // An unavailable native method returns an error object, not a
          // fresh-install record. Only complete, typed runtime information
          // may decide whether the first-time setup wizard is needed.
          if (!info || typeof info !== "object" || Array.isArray(info)
              || typeof info.onboarded !== "boolean"
              || typeof info.has_config_file !== "boolean"
              || typeof info.output_dir !== "string") {
            throw new Error("complete runtime information was not returned");
          }
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
          if (!Array.isArray(history)) {
            throw new Error("activity history was not returned");
          }
          window.renderActivityLog(history);
          window._syncActivityLogVisibility?.();
        });

        await step("subs_channels", async () => {
          const subsData = await bridgeCall("get_subs_channels");
          if (!Array.isArray(subsData) || subsData.length !== 2) {
            throw new Error("subscription data was not returned");
          }
          window.renderSubsTable(subsData[0], subsData[1]);
          window._primeBrowse(subsData[0]);
          window._populateIndexTable?.(subsData[0]);
        });

        // (Recent-downloads boot render removed — the Videos view now
        // self-loads from api.list_all_videos when its submode is opened.)

        await step("index_summary", async () => {
          const idx = requireSuccessfulReply(await bridgeCall("get_index_summary"), "index summary");
          if (!idx || typeof idx !== "object" || Array.isArray(idx)) {
            throw new Error("index summary was not returned");
          }
          window._applyIndexSummary?.(idx);
        });

        await step("queues", async () => {
          const q = requireSuccessfulReply(await bridgeCall("get_queues"), "queue state");
          if (!q || typeof q !== "object" || Array.isArray(q)) {
            throw new Error("queue state was not returned");
          }
          window.renderQueues(q);
        });

        await step("startup_ready", async () => {
          requireSuccessfulReply(await bridgeCall("startup_ready"), "startup readiness");
        });

        if (failedSteps.length) {
          offerSeedRetry(
            "Some startup data did not load. Use Retry startup load to try again.",
          );
          return false;
        }
        _seedComplete = true;
        clearSeedRetry();
        return true;
      } else {
        offerSeedRetry(
          "YTArchiver did not finish loading its startup data. " +
          "Use Retry startup load in a moment.",
        );
        // If the bridge arrives after its canonical timeout, retry
        // automatically as well as leaving the visible manual retry.
        offerBridgeReadyRetry();
        console.warn("[seed] pywebview bridge not detected — startup load is retryable");
        return false;
      }
    } catch (e) {
      offerBridgeReadyRetry();
      offerSeedRetry(`Startup data load failed: ${e?.message || e}`);
      console.error("seedLogs failed:", e);
      return false;
    }
  }

  function seedLogs() {
    if (_seedComplete) return Promise.resolve(true);
    if (_seedInFlight) return _seedInFlight;
    _seedInFlight = runSeedLogs().finally(() => { _seedInFlight = null; });
    return _seedInFlight;
  }

  window.seedLogs = seedLogs;
})();
