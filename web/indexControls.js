/* ═══════════════════════════════════════════════════════════════════════
   indexControls.js — Health → Library search-index controls

   Focused on the Index sub-tab:
     • Build / Rebuild buttons (archive rescan, FTS rebuild)
     • Stats panel (channels, videos, segments, hours, %, DB size)
     • Inline log mirror
     • Compat shim for the legacy `window._applyIndexSummary` consumers

   Related settings modules:
     • settingsInfra.js — Settings sub-tab nav + Archive Roots panel
     • metadataTab.js — Health → Library channel-information table

   Publishes:
     window.initIndexControls
     window._applyIndexDbStats

   Reads:
     window.pywebview.api.get_index_summary / get_index_db_stats /
                          archive_rescan / index_rebuild_fts /
                          index_rebuild_fts_state
     window.askDanger, window._showToast
   ═══════════════════════════════════════════════════════════════════════ */

(function () {
  "use strict";

  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }

  function nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
  }

  function applyIndexDbStats(db) {
    const segments = Number(db?.segments);
    if (!Number.isFinite(segments) || segments < 0) return;
    const label = Math.trunc(segments).toLocaleString();
    for (const id of ["stat-segments", "search-stat-segments"]) {
      const el = document.getElementById(id);
      if (el) el.textContent = label;
    }
  }

  function initIndexControls() {
    const bBuild = document.getElementById("btn-idx-build");
    const bRebuild = document.getElementById("btn-idx-rebuild");
    const statsEl = document.getElementById("index-stats-text");
    const progEl = document.getElementById("idx-progress");
    const logEl = document.getElementById("index-log");
    let ftsPollGeneration = 0;
    let statsWaitingForBridge = false;

    const libraryStatsAreVisible = () => {
      const view = statsEl?.closest(".settings-view");
      const panel = statsEl?.closest(".tab-panel");
      return !!view && !view.hidden && !!panel?.classList.contains("active");
    };

    const appendLog = (line) => {
      if (!logEl) return;
      const ln = document.createElement("div");
      ln.className = "log-line";
      ln.textContent = line;
      logEl.appendChild(ln);
      logEl.scrollTop = logEl.scrollHeight;
    };

    const refreshStats = async () => {
      if (!statsEl) return;
      if (!nativeBridgeUp()) {
        statsWaitingForBridge = true;
        // Preview / pre-ready state. Don't overwrite existing numbers once
        // we've already painted them (avoids a "flash of offline").
        if (!statsEl.dataset.populated) {
          statsEl.textContent = "— (loading…)";
        }
        return;
      }
      // Clear this synchronously before the first await. If bridge readiness
      // races the Library click or the delayed fallback, only the first path
      // retries the pending request.
      statsWaitingForBridge = false;
      const fmt = (v) => (v == null ? "—" :
        (typeof v === "number" ? v.toLocaleString() : String(v)));
      const _zeroOK = (v) => (v == null ? "—" :
        (typeof v === "number" ? v.toLocaleString() : String(v)));
      const _renderLines = (c, db) => {
        // Archive-wide transcription COVERAGE = transcribed videos / total
        // videos (from the slow index-DB stats `db`), NOT the old "% of
        // channels with auto-transcribe on". Shows loading until `db` arrives
        // (same as Segments / Hours below). no_speech videos are NOT counted
        // as transcribed, so a fully-checked archive with genuinely-silent
        // videos correctly reads just below 100%.
        const txPct = (db && db.total_videos)
          ? ((db.transcribed_videos || 0) * 100.0 / db.total_videos).toFixed(1) + "%"
          : "—";
        // db may be null while the slow query is in flight — show
        // "loading…" for those fields so the user sees they're
        // intentionally pending, not broken.
        // 2026-05-14: was plain "loading…" text inside `<pre>` set
        // via .textContent. Felt dead during the multi-second slow-
        // index query. Switched to innerHTML with an inline spinner
        // span on the pending rows so the user sees active motion.
        const _esc = window.YT?.util?.escapeHtml || window._escapeHtml
          || ((s) => String(s ?? "").replace(/[&<>"']/g, ch => ({
            "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"
          }[ch])));
        const _loading = '<span class="spinner-inline"></span>loading…';
        return [
          `Channels: ${_esc(fmt(c.channels))}`,
          `Videos in catalog: ${_esc(fmt(c.videos))}`,
          `Files on disk: ${_esc(fmt(c.physical_copies))}`,
          `Segments: ${db ? _esc(_zeroOK(db.segments)) : _loading}`,
          `Video hours: ${db ? _esc(_zeroOK(db.hours)) : _loading}`,
          `Transcribed: ${db ? _esc(txPct) : _loading}`,
          `Search database size: ${db ? _esc(db.index_db_size_label || "—") : _loading}`,
        ].join("\n");
      };
      const _paintCatalogStatus = (status) => {
        let note = statsEl.querySelector(".index-stats-note");
        if (status.phase === "done") {
          note?.remove();
          return;
        }
        if (!statsEl.dataset.populated) {
          statsEl.textContent = status.text;
          return;
        }
        if (!note) {
          note = document.createElement("span");
          note.className = "index-stats-note";
          note.setAttribute("role", "status");
          note.setAttribute("aria-live", "polite");
          statsEl.appendChild(note);
        }
        note.setAttribute("aria-live", status.announce === false ? "off" : "polite");
        note.textContent = status.phase === "loading"
          ? "Refreshing detailed statistics…"
          : status.text;
      };
      try {
        const outcome = await window.YT.bridge.catalogRead(
          "index-stats",
          async (context) => {
            const idx = await bridgeCall("get_index_summary");
            const c = (idx && idx.cards) || {};
            if (!context.isCurrent()) return null;
            // Paint cheap summary values before the detailed aggregate.
            statsEl.innerHTML = _renderLines(c, null);
            statsEl.dataset.populated = "1";
            let db = null;
            let dbError = null;
            try {
              db = await bridgeCall("get_index_db_stats");
              // The backend intentionally returns a render-safe object when
              // its aggregate fails. Treat its explicit error field as a
              // failure instead of displaying those placeholder zeros as
              // genuine archive statistics.
              if (db?.error) {
                dbError = new Error(String(db.error));
                db = {
                  ...db,
                  segments: null,
                  hours: null,
                  total_videos: null,
                  transcribed_videos: null,
                  index_db_bytes: null,
                  index_db_size_label: "—",
                };
              }
            } catch (error) {
              dbError = error;
              db = { segments: null, hours: null, index_db_size_label: "—" };
            }
            return { c, db, dbError };
          },
          {
            // Detailed Health statistics use a dedicated SQLite connection
            // (archive_scan.index_db_stats), so they must not occupy the
            // foreground Browse/Search lane while the user opens the library.
            lane: "diagnostics",
            label: "index statistics",
            onStatus: _paintCatalogStatus,
          });
        if (outcome.stale || !outcome.value) return;
        const { c, db, dbError } = outcome.value;
        statsEl.innerHTML = _renderLines(c, db);
        statsEl.dataset.populated = "1";
        if (!dbError) applyIndexDbStats(db);
        if (dbError) {
          const note = document.createElement("span");
          note.className = "index-stats-note";
          note.textContent = "Detailed statistics could not be loaded. Try again later.";
          statsEl.appendChild(note);
          try { console.warn("get_index_db_stats failed:", dbError); } catch (e) {}
        }
      } catch (e) {
        statsEl.textContent = `Stats unavailable: ${e}`;
      }
    };

    const monitorFtsRebuild = async ({ resumed = false } = {}) => {
      const generation = ++ftsPollGeneration;
      let bridgeFailures = 0;
      const poll = async () => {
        if (generation !== ftsPollGeneration || !nativeBridgeUp()) return;
        try {
          const state = await bridgeCall("index_rebuild_fts_state");
          if (generation !== ftsPollGeneration) return;
          bridgeFailures = 0;
          if (state?.running) {
            if (progEl) progEl.textContent = "Search index rebuild running…";
            setTimeout(poll, 750);
            return;
          }
          if (!state?.completed_at) return;
          if (state.ok) {
            const rows = Number(state.rows_indexed || 0).toLocaleString();
            const message = `Search index rebuild complete: ${rows} entries indexed.`;
            appendLog(message);
            if (progEl) progEl.textContent = message;
            window._showToast?.("Search index rebuild complete.", "ok");
            refreshStats();
          } else {
            const error = state.error || "unknown error";
            appendLog(`Search index rebuild failed: ${error}`);
            if (progEl) progEl.textContent = "Search index rebuild failed.";
            window._showToast?.(`Search index rebuild failed: ${error}`, "error");
          }
        } catch (e) {
          if (generation !== ftsPollGeneration) return;
          bridgeFailures += 1;
          if (bridgeFailures <= 5) {
            setTimeout(poll, 1000);
          } else {
            appendLog(`Could not read search index rebuild status: ${e}`);
            if (progEl) progEl.textContent = "Search index rebuild status unavailable.";
          }
        }
      };
      if (resumed) {
        appendLog("A search index rebuild is already running; monitoring it here.");
      }
      await poll();
    };

    const resumeFtsMonitor = async () => {
      if (!nativeBridgeUp()) return;
      try {
        const state = await bridgeCall("index_rebuild_fts_state");
        if (state?.running) monitorFtsRebuild({ resumed: true });
      } catch (_e) {}
    };

    const retryPendingStats = () => {
      if (statsWaitingForBridge && libraryStatsAreVisible()) refreshStats();
    };

    bBuild?.addEventListener("click", async () => {
      if (!nativeBridgeUp()) {
        window._showToast?.("YTArchiver isn't ready yet. Try again in a moment.", "warn");
        return;
      }
      if (progEl) progEl.textContent = "Building…";
      appendLog("Building / updating index…");
      try {
        const result = await bridgeCall("archive_rescan");
        if (result?.started) {
          appendLog("Archive rescan started; progress is shown below.");
          if (progEl) progEl.textContent = "Rescan running…";
        } else if (result?.already_running) {
          appendLog("Archive rescan is already running.");
          if (progEl) progEl.textContent = "Already running…";
        } else {
          const msg = result?.error || "Rescan could not start.";
          appendLog(`Build not started: ${msg}`);
          if (progEl) progEl.textContent = "Not started.";
          window._showToast?.(msg, "warn");
        }
      } catch (e) {
        appendLog(`Build failed to start: ${e}`);
        if (progEl) progEl.textContent = "Failed to start.";
      }
    });

    bRebuild?.addEventListener("click", async () => {
      if (!nativeBridgeUp()) {
        window._showToast?.("YTArchiver isn't ready yet. Try again in a moment.", "warn");
        return;
      }
      const ok = await window.askDanger(
        "Rebuild from scratch?",
        "Delete and rebuild the transcript search index from saved transcript data. " +
        "This may take several minutes on a large archive.",
        "Rebuild");
      if (!ok) return;
      if (progEl) progEl.textContent = "Starting search index rebuild…";
      appendLog("Starting search index rebuild…");
      try {
        const result = await bridgeCall("index_rebuild_fts");
        if (result?.ok && result?.started) {
          appendLog("Search index rebuild started.");
          if (progEl) progEl.textContent = "Search index rebuild running…";
          window._showToast?.("Search index rebuild started.", "ok");
          monitorFtsRebuild();
        } else if (/already running/i.test(result?.error || "")) {
          appendLog("Search index rebuild is already running.");
          if (progEl) progEl.textContent = "Search index rebuild already running…";
          window._showToast?.("Search index rebuild is already running.", "warn");
          monitorFtsRebuild({ resumed: true });
        } else {
          const msg = result?.error || "Search index rebuild could not start.";
          appendLog(`Search index rebuild not started: ${msg}`);
          if (progEl) progEl.textContent = "Search index rebuild not started.";
          window._showToast?.(msg, "error");
        }
      } catch (e) {
        appendLog(`Search index rebuild failed to start: ${e}`);
        if (progEl) progEl.textContent = "Failed to start.";
        window._showToast?.(`Search index rebuild failed to start: ${e}`, "error");
      }
    });

    // pywebview injects api AFTER DOMContentLoaded. Resume an interrupted FTS
    // monitor when it becomes available, but leave the expensive statistics
    // lazy: Health -> Library's existing refresh-on-show hook loads them when
    // the user actually asks to see that page. Starting the hidden aggregate
    // here used to delay Browse -> Channels during every cold launch.
    window.addEventListener("pywebviewready", () => {
      resumeFtsMonitor();
      retryPendingStats();
    });
    // Defensive: if the event already fired before we wired it, a short
    // delayed poll catches that case too.
    setTimeout(() => {
      if (nativeBridgeUp()) {
        resumeFtsMonitor();
        retryPendingStats();
      }
    }, 800);

    // If Library was requested before readiness, then hidden before the
    // bridge arrived, keep the request lazy. Retry only when the user returns
    // to Health with Library still selected.
    document.querySelector('.tab[data-tab="health"]')?.addEventListener(
      "click", () => setTimeout(retryPendingStats, 0));

    // Expose so Health > Library can trigger a refresh when
    // the user clicks back onto it (avoids stale "Loading…").
    window._refreshIndexStats = refreshStats;

  }

  // Compat shim for old _applyIndexSummary consumers.
  // Since the Index tab no longer has idx-channels/idx-videos/etc. elements,
  // _applyIndexSummary is now a no-op at refresh time — the new section
  // builds its own stats via refreshStats() inside initIndexControls. But
  // we still populate the sidebar badges that live outside the Index panel.
  (function compatApplyIndexSummary() {
    const orig = window._applyIndexSummary;
    window._applyIndexSummary = function (idx) {
      try {
        const c = (idx && idx.cards) || {};
        const setText = (id, txt) => {
          const el = document.getElementById(id);
          if (el) el.textContent = txt;
        };
        setText("stat-channels", (c.channels ?? "").toLocaleString?.() ?? "");
        setText("stat-videos", (c.videos ?? "").toLocaleString?.() ?? "");
      } catch (_e) {}
      if (typeof orig === "function") try { orig(idx); } catch (_e) {}
    };
  })();

  window._applyIndexDbStats = applyIndexDbStats;
  window.initIndexControls = initIndexControls;
})();
