/* ═══════════════════════════════════════════════════════════════════════
   indexControls.js — Settings → Index sub-tab controls

   After Patch 15 (v71.7) this file is focused on the Index sub-tab only:
     • Build / Rebuild buttons (archive rescan, FTS rebuild)
     • Stats panel (channels, videos, segments, hours, %, DB size)
     • Inline log mirror
     • Compat shim for the legacy `window._applyIndexSummary` consumers

   Sibling files extracted in the same patch:
     • settingsInfra.js — Settings sub-tab nav + Archive Roots panel
     • metadataTab.js — Settings → Metadata refresh-status table

   Publishes:
     window.initIndexControls

   Reads:
     window.pywebview.api.get_index_summary / get_index_db_stats /
                          archive_rescan / index_rebuild_fts
     window.askDanger, window._showToast
   ═══════════════════════════════════════════════════════════════════════ */

(function () {
  "use strict";

  function initIndexControls() {
    const getApi = () => window.pywebview?.api;
    const bBuild = document.getElementById("btn-idx-build");
    const bRebuild = document.getElementById("btn-idx-rebuild");
    const statsEl = document.getElementById("index-stats-text");
    const progEl = document.getElementById("idx-progress");
    const logEl = document.getElementById("index-log");

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
      const api = getApi();
      if (!api?.get_index_summary) {
        // Preview / pre-ready state. Don't overwrite existing numbers once
        // we've already painted them (avoids a "flash of offline").
        if (!statsEl.dataset.populated) {
          statsEl.textContent = "— (loading…)";
        }
        return;
      }
      const fmt = (v) => (v == null ? "—" :
        (typeof v === "number" ? v.toLocaleString() : String(v)));
      const _zeroOK = (v) => (v == null ? "—" :
        (typeof v === "number" ? v.toLocaleString() : String(v)));
      const _renderLines = (c, db) => {
        const pct = (c.transcribed_pct_channels != null)
          ? c.transcribed_pct_channels.toFixed(1) + "%"
          : "—";
        // db may be null while the slow query is in flight — show
        // "loading…" for those fields so the user sees they're
        // intentionally pending, not broken.
        // 2026-05-14: was plain "loading…" text inside `<pre>` set
        // via .textContent. Felt dead during the multi-second slow-
        // index query. Switched to innerHTML with an inline spinner
        // span on the pending rows so the user sees active motion.
        const _esc = (s) => String(s).replace(/[&<>"']/g, ch => ({
          "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"
        }[ch]));
        const _loading = '<span class="spinner-inline"></span>loading…';
        return [
          `Channels: ${_esc(fmt(c.channels))}`,
          `Videos: ${_esc(fmt(c.videos))}`,
          `Segments: ${db ? _esc(_zeroOK(db.segments)) : _loading}`,
          `Hours of video: ${db ? _esc(_zeroOK(db.hours)) : _loading}`,
          `Transcribed: ${_esc(pct)}`,
          `Index DB size: ${db ? _esc(db.index_db_size_label || "—") : _loading}`,
        ].join("\n");
      };
      try {
        const idx = await api.get_index_summary();
        const c = (idx && idx.cards) || {};
        // Render basics immediately so the panel isn't blank during
        // the multi-second index-DB query that follows.
        statsEl.innerHTML = _renderLines(c, null);
        statsEl.dataset.populated = "1";
        // Async-fetch the slow index-DB stats. Re-render once they
        // arrive. If the API doesn't exist (older backend), the
        // spinner placeholders just keep spinning.
        if (api.get_index_db_stats) {
          api.get_index_db_stats().then((db) => {
            if (db) statsEl.innerHTML = _renderLines(c, db);
          }).catch(() => {});
        }
      } catch (e) {
        statsEl.textContent = `Stats unavailable: ${e}`;
      }
    };

    bBuild?.addEventListener("click", async () => {
      const api = getApi();
      if (!api?.archive_rescan) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      if (progEl) progEl.textContent = "Building…";
      appendLog("Building / updating index…");
      try {
        await api.archive_rescan();
        appendLog("Build complete.");
        if (progEl) progEl.textContent = "Done.";
        await refreshStats();
      } catch (e) {
        appendLog(`Build failed: ${e}`);
        if (progEl) progEl.textContent = "Failed.";
      }
    });

    bRebuild?.addEventListener("click", async () => {
      const api = getApi();
      if (!api?.index_rebuild_fts) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      const ok = await window.askDanger(
        "Rebuild from scratch?",
        "Drop the FTS search index and rebuild it from every .jsonl on disk. " +
        "Safe, but can take minutes on large archives.",
        "Rebuild");
      if (!ok) return;
      if (progEl) progEl.textContent = "Rebuilding FTS…";
      appendLog("Rebuilding FTS index from scratch…");
      try {
        await api.index_rebuild_fts();
        appendLog("FTS rebuild complete.");
        if (progEl) progEl.textContent = "Done.";
        await refreshStats();
      } catch (e) {
        appendLog(`FTS rebuild failed: ${e}`);
        if (progEl) progEl.textContent = "Failed.";
      }
    });

    // pywebview injects api AFTER DOMContentLoaded — refresh once it's ready
    // so the Stats area populates without needing a sub-tab click.
    window.addEventListener("pywebviewready", () => { refreshStats(); });
    // Defensive: if the event already fired before we wired it, a short
    // delayed poll catches that case too.
    setTimeout(() => { if (getApi()?.get_index_summary) refreshStats(); }, 800);

    // Expose so the Settings > Index sub-tab can trigger a refresh when
    // the user clicks back onto it (avoids stale "Loading…").
    window._refreshIndexStats = refreshStats;

    // Initial paint
    refreshStats();
  }

  // Compat shim for the old _applyIndexSummary consumers (browse-tab preload).
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

  window.initIndexControls = initIndexControls;
})();
