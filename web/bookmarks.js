/**
 * web/bookmarks.js — Browse > Bookmarks sub-mode + week summary + redownload prompt
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

  // ─── Browse > Bookmarks sub-mode ─────────────────────────────────────
  async function refreshBookmarks() {
    const list = document.getElementById("bookmarks-list");
    if (!list) return;
    const api = window.pywebview?.api;
    if (!api?.bookmark_list) return;
    try {
      // bookmark_list() now returns {ok: bool, rows: [...]} to match
      // the other bookmark_* methods' shape. Back-compat: if an older
      // build returns the raw array, fall back gracefully.
      const res = await api.bookmark_list();
      const rows = Array.isArray(res) ? res : (res?.rows || []);
      if (!rows || rows.length === 0) {
        list.innerHTML = '<div class="browse-empty">No bookmarks yet. Right-click a transcript segment in Watch view to add one.</div>';
        return;
      }
      const frag = document.createDocumentFragment();
      for (const b of rows) {
        const row = document.createElement("div");
        row.className = "bookmark-row";
        // two bookmark kinds now.
        // start_time < 0 (sentinel = whole-video bookmark) renders
        // like a YouTube favorites card: title + thumbnail + channel,
        // no timestamp/transcript line.
        // start_time >= 0 renders as before: title + timestamp +
        // transcript text. Note field is gone (no longer captured).
        const isWholeVideo = Number(b.start_time) < 0;
        row.innerHTML = `
          <div class="bookmark-head">
            <span class="bookmark-title"></span>
            <span class="bookmark-meta"></span>
            <button class="icon-btn-slim" data-remove="${b.id}" title="Delete bookmark">\u00d7</button>
          </div>
          ${isWholeVideo ? "" : '<div class="bookmark-text"></div>'}
        `;
        row.querySelector(".bookmark-title").textContent = b.title || "(untitled)";
        row.querySelector(".bookmark-meta").textContent = isWholeVideo
          ? (b.channel || "")
          : `${b.channel || ""} \u00b7 ${_formatTs(b.start_time)}`;
        if (!isWholeVideo) {
          row.querySelector(".bookmark-text").textContent = b.text || "";
        }
        row.querySelector("[data-remove]").addEventListener("click", async (e) => {
          e.stopPropagation();
          const ok = await askDanger("Delete bookmark",
            `Remove this bookmark?\n\n"${b.text?.slice(0, 100) || ''}"`, "Delete");
          if (!ok) return;
          // audit SV-10: gate the success toast on the backend's
          // `{ok}` flag instead of firing it unconditionally. A
          // double-click would fire two "Bookmark removed" toasts
          // even though only the first actually deletes anything.
          try {
            const res = await api.bookmark_remove(b.id);
            if (res && res.ok === false) {
              window._showToast?.(
                res.error || "Couldn't delete (already gone?).", "warn");
              return;
            }
            window._showToast?.("Bookmark removed.", "ok");
          } catch (_re) {
            window._showToast?.(`Delete failed: ${_re}`, "error");
            return;
          }
          refreshBookmarks();
        });

        // Click-to-jump: open the bookmark's video in Watch view seeked to
        // the bookmark timestamp. Mirrors YTArchiver.py:29372 _on_bookmark_select
        // + :29389 "Jump to segment" right-click action. Clicking the note
        // editor or delete button must NOT trigger jump, so we scope to the
        // head + text area only.
        const _jumpToBookmark = async () => {
          // Whole-video bookmarks (sentinel start_time = -1) open at 0.
          let start = Number(b.start_time);
          if (!Number.isFinite(start) || start < 0) start = 0;
          const vid = b.video_id || "";
          const title = b.title || "";
          // Resolve the actual video file via the title+channel index.
          // Tries (video_id → title → fuzzy) and opens Watch view.
          if (!api?.recent_resolve) {
            window._showToast?.("Native mode required.", "warn");
            return;
          }
          try {
            const r = await api.recent_resolve(title, b.channel || "");
            if (r?.ok && r.filepath) {
              const videoObj = {
                filepath: r.filepath,
                title: title,
                channel: b.channel || "",
                video_id: vid || r.video_id || "",
                _seek_to: start, // Watch view uses this for initial seek
              };
              if (typeof window._openVideoInWatch === "function") {
                window._openVideoInWatch(videoObj);
              } else {
                // Fallback: click the Browse tab and render directly
                document.querySelector('.tab[data-tab="browse"]')?.click();
              }
            } else {
              window._showToast?.("Couldn't find the source video for this bookmark.", "warn");
            }
          } catch (err) {
            window._showToast?.("Jump failed: " + err, "error");
          }
        };
        // Whole-video bookmark rows don't render .bookmark-text — only
        // the .bookmark-head — so guard every text-element access
        // (audit: bookmarks.js H186) to avoid `null.addEventListener`.
        const _headEl = row.querySelector(".bookmark-head");
        const _textEl = row.querySelector(".bookmark-text");
        if (_headEl) {
          _headEl.addEventListener("click", (e) => {
            if (e.target.closest("[data-remove]")) return;
            _jumpToBookmark();
          });
          _headEl.style.cursor = "pointer";
          _headEl.title = "Click to jump to this moment";
        }
        if (_textEl) {
          _textEl.addEventListener("click", _jumpToBookmark);
          _textEl.style.cursor = "pointer";
          _textEl.title = "Click to jump to this moment";
        }
        frag.appendChild(row);
      }
      list.innerHTML = "";
      list.appendChild(frag);
    } catch (e) { console.warn("bookmarks:", e); }
  }

  // Called from seedLogs once channel data arrives.
  // `channels` comes from get_subs_channels (no avatar/banner URLs). To get
  // the real channel-art paths for the grid we hit browse_list_channels
  // which enriches with .ChannelArt/* URLs. Fall back to the plain list if
  // the richer call isn't available.
  window._primeBrowse = async function (channels) {
    const basic = (channels || []).slice().sort((a, b) =>
      (a.folder || "").localeCompare(b.folder || "", undefined, { sensitivity: "base" })
    );
    let enriched = basic;
    const api = window.pywebview?.api;
    if (api?.browse_list_channels) {
      try {
        const rich = await api.browse_list_channels();
        if (Array.isArray(rich) && rich.length) enriched = rich;
      } catch { /* fall through to basic */ }
    }
    _browseState.channels = enriched;
    window.renderChannelGrid(enriched, (c) => {
      _browseState.currentChannel = c;
      loadVideosFor(c);
      showView("videos");
    });
    // Populate "New this week" summary bar (async, best-effort).
    _refreshBrowseWeekSummary();
  };

  async function _refreshBrowseWeekSummary() {
    const bar = document.getElementById("browse-summary-bar");
    if (!bar) return;
    const api = window.pywebview?.api;
    if (!api?.browse_week_summary) return;
    try {
      const res = await api.browse_week_summary(7);
      if (!res?.ok) return;
      const nv = res.new_videos || 0;
      const nc = res.new_channels || 0;
      const tc = res.total_channels || 0;
      document.getElementById("bsb-new-videos").textContent = String(nv);
      document.getElementById("bsb-new-channels").textContent = String(nc);
      document.getElementById("bsb-total-channels").textContent = String(tc);
      bar.hidden = false;
      if (Array.isArray(res.channel_list) && res.channel_list.length) {
        bar.title = "Channels with new videos this week:\n " +
          res.channel_list.slice(0, 20).join("\n ") +
          (res.channel_list.length > 20 ? `\n \u2026 (+${res.channel_list.length - 20} more)` : "");
      } else {
        bar.title = "";
      }
    } catch { /* ignore */ }
  }
  window._refreshBrowseWeekSummary = _refreshBrowseWeekSummary;

  async function _askRedownload(channelName, resolution) {
    const api = window.pywebview?.api;
    if (!api?.chan_redownload) {
      window._showToast?.("Native mode required.", "warn");
      return;
    }
    const label = resolution === "best" ? "Best available" : `${resolution}p`;
    const ok = await askDanger("Redownload channel",
      `Redownload every video in "${channelName}" at ${label}?\n\n` +
      "This scans local files, fetches the YouTube catalog, matches by ID, " +
      "downloads each video, and replaces the originals.\n\n" +
      "Progress is saved \u2014 you can cancel and resume later.",
      "Start redownload");
    if (!ok) return;
    const res = await api.chan_redownload(channelName, resolution);
    if (res?.ok) {
      window._showToast?.(`Redownload started (${label}).`, "ok");
    } else {
      window._showToast?.(res?.error || "Redownload failed to start.", "error");
    }
  }

  window.refreshBookmarks = refreshBookmarks;
  window._primeBrowse = _primeBrowse;
  window._refreshBrowseWeekSummary = _refreshBrowseWeekSummary;
  window._askRedownload = _askRedownload;
})();
