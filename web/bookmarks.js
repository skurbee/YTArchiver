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
  // Capture like browseSearch.js does — bare _formatTs is a strict-mode
  // ReferenceError that blanked the whole Bookmarks list as soon as a
  // timed bookmark rendered.
  const _formatTs = (sec) =>
    (window._formatTs ? window._formatTs(sec) : String(sec));
  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }
  function nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
  }

  function _fmtBytes(bytes) {
    const n = Number(bytes || 0);
    if (!Number.isFinite(n) || n <= 0) return "";
    if (n >= 1073741824) return (n / 1073741824).toFixed(1) + " GB";
    if (n >= 1048576) return (n / 1048576).toFixed(1) + " MB";
    if (n >= 1024) return (n / 1024).toFixed(0) + " KB";
    return `${n} B`;
  }

  function _fmtDate(value) {
    const s = String(value || "").trim();
    if (!s) return "";
    const d = new Date(s);
    if (Number.isNaN(d.getTime())) return s;
    return d.toLocaleDateString(undefined, {
      year: "numeric",
      month: "short",
      day: "numeric",
    });
  }

  function _gradientFor(name) {
    const s = String(name || "").trim();
    const first = (s[0] || "?").toUpperCase();
    const hue = (first.charCodeAt(0) * 47) % 360;
    const hue2 = (hue + 40) % 360;
    return `linear-gradient(135deg, hsl(${hue}, 55%, 28%) 0%, hsl(${hue2}, 60%, 18%) 100%)`;
  }

  // ─── Browse > Bookmarks sub-mode ─────────────────────────────────────
  async function refreshBookmarks() {
    const list = document.getElementById("bookmarks-list");
    if (!list) return;
    if (!nativeBridgeUp()) return;
    try {
      // bookmark_list() now returns {ok: bool, rows: [...]} to match
      // the other bookmark_* methods' shape. Back-compat: if an older
      // build returns the raw array, fall back gracefully.
      const res = await bridgeCall("bookmark_list");
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
          </div>
          ${isWholeVideo ? "" : '<div class="bookmark-text"></div>'}
        `;
        const removeBtn = document.createElement("button");
        removeBtn.className = "icon-btn-slim";
        removeBtn.dataset.remove = String(b.id);
        removeBtn.title = "Delete bookmark";
        removeBtn.textContent = "\u00d7";
        row.querySelector(".bookmark-head")?.appendChild(removeBtn);
        row.querySelector(".bookmark-title").textContent = b.title || "(untitled)";
        row.querySelector(".bookmark-meta").textContent = isWholeVideo
          ? (b.channel || "")
          : `${b.channel || ""} \u00b7 ${_formatTs(b.start_time)}`;
        if (!isWholeVideo) {
          row.querySelector(".bookmark-text").textContent = b.text || "";
        }
        row.querySelector("[data-remove]").addEventListener("click", async (e) => {
          e.stopPropagation();
          // Whole-video bookmarks have no segment text; fall back to the
          // title (like the row above) so the confirm shows the video name
          // instead of empty quotes.
          const _bmLabel = (b.text && b.text.trim()) ? b.text : (b.title || "(untitled)");
          const ok = await askDanger("Delete bookmark",
            `Remove this bookmark?\n\n"${_bmLabel.slice(0, 100)}"`, "Delete");
          if (!ok) return;
          // audit SV-10: gate the success toast on the backend's
          // `{ok}` flag instead of firing it unconditionally. A
          // double-click would fire two "Bookmark removed" toasts
          // even though only the first actually deletes anything.
          try {
            const res = await bridgeCall("bookmark_remove", b.id);
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
          if (!nativeBridgeUp()) {
            window._showToast?.("Native mode required.", "warn");
            return;
          }
          try {
            const r = await bridgeCall("recent_resolve", title, b.channel || "");
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

  async function refreshBookmarksGrid() {
    const list = document.getElementById("bookmarks-list");
    if (!list || !nativeBridgeUp()) return;
    try {
      const res = await bridgeCall("bookmark_list");
      const rows = Array.isArray(res) ? res : (res?.rows || []);
      if (!rows || rows.length === 0) {
        list.innerHTML = '<div class="browse-empty">No bookmarks yet. Right-click a transcript segment in Watch view to add one.</div>';
        return;
      }
      const frag = document.createDocumentFragment();
      for (const b of rows) {
        const card = document.createElement("div");
        card.className = "bookmark-card";
        card.setAttribute("role", "button");
        card.tabIndex = 0;
        card.dataset.bookmarkId = String(b.id || "");

        const startRaw = Number(b.start_time);
        const isWholeVideo = (!Number.isFinite(startRaw) || startRaw < 0
          || (startRaw === 0 && !String(b.text || "").trim()));
        const start = isWholeVideo ? 0 : startRaw;
        const title = b.title || "(untitled)";
        const channel = b.channel || "";
        card.setAttribute("aria-label", isWholeVideo
          ? `Open bookmarked video ${title}`
          : `Open bookmark ${title} at ${_formatTs(start)}`);

        const thumb = document.createElement("div");
        thumb.className = "bookmark-thumb";
        thumb.style.background = _gradientFor(title);
        if (b.thumbnail_url) {
          const img = document.createElement("img");
          img.className = "bookmark-thumb-img";
          img.src = b.thumbnail_url;
          img.alt = "";
          img.loading = "lazy";
          img.decoding = "async";
          img.addEventListener("error", () => img.remove(), { once: true });
          thumb.appendChild(img);
        } else {
          const play = document.createElement("span");
          play.className = "bookmark-play";
          play.innerHTML = "&#9654;";
          thumb.appendChild(play);
        }

        const kind = document.createElement("span");
        kind.className = "bookmark-kind " + (isWholeVideo ? "bookmark-kind-video" : "bookmark-kind-time");
        kind.textContent = isWholeVideo ? "Video" : `At ${_formatTs(start)}`;
        thumb.appendChild(kind);

        const duration = document.createElement("span");
        duration.className = "bookmark-duration";
        duration.textContent = isWholeVideo ? (b.duration || "") : _formatTs(start);
        if (duration.textContent) thumb.appendChild(duration);

        const removeBtn = document.createElement("button");
        removeBtn.className = "bookmark-remove";
        removeBtn.dataset.remove = String(b.id || "");
        removeBtn.title = "Delete bookmark";
        removeBtn.textContent = "\u00d7";
        thumb.appendChild(removeBtn);

        const body = document.createElement("div");
        body.className = "bookmark-card-body";
        const titleEl = document.createElement("div");
        titleEl.className = "bookmark-card-title";
        titleEl.textContent = title;
        body.appendChild(titleEl);

        const channelEl = document.createElement("div");
        channelEl.className = "bookmark-card-channel";
        channelEl.textContent = channel;
        body.appendChild(channelEl);

        const meta = document.createElement("div");
        meta.className = "bookmark-card-meta";
        const metaParts = [];
        if (b.uploaded) metaParts.push(_fmtDate(b.uploaded));
        if (b.size_bytes) metaParts.push(_fmtBytes(b.size_bytes));
        if (b.views) metaParts.push(`${b.views} views`);
        meta.textContent = metaParts.join(" \u00b7 ");
        body.appendChild(meta);

        if (!isWholeVideo && b.text) {
          const snippet = document.createElement("div");
          snippet.className = "bookmark-card-snippet";
          snippet.textContent = b.text;
          body.appendChild(snippet);
        }
        if (b.note) {
          const note = document.createElement("div");
          note.className = "bookmark-card-note";
          note.textContent = b.note;
          body.appendChild(note);
        }

        card.appendChild(thumb);
        card.appendChild(body);

        removeBtn.addEventListener("click", async (e) => {
          e.stopPropagation();
          const label = (b.text && b.text.trim()) ? b.text : title;
          const ok = await askDanger(
            "Delete bookmark",
            `Remove this bookmark?\n\n"${label.slice(0, 100)}"`,
            "Delete");
          if (!ok) return;
          try {
            const removed = await bridgeCall("bookmark_remove", b.id);
            if (removed && removed.ok === false) {
              window._showToast?.(
                removed.error || "Couldn't delete (already gone?).", "warn");
              return;
            }
            window._showToast?.("Bookmark removed.", "ok");
          } catch (err) {
            window._showToast?.(`Delete failed: ${err}`, "error");
            return;
          }
          refreshBookmarksGrid();
        });

        const jump = async () => {
          const openObj = {
            filepath: b.filepath || "",
            title,
            channel,
            video_id: b.video_id || "",
            duration: b.duration || "",
            thumbnail_url: b.thumbnail_url || "",
            _seek_to: start,
          };
          try {
            if (openObj.filepath && typeof window._openVideoInWatch === "function") {
              window._openVideoInWatch(openObj);
              return;
            }
            const r = await bridgeCall("recent_resolve", title, channel);
            if (r?.ok && r.filepath && typeof window._openVideoInWatch === "function") {
              window._openVideoInWatch({
                filepath: r.filepath,
                title,
                channel,
                video_id: openObj.video_id || r.video_id || "",
                _seek_to: start,
              });
              return;
            }
            window._showToast?.("Couldn't find the source video for this bookmark.", "warn");
          } catch (err) {
            window._showToast?.("Jump failed: " + err, "error");
          }
        };
        card.addEventListener("click", (e) => {
          if (e.target.closest("[data-remove]")) return;
          jump();
        });
        card.addEventListener("keydown", (e) => {
          if (e.key !== "Enter" && e.key !== " ") return;
          e.preventDefault();
          jump();
        });
        frag.appendChild(card);
      }
      list.innerHTML = "";
      list.appendChild(frag);
    } catch (e) {
      console.warn("bookmarks:", e);
    }
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
    if (nativeBridgeUp()) {
      try {
        const rich = await bridgeCall("browse_list_channels");
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
    if (!nativeBridgeUp()) return;
    try {
      const res = await bridgeCall("browse_week_summary", 7);
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
    if (!nativeBridgeUp()) {
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
    const res = await bridgeCall("chan_redownload", channelName, resolution);
    if (res?.ok) {
      window._showToast?.(`Redownload started (${label}).`, "ok");
    } else {
      window._showToast?.(res?.error || "Redownload failed to start.", "error");
    }
  }

  // Per-VIDEO resolution picker → returns the chosen resolution string,
  // or null if cancelled. Mirrors the Watch view's redownload picker
  // (watchActions.js). This is SEPARATE from _askRedownload, which is the
  // whole-CHANNEL redownload confirm+executor. The per-video right-click
  // "Redownload…" was wrongly calling _askRedownload(title), which:
  //   • rendered "${undefined}p" → "undefinedp" as the resolution,
  //   • showed a "Redownload every video in «<title>»" channel dialog, and
  //   • on confirm fired chan_redownload(title, undefined) — the wrong API,
  //     keyed by the video title — while returning undefined, so the
  //     intended video_redownload never ran.
  // Returning the resolution lets the caller fire video_redownload itself.
  async function _askVideoRedownload(videoTitle) {
    const pick = await (window.askChoice ? window.askChoice({
      title: "Redownload at…",
      message: `Replace the local file for "${videoTitle || "this video"}" ` +
               `with a new download at the chosen resolution. This keeps the ` +
               `existing filename so transcripts and bookmarks still match.`,
      choices: [
        { label: "360p", value: "360" },
        { label: "480p", value: "480" },
        { label: "720p", value: "720" },
        { label: "1080p", value: "1080" },
        { label: "1440p", value: "1440" },
        { label: "2160p (4K)", value: "2160" },
        { label: "Best available", value: "best", primary: true },
      ],
    }) : null);
    if (!pick) return null;
    const _VALID_RES = new Set(
      ["audio", "144", "240", "360", "480", "720",
       "1080", "1440", "2160", "best"]);
    if (!_VALID_RES.has(pick)) {
      window._showToast?.(`Invalid resolution: ${pick}`, "error");
      return null;
    }
    return pick;
  }

  window.refreshBookmarks = refreshBookmarksGrid;
  window._primeBrowse = _primeBrowse;
  window._refreshBrowseWeekSummary = _refreshBrowseWeekSummary;
  window._askRedownload = _askRedownload;
  window._askVideoRedownload = _askVideoRedownload;
})();
