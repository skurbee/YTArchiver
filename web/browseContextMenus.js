/**
 * web/browseContextMenus.js — right-click menus on Browse tab cards.
 *
 * Extracted from app.js. Wires three context menus:
 *   - Channel cards in the Browse grid (Sync now, Refetch thumbnails,
 *     Re-transcribe channel, Repair captions, Edit, Remove, etc.)
 *   - Video cards in the per-channel grid
 *   - Bookmark rows in the Bookmarks sub-mode
 *
 * Exposed as window.initBrowseContextMenus; app.js boot calls it once.
 *
 * Depends on:
 *   - window._browseState (app.js)
 *   - window.showContextMenu (contextMenu.js)
 *   - window.askConfirm, window.askDanger, window.askQuestion (modals.js)
 *   - window._showToast (toasts.js)
 *   - window._askWhisperModel (app.js)
 *   - window._editChannelFromContext (editChannel.js)
 *   - window._removeChannelWithPrompt (app.js)
 *   - window.YT.bridge.bridgeCall (bridge.js)
 *   - window.pywebview.api.* (native bridge)
 */
(function () {
  "use strict";

  const _browseState = window._browseState || {};
  const showContextMenu = window.showContextMenu || (() => {});
  const askConfirm = window.askConfirm;
  const askDanger = window.askDanger;
  const askQuestion = window.askQuestion;
  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }

  // ─── Browse tab context menus ────────────────────────────────────────
  function initBrowseContextMenus() {
    // Channel grid cards
    const channelGrid = document.getElementById("channel-grid");
    if (channelGrid) {
      channelGrid.addEventListener("contextmenu", (e) => {
        const card = e.target.closest(".channel-card");
        if (!card) return;
        e.preventDefault();
        const name = card.querySelector(".channel-card-name")?.textContent || "";
        const api = window.pywebview?.api;
        // Live-count labels: pull pending counters stashed on the card by
        // renderChannelGrid. folder-level
        // "Transcribe untranscribed (N pending)" live count.
        const _pendTx = parseInt(card.dataset.pendingTx || "0", 10) || 0;
        const _pendMeta = parseInt(card.dataset.pendingMeta || "0", 10) || 0;
        const _txLabel = _pendTx > 0
          ? `Transcribe untranscribed (${_pendTx} pending)`
          : "Transcribe all missing";
        const _metaLabel = _pendMeta > 0
          ? `Recheck metadata (${_pendMeta} pending)`
          : "Recheck metadata";
        showContextMenu(e.clientX, e.clientY, [
          { label: "Open videos", action: () => card.click() },
          { label: "Show in Explorer", action: () => api?.chan_open_folder?.(name) },
          { label: "Open channel on YouTube", action: () => api?.chan_open_url?.(name) },
          { sep: true },
          { label: "Sync now", action: () => api?.sync_one_channel?.({ name }) },
          // refetch missing thumbnails for this channel.
          // Surfaces the same backend function used by the Settings >
          // Metadata "Refresh thumbnails" affordance.
          { label: "Refetch missing thumbnails", action: async () => {
            const r = await api?.refetch_thumbnails?.({ name });
            if (r?.started) {
              window._showToast?.(
                `Thumbnail refetch started for ${name}.`, "ok");
            } else if (r?.error) {
              window._showToast?.(r.error, "error");
            }
          }},
          // right-click → re-transcribe whole channel
          // with a Whisper model picker. Same API as the Subs context
          // menu version.
          { label: "Re-transcribe channel…", action: async () => {
            const model = await (window._askWhisperModel?.(`channel "${name}"`));
            if (!model) return;
            const ok = await askDanger(
              "Re-transcribe entire channel",
              `Queue every video in "${name}" for re-transcription with `
                + `Whisper ${model}?\n\nThis can take hours on large channels.`,
              "Queue all");
            if (!ok) return;
            const res = await api?.transcribe_retranscribe_channel?.(
              { name }, model);
            if (res?.ok) {
              window._showToast?.(
                `Queued ${res.queued} video(s) from ${name} for Whisper ${model}.`,
                "ok");
            } else {
              window._showToast?.(res?.error || "Channel retranscribe failed.",
                                  "error");
            }
          }},
          { label: _metaLabel, action: () => api?.metadata_recheck_channel?.({ name }) },
          // New 2026-04-23: comments refresh, separate from views/likes.
          // Submenu offers scope choices so a 4000-video channel doesn't
          // get re-hit end-to-end when the user only cares about recent
          // uploads. Backend at metadata.refresh_channel_comments honors
          // the `only_recent_days` field.
          { label: "Refresh comments\u2026",
            submenu: [
              { label: "Last 7 days",
                action: () => api?.metadata_refresh_comments_channel?.({ name }, 7) },
              { label: "Last 30 days",
                action: () => api?.metadata_refresh_comments_channel?.({ name }, 30) },
              { label: "Last 90 days",
                action: () => api?.metadata_refresh_comments_channel?.({ name }, 90) },
              { label: "All videos (slow)",
                action: () => api?.metadata_refresh_comments_channel?.({ name }, null) },
            ]},
          { label: _txLabel, action: () => _askTranscribeChannel(name) },
          { sep: true },
          { label: "Reorg folder",
            submenu: [
              { label: "Flat (no split)", action: () => api?.reorg_channel_folder?.({ name }, false, false, false) },
              { label: "Split by year", action: () => api?.reorg_channel_folder?.({ name }, true, false, false) },
              { label: "Split by year + month", action: () => api?.reorg_channel_folder?.({ name }, true, true, false) },
              { label: "Re-check dates + year/month", action: () => api?.reorg_channel_folder?.({ name }, true, true, true) },
              { label: "Fix file dates only", action: () => api?.chan_fix_file_dates?.({ name }) },
            ]},
          // "Fetch channel art" removed — now bundled with the metadata sweep.
          { label: "Redownload at\u2026",
            submenu: [
              { label: "Best available", action: () => _askRedownload(name, "best") },
              { label: "2160p (4K)", action: () => _askRedownload(name, "2160") },
              { label: "1440p", action: () => _askRedownload(name, "1440") },
              { label: "1080p", action: () => _askRedownload(name, "1080") },
              { label: "720p", action: () => _askRedownload(name, "720") },
              { label: "480p", action: () => _askRedownload(name, "480") },
              { label: "360p", action: () => _askRedownload(name, "360") },
            ]},
          { sep: true },
          { label: "Edit settings", action: () => window._editChannelFromContext?.(name) },
        ]);
      });
    }

    // Video grid cards (inside a channel) — also handles right-click on
    // year headers when Group-by-year is enabled, offering per-year
    // redownload + metadata scopes. Mirrors OLD's tree-view year / month
    // folder right-click (YTArchiver.py:26462 / :26498).
    const videoGrid = document.getElementById("video-grid");
    if (videoGrid) {
      videoGrid.addEventListener("contextmenu", (e) => {
        // Year header hit? Offer year-scoped actions.
        const yearHead = e.target.closest(".video-grid-year-head");
        if (yearHead) {
          e.preventDefault();
          const section = yearHead.parentElement;
          const year = section?.dataset?.year;
          const chan = _browseState.currentChannel?.folder
                    || _browseState.currentChannel?.name
                    || "";
          if (!year || year === "?" || !chan) return;
          const api = window.pywebview?.api;
          const _scope = { year: parseInt(year, 10) };
          const _yearInt = parseInt(year, 10);
          showContextMenu(e.clientX, e.clientY, [
            { label: `Redownload ${year} at\u2026`,
              submenu: [
                { label: "Best available",
                  action: () => api?.chan_redownload?.({ name: chan }, "best", _scope) },
                { label: "2160p (4K)",
                  action: () => api?.chan_redownload?.({ name: chan }, "2160", _scope) },
                { label: "1440p",
                  action: () => api?.chan_redownload?.({ name: chan }, "1440", _scope) },
                { label: "1080p",
                  action: () => api?.chan_redownload?.({ name: chan }, "1080", _scope) },
                { label: "720p",
                  action: () => api?.chan_redownload?.({ name: chan }, "720", _scope) },
                { label: "480p",
                  action: () => api?.chan_redownload?.({ name: chan }, "480", _scope) },
                { label: "360p",
                  action: () => api?.chan_redownload?.({ name: chan }, "360", _scope) },
              ]},
            // feature H-14: year-scoped metadata (parallel to the
            // full-channel metadata actions in Settings > Tools).
            // "Download" = fetch missing entries for YYYY only.
            // "Refresh views/likes" = re-hit every YYYY video to pick
            // up updated view counts / likes / comments.
            { label: `Download metadata for ${year}`, action: async () => {
              const res = await api?.metadata_queue_channel_year?.(
                { name: chan }, _yearInt, false);
              if (res?.ok) window._showToast?.(
                `Queued metadata download for ${chan} (${year}).`, "ok");
              else if (res?.error) window._showToast?.(res.error, "error");
            }},
            { label: `Refresh views/likes for ${year}`, action: async () => {
              const res = await api?.metadata_queue_channel_year?.(
                { name: chan }, _yearInt, true);
              if (res?.ok) window._showToast?.(
                `Queued metadata refresh for ${chan} (${year}).`, "ok");
              else if (res?.error) window._showToast?.(res.error, "error");
            }},
          ]);
          return;
        }
        const card = e.target.closest(".video-card");
        if (!card) return;
        e.preventDefault();
        const filepath = card.dataset.filepath || "";
        const videoId = card.dataset.videoId || "";
        const title = card.dataset.title || "";
        const channel = card.dataset.channel || "";
        const ytUrl = videoId ? `https://www.youtube.com/watch?v=${videoId}` : "";
        const api = window.pywebview?.api;
        showContextMenu(e.clientX, e.clientY, [
          { label: "Play video", action: () => {
            // The label promises the in-app Watch view (embedded HTML5
            // video + karaoke transcript), not the system default
            // external player. Route through _openVideoInWatch which is
            // the shared helper the Browse grid double-click also uses.
            if (filepath && typeof window._openVideoInWatch === "function") {
              window._openVideoInWatch({
                title, channel, filepath, video_id: videoId,
              });
            } else if (filepath && api?.browse_open_video) {
              // Defensive fallback: if the Watch helper got removed,
              // open externally so the user still gets video playback.
              api.browse_open_video(filepath);
            }
          }},
          { label: "Open on YouTube", action: () => {
            if (ytUrl) window.open(ytUrl, "_blank");
          }},
          { label: "Copy YouTube URL", action: () => {
            if (ytUrl) {
              navigator.clipboard?.writeText(ytUrl);
              window._showToast?.("URL copied.", "ok");
            }
          }},
          { label: "Show in Explorer", action: () => {
            if (filepath && api?.browse_show_in_explorer) api.browse_show_in_explorer(filepath);
          }},
          { sep: true },
          { label: "Refresh metadata", action: async () => {
            // Re-fetches views/likes/description/comments for this single
            // video and writes back to the channel's aggregated
            // Metadata.jsonl. Also covers the missing-metadata case —
            // fetch_single_video_metadata writes a fresh entry when one
            // doesn't exist. Same backend as Watch view's refresh button.
            if (!api?.browse_refresh_video_metadata) {
              window._showToast?.("Refresh unavailable.", "warn");
              return;
            }
            // Fall back to the currently-selected channel when the card's
            // dataset doesn't carry one (older renders, or grids that don't
            // set v.channel). Without this, the backend's
            // "Channel '' not in config" error would just look like silence.
            const _ch = channel
              || (window._browseState?.currentChannel?.folder || "");
            const payload = {
              filepath, video_id: videoId, title, channel: _ch,
            };
            console.log("[refresh-meta] sending", payload);
            window._showToast?.({ msg: "Refreshing metadata…", ttlMs: 15000 });
            try {
              const res = await api.browse_refresh_video_metadata(payload);
              console.log("[refresh-meta] result", res);
              if (res?.ok) {
                window._showToast?.("Metadata refreshed.", "ok");
              } else {
                const msg = res?.error || "Refresh failed.";
                window._showToast?.(msg, res?.transient ? "warn" : "error");
              }
            } catch (e) {
              console.error("[refresh-meta] threw", e);
              window._showToast?.(
                `Refresh failed: ${e?.message || e}`, "error");
            }
          }},
          { label: "Transcribe now", action: async () => {
            if (filepath && api?.transcribe_enqueue) {
              // Manual → Whisper model picker (60s countdown auto-picks default).
              const model = await (window._askWhisperModel?.(`"${title}"`));
              if (model === null) return;
              api.transcribe_enqueue(filepath, title);
              window._showToast?.("Queued for whisper.", "ok");
            }
          }},
          { label: "Re-transcribe…", action: async () => {
            // `_on_retranscribe` — ask for
            // a Whisper model, then queue a GPU task. No extra "are you
            // sure" confirm (the model picker Cancel handles that).
            if (!filepath || !api?.transcribe_retranscribe) return;
            const model = await (window._askWhisperModel?.(`"${title}"`));
            if (!model) return;
            const res = await api.transcribe_retranscribe(filepath, title, videoId || "");
            if (res?.ok) window._showToast?.(
              `Queued ${model} re-transcription.`, "ok");
            else window._showToast?.(res?.error || "Re-transcribe failed.", "error");
          }},
          { label: "Redownload\u2026", action: async () => {
            // prompt for resolution first (mirrors the
            // Watch view's video_redownload flow). Previously the
            // right-click Redownload fired with no resolution arg
            // so the backend used its default rather than the
            // resolution the user wanted.
            const _res = await (window._askRedownload?.(title));
            if (!_res) return;
            bridgeCall("video_redownload", videoId, title, _res);
          }},
          { sep: true },
          { label: "Delete file", cls: "dim",
            action: () => bridgeCall("video_delete_file", filepath) },
        ]);
      });
    }

    // Recent tab in grid view needs the same right-click menu
    // as the Browse > Channel video grid. Same card class, different
    // container, so we bind a second handler on #recent-grid.
    const recentGrid = document.getElementById("recent-grid");
    if (recentGrid) {
      recentGrid.addEventListener("contextmenu", (e) => {
        const card = e.target.closest(".video-card");
        if (!card) return;
        e.preventDefault();
        const filepath = card.dataset.filepath || "";
        const videoId = card.dataset.videoId || "";
        const title = card.dataset.title || "";
        const channel = card.dataset.channel || "";
        const ytUrl = videoId ? `https://www.youtube.com/watch?v=${videoId}` : "";
        const api = window.pywebview?.api;
        showContextMenu(e.clientX, e.clientY, [
          { label: "Play video", action: () => {
            // The label promises the in-app Watch view (embedded HTML5
            // video + karaoke transcript), not the system default
            // external player. Route through _openVideoInWatch which is
            // the shared helper the Browse grid double-click also uses.
            if (filepath && typeof window._openVideoInWatch === "function") {
              window._openVideoInWatch({
                title, channel, filepath, video_id: videoId,
              });
            } else if (filepath && api?.browse_open_video) {
              // Defensive fallback: if the Watch helper got removed,
              // open externally so the user still gets video playback.
              api.browse_open_video(filepath);
            }
          }},
          { label: "Open on YouTube", action: () => {
            if (ytUrl) window.open(ytUrl, "_blank");
          }},
          { label: "Copy YouTube URL", action: () => {
            if (ytUrl) {
              navigator.clipboard?.writeText(ytUrl);
              window._showToast?.("URL copied.", "ok");
            }
          }},
          { label: "Show in Explorer", action: () => {
            if (filepath && api?.browse_show_in_explorer) api.browse_show_in_explorer(filepath);
          }},
          { sep: true },
          { label: "Refresh metadata", action: async () => {
            if (!api?.browse_refresh_video_metadata) {
              window._showToast?.("Refresh unavailable.", "warn");
              return;
            }
            const payload = {
              filepath, video_id: videoId, title, channel,
            };
            console.log("[refresh-meta] sending", payload);
            window._showToast?.({ msg: "Refreshing metadata…", ttlMs: 15000 });
            try {
              const res = await api.browse_refresh_video_metadata(payload);
              console.log("[refresh-meta] result", res);
              if (res?.ok) {
                window._showToast?.("Metadata refreshed.", "ok");
              } else {
                const msg = res?.error || "Refresh failed.";
                window._showToast?.(msg, res?.transient ? "warn" : "error");
              }
            } catch (e) {
              console.error("[refresh-meta] threw", e);
              window._showToast?.(
                `Refresh failed: ${e?.message || e}`, "error");
            }
          }},
          { label: "Transcribe now", action: async () => {
            if (filepath && api?.transcribe_enqueue) {
              const model = await (window._askWhisperModel?.(`"${title}"`));
              if (model === null) return;
              api.transcribe_enqueue(filepath, title);
              window._showToast?.("Queued for whisper.", "ok");
            }
          }},
          { label: "Re-transcribe…", action: async () => {
            if (!filepath || !api?.transcribe_retranscribe) return;
            const model = await (window._askWhisperModel?.(`"${title}"`));
            if (!model) return;
            const res = await api.transcribe_retranscribe(filepath, title, videoId || "");
            if (res?.ok) window._showToast?.(
              `Queued ${model} re-transcription.`, "ok");
            else window._showToast?.(res?.error || "Re-transcribe failed.", "error");
          }},
          { label: "Redownload…", action: async () => {
            const _res = await (window._askRedownload?.(title));
            if (!_res) return;
            bridgeCall("video_redownload", videoId, title, _res);
          }},
          { sep: true },
          { label: "Delete file", cls: "dim",
            action: () => bridgeCall("video_delete_file", filepath) },
        ]);
      });
    }

    // Transcript segments in Watch view
    const transcript = document.getElementById("watch-transcript");
    if (transcript) {
      transcript.addEventListener("contextmenu", (e) => {
        const seg = e.target.closest(".seg");
        if (!seg) return;
        e.preventDefault();
        const ts = seg.querySelector(".timestamp")?.textContent || "";
        const text = seg.textContent.replace(ts, "").trim();
        const api = window.pywebview?.api;
        const v = _browseState.currentVideo || {};
        showContextMenu(e.clientX, e.clientY, [
          { label: `Copy segment`, action: () => navigator.clipboard?.writeText(text) },
          { label: `Copy timestamp + text`, action: () => navigator.clipboard?.writeText(`${ts} ${text}`) },
          { sep: true },
          { label: "Bookmark this moment\u2026", action: async () => {
            if (!api?.bookmark_add) return;
            const start = _parseTs(ts.replace(/[\[\]]/g, ""));
            // right-click on a transcript segment always
            // creates a timestamped bookmark with no note prompt.
            const res = await api.bookmark_add({
              video_id: v.video_id || "",
              title: v.title || "",
              channel: v.channel || "",
              start_time: start,
              text: text,
              note: "",
            });
            if (res?.ok) {
              window._showToast?.("Bookmarked.", "ok");
              try { window.refreshBookmarks?.(); } catch {}
            } else {
              window._showToast?.(res?.error || "Bookmark failed.", "error");
            }
          }},
        ]);
      });
    }
  }

  function _parseTs(s) {
    if (!s) return 0;
    const parts = s.split(":").map(x => parseInt(x, 10) || 0);
    if (parts.length === 3) return parts[0]*3600 + parts[1]*60 + parts[2];
    if (parts.length === 2) return parts[0]*60 + parts[1];
    return 0;
  }

  window.initBrowseContextMenus = initBrowseContextMenus;
})();
