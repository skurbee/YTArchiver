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

  function nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
  }

  // Shared "Cancel reorg / date fix" action for the Reorg submenus here
  // AND in the Subs table (columnSort.js references it via window).
  // Both backend passes stop at their next file checkpoint; the toast
  // reports whether anything was actually running so a stray click on
  // an idle app doesn't look like it did something.
  window._cancelFolderOps = async function () {
    if (!nativeBridgeUp()) {
      window._showToast?.("Native mode required.", "warn");
      return;
    }
    try {
      const [r1, r2] = await Promise.all([
        bridgeCall("reorg_cancel"),
        bridgeCall("chan_fix_dates_cancel"),
      ]);
      const anyRunning = !!(r1?.running || r2?.running);
      window._showToast?.(
        anyRunning
          ? "Cancelling — stops at the next file. Progress so far is kept."
          : "No reorganization or date fix is running.",
        anyRunning ? "warn" : "ok");
    } catch (err) {
      window._showToast?.("Cancel failed: " + err, "error");
    }
  };

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
        const api = window.YT?.api;
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
          { label: "Open folder", action: () => api?.chan_open_folder?.(name) },
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
          { label: "Re-transcribe channel…",
            title: "Redo every video with Whisper, replacing existing transcripts (use this to fix bad/corrupted ones)",
            action: async () => {
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
          { label: `${_metaLabel}\u2026`,
            submenu: [
              { label: "Last 7 days",
                action: () => api?.metadata_refresh_views_channel?.({ name }, 7) },
              { label: "Last 30 days",
                action: () => api?.metadata_refresh_views_channel?.({ name }, 30) },
              { label: "Last 90 days",
                action: () => api?.metadata_refresh_views_channel?.({ name }, 90) },
              { label: "All videos (slow)",
                action: () => api?.metadata_refresh_views_channel?.({ name }, null) },
            ]},
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
          { label: _txLabel,
            title: "Transcribe only videos that don't have a transcript yet (YouTube captions first, Whisper fallback)",
            action: () => window._askTranscribeChannel?.(name) },
          { sep: true },
          { label: "Reorg folder",
            submenu: [
              { label: "Flat (no split)", action: () => api?.reorg_channel_folder?.({ name }, false, false, false) },
              { label: "Split by year", action: () => api?.reorg_channel_folder?.({ name }, true, false, false) },
              { label: "Split by year + month", action: () => api?.reorg_channel_folder?.({ name }, true, true, false) },
              { label: "Re-check dates + year/month", action: () => api?.reorg_channel_folder?.({ name }, true, true, true) },
              { label: "Fix file dates only", action: () => api?.chan_fix_file_dates?.({ name }) },
              { sep: true },
              // Cancel affordance for the two long passes above — both
              // were previously unstoppable from the UI (audit S4).
              { label: "Cancel running reorg / date fix",
                action: () => window._cancelFolderOps?.() },
            ]},
          // "Fetch channel art" removed — now bundled with the metadata sweep.
          { label: "Redownload at\u2026",
            submenu: [
              { label: "Best available",
                action: () => window._askRedownload?.(name, "best") },
              { label: "2160p (4K)",
                action: () => window._askRedownload?.(name, "2160") },
              { label: "1440p",
                action: () => window._askRedownload?.(name, "1440") },
              { label: "1080p",
                action: () => window._askRedownload?.(name, "1080") },
              { label: "720p",
                action: () => window._askRedownload?.(name, "720") },
              { label: "480p",
                action: () => window._askRedownload?.(name, "480") },
              { label: "360p",
                action: () => window._askRedownload?.(name, "360") },
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
          const api = window.YT?.api;
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
        const api = window.YT?.api;
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
            } else if (filepath && nativeBridgeUp()) {
              // Defensive fallback: if the Watch helper got removed,
              // open externally so the user still gets video playback.
              api?.browse_open_video?.(filepath);
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
            if (filepath) api?.browse_show_in_explorer?.(filepath);
          }},
          { label: "Bookmark video", action: () => _bookmarkVideo({
            videoId, title,
            // The video-grid card's dataset.channel is often empty (older
            // renders don't set v.channel), so fall back to the channel
            // we drilled into — same fallback the Refresh metadata item
            // above uses.
            channel: channel || (window._browseState?.currentChannel?.folder || ""),
          }) },
          { sep: true },
          { label: "Refresh metadata", action: async () => {
            // Re-fetches views/likes/description/comments for this single
            // video and writes back to the channel's aggregated
            // Metadata.jsonl. Also covers the missing-metadata case —
            // fetch_single_video_metadata writes a fresh entry when one
            // doesn't exist. Same backend as Watch view's refresh button.
            if (!nativeBridgeUp()) {
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
              const res = await api?.browse_refresh_video_metadata?.(payload);
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
            if (filepath && nativeBridgeUp()) {
              // Manual → Whisper model picker (60s countdown auto-picks default).
              const model = await (window._askWhisperModel?.(`"${title}"`));
              if (model === null) return;
              api?.transcribe_enqueue?.(filepath, title, model || "");
              window._showToast?.("Queued for whisper.", "ok");
            }
          }},
          { label: "Re-transcribe…", action: async () => {
            // `_on_retranscribe` — ask for
            // a Whisper model, then queue a GPU task. No extra "are you
            // sure" confirm (the model picker Cancel handles that).
            if (!filepath || !nativeBridgeUp()) return;
            const model = await (window._askWhisperModel?.(`"${title}"`));
            if (!model) return;
            const res = await api?.transcribe_retranscribe?.(filepath, title, videoId || "");
            if (res?.ok) window._showToast?.(
              `Queued ${model} re-transcription.`, "ok");
            else window._showToast?.(res?.error || "Re-transcribe failed.", "error");
          }},
          { label: "Redownload\u2026", action: async () => {
            // Per-VIDEO resolution picker (mirrors the Watch view's
            // video_redownload flow), then queue the single-video
            // redownload. Uses _askVideoRedownload \u2014 NOT _askRedownload,
            // which is the whole-channel confirm/executor and produced the
            // "undefinedp" / "redownload every video in <title>" bug.
            const _res = await (window._askVideoRedownload?.(title));
            if (!_res) return;
            bridgeCall("video_redownload", videoId, title, _res);
            window._showToast?.(`Redownload queued at ${_res}.`, "ok");
          }},
          { sep: true },
          { label: "Delete file", cls: "dim", action: async () => {
            const res = await bridgeCall("video_delete_file", filepath);
            if (res?.ok) {
              card.remove();
              window._showToast?.(res.message || "Video moved to app trash.", "ok");
            } else {
              window._showToast?.(res?.error || "Delete failed.", "error");
            }
          }},
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
        const api = window.YT?.api;
        // Loose manual single-video downloads aren't part of a tracked
        // subscription. Refresh metadata + Redownload both require a
        // subscription channel and hard-fail ("Channel not in
        // subscriptions") on these rows, so omit them. Everything else
        // (Show in Explorer, Transcribe, Re-transcribe, etc.) works on a
        // loose file. Default to tracked when the flag is absent.
        const tracked = card.dataset.tracked !== "0";
        const items = [
          { label: "Play video", action: () => {
            // The label promises the in-app Watch view (embedded HTML5
            // video + karaoke transcript), not the system default
            // external player. Route through _openVideoInWatch which is
            // the shared helper the Browse grid double-click also uses.
            if (filepath && typeof window._openVideoInWatch === "function") {
              window._openVideoInWatch({
                title, channel, filepath, video_id: videoId,
              });
            } else if (filepath && nativeBridgeUp()) {
              // Defensive fallback: if the Watch helper got removed,
              // open externally so the user still gets video playback.
              api?.browse_open_video?.(filepath);
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
            if (filepath) api?.browse_show_in_explorer?.(filepath);
          }},
          { label: "Bookmark video", action: () => _bookmarkVideo({ videoId, title, channel }) },
          { sep: true },
          ...(tracked ? [{ label: "Refresh metadata", action: async () => {
            if (!nativeBridgeUp()) {
              window._showToast?.("Refresh unavailable.", "warn");
              return;
            }
            const payload = {
              filepath, video_id: videoId, title, channel,
            };
            console.log("[refresh-meta] sending", payload);
            window._showToast?.({ msg: "Refreshing metadata…", ttlMs: 15000 });
            try {
              const res = await api?.browse_refresh_video_metadata?.(payload);
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
          }}] : []),
          { label: "Transcribe now", action: async () => {
            if (filepath && nativeBridgeUp()) {
              const model = await (window._askWhisperModel?.(`"${title}"`));
              if (model === null) return;
              api?.transcribe_enqueue?.(filepath, title, model || "");
              window._showToast?.("Queued for whisper.", "ok");
            }
          }},
          { label: "Re-transcribe…", action: async () => {
            if (!filepath || !nativeBridgeUp()) return;
            const model = await (window._askWhisperModel?.(`"${title}"`));
            if (!model) return;
            const res = await api?.transcribe_retranscribe?.(filepath, title, videoId || "");
            if (res?.ok) window._showToast?.(
              `Queued ${model} re-transcription.`, "ok");
            else window._showToast?.(res?.error || "Re-transcribe failed.", "error");
          }},
          ...(tracked ? [{ label: "Redownload…", action: async () => {
            // Per-VIDEO picker (see note on the other Redownload item) —
            // _askVideoRedownload returns the resolution; _askRedownload
            // was the wrong (whole-channel) function.
            const _res = await (window._askVideoRedownload?.(title));
            if (!_res) return;
            bridgeCall("video_redownload", videoId, title, _res);
            window._showToast?.(`Redownload queued at ${_res}.`, "ok");
          }}] : []),
          { sep: true },
          { label: "Delete file", cls: "dim", action: async () => {
            const res = await bridgeCall("video_delete_file", filepath);
            if (res?.ok) {
              card.remove();
              window._showToast?.(res.message || "Video moved to app trash.", "ok");
            } else {
              window._showToast?.(res?.error || "Delete failed.", "error");
            }
          }},
        ];
        showContextMenu(e.clientX, e.clientY, items);
      });
    }

    // Manual Downloads grid — single/loose downloads. Same .video-card class
    // as the Videos grid, but "Refresh metadata" routes through
    // manual_refresh_metadata (writes the JSONL next to the loose file; the
    // channel-scoped refresh hard-fails on non-subscription rows). Metadata +
    // YouTube links are gated on a known video_id; Redownload is omitted (it
    // needs a subscription channel). Transcribe/Re-transcribe work on any file.
    const manualGrid = document.getElementById("manual-grid");
    if (manualGrid) {
      manualGrid.addEventListener("contextmenu", (e) => {
        const card = e.target.closest(".video-card");
        if (!card) return;
        e.preventDefault();
        const filepath = card.dataset.filepath || "";
        const videoId = card.dataset.videoId || "";
        const title = card.dataset.title || "";
        const channel = card.dataset.channel || "";
        const ytUrl = videoId ? `https://www.youtube.com/watch?v=${videoId}` : "";
        const api = window.YT?.api;
        const items = [
          { label: "Play video", action: () => {
            if (filepath && typeof window._openVideoInWatch === "function") {
              window._openVideoInWatch({ title, channel, filepath, video_id: videoId });
            } else if (filepath && nativeBridgeUp()) {
              api?.browse_open_video?.(filepath);
            }
          }},
          ...(ytUrl ? [
            { label: "Open on YouTube", action: () => window.open(ytUrl, "_blank") },
            { label: "Copy YouTube URL", action: () => {
              navigator.clipboard?.writeText(ytUrl);
              window._showToast?.("URL copied.", "ok");
            }},
          ] : []),
          { label: "Show in Explorer", action: () => {
            if (filepath) api?.browse_show_in_explorer?.(filepath);
          }},
          ...(videoId ? [{ label: "Bookmark video",
            action: () => _bookmarkVideo({ videoId, title, channel }) }] : []),
          { sep: true },
          // Metadata refresh only when we have a video_id to fetch by.
          ...(videoId ? [{ label: "Refresh metadata", action: async () => {
            if (!nativeBridgeUp()) {
              window._showToast?.("Refresh unavailable.", "warn");
              return;
            }
            window._showToast?.({ msg: "Refreshing metadata…", ttlMs: 15000 });
            try {
              const res = await api?.manual_refresh_metadata?.(
                { filepath, video_id: videoId, title, channel });
              if (res?.ok) {
                window._showToast?.("Metadata refreshed.", "ok");
              } else {
                window._showToast?.(res?.error || "Refresh failed.",
                                    res?.transient ? "warn" : "error");
              }
            } catch (err) {
              window._showToast?.(`Refresh failed: ${err?.message || err}`, "error");
            }
          }}] : []),
          { label: "Transcribe now", action: async () => {
            if (filepath && nativeBridgeUp()) {
              const model = await (window._askWhisperModel?.(`"${title}"`));
              if (model === null) return;
              api?.transcribe_enqueue?.(filepath, title, model || "");
              window._showToast?.("Queued for whisper.", "ok");
            }
          }},
          { label: "Re-transcribe…", action: async () => {
            if (!filepath || !nativeBridgeUp()) return;
            const model = await (window._askWhisperModel?.(`"${title}"`));
            if (!model) return;
            const res = await api?.transcribe_retranscribe?.(filepath, title, videoId || "");
            if (res?.ok) window._showToast?.(`Queued ${model} re-transcription.`, "ok");
            else window._showToast?.(res?.error || "Re-transcribe failed.", "error");
          }},
          { sep: true },
          { label: "Delete file", cls: "dim", action: async () => {
            const res = await bridgeCall("video_delete_file", filepath);
            if (res?.ok) {
              card.remove();
              window._showToast?.(res.message || "Video moved to app trash.", "ok");
            } else {
              window._showToast?.(res?.error || "Delete failed.", "error");
            }
          }},
        ];
        showContextMenu(e.clientX, e.clientY, items);
      });
    }

    // Transcript segments in Watch view
    const transcript = document.getElementById("watch-transcript");
    if (transcript) {
      transcript.addEventListener("contextmenu", (e) => {
        const seg = e.target.closest(".seg");
        if (!seg) return;
        e.preventDefault();
        // The Watch transcript renders as flowing text with NO ".timestamp"
        // element — each segment/word carries its start time in data-s
        // (seconds), the same source the click-to-seek path uses. Derive the
        // moment from the clicked word (most precise), falling back to the
        // segment start. Previously this read a non-existent ".timestamp"
        // node → "" → 0, so bookmarks recorded the player head (0:00 on an
        // un-played video) instead of where the text is actually spoken.
        const wordEl = e.target.closest(".word");
        const start = parseFloat((wordEl && wordEl.dataset.s) || seg.dataset.s) || 0;
        const ts = _fmtTs(start);
        const text = seg.textContent.trim();
        const api = window.YT?.api;
        const v = _browseState.currentVideo || {};
        showContextMenu(e.clientX, e.clientY, [
          { label: `Copy segment`, action: () => navigator.clipboard?.writeText(text) },
          { label: `Copy timestamp + text`, action: () => navigator.clipboard?.writeText(`${ts} ${text}`) },
          { sep: true },
          { label: "Bookmark this moment\u2026", action: async () => {
            if (!nativeBridgeUp()) {
              window._showToast?.("Native mode required.", "warn");
              return;
            }
            // right-click on a transcript segment always
            // creates a timestamped bookmark with no note prompt.
            const res = await api?.bookmark_add?.({
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

    // ── Visible ⋮ kebab on grid cards ──────────────────────────────────
    // The card right-click menus above are the only way to reach card
    // actions, and nothing on screen hints they exist. Inject a corner
    // kebab (lazily, on first hover, so no card renderer needs touching)
    // that re-fires the SAME contextmenu event the grid listeners catch.
    _initCardKebabs();
  }

  function _initCardKebabs() {
    if (window._cardKebabsInited) return;
    window._cardKebabsInited = true;
    const SEL = ".video-card, .channel-card";
    // Lazy inject: add a kebab the first time a card is hovered. Cards
    // rebuilt on re-render are fresh elements (no _kebabAdded flag) so
    // they get a kebab on their next hover — survives grid refreshes
    // without hooking every renderer.
    document.addEventListener("mouseover", (e) => {
      const card = e.target.closest(SEL);
      if (!card || card._kebabAdded) return;
      card._kebabAdded = true;
      const host = card.classList.contains("video-card")
        ? (card.querySelector(".video-thumb") || card)
        : card;
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "card-kebab";
      btn.tabIndex = -1;
      btn.title = "Actions";
      btn.setAttribute("aria-label", "Card actions");
      btn.innerHTML = "&#8942;";
      // stopPropagation on the button's OWN listeners so the click never
      // bubbles to the card's open-video / open-channel handler.
      btn.addEventListener("mousedown", (ev) => ev.stopPropagation());
      btn.addEventListener("click", (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        const r = btn.getBoundingClientRect();
        card.dispatchEvent(new MouseEvent("contextmenu", {
          bubbles: true, cancelable: true,
          clientX: Math.min(window.innerWidth - 8, r.left),
          clientY: Math.min(window.innerHeight - 8, r.bottom),
        }));
      });
      host.appendChild(btn);
    });
  }

  function _parseTs(s) {
    if (!s) return 0;
    const parts = s.split(":").map(x => parseInt(x, 10) || 0);
    if (parts.length === 3) return parts[0]*3600 + parts[1]*60 + parts[2];
    if (parts.length === 2) return parts[0]*60 + parts[1];
    return 0;
  }

  // seconds -> "m:ss" (or "h:mm:ss"); inverse of _parseTs, for the copy /
  // bookmark labels now that the transcript stores time in data-s seconds.
  function _fmtTs(sec) {
    sec = Math.max(0, Math.floor(Number(sec) || 0));
    const h = Math.floor(sec / 3600);
    const m = Math.floor((sec % 3600) / 60);
    const s = sec % 60;
    const mm = h ? String(m).padStart(2, "0") : String(m);
    return (h ? h + ":" : "") + mm + ":" + String(s).padStart(2, "0");
  }

  // Whole-video bookmark from a grid card's right-click menu. Mirrors the
  // Watch view's "Whole video" path (watchActions.js btn-bookmark-now):
  // start_time = -1 is the sentinel the Bookmarks sub-mode reads to render
  // a favorites-style row (title + channel, no timestamp/transcript). Grid
  // cards have no playhead, so there's no "moment vs. whole video" prompt —
  // it's always the whole video. Shared by the #video-grid and #recent-grid
  // menus so both stay in lockstep.
  async function _bookmarkVideo({ videoId, title, channel }) {
    const api = window.YT?.api;
    if (!nativeBridgeUp()) {
      window._showToast?.("Native mode required.", "warn");
      return;
    }
    const res = await api?.bookmark_add?.({
      video_id: videoId || "",
      title: title || "",
      channel: channel || "",
      start_time: -1,
      text: "",
      note: "",
    });
    if (res?.ok) {
      window._showToast?.("Video bookmarked.", "ok");
      try { window.refreshBookmarks?.(); } catch {}
    } else {
      window._showToast?.(res?.error || "Bookmark failed.", "error");
    }
  }

  window.initBrowseContextMenus = initBrowseContextMenus;
})();
