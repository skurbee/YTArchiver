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

  let _activeContextTrigger = null;
  let _contextTriggerObserver = null;
  let _contextTriggerKeyHandler = null;

  function clearContextTrigger() {
    if (_activeContextTrigger) {
      _activeContextTrigger.setAttribute("aria-expanded", "false");
    }
    _activeContextTrigger = null;
    if (_contextTriggerKeyHandler) {
      document.removeEventListener("keydown", _contextTriggerKeyHandler, true);
      _contextTriggerKeyHandler = null;
    }
  }

  function markContextTrigger(trigger) {
    if (!trigger) return;
    if (_activeContextTrigger && _activeContextTrigger !== trigger) {
      _activeContextTrigger.setAttribute("aria-expanded", "false");
    }
    _activeContextTrigger = trigger;
    trigger.setAttribute("aria-expanded", "true");

    const root = document.getElementById("ctx-menu-root");
    if (root && !_contextTriggerObserver && typeof MutationObserver === "function") {
      _contextTriggerObserver = new MutationObserver(() => {
        if (!root.querySelector(".ctx-menu")) clearContextTrigger();
      });
      _contextTriggerObserver.observe(root, { childList: true });
    }
    if (!_contextTriggerKeyHandler) {
      _contextTriggerKeyHandler = (event) => {
        if (event.key === "Escape" || event.key === "Tab"
            || ((event.key === "Enter" || event.key === " ")
                && document.activeElement?.closest?.(".ctx-menu-item"))) {
          clearContextTrigger();
        }
      };
      document.addEventListener("keydown", _contextTriggerKeyHandler, true);
    }
    setTimeout(() => {
      document.addEventListener("click", clearContextTrigger, { once: true });
    }, 0);
    // A second right-click can replace this menu without a click in between.
    // Clear the old owner's state during that next contextmenu capture; the
    // replacement Browse handler will mark its own owner during bubbling.
    document.addEventListener(
      "contextmenu", clearContextTrigger, { once: true, capture: true });
  }
  window._markBrowseContextTrigger = markContextTrigger;

  function wireKeyboardContextMenu(container, selector) {
    if (!container) return;
    container.addEventListener("keydown", (event) => {
      const keyboardMenu = event.key === "ContextMenu"
        || (event.shiftKey && event.key === "F10");
      if (!keyboardMenu) return;
      const target = event.target.closest(selector);
      if (!target || !container.contains(target)) return;
      event.preventDefault();
      event.stopPropagation();
      const rect = target.getBoundingClientRect();
      target.dispatchEvent(new MouseEvent("contextmenu", {
        bubbles: true,
        cancelable: true,
        clientX: Math.max(4, Math.min(window.innerWidth - 8, rect.left + 12)),
        clientY: Math.max(4, Math.min(window.innerHeight - 8, rect.top + 12)),
      }));
    });
  }

  function showTrashToast(message, kind) {
    window._showToast?.({
      msg: message,
      kind,
      ttlMs: kind === "warn" ? 7000 : 5000,
      action: {
        label: "View Trash",
        onClick: () => window._goToTrash?.(),
      },
    });
  }

  async function checkedAction(action, {
    success = "",
    failure = "Action failed.",
  } = {}) {
    try {
      const result = await action();
      if (!result?.ok) {
        window._showToast?.(result?.error || failure, "error");
        return result;
      }
      const successMessage = typeof success === "function"
        ? success(result) : success;
      if (successMessage) window._showToast?.(successMessage, "ok");
      return result;
    } catch (error) {
      window._showToast?.(
        `${failure.replace(/[.]+$/, "")}: ${error?.message || error}`,
        "error");
      return { ok: false, error: String(error) };
    }
  }

  async function copyText(text, success = "Copied.") {
    try {
      if (!navigator.clipboard?.writeText) {
        throw new Error("Clipboard access is unavailable");
      }
      await navigator.clipboard.writeText(String(text || ""));
      if (success) window._showToast?.(success, "ok");
      return true;
    } catch (error) {
      window._showToast?.(
        `Could not copy: ${error?.message || error}`, "error");
      return false;
    }
  }

  async function queueYearRedownload(api, channel, resolution, scope) {
    const label = resolution === "best" ? "best available" : `${resolution}p`;
    return checkedAction(
      () => api?.chan_redownload?.({ name: channel }, resolution, scope),
      {
        success: (result) => {
          const queued = Number(result?.queued);
          return Number.isFinite(queued)
            ? `Queued ${queued.toLocaleString()} video(s) from ${scope.year} at ${label}.`
            : `Redownload queued for ${scope.year} at ${label}.`;
        },
        failure: `Could not queue the ${scope.year} redownload.`,
      });
  }

  async function removeChannelAndLeaveDetail(name) {
    const result = await window._removeChannelWithPrompt?.(name);
    if (!(result?.ok || result?.subscription_removed)) return result;
    const current = _browseState.currentChannel;
    const currentIds = new Set([current?.folder, current?.name]
      .map(value => String(value || "").trim().toLowerCase())
      .filter(Boolean));
    if (currentIds.has(String(name || "").trim().toLowerCase())) {
      _browseState.currentChannel = null;
      _browseState.videos = [];
      document.querySelector(
        '.submode-btn[data-submode="channels"]')?.click();
    }
    return result;
  }

  async function queueTranscription(api, filepath, title, model) {
    if (!filepath || !nativeBridgeUp()) return;
    return checkedAction(
      () => api?.transcribe_enqueue?.(filepath, title, model || ""),
      { success: "Queued for Whisper.", failure: "Could not queue transcription." });
  }

  async function queueVideoRedownload(videoId, title, resolution) {
    return checkedAction(
      () => bridgeCall("video_redownload", videoId, title, resolution),
      {
        success: `Redownload queued at ${resolution}.`,
        failure: "Could not queue redownload.",
      });
  }

  function applyThumbnailToCard(card, thumbnailUrl) {
    if (!card || !thumbnailUrl) return;
    const wrap = card.querySelector(".video-thumb");
    if (!wrap) return;
    const previousImg = wrap.querySelector(".video-thumb-img");
    const placeholders = [...wrap.children].filter((child) =>
      child.tagName === "SPAN"
      && !child.classList.contains("video-removed-badge")
      && !child.classList.contains("video-duration-badge"));
    const img = document.createElement("img");
    img.className = "video-thumb-img";
    img.alt = "";
    img.loading = "eager";
    img.decoding = "async";
    // Keep the existing image/gradient + play marker visible until Chromium
    // has actually decoded the replacement. A successful bridge response only
    // guarantees that a URL was produced; the local file server request can
    // still fail independently.
    img.style.visibility = "hidden";
    img.addEventListener("load", () => {
      if (!img.isConnected || img.parentElement !== wrap) return;
      if (previousImg && previousImg !== img) previousImg.remove();
      placeholders.forEach((placeholder) => placeholder.remove());
      wrap.style.background = "";
      img.style.visibility = "";
    }, { once: true });
    img.addEventListener("error", () => {
      // Leave the previous image or gradient placeholder intact. In
      // particular, do not strand the card with a broken-image element after
      // a transient fileserver/drive failure.
      img.remove();
    }, { once: true });
    wrap.insertBefore(img, previousImg || wrap.firstChild);
    // A forced refresh can overwrite the same file path. The local image
    // server intentionally caches thumbnails, so give this one request a new
    // URL while preserving its existing access-token query string.
    try {
      const freshUrl = new URL(thumbnailUrl, window.location.href);
      freshUrl.searchParams.set("refresh", `${Date.now()}`);
      img.src = freshUrl.href;
    } catch (_error) {
      const separator = String(thumbnailUrl).includes("?") ? "&" : "?";
      img.src = `${thumbnailUrl}${separator}refresh=${Date.now()}`;
    }
  }

  function matchingVideoCards(payload) {
    const norm = (value) => String(value || "").replace(/\\/g, "/").toLowerCase();
    const filepath = norm(payload?.filepath);
    const videoId = String(payload?.video_id || "");
    return [...document.querySelectorAll(".video-card")].filter((candidate) => {
      if (filepath) return norm(candidate.dataset.filepath) === filepath;
      return !!videoId && candidate.dataset.videoId === videoId;
    });
  }

  function refreshChannelCards(channel, thumbnailPatch = null) {
    const reapplyThumbnail = () => {
      if (!thumbnailPatch?.url) return;
      matchingVideoCards(thumbnailPatch.payload)
        .filter((candidate) => candidate !== thumbnailPatch.card)
        .forEach((candidate) => applyThumbnailToCard(candidate, thumbnailPatch.url));
    };
    try {
      const result = window._refreshChannelVideosIfLoaded?.(channel || undefined);
      if (result && typeof result.catch === "function") {
        result.then(reapplyThumbnail).catch(() => {});
      } else {
        queueMicrotask(reapplyThumbnail);
      }
    } catch (_error) {
      // The clicked card is patched immediately above. A background grid
      // refresh is helpful but should never turn a successful repair into an
      // error toast.
    }
  }

  function trackedVideoMetadataMenu(api, card, payload) {
    const runMetadataRefresh = async ({ mode, progress, success, failure }) => {
      if (!nativeBridgeUp()) {
        window._showToast?.("Refresh unavailable.", "warn");
        return;
      }
      window._showToast?.({ msg: progress, ttlMs: 30000 });
      try {
        const res = await api?.browse_refresh_video_metadata?.({
          ...payload,
          mode,
        });
        if (!res?.ok) {
          window._showToast?.(
            res?.error || failure, res?.transient ? "warn" : "error");
          return;
        }
        if (res.thumbnail_url) applyThumbnailToCard(card, res.thumbnail_url);
        if (res.video_id) card.dataset.videoId = res.video_id;
        window._showToast?.(
          res.warning || success, res.warning ? "warn" : "ok");
        refreshChannelCards(payload.channel, res.thumbnail_url ? {
          payload,
          url: res.thumbnail_url,
          card,
        } : null);
      } catch (error) {
        window._showToast?.(
          `${failure} ${error?.message || error}`, "error");
      }
    };

    const refreshThumbnail = async () => {
      if (!nativeBridgeUp()) {
        window._showToast?.("Refresh unavailable.", "warn");
        return;
      }
      window._showToast?.({ msg: "Refreshing thumbnail…", ttlMs: 30000 });
      try {
        const res = await api?.browse_repair_video_thumbnail?.({
          ...payload,
          force: true,
        });
        if (!res?.ok) {
          window._showToast?.(
            res?.error || "Could not refresh the thumbnail.", "error");
          return;
        }
        if (res.thumbnail_url) applyThumbnailToCard(card, res.thumbnail_url);
        if (res.video_id) card.dataset.videoId = res.video_id;
        window._showToast?.(
          res.source === "local"
            ? "Thumbnail refreshed from the saved video."
            : "Thumbnail refreshed.",
          "ok",
        );
        refreshChannelCards(payload.channel, res.thumbnail_url ? {
          payload,
          url: res.thumbnail_url,
          card,
        } : null);
      } catch (error) {
        window._showToast?.(
          `Could not refresh the thumbnail. ${error?.message || error}`,
          "error",
        );
      }
    };

    return [
      {
        label: "Refresh views & likes",
        title: "Update this video's view and like counts",
        action: () => runMetadataRefresh({
          mode: "stats",
          progress: "Refreshing views and likes…",
          success: "Views and likes refreshed.",
          failure: "Could not refresh views and likes.",
        }),
      },
      {
        label: "Refresh comments",
        title: "Update the saved comments for this video",
        action: () => runMetadataRefresh({
          mode: "comments",
          progress: "Refreshing comments…",
          success: "Comments refreshed.",
          failure: "Could not refresh comments.",
        }),
      },
      {
        label: "Refresh thumbnail",
        title: "Replace this video's thumbnail",
        action: refreshThumbnail,
      },
      { sep: true },
      {
        label: "Refresh all",
        title: "Update views, likes, comments, and thumbnail",
        action: () => runMetadataRefresh({
          mode: "all",
          progress: "Refreshing video information…",
          success: "Video information refreshed.",
          failure: "Could not refresh video information.",
        }),
      },
    ];
  }

  function sameVideoIdentity(video, filepath, videoId) {
    const norm = (value) => String(value || "").replace(/\\/g, "/").toLowerCase();
    if (videoId && video?.video_id) return String(video.video_id) === String(videoId);
    return !!(filepath && video?.filepath && norm(video.filepath) === norm(filepath));
  }

  async function refreshAfterVideoTrash(card, filepath, videoId) {
    const parentId = card?.closest?.("#video-grid, #recent-grid, #manual-grid")?.id;
    if (parentId === "video-grid") {
      _browseState.videos = (_browseState.videos || []).filter(
        (video) => !sameVideoIdentity(video, filepath, videoId));
      const sort = document.getElementById("browse-sort")?.value || "newest";
      window.sortCurrentVideos?.(sort);
      const channel = _browseState.currentChannel;
      const channelName = channel?.folder || channel?.name || "";
      await Promise.allSettled([
        window._refreshChannelVideosIfLoaded?.(channelName),
        window.refreshSubsTable?.({ primeBrowse: false }),
      ]);
    } else if (parentId === "recent-grid") {
      await window._loadVideosView?.();
      await window.refreshSubsTable?.({ primeBrowse: false });
    } else if (parentId === "manual-grid") {
      await window._loadManualView?.();
    } else {
      card?.remove();
    }
  }

  async function moveVideoFileToTrash(card, filepath, videoId = "") {
    if (!filepath) {
      window._showToast?.("No downloaded file was found for this video.", "warn");
      return;
    }
    const isManualDownload = !!card?.closest?.("#manual-grid");
    const ok = await askDanger(
      isManualDownload ? "Remove downloaded video?" : "Move file to trash?",
      isManualDownload
        ? "Remove this downloaded video from YTArchiver?\n\n"
          + "If it is saved outside the archive, the file stays where it is."
        : "Move this video and its related files to YTArchiver Trash?\n\n"
          + "You can restore it from Trash.",
      isManualDownload ? "Remove" : "Move to trash");
    if (!ok) return;

    try {
      const res = await bridgeCall("video_delete_file", filepath);
      const moved = !!(res?.ok || res?.file_trashed);
      if (!moved) {
        window._showToast?.(
          res?.error || "Move to trash failed.", "error");
        return;
      }
      await refreshAfterVideoTrash(card, filepath, videoId);
      if (res?.catalog_entry_removed && res?.external_file_preserved) {
        window._showToast?.(
          res.warning || res.message ||
            "Removed from YTArchiver. The external file was left in place.",
          res.warning ? "warn" : "ok");
        return;
      }
      // Trash owns an authoritative backend count. Refresh it only after the
      // move has committed; never increment a guessed local badge.
      window._onTrashChanged?.();
      if (res?.warning || !res?.ok) {
        showTrashToast(
          res?.warning || res?.error ||
            "File moved to Trash, but the library refresh needs attention.",
          "warn");
      } else {
        showTrashToast(
          res.message || "Video moved to YTArchiver Trash.", "ok");
      }
    } catch (error) {
      window._showToast?.(
        `Move to trash failed: ${error?.message || error}`, "error");
    }
  }

  // Shared "Cancel reorg / date fix" action for the Reorg submenus here
  // AND in the Subs table (columnSort.js references it via window).
  // Both backend passes stop at their next file checkpoint; the toast
  // reports whether anything was actually running so a stray click on
  // an idle app doesn't look like it did something.
  window._cancelFolderOps = async function () {
    if (!nativeBridgeUp()) {
      window._showToast?.("YTArchiver isn't ready yet. Try again in a moment.", "warn");
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
        const name = card.dataset.channelName
          || card.querySelector(".channel-card-name")?.firstChild?.textContent
          || "";
        const channelRef = {
          name,
          folder: card.dataset.channelFolder || "",
          url: card.dataset.channelUrl || "",
        };
        const api = window.YT?.api;
        // Live counters are stashed on the card by renderChannelGrid.
        const _pendTx = Math.max(
          0, parseInt(card.dataset.pendingTx || "0", 10) || 0);
        const _pendMeta = parseInt(card.dataset.pendingMeta || "0", 10) || 0;
        const _missingInfoLabel = _pendMeta > 0
          ? `Fix missing information (${_pendMeta} pending)`
          : "Fix missing information";
        const _hasPendingRedownload = card.dataset.pendingRedownload === "1";
        const _redownloadRes = card.dataset.redownloadRes || "best";
        const _redownloadResLabel = _redownloadRes === "best"
          ? "Best available"
          : `${_redownloadRes}p`;
        showContextMenu(e.clientX, e.clientY, [
          { header: "Open & manage" },
          { label: "Open videos", cls: "primary", action: () => card.click() },
          { label: "Open folder", cls: "primary",
            action: () => checkedAction(
              () => api?.chan_open_folder?.(name),
              { failure: "Could not open channel folder." }) },
          { label: "Open channel on YouTube", cls: "primary",
            action: () => checkedAction(
              () => api?.chan_open_url?.(name),
              { failure: "Could not open the channel on YouTube." }) },
          { sep: true },
          { label: "Sync now", cls: "primary",
            action: async () => {
              try {
                const result = await api?.sync_one_channel?.({ name });
                window.YT?.bridge?.reportSyncOneResult?.(result, name);
              } catch (error) {
                window.YT?.bridge?.reportSyncOneResult?.({
                  ok: false,
                  error: "Sync failed: " + (error?.message || error),
                }, name);
              }
            }},
          { label: "Edit settings", cls: "primary",
            action: () => window._editChannelFromBrowse?.(name) },
          { sep: true },
          { header: "Maintenance" },
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
          { label: "Metadata",
            title: "Repair missing information or update saved YouTube details",
            submenu: [
              { label: _missingInfoLabel,
                title: "Fetch information and thumbnails only where they are missing",
                action: () => checkedAction(
                  () => api?.metadata_fill_missing_channel?.({ name }),
                  { success: "Missing-information check started.",
                    failure: "Missing-information check did not start." }) },
              { label: "Repair missing thumbnails",
                title: "Download only thumbnail image files that are missing",
                action: async () => {
                  const r = await api?.refetch_thumbnails?.({ name });
                  if (r?.started) {
                    window._showToast?.(
                      `Thumbnail repair started for ${name}.`, "ok");
                  } else {
                    window._showToast?.(
                      r?.error || "Thumbnail repair did not start.", "error");
                  }
                }},
              { sep: true },
              { label: "Refresh views & likes",
                title: "Refresh views, likes, comment totals, and YouTube availability",
                submenu: [
                  { label: "Last 7 days",
                    action: () => checkedAction(
                      () => api?.metadata_refresh_views_channel?.({ name }, 7),
                      { success: "Video-statistics refresh started.", failure: "Video-statistics refresh did not start." }) },
                  { label: "Last 30 days",
                    action: () => checkedAction(
                      () => api?.metadata_refresh_views_channel?.({ name }, 30),
                      { success: "Video-statistics refresh started.", failure: "Video-statistics refresh did not start." }) },
                  { label: "Last 90 days",
                    action: () => checkedAction(
                      () => api?.metadata_refresh_views_channel?.({ name }, 90),
                      { success: "Video-statistics refresh started.", failure: "Video-statistics refresh did not start." }) },
                  { label: "All videos (slow)",
                    action: () => checkedAction(
                      () => api?.metadata_refresh_views_channel?.({ name }, null),
                      { success: "Video-statistics refresh started.", failure: "Video-statistics refresh did not start." }) },
                ]},
              { label: "Refresh comments",
                title: "Fetch updated saved comments for archived videos",
                submenu: [
                  { label: "Last 7 days",
                    action: () => checkedAction(
                      () => api?.metadata_refresh_comments_channel?.({ name }, 7),
                      { success: "Comments refresh started.", failure: "Comments refresh did not start." }) },
                  { label: "Last 30 days",
                    action: () => checkedAction(
                      () => api?.metadata_refresh_comments_channel?.({ name }, 30),
                      { success: "Comments refresh started.", failure: "Comments refresh did not start." }) },
                  { label: "Last 90 days",
                    action: () => checkedAction(
                      () => api?.metadata_refresh_comments_channel?.({ name }, 90),
                      { success: "Comments refresh started.", failure: "Comments refresh did not start." }) },
                  { label: "All videos (slow)",
                    action: () => checkedAction(
                      () => api?.metadata_refresh_comments_channel?.({ name }, null),
                      { success: "Comments refresh started.", failure: "Comments refresh did not start." }) },
                ]},
            ]},
          { label: "Transcribe all missing",
            count: _pendTx,
            countDim: _pendTx === 0,
            countAriaLabel: `Transcribe all missing, ${_pendTx} untranscribed video${_pendTx === 1 ? "" : "s"}`,
            title: "Transcribe only videos that don't have a transcript yet (YouTube captions first, Whisper fallback)",
            action: () => window._askTranscribeChannel?.(channelRef) },
          { sep: true },
          { label: "Reorg folder",
            submenu: [
              { label: "Flat (no split)", action: () => checkedAction(
                () => api?.reorg_channel_folder?.({ name }, false, false, false),
                { success: "Folder reorganization started.", failure: "Folder reorganization did not start." }) },
              { label: "Split by year", action: () => checkedAction(
                () => api?.reorg_channel_folder?.({ name }, true, false, false),
                { success: "Folder reorganization started.", failure: "Folder reorganization did not start." }) },
              { label: "Split by year + month", action: () => checkedAction(
                () => api?.reorg_channel_folder?.({ name }, true, true, false),
                { success: "Folder reorganization started.", failure: "Folder reorganization did not start." }) },
              { label: "Re-check dates + year/month", action: () => checkedAction(
                () => api?.reorg_channel_folder?.({ name }, true, true, true),
                { success: "Date check and reorganization started.", failure: "Date check did not start." }) },
              { label: "Fix file dates only", action: () => checkedAction(
                () => api?.chan_fix_file_dates?.({ name }),
                { success: "File-date repair started.", failure: "File-date repair did not start." }) },
              { sep: true },
              // Cancel affordance for the two long passes above — both
              // were previously unstoppable from the UI (audit S4).
              { label: "Cancel running reorg / date fix",
                action: () => window._cancelFolderOps?.() },
            ]},
          // "Fetch channel art" removed — now bundled with the metadata sweep.
          _hasPendingRedownload
            ? {
                label: `Continue redownload at ${_redownloadResLabel}`,
                action: async () => {
                  const r = await api?.chan_redownload?.(
                    { name }, _redownloadRes);
                  if (r?.ok) {
                    window._showToast?.(
                      r.queued
                        ? `Queued redownload of ${name}.`
                        : `Resumed redownload of ${name}.`,
                      "ok");
                  } else {
                    window._showToast?.(
                      r?.error || "Resume failed.", "error");
                  }
                },
              }
            : {
                label: "Redownload at\u2026",
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
                 ],
              },
          { sep: true },
          { label: "Remove channel…",
            cls: "danger",
            title: "Stop syncing this channel and choose whether to keep its downloaded files",
            action: () => removeChannelAndLeaveDetail(name) },
        ]);
        markContextTrigger(card);
      });
      wireKeyboardContextMenu(channelGrid, ".channel-card");
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
                  action: () => queueYearRedownload(api, chan, "best", _scope) },
                { label: "2160p (4K)",
                  action: () => queueYearRedownload(api, chan, "2160", _scope) },
                { label: "1440p",
                  action: () => queueYearRedownload(api, chan, "1440", _scope) },
                { label: "1080p",
                  action: () => queueYearRedownload(api, chan, "1080", _scope) },
                { label: "720p",
                  action: () => queueYearRedownload(api, chan, "720", _scope) },
                { label: "480p",
                  action: () => queueYearRedownload(api, chan, "480", _scope) },
                { label: "360p",
                  action: () => queueYearRedownload(api, chan, "360", _scope) },
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
        const metadataChannel = channel
          || (_browseState.currentChannel?.folder
              || _browseState.currentChannel?.name || "");
        const metadataPayload = {
          filepath, video_id: videoId, title, channel: metadataChannel,
        };
        showContextMenu(e.clientX, e.clientY, [
          { label: "Play video", action: () => {
            // The label promises the in-app Watch view (embedded HTML5
            // video + karaoke transcript), not the system default
            // external player. Route through _openVideoInWatch which is
            // the shared helper the Browse grid double-click also uses.
            if (filepath && typeof window._openVideoInWatch === "function") {
              window._openVideoInWatch({
                title, channel, filepath, video_id: videoId, tracked: true,
              });
            } else if (filepath && nativeBridgeUp()) {
              window._openVideoExternally?.(filepath);
            }
          }},
          ...(ytUrl ? [
            { label: "Open on YouTube", action: () => window.open(ytUrl, "_blank") },
            { label: "Copy YouTube URL", action: () => copyText(ytUrl, "URL copied.") },
          ] : []),
          { label: "Show in Explorer", action: () => filepath && checkedAction(
            () => api?.browse_show_in_explorer?.(filepath),
            { failure: "Could not show this file in Explorer." }) },
          ...(videoId ? [{ label: "Bookmark video", action: () => _bookmarkVideo({
            videoId, title,
            channel: channel || (window._browseState?.currentChannel?.folder || ""),
          }) }] : []),
          { sep: true },
          ...(filepath ? [{
            label: "Metadata",
            title: "Refresh this video's saved information",
            submenu: trackedVideoMetadataMenu(
              api, card, metadataPayload),
          }] : []),
          { label: "Transcribe now", action: async () => {
            if (filepath && nativeBridgeUp()) {
              // Manual → Whisper model picker (60s countdown auto-picks default).
              const model = await (window._askWhisperModel?.(`"${title}"`));
              if (model === null) return;
              await queueTranscription(api, filepath, title, model);
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
          ...(videoId ? [{ label: "Redownload\u2026", action: async () => {
            // Per-VIDEO resolution picker (mirrors the Watch view's
            // video_redownload flow), then queue the single-video
            // redownload. Uses _askVideoRedownload \u2014 NOT _askRedownload,
            // which is the whole-channel confirm/executor and produced the
            // "undefinedp" / "redownload every video in <title>" bug.
            const _res = await (window._askVideoRedownload?.(title));
            if (!_res) return;
            await queueVideoRedownload(videoId, title, _res);
          }}] : []),
          { sep: true },
          { label: "Move file to trash\u2026", cls: "danger",
            action: () => moveVideoFileToTrash(card, filepath, videoId) },
        ]);
        markContextTrigger(card);
      });
      wireKeyboardContextMenu(videoGrid, ".video-card");
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
        // Former subscriptions and manual files can still be watched and
        // maintained locally, but only current subscriptions can redownload.
        const tracked = card.dataset.tracked !== "0";
        const metadataPayload = {
          filepath, video_id: videoId, title, channel,
        };
        const items = [
          { label: "Play video", action: () => {
            // The label promises the in-app Watch view (embedded HTML5
            // video + karaoke transcript), not the system default
            // external player. Route through _openVideoInWatch which is
            // the shared helper the Browse grid double-click also uses.
            if (filepath && typeof window._openVideoInWatch === "function") {
              window._openVideoInWatch({
                title, channel, filepath, video_id: videoId, tracked,
              });
            } else if (filepath && nativeBridgeUp()) {
              window._openVideoExternally?.(filepath);
            }
          }},
          ...(ytUrl ? [
            { label: "Open on YouTube", action: () => window.open(ytUrl, "_blank") },
            { label: "Copy YouTube URL", action: () => copyText(ytUrl, "URL copied.") },
          ] : []),
          { label: "Show in Explorer", action: () => filepath && checkedAction(
            () => api?.browse_show_in_explorer?.(filepath),
            { failure: "Could not show this file in Explorer." }) },
          ...(videoId ? [{ label: "Bookmark video",
            action: () => _bookmarkVideo({ videoId, title, channel }) }] : []),
          { sep: true },
          ...(tracked && filepath ? [{
            label: "Metadata",
            title: "Refresh this video's saved information",
            submenu: trackedVideoMetadataMenu(
              api, card, metadataPayload),
          }] : videoId ? [{ label: "Refresh metadata", action: async () => {
            if (!nativeBridgeUp()) {
              window._showToast?.("Refresh unavailable.", "warn");
              return;
            }
            window._showToast?.({ msg: "Refreshing metadata…", ttlMs: 15000 });
            try {
              const res = await api?.manual_refresh_metadata?.(metadataPayload);
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
              await queueTranscription(api, filepath, title, model);
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
          ...(tracked && videoId ? [{ label: "Redownload…", action: async () => {
            // Per-VIDEO picker (see note on the other Redownload item) —
            // _askVideoRedownload returns the resolution; _askRedownload
            // was the wrong (whole-channel) function.
            const _res = await (window._askVideoRedownload?.(title));
            if (!_res) return;
            await queueVideoRedownload(videoId, title, _res);
          }}] : []),
          { sep: true },
          { label: "Move file to trash\u2026", cls: "danger",
            action: () => moveVideoFileToTrash(card, filepath, videoId) },
        ];
        showContextMenu(e.clientX, e.clientY, items);
        markContextTrigger(card);
      });
      wireKeyboardContextMenu(recentGrid, ".video-card");
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
              window._openVideoInWatch({
                title, channel, filepath, video_id: videoId, tracked: false,
              });
            } else if (filepath && nativeBridgeUp()) {
              window._openVideoExternally?.(filepath);
            }
          }},
          ...(ytUrl ? [
            { label: "Open on YouTube", action: () => window.open(ytUrl, "_blank") },
            { label: "Copy YouTube URL", action: () => copyText(ytUrl, "URL copied.") },
          ] : []),
          { label: "Show in Explorer", action: () => filepath && checkedAction(
            () => api?.browse_show_in_explorer?.(filepath),
            { failure: "Could not show this file in Explorer." }) },
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
              await queueTranscription(api, filepath, title, model);
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
          { label: "Remove from YTArchiver\u2026", cls: "danger",
            action: () => moveVideoFileToTrash(card, filepath, videoId) },
        ];
        showContextMenu(e.clientX, e.clientY, items);
        markContextTrigger(card);
      });
      wireKeyboardContextMenu(manualGrid, ".video-card");
    }

    // Transcript segments in Watch view
    const transcript = document.getElementById("watch-transcript");
    if (transcript) {
      transcript.addEventListener("contextmenu", (e) => {
        const seg = e.target.closest(".seg")
          || e.target.closest(".transcript-para")?.querySelector(".seg");
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
        const items = [
          { label: `Copy segment`, action: () => copyText(text) },
          { label: `Copy timestamp + text`, action: () => copyText(`${ts} ${text}`) },
          ...(v.video_id ? [{ sep: true },
          { label: "Bookmark this moment\u2026", action: async () => {
            if (!nativeBridgeUp()) {
              window._showToast?.("YTArchiver isn't ready yet. Try again in a moment.", "warn");
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
          }}] : []),
        ];
        showContextMenu(e.clientX, e.clientY, items);
        markContextTrigger(e.target.closest("[aria-haspopup='menu']"));
      });
      wireKeyboardContextMenu(transcript, ".para-ts, .seg");
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
      btn.setAttribute("aria-haspopup", "menu");
      btn.setAttribute("aria-expanded", "false");
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
        markContextTrigger(btn);
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
      window._showToast?.("YTArchiver isn't ready yet. Try again in a moment.", "warn");
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
