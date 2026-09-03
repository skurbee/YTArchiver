/* ═══════════════════════════════════════════════════════════════════════
   removeChannel.js — shared "remove channel" two-step flow

   Every call site that removes a channel — Edit-panel Remove button,
   Delete-key on a Subs row, channel-card right-click "Remove channel",
   and channel-card context menus — funnels through this helper
   so the user gets the same two-step confirmation:

     1. "Remove channel from your subscriptions?" (danger)
     2. "Also move the downloaded folder to YTArchiver Trash?"

   Returns the backend result or null if the user cancelled.

   Publishes:
     window._removeChannelWithPrompt(name) -> Promise<result|null>

   Reads:
     window.pywebview.api.subs_remove_channel — Python bridge
     window.askDanger / askChoice — modals.js
     window._showToast — toasts.js
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

  let removalInFlight = false;

  async function refreshChannelViews() {
    try {
      if (typeof window.refreshSubsTable === "function") {
        await window.refreshSubsTable();
      }
      return true;
    } catch (error) {
      console.warn("[remove-channel] list refresh failed", error);
      return false;
    }
  }

  function removalCommitted(res) {
    // `ok` means the whole operation succeeded. `subscription_removed`
    // remains true when the subscription/folder commit succeeded but a later
    // catalog cleanup produced a warning.
    // `deleted_folder` alone is not enough: a failed config save can be
    // followed by a failed trash rollback, leaving files moved while the
    // subscription itself remains configured.
    return !!(res?.ok || res?.subscription_removed);
  }

  function leaveRemovedChannelDetail(name) {
    const state = window._browseState;
    const current = state?.currentChannel;
    if (!state || !current) return;
    const wanted = String(name || "").trim().toLowerCase();
    const aliases = [current.folder, current.name, current.folder_override]
      .map((value) => String(value || "").trim().toLowerCase())
      .filter(Boolean);
    if (!wanted || !aliases.includes(wanted)) return;
    const wasOpenDetail = state.submode === "channels"
      && (state.view === "videos" || state.view === "watch");
    state.currentChannel = null;
    state.videos = [];
    state.currentVideo = null;
    state.watchReturnTo = null;
    if (wasOpenDetail) {
      document.querySelector(
        '.submode-btn[data-submode="channels"]')?.click();
    }
  }

  async function undoKeptFilesRemoval(name, undoId) {
    try {
      const result = await bridgeCall("subs_undo_remove", undoId || null);
      if (!result?.ok) {
        window._showToast?.(result?.error || "Undo failed.", "error");
        return;
      }
      const refreshed = await refreshChannelViews();
      window._showToast?.(
        refreshed
          ? `Restored "${name}" to subscriptions.`
          : `Restored "${name}" to subscriptions. Reopen this tab to refresh the list.`,
        refreshed ? "ok" : "warn");
    } catch (error) {
      window._showToast?.("Undo failed: " + error, "error");
    }
  }

  window._removeChannelWithPrompt = async function (name) {
    if (!nativeBridgeUp()) {
      window._showToast?.("YTArchiver isn't ready yet. Try again in a moment.", "warn");
      return null;
    }
    const cleanName = String(name || "").trim();
    if (!cleanName) {
      window._showToast?.("Could not identify the channel to remove.", "error");
      return { ok: false, error: "Missing channel name" };
    }
    if (removalInFlight) {
      window._showToast?.(
        "A channel removal is already in progress. Please let it finish.",
        "warn");
      return { ok: false, in_progress: true };
    }

    removalInFlight = true;
    let progressToast = null;
    try {
      const ok1 = await window.askDanger(
        "Remove channel",
        `Remove channel "${cleanName}" from your subscriptions?\n\n` +
        "This stops future syncs. Your downloaded files will stay where they " +
        "are unless you choose to move them on the next screen.",
        "Remove");
      if (!ok1) return null;

      const fileChoice = await window.askChoice({
        title: "What should happen to downloaded files?",
        message: `Choose whether to keep "${cleanName}"'s downloaded videos, ` +
                 "transcripts, metadata, and thumbnails in the archive, or " +
                 "move the channel folder to YTArchiver Trash.",
        choices: [
          { label: "Keep files", value: "keep", kind: "primary" },
          { label: "Move files to Trash", value: "trash", kind: "danger" },
        ],
        cancel: "Cancel removal",
        cancelKind: "ghost",
      });
      if (fileChoice === null) return null;
      const moveFilesToTrash = fileChoice === "trash";

      progressToast = window._showToast?.({
        msg: moveFilesToTrash
          ? `Removing "${cleanName}" and moving its files to YTArchiver Trash…`
          : `Removing "${cleanName}" from subscriptions…`,
        kind: "warn",
        persist: true,
      });

      let res;
      try {
        res = await bridgeCall(
          "subs_remove_channel", { name: cleanName }, moveFilesToTrash);
      } catch (error) {
        progressToast?.dismiss?.(true);
        progressToast = null;
        window._showToast?.("Remove failed: " + error, "error");
        return { ok: false, error: String(error) };
      }

      const committed = removalCommitted(res);
      if (!committed) {
        progressToast?.dismiss?.(true);
        progressToast = null;
        const filesMovedWithoutRemoval = !!(
          res?.files_removed || res?.deleted_folder);
        window._showToast?.(
          filesMovedWithoutRemoval
            ? `${res?.error || "The subscription could not be removed."} ` +
              "Its folder may be in YTArchiver Trash; restart YTArchiver " +
              "so recovery can finish before trying again."
            : (res?.error || res?.delete_error || "Remove failed."),
          "error");
        return res || { ok: false, error: "Remove failed" };
      }

      const refreshed = await refreshChannelViews();
      // Every removal entry point shares this helper. If the user removed the
      // channel whose Browse detail page is open, leave that now-dead page
      // instead of keeping stale cards and actions on screen.
      leaveRemovedChannelDetail(cleanName);
      progressToast?.dismiss?.(true);
      progressToast = null;

      const filesMovedToTrash = moveFilesToTrash
        && !!(res?.files_removed || res?.deleted_folder);
      if (filesMovedToTrash) window._onTrashChanged?.();

      const catalogWarning = res?.catalog_warning
        || (res?.catalog_cleanup_ok === false ? res?.delete_error : "");
      const processingWarning = res?.processing_queue_warning || "";
      const refreshWarning = refreshed ? ""
        : "the channel list could not be refreshed; reopen this tab to update it.";
      const keepFilesUndoRefreshWarning = !!(
        refreshWarning && !moveFilesToTrash && res?.can_undo);
      if (catalogWarning || processingWarning
          || (refreshWarning && !keepFilesUndoRefreshWarning)) {
        const warningParts = [];
        if (catalogWarning) {
          warningParts.push(
            `the library index could not be fully cleaned up: ${catalogWarning}`);
        }
        if (processingWarning) {
          warningParts.push(
            `queued Processing work could not be fully cleaned up: ` +
            processingWarning);
        }
        if (refreshWarning) warningParts.push(refreshWarning);
        const message = `Channel removed, but ${warningParts.join(" ")}`;
        if (filesMovedToTrash) showTrashToast(message, "warn");
        else window._showToast?.(message, "warn");
      } else if (moveFilesToTrash) {
        if (filesMovedToTrash) {
          showTrashToast(
            "Channel removed — downloaded files moved to YTArchiver Trash.",
            "ok");
        } else if (res?.delete_error) {
          window._showToast?.(
            `Channel removed, but its downloaded files were left in place: ` +
            `${res.delete_error}`,
            "warn");
        } else {
          window._showToast?.(
            "Channel removed (no downloaded folder was found).", "ok");
        }
      } else if (res?.can_undo) {
        window._showToast?.({
          msg: `Removed "${cleanName}" from subscriptions. Files were kept.` +
            (keepFilesUndoRefreshWarning
              ? " Reopen this tab to refresh the list." : ""),
          kind: "warn",
          ttlMs: 10_000,
          action: {
            label: "Undo",
            onClick: () => undoKeptFilesRemoval(cleanName, res?.undo_id),
          },
        });
      } else {
        window._showToast?.(
          "Channel removed from subscriptions. Downloaded files were kept.",
          "ok");
      }

      return res;
    } catch (error) {
      window._showToast?.("Remove failed: " + error, "error");
      return { ok: false, error: String(error) };
    } finally {
      progressToast?.dismiss?.(true);
      removalInFlight = false;
    }
  };
})();
