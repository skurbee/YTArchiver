/* ═══════════════════════════════════════════════════════════════════════
   removeChannel.js — shared "remove channel" two-step flow

   Every call site that removes a channel — Edit-panel Remove button,
   Delete-key on a Subs row, channel-card right-click "Remove channel",
   bulk-delete from the Subs context menu — funnels through this helper
   so the user gets the same two-step confirmation:

     1. "Remove channel from your subscriptions?" (danger)
     2. "Also delete the on-disk folder?" (choice: Delete files / Keep)

   Returns {ok, deleted_folder, error?} or null if user cancelled.

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

  window._removeChannelWithPrompt = async function (name) {
    if (!nativeBridgeUp()) {
      window._showToast?.("Native mode required.", "warn");
      return null;
    }
    const ok1 = await window.askDanger(
      "Remove channel",
      `Remove channel "${name}" from your subscriptions?`,
      "Remove");
    if (!ok1) return null;
    // Step 2: optionally delete the on-disk folder too.
    const wantDelete = await window.askChoice({
      title: "Delete downloaded files?",
      message: `Also delete "${name}"'s downloaded videos, transcripts, ` +
               "metadata, and thumbnails from disk? This cannot be undone.",
      choices: [
        { label: "Delete files", value: "yes", kind: "danger" },
        { label: "Keep files", value: "no", kind: "primary" },
      ],
      cancel: "Back",
    });
    if (wantDelete === null) return null; // user cancelled
    const deleteFiles = wantDelete === "yes";
    let res;
    try {
      res = await bridgeCall("subs_remove_channel", { name }, deleteFiles);
    } catch (e) {
      window._showToast?.("Remove failed: " + e, "error");
      return { ok: false, error: String(e) };
    }
    if (!res?.ok) {
      window._showToast?.(res?.error || "Remove failed.", "error");
      return res;
    }
    if (deleteFiles) {
      if (res.deleted_folder) {
        window._showToast?.(`Channel removed — files deleted from disk.`, "ok");
      } else if (res.delete_error) {
        window._showToast?.(
          `Channel removed, but file delete failed: ${res.delete_error}`,
          "warn");
      } else {
        window._showToast?.("Channel removed (no files found to delete).", "ok");
      }
    } else {
      window._showToast?.("Channel removed (files kept on disk).", "ok");
    }
    return res;
  };
})();
