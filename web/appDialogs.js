/**
 * web/appDialogs.js — app-level dialogs invoked from the Python bridge.
 *
 * Three modal dialogs that Python triggers via `evaluate_js`:
 *   - window.askMetadataAlreadyDownloaded(channelName, count)
 *       Settings → Metadata flow: "channel already has metadata —
 *       check for new only, or refresh all counts?" Returns one of
 *       "append" / "overwrite" / "skip" (back-compat strings the
 *       Python sync pipeline reads).
 *   - window._showCloseDialog()
 *       Close-to-tray confirm. Triggered by main.py _on_closing when
 *       settings.close_behavior is "ask". Three buttons + remember
 *       checkbox. Routes to api.confirm_close.
 *   - window._askBookmarkKind()
 *       Bookmark add: timestamped vs. whole video. Returns "yes" /
 *       "no" / null (cancel).
 *
 * Depends on:
 *   - window.askChoice (modals.js)
 *   - window.pywebview.api.confirm_close (native bridge)
 */
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

  // Triggered by Python via evaluate_js — same three semantics as the
  // pre-rewrite tkinter dialog. "new" → Check for New (only fetch IDs
  // not already on disk; fast). "refresh" → Refresh Counts (re-hit
  // every video to update view counts; slow). "cancel" → do nothing.
  // The Python sync pipeline accepts "skip" / "overwrite" / "append"
  // for back-compat, so we map the new short labels to those.
  window.askMetadataAlreadyDownloaded = async function (channelName, count) {
    const choice = await window.askChoice({
      title: "Metadata Already Downloaded",
      message: `"${channelName}" already has metadata for ${count} video(s) on disk.\n\n` +
               `Check for New: only fetch IDs we haven't seen yet (fast).\n` +
               `Refresh Counts: re-hit every existing video to update view counts (slow).`,
      buttons: [
        { label: "Check for New", value: "new", kind: "primary" },
        { label: "Refresh Counts", value: "refresh", kind: "ghost" },
      ],
    });
    if (choice === "new") return "append";
    if (choice === "refresh") return "overwrite";
    return "skip";
  };

  // Close-to-tray confirm. Triggered by main.py's _on_closing handler
  // when settings.close_behavior is "ask". Three-button layout:
  // [Cancel] [Close to tray] [Quit] + a remember-choice checkbox.
  // Cancel sits on the left and receives initial focus.  Closing the whole
  // app is destructive while work may be running, so Enter must keep the
  // safe choice unless the user deliberately selects another button.
  window._showCloseDialog = function () {
    if (document.getElementById("close-confirm-modal")) return;
    // Do not hide, restyle, or resolve some other dialog just because the
    // window X was clicked.  Static dialogs live in the DOM while hidden;
    // the old class-only scan wrote `display:none` onto all of them and their
    // later openers could not make them visible again.  If a real dialog is
    // open, leave it intact and cancel this close attempt.  The user can
    // finish that dialog and click X again.
    const isVisible = window.YT?.modals?.isVisible;
    const existing = Array.from(document.querySelectorAll(".askq-backdrop"))
      .find((el) => {
        if (el.id === "close-confirm-modal") return false;
        if (typeof isVisible === "function") return isVisible(el);
        return !el.hidden && el.style.display !== "none";
      });
    if (existing) {
      existing.querySelector?.(".askq-dialog, .yt-modal")?.focus?.();
      window._showToast?.("Finish or close the open dialog first.", "warn");
      if (nativeBridgeUp()) {
        Promise.resolve(bridgeCall("confirm_close", "cancel", false))
          .catch(() => {});
      }
      return;
    }
    const modal = window.YT?.modals?.open;
    if (!modal) return;
    let remember = null;
    modal({
      bodyHtml: `
      <div class="askq-dialog">
        <div class="askq-header">Close YTArchiver?</div>
        <div class="askq-body">Quit completely, or close to the system tray and keep syncing in the background?</div>
        <label class="askq-check-row">
          <input type="checkbox" id="close-remember-choice" />
          Remember my choice
        </label>
        <div class="askq-buttons askq-buttons-actions askq-buttons-inline">
          <button class="btn btn-ghost" data-act="cancel">Cancel</button>
          <button class="btn btn-ghost" data-act="tray">Close to tray</button>
          <button class="btn btn-danger" data-act="quit">Quit</button>
        </div>
      </div>
      `,
      escapeValue: "cancel",
      outsideClickValue: "cancel",
      initialFocus: '[data-act="cancel"]',
      onMount: (root, resolveOuter) => {
        root.id = "close-confirm-modal";
        remember = root.querySelector("#close-remember-choice");
        root.querySelector('[data-act="cancel"]')?.addEventListener(
          "click", () => resolveOuter("cancel"));
        root.querySelector('[data-act="tray"]')?.addEventListener(
          "click", () => resolveOuter("tray"));
        root.querySelector('[data-act="quit"]')?.addEventListener(
          "click", () => resolveOuter("quit"));
      },
    }).then(async (action) => {
      const rem = !!remember?.checked;
      // Cancel = pure dismiss. Window stays open, no config change
      // (we deliberately ignore the Remember box here — saving "Cancel"
      // as the default close behavior makes no sense). The backend
      // call below MUST still happen: confirm_close is what releases
      // the reentrant-X guard (_close_dialog_pending) — skipping it
      // left the X button dead for the rest of the session after any
      // Cancel/Esc/backdrop dismissal.
      if (action === "cancel") {
        try { if (nativeBridgeUp()) await bridgeCall("confirm_close", "cancel", false); } catch {}
        return;
      }
      try {
        if (nativeBridgeUp()) await bridgeCall("confirm_close", action, rem);
      } catch {}
    });
  };

  // Bookmark Yes/No timestamp dialog. Replaces the old note-prompt flow.
  // Resolves to "yes" (bookmark with timestamp), "no" (bookmark whole
  // video), or null (cancel).
  window._askBookmarkKind = function () {
    return window.YT?.modals?.choice({
      title: "Add bookmark",
      message: "Bookmark this exact moment, or the entire video?",
      buttons: [
        { label: "Whole video", value: "no", kind: "ghost" },
        { label: "At timestamp", value: "yes", kind: "primary" },
      ],
      cancel: "Cancel",
      cancelKind: "danger",
    });
  };
})();
