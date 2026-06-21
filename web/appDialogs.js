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
  // Cancel sits on the left (ghost, muted) so it reads as "back out,
  // don't close yet". Quit is the primary (Enter-focused) action —
  // that's what the X-button most commonly means.
  window._showCloseDialog = function () {
    if (document.getElementById("close-confirm-modal")) return;
    // Dismiss any already-open modal before showing the close confirm.
    // Fire a synthetic Escape keydown FIRST so each modal's
    // Esc-handler runs and resolves its promise — display:none
    // alone left the await chains hanging forever (audit:
    // appDialogs H229).
    try {
      document.querySelectorAll(".askq-backdrop").forEach((el) => {
        if (el.id && el.id !== "close-confirm-modal"
            && el.style.display !== "none") {
          try {
            el.dispatchEvent(new KeyboardEvent("keydown", {
              key: "Escape", bubbles: true, cancelable: true,
            }));
          } catch {}
          // Belt-and-suspenders: hide if still visible after Esc.
          if (el.style.display !== "none") el.style.display = "none";
        }
      });
    } catch (_e) { /* non-fatal */ }
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
      onMount: (root, resolveOuter) => {
        root.id = "close-confirm-modal";
        remember = root.querySelector("#close-remember-choice");
        root.querySelector('[data-act="cancel"]')?.addEventListener(
          "click", () => resolveOuter("cancel"));
        root.querySelector('[data-act="tray"]')?.addEventListener(
          "click", () => resolveOuter("tray"));
        root.querySelector('[data-act="quit"]')?.addEventListener(
          "click", () => resolveOuter("quit"));
        setTimeout(() => root.querySelector('[data-act="quit"]')?.focus(), 30);
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
