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
    // User hit this by clicking X while a Diagnostics dialog was open —
    // close dialog appeared ON TOP of Diagnostics, two modals stacked.
    try {
      document.querySelectorAll(".askq-backdrop").forEach((el) => {
        if (el.id && el.id !== "close-confirm-modal"
            && el.style.display !== "none") {
          el.style.display = "none";
        }
      });
    } catch (_e) { /* non-fatal */ }
    const backdrop = document.createElement("div");
    backdrop.className = "askq-backdrop";
    backdrop.id = "close-confirm-modal";
    backdrop.innerHTML = `
      <div class="askq-dialog">
        <div class="askq-header">Close YTArchiver?</div>
        <div class="askq-body">Quit completely, or close to the system tray and keep syncing in the background?</div>
        <label style="display:flex;align-items:center;gap:6px;margin:8px 0 12px;
                      font-size:12px;color:var(--c-dim);user-select:none;">
          <input type="checkbox" id="close-remember-choice" />
          Remember my choice
        </label>
        <div class="askq-buttons askq-buttons-actions askq-buttons-inline">
          <button class="btn btn-ghost" data-act="cancel">Cancel</button>
          <button class="btn btn-ghost" data-act="tray">Close to tray</button>
          <button class="btn btn-danger" data-act="quit">Quit</button>
        </div>
      </div>
    `;
    document.body.appendChild(backdrop);
    const remember = backdrop.querySelector("#close-remember-choice");
    const choose = async (action) => {
      const rem = !!remember?.checked;
      backdrop.remove();
      // Cancel = pure dismiss. Window stays open, no config change
      // (we deliberately ignore the Remember box here — saving "Cancel"
      // as the default close behavior makes no sense).
      if (action === "cancel") return;
      try {
        await window.pywebview?.api?.confirm_close?.(action, rem);
      } catch {}
    };
    backdrop.querySelector('[data-act="cancel"]').addEventListener("click", () => choose("cancel"));
    backdrop.querySelector('[data-act="tray"]').addEventListener("click", () => choose("tray"));
    backdrop.querySelector('[data-act="quit"]').addEventListener("click", () => choose("quit"));
    backdrop.addEventListener("click", (e) => {
      if (e.target === backdrop) backdrop.remove();
    });
    document.addEventListener("keydown", function onKey(e) {
      if (e.key === "Escape") {
        document.removeEventListener("keydown", onKey);
        backdrop.remove();
      }
    });
    setTimeout(() => backdrop.querySelector('[data-act="quit"]')?.focus(), 30);
  };

  // Bookmark Yes/No timestamp dialog. Replaces the old note-prompt flow.
  // Resolves to "yes" (bookmark with timestamp), "no" (bookmark whole
  // video), or null (cancel).
  window._askBookmarkKind = function () {
    return new Promise((resolve) => {
      const backdrop = document.createElement("div");
      backdrop.className = "askq-backdrop";
      backdrop.innerHTML = `
        <div class="askq-dialog">
          <div class="askq-header">Add bookmark</div>
          <div class="askq-body">Bookmark this exact moment, or the
            entire video?</div>
          <div class="askq-buttons askq-buttons-actions">
            <button class="btn btn-ghost" data-act="no">Whole video</button>
            <button class="btn btn-primary" data-act="yes">At timestamp</button>
          </div>
          <div class="askq-buttons askq-buttons-cancel">
            <button class="btn btn-danger" data-act="cancel">Cancel</button>
          </div>
        </div>
      `;
      document.body.appendChild(backdrop);
      const close = (val) => { backdrop.remove(); resolve(val); };
      backdrop.querySelector('[data-act="yes"]').addEventListener("click", () => close("yes"));
      backdrop.querySelector('[data-act="no"]').addEventListener("click", () => close("no"));
      backdrop.querySelector('[data-act="cancel"]').addEventListener("click", () => close(null));
      backdrop.addEventListener("click", (e) => { if (e.target === backdrop) close(null); });
      document.addEventListener("keydown", function onKey(e) {
        if (e.key === "Escape") {
          document.removeEventListener("keydown", onKey);
          close(null);
        }
      });
      setTimeout(() => backdrop.querySelector('[data-act="yes"]')?.focus(), 30);
    });
  };
})();
