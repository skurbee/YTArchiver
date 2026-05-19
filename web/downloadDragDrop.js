/**
 * web/downloadDragDrop.js — drag a YouTube URL onto the Download tab
 *
 * Exposed as window.initDragDropUrl; app.js boot calls it once.
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

  // ─── Drag-and-drop URL on Download tab ───────────────────────────────
  function initDragDropUrl() {
    const panel = document.getElementById("panel-download");
    if (!panel) return;
    // drag-counter pattern so hover state doesn't flicker
    // or get stuck. dragleave fires on EVERY child element during the
    // drag — a bare add/remove on dragover/dragleave races itself and
    // leaves the hover class hanging when the user drops mid-hover.
    // Increment on dragenter, decrement on dragleave, only remove at 0.
    let _dragDepth = 0;
    panel.addEventListener("dragenter", (e) => {
      e.preventDefault();
      _dragDepth++;
      panel.classList.add("drag-hover");
    });
    panel.addEventListener("dragover", (e) => {
      e.preventDefault();
      panel.classList.add("drag-hover");
    });
    panel.addEventListener("dragleave", () => {
      _dragDepth = Math.max(0, _dragDepth - 1);
      if (_dragDepth === 0) panel.classList.remove("drag-hover");
    });
    panel.addEventListener("drop", async (e) => {
      e.preventDefault();
      _dragDepth = 0;
      panel.classList.remove("drag-hover");
      // Prefer URL (from address bar drag); fall back to text
      const url = e.dataTransfer.getData("text/uri-list") ||
                  e.dataTransfer.getData("text/plain");
      if (!url) return;
      const trimmed = url.trim();
      if (!/youtube|youtu\.be/i.test(trimmed)) {
        window._showToast?.("Drop a YouTube URL to archive.", "warn");
        return;
      }
      const input = document.querySelector("#panel-download .ctl-input");
      if (input) input.value = trimmed;
      if (window.pywebview?.api?.archive_single_video) {
        // pass the same readVideoOptions() dict the URL-
        // submit flow does, so dropped URLs honor the user's
        // resolution / save-to / custom-name fields instead of
        // silently using backend defaults.
        const opts = (typeof readVideoOptions === "function")
            ? readVideoOptions() : {};
        await window.pywebview.api.archive_single_video(trimmed, opts);
        window._showToast?.("Queued: " + trimmed.slice(0, 60), "ok");
      }
    });
  }

  window.initDragDropUrl = initDragDropUrl;
})();
