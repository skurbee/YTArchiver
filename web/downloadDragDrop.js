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
  function nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
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
      // Reject file:// drags entirely (audit: downloadDragDrop.js H239).
      // Use the parsed hostname validator shared with the typed field so a
      // foreign URL cannot smuggle "youtube.com" into its path or user-info.
      if (/^file:/i.test(trimmed)) {
        window._showToast?.("Drop a YouTube URL, not a file.", "warn");
        return;
      }
      const parsed = typeof window._parseYouTubeUrl === "function"
        ? window._parseYouTubeUrl(trimmed) : null;
      if (!parsed) {
        window._showToast?.("Drop a YouTube URL to archive.", "warn");
        return;
      }
      const looksLikeVideo = typeof window._urlLooksLikeVideo === "function"
        ? window._urlLooksLikeVideo(trimmed)
        : false;
      if (!looksLikeVideo) {
        window._showToast?.("Drop a single YouTube video URL here. Add channels from Browse.", "warn");
        return;
      }
      const input = document.querySelector("#panel-download .ctl-input");
      if (input) {
        input.value = trimmed;
        input.dispatchEvent(new Event("input", { bubbles: true }));
      }
      if (nativeBridgeUp()) {
        // Already-archived warning — same gate as the URL-submit flow, so a
        // dropped URL for a video already in the archive prompts before
        // re-downloading. Any failure falls through and allows the download.
        try {
          if (askConfirm) {
            const chk = await bridgeCall("single_video_archived", trimmed);
            if (chk?.ok && chk.archived) {
              const what = chk.title ? `"${chk.title}"` : "This video";
              const where = chk.channel ? ` in "${chk.channel}"` : "";
              const go = await askConfirm(
                "Already archived",
                `${what} is already archived${where}.\n\n` +
                `Download it again anyway?`,
                { confirm: "Download anyway" });
              if (!go) return;
            }
          }
        } catch { /* non-fatal — allow the download */ }
        // pass the same readVideoOptions() dict the URL-
        // submit flow does, so dropped URLs honor the user's
        // resolution / save-to / custom-name fields instead of
        // silently using backend defaults. Consume the cross-IIFE
        // helper from downloadUrl.js — the bareword `readVideoOptions`
        // is scoped to that file's IIFE and not visible here.
        const opts = (typeof window._readVideoOptions === "function")
            ? window._readVideoOptions() : {};
        const queue = typeof window._queueSingleVideo === "function"
          ? window._queueSingleVideo
          : async (url, options) => {
              try {
                const result = await bridgeCall("archive_single_video", url, options);
                return result?.ok ? result : {
                  ok: false,
                  error: result?.error || "The download could not be queued.",
                };
              } catch (err) {
                return { ok: false, error: err?.message || String(err) };
              }
            };
        const result = await queue(trimmed, opts);
        if (!result?.ok) {
          window._showToast?.(
            result?.error || "The download could not be queued.", "error");
          return; // URL stays in the field for correction/retry.
        }
        window._showToast?.("Queued: " + trimmed.slice(0, 60), "ok");
        // Match the typed-URL flow: a successfully queued URL is consumed,
        // and dispatching input also hides the options panel/button.
        if (input && input.value.trim() === trimmed) {
          input.value = "";
          input.dispatchEvent(new Event("input", { bubbles: true }));
        }
      } else {
        window._showToast?.("YTArchiver isn't ready yet. The URL is still here so you can try again.", "warn");
      }
    });
  }

  window.initDragDropUrl = initDragDropUrl;
})();
