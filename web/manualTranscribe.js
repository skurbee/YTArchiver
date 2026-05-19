/**
 * web/manualTranscribe.js — Manual Transcribe — pick a file + queue with Whisper
 *
 * Exposed as window.initManualTranscribe; app.js boot calls it once.
 */
(function () {
  "use strict";

  const _browseState = window._browseState || {};
  const showContextMenu = window.showContextMenu || (() => {});
  const askConfirm = window.askConfirm;
  const askDanger = window.askDanger;
  const askQuestion = window.askQuestion;
  const askChoice = window.askChoice;
  const askTextInput = window.askTextInput;
  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }

  // ─── Manual Transcribe dialog ────────────────────────────────────────
  function initManualTranscribe() {
    const backdrop = document.getElementById("manual-tx-backdrop");
    const pathEl = document.getElementById("manual-tx-path");
    const modelEl = document.getElementById("manual-tx-model");
    const openBtn = document.getElementById("btn-manual-transcribe");
    const browseBtn = document.getElementById("manual-tx-browse");
    const cancelBtn = document.getElementById("manual-tx-cancel");
    const confirmBtn = document.getElementById("manual-tx-confirm");
    if (!backdrop) return;

    const show = () => { backdrop.style.display = "flex"; };
    const hide = () => { backdrop.style.display = "none"; if (pathEl) pathEl.value = ""; };

    openBtn?.addEventListener("click", show);
    cancelBtn?.addEventListener("click", hide);
    backdrop.addEventListener("click", (e) => { if (e.target === backdrop) hide(); });
    // BUG FIX 2026-05-15 (audit): consistent Esc-to-close across modals.
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && backdrop.style.display !== "none") hide();
    });

    // "Transcribe folder..." — recursively queue every untranscribed video
    // under a picked folder. Native folder picker handles the prompt.
    document.getElementById("btn-transcribe-folder")?.addEventListener("click", async () => {
      const api = window.pywebview?.api;
      if (!api?.transcribe_folder) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      // Manual → ask Whisper model (60s countdown auto-picks Settings default).
      const model = await (window._askWhisperModel?.("this folder"));
      if (model === null) return;
      const res = await api.transcribe_folder();
      if (res?.ok) {
        window._showToast?.("Walking folder \u2014 watch the log for queue counts.", "ok");
      } else if (!res?.cancelled) {
        window._showToast?.(res?.error || "Folder transcribe failed.", "error");
      }
    });

    browseBtn?.addEventListener("click", async () => {
      const api = window.pywebview?.api;
      if (!api?.pick_file) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      const res = await api.pick_file("Pick a video to transcribe", null,
                                       [("Video files (*.mp4;*.mkv;*.webm;*.mov)")]);
      if (res?.ok && res.path) {
        pathEl.value = res.path;
      }
    });

    confirmBtn?.addEventListener("click", async () => {
      const api = window.pywebview?.api;
      const path = pathEl?.value || "";
      if (!path) { window._showToast?.("Pick a video file first.", "warn"); return; }
      if (!api?.transcribe_enqueue) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      const title = path.split(/[\\/]/).pop().replace(/\.[^.]+$/, "");
      // Manual → ask Whisper model (60s countdown auto-picks Settings default).
      const model = await (window._askWhisperModel?.(`"${title}"`));
      if (model === null) return;
      const res = await api.transcribe_enqueue(path, title);
      if (res?.ok) {
        window._showToast?.("Queued for transcription.", "ok");
        hide();
      } else {
        window._showToast?.("Queue failed.", "error");
      }
    });
  }

  window.initManualTranscribe = initManualTranscribe;
})();
