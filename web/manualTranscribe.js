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
  function nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
  }

  // ─── Manual Transcribe dialog ────────────────────────────────────────
  function initManualTranscribe() {
    if (window._manualTxInited) return;
    window._manualTxInited = true;
    const backdrop = document.getElementById("manual-tx-backdrop");
    const pathEl = document.getElementById("manual-tx-path");
    const modelEl = document.getElementById("manual-tx-model");
    const openBtn = document.getElementById("btn-manual-transcribe");
    const browseBtn = document.getElementById("manual-tx-browse");
    const cancelBtn = document.getElementById("manual-tx-cancel");
    const confirmBtn = document.getElementById("manual-tx-confirm");
    if (!backdrop) return;

    const show = () => { backdrop.hidden = false; };
    const hide = () => { backdrop.hidden = true; if (pathEl) pathEl.value = ""; };

    openBtn?.addEventListener("click", show);
    cancelBtn?.addEventListener("click", hide);
    backdrop.addEventListener("click", (e) => { if (e.target === backdrop) hide(); });
    window.YT?.modals?.registerEscapeClose?.(backdrop, hide);

    // "Transcribe folder..." — recursively queue every untranscribed video
    // under a picked folder. Native folder picker handles the prompt.
    document.getElementById("btn-transcribe-folder")?.addEventListener("click", async () => {
      if (!nativeBridgeUp()) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      // Manual → ask Whisper model (60s countdown auto-picks Settings default).
      const model = await (window._askWhisperModel?.("this folder"));
      if (model === null) return;
      const res = await bridgeCall("transcribe_folder", model);
      if (res?.ok) {
        window._showToast?.("Walking folder \u2014 watch the log for queue counts.", "ok");
      } else if (!res?.cancelled) {
        window._showToast?.(res?.error || "Folder transcribe failed.", "error");
      }
    });

    browseBtn?.addEventListener("click", async () => {
      if (!nativeBridgeUp()) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      const res = await bridgeCall("pick_file", "Pick a video to transcribe", null,
                                       [("Video files (*.mp4;*.mkv;*.webm;*.mov)")]);
      if (res?.ok && res.path) {
        pathEl.value = res.path;
      }
    });

    confirmBtn?.addEventListener("click", async () => {
      const path = pathEl?.value || "";
      if (!path) { window._showToast?.("Pick a video file first.", "warn"); return; }
      if (!nativeBridgeUp()) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      // Reject non-media files BEFORE the API call — Whisper would
      // otherwise waste minutes failing on a JSON / text file
      // (audit: manualTranscribe.js H238).
      if (!/\.(mp4|mkv|webm|mov|m4a|mp3|wav|flac|m4v|avi|wmv)$/i.test(path)) {
        window._showToast?.(
          "Pick a media file (.mp4/.mkv/.webm/etc).", "warn");
        return;
      }
      const title = path.split(/[\\/]/).pop().replace(/\.[^.]+$/, "");
      // Manual → ask Whisper model (60s countdown auto-picks Settings default).
      const model = await (window._askWhisperModel?.(`"${title}"`));
      if (model === null) return;
      const res = await bridgeCall("transcribe_enqueue", path, title, model);
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
