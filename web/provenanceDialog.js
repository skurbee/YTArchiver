/* ═══════════════════════════════════════════════════════════════════════
   provenanceDialog.js — "Embed file tags" modal (v80)

   Same shape as the Restore-punctuation dialog. Channel dropdown (or
   All channels), phase checkboxes (Transcript.txt headers / MP4 tags),
   dry-run toggle. On Run, calls api.provenance_embed and the task
   queues on the sync queue for pause/resume/cancel; progress shows in
   the main activity log.

   Publishes:
     window.initProvenanceDialog
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

  function initProvenanceDialog() {
      const btn = document.getElementById("btn-provenance");
      const bd = document.getElementById("provenance-backdrop");
      const chanSel = document.getElementById("provenance-channel");
      const txtEl = document.getElementById("provenance-do-txt");
      const mp4El = document.getElementById("provenance-do-mp4");
      const dryEl = document.getElementById("provenance-dryrun");
      const runBtn = document.getElementById("provenance-run");
      const closeBtn = document.getElementById("provenance-close");
      if (!btn || !bd || !chanSel || !runBtn) return;

      const _loadChannels = async () => {
        try {
          const channels = await window.YT?.util?.loadSubsChannels?.() || [];
          chanSel.innerHTML = "";
          const allOpt = document.createElement("option");
          allOpt.value = "";
          allOpt.textContent = "All channels";
          chanSel.appendChild(allOpt);
          for (const ch of channels) {
            const opt = document.createElement("option");
            opt.value = ch.folder || ch.name || "";
            opt.textContent = ch.displayName || ch.folder || ch.name
              || "(channel name missing)";
            chanSel.appendChild(opt);
          }
        } catch (e) {
          console.warn("provenance: failed to load channels", e);
          window._showToast?.(`Could not load channels: ${e}`, "warn");
        }
      };

      btn.addEventListener("click", async () => {
        await _loadChannels();
        if (txtEl) txtEl.checked = true;
        if (mp4El) mp4El.checked = true;
        if (dryEl) dryEl.checked = false;
        bd.hidden = false;
      });

      const _close = () => { bd.hidden = true; };
      closeBtn?.addEventListener("click", _close);
      bd.addEventListener("click", (e) => {
        if (e.target === bd) _close();
      });
      window.YT?.modals?.registerEscapeClose?.(bd, _close);

      runBtn.addEventListener("click", async () => {
        if (!nativeBridgeUp()) {
          window._showToast?.(
            "YTArchiver is still starting. Try again in a moment.", "warn");
          return;
        }
        const payload = {
          channel: chanSel.value || "",
          do_txt: !!txtEl?.checked,
          do_mp4: !!mp4El?.checked,
          dry_run: !!dryEl?.checked,
        };
        if (!payload.do_txt && !payload.do_mp4) {
          window._showToast?.("Choose at least one item to update.", "warn");
          return;
        }
        // Confirm an all-channels live MP4 run — it rewrites every
        // known-ID video file once (stream copy, hours of disk I/O on
        // a large archive). Resumable, but the user should opt in
        // knowingly. Header-only runs are quick and skip the prompt.
        if (!payload.channel && !payload.dry_run && payload.do_mp4) {
          const _ok = await window.askDanger(
            "Embed file details for all channels?",
            "This will update every video with a known YouTube ID and may "
            + "take hours on a large archive. Video quality and file dates "
            + "will not change. You can pause, cancel, or resume it from "
            + "Sync Tasks.",
            "Run on all channels");
          if (!_ok) return;
        }
        try {
          const res = await bridgeCall("provenance_embed", payload);
          if (res?.ok && res.queued) {
            const msg = res.started
              ? "File details queued — running now. Watch the main log."
              : "File details queued — will run when the sync queue resumes.";
            window._showToast?.(msg, "ok");
            _close();
          } else if (res?.ok && !res.queued) {
            window._showToast?.(
              res?.reason || "This file-details task is already queued.",
              "warn");
          } else {
            window._showToast?.(res?.error || "File details failed to start.", "warn");
          }
        } catch (e) {
          window._showToast?.(`File details could not be updated: ${e}`, "warn");
        }
      });
  }

  window.initProvenanceDialog = initProvenanceDialog;
})();
