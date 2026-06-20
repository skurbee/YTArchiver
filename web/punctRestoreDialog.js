/* ═══════════════════════════════════════════════════════════════════════
   punctRestoreDialog.js — "Restore transcript punctuation" modal

   Extracted from settingsTab.js (Patch 24, v72.6). Same shape as the
   Repair YT captions dialog. Channel dropdown (or All channels),
   dry-run toggle. On Run, calls api.punct_restore_segments and the
   task queues on the sync queue for pause/resume/cancel.

   Publishes:
     window.initPunctRestoreDialog
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

  function initPunctRestoreDialog() {
      const btn = document.getElementById("btn-punct-restore");
      const bd = document.getElementById("punct-restore-backdrop");
      const chanSel = document.getElementById("punct-restore-channel");
      const dryEl = document.getElementById("punct-restore-dryrun");
      const runBtn = document.getElementById("punct-restore-run");
      const closeBtn = document.getElementById("punct-restore-close");
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
          console.warn("punct-restore: failed to load channels", e);
          window._showToast?.(`Could not load channels: ${e}`, "warn");
        }
      };

      btn.addEventListener("click", async () => {
        await _loadChannels();
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
          window._showToast?.("Punctuation restore API not available.", "warn");
          return;
        }
        const payload = {
          channel: chanSel.value || "",
          dry_run: !!dryEl?.checked,
        };
        // Confirm a destructive all-channels live run — without this
        // a single Run click could spend hours rewriting every
        // segment in every channel (audit: punctRestoreDialog H236).
        // Uses the app's styled askDanger modal so the confirmation
        // matches the rest of the UI (the raw `window.confirm` it
        // previously used rendered as a browser-chrome popup).
        if (!payload.channel && !payload.dry_run) {
          const _ok = await window.askDanger(
            "Punctuation restore — ALL channels",
            "Dry-run is OFF. This rewrites every segment in every "
            + "transcript and can take hours.\n\nProceed?",
            "Run on all channels");
          if (!_ok) return;
        }
        try {
          const res = await bridgeCall("punct_restore_segments", payload);
          if (res?.ok && res.queued) {
            const msg = res.started
              ? "Punctuation restore queued — running now. Watch the main log."
              : "Punctuation restore queued — will run when the sync queue resumes.";
            window._showToast?.(msg, "ok");
            _close();
          } else if (res?.ok && !res.queued) {
            window._showToast?.(
              res?.error || "A punctuation task with this scope is already queued.",
              "warn");
          } else {
            window._showToast?.(res?.error || "Punctuation restore failed to start.", "warn");
          }
        } catch (e) {
          window._showToast?.(`Punctuation restore error: ${e}`, "warn");
        }
      });
  }

  window.initPunctRestoreDialog = initPunctRestoreDialog;
})();
