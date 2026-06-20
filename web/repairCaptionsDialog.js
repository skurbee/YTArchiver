/* ═══════════════════════════════════════════════════════════════════════
   repairCaptionsDialog.js — "Repair YT auto-captions" modal

   Extracted from settingsTab.js (Patch 24, v72.6). Re-parses
   YouTube auto-caption files with the v64.7 parser fix (handles
   word-rollover punctuation lost in earlier ingests). Channel
   dropdown or All; dry-run + include-already-punctuated toggles.
   On Run, kicks off the background worker via api.repair_yt_captions
   and closes the modal — progress streams to the main log.

   Publishes:
     window.initRepairCaptionsDialog
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

  function initRepairCaptionsDialog() {
      const btn = document.getElementById("btn-repair-yt-captions");
      const bd = document.getElementById("repair-yt-backdrop");
      const chanSel = document.getElementById("repair-yt-channel");
      const dryEl = document.getElementById("repair-yt-dryrun");
      const runBtn = document.getElementById("repair-yt-run");
      const closeBtn = document.getElementById("repair-yt-close");
      if (!btn || !bd || !chanSel || !runBtn) return;

      const _loadChannels = async () => {
        try {
          const channels = await window.YT?.util?.loadSubsChannels?.() || [];
          chanSel.innerHTML = "";
          // "All channels" is the default — full-archive fix is the
          // most common case for a one-shot repair.
          const allOpt = document.createElement("option");
          allOpt.value = "";
          allOpt.textContent = "All channels";
          chanSel.appendChild(allOpt);
          for (const ch of channels) {
            const opt = document.createElement("option");
            // Scope uses the on-disk folder name (matches backend's
            // `output_dir / channel_folder` Path.join). Fall back to
            // name if folder isn't set.
            opt.value = ch.folder || ch.name || "";
            opt.textContent = ch.displayName || ch.folder || ch.name
              || "(channel name missing)";
            chanSel.appendChild(opt);
          }
        } catch (e) {
          console.warn("repair-yt: failed to load channels", e);
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
          window._showToast?.("Repair API not available.", "warn");
          return;
        }
        const payload = {
          channel: chanSel.value || "",
          dry_run: !!dryEl?.checked,
        };
        // Confirm a destructive all-channels live run (audit:
        // repairCaptionsDialog H236). Uses the app's styled askDanger
        // modal — the raw `window.confirm` it previously used rendered
        // as a browser-chrome "127.0.0.1 says…" popup that looked like
        // a regression.
        if (!payload.channel && !payload.dry_run) {
          const _ok = await window.askDanger(
            "Repair YT captions — ALL channels",
            "Dry-run is OFF. This rewrites every YT-captioned transcript "
            + "in every channel and can take hours.\n\nProceed?",
            "Run on all channels");
          if (!_ok) return;
        }
        try {
          const res = await bridgeCall("repair_yt_captions", payload);
          if (res?.ok && res.queued) {
            const msg = res.started
              ? "Repair queued — running now. Watch the main log."
              : "Repair queued — will run when the sync queue resumes.";
            window._showToast?.(msg, "ok");
            _close();
          } else if (res?.ok && !res.queued) {
            window._showToast?.(
              res?.error || "A repair task with this scope is already queued.",
              "warn");
          } else {
            window._showToast?.(res?.error || "Repair failed to start.", "warn");
          }
        } catch (e) {
          window._showToast?.(`Repair error: ${e}`, "warn");
        }
      });
  }

  window.initRepairCaptionsDialog = initRepairCaptionsDialog;
})();
