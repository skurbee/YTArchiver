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
          const api = window.pywebview?.api;
          const list = await api?.get_subs_channels?.();
          const channels = Array.isArray(list) ? list : (list?.channels || []);
          chanSel.innerHTML = "";
          // "All channels" is the default — full-archive fix is the
          // most common case for a one-shot repair.
          const allOpt = document.createElement("option");
          allOpt.value = "";
          allOpt.textContent = "All channels";
          chanSel.appendChild(allOpt);
          const sorted = [...channels].sort((a, b) =>
            (a.name || "").toLowerCase().localeCompare(
              (b.name || "").toLowerCase()));
          for (const ch of sorted) {
            const opt = document.createElement("option");
            // Scope uses the on-disk folder name (matches backend's
            // `output_dir / channel_folder` Path.join). Fall back to
            // name if folder isn't set.
            opt.value = ch.folder || ch.name || "";
            opt.textContent = ch.name || ch.folder
              || "(channel name missing)";
            chanSel.appendChild(opt);
          }
        } catch (e) {
          console.warn("repair-yt: failed to load channels", e);
        }
      };

      btn.addEventListener("click", async () => {
        await _loadChannels();
        if (dryEl) dryEl.checked = false;
        bd.style.display = "flex";
      });

      const _close = () => { bd.style.display = "none"; };
      closeBtn?.addEventListener("click", _close);
      bd.addEventListener("click", (e) => {
        if (e.target === bd) _close();
      });
      document.addEventListener("keydown", (e) => {
        if (e.key === "Escape" && bd.style.display !== "none") _close();
      });

      runBtn.addEventListener("click", async () => {
        const api = window.pywebview?.api;
        if (!api?.repair_yt_captions) {
          window._showToast?.("Repair API not available.", "warn");
          return;
        }
        const payload = {
          channel: chanSel.value || "",
          dry_run: !!dryEl?.checked,
        };
        try {
          const res = await api.repair_yt_captions(payload);
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
