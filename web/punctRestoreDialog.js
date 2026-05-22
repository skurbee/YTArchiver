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
          const api = window.pywebview?.api;
          const list = await api?.get_subs_channels?.();
          const channels = Array.isArray(list) ? list : (list?.channels || []);
          chanSel.innerHTML = "";
          const allOpt = document.createElement("option");
          allOpt.value = "";
          allOpt.textContent = "All channels";
          chanSel.appendChild(allOpt);
          const sorted = [...channels].sort((a, b) =>
            (a.name || "").toLowerCase().localeCompare(
              (b.name || "").toLowerCase()));
          for (const ch of sorted) {
            const opt = document.createElement("option");
            opt.value = ch.folder || ch.name || "";
            opt.textContent = ch.name || ch.folder
              || "(channel name missing)";
            chanSel.appendChild(opt);
          }
        } catch (e) {
          console.warn("punct-restore: failed to load channels", e);
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
        if (!api?.punct_restore_segments) {
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
        if (!payload.channel && !payload.dry_run) {
          const _ok = window.confirm(
            "Punctuation restore — ALL channels with dry-run OFF.\n\n" +
            "This will rewrite every segment in every transcript and " +
            "can take hours.\n\nProceed?");
          if (!_ok) return;
        }
        try {
          const res = await api.punct_restore_segments(payload);
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
