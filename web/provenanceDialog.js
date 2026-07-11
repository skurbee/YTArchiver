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
          window._showToast?.("Embed file tags API not available.", "warn");
          return;
        }
        const payload = {
          channel: chanSel.value || "",
          do_txt: !!txtEl?.checked,
          do_mp4: !!mp4El?.checked,
          dry_run: !!dryEl?.checked,
        };
        if (!payload.do_txt && !payload.do_mp4) {
          window._showToast?.("Enable at least one phase.", "warn");
          return;
        }
        // Confirm an all-channels live MP4 run — it rewrites every
        // known-ID video file once (stream copy, hours of disk I/O on
        // a large archive). Resumable, but the user should opt in
        // knowingly. Header-only runs are quick and skip the prompt.
        if (!payload.channel && !payload.dry_run && payload.do_mp4) {
          const _ok = await window.askDanger(
            "Embed file tags — ALL channels",
            "This rewrites every known-ID video file once to embed its "
            + "tags (stream copy — no re-encode, dates preserved). On a "
            + "large archive this is hours of background disk work.\n\n"
            + "It runs as a Sync Task you can pause, cancel, and resume "
            + "any time — already-tagged files are skipped on re-runs."
            + "\n\nProceed?",
            "Run on all channels");
          if (!_ok) return;
        }
        try {
          const res = await bridgeCall("provenance_embed", payload);
          if (res?.ok && res.queued) {
            const msg = res.started
              ? "Embed file tags queued — running now. Watch the main log."
              : "Embed file tags queued — will run when the sync queue resumes.";
            window._showToast?.(msg, "ok");
            _close();
          } else if (res?.ok && !res.queued) {
            window._showToast?.(
              res?.reason || "An embed-tags task with this scope is already queued.",
              "warn");
          } else {
            window._showToast?.(res?.error || "Embed file tags failed to start.", "warn");
          }
        } catch (e) {
          window._showToast?.(`Embed file tags error: ${e}`, "warn");
        }
      });
  }

  window.initProvenanceDialog = initProvenanceDialog;
})();
