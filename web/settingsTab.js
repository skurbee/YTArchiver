/**
 * web/settingsTab.js — Settings tab init (config form, paths, yt-dlp
 * updater, backup export/import).
 *
 * Exposed as window.initSettingsTab; app.js boot calls it once.
 */
(function () {
  "use strict";

  const _browseState = window._browseState || {};
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

  // ─── Settings tab ───────────────────────────────────────────────────
  //
  // Settings is now a full tab (#panel-settings), not a modal. Switching
  // to the Settings tab re-loads the current config values so the tab is
  // always in sync with what's on disk.
  function initSettingsTab() {
    const panel = document.getElementById("panel-settings");
    const browseOut = document.getElementById("settings-browse-output");
    const browseVid = document.getElementById("settings-browse-video");
    const ytdlpBtn = document.getElementById("btn-ytdlp-update");
    const expBtn = document.getElementById("btn-export-channels");
    const impBtn = document.getElementById("btn-import-channels");
    const bkExpBtn = document.getElementById("btn-export-backup");
    const bkImpBtn = document.getElementById("btn-import-backup");
    if (!panel) return;

    // ─── Auto-save ──────────────────────────────────────────────────
    // No Save button: every editable field persists itself the moment it
    // changes. settings_save() is key-guarded server-side (each field only
    // writes when present in the payload) and serialized under a lock, so
    // sending a single {key: value} merges cleanly without disturbing the
    // others. The footer note flashes "Saved ✓" so the user gets the same
    // confirmation the Save toast used to give.
    let _flashTimer = null;
    function flashSaved(ok = true, msg) {
      const el = document.getElementById("settings-autosave-note");
      if (!el) return;
      if (_flashTimer) { clearTimeout(_flashTimer); _flashTimer = null; }
      el.textContent = msg || (ok ? "Saved ✓" : "Save failed");
      el.classList.toggle("is-saved", ok);
      el.classList.toggle("is-error", !ok);
      _flashTimer = setTimeout(() => {
        el.textContent = "Changes are saved automatically";
        el.classList.remove("is-saved", "is-error");
        _flashTimer = null;
      }, 1600);
    }
    async function saveField(key, value) {
      const api = window.pywebview?.api;
      if (!api?.settings_save) return;
      try {
        const res = await api.settings_save({ [key]: value });
        if (res?.ok) {
          flashSaved(true);
        } else {
          flashSaved(false);
          window._showToast?.(res?.error || "Save failed.", "error");
        }
      } catch (e) {
        flashSaved(false);
        window._showToast?.("Save failed: " + e, "error");
      }
    }
    // Last-persisted disk-staleness, so a blur on an invalid/blank value
    // can revert the field instead of saving garbage.
    let _diskLastGood = "24";

    // RAM estimate: each cached video row is ~1 KB of Python objects
    // (title + filepath strings + small numbers + dict overhead). We
    // rough-estimate by (channels) * (limit) * 1 KB and show it live
    // under the Preload limit field so the user can see the trade-off.
    const BYTES_PER_ROW = 1024;
    function _fmtMB(bytes) {
      if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} KB`;
      return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
    }
    async function _updatePreloadHints() {
      try {
        const api = window.pywebview?.api;
        let nCh = 0, nVids = 0;
        if (api?.get_index_summary) {
          const idx = await api.get_index_summary();
          nCh = idx?.cards?.channels ?? 0;
          nVids = idx?.cards?.videos ?? 0;
        }
        const allEl = document.getElementById("settings-preload-all");
        const hintB = document.getElementById("settings-preload-all-hint");
        if (!hintB) return;
        // Context-aware warning — show the user what the trade-off
        // actually is depending on which way the toggle is set, per
        // request: "On warning: longer load time on launch.
        // Off warning: longer load time on first browse tab load."
        if (allEl?.checked) {
          const est = nVids * BYTES_PER_ROW;
          hintB.innerHTML =
            `<span style="color:var(--c-log-sum);">\u26A0</span> ` +
            `All ${nVids.toLocaleString()} videos loaded at launch \u2014 ` +
            `startup takes longer (several seconds to a couple minutes on large archives). ` +
            `~${_fmtMB(est)} RAM.`;
        } else {
          hintB.innerHTML =
            `<span style="color:var(--c-log-sum);">\u26A0</span> ` +
            `Channels load on first click of each \u2014 brief "Loading\u2026" screen the ` +
            `first time you open a channel each session, then cached for the rest of the session.`;
        }
      } catch (_e) {}
    }

    async function load() {
      const api = window.pywebview?.api;
      if (!api?.settings_load) return;
      try {
        const s = await api.settings_load();
        document.getElementById("settings-output-dir").value = s.output_dir || "";
        document.getElementById("settings-video-dir").value = s.video_out_dir || "";
        document.getElementById("settings-whisper-model").value = s.whisper_model || "small";
        document.getElementById("settings-default-res").value = s.default_resolution || "720";
        document.getElementById("settings-log-mode").value = s.log_mode || "Simple";
        // Startup knobs
        const stEl = document.getElementById("settings-disk-staleness");
        if (stEl) { stEl.value = String(s.disk_scan_staleness_hours ?? 24); _diskLastGood = stEl.value; }
        const paEl = document.getElementById("settings-preload-all");
        if (paEl) paEl.checked = !!s.browse_preload_all;
        const avgEl = document.getElementById("settings-show-avg-size");
        if (avgEl) avgEl.checked = (s.show_avg_size !== false);
        // Apply current toggle to the Subs table right away so opening
        // Settings doesn't require a save to see the effect.
        window._applySubsAvgVisibility?.(s.show_avg_size !== false);
        // (Recent view List/Grid radios removed — Videos view is grid-only.)
        // X-button behavior — "ask" | "tray" | "quit". Default "ask"
        // so a user who never opens Settings still gets the modal.
        const cbEl = document.getElementById("settings-close-behavior");
        if (cbEl) {
          const cb = (s.close_behavior || "ask").toLowerCase();
          cbEl.value = ["ask","tray","quit"].includes(cb) ? cb : "ask";
        }
        // Auto-sync timing mode — read from the live scheduler state
        // (not the settings blob, since it's applied immediately).
        try {
          const amEl = document.getElementById("settings-autorun-mode");
          if (amEl && api.autorun_state) {
            const ast = await api.autorun_state();
            amEl.value = (ast && ast.mode === "clock") ? "clock" : "timer";
          }
        } catch (e) { /* ignore */ }
        // BUG FIX (2026-05-14): the custom `.yt-dd` widget mirrors a
        // hidden <select> via its own div trigger. When JS sets
        // sel.value programmatically there's no change event, so the
        // visible trigger label stays stuck at whatever was selected
        // at DOM-ready (the HTML default — `<option selected>`). User
        // saw "Ask each time" forever even with close_behavior=quit in
        // config. Fix: explicitly call each select's `_ytddRepaint`
        // after settings_load completes. Belt-and-suspenders: also
        // dispatch `change` so any listeners reacting to live selection
        // changes also fire (matches a user clicking the option).
        // Skip repaint if the Settings panel is hidden — repaint
        // reads layout sizes that are 0 / wrong while display:none,
        // so the cached widths would be junk until the next activate
        // event re-fires it anyway (audit: settingsTab H241).
        const _settingsPanel = document.getElementById("panel-settings");
        const _settingsHidden = !_settingsPanel
          || _settingsPanel.classList.contains("active") === false;
        if (!_settingsHidden) {
          document.querySelectorAll(".settings-view .ctl-select").forEach((sel) => {
            if (sel._ytddRepaint) sel._ytddRepaint();
          });
        }
        _updatePreloadHints();
        const vEl = document.getElementById("settings-ytdlp-version");
        if (vEl) vEl.textContent = "checking\u2026";
        try {
          const v = await api.ytdlp_version();
          if (vEl) vEl.textContent = v?.ok ? v.version : (v?.error || "not found");
        } catch { if (vEl) vEl.textContent = "check failed"; }
      } catch (e) { console.warn("settings load:", e); }
    }

    // ── Per-field auto-save wiring ──────────────────────────────────
    // Selects: persist the new value immediately on change.
    document.getElementById("settings-whisper-model")
      ?.addEventListener("change", (e) => saveField("whisper_model", e.target.value));
    document.getElementById("settings-default-res")
      ?.addEventListener("change", (e) => saveField("default_resolution", e.target.value));
    document.getElementById("settings-log-mode")
      ?.addEventListener("change", (e) => saveField("log_mode", e.target.value));
    document.getElementById("settings-close-behavior")
      ?.addEventListener("change", (e) => saveField("close_behavior", e.target.value));

    // Preload-all toggle: update the live hint AND persist.
    document.getElementById("settings-preload-all")
      ?.addEventListener("change", (e) => {
        _updatePreloadHints();
        saveField("browse_preload_all", !!e.target.checked);
      });

    // Avg-size toggle: live-apply to the Subs table AND persist.
    document.getElementById("settings-show-avg-size")
      ?.addEventListener("change", (e) => {
        window._applySubsAvgVisibility?.(!!e.target.checked);
        saveField("show_avg_size", !!e.target.checked);
      });

    // Disk-scan staleness: commit on blur (the `change` event) or Enter.
    // Validate first — a blank / negative / non-numeric value used to be
    // silently mapped to 0 ("always rescan"); now it's rejected and the
    // field reverts to the last persisted value so nothing bad is saved.
    const _diskEl = document.getElementById("settings-disk-staleness");
    _diskEl?.addEventListener("change", () => {
      const _v = _diskEl.value;
      const _n = parseInt(_v, 10);
      if (_v == null || _v === "" || !Number.isFinite(_n) || _n < 0) {
        window._showToast?.(
          "Disk-scan staleness must be a non-negative number.", "error");
        _diskEl.value = _diskLastGood;   // revert the bad input
        return;
      }
      _diskEl.value = String(_n);
      _diskLastGood = _diskEl.value;
      saveField("disk_scan_staleness_hours", _n);
    });
    _diskEl?.addEventListener("keydown", (e) => {
      if (e.key === "Enter") { e.preventDefault(); _diskEl.blur(); }
    });

    // Auto-sync timing mode — applied immediately via its own scheduler
    // call (not settings_save), so the countdown/clock label updates right
    // away. Flash the same "Saved" confirmation for consistency.
    document.getElementById("settings-autorun-mode")?.addEventListener("change", async (e) => {
      const _api = window.pywebview?.api;
      try {
        await _api?.autorun_set_mode?.(e.target.value);
        flashSaved(true);
      } catch (err) {
        flashSaved(false);
      }
    });

    // Reload fields whenever the user switches to the Settings tab.
    // initTabs wires clicks on .tab elements; we listen on the tab itself.
    const settingsTab = document.querySelector('.tab[data-tab="settings"]');
    settingsTab?.addEventListener("click", () => { setTimeout(load, 50); });
    // Also load once on boot so values are ready if the user switches fast.
    setTimeout(load, 200);

    // Bulk metadata buttons (formerly the hidden right-click on "All Channels"
    // in Browse — YTArchiver.py:24840). Both prompt for confirmation because
    // they queue N*K yt-dlp jobs.
    const metaQueueAll = document.getElementById("btn-metadata-queue-all");
    const metaRefresh = document.getElementById("btn-metadata-refresh-all");
    // Disable-during-await guard for both Queue and Refresh buttons.
    // Without this, an impatient double-click stacked two askConfirm
    // modals — the user could click through both, queuing the same
    // job twice (audit: settingsTab.js:170).
    metaQueueAll?.addEventListener("click", async () => {
      if (metaQueueAll.disabled) return;
      metaQueueAll.disabled = true;
      try {
        const api = window.pywebview?.api;
        if (!api?.metadata_queue_all) { window._showToast?.("Native mode required.", "warn"); return; }
        const ok = await askConfirm("Queue all metadata",
          "Queue a metadata download for EVERY subscribed channel?\n\n" +
          "For a large library this can run for hours and will hammer the yt-dlp " +
          "queue. You can cancel mid-pass from the Sync popover.",
          { confirm: "Queue all" });
        if (!ok) return;
        const res = await api.metadata_queue_all(false);
        if (res?.ok) window._showToast?.(`Queued ${res.channels} channel(s).`, "ok");
        else window._showToast?.(res?.error || "Queue failed.", "error");
      } finally {
        metaQueueAll.disabled = false;
      }
    });
    metaRefresh?.addEventListener("click", async () => {
      if (metaRefresh.disabled) return;
      metaRefresh.disabled = true;
      try {
        const api = window.pywebview?.api;
        if (!api?.metadata_queue_all) { window._showToast?.("Native mode required.", "warn"); return; }
        const ok = await askConfirm("Refresh views/likes",
          "Re-fetch view counts and like counts for every video on every channel?\n\n" +
          "Every on-disk video gets re-hit — previously-skipped failures too " +
          "(the failure flag is cleared first). Expect one yt-dlp call per video, " +
          "so this is slow on a 100k-video archive.",
          { confirm: "Refresh" });
        if (!ok) return;
        const res = await api.metadata_queue_all(true);
        if (res?.ok) window._showToast?.(`Queued refresh for ${res.channels} channel(s).`, "ok");
        else window._showToast?.(res?.error || "Refresh failed.", "error");
      } finally {
        metaRefresh.disabled = false;
      }
    });

    // "Realign misplaced thumbnails" — survey + (optionally) move
    // thumbs that ended up in a different year folder than the mp4
    // they belong to. Always shows a dry-run preview first; the
    // actual move only runs after explicit confirmation.
    (function wireThumbRealign() {
      const btn = document.getElementById("btn-thumb-realign");
      if (!btn || btn._wired) return;
      btn._wired = true;
      const orig = btn.textContent;
      let activeToken = null;
      let stopMode = false;   // when true, clicking the button cancels

      const setStop = (label) => {
        stopMode = true; btn.disabled = false; btn.textContent = label;
      };
      const reset = () => {
        stopMode = false; activeToken = null;
        btn.disabled = false; btn.textContent = orig;
      };
      // Poll a token until the pass finishes; resolves with the final
      // payload (progress streams to the Download log meanwhile).
      const pollUntilDone = async (api, token) => {
        const deadline = Date.now() + 60 * 60 * 1000;  // 1h safety
        while (Date.now() < deadline) {
          await new Promise(r => setTimeout(r, 600));
          let p;
          try { p = await api.realign_poll(token); }
          catch (e) { return { ok: false, error: String(e) }; }
          if (p && !p.pending) return p;
        }
        return { ok: false, error: "Timed out." };
      };

      btn.addEventListener("click", async () => {
        const api = window.pywebview?.api;
        if (!api?.realign_start) {
          window._showToast?.("Native mode required.", "warn"); return;
        }
        // Mid-pass: the button acts as a Stop control.
        if (stopMode && activeToken) {
          try { await api.realign_cancel(activeToken); } catch {}
          btn.disabled = true; btn.textContent = "Stopping…";
          return;
        }
        // ── Survey (dry run) ──
        let start;
        try { start = await api.realign_start(true); }
        catch (e) { window._showToast?.(String(e), "error"); return; }
        if (!start?.ok || !start.token) {
          window._showToast?.(start?.error || "Couldn't start scan.", "error");
          return;
        }
        activeToken = start.token;
        setStop("⏹ Stop scan");
        window._showToast?.(
          "Scanning thumbnails — progress in the Download log…", "ok");
        const preview = await pollUntilDone(api, activeToken);
        reset();
        if (!preview?.ok) {
          window._showToast?.(preview?.error || "Scan failed.", "error"); return;
        }
        if (preview.cancelled) {
          window._showToast?.("Thumbnail scan stopped.", "warn"); return;
        }
        const n = preview.misaligned || 0;
        const dups = preview.skipped_dest_exists || 0;
        const chans = Object.keys(preview.per_channel || {}).length;
        if (n === 0) {
          window._showToast?.(
            `All thumbnails aligned (${(preview.aligned||0).toLocaleString()} checked). Nothing to do.`,
            "ok");
          return;
        }
        const top = Object.entries(preview.per_channel || {})
          .sort((a,b) => (b[1].misaligned||0) - (a[1].misaligned||0))
          .slice(0, 8)
          .map(([name, d]) => `  • ${name}: ${d.misaligned}`).join("\n");
        const msg = `Found ${n.toLocaleString()} misplaced thumbnail(s) across ${chans} channel(s).\n\n`
          + `Top offenders:\n${top}\n\n`
          + `Each thumbnail will be moved next to its mp4 via same-volume rename. `
          + (dups > 0
              ? `${dups} thumbnail(s) have a duplicate already at the target folder — those will be SKIPPED (left in place, the existing one wins).\n\n`
              : "")
          + `Proceed?`;
        const go = await window.askChoice({
          title: "Realign misplaced thumbnails",
          message: msg,
          choices: [{ label: `Move ${n.toLocaleString()} thumbnail(s)`, value: "go", kind: "primary" }],
        });
        if (!go) return;
        // ── Move ──
        let mv;
        try { mv = await api.realign_start(false); }
        catch (e) { window._showToast?.(String(e), "error"); return; }
        if (!mv?.ok || !mv.token) {
          window._showToast?.(mv?.error || "Couldn't start move.", "error"); return;
        }
        activeToken = mv.token;
        setStop("⏹ Stop move");
        window._showToast?.(
          "Moving thumbnails — progress in the Download log…", "ok");
        const res = await pollUntilDone(api, activeToken);
        reset();
        if (!res?.ok) {
          window._showToast?.(res?.error || "Move failed.", "error"); return;
        }
        if (res.cancelled) {
          window._showToast?.(
            `Stopped — moved ${(res.moved||0).toLocaleString()} before stopping.`,
            "warn");
          return;
        }
        window._showToast?.(
          `Moved ${(res.moved||0).toLocaleString()} thumbnail(s) `
          + `across ${Object.keys(res.per_channel || {}).length} channel(s). `
          + (res.skipped_dest_exists > 0
             ? `${res.skipped_dest_exists} skipped (duplicate already at target).`
             : ""),
          "ok");
      });
    })();

    // Bug 5: "Scan & repair hidden sidecars" — walk the whole archive
    // and set the Windows HIDDEN attribute on any visible sidecar so
    // each folder shows only the videos + the Transcript.txt. Idempotent
    // and non-destructive (only flips the hidden bit). Progress streams
    // to the Download log.
    (function wireHideSidecars() {
      const btn = document.getElementById("btn-hide-sidecars");
      if (!btn || btn._wired) return;
      btn._wired = true;
      btn.addEventListener("click", async () => {
        const api = window.pywebview?.api;
        if (!api?.archive_repair_hidden_sidecars) {
          window._showToast?.("Native mode required.", "warn"); return;
        }
        const go = await window.askChoice({
          title: "Scan & repair hidden sidecars",
          message:
            "Walk every channel folder + the archive root and hide any stray "
            + "sidecar files (.info.json, leftover thumbnails, .jsonl, etc.) "
            + "that are currently visible.\n\n"
            + "This only sets the Windows ‘hidden’ attribute — no files are "
            + "moved or deleted, and your videos + Transcript.txt stay "
            + "visible. Progress streams to the Download log.\n\nProceed?",
          choices: [{ label: "Scan & repair", value: "go", kind: "primary" }],
        });
        if (!go) return;
        btn.disabled = true;
        const orig = btn.textContent;
        btn.textContent = "Repairing…";
        try {
          const res = await api.archive_repair_hidden_sidecars();
          if (res?.already_running) {
            window._showToast?.(
              "A repair pass is already running — see the log.", "warn");
          } else if (res?.ok) {
            window._showToast?.(
              "Hidden-sidecar repair started — see the Download log for "
              + "progress.", "ok");
          } else {
            window._showToast?.(res?.error || "Could not start repair.",
              "error");
          }
        } catch (err) {
          window._showToast?.(String(err), "error");
        } finally {
          // Work runs in the background; re-enable the button shortly.
          setTimeout(() => { btn.disabled = false; btn.textContent = orig; },
            1500);
        }
      });
    })();

    // audit SM-1: reset sync state button in Settings > Tools.
    // Picks a channel via a simple prompt, confirms, then calls
    // subs_reset_sync_state which clears the bootstrap flags.
    (function wireResetSyncState() {
      const btn = document.getElementById("btn-reset-sync-state");
      if (!btn) return;
      btn.addEventListener("click", async () => {
        const api = window.pywebview?.api;
        if (!api?.subs_reset_sync_state) {
          window._showToast?.("Native mode required.", "warn");
          return;
        }
        // Pick a channel. Use a simple prompt instead of a bespoke
        // modal — this is an infrequent admin op, the prompt is
        // minimum UI.
        let channels = [];
        try {
          const data = await api.get_subs_channels?.();
          if (Array.isArray(data) && data.length === 2) channels = data[0] || [];
        } catch (_e) { /* fall through */ }
        if (!channels.length) {
          window._showToast?.("No channels found.", "warn");
          return;
        }
        const names = channels.map(c => c.name || c.folder || "").filter(Boolean);
        // UI audit #1: replaced native window.prompt/confirm with
        // the styled askTextInput + askQuestion modals so this flow
        // doesn't jarringly fall back to the OS dialog look.
        const head = "Clears: initialized, sync_complete, init_complete, "
          + "batch_resume_index, init_batch_after, last_sync.\n"
          + "The next sync will bootstrap the channel from scratch.\n\n"
          + "Channels: " + names.slice(0, 60).join(", ")
          + (names.length > 60 ? ` … (+${names.length-60} more)` : "");
        const pick = await (window.askTextInput
          ? window.askTextInput({
              title: "Reset channel sync state",
              message: head,
              placeholder: "Channel name (case-insensitive)",
              confirm: "Continue",
              cancel: "Cancel",
            })
          : Promise.resolve(null));
        if (!pick || !pick.trim()) return;
        const want = pick.trim().toLowerCase();
        const ch = channels.find(c => (c.name || c.folder || "").toLowerCase() === want);
        if (!ch) {
          window._showToast?.(`No channel matched "${pick}".`, "warn");
          return;
        }
        const ok = await (window.askQuestion
          ? window.askQuestion({
              title: "Reset sync state",
              message: `Reset sync state for "${ch.name || ch.folder}"?\n\n`
                + "This does NOT delete any videos or config — it only "
                + "clears the flags that gate the fast-path so the next "
                + "sync walks the whole channel again.",
              confirm: "Reset",
              cancel: "Cancel",
              danger: true,
            })
          : Promise.resolve(false));
        if (!ok) return;
        try {
          const res = await api.subs_reset_sync_state({
            url: ch.url, folder: ch.folder, name: ch.name,
          });
          if (res?.ok) {
            window._showToast?.(
              `Reset ${res.cleared_flags} flag(s) on "${res.channel}".`, "ok");
          } else {
            window._showToast?.(res?.error || "Reset failed.", "warn");
          }
        } catch (e) {
          window._showToast?.(`Reset failed: ${e}`, "warn");
        }
      });
    })();

    // Video-length backfill (Settings > Tools). ffprobes files locally to
    // fill missing lengths in the Videos grid. The button toggles between
    // "Check / fix" and "Stop" depending on whether a pass is running.
    (function wireFixVideoLengths() {
      const btn = document.getElementById("btn-fix-video-lengths");
      if (!btn) return;
      const IDLE = "Check / fix…";
      let running = false;
      const setRunning = (r) => {
        running = r;
        btn.textContent = r ? "Stop" : IDLE;
        btn.classList.toggle("btn-danger", r);
      };
      // Reflect a pass already in progress (e.g. user re-opened Settings).
      (async () => {
        try {
          const s = await window.pywebview?.api?.video_lengths_backfill_running?.();
          if (s?.running) setRunning(true);
        } catch (_e) { /* non-fatal */ }
      })();
      // Backend calls this on completion (with the count filled): flip the
      // button back to idle and refresh the Videos grid if anything changed.
      window._onVideoLengthsBackfilled = function (filled) {
        setRunning(false);
        if (filled && window._loadVideosView) {
          try { window._loadVideosView(); } catch (_e) {}
        }
      };
      btn.addEventListener("click", async () => {
        const api = window.pywebview?.api;
        if (!api?.video_lengths_backfill_start) {
          window._showToast?.("Native mode required.", "warn");
          return;
        }
        if (running) {
          try { await api.video_lengths_backfill_cancel?.(); } catch (_e) {}
          window._showToast?.("Stopping… lengths filled so far are kept; re-run to resume.", "ok");
          setRunning(false);
          return;
        }
        let missing = 0;
        try {
          const c = await api.video_lengths_missing_count?.();
          if (c?.ok) missing = c.missing || 0;
        } catch (_e) { /* non-fatal */ }
        if (!missing) {
          window._showToast?.("Every video already has a length.", "ok");
          return;
        }
        const ok = await (window.askConfirm
          ? window.askConfirm("Check / fix video lengths",
              `${missing.toLocaleString()} video(s) have no stored length.\n\n`
              + "I'll read the true length from each file locally with ffprobe "
              + "(no YouTube). It runs in the background — progress shows in "
              + "the log, and you can Stop any time and re-run to resume.\n\n"
              + "Large archives can take ~15–30 minutes.",
              { confirm: "Fix now" })
          : Promise.resolve(true));
        if (!ok) return;
        try {
          const r = await api.video_lengths_backfill_start();
          if (r?.ok) {
            setRunning(true);
            window._showToast?.(
              `Filling ${missing.toLocaleString()} length(s) — progress in the log.`, "ok");
          } else {
            window._showToast?.(r?.error || "Couldn't start.", "warn");
          }
        } catch (e) {
          window._showToast?.(`Couldn't start: ${e}`, "warn");
        }
      });
    })();

    // (Save button removed — every field auto-saves. The path fields are
    // readonly and only change via the Browse pickers below, so they
    // persist right after a folder is chosen.)
    browseOut?.addEventListener("click", async () => {
      const cur = document.getElementById("settings-output-dir").value;
      const res = await window.pywebview?.api?.pick_folder?.("Archive root", cur);
      if (res?.ok && res.path) {
        document.getElementById("settings-output-dir").value = res.path;
        saveField("output_dir", res.path);
      }
    });
    browseVid?.addEventListener("click", async () => {
      const cur = document.getElementById("settings-video-dir").value;
      const res = await window.pywebview?.api?.pick_folder?.("Single-video downloads", cur);
      if (res?.ok && res.path) {
        document.getElementById("settings-video-dir").value = res.path;
        saveField("video_out_dir", res.path);
      }
    });

    ytdlpBtn?.addEventListener("click", async () => {
      const ok = await askConfirm("Update yt-dlp",
        "Run `yt-dlp -U` to fetch the latest release?\n\nOutput streams to the main log.",
        { confirm: "Update" });
      if (!ok) return;
      await window.pywebview?.api?.ytdlp_update?.();
    });

    expBtn?.addEventListener("click", async () => {
      const res = await window.pywebview?.api?.channels_export?.();
      if (res?.ok) window._showToast?.(`Exported ${res.count} channels.`, "ok");
      else if (!res?.cancelled) window._showToast?.(res?.error || "Export failed.", "error");
    });
    impBtn?.addEventListener("click", async () => {
      const res = await window.pywebview?.api?.channels_import?.();
      if (res?.ok) {
        const skipped = res.skipped || 0;
        const reasons = Array.isArray(res.skipped_reasons) ? res.skipped_reasons : [];
        if (skipped > 0 && reasons.length > 0) {
          // Audit U-13: surface per-skip reasons in a confirm modal so
          // the user can see WHY each channel was skipped (already
          // subscribed / missing URL / not a YouTube link / etc.).
          // Previously the toast just said "5 skipped" with no detail.
          const _esc = (s) => String(s ?? "").replace(/[&<>"']/g, c =>
            ({ "&": "&amp;", "<": "&lt;", ">": "&gt;",
               '"': "&quot;", "'": "&#39;" }[c]));
          // Group by reason for a tidy summary.
          const byReason = {};
          for (const r of reasons) {
            const key = r.reason || "(unknown)";
            (byReason[key] = byReason[key] || []).push(r.name || "(no name)");
          }
          const reasonHtml = Object.keys(byReason).map(reason => {
            const names = byReason[reason];
            const more = names.length > 5 ? ` <span style="color:#888;">+ ${names.length - 5} more</span>` : "";
            return `
              <div style="margin-bottom:8px;">
                <div style="font-weight:600;color:#cdd;">${_esc(reason)}
                  <span style="color:#888;font-weight:normal;">(${names.length})</span>
                </div>
                <div style="font-size:11px;color:#aaa;padding-left:10px;">
                  ${names.slice(0, 5).map(_esc).join("<br>")}${more}
                </div>
              </div>`;
          }).join("");
          // Open dialog and inject HTML before awaiting (same pattern as U-11).
          const dialogPromise = askQuestion({
            title: `Imported ${res.added} channels (${skipped} skipped)`,
            message: "",
            confirm: "OK",
            noCancel: true,
          });
          try {
            const body = document.querySelector(".askq-backdrop:last-child .askq-body");
            if (body) body.innerHTML =
              `<div style="margin-bottom:8px;color:#888;font-size:11px;">` +
              `Skipped channels by reason:</div>` + reasonHtml;
          } catch {}
          await dialogPromise;
        } else {
          window._showToast?.(`Added ${res.added} channels (${skipped} skipped).`, "ok");
        }
        // refresh the Subs table in place instead of
        // location.reload(). Reloading during an active sync
        // destroyed the in-flight Whisper progress UI, wiped log
        // state, and lost any unsaved Settings field edits. Refresh
        // helpers keep everything else intact.
        try { window.refreshSubsTable?.(); } catch {}
        try { window._primeBrowse?.(); } catch {}
      } else if (!res?.cancelled) {
        window._showToast?.(res?.error || "Import failed.", "error");
      }
    });

    bkExpBtn?.addEventListener("click", async () => {
      const res = await window.pywebview?.api?.export_full_backup?.();
      if (res?.ok) window._showToast?.(`Backup saved (${res.files} files).`, "ok");
      else if (!res?.cancelled) window._showToast?.(res?.error || "Backup failed.", "error");
    });
    bkImpBtn?.addEventListener("click", async () => {
      const api = window.pywebview?.api;
      if (!api) return;
      // Audit U-11: preview the backup BEFORE overwriting. Two-stage
      // flow: (1) preview returns the ZIP's manifest without writing
      // anything; (2) user reviews the file list + total size; (3) on
      // confirm, the same ZIP path is passed to import_full_backup
      // for the actual restore.
      const prev = await (api.import_full_backup_preview
        ? api.import_full_backup_preview()
        : Promise.resolve(null));
      if (!prev) {
        // Older backend without preview support \u2014 fall back to legacy
        // one-click flow with the strong-warning askDanger.
        const okLegacy = await askDanger("Restore backup",
          "Restoring a backup will OVERWRITE your current config, queue state, and journals.\n\n" +
          "A snapshot of the current config is saved to backups/ first, so you can roll back.",
          "Pick ZIP\u2026");
        if (!okLegacy) return;
        const res = await api.import_full_backup?.();
        _handleImportResult(res);
        return;
      }
      if (!prev.ok) {
        if (!prev.cancelled) {
          window._showToast?.(prev.error || "Preview failed.", "error");
        }
        return;
      }
      // Build the preview list. Each item: name, size, modified.
      const _esc = (s) => String(s ?? "").replace(/[&<>"']/g, c =>
        ({ "&": "&amp;", "<": "&lt;", ">": "&gt;",
           '"': "&quot;", "'": "&#39;" }[c]));
      const rows = (prev.items || []).map(it =>
        `<tr>
          <td>${_esc(it.name)}</td>
          <td style="text-align:right;">${_esc(it.size_label)}</td>
          <td style="color:#888;">${_esc(it.modified)}</td>
        </tr>`
      ).join("");
      const previewHtml =
        `<div style="max-height:280px;overflow:auto;
                     border:1px solid #2a2d33;border-radius:4px;
                     padding:6px;margin-top:8px;">
           <table style="width:100%;font-size:11px;
                         border-collapse:collapse;">
             <thead>
               <tr style="text-align:left;color:#888;">
                 <th>File</th><th style="text-align:right;">Size</th>
                 <th>Modified</th>
               </tr>
             </thead>
             <tbody>${rows}</tbody>
           </table>
         </div>
         <div style="margin-top:8px;font-size:11px;color:#888;">
           Total: ${prev.items.length} file(s) \u2014 ${_esc(prev.total_label)}.
           Your current config will be snapshotted before overwrite.
         </div>`;
      // Open the dialog (askQuestion creates the backdrop synchronously
      // and returns a promise that resolves on Confirm / Cancel / Esc).
      // We inject the rich preview HTML BEFORE awaiting \u2014 otherwise the
      // user would see an empty modal until they clicked something.
      const previewPromise = askQuestion({
        title: "Restore this backup?",
        message: "",  // body filled in below
        confirm: "Restore",
        cancel: "Cancel",
        danger: true,
      });
      try {
        const body = document.querySelector(".askq-backdrop:last-child .askq-body");
        if (body) body.innerHTML = previewHtml;
      } catch {}
      const confirmRestore = await previewPromise;
      if (!confirmRestore) return;
      const res = await api.import_full_backup?.(prev.zip_path);
      _handleImportResult(res);
    });

    function _handleImportResult(res) {
      if (res?.ok) {
        window._showToast?.(
          `Restored ${res.files_restored || res.files || "?"} files. ` +
          `Restart to apply.`,
          "ok",
          { ttlMs: 10000, action: { label: "Restart now", onClick: () => {
            window.pywebview?.api?.app_restart?.();
          }}}
        );
      } else if (res?.write_blocked) {
        window._showToast?.(
          "Write-gate off \u2014 config changes won't persist to disk.",
          "warn"
        );
      } else if (!res?.cancelled) {
        window._showToast?.(res?.error || "Restore failed.", "error");
      }
    }
  }

  window.initSettingsTab = initSettingsTab;
})();
