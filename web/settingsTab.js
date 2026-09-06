/**
 * web/settingsTab.js — Settings tab init (config form, paths, yt-dlp
 * updater, backup export/import).
 *
 * Exposed as window.initSettingsTab; app.js boot calls it once.
 */
(function () {
  "use strict";

  // Shared with Health so the same timestamp has the same meaning everywhere.
  window.YT = window.YT || {};
  window.YT.backupDates = {
    format(timestamp) {
      const ts = Number(timestamp);
      const date = new Date(ts * 1000);
      if (!Number.isFinite(ts) || ts <= 0 || !Number.isFinite(date.getTime())) return "unknown";
      const seconds = Math.max(0, Math.floor(Date.now() / 1000 - ts));
      const relative = seconds < 60 ? "just now"
        : seconds < 3600 ? `${Math.floor(seconds / 60)}m ago`
        : seconds < 86400 ? `${Math.floor(seconds / 3600)}h ago`
        : `${Math.floor(seconds / 86400)}d ago`;
      return `${date.toLocaleString()} (${relative})`;
    },
  };

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

  function nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
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
    const autorunModeSel = document.getElementById("settings-autorun-mode");
    const ytdlpBtn = document.getElementById("btn-ytdlp-update");
    const ytdlpChannelSel = document.getElementById("settings-ytdlp-channel");
    const ytdlpUpdateModeSel = document.getElementById("settings-ytdlp-update-mode");
    const expBtn = document.getElementById("btn-export-channels");
    const impBtn = document.getElementById("btn-import-channels");
    const bkExpBtn = document.getElementById("btn-export-backup");
    const bkImpBtn = document.getElementById("btn-import-backup");
    if (!panel) return;
    let _lastAutomaticBackupTs = 0;
    let _lastAutomaticBackupPath = "";
    if (autorunModeSel) {
      autorunModeSel.dataset.savedValue = autorunModeSel.value || "clock";
    }

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
      if (!nativeBridgeUp()) {
        flashSaved(false);
        window._showToast?.(
          "YTArchiver is still starting. Try again in a moment.", "warn");
        return false;
      }
      try {
        const res = await bridgeCall("settings_save", { [key]: value });
        if (res?.ok) {
          flashSaved(true);
          if (key === "output_dir") {
            window.dispatchEvent(new Event("archive-roots-changed"));
          }
          return true;
        } else {
          flashSaved(false);
          window._showToast?.(res?.error || "Save failed.", "error");
          return false;
        }
      } catch (e) {
        flashSaved(false);
        window._showToast?.("Save failed: " + e, "error");
        return false;
      }
    }
    function rememberControl(el, value) {
      if (!el) return;
      el.dataset.savedValue = typeof value === "boolean"
        ? (value ? "true" : "false") : String(value ?? "");
    }
    async function persistControl(el, key, value, options = {}) {
      if (!el) return false;
      const isChecked = options.checked === true;
      const fallback = isChecked ? !value : "";
      const savedRaw = el.dataset.savedValue;
      const previous = savedRaw === undefined
        ? fallback
        : (isChecked ? savedRaw === "true" : savedRaw);
      el.disabled = true;
      el._ytddRepaint?.();
      const ok = await saveField(key, value);
      if (ok) {
        rememberControl(el, value);
        options.onSaved?.(value, previous);
      } else {
        if (isChecked) el.checked = !!previous;
        else el.value = String(previous ?? "");
        el._ytddRepaint?.();
        options.onRollback?.(previous);
      }
      el.disabled = false;
      el._ytddRepaint?.();
      return ok;
    }
    // Last-persisted disk-staleness, so a blur on an invalid/blank value
    // can revert the field instead of saving garbage.
    let _diskLastGood = "24";
    let _ytdlpCheckLastGood = "1";
    let _trashRetentionLastGood = "30";
    let _ytdlpUpdateMode = "automatic";
    let _ytdlpAutoUpdatable = true;
    let _archiveCapacity = { mode: "percent", percent: 90, free_gb: 100 };
    let _archiveCapacityLastGood = "90";
    let _trafficStatus = null;

    function setTrashRetentionValue(select, value) {
      if (!select) return;
      const days = Number(value);
      const valid = Number.isInteger(days) && days >= 0 && days <= 3650;
      const normalized = String(valid ? days : 30);
      select.querySelectorAll("option[data-trash-custom]").forEach((option) => {
        option.remove();
      });
      if (!Array.from(select.options).some((option) => option.value === normalized)) {
        const option = document.createElement("option");
        option.value = normalized;
        option.textContent = `${days.toLocaleString()} days (custom)`;
        option.dataset.trashCustom = "1";
        select.appendChild(option);
      }
      select.value = normalized;
      _trashRetentionLastGood = normalized;
      select._ytddRepaint?.();
    }

    function renderTraffic(status) {
      if (status) _trafficStatus = status;
      const t = _trafficStatus;
      if (!t) return;
      const modeEl = document.getElementById("settings-youtube-traffic-mode");
      if (modeEl) {
        modeEl.value = t.mode || "conservative";
        modeEl._ytddRepaint?.();
      }
      const custom = document.getElementById("settings-traffic-custom");
      if (custom) custom.hidden = t.mode !== "custom";
      if (t.mode === "custom") {
        const effectiveValues = {
          "settings-traffic-daily": t.daily_limit,
          "settings-traffic-hourly": t.hourly_limit,
          "settings-traffic-min-gap": t.min_gap,
          "settings-traffic-max-gap": t.max_gap,
        };
        Object.entries(effectiveValues).forEach(([id, value]) => {
          const el = document.getElementById(id);
          if (el && value != null) el.value = String(value);
        });
      }
      const finite = Number(t.daily_limit) > 0;
      window._setBudgetAutosyncAvailable?.(finite);
      const used = document.getElementById("settings-traffic-meter");
      if (used) {
        const hourlyUsage =
          document.getElementById("settings-traffic-hourly-usage");
        const dailyUsage =
          document.getElementById("settings-traffic-daily-usage");
        const cooldown =
          document.getElementById("settings-traffic-cooldown");
        if (hourlyUsage) {
          hourlyUsage.textContent = finite
            ? `${Number(t.hourly_used).toLocaleString()} / ${Number(t.hourly_limit).toLocaleString()}`
            : `${Number(t.hourly_used).toLocaleString()} / \u221e`;
        }
        if (dailyUsage) {
          dailyUsage.textContent = finite
            ? `${Number(t.daily_used).toLocaleString()} / ${Number(t.daily_limit).toLocaleString()}`
            : `${Number(t.daily_used).toLocaleString()} / \u221e`;
        }
        if (cooldown && t.circuit?.active) {
          const until = new Date(t.circuit.cooldown_until * 1000);
          cooldown.textContent =
            `YouTube requests paused until ${until.toLocaleString()}`;
          cooldown.hidden = false;
        } else if (cooldown) {
          cooldown.textContent = "";
          cooldown.hidden = true;
        }
        used.classList.toggle("is-warning",
          !!t.circuit?.active
          || (finite && (t.hourly_remaining === 0 || t.daily_remaining === 0)));
      }
      const recommendation = document.getElementById("settings-traffic-recommendation");
      if (recommendation) {
        const p = t.projection || {};
        const cadence = document.getElementById("settings-traffic-cadence");
        const detail = document.getElementById("settings-traffic-recommendation-detail");
        const note = document.getElementById("settings-traffic-recommendation-note");
        const channels = Number(p.sweep?.channels) || 0;
        const units = Number(p.sweep?.units) || 0;
        const low = Number(p.recommended_hours_low) || 0;
        const high = Number(p.recommended_hours_high) || 0;
        const fits = p.fits_complete_sweep !== false;

        if (cadence) {
          if (!finite) {
            cadence.textContent = "Manual scheduling";
          } else if (!channels) {
            cadence.textContent = "Add channels to calculate";
          } else if (!fits) {
            cadence.textContent = "Increase the daily budget";
          } else if (low === high) {
            cadence.textContent = `About every ${low} hour${low === 1 ? "" : "s"}`;
          } else {
            cadence.textContent = `Every ${low}\u2013${high} hours`;
          }
        }
        if (detail) {
          if (!channels) {
            detail.textContent =
              "We\u2019ll estimate a safe auto-sync schedule from your archive.";
          } else if (!fits) {
            detail.textContent =
              `A complete sync needs about ${units.toLocaleString()} operations, ` +
              `more than the current ${Number(t.daily_limit).toLocaleString()}-operation daily budget.`;
          } else {
            detail.textContent =
              `Calculated from ${channels.toLocaleString()} channels and about ` +
              `${units.toLocaleString()} operations per complete sync.`;
          }
        }
        if (note) {
          note.hidden = finite;
          note.textContent = finite
            ? ""
            : "\u201cWhen budget allows\u201d is unavailable because Unlimited has no budget ceiling.";
        }
        recommendation.classList.toggle(
          "is-warning", !finite || !fits);
      }
    }

    async function refreshTraffic() {
      if (!nativeBridgeUp()) return;
      try {
        const status = await bridgeCall("youtube_traffic_status");
        if (status?.ok) renderTraffic(status);
      } catch (_e) { /* non-fatal */ }
    }

    async function saveTraffic(payload) {
      if (!nativeBridgeUp()) {
        renderTraffic(_trafficStatus);
        window._showToast?.(
          "YTArchiver is still starting. Try again in a moment.", "warn");
        return false;
      }
      try {
        const res = await bridgeCall("settings_save", payload);
        if (!res?.ok) {
          flashSaved(false);
          window._showToast?.(res?.error || "Save failed.", "error");
          renderTraffic(_trafficStatus);
          return false;
        }
        flashSaved(true);
        if (res.budget_autosync_disabled) {
          window._showToast?.(
            'Auto-sync was turned Off because "When budget allows" is unavailable in Unlimited mode.',
            "warn");
          window.dispatchEvent(new Event("autorun-state-changed"));
        }
        await refreshTraffic();
        return true;
      } catch (e) {
        flashSaved(false);
        window._showToast?.("Save failed: " + e, "error");
        renderTraffic(_trafficStatus);
        return false;
      }
    }

    function _capMode() {
      const mode = document.getElementById("settings-archive-capacity-mode")?.value;
      return mode === "free_gb" ? "free_gb" : "percent";
    }

    function _capClamp(mode, value) {
      const n = parseInt(value, 10);
      if (!Number.isFinite(n)) return null;
      return mode === "free_gb"
        ? Math.max(1, Math.min(1000000, n))
        : Math.max(1, Math.min(100, n));
    }

    function _renderArchiveCapacityControls() {
      const modeEl = document.getElementById("settings-archive-capacity-mode");
      const valueEl = document.getElementById("settings-archive-capacity-threshold");
      const unitEl = document.getElementById("settings-archive-capacity-unit");
      const mode = (_archiveCapacity.mode === "free_gb") ? "free_gb" : "percent";
      const value = mode === "free_gb" ? _archiveCapacity.free_gb : _archiveCapacity.percent;
      if (modeEl) modeEl.value = mode;
      if (valueEl) {
        valueEl.value = String(value);
        valueEl.placeholder = mode === "free_gb" ? "100" : "90";
      }
      if (unitEl) unitEl.textContent = mode === "free_gb" ? "GB free" : "% full";
      _archiveCapacityLastGood = String(value);
    }

    // RAM estimate is intentionally a range: real rows vary with title,
    // path depth, thumbnails, and Python object overhead.
    function _fmtBackupAge(ts, path = "") {
      if (!ts) return "Last backup: never";
      const days = Math.floor((Date.now() / 1000 - ts) / 86400);
      const label = window.YT.backupDates.format(ts);
      const location = path ? ` · Saved to: ${path}` : "";
      return days >= 14
        ? `⚠ Last backup: ${label} — consider exporting soon${location}`
        : `Last backup: ${label}${location}`;
    }

    function _fmtAutomaticBackupAge(ts, interval) {
      const prefix = interval === "off" ? "Automatic backups are off. " : "";
      if (!ts) {
        return prefix + "No automatic backup has run yet."
          + (interval === "off" ? "" : " YTArchiver checks while the app is open.");
      }
      return prefix + `Last automatic backup: ${window.YT.backupDates.format(ts)}`
        + (_lastAutomaticBackupPath ? ` · Saved to: ${_lastAutomaticBackupPath}` : "");
    }

    function _renderAutomaticBackupAge(interval) {
      const status = document.getElementById("backup-auto-age-display");
      if (status) {
        status.textContent = _fmtAutomaticBackupAge(
          _lastAutomaticBackupTs, interval || "off");
      }
    }

    function _fmtYtdlpCheckAge(ts) {
      if (!ts) return "never checked";
      const wholeDays = Math.max(0,
        Math.floor((Date.now() / 1000 - ts) / 86400));
      const label = wholeDays === 0 ? "today"
        : wholeDays === 1 ? "yesterday"
        : `${wholeDays} days ago`;
      return `last checked ${label}`;
    }

    function _renderYtdlpUpdateControls() {
      const daysEl = document.getElementById("settings-ytdlp-check-days");
      const noteEl = document.getElementById("settings-ytdlp-update-note");
      if (daysEl) daysEl.disabled = _ytdlpUpdateMode === "off";
      if (!noteEl) return;
      if (_ytdlpUpdateMode === "off") {
        noteEl.textContent = "Update checks are disabled";
      } else if (_ytdlpUpdateMode === "notify") {
        noteEl.textContent = "Reports new releases without installing them";
      } else if (!_ytdlpAutoUpdatable) {
        noteEl.textContent = "Package-managed yt-dlp detected — updates remain notify-only";
      } else {
        noteEl.textContent = "Installs when YouTube work is idle; no restart required";
      }
    }

    async function load() {
      if (!nativeBridgeUp()) return;
      try {
        const s = await bridgeCall("settings_load");
        const outputEl = document.getElementById("settings-output-dir");
        const videoDirEl = document.getElementById("settings-video-dir");
        const whisperEl = document.getElementById("settings-whisper-model");
        const defaultResEl = document.getElementById("settings-default-res");
        const logModeEl = document.getElementById("settings-log-mode");
        if (outputEl) { outputEl.value = s.output_dir || ""; rememberControl(outputEl, outputEl.value); }
        if (videoDirEl) { videoDirEl.value = s.video_out_dir || ""; rememberControl(videoDirEl, videoDirEl.value); }
        if (whisperEl) { whisperEl.value = s.whisper_model || "small"; rememberControl(whisperEl, whisperEl.value); }
        if (defaultResEl) { defaultResEl.value = s.default_resolution || "720"; rememberControl(defaultResEl, defaultResEl.value); }
        if (logModeEl) { logModeEl.value = s.log_mode || "Simple"; rememberControl(logModeEl, logModeEl.value); }
        const legacySubsEl = document.getElementById("settings-legacy-subs-tab");
        if (legacySubsEl) {
          legacySubsEl.checked = !!s.legacy_subs_tab;
          rememberControl(legacySubsEl, legacySubsEl.checked);
        }
        window._applyLegacySubsMode?.(!!s.legacy_subs_tab);
        if (ytdlpChannelSel) {
          ytdlpChannelSel.value =
            ["stable", "nightly"].includes(s.ytdlp_channel) ? s.ytdlp_channel : "stable";
          rememberControl(ytdlpChannelSel, ytdlpChannelSel.value);
          ytdlpChannelSel._ytddRepaint?.();
        }
        _ytdlpUpdateMode = ["automatic", "notify", "off"].includes(s.ytdlp_update_mode)
          ? s.ytdlp_update_mode : "automatic";
        if (ytdlpUpdateModeSel) {
          ytdlpUpdateModeSel.value = _ytdlpUpdateMode;
          rememberControl(ytdlpUpdateModeSel, _ytdlpUpdateMode);
          ytdlpUpdateModeSel._ytddRepaint?.();
        }
        const ytdlpCheckEl = document.getElementById("settings-ytdlp-check-days");
        if (ytdlpCheckEl) {
          const days = Math.max(1, Math.min(365,
            parseInt(s.ytdlp_update_check_days ?? 1, 10) || 1));
          ytdlpCheckEl.value = String(days);
          _ytdlpCheckLastGood = ytdlpCheckEl.value;
          rememberControl(ytdlpCheckEl, ytdlpCheckEl.value);
        }
        _renderYtdlpUpdateControls();
        const ytdlpCheckStatus = document.getElementById("settings-ytdlp-check-status");
        if (ytdlpCheckStatus) {
          ytdlpCheckStatus.textContent = _fmtYtdlpCheckAge(
            Number(s.last_ytdlp_update_check_ts) || 0);
        }
        // Startup knobs
        const stEl = document.getElementById("settings-disk-staleness");
        if (stEl) {
          stEl.value = String(s.disk_scan_staleness_hours ?? 24);
          _diskLastGood = stEl.value;
          rememberControl(stEl, stEl.value);
        }
        _archiveCapacity = {
          mode: s.archive_capacity_warning_mode === "free_gb" ? "free_gb" : "percent",
          percent: _capClamp("percent", s.archive_capacity_warning_percent ?? 90) ?? 90,
          free_gb: _capClamp("free_gb", s.archive_capacity_warning_free_gb ?? 100) ?? 100,
        };
        _renderArchiveCapacityControls();
        rememberControl(
          document.getElementById("settings-archive-capacity-mode"),
          _archiveCapacity.mode);
        rememberControl(
          document.getElementById("settings-archive-capacity-threshold"),
          _archiveCapacityLastGood);
        const avgEl = document.getElementById("settings-show-avg-size");
        if (avgEl) {
          avgEl.checked = (s.show_avg_size !== false);
          rememberControl(avgEl, avgEl.checked);
        }
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
          rememberControl(cbEl, cbEl.value);
        }
        // v80 auto-backup cadence — "off" | "daily" | "weekly" | "monthly".
        const abEl = document.getElementById("settings-auto-backup");
        if (abEl) {
          const ab = (s.auto_backup_interval || "off").toLowerCase();
          abEl.value = ["off","daily","weekly","monthly"].includes(ab)
            ? ab : "off";
          abEl.dataset.savedValue = abEl.value;
          _lastAutomaticBackupTs = Number(s.last_auto_backup_ts) || 0;
          _lastAutomaticBackupPath = s.last_auto_backup_path || "";
          _renderAutomaticBackupAge(abEl.value);
        }
        const trashRetentionEl = document.getElementById(
          "settings-trash-retention-days");
        if (trashRetentionEl) {
          setTrashRetentionValue(
            trashRetentionEl, s.trash_retention_days ?? 30);
        }
        const trafficMode = document.getElementById("settings-youtube-traffic-mode");
        if (trafficMode) {
          trafficMode.value = s.youtube_traffic_mode || "conservative";
        }
        const trafficValues = {
          "settings-traffic-daily": s.youtube_traffic_custom_daily ?? 750,
          "settings-traffic-hourly": s.youtube_traffic_custom_hourly ?? 90,
          "settings-traffic-min-gap": s.youtube_traffic_custom_min_gap ?? 10,
          "settings-traffic-max-gap": s.youtube_traffic_custom_max_gap ?? 20,
        };
        Object.entries(trafficValues).forEach(([id, value]) => {
          const el = document.getElementById(id);
          if (el) el.value = String(value);
        });
        if (s.youtube_traffic) renderTraffic(s.youtube_traffic);
        // Auto-sync timing is controlled by the live scheduler rather than
        // settings_save, so use its authoritative state when Settings opens.
        try {
          const state = await bridgeCall("autorun_state");
          if (autorunModeSel) {
            const mode = state?.mode === "clock" ? "clock" : "timer";
            autorunModeSel.value = mode;
            autorunModeSel.dataset.savedValue = mode;
            rememberControl(autorunModeSel, mode);
            autorunModeSel._ytddRepaint?.();
          }
        } catch (_e) { /* autoSync's periodic refresh can recover */ }
        // Launch at boot — read Registry state, not config.
        try {
          const bootState = await bridgeCall("launch_at_boot_get");
          const labEl = document.getElementById("settings-launch-at-boot");
          const lbmEl = document.getElementById("settings-boot-minimized");
          const lbmWrap = document.getElementById("settings-boot-minimized-wrap");
          if (labEl) {
            labEl.checked = !!bootState?.enabled;
            rememberControl(labEl, labEl.checked);
          }
          if (lbmEl) {
            lbmEl.checked = !!bootState?.minimized;
            rememberControl(lbmEl, lbmEl.checked);
          }
          if (lbmWrap) lbmWrap.hidden = !bootState?.enabled;
        } catch (_e) { /* non-fatal */ }
        // BUG FIX (2026-05-14): the custom `.yt-dd` widget mirrors a
        // hidden <select> via its own div trigger. When JS sets
        // sel.value programmatically there's no change event, so the
        // visible trigger label stays stuck at whatever was selected
        // at DOM-ready (the HTML default — `<option selected>`). User
        // saw "Ask each time" forever even with close_behavior=quit in
        // config. Explicitly repaint selects in whichever owning panel is
        // visible. Hidden-panel layout sizes are zero, so those controls are
        // repainted when their tab becomes active instead.
        document.querySelectorAll(".settings-view .ctl-select").forEach((sel) => {
          const ownerPanel = sel.closest(".tab-panel");
          if (ownerPanel?.classList.contains("active") && sel._ytddRepaint) {
            sel._ytddRepaint();
          }
        });
        // Backup age (T295)
        const bkAgeEl = document.getElementById("backup-age-display");
        if (bkAgeEl) bkAgeEl.textContent = _fmtBackupAge(s.last_backup_ts || 0, s.last_backup_path);
        const vEl = document.getElementById("settings-ytdlp-version");
        if (vEl) vEl.textContent = "checking\u2026";
        try {
          const v = await bridgeCall("ytdlp_version");
          if (vEl) vEl.textContent = v?.ok ? v.version : (v?.error || "not found");
          _ytdlpAutoUpdatable = v?.ok ? v.auto_updatable !== false : true;
          _renderYtdlpUpdateControls();
        } catch { if (vEl) vEl.textContent = "check failed"; }
      } catch (e) { console.warn("settings load:", e); }
    }

    // ── Per-field auto-save wiring ──────────────────────────────────
    // Selects: persist the new value immediately on change.
    document.getElementById("settings-whisper-model")
      ?.addEventListener("change", (e) =>
        persistControl(e.target, "whisper_model", e.target.value));
    document.getElementById("settings-default-res")
      ?.addEventListener("change", (e) =>
        persistControl(e.target, "default_resolution", e.target.value));
    document.getElementById("settings-log-mode")
      ?.addEventListener("change", (e) =>
        persistControl(e.target, "log_mode", e.target.value));
    document.getElementById("settings-legacy-subs-tab")
      ?.addEventListener("change", async (e) => {
        const enabled = !!e.target.checked;
        if (window._setDenseSubsPreference) {
          const saved = await window._setDenseSubsPreference(enabled);
          if (!saved) e.target.checked = !enabled;
          return;
        }
        window._applyLegacySubsMode?.(enabled);
        await persistControl(e.target, "legacy_subs_tab", enabled, {
          checked: true,
          onRollback: (previous) =>
            window._applyLegacySubsMode?.(!!previous),
        });
      });
    document.getElementById("settings-close-behavior")
      ?.addEventListener("change", (e) =>
        persistControl(e.target, "close_behavior", e.target.value));
    document.getElementById("settings-auto-backup")
      ?.addEventListener("change", async (e) => {
        const previous = e.target.dataset.savedValue || "off";
        const next = e.target.value;
        e.target.disabled = true;
        e.target._ytddRepaint?.();
        try {
          if (await saveField("auto_backup_interval", next)) {
            e.target.dataset.savedValue = next;
            _renderAutomaticBackupAge(next);
          } else {
            e.target.value = previous;
            e.target._ytddRepaint?.();
            _renderAutomaticBackupAge(previous);
          }
        } finally {
          e.target.disabled = false;
          e.target._ytddRepaint?.();
        }
      });
    document.getElementById("settings-trash-retention-days")
      ?.addEventListener("change", async (e) => {
        const days = Number(e.target.value);
        const allowed = [0, 7, 14, 30, 60, 90, 180, 365];
        const isLoadedCustom = !!e.target.selectedOptions?.[0]
          ?.dataset?.trashCustom;
        if (!allowed.includes(days) && !isLoadedCustom) {
          e.target.value = _trashRetentionLastGood;
          e.target._ytddRepaint?.();
          window._showToast?.("Choose one of the available Trash time periods.", "warn");
          return;
        }
        const previous = _trashRetentionLastGood;
        e.target.disabled = true;
        e.target._ytddRepaint?.();
        const saved = await saveField("trash_retention_days", days);
        e.target.disabled = false;
        e.target._ytddRepaint?.();
        if (saved) {
          _trashRetentionLastGood = String(days);
        } else {
          e.target.value = previous;
          e.target._ytddRepaint?.();
        }
      });
    ytdlpUpdateModeSel?.addEventListener("change", async (e) => {
      const previous = e.target.dataset.savedValue || "automatic";
      _ytdlpUpdateMode = ["automatic", "notify", "off"].includes(e.target.value)
        ? e.target.value : "automatic";
      _renderYtdlpUpdateControls();
      await persistControl(e.target, "ytdlp_update_mode", _ytdlpUpdateMode, {
        onRollback: () => {
          _ytdlpUpdateMode = previous;
          _renderYtdlpUpdateControls();
        },
      });
    });
    const _ytdlpCheckEl = document.getElementById("settings-ytdlp-check-days");
    _ytdlpCheckEl?.addEventListener("change", async () => {
      const raw = _ytdlpCheckEl.value;
      const days = parseInt(raw, 10);
      if (raw === "" || !Number.isFinite(days) || days < 1 || days > 365) {
        _ytdlpCheckEl.value = _ytdlpCheckLastGood;
        window._showToast?.("Update interval must be 1–365 days.", "warn");
        return;
      }
      _ytdlpCheckEl.value = String(days);
      const saved = await persistControl(
        _ytdlpCheckEl, "ytdlp_update_check_days", days);
      if (saved) _ytdlpCheckLastGood = _ytdlpCheckEl.value;
    });
    _ytdlpCheckEl?.addEventListener("keydown", (e) => {
      if (e.key === "Enter") { e.preventDefault(); _ytdlpCheckEl.blur(); }
    });
    document.getElementById("settings-youtube-traffic-mode")
      ?.addEventListener("change", async (e) => {
        const mode = e.target.value;
        e.target.disabled = true;
        e.target._ytddRepaint?.();
        try {
          await saveTraffic({ youtube_traffic_mode: mode });
        } finally {
          e.target.disabled = false;
          e.target._ytddRepaint?.();
        }
      });
    const trafficInputMap = {
      "settings-traffic-daily": "youtube_traffic_custom_daily",
      "settings-traffic-hourly": "youtube_traffic_custom_hourly",
      "settings-traffic-min-gap": "youtube_traffic_custom_min_gap",
      "settings-traffic-max-gap": "youtube_traffic_custom_max_gap",
    };
    Object.entries(trafficInputMap).forEach(([id, key]) => {
      const el = document.getElementById(id);
      el?.addEventListener("change", async () => {
        const value = parseInt(el.value, 10);
        if (!Number.isFinite(value)) {
          renderTraffic(_trafficStatus);
          window._showToast?.("Enter a whole number.", "error");
          return;
        }
        el.disabled = true;
        try {
          await saveTraffic({ [key]: value });
        } finally {
          el.disabled = false;
        }
      });
      el?.addEventListener("keydown", (e) => {
        if (e.key === "Enter") { e.preventDefault(); el.blur(); }
      });
    });

    // Avg-size toggle: live-apply to the Subs table AND persist.
    document.getElementById("settings-show-avg-size")
      ?.addEventListener("change", async (e) => {
        window._applySubsAvgVisibility?.(!!e.target.checked);
        await persistControl(e.target, "show_avg_size", !!e.target.checked, {
          checked: true,
          onRollback: (previous) =>
            window._applySubsAvgVisibility?.(!!previous),
        });
      });

    // Disk-scan staleness: commit on blur (the `change` event) or Enter.
    // Validate first — a blank / negative / non-numeric value used to be
    // silently mapped to 0 ("always rescan"); now it's rejected and the
    // field reverts to the last persisted value so nothing bad is saved.
    const _diskEl = document.getElementById("settings-disk-staleness");
    _diskEl?.addEventListener("change", async () => {
      const _v = _diskEl.value;
      const _n = parseInt(_v, 10);
      if (_v == null || _v === "" || !Number.isFinite(_n) || _n < 0) {
        window._showToast?.(
          "Rescan time must be 0 hours or more.", "error");
        _diskEl.value = _diskLastGood;   // revert the bad input
        return;
      }
      _diskEl.value = String(_n);
      const saved = await persistControl(
        _diskEl, "disk_scan_staleness_hours", _n);
      if (saved) _diskLastGood = _diskEl.value;
    });
    _diskEl?.addEventListener("keydown", (e) => {
      if (e.key === "Enter") { e.preventDefault(); _diskEl.blur(); }
    });

    const _capModeEl = document.getElementById("settings-archive-capacity-mode");
    const _capThresholdEl = document.getElementById("settings-archive-capacity-threshold");
    _capModeEl?.addEventListener("change", async (e) => {
      const previous = _archiveCapacity.mode;
      _archiveCapacity.mode = e.target.value === "free_gb" ? "free_gb" : "percent";
      _renderArchiveCapacityControls();
      await persistControl(
        e.target, "archive_capacity_warning_mode", _archiveCapacity.mode, {
          onSaved: () => rememberControl(
            _capThresholdEl, _archiveCapacityLastGood),
          onRollback: () => {
            _archiveCapacity.mode = previous;
            _renderArchiveCapacityControls();
          },
        });
    });
    _capThresholdEl?.addEventListener("change", async () => {
      const mode = _capMode();
      const n = _capClamp(mode, _capThresholdEl.value);
      if (n == null) {
        window._showToast?.(
          mode === "free_gb"
            ? "Archive warning threshold must be a whole number of GB."
            : "Archive warning threshold must be a whole-number percent.",
          "error");
        _capThresholdEl.value = _archiveCapacityLastGood;
        return;
      }
      _capThresholdEl.value = String(n);
      const key = mode === "free_gb"
        ? "archive_capacity_warning_free_gb"
        : "archive_capacity_warning_percent";
      const saved = await persistControl(_capThresholdEl, key, n);
      if (!saved) return;
      _archiveCapacityLastGood = String(n);
      if (mode === "free_gb") {
        _archiveCapacity.free_gb = n;
      } else {
        _archiveCapacity.percent = n;
      }
    });
    _capThresholdEl?.addEventListener("keydown", (e) => {
      if (e.key === "Enter") { e.preventDefault(); _capThresholdEl.blur(); }
    });

    // Launch at boot / start minimized to tray.
    document.getElementById("settings-launch-at-boot")?.addEventListener("change", async (e) => {
      const previous = e.target.dataset.savedValue === "true";
      if (!nativeBridgeUp()) {
        e.target.checked = previous;
        window._showToast?.(
          "YTArchiver is still starting. Try again in a moment.", "warn");
        return;
      }
      const minimized = !!document.getElementById("settings-boot-minimized")?.checked;
      const lbmWrap = document.getElementById("settings-boot-minimized-wrap");
      if (lbmWrap) lbmWrap.hidden = !e.target.checked;
      e.target.disabled = true;
      try {
        const res = await bridgeCall("launch_at_boot_set", !!e.target.checked, minimized);
        if (res?.ok) {
          rememberControl(e.target, e.target.checked);
          flashSaved(true);
        } else {
          throw new Error(res?.error || "Boot setting failed.");
        }
      } catch (err) {
        e.target.checked = previous;
        if (lbmWrap) lbmWrap.hidden = !previous;
        flashSaved(false);
        window._showToast?.(err?.message || String(err), "error");
      } finally {
        e.target.disabled = false;
      }
    });
    document.getElementById("settings-boot-minimized")?.addEventListener("change", async (e) => {
      const previous = e.target.dataset.savedValue === "true";
      if (!nativeBridgeUp()) {
        e.target.checked = previous;
        window._showToast?.(
          "YTArchiver is still starting. Try again in a moment.", "warn");
        return;
      }
      const enabled = !!document.getElementById("settings-launch-at-boot")?.checked;
      if (!enabled) {
        e.target.checked = previous;
        return;
      }
      e.target.disabled = true;
      try {
        const res = await bridgeCall("launch_at_boot_set", true, !!e.target.checked);
        if (res?.ok) {
          rememberControl(e.target, e.target.checked);
          flashSaved(true);
        } else {
          throw new Error(res?.error || "Boot setting failed.");
        }
      } catch (err) {
        e.target.checked = previous;
        flashSaved(false);
        window._showToast?.(err?.message || String(err), "error");
      } finally {
        e.target.disabled = false;
      }
    });

    // Auto-sync timing mode — applied immediately via its own scheduler
    // call (not settings_save), so the Download countdown/clock label updates
    // right away while Settings remains the canonical preference location.
    document.getElementById("settings-autorun-mode")?.addEventListener("change", async (e) => {
      const select = e.currentTarget;
      const requested = select.value === "clock" ? "clock" : "timer";
      const previous = select.dataset.savedValue ||
        (requested === "clock" ? "timer" : "clock");
      if (!nativeBridgeUp()) {
        select.value = previous;
        select._ytddRepaint?.();
        window._showToast?.(
          "YTArchiver is still starting. Try again in a moment.", "warn");
        return;
      }
      select.disabled = true;
      select._ytddRepaint?.();
      try {
        const result = await bridgeCall("autorun_set_mode", requested);
        if (!result?.ok) {
          throw new Error(result?.error || "Could not change auto-sync timing.");
        }
        select.dataset.savedValue = requested;
        rememberControl(select, requested);
        window.dispatchEvent(new Event("autorun-state-changed"));
        flashSaved(true);
      } catch (err) {
        select.value = previous;
        select._ytddRepaint?.();
        window.dispatchEvent(new Event("autorun-state-changed"));
        flashSaved(false);
        window._showToast?.(
          err?.message || "Could not change auto-sync timing.", "error");
      } finally {
        select.disabled = false;
        select._ytddRepaint?.();
      }
    });

    // Reload persistent preferences whenever the user opens Settings.
    // Re-read when either preferences or backup controls become visible.
    // Automatic backup now lives in Health, while the rest stays in Settings.
    document.querySelector('.tab[data-tab="settings"]')
      ?.addEventListener("click", () => { setTimeout(load, 50); });
    document.querySelector('.tab[data-tab="health"]')
      ?.addEventListener("click", () => { setTimeout(load, 50); });
    window.YT?.bridge?.ready?.then(load).catch(() => {});
    // Also load once on boot so values are ready if the user switches fast.
    setTimeout(load, 200);
    const _trafficRefreshIv = setInterval(() => {
      if (document.visibilityState === "visible"
          && panel.classList.contains("active")) {
        refreshTraffic();
      }
    }, 60_000);
    window.addEventListener("beforeunload", () => {
      clearInterval(_trafficRefreshIv);
    });

    // Bulk metadata buttons (formerly the hidden right-click on "All Channels"
    // in Browse — YTArchiver.py:24840). Both prompt for confirmation because
    // they queue N*K yt-dlp jobs.
    const metaQueueAll = document.getElementById("btn-metadata-queue-all");
    const metaRefresh = document.getElementById("btn-metadata-refresh-all");
    const metadataQueuedText = (res, prefix = "Queued") => {
      const queued = Number.isFinite(Number(res?.queued)) ? Number(res.queued) : 0;
      const total = Number.isFinite(Number(res?.channels)) ? Number(res.channels) : queued;
      const count = total !== queued
        ? `${queued} of ${total} channel(s)`
        : `${queued} channel(s)`;
      const paused = res?.paused === true ? " Queue is paused - resume to start." : "";
      return `${prefix} ${count}.${paused}`;
    };
    // Disable-during-await guard for both Queue and Refresh buttons.
    // Without this, an impatient double-click stacked two askConfirm
    // modals — the user could click through both, queuing the same
    // job twice (audit: settingsTab.js:170).
    metaQueueAll?.addEventListener("click", async () => {
      if (metaQueueAll.disabled) return;
      metaQueueAll.disabled = true;
      try {
        if (!nativeBridgeUp()) {
          window._showToast?.(
            "YTArchiver is still starting. Try again in a moment.", "warn");
          return;
        }
        const ok = await askConfirm("Queue all metadata",
          "Queue a metadata download for EVERY subscribed channel?\n\n" +
          "Large libraries can take several hours. You can cancel from " +
          "Sync Tasks.",
          { confirm: "Queue all" });
        if (!ok) return;
        const res = await bridgeCall("metadata_queue_all", false);
        if (res?.ok) window._showToast?.(metadataQueuedText(res), res?.paused === true ? "warn" : "ok");
        else window._showToast?.(res?.error || "Queue failed.", "error");
      } finally {
        metaQueueAll.disabled = false;
      }
    });
    metaRefresh?.addEventListener("click", async () => {
      if (metaRefresh.disabled) return;
      metaRefresh.disabled = true;
      try {
        if (!nativeBridgeUp()) {
          window._showToast?.(
            "YTArchiver is still starting. Try again in a moment.", "warn");
          return;
        }
        const ok = await askConfirm("Refresh views/likes",
          "Re-fetch view counts and like counts for every video on every channel?\n\n" +
          "This checks every archived video again, including videos skipped " +
          "after earlier errors. Large libraries can take several hours. " +
          "You can cancel from Sync Tasks.",
          { confirm: "Refresh" });
        if (!ok) return;
        const res = await bridgeCall("metadata_queue_all", true);
        if (res?.ok) window._showToast?.(metadataQueuedText(res, "Queued refresh for"), res?.paused === true ? "warn" : "ok");
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
      const pollUntilDone = async (token) => {
        const deadline = Date.now() + 60 * 60 * 1000;  // 1h safety
        while (Date.now() < deadline) {
          await new Promise(r => setTimeout(r, 600));
          let p;
          try { p = await bridgeCall("realign_poll", token); }
          catch (e) { return { ok: false, error: String(e) }; }
          if (p && !p.pending) return p;
        }
        try { await bridgeCall("realign_cancel", token); } catch {}
        return { ok: false, error: "Timed out; cancellation requested." };
      };

      btn.addEventListener("click", async () => {
        if (!nativeBridgeUp()) {
          window._showToast?.(
            "YTArchiver is still starting. Try again in a moment.", "warn");
          return;
        }
        // Mid-pass: the button acts as a Stop control.
        if (stopMode && activeToken) {
          try { await bridgeCall("realign_cancel", activeToken); } catch {}
          btn.disabled = true; btn.textContent = "Stopping…";
          return;
        }
        // ── Survey (dry run) ──
        let start;
        try { start = await bridgeCall("realign_start", true); }
        catch (e) { window._showToast?.(String(e), "error"); return; }
        if (!start?.ok || !start.token) {
          window._showToast?.(start?.error || "Couldn't start scan.", "error");
          return;
        }
        activeToken = start.token;
        setStop("⏹ Stop scan");
        window._showToast?.(
          "Scanning thumbnails — progress in the Download log…", "ok");
        const preview = await pollUntilDone(activeToken);
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
        const thumbWord = n === 1 ? "thumbnail" : "thumbnails";
        const channelWord = chans === 1 ? "channel" : "channels";
        const duplicateNote = dups > 0
          ? `\n\n${dups.toLocaleString()} already ${dups === 1 ? "has" : "have"} `
            + `a thumbnail in the correct folder and will be left alone.`
          : "";
        const msg = `Found ${n.toLocaleString()} misplaced ${thumbWord} across `
          + `${chans.toLocaleString()} ${channelWord}.\n\n`
          + `Move ${n === 1 ? "it" : "them"} next to the matching `
          + `${n === 1 ? "video" : "videos"}?${duplicateNote}`;
        const go = await window.askChoice({
          title: "Realign misplaced thumbnails",
          message: msg,
          choices: [{ label: `Move ${n.toLocaleString()} ${thumbWord}`, value: "go", kind: "primary" }],
        });
        if (!go) return;
        // ── Move ──
        let mv;
        try { mv = await bridgeCall("realign_start", false); }
        catch (e) { window._showToast?.(String(e), "error"); return; }
        if (!mv?.ok || !mv.token) {
          window._showToast?.(mv?.error || "Couldn't start move.", "error"); return;
        }
        activeToken = mv.token;
        setStop("⏹ Stop move");
        window._showToast?.(
          "Moving thumbnails — progress in the Download log…", "ok");
        const res = await pollUntilDone(activeToken);
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
        if (!nativeBridgeUp()) {
          window._showToast?.(
            "YTArchiver is still starting. Try again in a moment.", "warn");
          return;
        }
        const go = await window.askChoice({
          title: "Repair hidden support files",
          message:
            "Check every archive folder and hide support files that are "
            + "currently visible.\n\n"
            + "This changes only the Windows Hidden setting. Nothing is "
            + "moved or deleted, and videos plus readable transcript files "
            + "stay visible. Progress appears in the Download log.\n\nContinue?",
          choices: [{ label: "Scan & repair", value: "go", kind: "primary" }],
        });
        if (!go) return;
        btn.disabled = true;
        const orig = btn.textContent;
        btn.textContent = "Repairing…";
        try {
          const res = await bridgeCall("archive_repair_hidden_sidecars");
          if (res?.already_running) {
            window._showToast?.(
              "A repair pass is already running — see the log.", "warn");
          } else if (res?.ok) {
            window._showToast?.(
              "Support-file repair started — see the Download log for "
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

    // audit SM-1: reset sync state button in Health > Library.
    // Picks a channel, disambiguates duplicate display names, confirms, then
    // calls subs_reset_sync_state with the channel's full identity.
    (function wireResetSyncState() {
      const btn = document.getElementById("btn-reset-sync-state");
      if (!btn) return;
      btn.addEventListener("click", async () => {
        if (!nativeBridgeUp()) {
          window._showToast?.(
            "YTArchiver is still starting. Try again in a moment.", "warn");
          return;
        }
        let channels = [];
        let channelLoadError = "";
        try {
          const data = await bridgeCall("get_subs_channels");
          if (Array.isArray(data) && data.length === 2) channels = data[0] || [];
        } catch (e) {
          channelLoadError = String(e);
          console.warn("get_subs_channels failed:", e);
        }
        if (channelLoadError) {
          window._showToast?.(
            `Could not load channels: ${channelLoadError}`, "error");
          return;
        }
        if (!channels.length) {
          window._showToast?.("No channels found.", "warn");
          return;
        }
        const names = channels.map(c => c.name || c.folder || "").filter(Boolean);
        // Use styled askTextInput + askQuestion modals so this flow
        // stays visually consistent with the rest of the app.
        const head = "The next sync will recheck the entire channel from "
          + "the beginning. No downloaded files are removed.\n\n"
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
        const matches = channels.filter(
          c => (c.name || c.folder || "").toLowerCase() === want);
        if (!matches.length) {
          window._showToast?.(`No channel matched "${pick}".`, "warn");
          return;
        }
        let ch = matches[0];
        if (matches.length > 1) {
          const selected = await (window.askChoice
            ? window.askChoice({
                title: "Choose the channel",
                message: `More than one channel is named "${pick}".`,
                choices: matches.map((candidate, index) => ({
                  label: `${candidate.name || candidate.folder} — `
                    + `${candidate.folder || candidate.url || `channel ${index + 1}`}`,
                  value: String(index),
                  kind: "primary",
                })),
                cancelKind: "ghost",
              })
            : Promise.resolve(null));
          if (selected == null || !matches[Number(selected)]) return;
          ch = matches[Number(selected)];
        }
        const ok = await (window.askQuestion
          ? window.askQuestion({
              title: "Reset sync state",
              message: `Reset sync state for "${ch.name || ch.folder}"?\n\n`
                + "No videos or settings will be deleted. The next sync "
                + "will fully recheck this channel.",
              confirm: "Reset",
              cancel: "Cancel",
              danger: true,
            })
          : Promise.resolve(false));
        if (!ok) return;
        try {
          const res = await bridgeCall("subs_reset_sync_state", {
            url: ch.url, folder: ch.folder, name: ch.name,
          });
          if (res?.ok) {
            window._showToast?.(
              `Sync history reset for "${res.channel}".`, "ok");
          } else {
            window._showToast?.(res?.error || "Reset failed.", "warn");
          }
        } catch (e) {
          window._showToast?.(`Reset failed: ${e}`, "warn");
        }
      });
    })();

    // Video-length backfill (Health > Library). ffprobes files locally to
    // fill missing lengths in the Videos grid. The button toggles between
    // "Check / fix" and "Stop" depending on whether a pass is running.
    (function wireFixVideoLengths() {
      const btn = document.getElementById("btn-fix-video-lengths");
      if (!btn) return;
      const IDLE = "Check / fix…";
      let running = false;
      const setRunning = (r) => {
        running = r;
        btn.disabled = false;
        btn.textContent = r ? "Stop" : IDLE;
        btn.classList.toggle("btn-danger", r);
      };
      // Reflect a pass already in progress (e.g. user re-opened Settings).
      (async () => {
        try {
          if (!nativeBridgeUp()) return;
          const s = await bridgeCall("video_lengths_backfill_running");
          if (s?.running) setRunning(true);
        } catch (_e) { /* non-fatal */ }
      })();
      // Backend calls this on completion (with the count filled): flip the
      // button back to idle and refresh the Videos grid if anything changed.
      window._onVideoLengthsBackfilled = function (result) {
        setRunning(false);
        const payload = (result && typeof result === "object")
          ? result : { ok: true, filled: Number(result) || 0 };
        const filled = Number(payload.filled) || 0;
        if (filled && window._loadVideosView) {
          try { window._loadVideosView(); } catch (_e) {}
        }
        if (!payload.ok && payload.error) {
          window._showToast?.(
            `Video-length check failed: ${payload.error}`, "error");
        }
      };
      btn.addEventListener("click", async () => {
        if (!nativeBridgeUp()) {
          window._showToast?.(
            "YTArchiver is still starting. Try again in a moment.", "warn");
          return;
        }
        if (running) {
          btn.disabled = true;
          btn.textContent = "Stopping…";
          try {
            const stopped = await bridgeCall("video_lengths_backfill_cancel");
            if (!stopped?.ok) {
              throw new Error(stopped?.error || "Could not stop the check.");
            }
            window._showToast?.(
              "Stopping… lengths filled so far are kept.", "ok");
          } catch (error) {
            setRunning(true);
            window._showToast?.(
              error?.message || String(error), "error");
          }
          return;
        }
        let missing = 0;
        try {
          const c = await bridgeCall("video_lengths_missing_count");
          if (!c?.ok) {
            throw new Error(c?.error || "Could not read the video library.");
          }
          missing = c.missing || 0;
        } catch (e) {
          window._showToast?.(
            e?.message || "Could not check missing video lengths.", "error");
          return;
        }
        if (!missing) {
          window._showToast?.(
            "Every available video already has a length.", "ok");
          return;
        }
        const ok = await (window.askConfirm
          ? window.askConfirm("Check / fix video lengths",
              `${missing.toLocaleString()} available video(s) have no stored length.\n\n`
              + "YTArchiver will read the length directly from each local "
              + "video file without contacting YouTube. It runs in the "
              + "background — progress shows in "
              + "the log, and you can Stop any time and re-run to resume.\n\n"
              + "Large archives may take 15–30 minutes.",
              { confirm: "Fix now" })
          : Promise.resolve(true));
        if (!ok) return;
        try {
          const r = await bridgeCall("video_lengths_backfill_start");
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
      const field = document.getElementById("settings-output-dir");
      const cur = field.value;
      try {
        const res = await bridgeCall(
          "pick_folder", "Select archive folder", cur);
        if (res?.ok && res.path) {
          field.value = res.path;
          await persistControl(field, "output_dir", res.path);
        } else if (!res?.cancelled) {
          window._showToast?.(
            res?.error || "Could not choose an archive folder.", "error");
        }
      } catch (error) {
        window._showToast?.(`Could not choose an archive folder: ${error}`, "error");
      }
    });
    browseVid?.addEventListener("click", async () => {
      const field = document.getElementById("settings-video-dir");
      const cur = field.value;
      try {
        const res = await bridgeCall(
          "pick_folder", "Single-video downloads", cur);
        if (res?.ok && res.path) {
          field.value = res.path;
          await persistControl(field, "video_out_dir", res.path);
        } else if (!res?.cancelled) {
          window._showToast?.(
            res?.error || "Could not choose a video folder.", "error");
        }
      } catch (error) {
        window._showToast?.(`Could not choose a video folder: ${error}`, "error");
      }
    });

    // Human label for the currently-selected channel.
    function _channelLabel() {
      return ytdlpChannelSel?.value === "nightly" ? "beta (nightly)" : "stable";
    }

    // ytdlp_update() fires a background thread and returns immediately, so
    // the version label won't reflect a completed update on its own. Poll
    // ytdlp_version (its cache is busted on update success) until the string
    // changes, then paint it. Falls back to a plain refresh on timeout so
    // the label never gets stuck on "updating…".
    async function _refreshYtdlpVersion() {
      const vEl = document.getElementById("settings-ytdlp-version");
      if (!vEl) return;
      try {
        const v = await bridgeCall("ytdlp_version");
        vEl.textContent = v?.ok ? v.version : (v?.error || "not found");
      } catch { vEl.textContent = "check failed"; }
    }
    async function _pollYtdlpVersion(prevVersion) {
      const vEl = document.getElementById("settings-ytdlp-version");
      if (vEl) vEl.textContent = "updating…";
      for (let i = 0; i < 30; i++) {          // ~60s ceiling
        await new Promise((r) => setTimeout(r, 2000));
        let v;
        try { v = await bridgeCall("ytdlp_version"); } catch { continue; }
        if (v?.ok && v.version && v.version !== prevVersion) {
          if (vEl) vEl.textContent = v.version;
          return;
        }
      }
      await _refreshYtdlpVersion();           // timed out — show current
    }

    window._onYtdlpUpdateStatus = function (payload) {
      const status = String(payload?.status || "");
      const message = String(payload?.message || "yt-dlp update status changed.");
      const version = String(payload?.version || "");
      const vEl = document.getElementById("settings-ytdlp-version");
      if (status === "success" && version && vEl) vEl.textContent = version;
      if (status === "success" || status === "available") {
        const ageEl = document.getElementById("settings-ytdlp-check-status");
        if (ageEl) ageEl.textContent = "last checked today";
      }
      const tone = status === "success" ? "ok"
        : status === "error" ? "error"
        : (status === "deferred" || status === "available") ? "warn" : "";
      window._showToast?.(message, tone);
    };

    ytdlpBtn?.addEventListener("click", async () => {
      const ok = await askConfirm("Update yt-dlp",
        `Update yt-dlp to the latest ${_channelLabel()} release?\n\n` +
        "Output streams to the main log.",
        { confirm: "Update" });
      if (!ok) return;
      const prev = document.getElementById("settings-ytdlp-version")?.textContent || "";
      const result = await bridgeCall("ytdlp_update");
      if (result?.pending || result?.running) {
        window._showToast?.(
          "yt-dlp update queued until current YouTube work is idle.");
        return;
      }
      if (!result?.ok) {
        window._showToast?.(result?.error || "Couldn't start yt-dlp update.", "error");
        return;
      }
      _pollYtdlpVersion(prev);
    });

    // Release-channel switch. Persist the choice, then offer to update
    // right away — switching channels only takes effect once yt-dlp is
    // actually updated to it (`--update-to <channel>`). Declining leaves
    // the preference saved; the next Update (or startup nudge) honors it.
    ytdlpChannelSel?.addEventListener("change", async (e) => {
      const channel = e.target.value === "nightly" ? "nightly" : "stable";
      if (!await persistControl(e.target, "ytdlp_channel", channel)) return;
      const label = channel === "nightly" ? "beta (nightly)" : "stable";
      const go = await askConfirm(`Switch to ${label}?`,
        (channel === "nightly"
          ? "Beta pulls YouTube fixes ahead of stable — good when stable is up to date but downloads still fail with 403 errors.\n\n"
          : "This switches back to the stable channel (may downgrade to the latest stable release).\n\n") +
        `Update yt-dlp to ${label} now?\n\nIf YouTube work is active, the update will wait for an idle window.`,
        { confirm: "Update now", cancel: "Later" });
      if (go) {
        const prev = document.getElementById("settings-ytdlp-version")?.textContent || "";
        const result = await bridgeCall("ytdlp_update");
        if (result?.pending || result?.running) {
          window._showToast?.(
            "yt-dlp update queued until current YouTube work is idle.");
          return;
        }
        if (!result?.ok) {
          window._showToast?.(
            result?.error || "Couldn't start yt-dlp update.", "error");
          return;
        }
        _pollYtdlpVersion(prev);
      }
    });

    expBtn?.addEventListener("click", async () => {
      const res = await bridgeCall("channels_export");
      if (res?.ok) window._showToast?.(`Exported ${res.count} channels.`, "ok");
      else if (!res?.cancelled) window._showToast?.(res?.error || "Export failed.", "error");
    });
    impBtn?.addEventListener("click", async () => {
      const res = await bridgeCall("channels_import");
      if (res?.ok) {
        const skipped = res.skipped || 0;
        const reasons = Array.isArray(res.skipped_reasons) ? res.skipped_reasons : [];
        if (skipped > 0 && reasons.length > 0) {
          // Audit U-13: surface per-skip reasons in a confirm modal so
          // the user can see WHY each channel was skipped (already
          // subscribed / missing URL / not a YouTube link / etc.).
          // Previously the toast just said "5 skipped" with no detail.
          const _esc = window.YT?.util?.escapeHtml || window._escapeHtml
            || ((s) => String(s ?? "").replace(/[&<>"']/g, c =>
              ({ "&": "&amp;", "<": "&lt;", ">": "&gt;",
                 '"': "&quot;", "'": "&#39;" }[c])));
          // Group by reason for a tidy summary.
          const byReason = {};
          for (const r of reasons) {
            const key = r.reason || "(unknown)";
            (byReason[key] = byReason[key] || []).push(r.name || "(no name)");
          }
          const reasonHtml = Object.keys(byReason).map(reason => {
            const names = byReason[reason];
            const more = names.length > 5 ? ` <span class="import-skip-more">+ ${names.length - 5} more</span>` : "";
            return `
              <div class="import-skip-group">
                <div class="import-skip-title">${_esc(reason)}
                  <span class="import-skip-count">(${names.length})</span>
                </div>
                <div class="import-skip-names">
                  ${names.slice(0, 5).map(_esc).join("<br>")}${more}
                </div>
              </div>`;
          }).join("");
          await askQuestion({
            title: `Imported ${res.added} channels (${skipped} skipped)`,
            message: "Skipped channels by reason.",
            bodyHtml:
              `<div class="import-skip-intro">` +
              `Skipped channels by reason:</div>` + reasonHtml,
            confirm: "OK",
            noCancel: true,
          });
        } else {
          window._showToast?.(`Added ${res.added} channels (${skipped} skipped).`, "ok");
        }
        // refresh the Subs table in place instead of
        // location.reload(). Reloading during an active sync
        // destroyed the in-flight Whisper progress UI, wiped log
        // state, and lost any unsaved Settings field edits. Refresh
        // helpers keep everything else intact.
        try { window.refreshSubsTable?.(); } catch {}
      } else if (!res?.cancelled) {
        window._showToast?.(res?.error || "Import failed.", "error");
      }
    });

    bkExpBtn?.addEventListener("click", async () => {
      const res = await bridgeCall("export_full_backup");
      if (res?.ok) {
        if (res.fts_skipped) {
          window._showToast?.(
            `Backup saved (${res.files} files), but the Search index was ` +
              "too large to include. Search can be rebuilt after a restore. " +
              (res.bookmarks_included
                ? "Your bookmarks and notes are included."
                : "This backup does not separately preserve bookmarks or notes."),
            "warn",
            { ttlMs: 12000 }
          );
        } else {
          window._showToast?.(`Backup saved (${res.files} files).` +
            (res.bookmarks_included ? " Your bookmarks and notes are included." : ""), "ok");
        }
        const bkAgeEl = document.getElementById("backup-age-display");
        if (bkAgeEl) bkAgeEl.textContent = _fmtBackupAge(res.last_backup_ts || Date.now() / 1000, res.path);
      } else if (!res?.cancelled) {
        window._showToast?.(res?.error || "Backup failed.", "error");
      }
    });
    bkImpBtn?.addEventListener("click", async () => {
      if (!nativeBridgeUp()) return;
      // Audit U-11: preview the backup BEFORE overwriting. Two-stage
      // flow: (1) preview returns the ZIP's manifest without writing
      // anything; (2) user reviews the file list + total size; (3) on
      // confirm, the same ZIP path is passed to import_full_backup
      // for the actual restore.
      const prev = await bridgeCall("import_full_backup_preview");
      if (!prev) {
        // Older backend without preview support \u2014 fall back to legacy
        // one-click flow with the strong-warning askDanger.
        const okLegacy = await askDanger("Restore backup",
          "Restoring replaces your current settings and saved app state.\n\n" +
          "YTArchiver saves a safety copy of your current settings first.",
          "Pick ZIP\u2026");
        if (!okLegacy) return;
        const res = await bridgeCall("import_full_backup");
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
      const _esc = window.YT?.util?.escapeHtml || window._escapeHtml
        || ((s) => String(s ?? "").replace(/[&<>"']/g, c =>
          ({ "&": "&amp;", "<": "&lt;", ">": "&gt;",
             '"': "&quot;", "'": "&#39;" }[c])));
      const rows = (prev.items || []).map(it =>
        `<tr>
          <td>${_esc(it.name)}</td>
          <td class="backup-preview-num">${_esc(it.size_label)}</td>
          <td class="backup-preview-muted">${_esc(it.modified)}</td>
        </tr>`
      ).join("");
      const backupIndexNote = String(prev.fts_skipped || "")
        .replace(/\bFTS(?:\s+DB)?\b/gi, "search index");
      const ftsWarn = backupIndexNote
        ? `<div class="backup-preview-warning">
             ${_esc(backupIndexNote)}
           </div>`
        : "";
      const bookmarkNote = prev.bookmarks_included
        ? "Bookmarks and notes will be restored from this backup."
        : "This older backup has no bookmark data. Your current bookmarks and notes " +
          "will be retained if they can be read safely. Restoring will stop if they cannot.";
      const zipName = prev.zip_name || String(prev.zip_path || "Selected backup").split(/[\\/]/).pop();
      const created = Number(prev.created_at || prev.manifest?.created_at);
      const backupDate = created > 0 && Number.isFinite(created)
        ? `Created: ${window.YT.backupDates.format(created)}`
        : "Creation time was not recorded in this older backup."
          + (prev.zip_modified_at ? ` ZIP file modified: ${window.YT.backupDates.format(prev.zip_modified_at)}` : "");
      const indexIncluded = prev.index_included ?? prev.manifest?.fts_db_included;
      const names = (prev.items || []).map(item => String(item.name));
      const hasConfig = names.some(name => /(?:^|\/)ytarchiver_config\.json$/.test(name));
      const content = [
        hasConfig ? "Settings and subscriptions included" : "Settings file not found",
        prev.bookmarks_included ? "Bookmarks and notes included" : "Bookmarks retained from this installation",
        indexIncluded ? "Search index included" : "Search index not included — can be rebuilt",
      ];
      const previewHtml =
        `<div class="backup-preview-identity">
           <strong>${_esc(zipName)}</strong>
           <p>${_esc(backupDate)}</p>
           <p>${_esc(content.join(" · "))}</p>
           <p>${_esc(prev.zip_path || "")}</p>
         </div>
         <details><summary>Included files (${(prev.items || []).length})</summary>
         <div class="backup-preview-frame">
           <table class="backup-preview-table">
             <thead>
               <tr>
                 <th>File</th><th class="backup-preview-num">Size</th>
                 <th>Modified</th>
               </tr>
             </thead>
             <tbody>${rows}</tbody>
           </table>
         </div></details>
         <div class="backup-preview-total">
           Total: ${(prev.items || []).length} file(s) \u2014 ${_esc(prev.total_label)}.
           Your current settings are backed up before anything is replaced.
         </div>${ftsWarn}<div class="backup-preview-total">${_esc(bookmarkNote)}</div>`;
      const confirmRestore = await askQuestion({
        title: "Restore this backup?",
        message: "Review the backup contents before restoring.",
        bodyHtml: previewHtml,
        confirm: "Restore",
        cancel: "Cancel",
        danger: true,
      });
      if (!confirmRestore) return;
      const res = await bridgeCall("import_full_backup", prev.zip_path);
      _handleImportResult(res);
    });

    function _handleImportResult(res) {
      if (res?.ok) {
        const bookmarkMessage = res.bookmarks_source === "current_installation"
          ? "Your current bookmarks and notes were retained. "
          : res.bookmarks_source === "backup"
            ? "Bookmarks and notes were restored from the backup. " : "";
        window._showToast?.(
          `Restored ${res.files_restored || res.files || "?"} files. ` +
          bookmarkMessage +
          `Restart to apply.`,
          "ok",
          { ttlMs: 10000, action: { label: "Restart now", onClick: () => {
            bridgeCall("app_restart");
          }}}
        );
      } else if (res?.needs_restart) {
        window._showToast?.(
          (res?.error || "Restore could not finish safely.") +
            " Restart YTArchiver before using it again.",
          "error",
          { ttlMs: 0, action: { label: "Restart now", onClick: () => {
            bridgeCall("app_restart");
          }}}
        );
      } else if (res?.write_blocked) {
        window._showToast?.(
          res?.error ||
            "Settings are temporarily read-only. Restart YTArchiver and try again.",
          "warn"
        );
      } else if (!res?.cancelled) {
        window._showToast?.(res?.error || "Restore failed.", "error");
      }
    }
  }

  window.initSettingsTab = initSettingsTab;
})();
