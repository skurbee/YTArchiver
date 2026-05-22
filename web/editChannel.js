/**
 * web/editChannel.js — inline edit-channel panel for the Subs tab.
 *
 * Loading a row populates the form fields; Update/Remove/Cancel dispatch
 * to the bridge. Handles add and edit modes, three-state Update button
 * (disabled until changes pending), folder-name input that mirrors into
 * both `folder` and `folder_override` on save, From-date YYYY/MM/DD
 * with auto-advance, conditional Compress sub-fields, and the
 * "Continue redownload" / "Recheck resolution" affordances.
 *
 * Exposed as window.initEditChannelPanel; app.js boot calls it once.
 * Also publishes:
 *   - window.refreshSubsTable — re-fetches the Subs table data
 *   - window._editChannelFromContext(folder, urlGuess) — entry point
 *     used by the Subs context menu / double-click handlers.
 *
 * Depends on:
 *   - window.askConfirm, window.askQuestion, window.askDanger (modals.js)
 *   - window._showToast (toasts.js)
 *   - window._removeChannelWithPrompt (app.js)
 *   - window.renderSubsTable, window._primeBrowse (app.js)
 *   - window._subsAllRows (app.js, optional)
 *   - window.pywebview.api.* (native bridge)
 */
(function () {
  "use strict";

  function flashError(msg) {
    console.warn("[subs]", msg);
    if (window._showToast) {
      window._showToast(msg, "error");
    } else {
      alert(msg);
    }
  }
  function flashOk(msg) {
    console.info("[subs]", msg);
    window._showToast?.(msg, "ok");
  }

  function initEditChannelPanel() {
    const box = document.getElementById("edit-channel-box");
    const label = document.getElementById("edit-channel-label");
    const update = document.getElementById("btn-edit-update");
    const remove = document.getElementById("btn-edit-remove");
    const cancel = document.getElementById("btn-edit-cancel");
    if (!box) return;

    const resetFields = () => {
      ["edit-folder","edit-url","edit-min-dur","edit-max-dur"].forEach(id => {
        const el = document.getElementById(id); if (el) el.value = "";
      });
      ["edit-transcribe","edit-compress"].forEach(id => {
        const el = document.getElementById(id); if (el) el.checked = false;
      });
      document.getElementById("edit-metadata").checked = true;
      document.getElementById("edit-resolution").value = "720";
      const subs = document.querySelector('input[name="edit-range"][value="subscribe"]');
      if (subs) { subs.checked = true; subs.dispatchEvent(new Event("change")); }
      document.getElementById("edit-compress")?.dispatchEvent(new Event("change"));
    };

    // Panel is always visible. `collapsed` class hides everything past the
    // Folder Name + Channel URL row until the user types or we switch to
    // Edit mode. Start collapsed.
    box.classList.add("collapsed");

    const _updateCollapsed = () => {
      const nameVal = (document.getElementById("edit-folder")?.value || "").trim();
      const urlVal = (document.getElementById("edit-url")?.value || "").trim();
      const editing = !!_editingIdentity;
      const shouldShow = Boolean(nameVal || urlVal || editing);
      box.classList.toggle("collapsed", !shouldShow);
    };
    document.getElementById("edit-folder")?.addEventListener("input", _updateCollapsed);
    document.getElementById("edit-url")?.addEventListener("input", _updateCollapsed);

    // Reverse URL-type nudge: if the user pastes a video URL into the
    // channel URL field, show a handoff button to the Download tab.
    const _editUrlVideoNudge = document.getElementById("edit-url-video-nudge");
    const _editUrlField = document.getElementById("edit-url");
    const _updateEditUrlNudge = () => {
      if (!_editUrlVideoNudge || !_editUrlField) return;
      const t = (_editUrlField.value || "").trim();
      const isVideo = /^(?:https?:\/\/)?(?:(?:www|m)\.)?(?:youtube\.com\/(?:watch\?v=|shorts\/|live\/)|youtu\.be\/)[\w-]+/i.test(t);
      _editUrlVideoNudge.hidden = !isVideo;
    };
    _editUrlField?.addEventListener("input", _updateEditUrlNudge);
    _editUrlField?.addEventListener("paste", () => setTimeout(_updateEditUrlNudge, 10));
    _updateEditUrlNudge();

    document.getElementById("btn-edit-url-to-download")?.addEventListener("click", () => {
      const url = (_editUrlField?.value || "").trim();
      if (!url) return;
      document.querySelector('.tab[data-tab="download"]')?.click();
      setTimeout(() => {
        const dl = document.getElementById("url-input");
        if (dl) {
          dl.value = url;
          dl.dispatchEvent(new Event("input", { bubbles: true }));
          dl.focus();
        }
      }, 80);
      if (_editUrlField) {
        _editUrlField.value = "";
        _editUrlField.dispatchEvent(new Event("input", { bubbles: true }));
      }
      _updateEditUrlNudge();
    });

    const openPanel = (mode, channel) => {
      const ds = document.getElementById("edit-diskstats");
      if (mode === "edit" && channel) {
        label.textContent = `Edit channel — ${channel.folder}`;
        // Single folder field: prefer folder_override (on-disk name)
        // over folder (display name).
        const _folderVal = channel.folder_override || channel.folder || channel.name || "";
        document.getElementById("edit-folder").value = _folderVal;
        document.getElementById("edit-url").value = channel.url || "";
        window._editOriginalSnapshot = {
          folder: _folderVal, url: channel.url || "",
          resolution: String(channel.resolution || "720").replace("p",""),
          min_duration: channel.min_duration || 0,
          max_duration: channel.max_duration || 0,
          mode: channel.mode || "new",
          folder_org: (channel.split_months ? "months"
                         : (channel.split_years ? "years" : "flat")),
          from_date: channel.from_date || channel.date_after || "",
          auto_transcribe: !!channel.auto_transcribe,
          auto_metadata: !!channel.auto_metadata,
          compress_enabled: !!channel.compress_enabled,
        };
        document.getElementById("edit-resolution").value = String(channel.resolution || "720").replace("p", "");
        document.getElementById("edit-min-dur").value = channel.min_duration || "";
        document.getElementById("edit-max-dur").value = channel.max_duration || "";
        const _mode = (channel.mode || "new").toLowerCase();
        const _rangeVal = _mode === "full" ? "all"
                         : (_mode === "date" || _mode === "fromdate") ? "fromdate"
                         : "subscribe";
        const _rangeRadio = document.querySelector(
          `input[name="edit-range"][value="${_rangeVal}"]`);
        if (_rangeRadio) {
          _rangeRadio.checked = true;
          _rangeRadio.dispatchEvent(new Event("change", { bubbles: true }));
        }
        const _fromDate = channel.from_date || channel.date_after || "";
        // Stricter regex: dashes must be present consistently or absent
        // entirely. Old `\d{4}-?\d{2}-?\d{2}` accepted mixed "1999-0115"
        // which then sliced wrong (audit: editChannel.js:147).
        const _dateMatch = /^(\d{4})(-?)(\d{2})\2(\d{2})$/.exec(_fromDate);
        if (_dateMatch) {
          const y = _dateMatch[1], m = _dateMatch[3], d = _dateMatch[4];
          const dy = document.getElementById("edit-date-year");
          const dm = document.getElementById("edit-date-month");
          const dd = document.getElementById("edit-date-day");
          if (dy) dy.value = y;
          if (dm) dm.value = m;
          if (dd) dd.value = d;
        }
        const _folderOrg = channel.split_months ? "months"
                            : (channel.split_years ? "years" : "flat");
        const foEl2 = document.getElementById("edit-folder-org");
        if (foEl2) foEl2.value = _folderOrg;
        document.getElementById("edit-transcribe").checked = !!channel.auto_transcribe;
        document.getElementById("edit-metadata").checked = !!channel.auto_metadata;
        document.getElementById("edit-compress").checked = !!channel.compress_enabled;
        // Compress sub-fields (level / output res / batch size) were
        // never loaded from the channel dict when the panel opened,
        // so the dropdowns showed HTML defaults. Any save then stomped
        // the real saved values with those defaults. Load alongside.
        const _lvlEl = document.getElementById("edit-compress-quality");
        if (_lvlEl && channel.compress_level) _lvlEl.value = channel.compress_level;
        const _cResEl = document.getElementById("edit-compress-res");
        if (_cResEl && channel.compress_output_res) _cResEl.value = channel.compress_output_res;
        const _batchEl = document.getElementById("edit-compress-batch");
        if (_batchEl && channel.compress_batch_size) _batchEl.value = String(channel.compress_batch_size);
        document.getElementById("edit-compress")?.dispatchEvent(new Event("change"));
        _updateCollapsed();
        update.disabled = true;
        update.textContent = "💾 Update channel";
        remove.style.display = "";
        if (ds) {
          ds.hidden = false;
          const subsRow = (window._subsAllRows || [])
            .find(r => (r.folder || "").toLowerCase() === (channel.folder || "").toLowerCase());
          document.getElementById("ds-videos").textContent = subsRow?.n_vids ?? "—";
          document.getElementById("ds-size").textContent = subsRow?.size ?? "—";
          document.getElementById("ds-last-sync").textContent = subsRow?.last_sync ?? "—";
          const txSep = document.getElementById("ds-tx-sep");
          const txLbl = document.getElementById("ds-tx-label");
          const txVal = document.getElementById("ds-tx-count");
          if (txSep) txSep.hidden = true;
          if (txLbl) txLbl.hidden = true;
          if (txVal) txVal.hidden = true;
          const api = window.pywebview?.api;
          if (api?.channel_transcription_stats) {
            api.channel_transcription_stats(channel.folder || channel.name || "")
              .then((res) => {
                if (!res?.ok) return;
                if (!res.total) return;
                if (txSep) txSep.hidden = false;
                if (txLbl) txLbl.hidden = false;
                if (txVal) {
                  txVal.hidden = false;
                  const pct = res.total ? Math.round(100 * res.transcribed / res.total) : 0;
                  txVal.textContent = `${res.transcribed} / ${res.total} (${pct}%)`;
                  txVal.title = `Pending: ${res.pending}, Failed: ${res.failed}`;
                }
              })
              .catch(() => {});
          }
        }
      } else {
        label.textContent = "Add channel";
        resetFields();
        update.disabled = true;
        update.textContent = "Add channel";
        remove.style.display = "none";
        if (ds) ds.hidden = true;
        _updateCollapsed();
        window._editOriginalSnapshot = null;
      }
      if (mode === "edit") {
        box.scrollIntoView({ behavior: "smooth", block: "end" });
      }
    };

    const closePanel = () => {
      _editingIdentity = null;
      resetFields();
      label.textContent = "Add channel";
      update.disabled = true;
      update.textContent = "Add channel";
      remove.style.display = "none";
      const ds = document.getElementById("edit-diskstats");
      if (ds) ds.hidden = true;
      _updateCollapsed();
      window._editOriginalSnapshot = null;
    };

    // Dirty check before discarding edits. update.disabled === false
    // means _checkEditChanges saw a change relative to the snapshot
    // (Add mode treats any non-empty folder+url as "dirty"). Without
    // this confirm, Cancel silently nuked unsaved changes (audit:
    // editChannel.js:225).
    cancel.addEventListener("click", async () => {
      if (!update.disabled && window.askConfirm) {
        try {
          const ok = await window.askConfirm(
            "Discard changes?",
            "You have unsaved edits to this channel. Discard them?",
            { confirm: "Discard", cancel: "Keep editing" });
          if (!ok) return;
        } catch { /* if confirm fails, fall through to close */ }
      }
      closePanel();
    });

    // Live change-detection three-state Update button.
    const _editFields = [
      "edit-folder", "edit-url",
      "edit-resolution", "edit-min-dur", "edit-max-dur",
      "edit-folder-org",
      "edit-transcribe", "edit-metadata", "edit-compress",
      "edit-date-year", "edit-date-month", "edit-date-day",
    ];
    const _checkEditChanges = () => {
      const snap = window._editOriginalSnapshot;
      if (!snap) {
        const fv = (document.getElementById("edit-folder")?.value || "").trim();
        const uv = (document.getElementById("edit-url")?.value || "").trim();
        update.disabled = !(fv && uv);
        return;
      }
      const _folderCur = (document.getElementById("edit-folder")?.value || "").trim();
      const _rangeCur = (document.querySelector('input[name="edit-range"]:checked')?.value || "subscribe");
      const _dateCur = [
        (document.getElementById("edit-date-year")?.value || "").trim(),
        (document.getElementById("edit-date-month")?.value || "").trim(),
        (document.getElementById("edit-date-day")?.value || "").trim(),
      ].filter(Boolean).join("");
      const cur = {
        folder: _folderCur,
        folder_override: _folderCur,
        url: (document.getElementById("edit-url")?.value || "").trim(),
        resolution: String(document.getElementById("edit-resolution")?.value || "720").replace("p",""),
        min_duration: parseInt(document.getElementById("edit-min-dur")?.value, 10) || 0,
        max_duration: parseInt(document.getElementById("edit-max-dur")?.value, 10) || 0,
        mode: _rangeCur === "all" ? "full"
                         : (_rangeCur === "fromdate" ? "date" : "new"),
        folder_org: (document.getElementById("edit-folder-org")?.value || "flat"),
        from_date: _dateCur,
        auto_transcribe:!!document.getElementById("edit-transcribe")?.checked,
        auto_metadata: !!document.getElementById("edit-metadata")?.checked,
        compress_enabled:!!document.getElementById("edit-compress")?.checked,
      };
      const dirty = Object.keys(snap).some(k => String(snap[k]) !== String(cur[k]));
      update.disabled = !dirty;
    };
    for (const id of _editFields) {
      const el = document.getElementById(id);
      if (!el) continue;
      const ev = (el.type === "checkbox") ? "change" : "input";
      el.addEventListener(ev, _checkEditChanges);
    }

    // "Continue" / "Cancel" redownload buttons.
    const continueBtn = document.getElementById("edit-res-continue");
    const cancelBtn = document.getElementById("edit-res-cancel");
    const refreshContinueBtn = async (name) => {
      if (!continueBtn) return;
      continueBtn.hidden = true;
      if (cancelBtn) cancelBtn.hidden = true;
      if (!name) return;
      const api = window.pywebview?.api;
      if (!api?.chan_redownload_progress_peek) return;
      try {
        const p = await api.chan_redownload_progress_peek(name);
        if (p?.ok && p.pending) {
          continueBtn.hidden = false;
          const res = p.resolution || "best";
          const lab = res === "best" ? "Best" : `${res}p`;
          continueBtn.textContent = `↻ Continue ${lab} (${p.done || 0} done)`;
          continueBtn.dataset.resolution = res;
          continueBtn.title = `Resume in-progress ${lab} redownload (${p.done || 0} videos complete)`;
          if (cancelBtn) cancelBtn.hidden = false;
        }
      } catch {}
    };
    const _editFolderEl = document.getElementById("edit-folder");
    _editFolderEl?.addEventListener("change", () => {
      refreshContinueBtn(_editFolderEl.value.trim());
    });
    const _editLabel = document.getElementById("edit-channel-label");
    if (_editLabel) {
      // If a previous initEditChannelPanel call (hot-reload, late-init
      // race) attached an observer, disconnect it before observing
      // again. Otherwise each call stacked another live observer and
      // every label flip fired N redundant peek() API calls (audit:
      // editChannel.js:321).
      if (_editLabel._mo) {
        try { _editLabel._mo.disconnect(); } catch {}
      }
      const mo = new MutationObserver(() => {
        refreshContinueBtn(_editFolderEl?.value.trim() || "");
      });
      mo.observe(_editLabel, { childList: true, characterData: true, subtree: true });
      _editLabel._mo = mo;
    }
    continueBtn?.addEventListener("click", async () => {
      const name = _editFolderEl?.value.trim() || "";
      if (!name) return;
      const res = continueBtn.dataset.resolution || "best";
      const api = window.pywebview?.api;
      if (!api?.chan_redownload) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      const r = await api.chan_redownload(name, res);
      if (r?.ok) {
        if (r.queued) {
          window._showToast?.(`Queued redownload of ${name}.`, "ok");
        } else {
          window._showToast?.("Redownload resumed.", "ok");
        }
      } else {
        window._showToast?.(r?.error || "Resume failed.", "error");
      }
    });
    cancelBtn?.addEventListener("click", async () => {
      const name = _editFolderEl?.value.trim() || "";
      if (!name) return;
      const api = window.pywebview?.api;
      if (!api?.chan_cancel_redownload) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      const ok = await window.askQuestion?.({
        title: "Cancel Redownload",
        message: `Cancel the redownload for "${name}" and discard progress?`,
        confirm: "Cancel Redownload",
        cancel: "Keep",
        danger: true,
      });
      if (!ok) return;
      try {
        const r = await api.chan_cancel_redownload(name);
        if (!r?.ok) {
          window._showToast?.(r?.error || "Cancel failed.", "error");
          return;
        }
        await refreshContinueBtn(name);
        try { window.refreshSubsTable?.(); } catch {}
        const bits = [];
        if (r.was_running) bits.push("stopped running job");
        else if (r.was_queued) bits.push("removed from queue");
        if (r.progress_removed) bits.push("cleared saved progress");
        const msg = bits.length
          ? `Cancelled — ${bits.join(", ")}.`
          : "Nothing to cancel.";
        window._showToast?.(msg, "ok");
      } catch (e) {
        window._showToast?.("Error: " + e, "error");
      }
    });

    // Reset resolution — restores the dropdown to its default value
    // (720p, marked `selected` in the HTML). The button never had a
    // handler attached after the v71.x extraction — clicking it did
    // nothing.
    const resetBtn = document.getElementById("edit-res-reset");
    resetBtn?.addEventListener("click", (ev) => {
      ev.preventDefault();
      const resSel = document.getElementById("edit-resolution");
      if (!resSel) return;
      // Find the option marked `default` in the HTML (the `selected`
      // attribute reflects the initial default, not the current value).
      let def = "720";
      for (const opt of resSel.options) {
        if (opt.defaultSelected) { def = opt.value; break; }
      }
      resSel.value = def;
      // Fire `change` so any live-mirroring widget (custom yt-dd
      // wrapper) syncs its visible label to the new value.
      resSel.dispatchEvent(new Event("change", { bubbles: true }));
    });

    // Recheck resolution — scans the channel folder with ffprobe and
    // offers to queue mismatches for redownload.
    const recheckBtn = document.getElementById("edit-res-recheck");
    recheckBtn?.addEventListener("click", async () => {
      const api = window.pywebview?.api;
      if (!api?.chan_scan_resolution_mismatch) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      const name = (document.getElementById("edit-folder")?.value || "").trim();
      const target = (document.getElementById("edit-resolution")?.value || "720").trim();
      if (!name) {
        window._showToast?.("Pick or fill a channel first.", "warn");
        return;
      }
      window._showToast?.("Scanning video files…", "ok");
      try {
        // Token+poll pattern — bridge returns immediately with a token;
        // poll every 500ms until the worker thread reports done. The
        // old synchronous call could freeze the UI for minutes on
        // large channels.
        const startRes = await api.chan_scan_resolution_mismatch(name, target);
        if (!startRes?.ok) {
          window._showToast?.(startRes?.error || "Scan failed.", "error");
          return;
        }
        let res = startRes;
        if (startRes.token && startRes.started) {
          const deadline = Date.now() + 30 * 60 * 1000;
          while (Date.now() < deadline) {
            await new Promise(r => setTimeout(r, 500));
            const p = await api.chan_scan_resolution_mismatch_poll(startRes.token);
            if (!p?.pending) { res = p; break; }
          }
          if (!res || res.pending) {
            window._showToast?.("Scan timed out.", "error");
            return;
          }
        }
        if (!res?.ok) {
          window._showToast?.(res?.error || "Scan failed.", "error");
          return;
        }
        const mismatch = res.mismatch || 0;
        const total = res.total || 0;
        if (mismatch === 0) {
          window._showToast?.(`All ${total} video(s) already at ${target}p or higher.`, "ok");
          return;
        }
        const lab = target === "best" ? "Best quality" : `${target}p`;
        const ok = await window.askDanger(
          "Redownload at target resolution",
          `Redownload ${mismatch} of ${total} video(s) in "${name}" at ${lab}?\n\n` +
          "This scans local files, fetches the YouTube catalog, matches by ID, " +
          "downloads each video, and replaces the originals. Progress is saved — " +
          "you can cancel and resume later.",
          "Start redownload");
        if (!ok) return;
        const r2 = await api.chan_redownload(name, target);
        if (r2?.ok) window._showToast?.(`Redownload started (${lab}).`, "ok");
        else window._showToast?.(r2?.error || "Redownload failed.", "error");
      } catch (e) {
        window._showToast?.("Error: " + e, "error");
      }
    });

    // Restore defaults button — pulls from settings and applies.
    document.getElementById("btn-edit-restore")?.addEventListener("click", async () => {
      const defs = await window.pywebview?.api?.subs_get_defaults?.();
      if (!defs) return;
      document.getElementById("edit-resolution").value = defs.resolution || "720";
      document.getElementById("edit-min-dur").value = defs.min_duration ?? 3;
      document.getElementById("edit-max-dur").value = "";
      document.getElementById("edit-transcribe").checked = !!defs.auto_transcribe;
      document.getElementById("edit-metadata").checked = !!defs.auto_metadata;
      document.getElementById("edit-compress").checked = !!defs.compress_enabled;
      document.getElementById("edit-compress")?.dispatchEvent(new Event("change"));
      document.getElementById("edit-folder-org").value = defs.folder_org || "years";
      const rangeRadio = document.querySelector(`input[name="edit-range"][value="${defs.mode === 'full' ? 'all' : defs.mode || 'subscribe'}"]`);
      if (rangeRadio) { rangeRadio.checked = true; rangeRadio.dispatchEvent(new Event("change")); }
      // Fire the dirty-check explicitly so the Update button enables
      // after a Restore Defaults — only compress fires `change`
      // events, so other fields' programmatic value-writes never
      // reached the input/change listener (audit: editChannel L88).
      try { _checkEditChanges?.(); } catch {}
      window._showToast?.("Defaults restored.", "ok");
    });

    // Collect form state into a payload.
    const collectPayload = () => {
      const range = document.querySelector('input[name="edit-range"]:checked')?.value || "subscribe";
      const fromYear = document.getElementById("edit-date-year")?.value || "";
      const fromMonth = document.getElementById("edit-date-month")?.value || "";
      const fromDay = document.getElementById("edit-date-day")?.value || "";
      const from_date = fromYear ? `${fromYear.padStart(4, "0")}-${(fromMonth || "01").padStart(2, "0")}-${(fromDay || "01").padStart(2, "0")}` : "";
      const _folderVal = document.getElementById("edit-folder").value.trim();
      return {
        folder: _folderVal,
        url: document.getElementById("edit-url").value.trim(),
        folder_override: _folderVal,
        resolution: document.getElementById("edit-resolution").value,
        // parseInt(...) || 0 silently mapped NaN to 0 — clearing the
        // input was indistinguishable from "set to 0". Refuse to
        // persist invalid input and keep the user's previously-saved
        // value by omitting the field entirely when the input is
        // empty / non-numeric. (collectPayload's callers filter out
        // undefined fields before update_channel.)
        min_duration: (() => {
          const _v = document.getElementById("edit-min-dur").value;
          if (_v === "" || _v == null) return undefined;
          const _n = parseInt(_v, 10);
          return Number.isFinite(_n) && _n >= 0 ? _n : undefined;
        })(),
        max_duration: (() => {
          const _v = document.getElementById("edit-max-dur").value;
          if (_v === "" || _v == null) return undefined;
          const _n = parseInt(_v, 10);
          return Number.isFinite(_n) && _n >= 0 ? _n : undefined;
        })(),
        folder_org: document.getElementById("edit-folder-org").value,
        auto_transcribe: document.getElementById("edit-transcribe").checked,
        auto_metadata: document.getElementById("edit-metadata").checked,
        compress_enabled: document.getElementById("edit-compress").checked,
        compress_level: document.getElementById("edit-compress-quality")?.value || "Generous",
        compress_output_res: document.getElementById("edit-compress-res")?.value || "720",
        compress_batch_size: (function () {
          const _raw = parseInt(document.getElementById("edit-compress-batch")?.value, 10);
          return Number.isFinite(_raw) && _raw >= 0 ? _raw : 20;
        })(),
        range,
        from_date,
      };
    };

    let _editingIdentity = null;
    const _origOpenPanel = openPanel;
    const wrappedOpenPanel = (mode, channel) => {
      _origOpenPanel(mode, channel);
      _editingIdentity = (mode === "edit" && channel)
        ? { url: channel.url, name: channel.folder || channel.name }
        : null;
    };
    window._editChannelFromContext = (folder, urlGuess) => {
      // No URL fallback construction here. Previously the fallback
      // built a guessed `youtube.com/@{folder-without-spaces}` URL
      // that was indistinguishable from a real URL when persisted —
      // a folder name with `?`, `#`, `/`, or `..` produced a
      // malformed/invalid URL that silently saved into the channel
      // record. Leave url empty when we don't have a real one; the
      // edit panel surfaces the missing URL so the user pastes a
      // legitimate one before saving.
      const chan = { folder, url: urlGuess || "" };
      if (window.pywebview?.api?.subs_get_channel) {
        window.pywebview.api.subs_get_channel({ name: folder }).then(res => {
          const channel = (res && res.ok && res.channel) ? {
            ...res.channel,
            folder: res.channel.name || res.channel.folder,
          } : chan;
          wrappedOpenPanel("edit", channel);
        }).catch(() => wrappedOpenPanel("edit", chan));
      } else {
        wrappedOpenPanel("edit", chan);
      }
    };

    const _subsTbody = document.getElementById("subs-table-body");
    if (_subsTbody) {
      _subsTbody.addEventListener("dblclick", (e) => {
        const tr = e.target.closest("tr");
        if (!tr) return;
        const folder = tr.dataset.channelName
          || (tr.querySelector(".col-folder")?.textContent || "").trim();
        if (folder) window._editChannelFromContext(folder);
      });
    }

    update.addEventListener("click", async () => {
      // Disable the button during the in-flight subs_check_duplicate +
      // subs_update_channel/subs_add_channel sequence. Without this
      // guard a double-click fires two concurrent saves in parallel
      // — last-write-wins with potentially stale snapshot data on
      // the second, or duplicate "Channel added" toasts and DB rows
      // on the Add path. Snapshot _editingIdentity locally before
      // the awaits since closePanel() inside the success branch
      // nulls it, which would otherwise make the post-await toast
      // report the wrong action ("Channel added" vs "Channel
      // updated") if the user clicked Cancel mid-flight.
      if (update.disabled) return;
      update.disabled = true;
      const _savedIdentity = _editingIdentity;
      try {
      const payload = collectPayload();
      if (!payload.folder) { flashError("Folder name is required."); return; }
      if (!payload.url) { flashError("Channel URL is required."); return; }
      const api = window.pywebview?.api;
      if (!api) { flashError("Not running in native mode — writes disabled."); return; }

      if (api.subs_check_duplicate) {
        try {
          const dup = await api.subs_check_duplicate(
            payload.url, payload.folder, _editingIdentity || null);
          if (dup?.ok && (dup.dup_url || dup.dup_folder)) {
            const parts = [];
            if (dup.dup_url) parts.push(`• URL already used by:\n ${dup.dup_url}`);
            if (dup.dup_folder) parts.push(`• Folder name already taken by:\n ${dup.dup_folder}`);
            await window.askConfirm(
              "Duplicate channel",
              "This clashes with an existing subscription:\n\n" +
              parts.join("\n\n") +
              "\n\nResolve the conflict (change the URL or folder name) " +
              "and try again.",
              { confirm: "OK", noCancel: true });
            return;
          }
        } catch { /* if check fails, let the real add surface the error */ }
      }

      let res;
      try {
        if (_editingIdentity) {
          res = await api.subs_update_channel(_editingIdentity, payload);
        } else {
          res = await api.subs_add_channel(payload);
        }
      } catch (e) { flashError("Error: " + e); return; }
      // Defensive: subs_update_channel can return undefined on bridge
      // failures (audit: editChannel.js:565). Old `!res.ok` threw
      // TypeError on undefined and silently bailed without a toast.
      if (!res || !res.ok) {
        flashError(res?.error || "Unknown error");
        return;
      }
      if (res.write_blocked) {
        flashError("Saved in memory but disk write is gated.");
      } else {
        // Use the snapshot taken before the awaits — closePanel()
        // below nulls _editingIdentity, so this branch would
        // otherwise toast "Channel added" on an update if the user
        // cancelled during the in-flight call.
        flashOk(_savedIdentity ? "Channel updated." : "Channel added.");
      }
      await refreshSubsTable();
      closePanel();
      } finally {
        update.disabled = false;
      }
    });

    remove.addEventListener("click", async () => {
      if (!_editingIdentity) return;
      const res = await window._removeChannelWithPrompt(_editingIdentity.name);
      if (!res || !res.ok) return;
      await refreshSubsTable();
      closePanel();
    });

    async function refreshSubsTable() {
      const api = window.pywebview?.api;
      if (!api) return;
      try {
        const data = await api.get_subs_channels();
        if (Array.isArray(data) && data.length === 2) {
          window.renderSubsTable(data[0], data[1]);
          window._primeBrowse(data[0]);
        }
      } catch (e) { console.warn("refresh failed", e); }
    }
    window.refreshSubsTable = refreshSubsTable;

    // ── Conditional group visibility toggles ──
    const compressBox = document.getElementById("edit-compress");
    const compressGroup = document.getElementById("edit-compress-group");
    const syncCompressVis = () => {
      if (compressGroup) compressGroup.hidden = !compressBox?.checked;
    };
    compressBox?.addEventListener("change", syncCompressVis);
    syncCompressVis();

    const dateGroup = document.getElementById("edit-date-group");
    document.querySelectorAll('input[name="edit-range"]').forEach(r => {
      r.addEventListener("change", (e) => {
        if (dateGroup) dateGroup.hidden = (e.target.value !== "fromdate");
        if (e.target.value === "fromdate" && dateGroup) {
          document.getElementById("edit-date-year")?.focus();
        }
        try { _checkEditChanges(); } catch {}
      });
    });

    // Auto-advance between YYYY/MM/DD as user types digits
    const dateParts = [
      ["edit-date-year", 4, "edit-date-month"],
      ["edit-date-month", 2, "edit-date-day"],
      ["edit-date-day", 2, null],
    ];
    for (const [id, maxLen, nextId] of dateParts) {
      const el = document.getElementById(id);
      if (!el) continue;
      el.addEventListener("input", () => {
        el.value = el.value.replace(/\D/g, "");
        if (el.value.length >= maxLen && nextId) {
          document.getElementById(nextId)?.focus();
        }
      });
      el.addEventListener("keydown", (e) => {
        if (e.key === "Backspace" && el.value === "") {
          const prev = dateParts.find(([_, __, n]) => n === id);
          if (prev) document.getElementById(prev[0])?.focus();
        }
      });
    }
  }

  window.initEditChannelPanel = initEditChannelPanel;
})();
