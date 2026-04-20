/* ═══════════════════════════════════════════════════════════════════════
   app.js — tab switching, splitter, log-mode toggle, Python bridge wiring
   ═══════════════════════════════════════════════════════════════════════ */

(function () {
  "use strict";

  // ─── Dark askquestion dialog (replaces browser confirm/alert) ────────
  //
  // Usage:
  // const ok = await askQuestion({
  // title: "Remove channel",
  // message: "Are you sure?\n\nFiles on disk are not deleted.",
  // confirm: "Remove", cancel: "Cancel",
  // danger: true,
  // });
  window.askQuestion = function (opts) {
    return new Promise((resolve) => {
      const cfg = Object.assign({
        title: "Confirm",
        message: "",
        confirm: "OK",
        cancel: "Cancel",
        danger: false,
        noCancel: false,
      }, opts || {});

      const backdrop = document.createElement("div");
      backdrop.className = "askq-backdrop";
      backdrop.innerHTML = `
        <div class="askq-dialog">
          <div class="askq-header"></div>
          <div class="askq-body"></div>
          <div class="askq-buttons">
            ${cfg.noCancel ? "" : '<button class="btn btn-ghost" data-act="cancel"></button>'}
            <button class="btn ${cfg.danger ? "btn-danger" : "btn-primary"}" data-act="confirm"></button>
          </div>
        </div>
      `;
      backdrop.querySelector(".askq-header").textContent = cfg.title;
      backdrop.querySelector(".askq-body").textContent = cfg.message;
      backdrop.querySelector('[data-act="confirm"]').textContent = cfg.confirm;
      const cancelBtn = backdrop.querySelector('[data-act="cancel"]');
      if (cancelBtn) cancelBtn.textContent = cfg.cancel;
      document.body.appendChild(backdrop);

      const cleanup = (value) => {
        backdrop.style.animation = "askq-fade 0.12s ease-out reverse";
        setTimeout(() => backdrop.remove(), 120);
        document.removeEventListener("keydown", onKey);
        resolve(value);
      };

      backdrop.querySelector('[data-act="confirm"]').addEventListener("click", () => cleanup(true));
      if (cancelBtn) cancelBtn.addEventListener("click", () => cleanup(false));
      backdrop.addEventListener("click", (e) => {
        if (e.target === backdrop) cleanup(false);
      });

      const onKey = (e) => {
        if (e.key === "Escape") cleanup(false);
        if (e.key === "Enter") cleanup(true);
      };
      document.addEventListener("keydown", onKey);

      // Focus the default button
      setTimeout(() => backdrop.querySelector('[data-act="confirm"]').focus(), 30);
    });
  };

  // Shortcut helpers
  window.askConfirm = (title, message, opts) =>
    askQuestion(Object.assign({ title, message }, opts || {}));
  window.askDanger = (title, message, confirmLabel) =>
    askQuestion({ title, message, confirm: confirmLabel || "Remove", danger: true });

  // ─── Ready-gate ──────────────────────────────────────────────────────
  // Called from Python (evaluate_js) when Stage 1 of startup finishes.
  // Ungrays every [data-needs-ready] button — Sync Subbed, etc. Before
  // this fires, those buttons render with `disabled` set so clicks no-op.
  window._setReady = function (ready) {
    const on = !!ready;
    document.querySelectorAll("[data-needs-ready]").forEach(el => {
      if (on) {
        el.removeAttribute("disabled");
        el.classList.remove("is-locked-pre-ready");
      } else {
        el.setAttribute("disabled", "");
        el.classList.add("is-locked-pre-ready");
      }
    });
  };
  // Default: lock until Python says otherwise.
  document.addEventListener("DOMContentLoaded", () => {
    try { window._setReady(false); } catch (_e) {}
  });

  // ─── Multi-choice dialog (e.g. Metadata Already Downloaded) ──────────
  // Usage:
  // const choice = await askChoice({
  // title: "Metadata Already Downloaded",
  // message: "X already has metadata on disk. What do you want to do?",
  // choices: [
  // { label: "Skip existing", value: "skip", primary: true },
  // { label: "Overwrite", value: "overwrite" },
  // { label: "Append new only",value: "append" },
  // ],
  // });
  // → returns the chosen `value` or null if cancelled.
  // ─── Remove-channel two-step helper ─────────────────────────────────
  // Shared so every call site (edit-panel Remove button, Delete-key on
  // Subs row, channel-card right-click "Remove channel") gets the same
  // two-step flow: remove subscription → optional delete files on disk.
  // Returns {ok, deleted_folder, error?} or null if user cancelled.
  window._removeChannelWithPrompt = async function (name) {
    const api = window.pywebview?.api;
    if (!api?.subs_remove_channel) {
      window._showToast?.("Native mode required.", "warn");
      return null;
    }
    const ok1 = await window.askDanger(
      "Remove channel",
      `Remove channel "${name}" from your subscriptions?`,
      "Remove");
    if (!ok1) return null;
    // Step 2: optionally delete the on-disk folder too.
    const wantDelete = await window.askChoice({
      title: "Delete downloaded files?",
      message: `Also delete "${name}"'s downloaded videos, transcripts, ` +
               "metadata, and thumbnails from disk? This cannot be undone.",
      choices: [
        { label: "Delete files", value: "yes", kind: "danger" },
        { label: "Keep files", value: "no", kind: "primary" },
      ],
      cancel: "Back",
    });
    if (wantDelete === null) return null; // user cancelled
    const deleteFiles = wantDelete === "yes";
    let res;
    try {
      res = await api.subs_remove_channel({ name }, deleteFiles);
    } catch (e) {
      window._showToast?.("Remove failed: " + e, "error");
      return { ok: false, error: String(e) };
    }
    if (!res?.ok) {
      window._showToast?.(res?.error || "Remove failed.", "error");
      return res;
    }
    if (deleteFiles) {
      if (res.deleted_folder) {
        window._showToast?.(`Channel removed \u2014 files deleted from disk.`, "ok");
      } else if (res.delete_error) {
        window._showToast?.(
          `Channel removed, but file delete failed: ${res.delete_error}`,
          "warn");
      } else {
        window._showToast?.("Channel removed (no files found to delete).", "ok");
      }
    } else {
      window._showToast?.("Channel removed (files kept on disk).", "ok");
    }
    return res;
  };

  window.askChoice = function (opts) {
    return new Promise((resolve) => {
      const cfg = Object.assign({
        title: "Choose",
        message: "",
        choices: [],
        buttons: null, // alias for `choices` so callers can use either name
        cancel: "Cancel",
        cancelPlacement: "left", // "left" (default) or "right"
        countdownSecs: 0, // >0 → auto-pick primary after this many seconds
        countdownLabel: "", // optional prefix for the "...in Ns" line
      }, opts || {});
      // Accept `buttons` as an alias for `choices` (older callers pass buttons)
      const choices = (cfg.buttons && cfg.buttons.length) ? cfg.buttons : (cfg.choices || []);
      const backdrop = document.createElement("div");
      backdrop.className = "askq-backdrop";
      const buttonsHtml = choices.map((c, i) => {
        // Support both `kind: "primary"|"danger"|"ghost"` AND `primary: bool` / `danger: bool`
        const kind = c.kind || (c.primary ? "primary" : c.danger ? "danger" : "ghost");
        const cls = kind === "primary" ? "btn btn-primary"
                  : kind === "danger" ? "btn btn-danger"
                                       : "btn btn-ghost";
        return `<button class="${cls}" data-value="${escapeHtml(c.value)}">${escapeHtml(c.label)}</button>`;
      }).join("");
      const hasCountdown = cfg.countdownSecs > 0;
      const cancelBtn = '<button class="btn btn-ghost" data-act="cancel"></button>';
      const btnsInner = cfg.cancelPlacement === "right"
        ? `${buttonsHtml}${cancelBtn}`
        : `${cancelBtn}${buttonsHtml}`;
      backdrop.innerHTML = `
        <div class="askq-dialog">
          <div class="askq-header"></div>
          <div class="askq-body"></div>
          ${hasCountdown ? '<div class="askq-countdown" style="margin:8px 0 12px;font-size:12px;color:var(--c-dim);"></div>' : ""}
          <div class="askq-buttons">
            ${btnsInner}
          </div>
        </div>
      `;
      backdrop.querySelector(".askq-header").textContent = cfg.title;
      backdrop.querySelector(".askq-body").textContent = cfg.message;
      backdrop.querySelector('[data-act="cancel"]').textContent = cfg.cancel;
      document.body.appendChild(backdrop);

      let countdownTimer = null;
      const cleanup = (val) => {
        if (countdownTimer) { clearInterval(countdownTimer); countdownTimer = null; }
        backdrop.style.animation = "askq-fade 0.12s ease-out reverse";
        setTimeout(() => backdrop.remove(), 120);
        document.removeEventListener("keydown", onKey);
        resolve(val);
      };
      backdrop.querySelectorAll("[data-value]").forEach(b => {
        b.addEventListener("click", () => cleanup(b.dataset.value));
      });
      backdrop.querySelector('[data-act="cancel"]').addEventListener("click", () => cleanup(null));
      backdrop.addEventListener("click", (e) => { if (e.target === backdrop) cleanup(null); });
      const onKey = (e) => { if (e.key === "Escape") cleanup(null); };
      document.addEventListener("keydown", onKey);

      const primary = choices.find(c => c.primary || c.kind === "primary");
      if (primary) {
        setTimeout(() => backdrop.querySelector(`[data-value="${primary.value}"]`)?.focus(), 30);
      }

      // Mirrors YTArchiver.py:22030 whisper-model dialog — live-tick countdown
      // then auto-select the primary choice at zero. Any user interaction
      // (button click, Esc, focus on a button) cancels the auto-select via
      // the standard cleanup path.
      if (hasCountdown && primary) {
        let remaining = cfg.countdownSecs;
        const cdEl = backdrop.querySelector(".askq-countdown");
        const baseLabel = cfg.countdownLabel
          || `Auto-selecting ${primary.label} in`;
        const render = () => {
          if (cdEl) cdEl.textContent = `${baseLabel} ${remaining}s\u2026`;
        };
        render();
        countdownTimer = setInterval(() => {
          remaining -= 1;
          if (remaining <= 0) {
            cleanup(primary.value);
            return;
          }
          render();
        }, 1000);
      }
    });
  };

  // ─── Text input modal (for bookmark notes, rename prompts, etc.) ────
  // Usage:
  // const txt = await askTextInput({
  // title: "Add bookmark",
  // message: "At 2:45:",
  // placeholder: "Add a note (optional)",
  // confirm: "Save",
  // cancel: "Cancel",
  // initial: "",
  // allowEmpty: true,
  // });
  // Returns the entered string, or null if cancelled.
  window.askTextInput = function (opts) {
    return new Promise((resolve) => {
      const cfg = Object.assign({
        title: "Enter text",
        message: "",
        placeholder: "",
        initial: "",
        confirm: "Save",
        cancel: "Cancel",
        allowEmpty: false,
      }, opts || {});
      const backdrop = document.createElement("div");
      backdrop.className = "askq-backdrop";
      backdrop.innerHTML = `
        <div class="askq-dialog">
          <div class="askq-header"></div>
          <div class="askq-body"></div>
          <input type="text" class="askq-input" />
          <div class="askq-buttons">
            <button class="btn btn-ghost" data-act="cancel"></button>
            <button class="btn btn-primary" data-act="confirm"></button>
          </div>
        </div>
      `;
      backdrop.querySelector(".askq-header").textContent = cfg.title;
      backdrop.querySelector(".askq-body").textContent = cfg.message;
      const input = backdrop.querySelector(".askq-input");
      input.placeholder = cfg.placeholder || "";
      input.value = cfg.initial || "";
      backdrop.querySelector('[data-act="cancel"]').textContent = cfg.cancel;
      backdrop.querySelector('[data-act="confirm"]').textContent = cfg.confirm;
      document.body.appendChild(backdrop);

      const cleanup = (val) => {
        backdrop.style.animation = "askq-fade 0.12s ease-out reverse";
        setTimeout(() => backdrop.remove(), 120);
        document.removeEventListener("keydown", onKey);
        resolve(val);
      };
      const save = () => {
        const val = input.value || "";
        if (!cfg.allowEmpty && !val.trim()) { input.focus(); return; }
        cleanup(val);
      };
      backdrop.querySelector('[data-act="confirm"]').addEventListener("click", save);
      backdrop.querySelector('[data-act="cancel"]').addEventListener("click", () => cleanup(null));
      backdrop.addEventListener("click", (e) => { if (e.target === backdrop) cleanup(null); });
      const onKey = (e) => {
        if (e.key === "Escape") cleanup(null);
        else if (e.key === "Enter") { e.preventDefault(); save(); }
      };
      document.addEventListener("keydown", onKey);

      setTimeout(() => { input.focus(); input.select(); }, 30);
    });
  };

  // Expose so backend can trigger via window.evaluate_js(...)
  // Mirrors YTArchiver.py:26669 _metadata_choice_dialog — same three semantics:
  // "new" → Check for New (only fetch IDs not already on disk; fast)
  // "refresh" → Refresh Counts (re-hit every video to update view counts; slow)
  // "cancel" → Cancel (do nothing)
  //
  // The backend (`prompt_metadata_already_downloaded` in main.py) accepts
  // the string values "skip" / "overwrite" / "append" for back-compat, so we
  // pass through both the OLD-app names and the NEW shortcuts.
  window.askMetadataAlreadyDownloaded = async function (channelName, count) {
    const choice = await askChoice({
      title: "Metadata Already Downloaded",
      message: `"${channelName}" already has metadata for ${count} video(s) on disk.\n\n` +
               `Check for New: only fetch IDs we haven't seen yet (fast).\n` +
               `Refresh Counts: re-hit every existing video to update view counts (slow).`,
      buttons: [
        { label: "Check for New", value: "new", kind: "primary" },
        { label: "Refresh Counts", value: "refresh", kind: "ghost" },
        { label: "Cancel", value: "cancel", kind: "ghost" },
      ],
    });
    // Map OLD-app returns back to the back-compat strings the existing
    // Python sync pipeline expects.
    if (choice === "new") return "append";
    if (choice === "refresh") return "overwrite";
    return "skip";
  };

  // ─── Toast notifications ─────────────────────────────────────────────
  // Usage: window._showToast("message", "ok" | "error" | "warn")
  // Usage: window._showToast({ msg: "...", kind: "warn", action: {label, onClick}, ttlMs })
  window._showToast = function (msgOrOpts, kind) {
    const root = document.getElementById("toast-root");
    if (!root) return;
    const opts = typeof msgOrOpts === "string"
      ? { msg: msgOrOpts, kind }
      : (msgOrOpts || {});
    const ttl = opts.ttlMs ?? (opts.kind === "error" ? 4500 : 2500);
    const el = document.createElement("div");
    el.className = "toast " + (opts.kind || "");
    const msgEl = document.createElement("span");
    msgEl.textContent = opts.msg || "";
    el.appendChild(msgEl);
    if (opts.action) {
      const btn = document.createElement("button");
      btn.className = "toast-action";
      btn.textContent = opts.action.label || "Undo";
      btn.style.cssText = "margin-left:10px;background:transparent;border:none;color:var(--c-log-blue);cursor:pointer;text-decoration:underline;font-size:inherit;";
      btn.addEventListener("click", () => {
        try { opts.action.onClick?.(); } catch {}
        el.remove();
      });
      el.appendChild(btn);
    }
    root.appendChild(el);
    setTimeout(() => {
      el.style.transition = "opacity 0.25s, transform 0.25s";
      el.style.opacity = "0";
      el.style.transform = "translateX(20px)";
      setTimeout(() => el.remove(), 300);
    }, ttl);
  };

  // ─── Tab switching ───────────────────────────────────────────────────
  function initTabs() {
    const tabs = document.querySelectorAll(".tab");
    tabs.forEach(t => {
      t.addEventListener("click", () => {
        // If we're switching AWAY from Browse and a Watch-view <video>
        // is playing, PAUSE it (don't unload) — `display:none` on the
        // panel doesn't stop HTML5 audio on its own. Returning to
        // Browse should resume the user right where they left off
        // with the same video still loaded. "if the user
        // switches tabs while it's playing, pause playback" — but
        // the video needs to still be up when they come back, not
        // torn down into an empty placeholder.
        const currentlyActive = document.querySelector(".tab.active")?.dataset.tab;
        const target = t.dataset.tab;
        if (currentlyActive === "browse" && target !== "browse" &&
            typeof window._pauseWatchVideo === "function") {
          window._pauseWatchVideo();
        }
        tabs.forEach(x => x.classList.remove("active"));
        t.classList.add("active");
        document.querySelectorAll(".tab-panel").forEach(p => p.classList.remove("active"));
        const panel = document.getElementById("panel-" + target);
        if (panel) panel.classList.add("active");
      });
    });
  }

  // ─── Paned splitter (drag to resize activity log vs main log) ────────
  function initSplitter() {
    const splitter = document.getElementById("paned-splitter");
    const top = document.querySelector(".activity-log-frame");
    const container = document.getElementById("log-paned");
    if (!splitter || !top || !container) return;

    let dragging = false;
    let startY = 0;
    let startTopH = 0;

    splitter.addEventListener("mousedown", (e) => {
      dragging = true;
      startY = e.clientY;
      startTopH = top.getBoundingClientRect().height;
      document.body.style.cursor = "ns-resize";
      e.preventDefault();
    });

    window.addEventListener("mousemove", (e) => {
      if (!dragging) return;
      const dy = e.clientY - startY;
      const containerH = container.getBoundingClientRect().height;
      const splitterH = 6;
      const minTop = 32;
      const maxTop = containerH - 80 - splitterH;
      let newH = Math.max(minTop, Math.min(maxTop, startTopH + dy));
      top.style.flex = `0 0 ${newH}px`;
    });

    window.addEventListener("mouseup", () => {
      if (dragging) {
        dragging = false;
        document.body.style.cursor = "";
      }
    });
  }

  // ─── Browse sub-mode toggle + YouTube-style view flow ────────────────
  //
  // Sub-modes (left sidebar): Channels / Search / Graph / Bookmarks / Index
  // Within "Channels": channel grid \u2192 video grid \u2192 watch view
  const _browseState = {
    submode: "channels", // current sidebar mode
    view: "channels", // within Channels: channels|videos|watch
    channels: [], // source data
    currentChannel: null,
    videos: [],
    currentVideo: null,
  };

  function initBrowseSubmodes() {
    const buttons = document.querySelectorAll(".submode-btn");
    buttons.forEach(b => {
      b.addEventListener("click", () => {
        buttons.forEach(x => x.classList.remove("active"));
        b.classList.add("active");
        _browseState.submode = b.dataset.submode;
        browseNavigate();
      });
    });

    // Back button — channels → videos → watch unwind, with scroll-position
    // restoration per-view so clicking back to the channel grid lands on
    // the same card the user drilled out of. Matches YTArchiver.py:28167
    // _on_browse_back_to_grid (scope tracking + grid restore).
    //
    // When the Watch view was entered from somewhere OTHER than the
    // channel→videos→watch drilldown (e.g., double-clicking a row in
    // the Recent submode, clicking a search hit, jumping from a
    // bookmark), we record `_browseState.watchReturnTo` at entry time
    // and return there. Without that, Back from Recent → Watch would
    // `showView("videos")` with no `currentChannel` set, landing the
    // user on a blank video grid — this was reported
    window._browseGoBack = () => {
      if (_browseState.view === "watch") {
        const returnTo = _browseState.watchReturnTo;
        _browseState.watchReturnTo = null;
        if (returnTo === "videos" && _browseState.currentChannel) {
          // Normal channel-drill return path: back to the video grid.
          showView("videos");
          _restoreScroll("video-grid");
          return;
        }
        if (returnTo === "recent" || returnTo === "search" ||
            returnTo === "bookmarks" || returnTo === "graph") {
          // The user launched Watch from a Browse submode. Re-click
          // the submode button so the sidebar's `.active` state
          // updates at the same time the view swaps.
          const btn = document.querySelector(
            `.submode-btn[data-submode="${returnTo}"]`);
          if (btn) { btn.click(); return; }
        }
        // Fallbacks: prefer current-channel video grid if one's set;
        // otherwise drop to the Channels grid. Never leave the user
        // staring at a blank video-grid page with no channel loaded.
        if (_browseState.currentChannel) {
          showView("videos");
          _restoreScroll("video-grid");
        } else {
          showView("channels");
          _restoreScroll("channel-grid");
        }
      } else if (_browseState.view === "videos") {
        showView("channels");
        _restoreScroll("channel-grid", _browseState.currentChannel);
      }
    };
    document.getElementById("browse-back-btn")?.addEventListener("click", window._browseGoBack);

    // Mouse button 4 (back) / 5 (forward) — browser-style navigation
    // across the Browse tab. the user uses a 5-button mouse and expected
    // these to work like in a browser. `mouseup` event's `.button`
    // gives us 3 for back, 4 for forward on Windows; we listen on
    // `auxclick` which fires for non-primary buttons. `preventDefault`
    // blocks the browser's native gesture (which would try to navigate
    // the pywebview URL).
    //
    // Forward: re-enters the Watch view with the most recently viewed
    // video. Stored in `_browseState.lastWatched` when `_browseGoBack`
    // leaves Watch. If there's nothing to forward to, it's a no-op.
    window.addEventListener("mouseup", (e) => {
      if (e.button === 3) {
        // Back
        e.preventDefault();
        if (_browseState.view === "watch") {
          // Stash the current video so Forward can re-enter.
          _browseState.lastWatched = _browseState.currentVideo || null;
        }
        window._browseGoBack?.();
      } else if (e.button === 4) {
        // Forward — re-open the last Watch view we backed out of.
        e.preventDefault();
        const v = _browseState.lastWatched;
        if (v && typeof window._openVideoInWatch === "function") {
          _browseState.lastWatched = null;
          window._openVideoInWatch(v);
        }
      }
    });

    // Sort dropdown
    document.getElementById("browse-sort")?.addEventListener("change", (e) => {
      sortCurrentVideos(e.target.value);
    });

    // Group-by-year checkbox triggers a re-render with current sort
    document.getElementById("browse-group-year")?.addEventListener("change", () => {
      const sort = document.getElementById("browse-sort")?.value || "newest";
      sortCurrentVideos(sort);
    });
    // Group-by-month does the same, for channels organized yyyy/mm.
    document.getElementById("browse-group-month")?.addEventListener("change", () => {
      const sort = document.getElementById("browse-sort")?.value || "newest";
      sortCurrentVideos(sort);
    });

    // Filter input
    document.getElementById("browse-filter")?.addEventListener("input", (e) => {
      filterCurrentView(e.target.value);
    });
  }

  function browseNavigate() {
    const mode = _browseState.submode;
    // Leaving Watch via a submode click — pause the <video> element
    // before we hide it. `display:none` does NOT pause HTML5 media
    // on its own (a bug: mouse back from Watch kept audio
    // playing in the background). `showView` does this too but only
    // when called through its own path; submode clicks bypass
    // showView for the non-channels modes.
    if (_browseState.view === "watch" &&
        typeof window._stopWatchVideo === "function") {
      try { window._stopWatchVideo(); } catch (e) { /* noop */ }
    }
    // Hide all views
    document.querySelectorAll(".browse-view").forEach(v => v.style.display = "none");
    const toolbar = document.getElementById("browse-main-toolbar");
    const backBtn = document.getElementById("browse-back-btn");
    const sortWrap = document.getElementById("browse-sort-wrap");
    const title = document.getElementById("browse-main-title");
    const filter = document.getElementById("browse-filter");

    if (mode === "channels") {
      // Restart in channel grid
      _browseState.view = "channels";
      showView("channels");
    } else if (mode === "recent") {
      // Recent was moved from a top-level tab into a Browse submode
      // (users wanted all library navigation in one place). Reuses
      // the same #recent-table / #recent-table-body ids as before so
      // the existing render + click wiring keeps working.
      document.getElementById("view-recent").style.display = "";
      title.textContent = "Recent downloads";
      backBtn.style.display = "none";
      sortWrap.style.display = "none";
      if (filter) filter.style.display = "none"; // recent has its own filter
      _browseState.view = "recent";
    } else if (mode === "search") {
      document.getElementById("view-search").style.display = "";
      title.textContent = "Search transcripts";
      backBtn.style.display = "none";
      sortWrap.style.display = "none";
      if (filter) filter.placeholder = "Search \u2026";
    } else if (mode === "graph") {
      document.getElementById("view-graph").style.display = "";
      title.textContent = "Word frequency";
      backBtn.style.display = "none";
      sortWrap.style.display = "none";
      populateGraphChannels();
    } else if (mode === "bookmarks") {
      document.getElementById("view-bookmarks").style.display = "";
      title.textContent = "Bookmarks";
      backBtn.style.display = "none";
      sortWrap.style.display = "none";
      refreshBookmarks();
    } else if (mode === "index") {
      document.getElementById("view-index").style.display = "";
      title.textContent = "Archive index";
      backBtn.style.display = "none";
      sortWrap.style.display = "none";
    }
  }

  // Per-view scroll state — remembers `scrollTop` of each grid so the back
  // button lands the user where they left off, plus which card was
  // currently "focused" so we can scroll that one into view + flash it.
  const _scrollSaved = { "channel-grid": 0, "video-grid": 0 };

  function _saveScroll(gridId) {
    const el = document.getElementById(gridId);
    if (!el) return;
    // Walk up to the scrollable ancestor — channel-grid itself may not scroll.
    let s = el;
    while (s && s !== document.body) {
      if (s.scrollHeight > s.clientHeight + 4) break;
      s = s.parentElement;
    }
    if (s && s !== document.body) {
      _scrollSaved[gridId] = s.scrollTop;
    }
  }

  function _restoreScroll(gridId, focusItem) {
    const el = document.getElementById(gridId);
    if (!el) return;
    let s = el;
    while (s && s !== document.body) {
      if (s.scrollHeight > s.clientHeight + 4) break;
      s = s.parentElement;
    }
    // Give the view one paint cycle, then restore
    setTimeout(() => {
      if (s && s !== document.body) s.scrollTop = _scrollSaved[gridId] || 0;
      // If we have a focus target, flash it briefly so the user orients
      if (focusItem) {
        const name = focusItem.folder || focusItem.name || "";
        const card = [...el.querySelectorAll(".channel-card")].find(c =>
          (c.querySelector(".channel-card-name")?.textContent || "") === name);
        if (card) {
          card.classList.add("flash-hit");
          setTimeout(() => card.classList.remove("flash-hit"), 1400);
        }
      }
    }, 60);
  }

  // Pause + unload the Watch view's <video> element. Called whenever
  // the user navigates away from the Watch view — without this the
  // element keeps playing in the background because `display:none` does
  // NOT pause HTML5 media. users hit this: clicked Back, video kept
  // playing audio with no UI to stop it. Clearing `src` + calling
  // `load()` cuts the stream so the browser releases the file handle
  // too (matters for the local_fileserver 206-range requests).
  function _stopWatchVideo() {
    // Full teardown — used when the user is DONE watching (Back
    // button, Library sidebar click). Pauses + unloads the media
    // resource. After this, returning to the Watch view shows the
    // empty placeholder and the video needs to be re-opened.
    const vEl = document.getElementById("watch-video");
    if (!vEl) return;
    try { vEl.pause(); } catch (e) { /* noop */ }
    try {
      vEl.removeAttribute("src");
      // `load()` is what actually tears down the HTMLMediaElement's
      // internal resource; without it, `src=""` just blanks the URL
      // but the stream keeps buffering.
      if (typeof vEl.load === "function") vEl.load();
    } catch (e) { /* noop */ }
  }
  window._stopWatchVideo = _stopWatchVideo;

  function _pauseWatchVideo() {
    // Soft pause — used when the user navigates AWAY from Browse to
    // a different top-level tab while a video is loaded. Keeps src +
    // playhead intact so returning to Browse resumes right where they
    // left off. "pausing a video, going to a different tab,
    // then back to browse, closes the video ... it should still be
    // up (but if the user switches tabs while it's playing, pause)".
    const vEl = document.getElementById("watch-video");
    if (!vEl) return;
    try { vEl.pause(); } catch (e) { /* noop */ }
  }
  window._pauseWatchVideo = _pauseWatchVideo;

  function showView(viewName) {
    // Save scroll of outgoing view before swapping
    if (_browseState.view === "channels") _saveScroll("channel-grid");
    if (_browseState.view === "videos") _saveScroll("video-grid");
    // Leaving the Watch view — stop the backgrounded <video> element
    // so audio doesn't keep playing after Back / tab swap. Must happen
    // BEFORE we mutate _browseState.view so the "was in watch?" check
    // is accurate.
    if (_browseState.view === "watch" && viewName !== "watch") {
      _stopWatchVideo();
    }
    _browseState.view = viewName;
    document.querySelectorAll(".browse-view").forEach(v => v.style.display = "none");
    const title = document.getElementById("browse-main-title");
    const backBtn = document.getElementById("browse-back-btn");
    const sortWrap = document.getElementById("browse-sort-wrap");
    const filter = document.getElementById("browse-filter");

    if (viewName === "channels") {
      document.getElementById("view-channels").style.display = "";
      title.textContent = "Channels";
      backBtn.style.display = "none";
      sortWrap.style.display = "none";
      if (filter) { filter.placeholder = "Filter channels\u2026"; filter.value = ""; }
    } else if (viewName === "videos") {
      document.getElementById("view-videos").style.display = "";
      title.textContent = _browseState.currentChannel?.folder || "Videos";
      backBtn.style.display = "";
      sortWrap.style.display = "";
      if (filter) { filter.placeholder = "Filter videos\u2026"; filter.value = ""; }
    } else if (viewName === "watch") {
      document.getElementById("view-watch").style.display = "";
      title.textContent = _browseState.currentVideo?.title || "Watch";
      backBtn.style.display = "";
      sortWrap.style.display = "none";
      if (filter) filter.style.display = "none";
    }
    if (filter) filter.style.display = viewName === "watch" ? "none" : "";
  }

  // ─── Search viewer pane — shows context around a clicked hit ────────
  // Mirrors YTArchiver.py:29598 PanedWindow viewer. Loads N segments
  // before + hit + N segments after via `api.browse_search_context`, and
  // renders them with the hit highlighted. Up/Down "Load earlier / later"
  // buttons expand the window in chunks.
  const _searchViewerState = {
    segmentId: null,
    before: 30,
    after: 30,
    query: "",
    title: "",
  };

  async function _loadSearchViewer(resultRow, query) {
    const body = document.getElementById("search-viewer-body");
    const titleEl = document.getElementById("search-viewer-title");
    const metaEl = document.getElementById("search-viewer-meta");
    const bEarly = document.getElementById("search-viewer-earlier");
    const bLater = document.getElementById("search-viewer-later");
    if (!body || !titleEl) return;
    const api = window.pywebview?.api;
    if (!api?.browse_search_context) {
      body.innerHTML = '<div class="browse-empty">Viewer pane requires native mode.</div>';
      return;
    }
    _searchViewerState.segmentId = resultRow.segment_id;
    _searchViewerState.before = 30;
    _searchViewerState.after = 30;
    _searchViewerState.query = query || "";
    _searchViewerState.title = resultRow.title || "";
    _searchViewerState._videoId = resultRow.video_id || "";
    _searchViewerState._jsonlPath = resultRow.jsonl_path || "";
    _searchViewerState._channel = resultRow.channel || "";

    titleEl.textContent = resultRow.title || "(untitled)";
    metaEl.textContent = `${resultRow.channel || ""} \u00b7 ${_formatTs(resultRow.start_time)}`;
    body.innerHTML = '<div class="browse-empty">Loading\u2026</div>';

    let ctx;
    try {
      ctx = await api.browse_search_context({
        segment_id: resultRow.segment_id,
        before: _searchViewerState.before,
        after: _searchViewerState.after,
        query: _searchViewerState.query,
      });
    } catch (e) {
      body.innerHTML = `<div class="browse-empty">Error: ${escapeHtml(String(e))}</div>`;
      return;
    }
    if (!ctx?.ok) {
      body.innerHTML = `<div class="browse-empty">${escapeHtml(ctx?.error || "No context available.")}</div>`;
      return;
    }
    _renderSearchViewer(ctx, resultRow.segment_id);
    if (bEarly) bEarly.hidden = !ctx.before_more;
    if (bLater) bLater.hidden = !ctx.after_more;
  }

  function _renderSearchViewer(ctx, clickedId) {
    const body = document.getElementById("search-viewer-body");
    if (!body) return;
    body.innerHTML = "";
    const frag = document.createDocumentFragment();
    const q = _searchViewerState.query;
    const qWords = q ? q.toLowerCase().replace(/["*]/g, "").split(/\s+/).filter(Boolean) : [];
    let scrollTarget = null;
    for (const seg of (ctx.segments || [])) {
      const row = document.createElement("div");
      row.className = "sv-seg" + (seg.is_hit ? " hit" : "");
      if (seg.id === clickedId) row.classList.add("clicked");
      const tsEl = document.createElement("span");
      tsEl.className = "sv-ts";
      tsEl.textContent = `[${_formatTs(seg.s)}]`;
      const txtEl = document.createElement("span");
      txtEl.className = "sv-text";
      // Highlight query words inside the text for visual parity with the
      // snippet <mark> tags from the list side.
      if (qWords.length) {
        const esc = escapeHtml(seg.t || "");
        let html = esc;
        for (const w of qWords) {
          if (!w) continue;
          const re = new RegExp("(" + w.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + ")", "gi");
          html = html.replace(re, '<mark>$1</mark>');
        }
        txtEl.innerHTML = html;
      } else {
        txtEl.textContent = seg.t || "";
      }
      row.append(tsEl, txtEl);
      // Click a segment in the viewer → open in Watch view at that ts
      row.addEventListener("click", () => {
        _openSearchResultInWatch(_searchViewerState, seg);
      });
      if (seg.id === clickedId) scrollTarget = row;
      frag.appendChild(row);
    }
    body.appendChild(frag);
    if (scrollTarget) {
      setTimeout(() => {
        scrollTarget.scrollIntoView({ behavior: "instant", block: "center" });
      }, 20);
    }
  }

  function _openSearchResultInWatch(state, seg) {
    const api = window.pywebview?.api;
    if (!api?.browse_resolve_segment) return;
    api.browse_resolve_segment(state._jsonlPath || "",
                               state._videoId || "",
                               state.title || "").then((res) => {
      if (!res?.ok || !res.filepath) {
        window._showToast?.("Couldn't resolve source video.", "warn");
        return;
      }
      window._openVideoInWatch({
        filepath: res.filepath,
        title: state.title,
        channel: state._channel || res.channel || "",
        video_id: state._videoId || res.video_id || "",
        _seek_to: Number(seg.s) || 0,
      });
    }).catch(() => {});
  }

  function _initSearchViewerLoadMore() {
    const api = window.pywebview?.api;
    document.getElementById("search-viewer-earlier")?.addEventListener("click", async () => {
      if (!_searchViewerState.segmentId) return;
      _searchViewerState.before += 30;
      const ctx = await api.browse_search_context({
        segment_id: _searchViewerState.segmentId,
        before: _searchViewerState.before,
        after: _searchViewerState.after,
        query: _searchViewerState.query,
      });
      if (ctx?.ok) {
        _renderSearchViewer(ctx, _searchViewerState.segmentId);
        document.getElementById("search-viewer-earlier").hidden = !ctx.before_more;
        document.getElementById("search-viewer-later").hidden = !ctx.after_more;
      }
    });
    document.getElementById("search-viewer-later")?.addEventListener("click", async () => {
      if (!_searchViewerState.segmentId) return;
      _searchViewerState.after += 30;
      const ctx = await api.browse_search_context({
        segment_id: _searchViewerState.segmentId,
        before: _searchViewerState.before,
        after: _searchViewerState.after,
        query: _searchViewerState.query,
      });
      if (ctx?.ok) {
        _renderSearchViewer(ctx, _searchViewerState.segmentId);
        document.getElementById("search-viewer-earlier").hidden = !ctx.before_more;
        document.getElementById("search-viewer-later").hidden = !ctx.after_more;
      }
    });
  }

  // ─── Un-indexed warning banner (Search + Graph views) ───────────────
  // Fetches the count of transcript files on disk that aren't in the FTS
  // index, shows/hides the amber banner accordingly. Mirrors
  // YTArchiver.py:24756 _update_index_warning.
  async function _refreshUnindexedWarning() {
    const api = window.pywebview?.api;
    if (!api?.index_unindexed_count) return;
    let res;
    try { res = await api.index_unindexed_count(); } catch { return; }
    if (!res?.ok) return;
    const n = Number(res.unindexed) || 0;
    const show = n > 0;
    for (const pair of [
      ["search-unindexed-warning", "search-unindexed-text"],
      ["graph-unindexed-warning", "graph-unindexed-text"],
    ]) {
      const banner = document.getElementById(pair[0]);
      const txt = document.getElementById(pair[1]);
      if (!banner) continue;
      banner.hidden = !show;
      if (txt) {
        txt.textContent = show
          ? `${n.toLocaleString()} transcript file${n === 1 ? "" : "s"} on disk ` +
            "aren't yet in the search index. Results + graph may be incomplete " +
            "until you rescan."
          : "";
      }
    }
  }
  window._refreshUnindexedWarning = _refreshUnindexedWarning;

  // Wire the "Rescan now" buttons on each banner to fire archive_rescan.
  function _initUnindexedRescanBtns() {
    const handler = async (e) => {
      e.preventDefault();
      const api = window.pywebview?.api;
      if (!api?.archive_rescan) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      window._showToast?.("Rescanning archive for new transcripts\u2026", "ok");
      try {
        await api.archive_rescan();
        // Poll until the rescan clears the unindexed count or times out.
        let tries = 0;
        const tick = async () => {
          tries++;
          await _refreshUnindexedWarning();
          const banner = document.getElementById("search-unindexed-warning");
          if (banner && !banner.hidden && tries < 60) {
            setTimeout(tick, 2000);
          }
        };
        setTimeout(tick, 1500);
      } catch (err) {
        window._showToast?.("Rescan failed.", "error");
      }
    };
    document.getElementById("search-rescan-btn")?.addEventListener("click", handler);
    document.getElementById("graph-rescan-btn")?.addEventListener("click", handler);
  }

  // ─── Browse > Search sub-mode ────────────────────────────────────────
  function initSearchView() {
    _initUnindexedRescanBtns();
    _initSearchViewerLoadMore();
    // Fire once on first Search-view click; refresh when user re-enters.
    document.querySelector('[data-view="search"]')?.addEventListener("click", _refreshUnindexedWarning);
    document.querySelector('[data-view="graph"]')?.addEventListener("click", _refreshUnindexedWarning);
    // And once on boot so the banner is correct right away.
    setTimeout(_refreshUnindexedWarning, 800);

    const input = document.getElementById("search-query");
    const scope = document.getElementById("search-scope");
    const btn = document.getElementById("btn-search-run");
    const results = document.getElementById("search-results");
    const counter = document.getElementById("search-count");
    const doSearch = async () => {
      const q = (input?.value || "").trim();
      if (!q) {
        results.innerHTML = '<div class="browse-empty">Type a query and press Search or Enter.</div>';
        counter.textContent = "\u2014";
        return;
      }
      results.innerHTML = '<div class="search-progress-bar" id="search-progress-bar"></div>' +
                          '<div class="browse-empty">Searching\u2026</div>';
      counter.textContent = "\u2026";
      const api = window.pywebview?.api;
      if (!api?.browse_search) {
        results.innerHTML = '<div class="browse-empty">Search requires native mode.</div>';
        return;
      }
      const chan = (scope?.value === "channel")
        ? (_browseState.currentChannel?.folder || null) : null;
      try {
        const rows = await api.browse_search(q, chan, 200);
        if (!Array.isArray(rows) || rows.length === 0 || (rows[0] && rows[0].error)) {
          const errMsg = (rows && rows[0] && rows[0].error) ? `Search error: ${rows[0].error}` : "No matches.";
          results.innerHTML = `<div class="browse-empty">${escapeHtml(errMsg)}</div>`;
          counter.textContent = "0 matches";
          return;
        }
        counter.textContent = `${rows.length.toLocaleString()} matches`;
        const frag = document.createDocumentFragment();
        for (const r of rows) {
          const row = document.createElement("div");
          row.className = "search-result";
          row.innerHTML = `
            <div class="search-result-head">
              <span class="search-result-title"></span>
              <span class="search-result-meta"></span>
            </div>
            <div class="search-result-snippet"></div>
          `;
          row.querySelector(".search-result-title").textContent = r.title || "(untitled)";
          row.querySelector(".search-result-meta").textContent =
            `${r.channel || ""} \u00b7 ${_formatTs(r.start_time)}`;
          // Snippet comes with <mark> tags from FTS5 — safe to inject
          row.querySelector(".search-result-snippet").innerHTML = r.snippet || escapeHtml(r.text || "");
          row.title = "Double-click to open in Watch view at this timestamp";
          const openHit = async () => {
            // Resolve the real video file (sibling of the jsonl_path) and
            // build a Watch view that can actually play. Falls back to the
            // transcript-only view when the video file is missing.
            let video = {
              title: r.title, channel: r.channel, video_id: r.video_id,
              start_at: Number(r.start_time) || 0,
            };
            try {
              const res = await api.browse_resolve_segment?.(
                r.jsonl_path || "", r.video_id || "", r.title || "");
              if (res?.ok && res.filepath) {
                video.filepath = res.filepath;
                if (res.channel) video.channel = res.channel;
              }
            } catch { /* leave video.filepath undefined */ }
            _browseState.currentVideo = video;
            showView("watch");
            try {
              const res = await api.browse_get_transcript({
                video_id: r.video_id, jsonl_path: r.jsonl_path, title: video.title || "",
              });
              const segs = Array.isArray(res)
                ? res
                : (res?.segments || []);
              const sourceInfo = (res && !Array.isArray(res)) ? (res.source || null) : null;
              const rendered = segs.map(seg => ({
                ts: _formatTs(seg.s), text: seg.t, words: seg.w, s: seg.s, e: seg.e,
              }));
              window.renderWatchView(video, rendered, sourceInfo);
              // Seek the <video> to the hit's start_time
              const vEl = document.getElementById("watch-video");
              if (vEl && video.start_at > 0) {
                const seek = () => {
                  try { vEl.currentTime = video.start_at; vEl.play?.().catch(() => {}); }
                  catch { /* ignore */ }
                };
                if (vEl.readyState >= 1) seek();
                else vEl.addEventListener("loadedmetadata", seek, { once: true });
              }
              // Scroll transcript to the clicked segment. Uses the
              // container-local scroll helper so we don't also scroll
              // the outer .browse-view (which would push the video
              // out of frame).
              setTimeout(() => {
                const tr = document.getElementById("watch-transcript");
                if (!tr) return;
                const segs = tr.querySelectorAll(".seg");
                for (const s of segs) {
                  const ts = s.querySelector(".timestamp")?.textContent || "";
                  if (ts && ts.includes(_formatTs(r.start_time))) {
                    window._scrollTranscriptTo?.(tr, s);
                    s.classList.add("search-hit");
                    break;
                  }
                }
              }, 80);
            } catch (e) { console.warn(e); }
          };
          // Single-click → load context in the right-side viewer pane
          // (stay on the Search view). Double-click → open in Watch view.
          row.addEventListener("click", () => {
            results.querySelectorAll(".search-result.selected")
                   .forEach(x => x.classList.remove("selected"));
            row.classList.add("selected");
            _loadSearchViewer(r, q);
          });
          row.addEventListener("dblclick", (e) => { e.preventDefault(); openHit(); });
          frag.appendChild(row);
        }
        results.innerHTML = "";
        results.appendChild(frag);
      } catch (e) {
        results.innerHTML = `<div class="browse-empty">Search failed: ${escapeHtml(String(e))}</div>`;
      }
    };
    btn?.addEventListener("click", doSearch);
    input?.addEventListener("keydown", (e) => { if (e.key === "Enter") doSearch(); });

    // FTS5 operator buttons — click-to-insert at the current cursor position
    document.querySelectorAll(".search-op-btn").forEach(opBtn => {
      opBtn.addEventListener("click", (e) => {
        e.preventDefault();
        if (!input) return;
        const op = opBtn.dataset.op || "";
        const start = input.selectionStart ?? input.value.length;
        const end = input.selectionEnd ?? input.value.length;
        const before = input.value.slice(0, start);
        const selected = input.value.slice(start, end);
        const after = input.value.slice(end);
        let insert;
        if (op === '"…"') {
          // Wrap selection (if any) in double quotes for exact phrase search
          insert = selected ? `"${selected}"` : '""';
        } else if (op === "*") {
          // Append a prefix-match wildcard to the word at cursor
          insert = "*";
        } else {
          // Boolean op — pad with spaces
          insert = `${before.endsWith(" ") ? "" : " "}${op} `;
        }
        const newVal = before + (op === '"…"' ? "" : selected) + insert + after;
        input.value = newVal;
        // Position cursor sensibly
        const newPos = op === '"…"'
          ? start + 1 + selected.length
          : before.length + insert.length + selected.length;
        input.setSelectionRange(newPos, newPos);
        input.focus();
      });
    });
  }

  function escapeHtml(s) {
    return String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;")
      .replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }

  // ─── Auto-sync dropdown + countdown ──────────────────────────────────
  function initAutorun() {
    const sel = document.getElementById("auto-sync-select");
    const cd = document.getElementById("autorun-countdown");
    if (!sel) return;
    // Restore saved interval + push countdown
    const tick = async () => {
      const api = window.pywebview?.api;
      if (!api?.autorun_state) return;
      try {
        const st = await api.autorun_state();
        if (!st) return;
        if (sel.value !== st.label) sel.value = st.label;
        if (cd) {
          // Three states, matching classic's _tick_countdown:
          // - Sync running → "waiting for queue..." (countdown paused)
          // - Countdown active → "next in Xm Ys"
          // - Off / idle → empty
          if (st.mins > 0 && st.waiting_for_sync) {
            cd.textContent = "waiting for queue\u2026";
          } else if (st.mins > 0 && st.seconds_remaining != null) {
            cd.textContent = `next in ${_fmtRemain(st.seconds_remaining)}`;
          } else {
            cd.textContent = "";
          }
        }
      } catch (e) { /* ignore */ }
    };
    sel.addEventListener("change", async () => {
      const api = window.pywebview?.api;
      if (!api?.autorun_set) return;
      try {
        await api.autorun_set(sel.value);
        window._showToast?.(sel.value === "Off" ? "Auto-sync off." : `Auto-sync every ${sel.value}.`, "ok");
        tick();
      } catch (e) { window._showToast?.("Error: " + e, "error"); }
    });
    tick();
    setInterval(tick, 1000); // update the countdown every second
  }

  function _fmtRemain(sec) {
    if (sec <= 0) return "now";
    if (sec < 60) return `${sec}s`;
    if (sec < 3600) return `${Math.floor(sec/60)}m ${sec%60}s`;
    return `${Math.floor(sec/3600)}h ${Math.floor((sec%3600)/60)}m`;
  }

  // ─── Deferred livestreams drawer ─────────────────────────────────────
  async function refreshDeferredLivestreams() {
    const api = window.pywebview?.api;
    const wrap = document.getElementById("deferred-livestreams");
    const list = document.getElementById("deferred-list");
    const count = document.getElementById("deferred-count");
    if (!wrap || !list) return;
    if (!api?.livestreams_list) { wrap.hidden = true; return; }
    try {
      const res = await api.livestreams_list();
      const items = res?.items || [];
      if (!items.length) { wrap.hidden = true; return; }
      wrap.hidden = false;
      if (count) count.textContent = `${items.length}`;
      list.innerHTML = "";
      for (const it of items) {
        const row = document.createElement("div");
        row.className = "deferred-row";
        row.innerHTML = `
          <span class="deferred-id"></span>
          <span class="deferred-title"></span>
          <button data-drop title="Forget this one">&times;</button>
        `;
        row.querySelector(".deferred-id").textContent = it.video_id;
        row.querySelector(".deferred-title").textContent = it.title
          ? ` \u2014 ${it.title.slice(0, 60)}` : "";
        row.querySelector("[data-drop]").addEventListener("click", async () => {
          await api.livestreams_drop(it.video_id);
          refreshDeferredLivestreams();
        });
        list.appendChild(row);
      }
    } catch (e) { console.warn("deferred:", e); }
  }

  function initDeferredLivestreams() {
    document.getElementById("btn-deferred-refresh")?.addEventListener("click", () => {
      window.pywebview?.api?.sync_start_all?.();
      window._showToast?.("Retrying deferred livestreams via Sync Subbed.", "ok");
    });
    document.getElementById("btn-deferred-clear")?.addEventListener("click", async () => {
      const ok = await askDanger(
        "Clear deferred livestreams",
        "Forget every deferred livestream in the journal?\n\nThis doesn't delete any files.", "Clear");
      if (!ok) return;
      const api = window.pywebview?.api;
      const r = await api?.livestreams_list?.();
      for (const it of (r?.items || [])) {
        await api?.livestreams_drop?.(it.video_id);
      }
      refreshDeferredLivestreams();
    });
    refreshDeferredLivestreams();
    setInterval(refreshDeferredLivestreams, 30_000);
  }

  // ─── Last Full Sync live label ───────────────────────────────────────
  function initLastSyncTicker() {
    const el = document.getElementById("last-full-sync");
    if (!el) return;
    const tick = async () => {
      const api = window.pywebview?.api;
      if (!api?.get_last_sync_label) return;
      try {
        const r = await api.get_last_sync_label();
        if (r?.label) el.textContent = r.label;
      } catch (e) { /* ignore */ }
    };
    tick();
    setInterval(tick, 60_000); // update every minute
  }

  // ─── Splitter position persistence ───────────────────────────────────
  function persistSplitterOnResize() {
    const top = document.querySelector(".activity-log-frame");
    if (!top) return;
    let saveTimer = null;
    const obs = new ResizeObserver(() => {
      clearTimeout(saveTimer);
      saveTimer = setTimeout(() => {
        const h = top.getBoundingClientRect().height;
        window.pywebview?.api?.window_state_save?.({ splitter_top_px: Math.round(h) });
      }, 400);
    });
    obs.observe(top);
    // Apply saved height on load
    window.pywebview?.api?.window_state_load?.().then((st) => {
      if (st?.splitter_top_px && top) top.style.flex = `0 0 ${st.splitter_top_px}px`;
    }).catch(() => {});
  }

  // ─── Column width persistence (Subs + Recent) ───────────────────────
  function persistColumnWidths() {
    // Wire resize handles immediately — these don't need the API.
    // Since HTML tables don't natively support col drag-resize, add a
    // simple mousedown-on-th-border handler that updates <col> width.
    _wireColResize(".subs-table", "subs");
    _wireColResize(".recent-table", "recent");

    // Apply saved widths — needs pywebview.api, which may not be ready
    // at DOMContentLoaded. Retry on `pywebviewready` + poll fallback to
    // cover the case where the event already fired before we listened.
    // Without this, saved widths were silently dropped on every boot
    // (reported: Subs column widths don't persist across restart).
    const applySaved = () => {
      const api = window.pywebview?.api;
      if (!api?.window_state_load) return false;
      api.window_state_load().then((st) => {
        if (!st || !st.col_widths) return;
        _applyColWidths(".subs-table", st.col_widths.subs);
        _applyColWidths(".recent-table", st.col_widths.recent);
      }).catch(() => {});
      return true;
    };
    if (!applySaved()) {
      window.addEventListener("pywebviewready", () => { applySaved(); },
                              { once: true });
      // Belt-and-suspenders: poll briefly in case `pywebviewready` fired
      // before this listener was attached (same pattern as queue-auto).
      let tries = 0;
      const poll = () => {
        if (applySaved()) return;
        if (++tries < 20) setTimeout(poll, 150);
      };
      setTimeout(poll, 150);
    }
  }

  function _applyColWidths(tableSel, widths) {
    if (!widths) return;
    const table = document.querySelector(tableSel);
    if (!table) return;
    const cols = table.querySelectorAll("colgroup col");
    const ths = table.querySelectorAll("thead th");
    ths.forEach((th, i) => {
      const key = th.dataset.sort || th.textContent.trim().toLowerCase();
      if (widths[key] && cols[i]) cols[i].style.width = widths[key] + "px";
    });
  }

  function _wireColResize(tableSel, saveKey) {
    const table = document.querySelector(tableSel);
    if (!table) return;
    const ths = table.querySelectorAll("thead th");

    // Shared flag — when set, the next `click` on any th is swallowed so
    // a resize-drag doesn't also trigger a column sort. User feedback: // resize "acts weird" because sort fires alongside.
    if (!table._resizeState) {
      table._resizeState = { suppressNextSortClick: false };
      table.addEventListener("click", (e) => {
        if (table._resizeState.suppressNextSortClick && e.target.closest("th")) {
          e.stopImmediatePropagation();
          e.preventDefault();
          table._resizeState.suppressNextSortClick = false;
        }
      }, true); // capture phase so we win before sort's bubble-phase handler
    }

    ths.forEach((th, i) => {
      // Add a grab handle on the right edge of the header cell.
      // Skip the last column — nowhere for its extra width to go with
      // `table-layout: fixed` + `overflow-x: hidden`.
      if (th.querySelector(".col-resizer")) return;
      if (i === ths.length - 1) return;
      const handle = document.createElement("div");
      handle.className = "col-resizer";
      th.style.position = "relative";
      th.appendChild(handle);
      let startX = 0, startW = 0, startNextW = 0;
      handle.addEventListener("mousedown", (e) => {
        e.stopPropagation();
        e.preventDefault();
        startX = e.clientX;
        const cols = table.querySelectorAll("colgroup col");
        const thisCol = cols[i];
        const nextCol = cols[i + 1];
        if (!thisCol || !nextCol) return;
        // Seed from the current rendered widths so dragging feels accurate
        // even if this is the first time the user touches the divider.
        const ths2 = table.querySelectorAll("thead th");
        startW = ths2[i] ?.getBoundingClientRect().width || 100;
        startNextW = ths2[i + 1]?.getBoundingClientRect().width || 100;
        document.body.style.cursor = "col-resize";
        table._resizeState.suppressNextSortClick = true;
        const move = (ev) => {
          let dw = ev.clientX - startX;
          // Clamp so neither this nor the next column goes below 40px.
          dw = Math.max(-(startW - 40), Math.min(startNextW - 40, dw));
          thisCol.style.width = (startW + dw) + "px";
          nextCol.style.width = (startNextW - dw) + "px";
        };
        const up = () => {
          document.removeEventListener("mousemove", move);
          document.removeEventListener("mouseup", up);
          document.body.style.cursor = "";
          // Save widths — matches YTArchiver.py chan_col_widths persistence.
          const widths = {};
          table.querySelectorAll("thead th").forEach((t, idx) => {
            const key = t.dataset.sort || t.textContent.trim().toLowerCase();
            const col = table.querySelectorAll("colgroup col")[idx];
            if (col && col.style.width) {
              widths[key] = parseInt(col.style.width);
            }
          });
          const api = window.pywebview?.api;
          if (api?.window_state_save) {
            api.window_state_load().then((st) => {
              const cw = (st && st.col_widths) || {};
              cw[saveKey] = widths;
              api.window_state_save({ col_widths: cw });
            });
          }
        };
        document.addEventListener("mousemove", move);
        document.addEventListener("mouseup", up);
      });
      // Also swallow click on the handle itself to keep sort inert.
      handle.addEventListener("click", (e) => {
        e.stopPropagation();
        e.preventDefault();
      });
    });
  }

  // ─── Browse > Graph sub-mode (word frequency + word cloud) ───────────
  // Mirrors YTArchiver.py Graph/Frequency sub-mode: 3 chart types (Line,
  // Bar, Word Cloud), per-1000-segments Normalize toggle, click-graph-to-
  // drill-into-search, and CSV export of the plot data.
  let _graphChart = null;
  let _graphLastData = null; // last-rendered { labels, values | series, bucket, word, channel }
  let _graphType = "line"; // current chart type

  function initGraphView() {
    const btn = document.getElementById("btn-graph-run");
    if (btn) btn.addEventListener("click", drawGraph);
    // Chart-type buttons
    const typeBtns = document.querySelectorAll(".chart-type-btn");
    typeBtns.forEach(b => {
      b.addEventListener("click", () => {
        typeBtns.forEach(x => x.classList.remove("active"));
        b.classList.add("active");
        _graphType = b.dataset.type || "line";
        // Re-render from cached data if we have any
        if (_graphLastData) drawGraphFromData(_graphLastData);
      });
    });
    // Normalize toggle re-runs the query (the divisor is server-side).
    document.getElementById("graph-normalize")?.addEventListener("change", () => {
      if ((document.getElementById("graph-word")?.value || "").trim()) {
        drawGraph();
      }
    });
    // CSV export
    document.getElementById("btn-graph-export-csv")?.addEventListener("click", _exportGraphCsv);
  }

  function _graphDestroy() {
    if (_graphChart) {
      try { _graphChart.destroy(); } catch {}
      _graphChart = null;
    }
  }

  async function drawGraph() {
    const wordEl = document.getElementById("graph-word");
    const chanEl = document.getElementById("graph-channel");
    const bucketEl = document.getElementById("graph-bucket");
    const emptyEl = document.getElementById("graph-empty");
    const canvas = document.getElementById("graph-canvas");
    if (!canvas || !wordEl) return;

    const word = (wordEl.value || "").trim();
    const bucket = bucketEl?.value || "month";
    const channel = (chanEl && chanEl.value !== "All") ? chanEl.value : null;
    const normalize = !!document.getElementById("graph-normalize")?.checked;

    // Word Cloud doesn't require a word — it's "what are the most-spoken
    // words overall?". Line/Bar both need a word.
    if (_graphType !== "wordcloud" && !word) {
      window._showToast?.("Enter a word to plot.", "warn");
      return;
    }

    const api = window.pywebview?.api;
    if (!api) {
      if (emptyEl) emptyEl.textContent = "Graph requires native mode.";
      return;
    }

    if (emptyEl) emptyEl.textContent = "Querying\u2026";

    // Word Cloud path
    if (_graphType === "wordcloud") {
      if (!api.browse_word_cloud) {
        if (emptyEl) emptyEl.textContent = "Word cloud backend not wired yet.";
        return;
      }
      let cloud;
      try { cloud = await api.browse_word_cloud(channel, 120); }
      catch (e) { if (emptyEl) emptyEl.textContent = "Error: " + e; return; }
      if (!cloud?.ok || !Array.isArray(cloud.words) || !cloud.words.length) {
        if (emptyEl) emptyEl.textContent = cloud?.error || "No words found.";
        return;
      }
      if (emptyEl) emptyEl.textContent = "";
      _renderWordCloud(cloud.words);
      _graphLastData = { cloud: cloud.words, channel };
      return;
    }

    // Line / Bar path
    if (!api.browse_graph) {
      if (emptyEl) emptyEl.textContent = "Graph requires native mode.";
      return;
    }
    let data;
    try { data = await api.browse_graph(word, channel, bucket, normalize); }
    catch (e) { if (emptyEl) emptyEl.textContent = "Error: " + e; return; }
    if (data?.error) { if (emptyEl) emptyEl.textContent = data.error; return; }
    if (!data?.labels?.length) {
      if (emptyEl) emptyEl.textContent = `No occurrences found.`;
      _graphDestroy();
      return;
    }
    if (emptyEl) emptyEl.textContent = "";

    _graphLastData = Object.assign({}, data,
      { word, channel, bucket, normalize });
    drawGraphFromData(_graphLastData);
  }

  function drawGraphFromData(data) {
    if (!data) return;
    const canvas = document.getElementById("graph-canvas");
    if (!canvas) return;
    if (typeof Chart === "undefined") return;

    // Word cloud uses its own renderer, not Chart.js
    if (_graphType === "wordcloud") {
      _renderWordCloud(data.cloud || []);
      return;
    }
    _hideWordCloud();

    _graphDestroy();
    const palette = ["#6cb4ee", "#e87aac", "#3dd68c", "#c7e64f",
                     "#c084fc", "#ff8c42", "#38d9e0"];

    let datasets;
    if (Array.isArray(data.series) && data.series.length) {
      datasets = data.series.map((s, i) => ({
        label: `"${s.word}"`,
        data: s.values,
        borderColor: palette[i % palette.length],
        backgroundColor: _graphType === "bar"
          ? palette[i % palette.length] + "aa"
          : palette[i % palette.length] + "28",
        tension: 0.25,
        fill: _graphType === "line",
        pointRadius: 3,
        pointBackgroundColor: palette[i % palette.length],
      }));
    } else {
      datasets = [{
        label: `"${data.word}"${data.channel ? " in " + data.channel : ""}` +
               (data.normalize ? " (per 1k)" : ""),
        data: data.values,
        borderColor: "#6cb4ee",
        backgroundColor: _graphType === "bar"
          ? "rgba(108, 180, 238, 0.75)"
          : "rgba(108, 180, 238, 0.15)",
        tension: 0.25,
        fill: _graphType === "line",
        pointRadius: 3,
        pointBackgroundColor: "#6cb4ee",
      }];
    }

    _graphChart = new Chart(canvas.getContext("2d"), {
      type: _graphType,
      data: { labels: data.labels, datasets: datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        // Deferred resize prevents Chart.js from getting stuck at
        // height=0 when the browser throttles layout during window
        // drag. graph "collapses and never recovers" when the
        // window is squished narrow.
        resizeDelay: 50,
        animation: { duration: 300 },
        scales: {
          x: { ticks: { color: "#a0aabb" }, grid: { color: "#2a2c30" } },
          y: { ticks: { color: "#a0aabb" }, grid: { color: "#2a2c30" }, beginAtZero: true,
               title: {
                 display: !!data.normalize,
                 text: data.normalize ? "matches per 1000 segments" : "",
                 color: "#a0aabb",
               } },
        },
        plugins: {
          legend: { labels: { color: "#dde1e8" } },
          tooltip: { backgroundColor: "#1c1e21",
                     titleColor: "#dde1e8", bodyColor: "#dde1e8" },
        },
        // Click-to-drill: clicking a data point jumps to Search tab with
        // the word + the bucket's date range filled in.
        onClick: (evt, els) => {
          if (!els || !els.length) return;
          const idx = els[0].index;
          const label = data.labels[idx];
          const word = data.word || (data.series?.[els[0].datasetIndex]?.word) || "";
          _drillIntoSearch(word, label, data.bucket || "month", data.channel);
        },
      },
    });
    // Belt-and-suspenders: Chart.js's built-in ResizeObserver occasionally
    // fails to re-measure after the container shrinks to a very small
    // size and then grows back. We attach our OWN observer on the
    // wrapper and explicitly call chart.resize() whenever its rect
    // changes. Wrapped in rAF so rapid drag events coalesce into one
    // resize per frame. Also listens on window resize as a fallback
    // for environments where ResizeObserver misfires entirely.
    try {
      const wrap = canvas.parentElement;
      if (wrap && !wrap._graphResizeObs) {
        let raf = 0;
        const kick = () => {
          cancelAnimationFrame(raf);
          raf = requestAnimationFrame(() => {
            try { _graphChart && _graphChart.resize(); } catch {}
          });
        };
        if (typeof ResizeObserver === "function") {
          const ro = new ResizeObserver(kick);
          ro.observe(wrap);
          wrap._graphResizeObs = ro;
        }
        // Window-resize as a second safety net
        window.addEventListener("resize", kick);
      }
    } catch (e) { /* resize-safety is best-effort */ }
  }

  // Switch to the Search view and pre-fill the query + year range so the
  // user sees the exact segments behind a clicked graph point. Mirrors
  // YTArchiver.py:30507 _on_graph_click.
  function _drillIntoSearch(word, bucketLabel, bucket, channel) {
    if (!word) return;
    // Activate the Search sub-view within Browse
    document.querySelector('.tab[data-tab="browse"]')?.click();
    document.querySelector('[data-view="search"]')?.click();
    const q = document.getElementById("search-query");
    const yf = document.getElementById("search-year-from");
    const yt = document.getElementById("search-year-to");
    const scope = document.getElementById("search-scope");
    if (q) q.value = word;
    // Parse bucket label — "2024" (year), "2024-03" (month), or a week key
    const m1 = /^(\d{4})$/.exec(bucketLabel || "");
    const m2 = /^(\d{4})-(\d{2})$/.exec(bucketLabel || "");
    if (m1) { if (yf) yf.value = m1[1]; if (yt) yt.value = m1[1]; }
    else if (m2) { if (yf) yf.value = m2[1]; if (yt) yt.value = m2[1]; }
    if (scope && channel) {
      // Select the channel if it's in the scope dropdown
      for (const opt of scope.options) {
        if (opt.value === "channel") { scope.value = "channel"; break; }
      }
    }
    // Fire the search
    setTimeout(() => document.getElementById("btn-search-run")?.click(), 80);
  }

  // Word cloud renderer — no Chart.js; just positions span elements sized
  // proportional to their frequency. Matches YTArchiver.py's matplotlib
  // word-cloud conceptually but DOM-based so we don't pull another lib.
  function _renderWordCloud(words) {
    _graphDestroy();
    const canvas = document.getElementById("graph-canvas");
    const wrap = canvas?.parentElement;
    if (!wrap) return;
    // Hide the canvas, show the cloud overlay
    canvas.style.display = "none";
    let cloud = document.getElementById("graph-wordcloud");
    if (!cloud) {
      cloud = document.createElement("div");
      cloud.id = "graph-wordcloud";
      cloud.className = "graph-wordcloud";
      wrap.appendChild(cloud);
    }
    cloud.style.display = "";
    cloud.innerHTML = "";
    if (!words.length) {
      cloud.innerHTML = '<div class="browse-empty">No words.</div>';
      return;
    }
    // Scale font sizes between 12px and 52px based on rank.
    const max = words[0].count || 1;
    const min = words[words.length - 1].count || 1;
    const palette = ["#6cb4ee", "#e87aac", "#3dd68c", "#c7e64f",
                     "#c084fc", "#ff8c42", "#38d9e0", "#dde1e8"];
    for (let i = 0; i < words.length; i++) {
      const w = words[i];
      const ratio = max === min ? 0.5 :
        (Math.log(w.count) - Math.log(min)) / (Math.log(max) - Math.log(min));
      const size = 12 + Math.round(ratio * 40);
      const span = document.createElement("span");
      span.className = "wc-word";
      span.textContent = w.word;
      span.style.fontSize = size + "px";
      span.style.color = palette[i % palette.length];
      span.title = `${w.word} \u2014 ${(w.count).toLocaleString()} occurrence${w.count === 1 ? "" : "s"}`;
      // Click = seed the Word field with this word and re-plot as line.
      span.addEventListener("click", () => {
        const wordEl = document.getElementById("graph-word");
        if (wordEl) wordEl.value = w.word;
        // Switch back to line chart
        document.querySelector('.chart-type-btn[data-type="line"]')?.click();
      });
      cloud.appendChild(span);
    }
  }

  function _hideWordCloud() {
    const cloud = document.getElementById("graph-wordcloud");
    if (cloud) cloud.style.display = "none";
    const canvas = document.getElementById("graph-canvas");
    if (canvas) canvas.style.display = "";
  }

  // Export the last-plotted data as CSV. Works for Line / Bar (labels +
  // values), or Word Cloud (word, count).
  async function _exportGraphCsv() {
    if (!_graphLastData) {
      window._showToast?.("Plot something first.", "warn");
      return;
    }
    let rows = [];
    if (_graphLastData.cloud) {
      rows.push(["word", "count"]);
      for (const w of _graphLastData.cloud) rows.push([w.word, w.count]);
    } else if (Array.isArray(_graphLastData.series) && _graphLastData.series.length) {
      rows.push(["bucket", ..._graphLastData.series.map(s => s.word)]);
      for (let i = 0; i < _graphLastData.labels.length; i++) {
        rows.push([_graphLastData.labels[i],
                   ..._graphLastData.series.map(s => s.values[i] ?? "")]);
      }
    } else {
      rows.push(["bucket", _graphLastData.word || "count"]);
      for (let i = 0; i < _graphLastData.labels.length; i++) {
        rows.push([_graphLastData.labels[i], _graphLastData.values[i] ?? ""]);
      }
    }
    const csv = rows.map(r => r.map(c => {
      const s = String(c ?? "");
      return (s.includes(",") || s.includes('"') || s.includes("\n"))
        ? `"${s.replace(/"/g, '""')}"` : s;
    }).join(",")).join("\n");

    const api = window.pywebview?.api;
    const fname = _graphLastData.cloud ? "wordcloud.csv" :
      `graph_${(_graphLastData.word || "data").replace(/[^\w-]+/g, "_")}.csv`;
    if (api?.save_text_to_file) {
      const res = await api.save_text_to_file(fname, csv);
      if (res?.ok) window._showToast?.("CSV saved.", "ok");
      else window._showToast?.(res?.error || "Save failed.", "error");
    } else {
      // Browser preview fallback
      const blob = new Blob([csv], { type: "text/csv" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url; a.download = fname; a.click();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
    }
  }

  // Populate the Graph's channel dropdown from the DB
  async function populateGraphChannels() {
    const sel = document.getElementById("graph-channel");
    if (!sel) return;
    const api = window.pywebview?.api;
    if (!api?.browse_list_channels) return;
    try {
      const chans = await api.browse_list_channels();
      if (!chans) return;
      sel.innerHTML = '<option value="All">All</option>' +
        chans.map(c => `<option value="${escapeHtml(c.name || c.folder)}">${escapeHtml(c.name || c.folder)}</option>`).join("");
    } catch (e) { /* ignore */ }
  }

  // Wire Bookmarks → Export CSV button (backend shows save dialog)
  function initBookmarksExport() {
    const btn = document.getElementById("btn-bookmarks-export");
    if (!btn) return;
    btn.addEventListener("click", async () => {
      const api = window.pywebview?.api;
      if (!api?.bookmark_export_csv) {
        window._showToast?.("Native mode required for export.", "warn");
        return;
      }
      const res = await api.bookmark_export_csv();
      if (res?.ok) {
        window._showToast?.(`Exported ${res.count} bookmark(s) to CSV.`, "ok");
      } else if (res?.cancelled) {
        /* no-op */
      } else {
        window._showToast?.(res?.error || "Export failed.", "error");
      }
    });
  }

  // ─── Recent tab live filter ──────────────────────────────────────────
  function initRecentFilter() {
    const input = document.getElementById("recent-filter");
    if (!input) return;
    let deb = null;
    input.addEventListener("input", () => {
      clearTimeout(deb);
      deb = setTimeout(() => {
        window._applyRecentFilter?.(input.value);
      }, 100);
    });
    input.addEventListener("keydown", (e) => {
      if (e.key === "Escape") { input.value = ""; window._applyRecentFilter?.(""); }
    });
  }

  // ─── Autorun history full-view dialog ────────────────────────────────
  function initAutorunHistoryDialog() {
    const backdrop = document.getElementById("autorun-history-backdrop");
    const cancelBtn = document.getElementById("autorun-history-cancel");
    const exportBtn = document.getElementById("autorun-history-export");
    if (!backdrop) return;

    const show = async () => {
      backdrop.style.display = "flex";
      await refreshHistory();
    };
    const hide = () => { backdrop.style.display = "none"; };

    async function refreshHistory() {
      const api = window.pywebview?.api;
      if (!api?.get_activity_log_history) return;
      try {
        const hist = await api.get_activity_log_history();
        const entries = Array.isArray(hist) ? hist : [];
        const body = document.getElementById("autorun-history-entries");
        const count = document.getElementById("autorun-history-count");
        if (count) count.textContent = `${entries.length} entries`;
        if (!body) return;
        window._autorunHistoryRaw = entries;
        _paintAutorunHistory(entries, "");
      } catch (e) { console.warn("autorun hist:", e); }
    }

    document.getElementById("autorun-history-filter")?.addEventListener("input", (e) => {
      _paintAutorunHistory(window._autorunHistoryRaw || [], e.target.value || "");
    });

    cancelBtn?.addEventListener("click", hide);
    backdrop.addEventListener("click", (e) => { if (e.target === backdrop) hide(); });
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && backdrop.style.display === "flex") hide();
    });

    // Trigger: long-press Clear button in the activity-log row header, OR
    // right-click on the activity log → "View full history"
    document.getElementById("btn-clear")?.addEventListener("contextmenu", (e) => {
      e.preventDefault();
      show();
    });
    // Also expose a global so tray / other triggers can open it
    window._openAutorunHistory = show;

    exportBtn?.addEventListener("click", async () => {
      const entries = window._autorunHistoryRaw || [];
      if (!entries.length) {
        window._showToast?.("No autorun history to export.", "warn");
        return;
      }
      // Flatten entries (segments joined) into a single plain-text column
      const lines = entries.map(ent => {
        if (typeof ent === "string") return ent;
        const segs = ent.segments || [];
        return segs.map(s => (s && s[0]) || "").join("").trim();
      });
      const csv = "entry\n" + lines.map(l => `"${l.replace(/"/g, '""')}"`).join("\n");
      const api = window.pywebview?.api;
      if (api?.save_text_to_file) {
        const r = await api.save_text_to_file("autorun_history.csv", csv);
        if (r?.ok) window._showToast?.("Saved.", "ok");
      } else {
        const blob = new Blob([csv], { type: "text/csv" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url; a.download = "autorun_history.csv"; a.click();
        setTimeout(() => URL.revokeObjectURL(url), 1000);
      }
    });
  }

  function _paintAutorunHistory(entries, filter) {
    const body = document.getElementById("autorun-history-entries");
    if (!body) return;
    body.innerHTML = "";
    const q = (filter || "").toLowerCase().trim();
    const matches = [];
    for (const ent of entries) {
      const text = typeof ent === "string"
        ? ent
        : (ent.segments || []).map(s => s && s[0] || "").join("");
      if (q && !text.toLowerCase().includes(q)) continue;
      matches.push({ text, ent });
    }
    if (!matches.length) {
      body.innerHTML = '<div style="padding: 16px; color: var(--c-dim); font-style: italic; text-align:center;">No matching entries.</div>';
      return;
    }
    const frag = document.createDocumentFragment();
    for (const { text } of matches) {
      const row = document.createElement("div");
      row.className = "ah-row";
      row.textContent = text;
      row.style.cssText = "padding: 3px 12px; white-space: pre; color: var(--c-log-txt); border-bottom: 1px solid rgba(255,255,255,0.02);";
      frag.appendChild(row);
    }
    body.appendChild(frag);
  }

  // ─── Scan archive button (Browse tab toolbar) ───────────────────────
  function initScanArchive() {
    document.getElementById("btn-scan-archive")?.addEventListener("click", async () => {
      const api = window.pywebview?.api;
      if (!api?.archive_rescan) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      await api.archive_rescan();
      window._showToast?.("Archive rescan started \u2014 check the log.", "ok");
    });
  }

  // Push-event handler: backend calls this when archive_rescan finishes
  // so the currently-open Browse grid refreshes to reflect pruned /
  // newly-registered rows. "I click Rescan, wait 5 minutes,
  // nothing changes in the program."
  window._onArchiveRescanComplete = function () {
    try {
      // If we're viewing a channel's video grid right now, re-query it.
      const ch = (_browseState?.currentChannel) || null;
      if (ch && _browseState.view === "videos" &&
          typeof window._reloadCurrentChannelVideos === "function") {
        window._reloadCurrentChannelVideos();
        window._showToast?.("Archive rescan complete \u2014 grid refreshed.", "ok");
        return;
      }
      // Channel list view — reload the channel cards so
      // per-channel counts update.
      if (_browseState?.view === "channels" &&
          typeof window._reloadChannelsGrid === "function") {
        window._reloadChannelsGrid();
      }
      window._showToast?.("Archive rescan complete.", "ok");
    } catch (e) { /* noop */ }
  };

  // ─── Subs filter input (live) ────────────────────────────────────────
  function initSubsFilter() {
    const input = document.getElementById("subs-filter");
    if (!input) return;
    let deb = null;
    input.addEventListener("input", () => {
      clearTimeout(deb);
      deb = setTimeout(() => {
        window._applySubsFilter?.(input.value);
      }, 100);
    });
    input.addEventListener("keydown", (e) => {
      if (e.key === "Escape") { input.value = ""; window._applySubsFilter?.(""); }
    });
  }

  // ─── Browse > Bookmarks sub-mode ─────────────────────────────────────
  async function refreshBookmarks() {
    const list = document.getElementById("bookmarks-list");
    if (!list) return;
    const api = window.pywebview?.api;
    if (!api?.bookmark_list) return;
    try {
      const rows = await api.bookmark_list();
      if (!rows || rows.length === 0) {
        list.innerHTML = '<div class="browse-empty">No bookmarks yet. Right-click a transcript segment in Watch view to add one.</div>';
        return;
      }
      const frag = document.createDocumentFragment();
      for (const b of rows) {
        const row = document.createElement("div");
        row.className = "bookmark-row";
        row.innerHTML = `
          <div class="bookmark-head">
            <span class="bookmark-title"></span>
            <span class="bookmark-meta"></span>
            <button class="icon-btn-slim" data-remove="${b.id}" title="Delete bookmark">\u00d7</button>
          </div>
          <div class="bookmark-text"></div>
          <div class="bookmark-note" contenteditable="true"></div>
        `;
        row.querySelector(".bookmark-title").textContent = b.title || "(untitled)";
        row.querySelector(".bookmark-meta").textContent =
          `${b.channel || ""} \u00b7 ${_formatTs(b.start_time)}`;
        row.querySelector(".bookmark-text").textContent = b.text || "";
        const noteEl = row.querySelector(".bookmark-note");
        noteEl.textContent = b.note || "";
        noteEl.dataset.placeholder = "Add a note\u2026";
        if (!b.note) noteEl.classList.add("bookmark-note-empty");
        noteEl.addEventListener("focus", () => {
          noteEl.classList.remove("bookmark-note-empty");
        });
        noteEl.addEventListener("input", () => {
          noteEl.classList.toggle("bookmark-note-empty",
            noteEl.textContent.trim() === "");
        });
        noteEl.addEventListener("keydown", (ev) => {
          // Ctrl+Enter saves and blurs; Escape cancels to the stored note.
          if (ev.key === "Enter" && (ev.ctrlKey || ev.metaKey)) {
            ev.preventDefault();
            noteEl.blur();
          } else if (ev.key === "Escape") {
            ev.preventDefault();
            noteEl.textContent = b.note || "";
            noteEl.classList.toggle("bookmark-note-empty", !b.note);
            noteEl.blur();
          }
        });
        noteEl.addEventListener("blur", async () => {
          const t = noteEl.textContent.trim();
          if (t === (b.note || "")) return; // no change
          const res = await api.bookmark_update_note?.(b.id, t);
          if (res?.ok) {
            b.note = t; // reflect in local data so Escape-revert is accurate
            noteEl.classList.add("bookmark-note-saved");
            setTimeout(() => noteEl.classList.remove("bookmark-note-saved"), 1200);
          } else {
            window._showToast?.("Note save failed.", "error");
          }
        });
        row.querySelector("[data-remove]").addEventListener("click", async (e) => {
          e.stopPropagation();
          const ok = await askDanger("Delete bookmark",
            `Remove this bookmark?\n\n"${b.text?.slice(0, 100) || ''}"`, "Delete");
          if (!ok) return;
          await api.bookmark_remove(b.id);
          refreshBookmarks();
        });

        // Click-to-jump: open the bookmark's video in Watch view seeked to
        // the bookmark timestamp. Mirrors YTArchiver.py:29372 _on_bookmark_select
        // + :29389 "Jump to segment" right-click action. Clicking the note
        // editor or delete button must NOT trigger jump, so we scope to the
        // head + text area only.
        const _jumpToBookmark = async () => {
          const start = Number(b.start_time) || 0;
          const vid = b.video_id || "";
          const title = b.title || "";
          // Resolve the actual video file via the title+channel index.
          // Tries (video_id → title → fuzzy) and opens Watch view.
          if (!api?.recent_resolve) {
            window._showToast?.("Native mode required.", "warn");
            return;
          }
          try {
            const r = await api.recent_resolve(title, b.channel || "");
            if (r?.ok && r.filepath) {
              const videoObj = {
                filepath: r.filepath,
                title: title,
                channel: b.channel || "",
                video_id: vid || r.video_id || "",
                _seek_to: start, // Watch view uses this for initial seek
              };
              if (typeof window._openVideoInWatch === "function") {
                window._openVideoInWatch(videoObj);
              } else {
                // Fallback: click the Browse tab and render directly
                document.querySelector('.tab[data-tab="browse"]')?.click();
              }
            } else {
              window._showToast?.("Couldn't find the source video for this bookmark.", "warn");
            }
          } catch (err) {
            window._showToast?.("Jump failed: " + err, "error");
          }
        };
        row.querySelector(".bookmark-head").addEventListener("click", (e) => {
          // Only fire if the user clicked the title/meta, not the × button.
          if (e.target.closest("[data-remove]")) return;
          _jumpToBookmark();
        });
        row.querySelector(".bookmark-text").addEventListener("click", _jumpToBookmark);
        row.querySelector(".bookmark-head").style.cursor = "pointer";
        row.querySelector(".bookmark-text").style.cursor = "pointer";
        row.querySelector(".bookmark-head").title = "Click to jump to this moment";
        row.querySelector(".bookmark-text").title = "Click to jump to this moment";
        frag.appendChild(row);
      }
      list.innerHTML = "";
      list.appendChild(frag);
    } catch (e) { console.warn("bookmarks:", e); }
  }

  // Called from seedLogs once channel data arrives.
  // `channels` comes from get_subs_channels (no avatar/banner URLs). To get
  // the real channel-art paths for the grid we hit browse_list_channels
  // which enriches with .ChannelArt/* URLs. Fall back to the plain list if
  // the richer call isn't available.
  window._primeBrowse = async function (channels) {
    const basic = (channels || []).slice().sort((a, b) =>
      (a.folder || "").localeCompare(b.folder || "", undefined, { sensitivity: "base" })
    );
    let enriched = basic;
    const api = window.pywebview?.api;
    if (api?.browse_list_channels) {
      try {
        const rich = await api.browse_list_channels();
        if (Array.isArray(rich) && rich.length) enriched = rich;
      } catch { /* fall through to basic */ }
    }
    _browseState.channels = enriched;
    window.renderChannelGrid(enriched, (c) => {
      _browseState.currentChannel = c;
      loadVideosFor(c);
      showView("videos");
    });
    // Populate "New this week" summary bar (async, best-effort).
    _refreshBrowseWeekSummary();
  };

  async function _refreshBrowseWeekSummary() {
    const bar = document.getElementById("browse-summary-bar");
    if (!bar) return;
    const api = window.pywebview?.api;
    if (!api?.browse_week_summary) return;
    try {
      const res = await api.browse_week_summary(7);
      if (!res?.ok) return;
      const nv = res.new_videos || 0;
      const nc = res.new_channels || 0;
      const tc = res.total_channels || 0;
      document.getElementById("bsb-new-videos").textContent = String(nv);
      document.getElementById("bsb-new-channels").textContent = String(nc);
      document.getElementById("bsb-total-channels").textContent = String(tc);
      bar.hidden = false;
      if (Array.isArray(res.channel_list) && res.channel_list.length) {
        bar.title = "Channels with new videos this week:\n " +
          res.channel_list.slice(0, 20).join("\n ") +
          (res.channel_list.length > 20 ? `\n \u2026 (+${res.channel_list.length - 20} more)` : "");
      } else {
        bar.title = "";
      }
    } catch { /* ignore */ }
  }
  window._refreshBrowseWeekSummary = _refreshBrowseWeekSummary;

  async function _askRedownload(channelName, resolution) {
    const api = window.pywebview?.api;
    if (!api?.chan_redownload) {
      window._showToast?.("Native mode required.", "warn");
      return;
    }
    const label = resolution === "best" ? "Best available" : `${resolution}p`;
    const ok = await askDanger("Redownload channel",
      `Redownload every video in "${channelName}" at ${label}?\n\n` +
      "This scans local files, fetches the YouTube catalog, matches by ID, " +
      "downloads each video, and replaces the originals.\n\n" +
      "Progress is saved \u2014 you can cancel and resume later.",
      "Start redownload");
    if (!ok) return;
    const res = await api.chan_redownload(channelName, resolution);
    if (res?.ok) {
      window._showToast?.(`Redownload started (${label}).`, "ok");
    } else {
      window._showToast?.(res?.error || "Redownload failed to start.", "error");
    }
  }
  window._askRedownload = _askRedownload;

  // Transcribe a channel, handling the first-time "Follow / Combined" radio
  // dialog if the backend asks for it. Ports YTArchiver.py:5919-5952 modal.
  // ─── Manual-queue Whisper model picker ───────────────────────
  // Mirrors YTArchiver.py:22030 `_ask_whisper_model_dialog`. Shows a 4-option
  // modal (tiny/small/medium/large-v3) with a 60-second countdown that
  // auto-picks the Settings-stored default. Used ONLY when the user
  // manually queues a video/channel/folder — sync-triggered auto-transcribes
  // use the Settings default silently. Returns the chosen model name, or
  // null on cancel. Swaps the running whisper model via the backend so the
  // next job uses it.
  async function _askWhisperModel(contextLabel = "") {
    const api = window.pywebview?.api;
    let currentDefault = "small";
    try {
      const s = await api?.settings_load?.();
      if (s?.whisper_model) currentDefault = String(s.whisper_model);
    } catch (_e) {}
    const models = [
      { name: "tiny", blurb: "fastest, lowest quality" },
      { name: "small", blurb: "balanced (default)" },
      { name: "medium", blurb: "higher quality, slower" },
      { name: "large-v3", blurb: "best quality, slowest" },
    ];
    const choices = models.map((m) => ({
      label: `${m.name} \u2014 ${m.blurb}`,
      value: m.name,
      primary: m.name === currentDefault,
    }));
    const msg = contextLabel
      ? `Pick a Whisper model for ${contextLabel}.`
      : "Pick a Whisper model for this job.";
    const pick = await askChoice({
      title: "Transcribe \u2014 Whisper model",
      message: msg,
      choices,
      countdownSecs: 60,
      countdownLabel: `Auto-selecting ${currentDefault} in`,
    });
    if (pick === null) return null;
    // Swap the running whisper process for the next job ONLY — do NOT
    // persist to config. The Settings > Whisper model dropdown is the
    // authoritative place for the default; a one-off manual pick for a
    // single retranscribe shouldn't mutate it. Second arg `false` =
    // don't persist. "manual retranscriptions have nothing to
    // do with that [settings default]".
    try {
      await api?.transcribe_swap_model?.(pick, false);
    } catch (_e) {}
    return pick;
  }
  window._askWhisperModel = _askWhisperModel;

  async function _askTranscribeChannel(channelName, combined) {
    const api = window.pywebview?.api;
    if (!api?.chan_transcribe_all) {
      window._showToast?.("Native mode required.", "warn");
      return;
    }
    // Manual channel transcribe → ask which whisper model (60s countdown
    // auto-picks Settings default). Skip on the recursive call after the
    // Follow-org/Combined dialog resolves.
    if (combined === undefined) {
      const model = await _askWhisperModel(`"${channelName}"`);
      if (model === null) return; // user cancelled
    }
    const res = await api.chan_transcribe_all(channelName, combined);
    if (res?.ok === false) {
      window._showToast?.(res.error || "Transcribe failed to start.", "error");
      return;
    }
    if (res?.needs_choice) {
      // First-time transcribe on an organized channel — ask the user.
      // 60-second countdown auto-selects Follow-organization (matches OLD's
      // `_ask_whisper_model_dialog` pattern, YTArchiver.py:22030, and is the
      // safe default since it mirrors the channel's folder layout).
      const pick = await askChoice({
        title: "Transcribe \u2014 " + channelName,
        message: "Where should transcript files be placed?",
        choices: [
          { label: `Follow organization (${res.org_label} folders)`,
            value: "follow", primary: true },
          { label: "Combined (one file for entire channel)",
            value: "combined" },
        ],
        countdownSecs: 60,
        countdownLabel: "Auto-selecting Follow organization in",
      });
      if (pick === null) return; // user cancelled
      // Recurse with the resolved choice.
      return _askTranscribeChannel(channelName, pick === "combined");
    }
    if (res?.ok && res.queued != null) {
      window._showToast?.(
        `Queued ${res.queued} video(s) for transcription.`, "ok");
    }
  }
  window._askTranscribeChannel = _askTranscribeChannel;

  // Exposed so other modules (e.g. Recent table dblclick) can pop a video
  // into the Watch view with a proper transcript + karaoke bind.
  window._openVideoInWatch = async function (video) {
    if (!video) return;
    // Ensure we're on the Browse tab and in Watch view.
    document.querySelector('.tab[data-tab="browse"]')?.click();
    // Record the view we're leaving so Back can return the user
    // to it. `_browseState.view` was "videos" (normal channel-drill
    // flow), "channels" (quick jump from a card), or a submode name
    // ("recent" / "search" / "bookmarks" / "graph"). The
    // `_browseGoBack` handler dispatches off this. Without this,
    // Back from Recent → Watch landed on a blank #video-grid
    // because no currentChannel was ever set — a bug report.
    _browseState.watchReturnTo = _browseState.view || null;
    _browseState.currentVideo = video;
    showView("watch");
    const api = window.pywebview?.api;
    let transcript = null;
    let sourceInfo = null;
    if (api?.browse_get_transcript) {
      try {
        const res = await api.browse_get_transcript({
          video_id: video.video_id || undefined,
          title: video.title || "",
        });
        // New return shape: {ok, segments, source}. Back-compat: if the
        // result is a plain array, treat it as old-style segment list.
        if (Array.isArray(res)) {
          transcript = res;
        } else if (res && res.segments) {
          transcript = res.segments;
          sourceInfo = res.source || null;
        }
      } catch { /* ignore */ }
    }
    if (!transcript || transcript.length === 0) {
      transcript = synthesizeTranscript(video);
    } else {
      transcript = transcript.map(seg => ({
        ts: _formatTs(seg.s), text: seg.t, words: seg.w, s: seg.s, e: seg.e,
      }));
    }
    window.renderWatchView(video, transcript, sourceInfo);
    // If the caller passed a seek target (bookmark jump, search-result jump,
    // transcript-segment click from elsewhere), seek the <video> element
    // once it's ready. Wait for `loadedmetadata` so duration is known.
    const seekTo = Number(video._seek_to);
    if (Number.isFinite(seekTo) && seekTo >= 0) {
      const vEl = document.querySelector("#watch-video video") ||
                  document.getElementById("watch-video");
      if (vEl) {
        const doSeek = () => {
          try {
            vEl.currentTime = seekTo;
            vEl.play().catch(() => {});
          } catch { /* noop */ }
        };
        if (vEl.readyState >= 1) doSeek();
        else vEl.addEventListener("loadedmetadata", doSeek, { once: true });
      }
    }
  };

  // Expose a "reload the currently-viewed channel's grid" handle
  // so push events (archive_rescan complete, etc.) can force the
  // grid to re-query after the DB changes under it.
  window._reloadCurrentChannelVideos = () => {
    const ch = (typeof _browseState !== "undefined")
      ? _browseState.currentChannel : null;
    if (ch) loadVideosFor(ch);
  };
  window._reloadChannelsGrid = () => {
    // Re-fetch the channel list (Subs table + per-channel counts).
    const api = window.pywebview?.api;
    if (!api?.get_index_summary) return;
    api.get_index_summary().then((idx) => {
      if (typeof window._applyIndexSummary === "function") {
        window._applyIndexSummary(idx);
      }
    }).catch(() => {});
  };

  async function loadVideosFor(channel) {
    const api = window.pywebview?.api;
    const name = channel.folder || channel.name || "";
    const sort = document.getElementById("browse-sort")?.value || "newest";

    // Show/hide the "Group by month" checkbox based on this channel's
    // folder layout. Only makes sense when the channel is organized
    // yyyy/mm on disk — otherwise there's nothing to group by.
    const monthWrap = document.getElementById("browse-group-month-wrap");
    if (monthWrap) {
      monthWrap.style.display = channel.split_months ? "" : "none";
    }
    // Uncheck month-grouping when switching to a channel that doesn't
    // support it, to avoid a stale-state re-render.
    if (!channel.split_months) {
      const mcb = document.getElementById("browse-group-month");
      if (mcb) mcb.checked = false;
    }

    // Clear the previous channel's grid + update the breadcrumb title
    // IMMEDIATELY so switching channels never shows stale content.
    _browseState.videos = [];
    const grid = document.getElementById("video-grid");
    if (grid) {
      grid.classList.remove("video-grid-grouped");
      grid.innerHTML = '<div class="browse-loading">Loading\u2026</div>';
    }
    const titleEl = document.getElementById("browse-main-title");
    if (titleEl) titleEl.textContent = name;

    // Track the in-flight channel name so if another channel is clicked
    // before this one's fetch returns, we discard the stale result.
    const myLoadSeq = (loadVideosFor._seq = (loadVideosFor._seq || 0) + 1);

    // Native mode → real DB
    if (api && api.browse_list_videos) {
      try {
        const rows = await api.browse_list_videos(name, sort, 50000);
        if (myLoadSeq !== loadVideosFor._seq) return; // stale, user clicked another channel
        if (Array.isArray(rows) && rows.length > 0) {
          _browseState.videos = rows.map(r => {
            // Prefer the YouTube upload time (file mtime — yt-dlp --mtime)
            // over the DB-insertion time. Falls back to added_ts when the
            // file is missing (e.g. moved offline).
            const epoch = r.upload_ts || r.added_ts || 0;
            return {
              title: r.title || "",
              channel: r.channel || name,
              filepath: r.filepath || "",
              video_id: r.video_id || "",
              uploaded: _formatAddedTs(epoch),
              duration: "",
              views: "",
              upload_ts: epoch * 1000,
              view_count: 0,
              size_bytes: r.size_bytes || 0,
              tx_status: r.tx_status || "pending",
              year: r.year, month: r.month,
              // Thumbnail sidecar (file:// URL from .Thumbnails/ or next-to-video)
              thumbnail: r.thumbnail || "",
              thumbnail_url: r.thumbnail_url || "",
            };
          });
          sortCurrentVideos(sort);
          return;
        }
      } catch (e) { console.warn("browse_list_videos failed:", e); }
    }

    // Fallback for preview mode — synthesize placeholder videos
    const count = Math.min(Math.max(12, channel.n_vids || 24), 48);
    const vids = [];
    for (let i = 0; i < count; i++) {
      const minutes = (i * 37 + 3) % 58 + 2;
      const secs = (i * 17) % 60;
      vids.push({
        title: `${name || "Video"}: sample episode #${i + 1}`,
        channel: name,
        uploaded: i < 2 ? "2 days ago" : i + " weeks ago",
        duration: `${minutes}:${String(secs).padStart(2, "0")}`,
        views: (((i * 1337) % 50000) + 800).toLocaleString(),
        upload_ts: Date.now() - (i * 86400000 * 3),
        view_count: ((i * 1337) % 50000) + 800,
      });
    }
    _browseState.videos = vids;
    sortCurrentVideos(sort);
  }

  function _formatAddedTs(ts) {
    if (!ts) return "";
    const now = Date.now() / 1000;
    const age = now - ts;
    if (age < 60) return "just now";
    if (age < 3600) return Math.floor(age / 60) + "m ago";
    if (age < 86400) return Math.floor(age / 3600) + "h ago";
    if (age < 86400*30) return Math.floor(age / 86400) + "d ago";
    if (age < 86400*365) return Math.floor(age / (86400*30)) + "mo ago";
    const years = Math.floor(age / (86400*365));
    return years + (years === 1 ? "y ago" : "y ago");
  }

  function sortCurrentVideos(sortBy) {
    const vids = _browseState.videos.slice();
    if (sortBy === "newest") vids.sort((a, b) => b.upload_ts - a.upload_ts);
    else if (sortBy === "oldest") vids.sort((a, b) => a.upload_ts - b.upload_ts);
    else if (sortBy === "most_viewed") vids.sort((a, b) => b.view_count - a.view_count);
    const groupByYear = !!document.getElementById("browse-group-year")?.checked;
    const groupByMonth = !!document.getElementById("browse-group-month")?.checked;
    // Contextual nudge when no metadata on this channel yet — matches
    // YTArchiver.py:25091 _grid_meta_banner_lbl. Banner appears above the
    // grid, clicking it queues metadata for the channel.
    _refreshVideoGridMetaBanner(vids);
    window.renderVideoGrid(vids, async (v) => {
      _browseState.currentVideo = v;
      showView("watch");
      // Try real transcript from DB first, fall back to synthesized
      const api = window.pywebview?.api;
      let transcript = null;
      let sourceInfo = null;
      if (api && api.browse_get_transcript) {
        try {
          const res = await api.browse_get_transcript({
            video_id: v.video_id || undefined,
            title: v.title,
          });
          if (Array.isArray(res)) transcript = res;
          else if (res && res.segments) {
            transcript = res.segments;
            sourceInfo = res.source || null;
          }
        } catch (e) { console.warn("get_transcript failed:", e); }
      }
      if (!transcript || transcript.length === 0) {
        transcript = synthesizeTranscript(v);
      } else {
        // Convert DB schema (s/e/t/w) to renderer schema (ts/text)
        transcript = transcript.map(seg => ({
          ts: _formatTs(seg.s),
          text: seg.t,
          words: seg.w,
          s: seg.s, e: seg.e,
        }));
      }
      window.renderWatchView(v, transcript, sourceInfo);
    }, { groupByYear, groupByMonth });
  }

  function _refreshVideoGridMetaBanner(vids) {
    const grid = document.getElementById("video-grid");
    if (!grid) return;
    // Remove any prior banner
    grid.parentElement?.querySelector(".meta-nudge-banner")?.remove();
    if (!vids || !vids.length) return;
    // Detect: no video has a view_count OR an uploaded string — classic
    // "this channel hasn't had a metadata pass yet".
    const anyMeta = vids.some(v => (v.view_count && v.view_count > 0) ||
                                    (v.views && String(v.views).trim()) ||
                                    (v.uploaded && String(v.uploaded).trim()));
    if (anyMeta) return;
    const ch = _browseState.currentChannel;
    if (!ch) return;
    const banner = document.createElement("div");
    banner.className = "meta-nudge-banner";
    banner.innerHTML = `
      <span class="meta-nudge-icon">&#x1F4E5;</span>
      <span class="meta-nudge-text">
        No metadata yet for <b></b>. Click to queue a fetch (views, likes,
        descriptions, thumbnails, and top 50 comments per video).
      </span>
      <button class="btn btn-primary btn-thin">Download metadata</button>
    `;
    banner.querySelector("b").textContent = ch.folder || ch.name || "this channel";
    banner.querySelector("button").addEventListener("click", async (e) => {
      e.stopPropagation();
      const api = window.pywebview?.api;
      if (!api?.metadata_recheck_channel) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      const name = ch.folder || ch.name || "";
      const res = await api.metadata_recheck_channel({ name });
      if (res?.ok) {
        window._showToast?.(`Metadata fetch started for ${name}.`, "ok");
        banner.remove();
      } else {
        window._showToast?.(res?.error || "Start failed.", "error");
      }
    });
    grid.parentElement?.insertBefore(banner, grid);
  }

  function _formatTs(sec) {
    if (sec == null) return "0:00";
    const m = Math.floor(sec / 60);
    const s = Math.floor(sec % 60);
    return `${m}:${String(s).padStart(2, "0")}`;
  }
  // Expose for logs.js — the retranscribe-complete handler lives there
  // and needs to reformat timestamps when it re-renders the transcript.
  window._formatTs = _formatTs;

  function synthesizeTranscript(video) {
    const sample = [
      "Welcome back to the channel everybody I'm so excited to be here today.",
      "We've got a really interesting topic to cover and I think you're going to enjoy it.",
      "Let's jump right in and take a look at what we've got.",
      "So the first thing I want to talk about is the overall structure.",
      "You can see here that the design is actually pretty straightforward once you break it down.",
      "And that brings us to the second point which is really the core of what we're discussing.",
      "Now I know some of you may be thinking this seems overly complicated.",
      "But stick with me because it's going to make sense in just a moment.",
      "Alright let's move on to the third section.",
      "This is where things get really interesting and honestly a little bit wild.",
      "If you've been following along up to this point you'll love what comes next.",
      "Okay so to wrap things up let me just summarize the key takeaways here.",
      "Thanks for watching and I'll catch you in the next one.",
    ];
    const segs = [];
    for (let i = 0; i < sample.length; i++) {
      const minute = Math.floor(i * 0.7);
      const secs = (i * 42) % 60;
      segs.push({ ts: `${minute}:${String(secs).padStart(2, "0")}`, text: sample[i] });
    }
    return segs;
  }

  /* Browse sub-mode hook-ups — all synthesize data for preview only.
     Phase 6 replaces these with real bridge calls (FTS5, JSONL indexer,
     bookmarks table, per-channel stats). */
  function initBrowseSubmodeContent() {
    // Search is fully wired by initSearchSubmode() (native + FTS) above.
    // Graph is wired by initGraphView() (Chart.js, native data).
    // No synthesized fallback needed once running in pywebview.
    //
    // Preserve only the keydown→click Enter shortcut for the search input.
    document.getElementById("search-query")?.addEventListener("keydown", (e) => {
      if (e.key === "Enter") document.getElementById("btn-search-run")?.click();
    });
  }

  function synthSearchResults(q, regex) {
    const channels = _browseState.channels || [];
    const rx = (() => {
      try { return new RegExp(regex ? q : escapeForRegex(q), "gi"); }
      catch { return new RegExp(escapeForRegex(q), "gi"); }
    })();
    const sample = [
      "and so that brings us to the really interesting part of this discussion",
      "which is honestly something i didn't expect when we started",
      "but it actually makes a lot of sense once you think about it",
      "the first time this came up on the show we had no idea how it would play out",
      "so here's the thing though you have to understand the context of the era",
      "the entire budget was basically spent on one very specific thing",
      "and that turned out to be a huge deal for the company long term",
    ];
    const results = [];
    for (let i = 0; i < 25; i++) {
      const chan = channels[i % Math.max(1, channels.length)]?.folder || "Channel";
      const text = sample[i % sample.length] + " " + q + " " + sample[(i + 2) % sample.length];
      const minute = i * 7 % 60;
      const secs = (i * 13) % 60;
      results.push({
        channel: chan,
        title: `Episode ${i + 1}`,
        timestamp: `${minute}:${String(secs).padStart(2, "0")}`,
        snippet: text,
      });
    }
    return results;
  }

  function renderSearchResults(container, hits, q) {
    container.innerHTML = "";
    if (hits.length === 0) {
      container.innerHTML = '<div class="browse-empty">No hits.</div>';
      return;
    }
    const rx = new RegExp("(" + escapeForRegex(q) + ")", "gi");
    const frag = document.createDocumentFragment();
    for (const h of hits) {
      const row = document.createElement("div");
      row.className = "search-result";
      row.title = "Double-click to open in Watch view at this timestamp";
      row.innerHTML = `
        <span class="ts">[${h.timestamp}]</span>
        <span class="snippet"></span>
        <span class="meta"></span>
      `;
      row.querySelector(".snippet").innerHTML = (h.snippet || "")
        .replace(rx, "<mark>$1</mark>");
      row.querySelector(".meta").textContent = `${h.channel || ""} \u00b7 ${h.title || ""}`;
      row.addEventListener("dblclick", () => _openSearchHitInWatch(h));
      frag.appendChild(row);
    }
    container.appendChild(frag);
  }

  async function _openSearchHitInWatch(hit) {
    const api = window.pywebview?.api;
    if (!api?.browse_resolve_segment) {
      window._showToast?.("Native mode required for playback.", "warn");
      return;
    }
    try {
      const res = await api.browse_resolve_segment(
        hit.jsonl_path || "", hit.video_id || "", hit.title || "");
      if (!res?.ok) {
        window._showToast?.(res?.error || "Video file not found.", "error");
        return;
      }
      const video = {
        title: res.title || hit.title || "",
        channel: res.channel || hit.channel || "",
        filepath: res.filepath,
        video_id: res.video_id || hit.video_id || "",
        start_at: Number(hit.start_time) || 0,
      };
      _browseState.currentVideo = video;
      showView("watch");
      // Load real transcript, fall back to synthesized
      let transcript = null;
      let sourceInfo = null;
      if (api.browse_get_transcript) {
        try {
          const res = await api.browse_get_transcript({
            video_id: video.video_id || undefined,
            jsonl_path: hit.jsonl_path,
            title: video.title,
          });
          if (Array.isArray(res)) transcript = res;
          else if (res && res.segments) {
            transcript = res.segments;
            sourceInfo = res.source || null;
          }
        } catch { /* ignore */ }
      }
      if (!transcript || transcript.length === 0) {
        transcript = synthesizeTranscript(video);
      } else {
        transcript = transcript.map(seg => ({
          ts: _formatTs(seg.s), text: seg.t, words: seg.w, s: seg.s, e: seg.e,
        }));
      }
      window.renderWatchView(video, transcript, sourceInfo);
      // Seek + flash-highlight the segment once the <video> element is ready.
      const vEl = document.getElementById("watch-video");
      if (vEl && video.start_at > 0) {
        const seek = () => {
          try { vEl.currentTime = video.start_at; vEl.play?.().catch(() => {}); }
          catch { /* ignore */ }
        };
        if (vEl.readyState >= 1) seek();
        else vEl.addEventListener("loadedmetadata", seek, { once: true });
      }
    } catch (e) {
      console.warn("open search hit failed:", e);
      window._showToast?.("Could not open video.", "error");
    }
  }

  function escapeForRegex(s) {
    return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }

  function drawWordFrequencyGraph(word) {
    const canvas = document.getElementById("graph-canvas");
    const empty = document.getElementById("graph-empty");
    if (!canvas) return;
    empty.style.display = "none";

    const ctx = canvas.getContext("2d");
    const w = canvas.width, h = canvas.height;
    ctx.clearRect(0, 0, w, h);

    // Background
    ctx.fillStyle = "#0a0b0d";
    ctx.fillRect(0, 0, w, h);

    // Axis
    ctx.strokeStyle = "#2a2c30";
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(40, h - 30);
    ctx.lineTo(w - 10, h - 30);
    ctx.moveTo(40, 10);
    ctx.lineTo(40, h - 30);
    ctx.stroke();

    // Synthesized data: 24 months
    const points = [];
    for (let i = 0; i < 24; i++) {
      const val = Math.sin((i + word.length) * 0.35) * 40 + 60 + Math.random() * 20;
      points.push(val);
    }
    const maxVal = Math.max(...points);
    const xStep = (w - 60) / (points.length - 1);
    // Line
    ctx.strokeStyle = "#6cb4ee";
    ctx.lineWidth = 2;
    ctx.beginPath();
    points.forEach((v, i) => {
      const x = 40 + i * xStep;
      const y = (h - 30) - (v / maxVal) * (h - 50);
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.stroke();
    // Points
    ctx.fillStyle = "#6cb4ee";
    points.forEach((v, i) => {
      const x = 40 + i * xStep;
      const y = (h - 30) - (v / maxVal) * (h - 50);
      ctx.beginPath();
      ctx.arc(x, y, 3, 0, Math.PI * 2);
      ctx.fill();
    });
    // Title
    ctx.fillStyle = "#dde1e8";
    ctx.font = "12px Segoe UI, sans-serif";
    ctx.fillText(`Frequency of "${word}" \u2014 last 24 months (synthesized)`, 50, 20);
  }

  function populateIndexTable(channels) {
    const tbody = document.getElementById("index-table-body");
    if (!tbody) return;
    tbody.innerHTML = "";
    const frag = document.createDocumentFragment();
    for (const c of channels.slice(0, 100)) {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td></td>
        <td class="right">${c.n_vids ? c.n_vids.toLocaleString() : "\u2014"}</td>
        <td class="right">${c.size || "\u2014"}</td>
        <td class="right">\u2014</td>
        <td class="right">${c.auto_transcribe ? "on" : "\u2014"}</td>
      `;
      tr.cells[0].textContent = c.folder;
      frag.appendChild(tr);
    }
    tbody.appendChild(frag);
  }
  window._populateIndexTable = populateIndexTable;

  /** Apply real Index-tab summary (from backend) — overrides static placeholders. */
  window._applyIndexSummary = function (idx) {
    if (!idx) return;
    const c = idx.cards || {};
    const setText = (id, txt) => { const el = document.getElementById(id); if (el) el.textContent = txt; };
    setText("idx-channels", (c.channels ?? "\u2014").toLocaleString?.() ?? c.channels);
    setText("idx-videos", (c.videos ?? "\u2014").toLocaleString?.() ?? c.videos);
    setText("idx-size", c.size_label || "\u2014");
    setText("idx-transcribed", c.transcribed_pct_channels != null
                               ? c.transcribed_pct_channels.toFixed(1) + "%"
                               : "\u2014");
    // Sidebar stats as well
    setText("stat-channels", (c.channels ?? "").toLocaleString?.() ?? "");
    setText("stat-videos", (c.videos ?? "").toLocaleString?.() ?? "");

    // Per-channel table
    if (Array.isArray(idx.per_channel)) {
      populateIndexTable(idx.per_channel);
    }
    // "Last built" status line under the control row.
    const last = document.getElementById("idx-last-built");
    if (last) {
      const t = new Date();
      const hh = t.getHours();
      const mm = String(t.getMinutes()).padStart(2, "0");
      const ampm = hh >= 12 ? "pm" : "am";
      const h12 = ((hh + 11) % 12) + 1;
      last.textContent = `Last refresh \u00b7 ${h12}:${mm}${ampm}`;
    }
  };

  // Sync + GPU queue "Auto" checkboxes — when on, adding an item to an empty
  // queue auto-starts the queue. Mirrors YTArchiver.py autorun_sync +
  // autorun_gpu config keys. State is persisted to config via the backend.
  function initQueueAutoCheckboxes() {
    const syncCB = document.getElementById("sync-auto-checkbox");
    const gpuCB = document.getElementById("gpu-auto-checkbox");

    // Restore saved state on load. `window.pywebview.api` isn't injected
    // until AFTER DOMContentLoaded (pywebview fires a `pywebviewready`
    // event when it's ready, but boot() runs on DOMContentLoaded), so
    // the first api lookup usually returns undefined and the original
    // restore silently no-op'd — reported: toggle Auto, restart,
    // Auto is back to default. Fix: re-resolve the api when pywebview
    // signals ready, with a 600ms fallback poll in case the event was
    // missed or we're racing boot.
    const restore = () => {
      const api = window.pywebview?.api;
      if (!api?.queue_auto_get) return false;
      api.queue_auto_get().then((st) => {
        if (!st) return;
        if (syncCB) syncCB.checked = !!st.sync;
        if (gpuCB) gpuCB.checked = !!st.gpu;
      }).catch(() => {});
      return true;
    };
    if (!restore()) {
      window.addEventListener("pywebviewready", () => { restore(); },
                              { once: true });
      // Belt-and-suspenders: poll briefly in case `pywebviewready` was
      // already dispatched before we registered the listener.
      let tries = 0;
      const poll = () => {
        if (restore()) return;
        if (++tries < 20) setTimeout(poll, 150);
      };
      setTimeout(poll, 150);
    }

    // Change handler: re-resolve api each call (same api-timing gotcha).
    syncCB?.addEventListener("change", () => {
      const api = window.pywebview?.api;
      api?.queue_auto_set?.("sync", syncCB.checked);
    });
    gpuCB?.addEventListener("change", () => {
      const api = window.pywebview?.api;
      api?.queue_auto_set?.("gpu", gpuCB.checked);
    });
  }

  // Wire up the Index view (Stats + Build/Rebuild + Log). Archive Roots +
  // Auto-update-every-N live in the Settings tab (per project convention) —
  // see initSettingsArchiveRoots below. Delete-All-Transcriptions is a
  // right-click action on any root entry in Settings.
  //
  // API note: pywebview injects `window.pywebview.api` AFTER init runs, so
  // we re-resolve it on every call instead of caching it once. Otherwise
  // the Stats area gets stuck on "backend offline" for the lifetime of the
  // window.
  function initIndexControls() {
    const getApi = () => window.pywebview?.api;
    const bBuild = document.getElementById("btn-idx-build");
    const bRebuild = document.getElementById("btn-idx-rebuild");
    const statsEl = document.getElementById("index-stats-text");
    const progEl = document.getElementById("idx-progress");
    const logEl = document.getElementById("index-log");

    const appendLog = (line) => {
      if (!logEl) return;
      const ln = document.createElement("div");
      ln.className = "log-line";
      ln.textContent = line;
      logEl.appendChild(ln);
      logEl.scrollTop = logEl.scrollHeight;
    };

    const refreshStats = async () => {
      if (!statsEl) return;
      const api = getApi();
      if (!api?.get_index_summary) {
        // Preview / pre-ready state. Don't overwrite existing numbers once
        // we've already painted them (avoids a "flash of offline").
        if (!statsEl.dataset.populated) {
          statsEl.textContent = "\u2014 (loading\u2026)";
        }
        return;
      }
      const fmt = (v) => (v == null ? "\u2014" :
        (typeof v === "number" ? v.toLocaleString() : String(v)));
      try {
        const idx = await api.get_index_summary();
        const c = (idx && idx.cards) || {};
        const pct = (c.transcribed_pct_channels != null)
          ? c.transcribed_pct_channels.toFixed(1) + "%"
          : "\u2014";
        const lines = [
          `Channels: ${fmt(c.channels)}`,
          `Videos: ${fmt(c.videos)}`,
          `Segments: ${fmt(c.segments)}`,
          `Transcribed: ${pct}`,
          `Total size: ${c.size_label || "\u2014"}`,
          `Hours of video: ${fmt(c.hours)}`,
        ];
        statsEl.textContent = lines.join("\n");
        statsEl.dataset.populated = "1";
      } catch (e) {
        statsEl.textContent = `Stats unavailable: ${e}`;
      }
    };

    bBuild?.addEventListener("click", async () => {
      const api = getApi();
      if (!api?.archive_rescan) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      if (progEl) progEl.textContent = "Building\u2026";
      appendLog("Building / updating index\u2026");
      try {
        await api.archive_rescan();
        appendLog("Build complete.");
        if (progEl) progEl.textContent = "Done.";
        await refreshStats();
      } catch (e) {
        appendLog(`Build failed: ${e}`);
        if (progEl) progEl.textContent = "Failed.";
      }
    });

    bRebuild?.addEventListener("click", async () => {
      const api = getApi();
      if (!api?.index_rebuild_fts) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      const ok = await window.askDanger(
        "Rebuild from scratch?",
        "Drop the FTS search index and rebuild it from every .jsonl on disk. " +
        "Safe, but can take minutes on large archives.",
        "Rebuild");
      if (!ok) return;
      if (progEl) progEl.textContent = "Rebuilding FTS\u2026";
      appendLog("Rebuilding FTS index from scratch\u2026");
      try {
        await api.index_rebuild_fts();
        appendLog("FTS rebuild complete.");
        if (progEl) progEl.textContent = "Done.";
        await refreshStats();
      } catch (e) {
        appendLog(`FTS rebuild failed: ${e}`);
        if (progEl) progEl.textContent = "Failed.";
      }
    });

    // pywebview injects api AFTER DOMContentLoaded — refresh once it's ready
    // so the Stats area populates without needing a sub-tab click.
    window.addEventListener("pywebviewready", () => { refreshStats(); });
    // Defensive: if the event already fired before we wired it, a short
    // delayed poll catches that case too.
    setTimeout(() => { if (getApi()?.get_index_summary) refreshStats(); }, 800);

    // Expose so the Settings > Index sub-tab can trigger a refresh when
    // the user clicks back onto it (avoids stale "Loading…").
    window._refreshIndexStats = refreshStats;

    // Initial paint
    refreshStats();
  }

  // Settings sub-tab switcher: [General] [Performance] [Appearance]
  // [Tools] [Index]. Clicking the Index sub-nav also triggers a fresh
  // stats fetch + log refresh so the numbers are current each time the
  // user lands on it. The shared Save button footer is hidden on the
  // Index view since that view has its own Build / Rebuild actions and
  // no form fields to save.
  function initSettingsSubTabs() {
    const buttons = document.querySelectorAll(".settings-subnav-btn");
    const views = {
      general: document.getElementById("settings-view-general"),
      performance: document.getElementById("settings-view-performance"),
      appearance: document.getElementById("settings-view-appearance"),
      tools: document.getElementById("settings-view-tools"),
      index: document.getElementById("settings-view-index"),
    };
    const saveFooter = document.getElementById("settings-actions-footer");
    if (!buttons.length) return;
    const show = (key) => {
      buttons.forEach(b => b.classList.toggle("active", b.dataset.settingsView === key));
      for (const k of Object.keys(views)) {
        if (views[k]) views[k].style.display = (k === key) ? "" : "none";
      }
      // Hide Save on Index (has its own actions). Show on everything else.
      if (saveFooter) saveFooter.style.display = (key === "index") ? "none" : "";
      if (key === "index") {
        // Pull fresh numbers every visit — stale "Loading…" was showing
        // up after first paint because the initial fetch was racing boot.
        if (typeof window._refreshIndexStats === "function") {
          window._refreshIndexStats();
        }
      }
    };
    buttons.forEach(b => {
      b.addEventListener("click", () => show(b.dataset.settingsView || "general"));
    });
  }

  // Settings > Index sub-tab: Archive Roots list + Auto-update-every-N +
  // delete-all-transcriptions right-click menu. Moved here from the
  // Browse > Index view per the user's request 2026-04-18.
  function initSettingsArchiveRoots() {
    const getApi = () => window.pywebview?.api;
    const rootsList = document.getElementById("settings-roots-list");
    const bAdd = document.getElementById("btn-settings-add-root");
    const bRemove = document.getElementById("btn-settings-remove-root");
    const autoCB = document.getElementById("settings-auto-index-enabled");
    const autoThr = document.getElementById("settings-auto-index-threshold");
    if (!rootsList && !autoCB) return;

    let _selectedRoot = null;

    const _confirmDeleteAllTranscriptions = async (folder) => {
      const api = getApi();
      if (!api?.index_count_transcripts || !api?.index_delete_all_transcripts) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      window._showToast?.("Counting transcript files\u2026", "ok");
      const count = await api.index_count_transcripts(folder);
      if (!count?.ok) {
        window._showToast?.(count?.error || "Count failed.", "error");
        return;
      }
      const { txt_count = 0, jsonl_count = 0, total = 0, total_bytes = 0 } = count;
      if (!total) {
        window._showToast?.(`No transcript files under ${folder}.`, "ok");
        return;
      }
      const mb = (total_bytes / (1024 * 1024)).toFixed(1);
      const ok1 = await window.askDanger(
        "Delete all transcriptions?",
        `This will permanently delete ALL transcript files under:\n\n${folder}\n\n` +
        ` \u2022 ${txt_count.toLocaleString()} transcript .txt file(s)\n` +
        ` \u2022 ${jsonl_count.toLocaleString()} hidden .jsonl file(s)\n\n` +
        `Total: ${total.toLocaleString()} file(s), ${mb} MB\n\n` +
        "This cannot be undone. Are you sure?",
        "Continue");
      if (!ok1) return;
      const ok2 = await window.askDanger(
        "Final confirmation",
        `Are you absolutely sure?\n\nAll ${total.toLocaleString()} transcript ` +
        "file(s) will be permanently deleted. The FTS search index will " +
        "also be cleared. Sync will have to re-run Whisper on every video.",
        "Yes, DELETE EVERYTHING");
      if (!ok2) return;
      const res = await api.index_delete_all_transcripts(folder, "YES-DELETE-ALL");
      if (res?.ok) {
        window._showToast?.("Transcripts deleted.", "warn");
      } else {
        window._showToast?.(res?.error || "Delete failed.", "error");
      }
    };

    const renderRoots = async () => {
      if (!rootsList) return;
      rootsList.innerHTML = "";
      let outDir = "";
      let extras = [];
      try {
        const s = await getApi()?.settings_load?.();
        outDir = (s?.output_dir || "").trim();
        extras = Array.isArray(s?.tp_archive_roots) ? s.tp_archive_roots : [];
      } catch (_e) {}
      const entries = [];
      if (outDir) entries.push({ path: outDir, auto: true });
      for (const r of extras) if (r) entries.push({ path: r, auto: false });
      for (const e of entries) {
        const row = document.createElement("div");
        row.className = "root-entry" + (e.auto ? " auto" : "");
        row.dataset.path = e.path;
        row.textContent = e.auto ? `[auto] ${e.path}` : e.path;
        row.addEventListener("click", () => {
          rootsList.querySelectorAll(".root-entry.selected")
            .forEach(el => el.classList.remove("selected"));
          row.classList.add("selected");
          _selectedRoot = e.path;
        });
        row.addEventListener("contextmenu", (ev) => {
          ev.preventDefault();
          rootsList.querySelectorAll(".root-entry.selected")
            .forEach(el => el.classList.remove("selected"));
          row.classList.add("selected");
          _selectedRoot = e.path;
          if (window.showContextMenu) {
            window.showContextMenu(ev.clientX, ev.clientY, [
              { label: "\u{1F5D1} Delete All Transcriptions",
                cls: "danger",
                action: () => _confirmDeleteAllTranscriptions(e.path) },
            ]);
          }
        });
        rootsList.appendChild(row);
      }
    };

    bAdd?.addEventListener("click", async () => {
      const api = getApi();
      if (!api?.pick_folder) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      const res = await api.pick_folder("Select Archive Root Folder");
      if (!res?.ok || !res.path) return;
      try {
        const s = await api.settings_load();
        const extras = Array.isArray(s?.tp_archive_roots) ? s.tp_archive_roots : [];
        if (extras.includes(res.path)) return;
        extras.push(res.path);
        await api.settings_save({ tp_archive_roots: extras });
        await renderRoots();
      } catch (e) {
        window._showToast?.("Add root failed: " + e, "error");
      }
    });

    bRemove?.addEventListener("click", async () => {
      const api = getApi();
      if (!_selectedRoot) {
        window._showToast?.("Pick a root to remove first.", "warn");
        return;
      }
      if (!api) return;
      try {
        const s = await api.settings_load();
        const outDir = (s?.output_dir || "").trim();
        if (_selectedRoot === outDir) {
          window._showToast?.(
            "The auto-detected output folder cannot be removed here. " +
            "Change the Archive root field above instead.", "warn");
          return;
        }
        const extras = (s?.tp_archive_roots || []).filter(r => r !== _selectedRoot);
        await api.settings_save({ tp_archive_roots: extras });
        _selectedRoot = null;
        await renderRoots();
      } catch (e) {
        window._showToast?.("Remove failed: " + e, "error");
      }
    });

    const persistAuto = async () => {
      const api = getApi();
      if (!api?.settings_save) return;
      const enabled = !!autoCB?.checked;
      let n = parseInt(autoThr?.value || "10", 10);
      if (!Number.isFinite(n) || n < 1) n = 10;
      if (n > 9999) n = 9999;
      if (autoThr) autoThr.value = String(n);
      try {
        await api.settings_save({
          auto_index_enabled: enabled,
          auto_index_threshold: n,
        });
      } catch (_e) {}
    };
    const loadSavedAuto = async () => {
      try {
        const s = await getApi()?.settings_load?.();
        if (s) {
          if (autoCB) autoCB.checked = !!s.auto_index_enabled;
          if (autoThr) autoThr.value = String(s.auto_index_threshold || 10);
        }
      } catch (_e) {}
    };
    loadSavedAuto();
    autoCB?.addEventListener("change", persistAuto);
    autoThr?.addEventListener("blur", persistAuto);
    autoThr?.addEventListener("keydown", (e) => {
      if (e.key === "Enter") { e.preventDefault(); persistAuto(); }
    });

    renderRoots();
    // Re-fetch once pywebview is ready (initial calls may have fired before
    // the bridge was live).
    window.addEventListener("pywebviewready", () => {
      renderRoots();
      loadSavedAuto();
    });
    setTimeout(() => {
      if (getApi()?.settings_load) { renderRoots(); loadSavedAuto(); }
    }, 800);
  }
  window._initSettingsArchiveRoots = initSettingsArchiveRoots;

  // Compat shim for the old _applyIndexSummary consumers (browse-tab preload).
  // Since the Index tab no longer has idx-channels/idx-videos/etc. elements,
  // _applyIndexSummary is now a no-op at refresh time — the new section
  // builds its own stats via refreshStats() inside initIndexControls. But
  // we still populate the sidebar badges that live outside the Index panel.
  (function compatApplyIndexSummary() {
    const orig = window._applyIndexSummary;
    window._applyIndexSummary = function (idx) {
      try {
        const c = (idx && idx.cards) || {};
        const setText = (id, txt) => {
          const el = document.getElementById(id);
          if (el) el.textContent = txt;
        };
        setText("stat-channels", (c.channels ?? "").toLocaleString?.() ?? "");
        setText("stat-videos", (c.videos ?? "").toLocaleString?.() ?? "");
      } catch (_e) {}
      if (typeof orig === "function") try { orig(idx); } catch (_e) {}
    };
  })();

  function filterCurrentView(q) {
    q = (q || "").toLowerCase().trim();
    if (_browseState.view === "channels") {
      const filtered = !q
        ? _browseState.channels
        : _browseState.channels.filter(c => (c.folder || "").toLowerCase().includes(q));
      window.renderChannelGrid(filtered, (c) => {
        _browseState.currentChannel = c;
        loadVideosFor(c);
        showView("videos");
      });
    } else if (_browseState.view === "videos") {
      const filtered = !q
        ? _browseState.videos
        : _browseState.videos.filter(v => (v.title || "").toLowerCase().includes(q));
      const groupByYear = !!document.getElementById("browse-group-year")?.checked;
      window.renderVideoGrid(filtered, (v) => {
        _browseState.currentVideo = v;
        showView("watch");
        window.renderWatchView(v, synthesizeTranscript(v));
      }, { groupByYear });
    }
  }

  // ─── Log mode dropdown (Simple / Verbose) ────────────────────────────
  // Matches YTArchiver's ttk.Combobox with values=["Simple","Verbose"].
  function initLogMode() {
    // The log-mode dropdown now lives on the Settings tab as
    // `settings-log-mode`; keep listening to either id so changes made
    // via Settings propagate into `document.body.dataset.logMode`
    // (CSS rules use it to hide/show verbose-only rows).
    const sel = document.getElementById("log-mode-select")
              || document.getElementById("settings-log-mode");
    if (!sel) return;
    document.body.dataset.logMode = sel.value || "Simple";
    sel.addEventListener("change", (e) => {
      const mode = e.target.value;
      document.body.dataset.logMode = mode;
      if (window.pywebview?.api?.set_log_mode) {
        window.pywebview.api.set_log_mode(mode);
      }
    });
  }

  // ─── Edit-channel inline panel (Subs tab) ────────────────────────────
  //
  // Mirrors YTArchiver's Edit Settings panel (idle-14). Loading a row
  // populates the fields; Update/Remove/Cancel dispatch to bridge.
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
      // Expand when there's text in either the name or URL field, or when
      // we're currently editing an existing channel (_editingIdentity set).
      const nameVal = (document.getElementById("edit-folder")?.value || "").trim();
      const urlVal = (document.getElementById("edit-url")?.value || "").trim();
      const editing = !!_editingIdentity;
      const shouldShow = Boolean(nameVal || urlVal || editing);
      box.classList.toggle("collapsed", !shouldShow);
    };
    document.getElementById("edit-folder")?.addEventListener("input", _updateCollapsed);
    document.getElementById("edit-url")?.addEventListener("input", _updateCollapsed);

    // Reverse URL-type nudge on the channel URL field — if the user pastes
    // a video URL here (instead of the Download tab), show a handoff
    // button. Mirrors classic's _video_nudge_frame (YTArchiver.py:5017).
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

    // Handoff: move the video URL over to the Download tab's single-video
    // box and focus it. Mirrors classic's _go_to_download_video
    // (YTArchiver.py:5020).
    document.getElementById("btn-edit-url-to-download")?.addEventListener("click", () => {
      const url = (_editUrlField?.value || "").trim();
      if (!url) return;
      // Switch to Download tab
      document.querySelector('.tab[data-tab="download"]')?.click();
      // Seed the Download-tab URL field + dispatch input so validators fire
      setTimeout(() => {
        const dl = document.getElementById("url-input");
        if (dl) {
          dl.value = url;
          dl.dispatchEvent(new Event("input", { bubbles: true }));
          dl.focus();
        }
      }, 80);
      // Clear the edit-url so the channel form doesn't keep showing the video URL
      if (_editUrlField) {
        _editUrlField.value = "";
        _editUrlField.dispatchEvent(new Event("input", { bubbles: true }));
      }
      _updateEditUrlNudge();
    });

    const openPanel = (mode, channel) => {
      // Panel is always shown. Just populate fields + swap label.
      const ds = document.getElementById("edit-diskstats");
      if (mode === "edit" && channel) {
        label.textContent = `Edit channel \u2014 ${channel.folder}`;
        // Single folder field: prefer folder_override (on-disk name)
        // over folder (display name) because OLD YTArchiver's edit
        // panel does the same — see YTArchiver.py:5432
        // `new_name_var.set(ch.get("folder_override", ch["name"]))`.
        // This way a channel with folder="Branch Education" and
        // folder_override="Branch Education" shows once, not twice.
        const _folderVal = channel.folder_override || channel.folder || channel.name || "";
        document.getElementById("edit-folder").value = _folderVal;
        document.getElementById("edit-url").value = channel.url || "";
        // Snapshot the loaded values so we can flip the button between
        // disabled (no pending changes) and "\u{1F4BE} Update channel"
        // (changes pending). Matches YTArchiver.py:17095 _validate_add_btn.
        // Snapshot.folder holds whatever the input is currently showing
        // (the merged folder_override || folder value). The save path
        // writes the same value back to both fields, so we compare
        // against this merged display value — `folder_override` is not
        // a separate snapshot key anymore.
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
        // Range radio — map config `mode` to one of the 3 UI values.
        // OLD uses "new" (subscribe), "full" (entire channel), "date"
        // or "fromdate" (from date). Missing field defaults to
        // subscribe (matches fresh-add default).
        const _mode = (channel.mode || "new").toLowerCase();
        const _rangeVal = _mode === "full" ? "all"
                         : (_mode === "date" || _mode === "fromdate") ? "fromdate"
                         : "subscribe";
        const _rangeRadio = document.querySelector(
          `input[name="edit-range"][value="${_rangeVal}"]`);
        if (_rangeRadio) {
          _rangeRadio.checked = true;
          // Fire change so any dependent UI (date-input visibility)
          // updates. The 3520-ish listener toggles `edit-date-group`
          // based on the checked value.
          _rangeRadio.dispatchEvent(new Event("change", { bubbles: true }));
        }
        // Pre-fill the From-date inputs when the channel has a stored
        // date so switching back to "From date" shows the right value.
        const _fromDate = channel.from_date || channel.date_after || "";
        if (_fromDate && /^\d{4}-?\d{2}-?\d{2}$/.test(_fromDate)) {
          const _clean = _fromDate.replace(/-/g, "");
          const y = _clean.slice(0, 4), m = _clean.slice(4, 6), d = _clean.slice(6, 8);
          const dy = document.getElementById("edit-date-year");
          const dm = document.getElementById("edit-date-month");
          const dd = document.getElementById("edit-date-day");
          if (dy) dy.value = y;
          if (dm) dm.value = m;
          if (dd) dd.value = d;
        }
        // Folder Org — OLD stores split_years / split_months as
        // booleans; map to the 3-option dropdown.
        const _folderOrg = channel.split_months ? "months"
                            : (channel.split_years ? "years" : "flat");
        const foEl2 = document.getElementById("edit-folder-org");
        if (foEl2) foEl2.value = _folderOrg;
        document.getElementById("edit-transcribe").checked = !!channel.auto_transcribe;
        document.getElementById("edit-metadata").checked = !!channel.auto_metadata;
        document.getElementById("edit-compress").checked = !!channel.compress_enabled;
        document.getElementById("edit-compress")?.dispatchEvent(new Event("change"));
        _updateCollapsed();
        // Start with the button disabled — only enable once the user has
        // actually changed a field. Matches YTArchiver.py:17095 behavior.
        update.disabled = true;
        update.textContent = "\u{1F4BE} Update channel";
        remove.style.display = "";
        // Fill disk stats from the Subs-table row we already have
        if (ds) {
          ds.hidden = false;
          const subsRow = (window._subsAllRows || [])
            .find(r => (r.folder || "").toLowerCase() === (channel.folder || "").toLowerCase());
          document.getElementById("ds-videos").textContent = subsRow?.n_vids ?? "\u2014";
          document.getElementById("ds-size").textContent = subsRow?.size ?? "\u2014";
          document.getElementById("ds-last-sync").textContent = subsRow?.last_sync ?? "\u2014";
          // Hide transcribed stats until async lookup lands
          const txSep = document.getElementById("ds-tx-sep");
          const txLbl = document.getElementById("ds-tx-label");
          const txVal = document.getElementById("ds-tx-count");
          if (txSep) txSep.hidden = true;
          if (txLbl) txLbl.hidden = true;
          if (txVal) txVal.hidden = true;
          // Fetch coverage (non-blocking)
          const api = window.pywebview?.api;
          if (api?.channel_transcription_stats) {
            api.channel_transcription_stats(channel.folder || channel.name || "")
              .then((res) => {
                if (!res?.ok) return;
                if (!res.total) return; // no DB entries yet — keep hidden
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
        window._editOriginalSnapshot = null; // add mode — no snapshot
      }
      // Only scroll the panel into view when we're opening in Edit mode —
      // unwanted scroll on every boot if Add mode auto-scrolls.
      if (mode === "edit") {
        box.scrollIntoView({ behavior: "smooth", block: "end" });
      }
    };

    const closePanel = () => {
      // "Cancel" returns the panel to its Add-mode collapsed default
      // (since it's always visible — we don't actually hide it anymore).
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

    cancel.addEventListener("click", closePanel);

    // Live change-detection: when editing, any field change enables the
    // "\u{1F4BE} Update channel" button; if the user then undoes back to the
    // original snapshot, disable again. Matches YTArchiver.py:17095
    // _validate_add_btn three-state behavior.
    const _editFields = [
      "edit-folder", "edit-url",
      "edit-resolution", "edit-min-dur", "edit-max-dur",
      "edit-folder-org",
      "edit-transcribe", "edit-metadata", "edit-compress",
      "edit-date-year", "edit-date-month", "edit-date-day",
    ];
    const _checkEditChanges = () => {
      const snap = window._editOriginalSnapshot;
      // Add mode: enable only when URL + folder name are both non-empty.
      if (!snap) {
        const fv = (document.getElementById("edit-folder")?.value || "").trim();
        const uv = (document.getElementById("edit-url")?.value || "").trim();
        update.disabled = !(fv && uv);
        return;
      }
      // Folder Name in the UI maps to BOTH `folder` and `folder_override`
      // on the channel record (see openPanel comment). Change-detection
      // compares the single input against the snapshot's `folder_override`
      // first, then `folder`, matching the load precedence. That way
      // typing a new value dirties the form exactly once.
      const _folderCur = (document.getElementById("edit-folder")?.value || "").trim();
      const _rangeCur = (document.querySelector('input[name="edit-range"]:checked')?.value || "subscribe");
      const _dateCur = [
        (document.getElementById("edit-date-year")?.value || "").trim(),
        (document.getElementById("edit-date-month")?.value || "").trim(),
        (document.getElementById("edit-date-day")?.value || "").trim(),
      ].filter(Boolean).join("");
      const cur = {
        folder: _folderCur,
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

    // "Continue" button — only visible when the channel has an in-progress
    // redownload. Peek on panel open + resume clicks `chan_redownload` with
    // the saved resolution. Mirrors YTArchiver.py:5473 _has_pending_redownload.
    const continueBtn = document.getElementById("edit-res-continue");
    const refreshContinueBtn = async (name) => {
      if (!continueBtn) return;
      continueBtn.hidden = true;
      if (!name) return;
      const api = window.pywebview?.api;
      if (!api?.chan_redownload_progress_peek) return;
      try {
        const p = await api.chan_redownload_progress_peek(name);
        if (p?.ok && p.pending) {
          continueBtn.hidden = false;
          const res = p.resolution || "best";
          const label = res === "best" ? "Best" : `${res}p`;
          continueBtn.textContent = `\u21BB Continue ${label} (${p.done || 0} done)`;
          continueBtn.dataset.resolution = res;
          continueBtn.title = `Resume in-progress ${label} redownload (${p.done || 0} videos complete)`;
        }
      } catch {}
    };
    // Hook into openPanel's edit branch — whenever a channel loads, peek.
    const _editFolderEl = document.getElementById("edit-folder");
    _editFolderEl?.addEventListener("change", () => {
      refreshContinueBtn(_editFolderEl.value.trim());
    });
    // Also fire once on panel load via MutationObserver on the label
    const _editLabel = document.getElementById("edit-channel-label");
    if (_editLabel) {
      const mo = new MutationObserver(() => {
        refreshContinueBtn(_editFolderEl?.value.trim() || "");
      });
      mo.observe(_editLabel, { childList: true, characterData: true, subtree: true });
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
      if (r?.ok) window._showToast?.("Redownload resumed.", "ok");
      else window._showToast?.(r?.error || "Resume failed.", "error");
    });

    // "\u2713 Recheck resolution" — scans the channel folder with ffprobe
    // (via backend api), counts videos below target resolution, and offers
    // to queue them for redownload at the new res. Matches YTArchiver.py:
    // 5155 res_check_btn flow.
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
      window._showToast?.("Scanning video files\u2026", "ok");
      try {
        const res = await api.chan_scan_resolution_mismatch(name, target);
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
        const label = target === "best" ? "Best quality" : `${target}p`;
        const ok = await askDanger(
          "Redownload at target resolution",
          `Redownload ${mismatch} of ${total} video(s) in "${name}" at ${label}?\n\n` +
          "This scans local files, fetches the YouTube catalog, matches by ID, " +
          "downloads each video, and replaces the originals. Progress is saved — " +
          "you can cancel and resume later.",
          "Start redownload");
        if (!ok) return;
        const r2 = await api.chan_redownload(name, target);
        if (r2?.ok) window._showToast?.(`Redownload started (${label}).`, "ok");
        else window._showToast?.(r2?.error || "Redownload failed.", "error");
      } catch (e) {
        window._showToast?.("Error: " + e, "error");
      }
    });

    // (Preview-folder-name button removed — Test URL already fills the
    // Folder Name field from the canonical channel name probe, so a
    // separate preview button was redundant.)

    // Test URL button — probe via yt-dlp and show the canonical name + count
    document.getElementById("btn-edit-test")?.addEventListener("click", async () => {
      const urlInput = document.getElementById("edit-url");
      const folderInput = document.getElementById("edit-folder");
      const url = urlInput?.value.trim();
      if (!url) { window._showToast?.("Enter a URL first.", "warn"); return; }
      window._showToast?.("Probing\u2026", "ok");
      const res = await window.pywebview?.api?.subs_test_url?.(url);
      if (!res) { window._showToast?.("Native mode required.", "warn"); return; }
      if (res.ok) {
        window._showToast?.(`\u2713 ${res.name}${res.total ? " ("+res.total.toLocaleString()+" videos)" : ""}`, "ok");
        if (res.url && res.url !== url) urlInput.value = res.url;

        // Prefill/update the Name field from the canonical probe result.
        const probed = (res.name || "").trim();
        const current = (folderInput.value || "").trim();
        if (probed) {
          if (!current) {
            folderInput.value = probed;
            folderInput.classList.add("edit-autofilled");
            setTimeout(() => folderInput.classList.remove("edit-autofilled"), 1800);
          } else if (current.toLowerCase() !== probed.toLowerCase()) {
            // Ask before overwriting a user-typed name.
            const replace = await askConfirm(
              "Channel name mismatch",
              `YouTube says this channel is named:\n\n ${probed}\n\n` +
              `Your Name field currently has:\n\n ${current}\n\n` +
              `Use the probed name? (Renames the on-disk folder if this is Edit mode.)`,
              { confirm: "Use probed name" });
            if (replace) {
              folderInput.value = probed;
              folderInput.classList.add("edit-autofilled");
              setTimeout(() => folderInput.classList.remove("edit-autofilled"), 1800);
            }
          }
        }
      } else {
        window._showToast?.(res.error || "Probe failed.", "error");
      }
    });

    // Restore defaults button — pulls from settings and applies
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
      window._showToast?.("Defaults restored.", "ok");
    });

    // Collect form state into a payload
    const collectPayload = () => {
      const range = document.querySelector('input[name="edit-range"]:checked')?.value || "subscribe";
      const fromYear = document.getElementById("edit-date-year")?.value || "";
      const fromMonth = document.getElementById("edit-date-month")?.value || "";
      const fromDay = document.getElementById("edit-date-day")?.value || "";
      const from_date = fromYear ? `${fromYear.padStart(4, "0")}-${(fromMonth || "01").padStart(2, "0")}-${(fromDay || "01").padStart(2, "0")}` : "";
      // Single "Folder Name" input → write to BOTH folder and
      // folder_override so OLD YTArchiver's `channel_folder_name()` (which
      // reads `folder_override OR name`) picks up the user's intent.
      // Matches OLD behavior — see YTArchiver.py:5432 in _set_edit_mode.
      const _folderVal = document.getElementById("edit-folder").value.trim();
      return {
        folder: _folderVal,
        url: document.getElementById("edit-url").value.trim(),
        folder_override: _folderVal,
        resolution: document.getElementById("edit-resolution").value,
        min_duration: parseInt(document.getElementById("edit-min-dur").value) || 0,
        max_duration: parseInt(document.getElementById("edit-max-dur").value) || 0,
        folder_org: document.getElementById("edit-folder-org").value,
        auto_transcribe: document.getElementById("edit-transcribe").checked,
        auto_metadata: document.getElementById("edit-metadata").checked,
        compress_enabled: document.getElementById("edit-compress").checked,
        compress_level: document.getElementById("edit-compress-quality")?.value || "Generous",
        compress_output_res: document.getElementById("edit-compress-res")?.value || "720",
        range,
        from_date,
      };
    };

    // Track which channel is being edited (for identity matching on update/remove)
    let _editingIdentity = null;
    const _origOpenPanel = openPanel;
    const wrappedOpenPanel = (mode, channel) => {
      _origOpenPanel(mode, channel);
      _editingIdentity = (mode === "edit" && channel)
        ? { url: channel.url, name: channel.folder || channel.name }
        : null;
    };
    // Reassign for the closure chain used by double-click + context menu hooks
    window._editChannelFromContext = (folder, urlGuess) => {
      const chan = { folder, url: urlGuess || ("https://www.youtube.com/@" + folder.replace(/\s+/g, "")) };
      // Try to fetch the real channel data from backend first
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

    // Double-click wiring (replaces earlier binding) — target the Subs table
    // where channel rows live. Was silently failing (ReferenceError) before.
    const _subsTbody = document.getElementById("subs-table-body");
    if (_subsTbody) {
      _subsTbody.addEventListener("dblclick", (e) => {
        const tr = e.target.closest("tr");
        if (!tr) return;
        const folder = tr.querySelector(".col-folder")?.textContent;
        if (folder) window._editChannelFromContext(folder);
      });
    }

    // Update button → Add or Update
    update.addEventListener("click", async () => {
      const payload = collectPayload();
      if (!payload.folder) { flashError("Folder name is required."); return; }
      if (!payload.url) { flashError("Channel URL is required."); return; }
      const api = window.pywebview?.api;
      if (!api) { flashError("Not running in native mode \u2014 writes disabled."); return; }

      // Duplicate pre-check when ADDING (editing is allowed to keep its own url)
      if (!_editingIdentity && api.subs_check_duplicate) {
        try {
          const dup = await api.subs_check_duplicate(payload.url, payload.folder);
          if (dup?.ok && (dup.dup_url || dup.dup_folder)) {
            const parts = [];
            if (dup.dup_url) parts.push(`\u2022 URL already used by:\n ${dup.dup_url}`);
            if (dup.dup_folder) parts.push(`\u2022 Folder name already taken by:\n ${dup.dup_folder}`);
            const proceed = await askDanger(
              "Duplicate channel",
              "This would clash with an existing subscription:\n\n" +
              parts.join("\n\n") +
              "\n\nAdding will fail on the backend. Continue anyway?",
              "Try anyway");
            if (!proceed) return;
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
      if (!res.ok) { flashError(res.error || "Unknown error"); return; }
      if (res.write_blocked) {
        flashError("Saved in memory but disk write is gated.");
      } else {
        flashOk(_editingIdentity ? "Channel updated." : "Channel added.");
      }
      await refreshSubsTable();
      closePanel();
    });

    // Remove button — two-step (Remove subscription → optional delete files).
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

    // ── Conditional group visibility toggles ──
    // Compress: show Res/Quality/Batch only when checkbox is checked
    const compressBox = document.getElementById("edit-compress");
    const compressGroup = document.getElementById("edit-compress-group");
    const syncCompressVis = () => {
      if (compressGroup) compressGroup.hidden = !compressBox?.checked;
    };
    compressBox?.addEventListener("change", syncCompressVis);
    syncCompressVis();

    // From-date: show YYYY/MM/DD boxes only when the radio is selected
    const dateGroup = document.getElementById("edit-date-group");
    document.querySelectorAll('input[name="edit-range"]').forEach(r => {
      r.addEventListener("change", (e) => {
        if (dateGroup) dateGroup.hidden = (e.target.value !== "fromdate");
        if (e.target.value === "fromdate" && dateGroup) {
          document.getElementById("edit-date-year")?.focus();
        }
        // Range is a radio group (not in _editFields which does direct
        // document.getElementById(id) wiring), so trigger the dirty
        // check manually — otherwise flipping Range wouldn't enable
        // the Update button.
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

  function flashError(msg) {
    console.warn("[subs]", msg);
    window._showToast?.(msg, "error") ?? alert(msg);
  }
  function flashOk(msg) {
    console.info("[subs]", msg);
    window._showToast?.(msg, "ok");
  }

  // ─── Recent tab context menu (matches _recent_ctx_menu at line 33174) ─
  function initRecentContextMenu() {
    const tbody = document.getElementById("recent-table-body");
    if (!tbody) return;

    // Clicking the header row (th) deselects any row in the tbody.
    // Matches YTArchiver.py:32306 _header_deselect_browse.
    const thead = tbody.parentElement?.querySelector("thead");
    thead?.addEventListener("click", (e) => {
      if (e.target.closest(".col-resizer")) return; // don't deselect on resize
      tbody.querySelectorAll("tr.row-selected").forEach(r => r.classList.remove("row-selected"));
    });

    // Extended multi-select: Ctrl/Cmd-click toggles a row, Shift-click
    // selects a range from last anchor to clicked row. Matches
    // YTArchiver.py:32411 `selectmode="extended"`.
    let _anchor = null;
    tbody.addEventListener("click", (e) => {
      const tr = e.target.closest("tr");
      if (!tr) return;
      // Don't clobber selection if the click was on a button/link inside the row.
      if (e.target.closest("button, a, input")) return;
      const rows = [...tbody.querySelectorAll("tr")];
      if (e.shiftKey && _anchor) {
        const a = rows.indexOf(_anchor);
        const b = rows.indexOf(tr);
        if (a === -1 || b === -1) return;
        const lo = Math.min(a, b), hi = Math.max(a, b);
        rows.forEach((r, i) => {
          r.classList.toggle("row-selected", i >= lo && i <= hi);
        });
      } else if (e.ctrlKey || e.metaKey) {
        tr.classList.toggle("row-selected");
        _anchor = tr;
      } else {
        rows.forEach(r => r.classList.remove("row-selected"));
        tr.classList.add("row-selected");
        _anchor = tr;
      }
    });
    // Click outside any row deselects everything. Scoped to the recent
    // panel so clicking the main log or a different tab doesn't fire.
    const recentPanel = document.getElementById("panel-recent") ||
                        tbody.closest(".tab-panel") || document;
    recentPanel.addEventListener("click", (e) => {
      if (e.target.closest("#recent-table-body tr")) return;
      if (e.target.closest(".recent-table thead")) return;
      if (e.target.closest("button, a, input, select")) return;
      tbody.querySelectorAll("tr.row-selected").forEach(r => r.classList.remove("row-selected"));
      _anchor = null;
    });

    tbody.addEventListener("contextmenu", (e) => {
      const tr = e.target.closest("tr");
      if (!tr) return;
      e.preventDefault();
      // Respect existing selection if the right-clicked row is part of it;
      // otherwise select just this one.
      if (!tr.classList.contains("row-selected")) {
        tbody.querySelectorAll("tr.row-selected").forEach(x => x.classList.remove("row-selected"));
        tr.classList.add("row-selected");
      }
      const selected = [...tbody.querySelectorAll("tr.row-selected")];
      const n = selected.length;
      const title = tr.querySelector(".col-title")?.textContent || "";
      const channel = tr.querySelector(".col-channel")?.textContent || "";
      const items = n > 1 ? [
        { label: `Delete ${n} files`, cls: "dim",
          action: async () => {
            const ok = await askDanger(
              `Delete ${n} files`,
              `Permanently delete ${n} file(s) from disk?\n\nThis cannot be undone.`,
              "Delete");
            if (!ok) return;
            for (const row of selected) {
              const t = row.querySelector(".col-title")?.textContent;
              const c = row.querySelector(".col-channel")?.textContent;
              window.pywebview?.api?.recent_delete_file?.(t, c);
            }
          }},
      ] : [
        // "Play video" opens the Browse Watch view (embedded HTML5 <video>
        // + karaoke transcript), NOT a separate VLC window.
        { label: "Play video", action: async () => {
          const api = window.pywebview?.api;
          // Resolve filepath + video_id via the recent lookup, then hand off
          // to the shared Watch-view opener used by the Browse grid.
          let fp = "", vid = "";
          try {
            const res = await api?.recent_resolve?.(title, channel);
            if (res?.ok) { fp = res.filepath || ""; vid = res.video_id || ""; }
          } catch {}
          if (!fp) {
            window._showToast?.("Couldn't locate file on disk.", "error");
            return;
          }
          window._openVideoInWatch?.({
            title, channel, filepath: fp, video_id: vid,
          });
        }},
        { label: "Play in external player", action: () => window.pywebview?.api?.recent_play?.(title, channel) },
        { label: "Show in Explorer", action: () => window.pywebview?.api?.recent_show_in_explorer?.(title, channel) },
        { label: "Open video on YouTube", action: () => window.pywebview?.api?.recent_open_youtube?.(title, channel) },
        { sep: true },
        // Re-queue download — fetches this video's URL from recent_downloads
        // and re-drives the single-video flow. Mirrors OLD's Recent menu
        // item (YTArchiver.py:33174 + friends).
        { label: "Re-queue download",
          action: async () => {
            const r = await window.pywebview?.api?.recent_requeue?.(title, channel);
            if (r?.ok) {
              window._showToast?.("Re-queued download.", "ok");
            } else {
              window._showToast?.(r?.error || "Could not requeue.", "error");
            }
          }},
        { sep: true },
        { label: "Delete File", cls: "dim",
          action: async () => {
            const ok = await askDanger("Delete file",
              `Delete "${title}" from disk?\n\nThis cannot be undone.`, "Delete");
            if (!ok) return;
            window.pywebview?.api?.recent_delete_file?.(title, channel);
          }},
      ];
      showContextMenu(e.clientX, e.clientY, items);
    });
  }

  // ─── Browse tab context menus ────────────────────────────────────────
  function initBrowseContextMenus() {
    // Channel grid cards
    const channelGrid = document.getElementById("channel-grid");
    if (channelGrid) {
      channelGrid.addEventListener("contextmenu", (e) => {
        const card = e.target.closest(".channel-card");
        if (!card) return;
        e.preventDefault();
        const name = card.querySelector(".channel-card-name")?.textContent || "";
        const api = window.pywebview?.api;
        // Live-count labels: pull pending counters stashed on the card by
        // renderChannelGrid. Matches OLD YTArchiver.py:26322 folder-level
        // "Transcribe untranscribed (N pending)" live count.
        const _pendTx = parseInt(card.dataset.pendingTx || "0", 10) || 0;
        const _pendMeta = parseInt(card.dataset.pendingMeta || "0", 10) || 0;
        const _txLabel = _pendTx > 0
          ? `Transcribe untranscribed (${_pendTx} pending)`
          : "Transcribe all missing";
        const _metaLabel = _pendMeta > 0
          ? `Recheck metadata (${_pendMeta} pending)`
          : "Recheck metadata";
        showContextMenu(e.clientX, e.clientY, [
          { label: "Open videos", action: () => card.click() },
          { label: "Show in Explorer", action: () => api?.chan_open_folder?.(name) },
          { label: "Open channel on YouTube", action: () => api?.chan_open_url?.(name) },
          { sep: true },
          { label: "Sync now", action: () => api?.sync_one_channel?.({ name }) },
          { label: _metaLabel, action: () => api?.metadata_recheck_channel?.({ name }) },
          { label: _txLabel, action: () => _askTranscribeChannel(name) },
          { sep: true },
          { label: "Reorg folder",
            submenu: [
              { label: "Flat (no split)", action: () => api?.reorg_channel_folder?.({ name }, false, false, false) },
              { label: "Split by year", action: () => api?.reorg_channel_folder?.({ name }, true, false, false) },
              { label: "Split by year + month", action: () => api?.reorg_channel_folder?.({ name }, true, true, false) },
              { label: "Re-check dates + year/month", action: () => api?.reorg_channel_folder?.({ name }, true, true, true) },
              { label: "Fix file dates only", action: () => api?.chan_fix_file_dates?.({ name }) },
            ]},
          // "Fetch channel art" removed — now bundled with the metadata sweep.
          { label: "Redownload at\u2026",
            submenu: [
              { label: "Best available", action: () => _askRedownload(name, "best") },
              { label: "2160p (4K)", action: () => _askRedownload(name, "2160") },
              { label: "1440p", action: () => _askRedownload(name, "1440") },
              { label: "1080p", action: () => _askRedownload(name, "1080") },
              { label: "720p", action: () => _askRedownload(name, "720") },
              { label: "480p", action: () => _askRedownload(name, "480") },
              { label: "360p", action: () => _askRedownload(name, "360") },
            ]},
          { sep: true },
          { label: "Edit settings", action: () => window._editChannelFromContext?.(name) },
        ]);
      });
    }

    // Video grid cards (inside a channel) — also handles right-click on
    // year headers when Group-by-year is enabled, offering per-year
    // redownload + metadata scopes. Mirrors OLD's tree-view year / month
    // folder right-click (YTArchiver.py:26462 / :26498).
    const videoGrid = document.getElementById("video-grid");
    if (videoGrid) {
      videoGrid.addEventListener("contextmenu", (e) => {
        // Year header hit? Offer year-scoped actions.
        const yearHead = e.target.closest(".video-grid-year-head");
        if (yearHead) {
          e.preventDefault();
          const section = yearHead.parentElement;
          const year = section?.dataset?.year;
          const chan = _browseState.currentChannel?.folder
                    || _browseState.currentChannel?.name
                    || "";
          if (!year || year === "?" || !chan) return;
          const api = window.pywebview?.api;
          const _scope = { year: parseInt(year, 10) };
          showContextMenu(e.clientX, e.clientY, [
            { label: `Redownload ${year} at\u2026`,
              submenu: [
                { label: "Best available",
                  action: () => api?.chan_redownload?.({ name: chan }, "best", _scope) },
                { label: "2160p (4K)",
                  action: () => api?.chan_redownload?.({ name: chan }, "2160", _scope) },
                { label: "1440p",
                  action: () => api?.chan_redownload?.({ name: chan }, "1440", _scope) },
                { label: "1080p",
                  action: () => api?.chan_redownload?.({ name: chan }, "1080", _scope) },
                { label: "720p",
                  action: () => api?.chan_redownload?.({ name: chan }, "720", _scope) },
                { label: "480p",
                  action: () => api?.chan_redownload?.({ name: chan }, "480", _scope) },
                { label: "360p",
                  action: () => api?.chan_redownload?.({ name: chan }, "360", _scope) },
              ]},
          ]);
          return;
        }
        const card = e.target.closest(".video-card");
        if (!card) return;
        e.preventDefault();
        const filepath = card.dataset.filepath || "";
        const videoId = card.dataset.videoId || "";
        const title = card.dataset.title || "";
        const ytUrl = videoId ? `https://www.youtube.com/watch?v=${videoId}` : "";
        const api = window.pywebview?.api;
        showContextMenu(e.clientX, e.clientY, [
          { label: "Play video", action: () => {
            if (filepath && api?.browse_open_video) api.browse_open_video(filepath);
          }},
          { label: "Open on YouTube", action: () => {
            if (ytUrl) window.open(ytUrl, "_blank");
          }},
          { label: "Copy YouTube URL", action: () => {
            if (ytUrl) {
              navigator.clipboard?.writeText(ytUrl);
              window._showToast?.("URL copied.", "ok");
            }
          }},
          { label: "Show in Explorer", action: () => {
            if (filepath && api?.browse_show_in_explorer) api.browse_show_in_explorer(filepath);
          }},
          { sep: true },
          { label: "Transcribe now", action: async () => {
            if (filepath && api?.transcribe_enqueue) {
              // Manual → Whisper model picker (60s countdown auto-picks default).
              const model = await (window._askWhisperModel?.(`"${title}"`));
              if (model === null) return;
              api.transcribe_enqueue(filepath, title);
              window._showToast?.("Queued for whisper.", "ok");
            }
          }},
          { label: "Re-transcribe", action: async () => {
            // Mirrors OLD YTArchiver.py:28357 `_on_retranscribe` — ask for
            // a Whisper model, then queue a GPU task. No extra "are you
            // sure" confirm (the model picker Cancel handles that).
            if (!filepath || !api?.transcribe_retranscribe) return;
            const model = await (window._askWhisperModel?.(`"${title}"`));
            if (!model) return;
            const res = await api.transcribe_retranscribe(filepath, title, videoId || "");
            if (res?.ok) window._showToast?.(
              `Queued ${model} re-transcription.`, "ok");
            else window._showToast?.(res?.error || "Re-transcribe failed.", "error");
          }},
          { label: "Redownload\u2026", action: () => bridgeCall("video_redownload", videoId, title) },
          { sep: true },
          { label: "Delete file", cls: "dim",
            action: () => bridgeCall("video_delete_file", filepath) },
        ]);
      });
    }

    // Transcript segments in Watch view
    const transcript = document.getElementById("watch-transcript");
    if (transcript) {
      transcript.addEventListener("contextmenu", (e) => {
        const seg = e.target.closest(".seg");
        if (!seg) return;
        e.preventDefault();
        const ts = seg.querySelector(".timestamp")?.textContent || "";
        const text = seg.textContent.replace(ts, "").trim();
        const api = window.pywebview?.api;
        const v = _browseState.currentVideo || {};
        showContextMenu(e.clientX, e.clientY, [
          { label: `Copy segment`, action: () => navigator.clipboard?.writeText(text) },
          { label: `Copy timestamp + text`, action: () => navigator.clipboard?.writeText(`${ts} ${text}`) },
          { sep: true },
          { label: "Bookmark this moment\u2026", action: async () => {
            if (!api?.bookmark_add) return;
            const start = _parseTs(ts.replace(/[\[\]]/g, ""));
            // Match YTArchiver.py:29412 _add_bookmark — prompt for optional note.
            const note = await askTextInput?.({
              title: "Add bookmark",
              message: `At ${ts || "this moment"}:\n\n\u201C${text.slice(0, 140)}${text.length > 140 ? "\u2026" : ""}\u201D`,
              placeholder: "Add a note (optional)",
              confirm: "Save bookmark",
              cancel: "Cancel",
              allowEmpty: true,
            }) ?? "";
            if (note === null) return; // user cancelled
            const res = await api.bookmark_add({
              video_id: v.video_id || "",
              title: v.title || "",
              channel: v.channel || "",
              start_time: start,
              text: text,
              note: note || "",
            });
            if (res?.ok) window._showToast?.("Bookmarked.", "ok");
            else window._showToast?.(res?.error || "Bookmark failed.", "error");
          }},
        ]);
      });
    }
  }

  function _parseTs(s) {
    if (!s) return 0;
    const parts = s.split(":").map(x => parseInt(x, 10) || 0);
    if (parts.length === 3) return parts[0]*3600 + parts[1]*60 + parts[2];
    if (parts.length === 2) return parts[0]*60 + parts[1];
    return 0;
  }

  // ─── Subs context menu ───────────────────────────────────────────────
  // Matches YTArchiver's _chan_ctx_menu (line 5596+): Sync now / Edit
  // settings / Open folder in Explorer / Open URL in browser / separator /
  // Organize by year / Organize by month / separator / Remove channel.
  // ─── Parent Folder native picker (Subs tab) ──────────────────────────
  function initParentFolderPicker() {
    const input = document.getElementById("parent-folder-input");
    const btn = document.getElementById("btn-browse-parent");
    if (!btn) return;
    btn.addEventListener("click", async () => {
      const api = window.pywebview?.api;
      if (!api?.pick_folder) {
        window._showToast?.("Folder picker only works in native mode.", "warn");
        return;
      }
      const current = input?.value || "";
      try {
        const res = await api.pick_folder("Choose archive root", current);
        if (res?.ok && res.path) {
          if (input) input.value = res.path;
          const saveRes = await api.set_parent_folder(res.path);
          if (saveRes?.ok) {
            window._showToast?.("Parent folder saved.", "ok");
          } else if (saveRes?.write_blocked) {
            window._showToast?.("Write-gate off \u2014 path not saved to disk.", "warn");
          } else {
            window._showToast?.(saveRes?.error || "Save failed.", "error");
          }
        }
      } catch (e) { window._showToast?.("Error: " + e, "error"); }
    });
  }

  // ─── Column sort on Subs + Recent tables ─────────────────────────────
  function initColumnSort() {
    // Subs table
    const subsThead = document.querySelector(".subs-table thead");
    if (subsThead) wireTableSort(subsThead, "subs-table-body",
                                 { folder: "string", res: "string",
                                   min: "num", max: "num",
                                   compress: "string", transcribed: "string", metadata: "string",
                                   last_sync: "string", n_vids: "num",
                                   size: "size", avg_size: "size" });
    // Recent table
    const recentThead = document.querySelector(".recent-table thead");
    if (recentThead) wireTableSort(recentThead, "recent-table-body",
                                   { title: "string", channel: "string",
                                     time: "age", duration: "dur", size: "size" });
  }

  function wireTableSort(thead, tbodyId, kinds) {
    const ths = thead.querySelectorAll("th");
    let currentSort = { col: null, dir: 1 };
    ths.forEach((th, i) => {
      th.style.cursor = "pointer";
      th.addEventListener("click", () => {
        const col = th.dataset.sort || th.textContent.trim().toLowerCase();
        const dir = (currentSort.col === col) ? -currentSort.dir : 1;
        currentSort = { col, dir };
        sortTableBody(tbodyId, i, kinds[col] || "string", dir);
        // Arrow indicator
        ths.forEach(x => x.dataset.arrow = "");
        th.dataset.arrow = dir > 0 ? "\u25B2" : "\u25BC";
      });
    });
  }

  function sortTableBody(tbodyId, colIdx, kind, dir) {
    const tbody = document.getElementById(tbodyId);
    if (!tbody) return;
    const rows = Array.from(tbody.querySelectorAll("tr"));
    rows.sort((a, b) => {
      const av = (a.cells[colIdx]?.textContent || "").trim();
      const bv = (b.cells[colIdx]?.textContent || "").trim();
      const cmp = compareByKind(av, bv, kind);
      return dir > 0 ? cmp : -cmp;
    });
    const frag = document.createDocumentFragment();
    rows.forEach(r => frag.appendChild(r));
    tbody.appendChild(frag);
  }

  function compareByKind(a, b, kind) {
    if (kind === "num") {
      const ai = parseFloat(a.replace(/[^\d.\-]/g, "")) || 0;
      const bi = parseFloat(b.replace(/[^\d.\-]/g, "")) || 0;
      return ai - bi;
    }
    if (kind === "size") {
      return parseBytes(a) - parseBytes(b);
    }
    if (kind === "dur") {
      return parseDuration(a) - parseDuration(b);
    }
    if (kind === "age") {
      return parseAge(a) - parseAge(b);
    }
    return a.toLowerCase().localeCompare(b.toLowerCase());
  }
  function parseBytes(s) {
    if (!s) return 0;
    const m = s.match(/([\d.]+)\s*(KB|MB|GB|TB|B)/i);
    if (!m) return 0;
    const mult = { b: 1, kb: 1024, mb: 1024**2, gb: 1024**3, tb: 1024**4 }[m[2].toLowerCase()] || 1;
    return parseFloat(m[1]) * mult;
  }
  function parseDuration(s) {
    if (!s) return 0;
    const parts = s.split(":").map(x => parseInt(x, 10) || 0);
    if (parts.length === 3) return parts[0]*3600 + parts[1]*60 + parts[2];
    if (parts.length === 2) return parts[0]*60 + parts[1];
    return 0;
  }
  function parseAge(s) {
    if (!s) return 0;
    const m = s.match(/(\d+)\s*(m|h|d|w)/i);
    if (!m) return 0;
    const n = parseInt(m[1], 10);
    const unit = { m: 60, h: 3600, d: 86400, w: 604800 }[m[2].toLowerCase()] || 60;
    return n * unit;
  }

  function initSubsContextMenu() {
    const tbody = document.getElementById("subs-table-body");
    if (!tbody) return;

    // Make the tbody focusable so keyboard events fire when the Subs tab is
    // focused. Single-click selects the row; Enter opens edit; Delete removes.
    tbody.setAttribute("tabindex", "0");
    tbody.addEventListener("click", (e) => {
      const tr = e.target.closest("tr");
      if (!tr) return;
      tbody.querySelectorAll("tr.row-selected").forEach(x => x.classList.remove("row-selected"));
      tr.classList.add("row-selected");
      tbody.focus();
    });
    tbody.addEventListener("keydown", async (e) => {
      const selected = tbody.querySelector("tr.row-selected");
      if (!selected) return;
      const folder = selected.querySelector(".col-folder")?.textContent;
      if (!folder) return;
      if (e.key === "Enter" || e.key === "F2") {
        e.preventDefault();
        window._editChannelFromContext?.(folder);
      } else if (e.key === "Delete") {
        e.preventDefault();
        const res = await window._removeChannelWithPrompt(folder);
        if (res && res.ok) location.reload();
      } else if (e.key === "ArrowDown" || e.key === "ArrowUp") {
        e.preventDefault();
        const allTrs = [...tbody.querySelectorAll("tr")];
        const idx = allTrs.indexOf(selected);
        const next = e.key === "ArrowDown" ? Math.min(allTrs.length - 1, idx + 1) : Math.max(0, idx - 1);
        allTrs.forEach(t => t.classList.remove("row-selected"));
        allTrs[next].classList.add("row-selected");
        allTrs[next].scrollIntoView({ block: "nearest" });
      }
    });

    tbody.addEventListener("contextmenu", (e) => {
      const tr = e.target.closest("tr");
      if (!tr) return;
      e.preventDefault();
      // Visual select
      tbody.querySelectorAll("tr.row-selected").forEach(x => x.classList.remove("row-selected"));
      tr.classList.add("row-selected");
      const chan = tr.querySelector(".col-folder")?.textContent || "";

      const api = window.pywebview?.api;
      // Dynamic-label helpers — peek at live queue state so menu items
      // reflect what's already queued. Matches OLD's _chan_ctx_menu label
      // mutation (YTArchiver.py:5596 — "Add to Sync List" → "Already in
      // Sync List" → "Channel Transcribing...").
      const _syncState = window._queueHasSyncForChannel?.(chan);
      const _gpuState = window._queueHasGpuForChannel?.(chan);
      const _syncLabel = _syncState === "running" ? "Syncing now \u2026 (already running)"
                       : _syncState === "queued" ? "Already in Sync queue"
                                                  : "Sync now";
      const _syncDisabled = Boolean(_syncState);
      const _txLabel = _gpuState === "running" ? "Channel transcribing \u2026"
                     : _gpuState === "queued" ? "Already queued for transcribe"
                                               : "Transcribe channel";
      const _txDisabled = Boolean(_gpuState);
      // Match YTArchiver.py _chan_ctx_menu (line 5596-6180): 15-item menu
      // with sub-menus for organization mode + redownload quality.
      showContextMenu(e.clientX, e.clientY, [
        { label: _syncLabel, cls: _syncDisabled ? "dim" : "",
          action: () => { if (!_syncDisabled) api?.sync_one_channel?.({ name: chan }); }},
        { label: "Edit settings", action: () => window._editChannelFromContext?.(chan) },
        { label: "Open folder in Explorer", action: () => api?.chan_open_folder?.(chan) },
        { label: "Open URL in browser", action: () => api?.chan_open_url?.(chan) },
        { sep: true },
        { label: "Reorg folder",
          submenu: [
            { label: "Flat (no split)", action: () => api?.reorg_channel_folder?.({ name: chan }, false, false, false) },
            { label: "Split by year", action: () => api?.reorg_channel_folder?.({ name: chan }, true, false, false) },
            { label: "Split by year + month", action: () => api?.reorg_channel_folder?.({ name: chan }, true, true, false) },
            { label: "Re-apply organization", action: () => api?.reorg_channel_folder?.({ name: chan }, null, null, false) },
            // Recheck-dates + fix-file-dates are long operations — OLD app
            // shows an all-caps warning dialog. YTArchiver.py:5721-5742.
            { label: "Re-check dates + year/month", action: async () => {
              const ok = await askDanger("Re-check dates",
                `Re-check upload dates for every video in "${chan}" and re-sort into Year/Month folders?\n\n` +
                `\u26A0\uFE0F WARNING: THIS CAN TAKE MULTIPLE HOURS ON LARGE CHANNELS`,
                "Re-check dates");
              if (!ok) return;
              api?.reorg_channel_folder?.({ name: chan }, true, true, true);
            }},
            { label: "Fix file dates only", action: async () => {
              const ok = await askDanger("Fix file dates",
                `Re-fetch upload dates from YouTube for every video in "${chan}" ` +
                `and stamp each file's mtime to match?\n\n` +
                `\u26A0\uFE0F WARNING: THIS CAN TAKE MULTIPLE HOURS ON LARGE CHANNELS`,
                "Fix dates");
              if (!ok) return;
              api?.chan_fix_file_dates?.({ name: chan });
            }},
          ]},
        // "Fetch channel art" used to live here but the user flagged it as
        // redundant — channel art is fetched automatically as part of the
        // full metadata sweep. Removed to keep the menu focused.
        { sep: true },
        { label: _txLabel, cls: _txDisabled ? "dim" : "",
          action: () => { if (!_txDisabled) _askTranscribeChannel(chan); }},
        { label: "Download metadata", action: () => api?.metadata_recheck_channel?.({ name: chan }) },
        { sep: true },
        { label: "Redownload at\u2026",
          submenu: [
            { label: "Best available", action: () => _askRedownload(chan, "best") },
            { label: "2160p (4K)", action: () => _askRedownload(chan, "2160") },
            { label: "1440p", action: () => _askRedownload(chan, "1440") },
            { label: "1080p", action: () => _askRedownload(chan, "1080") },
            { label: "720p", action: () => _askRedownload(chan, "720") },
            { label: "480p", action: () => _askRedownload(chan, "480") },
            { label: "360p", action: () => _askRedownload(chan, "360") },
          ]},
        { sep: true },
        { label: "Remove channel", cls: "dim", action: async () => {
          // Two-step (subscription → optional disk delete) via shared helper.
          const res = await window._removeChannelWithPrompt(chan);
          if (!res || !res.ok) return;
          // Refresh Subs data without a full page reload, so the undo toast survives
          if (api?.get_subs_channels) {
            const data = await api.get_subs_channels();
            if (Array.isArray(data) && data.length === 2) {
              window.renderSubsTable(data[0], data[1]);
              window._primeBrowse?.(data[0]);
            }
          }
          if (res?.can_undo) {
            window._showToast({
              msg: `Removed "${chan}".`,
              kind: "warn",
              ttlMs: 10_000,
              action: {
                label: "Undo",
                onClick: async () => {
                  const u = await api.subs_undo_remove();
                  if (u?.ok) {
                    window._showToast?.("Channel restored.", "ok");
                    const data = await api.get_subs_channels();
                    if (Array.isArray(data) && data.length === 2) {
                      window.renderSubsTable(data[0], data[1]);
                      window._primeBrowse?.(data[0]);
                    }
                  } else {
                    window._showToast?.(u?.error || "Undo failed.", "error");
                  }
                },
              },
            });
          }
        }},
      ]);
    });
  }

  function bridgeCall(method, ...args) {
    if (window.pywebview && window.pywebview.api && window.pywebview.api[method]) {
      return window.pywebview.api[method](...args);
    } else {
      console.info("[bridge] no pywebview — would call", method, args);
    }
  }

  // Expose as a global so logs.js (and future modules) can reach it
  window.showContextMenu = showContextMenu;

  function showContextMenu(x, y, items) {
    closeContextMenu();
    const root = document.getElementById("ctx-menu-root");
    const menu = document.createElement("div");
    menu.className = "ctx-menu";
    menu.style.left = x + "px";
    menu.style.top = y + "px";
    for (const it of items) {
      if (it.sep) {
        const sep = document.createElement("div");
        sep.className = "ctx-menu-sep";
        menu.appendChild(sep);
        continue;
      }
      const row = document.createElement("div");
      row.className = "ctx-menu-item" + (it.cls ? " " + it.cls : "");
      row.textContent = it.label;
      if (it.submenu) {
        row.classList.add("ctx-submenu-wrap");
        const sub = document.createElement("div");
        sub.className = "ctx-submenu";
        for (const sit of it.submenu) {
          const srow = document.createElement("div");
          srow.className = "ctx-menu-item";
          srow.textContent = sit.label;
          srow.addEventListener("click", (e) => {
            e.stopPropagation();
            closeContextMenu();
            if (sit.action) sit.action();
          });
          sub.appendChild(srow);
        }
        row.appendChild(sub);
      } else if (it.action) {
        row.addEventListener("click", () => {
          closeContextMenu();
          it.action();
        });
      }
      menu.appendChild(row);
    }
    root.appendChild(menu);
    // Clamp to viewport
    const r = menu.getBoundingClientRect();
    if (r.right > window.innerWidth) menu.style.left = (window.innerWidth - r.width - 4) + "px";
    if (r.bottom > window.innerHeight) menu.style.top = (window.innerHeight - r.height - 4) + "px";
    // Close on outside click / Escape
    setTimeout(() => {
      document.addEventListener("click", closeContextMenu, { once: true });
      document.addEventListener("keydown", onCtxKey);
    }, 0);
  }
  function closeContextMenu() {
    const root = document.getElementById("ctx-menu-root");
    if (root) root.innerHTML = "";
    document.removeEventListener("keydown", onCtxKey);
  }
  function onCtxKey(e) {
    if (e.key === "Escape") closeContextMenu();
  }

  // ─── Log right-click menu (Copy / Copy all / Clear / Save to file) ──
  function initLogContextMenu() {
    const handlers = [
      { el: document.getElementById("main-log"), label: "Main log" },
      { el: document.getElementById("activity-log"), label: "Activity log" },
      { el: document.getElementById("subs-mini-log"), label: "Mini log" },
      { el: document.getElementById("recent-mini-log"), label: "Mini log" },
      { el: document.getElementById("browse-mini-log"), label: "Mini log" },
      { el: document.getElementById("settings-mini-log"), label: "Mini log" },
    ];
    for (const { el, label } of handlers) {
      if (!el) continue;
      el.addEventListener("contextmenu", (e) => {
        e.preventDefault();
        const sel = window.getSelection()?.toString() || "";
        const lineEl = e.target.closest(".log-line");
        const items = [];
        if (sel) {
          items.push({ label: "Copy selection",
            action: () => navigator.clipboard?.writeText(sel) });
        }
        if (lineEl) {
          items.push({ label: "Copy this line",
            action: () => navigator.clipboard?.writeText(lineEl.innerText) });
        }
        items.push({ label: `Copy all (${label})`,
          action: () => navigator.clipboard?.writeText(el.innerText) });
        items.push({ sep: true });
        items.push({ label: "Save to file\u2026",
          action: async () => {
            const text = el.innerText;
            if (window.pywebview?.api?.save_text_to_file) {
              const res = await window.pywebview.api.save_text_to_file("ytarchiver_log.txt", text);
              if (res?.ok) window._showToast?.("Log saved.", "ok");
              else window._showToast?.(res?.error || "Save failed.", "error");
            } else {
              // Fallback: blob download (works in browser preview)
              const blob = new Blob([text], { type: "text/plain" });
              const url = URL.createObjectURL(blob);
              const a = document.createElement("a");
              a.href = url; a.download = "ytarchiver_log.txt"; a.click();
              setTimeout(() => URL.revokeObjectURL(url), 1000);
            }
          }});
        items.push({ sep: true });
        items.push({ label: "Clear", cls: "dim",
          action: async () => {
            const ok = await askConfirm("Clear log",
              `Clear the ${label.toLowerCase()}?\n\nThis only clears the visible log \u2014 no files on disk are affected.`,
              { confirm: "Clear", danger: true });
            if (!ok) return;
            el.innerHTML = "";
            if (label === "Activity log") syncActivityLogVisibility();
          }});
        showContextMenu(e.clientX, e.clientY, items);
      });
    }
  }

  // ─── URL field + Download button ────────────────────────────────────
  //
  // Behavior matches YTArchiver.py:19706-19708 + _validate_download_btn:
  // - Field is empty → "▶ Download" hidden, Sync Subbed is the main action
  // - YouTube URL typed → "▶ Download" appears next to the URL field
  // - Click Download OR press Enter → calls archive_single_video + clears input
  // - Escape clears the field
  //
  // The old "Paste & archive" button is gone — pasting a URL just shows the
  // Download button, which is more discoverable and matches the original.
  const _YT_RE = /(?:youtube\.com\/(?:watch\?v=|shorts\/|embed\/|live\/)|youtu\.be\/)[\w-]{6,}/i;

  function initUrlField() {
    const input = document.getElementById("url-input");
    const btn = document.getElementById("btn-download-single");
    const errRow = document.getElementById("url-error-row");
    const errText = document.getElementById("url-error-text");
    const voPanel = document.getElementById("video-opts-panel");
    const nudgePanel = document.getElementById("channel-nudge-panel");
    if (!input || !btn) return;

    const urlLooksLikeVideo = (s) => _YT_RE.test((s || "").trim());
    const urlLooksLikeChannel = (s) => {
      const t = (s || "").trim();
      if (!t) return false;
      return /youtube\.com\/@|youtube\.com\/c\/|youtube\.com\/channel\/|youtube\.com\/user\/|youtube\.com\/playlist/i.test(t);
    };
    // Show persistent error below URL field when input doesn't look like a
    // recognized YouTube URL. Matches YTArchiver.py:17060 url_error_var.
    const setErr = (msg) => {
      if (!errRow || !errText) return;
      if (msg) { errText.textContent = msg; errRow.hidden = false; }
      else { errText.textContent = ""; errRow.hidden = true; }
    };
    const refreshErr = () => {
      const t = (input.value || "").trim();
      if (!t) { setErr(""); return; }
      if (urlLooksLikeVideo(t) || urlLooksLikeChannel(t)) { setErr(""); return; }
      setErr("Invalid URL (must be a YouTube video, channel, or playlist).");
    };

    // Panel visibility — matches YTArchiver.py:17008 process_url_update flow.
    // Show video-options when URL is a video; show channel-nudge when it's
    // a channel URL we don't already have in subs.
    const refreshPanels = () => {
      const t = (input.value || "").trim();
      const isVid = urlLooksLikeVideo(t);
      const isChan = !isVid && urlLooksLikeChannel(t);
      if (voPanel) voPanel.hidden = !isVid;
      if (nudgePanel) nudgePanel.hidden = !isChan;
    };

    const updateBtnVisibility = () => {
      const show = urlLooksLikeVideo(input.value);
      btn.hidden = !show;
      refreshErr();
      refreshPanels();
    };

    // Read the Video-options panel into a plain dict to send to the backend.
    const readVideoOptions = () => {
      const saveTo = document.getElementById("vo-save-to")?.value?.trim() || "";
      const res = document.getElementById("vo-resolution")?.value || "1080";
      const dateFile = !!document.getElementById("vo-date-file")?.checked;
      const addDate = !!document.getElementById("vo-add-date")?.checked;
      const useYtTitle = !!document.getElementById("vo-use-yt-title")?.checked;
      const customName = document.getElementById("vo-custom-name")?.value?.trim() || "";
      return {
        save_to: saveTo,
        resolution: res,
        date_file: dateFile,
        add_date: addDate,
        use_yt_title: useYtTitle,
        custom_name: customName,
      };
    };

    const submit = async () => {
      const url = (input.value || "").trim();
      if (!urlLooksLikeVideo(url)) {
        window._showToast?.("Paste a YouTube video URL first.", "warn");
        return;
      }
      if (!window.pywebview?.api?.archive_single_video) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      const opts = readVideoOptions();
      await window.pywebview.api.archive_single_video(url, opts);
      window._showToast?.("Queued: " + url.slice(0, 60), "ok");
      input.value = "";
      updateBtnVisibility();
    };

    input.addEventListener("input", updateBtnVisibility);
    input.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        if (urlLooksLikeVideo(input.value)) submit();
        else if (!input.value.trim()) document.getElementById("btn-sync-subbed")?.click();
      } else if (e.key === "Escape") {
        input.value = "";
        updateBtnVisibility();
      }
    });
    // paste fires before `input` in some engines — delay the sync so the
    // pasted text is actually reflected in input.value
    input.addEventListener("paste", () => setTimeout(updateBtnVisibility, 10));
    btn.addEventListener("click", submit);

    // Video options: Use-YT-title ↔ custom-name enable/disable.
    // Mirrors YTArchiver.py:4436 _toggle_custom_name.
    const useYtTitleCB = document.getElementById("vo-use-yt-title");
    const customNameInput = document.getElementById("vo-custom-name");
    const syncCustomName = () => {
      if (!customNameInput || !useYtTitleCB) return;
      customNameInput.disabled = useYtTitleCB.checked;
    };
    useYtTitleCB?.addEventListener("change", syncCustomName);
    syncCustomName();

    // Save-to folder Browse button → pywebview native folder picker
    document.getElementById("vo-save-to-browse")?.addEventListener("click", async () => {
      const api = window.pywebview?.api;
      if (!api?.pick_folder) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      const saveInput = document.getElementById("vo-save-to");
      const current = saveInput?.value || "";
      const res = await api.pick_folder("Save video to…", current);
      if (res?.ok && res.path) {
        if (saveInput) saveInput.value = res.path;
      }
    });

    // Channel-nudge button: swap to Subs tab + pre-fill the Add Channel URL
    // field (edit-url — the new Subs panel collapses to edit-form, there's
    // no separate "Add" form). Mirrors YTArchiver.py:4462 _go_to_add_channel.
    document.getElementById("btn-channel-nudge-add")?.addEventListener("click", () => {
      const url = (input.value || "").trim();
      if (!url) return;
      // Switch to Subs tab
      document.querySelector('.tab[data-tab="subs"]')?.click();
      // Seed the edit-url field + fire input so collapsed panel expands
      setTimeout(() => {
        const urlField = document.getElementById("edit-url");
        if (urlField) {
          urlField.value = url;
          urlField.dispatchEvent(new Event("input", { bubbles: true }));
          urlField.scrollIntoView({ behavior: "smooth", block: "center" });
          urlField.focus();
        }
      }, 80);
      // Clear the Download-tab URL so the nudge hides
      input.value = "";
      updateBtnVisibility();
    });

    // Initial sync in case there's a value restored from somewhere
    updateBtnVisibility();
  }

  // ─── Drag-and-drop URL on Download tab ───────────────────────────────
  function initDragDropUrl() {
    const panel = document.getElementById("panel-download");
    if (!panel) return;
    panel.addEventListener("dragover", (e) => {
      e.preventDefault();
      panel.classList.add("drag-hover");
    });
    panel.addEventListener("dragleave", () => {
      panel.classList.remove("drag-hover");
    });
    panel.addEventListener("drop", async (e) => {
      e.preventDefault();
      panel.classList.remove("drag-hover");
      // Prefer URL (from address bar drag); fall back to text
      const url = e.dataTransfer.getData("text/uri-list") ||
                  e.dataTransfer.getData("text/plain");
      if (!url) return;
      const trimmed = url.trim();
      if (!/youtube|youtu\.be/i.test(trimmed)) {
        window._showToast?.("Drop a YouTube URL to archive.", "warn");
        return;
      }
      const input = document.querySelector("#panel-download .ctl-input");
      if (input) input.value = trimmed;
      if (window.pywebview?.api?.archive_single_video) {
        await window.pywebview.api.archive_single_video(trimmed);
        window._showToast?.("Queued: " + trimmed.slice(0, 60), "ok");
      }
    });
  }

  // ─── Keyboard shortcuts ──────────────────────────────────────────────
  function initKeyboardShortcuts() {
    document.addEventListener("keydown", (e) => {
      // Ignore when typing in an input
      const tag = e.target.tagName;
      const editing = tag === "INPUT" || tag === "TEXTAREA" ||
                      e.target.isContentEditable;

      // Ctrl+Q: quit (close window via tray-quit path)
      if ((e.ctrlKey || e.metaKey) && e.key === "q") {
        e.preventDefault();
        if (window.pywebview?.api?.window_quit) window.pywebview.api.window_quit();
        return;
      }
      // Ctrl+F: focus Browse > Search input (if on Browse)
      if ((e.ctrlKey || e.metaKey) && e.key === "f") {
        const searchInput = document.getElementById("search-query");
        if (searchInput && document.getElementById("view-search").style.display !== "none") {
          e.preventDefault();
          searchInput.focus();
          searchInput.select();
        }
        return;
      }
      // F5: reload (dev convenience)
      if (e.key === "F5") {
        e.preventDefault();
        location.reload();
        return;
      }
      // F11: fullscreen toggle (native via pywebview, HTML5 in browser preview)
      if (e.key === "F11") {
        e.preventDefault();
        const api = window.pywebview?.api;
        if (api?.window_toggle_fullscreen) {
          api.window_toggle_fullscreen();
        } else if (document.fullscreenElement) {
          document.exitFullscreen?.();
        } else {
          document.documentElement.requestFullscreen?.();
        }
        return;
      }
      // Ctrl+L: focus the URL field on Download tab
      if ((e.ctrlKey || e.metaKey) && e.key === "l") {
        e.preventDefault();
        const tabs = document.querySelectorAll(".tab");
        tabs[0]?.click();
        const input = document.querySelector("#panel-download .ctl-input");
        if (input) { input.focus(); input.select(); }
        return;
      }
      // Ctrl+S: Sync Subbed
      if ((e.ctrlKey || e.metaKey) && e.key === "s") {
        e.preventDefault();
        document.getElementById("btn-sync-subbed")?.click();
        return;
      }
      // Ctrl+K: jump to Subs tab + focus filter
      if ((e.ctrlKey || e.metaKey) && e.key === "k") {
        e.preventDefault();
        const tabs = document.querySelectorAll(".tab");
        tabs[1]?.click();
        return;
      }
      // Ctrl+P: open Sync Tasks popover
      if ((e.ctrlKey || e.metaKey) && e.key === "p") {
        e.preventDefault();
        document.getElementById("btn-sync-tasks")?.click();
        return;
      }
      // Number keys 1-5: switch tabs (Download / Subs / Recent / Settings / Browse)
      if (!editing && /^[1-5]$/.test(e.key)) {
        const tabs = document.querySelectorAll(".tab");
        const idx = parseInt(e.key, 10) - 1;
        if (tabs[idx]) tabs[idx].click();
        return;
      }
      // Escape: close context menus, popovers, dialogs
      if (e.key === "Escape") {
        closeContextMenu();
        document.querySelectorAll(".queue-popover.open").forEach(p => p.classList.remove("open"));
      }
    });
  }

  // ─── Clear log button wiring ─────────────────────────────────────────
  function initClearLog() {
    // "Clear log" button (next to Sync Subbed) — clears ONLY the main log,
    // matching original YTArchiver behavior. Activity log has its own
    // separate Clear button below it.
    const btn = document.getElementById("btn-clear-log");
    if (btn) {
      btn.addEventListener("click", async () => {
        const ok = await askConfirm("Clear log",
          "Clear the main log?\n\nThis only clears the visible log \u2014 no files are affected.",
          { confirm: "Clear", danger: true });
        if (!ok) return;
        window.clearLog?.("main-log");
      });
    }

    // "Clear" button in the activity-log bar — empties the activity log
    // history (the top strip showing Trnscr / Metdta / Sync rows) AND
    // persists the clear to config so relaunch doesn't restore it.
    const actBtn = document.getElementById("btn-clear");
    if (actBtn) {
      actBtn.addEventListener("click", async () => {
        const ok = await askConfirm("Clear activity log",
          "Permanently clear the activity-log history? This cannot be undone.",
          { confirm: "Clear", danger: true });
        if (!ok) return;
        // Persist the clear BEFORE wiping the UI — if the backend fails,
        // the UI still shows the stale entries so the user knows.
        const api = window.pywebview?.api;
        if (api?.autorun_history_clear) {
          try {
            const res = await api.autorun_history_clear();
            if (!res?.ok) {
              window._showToast?.(res?.error || "Clear failed.", "error");
              return;
            }
          } catch (e) {
            window._showToast?.("Clear failed: " + e, "error");
            return;
          }
        }
        window.clearLog?.("activity-log");
        syncActivityLogVisibility();
        window._showToast?.("Activity log cleared.", "ok");
      });
    }
  }

  // ─── Watch view action buttons ───────────────────────────────────────
  function initWatchActions() {
    // Playback speed
    const speedSel = document.getElementById("watch-speed");
    const vEl = document.getElementById("watch-video");
    speedSel?.addEventListener("change", () => {
      if (vEl) vEl.playbackRate = parseFloat(speedSel.value) || 1.0;
    });

    // Video-scoped keyboard shortcuts (only active when the watch view is visible)
    document.addEventListener("keydown", (e) => {
      const watchVisible = document.getElementById("view-watch")
        && document.getElementById("view-watch").style.display !== "none";
      if (!watchVisible || !vEl) return;
      const editing = e.target.tagName === "INPUT" || e.target.isContentEditable;
      if (editing) return;
      if (e.key === " " || e.code === "Space") {
        e.preventDefault();
        if (vEl.paused) vEl.play().catch(()=>{}); else vEl.pause();
      } else if (e.key === "ArrowLeft") {
        e.preventDefault();
        vEl.currentTime = Math.max(0, vEl.currentTime - 5);
      } else if (e.key === "ArrowRight") {
        e.preventDefault();
        vEl.currentTime = Math.min(vEl.duration || vEl.currentTime, vEl.currentTime + 5);
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        vEl.volume = Math.min(1, (vEl.volume || 0) + 0.1);
      } else if (e.key === "ArrowDown") {
        e.preventDefault();
        vEl.volume = Math.max(0, (vEl.volume || 0) - 0.1);
      } else if (e.key === "b" || e.key === "B") {
        e.preventDefault();
        document.getElementById("btn-bookmark-now")?.click();
      } else if (e.key === "m" || e.key === "M") {
        e.preventDefault();
        vEl.muted = !vEl.muted;
      }
    });

    document.getElementById("btn-open-external")?.addEventListener("click", () => {
      const v = _browseState.currentVideo;
      if (!v?.filepath) { window._showToast?.("No file loaded.", "warn"); return; }
      window.pywebview?.api?.browse_open_video?.(v.filepath);
    });
    // Redownload current video — opens a resolution picker, then calls
    // backend.video_redownload. Mirrors the grid card right-click action.
    document.getElementById("btn-watch-redownload")?.addEventListener("click", async () => {
      const v = _browseState.currentVideo;
      if (!v?.video_id && !v?.filepath) {
        window._showToast?.("No video loaded.", "warn");
        return;
      }
      const pick = await (window.askChoice ? window.askChoice({
        title: "Redownload at\u2026",
        message: `Replace the local file for "${v.title || v.video_id}" ` +
                 `with a new download at the chosen resolution. This keeps the ` +
                 `existing filename so transcripts and bookmarks still match.`,
        // Low → high order matches OLD YTArchiver's RESOLUTION_OPTIONS
        // list at YTArchiver.py:197 (`audio`, `144`, `240`, …, `2160`,
        // `best`) — reads left-to-right as "what size do I want?"
        // rather than reverse-descending. Best stays primary (default).
        choices: [
          { label: "360p", value: "360" },
          { label: "480p", value: "480" },
          { label: "720p", value: "720" },
          { label: "1080p", value: "1080" },
          { label: "1440p", value: "1440" },
          { label: "2160p (4K)", value: "2160" },
          { label: "Best available", value: "best", primary: true },
        ],
      }) : null);
      if (!pick) return;
      bridgeCall("video_redownload", v.video_id || "", v.title || "", pick);
      window._showToast?.(`Redownload queued at ${pick}.`, "ok");
    });

    // Re-transcribe current video — matches OLD YTArchiver.py:28357
    // `_on_retranscribe`: show the 4-option Whisper model picker and
    // queue a GPU task. No separate "are you sure" confirm — the model
    // dialog IS the confirm (Cancel dismisses, Escape dismisses). OLD
    // doesn't warn about deleted sidecars because transcripts aren't
    // per-video sidecars — they're appended to a per-month aggregated
    // Transcript.txt managed by the transcription pipeline.
    document.getElementById("btn-watch-retranscribe")?.addEventListener("click", async () => {
      const v = _browseState.currentVideo;
      const api = window.pywebview?.api;
      if (!api?.transcribe_retranscribe || !v?.filepath) {
        window._showToast?.("No file loaded.", "warn");
        return;
      }
      const model = await (window._askWhisperModel?.(`"${v.title}"`));
      if (!model) return; // user cancelled
      const res = await api.transcribe_retranscribe(
        v.filepath, v.title || "", v.video_id || "");
      if (res?.ok) {
        window._showToast?.(
          `Queued ${model} re-transcription.`, "ok");
      } else {
        window._showToast?.(res?.error || "Re-transcribe failed.", "error");
      }
    });

    document.getElementById("btn-bookmark-now")?.addEventListener("click", async () => {
      const vEl = document.getElementById("watch-video");
      const v = _browseState.currentVideo;
      if (!v || !window.pywebview?.api?.bookmark_add) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      const t = vEl ? vEl.currentTime : 0;
      // Find current transcript text
      const segs = document.querySelectorAll("#watch-transcript .seg");
      let text = "";
      for (const s of segs) {
        if (s.classList.contains("active")) { text = s.textContent; break; }
      }
      const res = await window.pywebview.api.bookmark_add({
        video_id: v.video_id || "",
        title: v.title || "",
        channel: v.channel || "",
        start_time: t,
        text: text.slice(0, 200),
      });
      if (res?.ok) window._showToast?.("Bookmarked @ " + _formatTs(t), "ok");
      else window._showToast?.(res?.error || "Bookmark failed.", "error");
    });

    // Watch-find: cycle through ALL matches. Matches YTArchiver.py:29682
    // Ctrl+F → Enter loops through matches with persistent highlight +
    // running "3 of 11" count.
    const watchFind = document.getElementById("watch-find");
    const watchFindCount = document.getElementById("watch-find-count");
    const watchFindNext = document.getElementById("watch-find-next");
    const watchFindPrev = document.getElementById("watch-find-prev");
    const findState = { matches: [], idx: -1, q: "" };

    function _rebuildFindMatches() {
      const q = (watchFind?.value || "").toLowerCase().trim();
      const tr = document.getElementById("watch-transcript");
      if (!tr) return;
      // Clear all marks
      tr.querySelectorAll(".find-hit, .find-hit-active").forEach(e => {
        e.classList.remove("find-hit", "find-hit-active");
      });
      findState.q = q;
      findState.matches = [];
      findState.idx = -1;
      if (!q) {
        if (watchFindCount) watchFindCount.textContent = "";
        return;
      }
      const segs = tr.querySelectorAll(".seg");
      for (const s of segs) {
        if (s.textContent.toLowerCase().includes(q)) {
          s.classList.add("find-hit");
          findState.matches.push(s);
        }
      }
      if (watchFindCount) {
        watchFindCount.textContent = findState.matches.length
          ? `0 of ${findState.matches.length}`
          : "no matches";
      }
      // Auto-jump to first match
      if (findState.matches.length) _findGoTo(0);
    }

    function _findGoTo(i) {
      if (!findState.matches.length) return;
      // Clamp + wrap
      const n = findState.matches.length;
      const idx = ((i % n) + n) % n;
      // Update active highlight
      if (findState.idx >= 0 && findState.matches[findState.idx]) {
        findState.matches[findState.idx].classList.remove("find-hit-active");
      }
      findState.idx = idx;
      const el = findState.matches[idx];
      if (el) {
        el.classList.add("find-hit-active");
        // Container-local scroll — scrollIntoView would also scroll
        // the outer .browse-view and push the video out of frame.
        const tr = document.getElementById("watch-transcript");
        if (tr && window._scrollTranscriptTo) {
          window._scrollTranscriptTo(tr, el);
        } else {
          el.scrollIntoView({ behavior: "smooth", block: "center" });
        }
      }
      if (watchFindCount) {
        watchFindCount.textContent = `${idx + 1} of ${n}`;
      }
    }

    watchFind?.addEventListener("input", _rebuildFindMatches);
    watchFind?.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        if (!findState.matches.length) {
          _rebuildFindMatches();
          return;
        }
        _findGoTo(findState.idx + (e.shiftKey ? -1 : 1));
      } else if (e.key === "Escape") {
        watchFind.value = "";
        _rebuildFindMatches();
        watchFind.blur();
      }
    });
    watchFindNext?.addEventListener("click", () => _findGoTo(findState.idx + 1));
    watchFindPrev?.addEventListener("click", () => _findGoTo(findState.idx - 1));
  }

  // ─── Activity log auto-hide when empty ───────────────────────────────
  // When there are no autorun_history entries to show, collapse the
  // activity-log-frame so users don't see an empty 3-row blank above
  // the main log.
  function syncActivityLogVisibility() {
    const el = document.getElementById("activity-log");
    const frame = document.querySelector(".activity-log-frame");
    const splitter = document.getElementById("paned-splitter");
    if (!el || !frame) return;
    const hasItems = el.childElementCount > 0;
    frame.hidden = !hasItems;
    if (splitter) splitter.hidden = !hasItems;
  }
  window._syncActivityLogVisibility = syncActivityLogVisibility;

  // ─── Clear buttons auto-hide when the log they clear is empty ────────
  // "Clear log" (main log) and "Clear" (activity log) should only appear
  // when there's actually something to clear. Otherwise they're clutter.
  function syncClearButtonsVisibility() {
    const mainLog = document.getElementById("main-log");
    const actLog = document.getElementById("activity-log");
    const clearLogBtn = document.getElementById("btn-clear-log");
    const clearActBtn = document.getElementById("btn-clear");
    if (clearLogBtn) {
      clearLogBtn.hidden = !mainLog || mainLog.childElementCount === 0;
    }
    if (clearActBtn) {
      clearActBtn.hidden = !actLog || actLog.childElementCount === 0;
    }
  }
  window._syncClearButtonsVisibility = syncClearButtonsVisibility;

  // Watch for additions to the activity + main log via MutationObserver
  function observeActivityLog() {
    const act = document.getElementById("activity-log");
    const main = document.getElementById("main-log");
    if (act) {
      const obs = new MutationObserver(() => {
        syncActivityLogVisibility();
        syncClearButtonsVisibility();
      });
      obs.observe(act, { childList: true });
    }
    if (main) {
      const obs2 = new MutationObserver(() => syncClearButtonsVisibility());
      obs2.observe(main, { childList: true });
    }
    syncActivityLogVisibility();
    syncClearButtonsVisibility();
  }

  // ─── Sync Subbed button ──────────────────────────────────────────────
  function initSyncButton() {
    const btn = document.getElementById("btn-sync-subbed");
    const pauseBtn = document.getElementById("btn-pause");
    if (!btn) return;

    btn.addEventListener("click", async () => {
      const api = window.pywebview?.api;
      if (!api) {
        window._showToast?.("Sync requires native mode (launch main.py).", "warn");
        return;
      }
      try {
        const res = await api.sync_start_all();
        if (!res.ok) {
          window._showToast?.(res.error || "Sync failed to start", "error");
          return;
        }
        window._showToast?.("Sync started.", "ok");
      } catch (e) {
        window._showToast?.("Error: " + e, "error");
      }
    });

    // Big Pause button on the Download tab — GLOBAL pause/resume for
    // both the sync pipeline AND the GPU queue (transcribe / compress).
    // Decision source: _blinkState (client mirror kept in sync via backend
    // push notifications). Using client state here means the toggle always
    // matches what the user is visually seeing on the button — no async
    // roundtrip race, no "api.queue_is_paused undefined at click time"
    // falsy-short-circuit bug.
    //
    // This mirrors OLD's global pause button that gated every worker at
    // once. To target one queue independently, use the Pause button
    // inside the Sync Tasks / GPU Tasks popover.
    pauseBtn?.addEventListener("click", async () => {
      // Three click paths:
      // 1. Worker thread alive + not paused → pause (both queues)
      // 2. Worker thread alive + paused → resume (both queues)
      // 3. Worker thread dead + items queued + paused →
      // start a fresh sync thread. sync_start_all clears every
      // pause flag AND spawns the worker, so the restored queue
      // gets processed. Click-triggered only; never automatic —
      // matches Project rule: "launching with items in queue
      // must never auto-start" (main.py:168).
      const api = window.pywebview?.api;
      if (!api) return;
      const s = _blinkState.sync;
      const g = _blinkState.gpu;
      const syncActivelyPaused = s.running && s.paused;
      const gpuActivelyPaused = g.running && g.paused;
      const anyActivelyPaused = syncActivelyPaused || gpuActivelyPaused;
      const syncDeadPausedWithItems = !s.running && s.paused && s.count > 0;
      const gpuDeadPausedWithItems = !g.running && g.paused && g.count > 0;
      const deadPausedWithItems =
        syncDeadPausedWithItems || gpuDeadPausedWithItems;
      try {
        if (anyActivelyPaused) {
          if (typeof api.queue_resume === "function") {
            await api.queue_resume("both");
          }
          window._showToast?.("Resumed.", "ok");
        } else if (deadPausedWithItems) {
          // Clear pause flags AND start the sync thread so the queue
          // actually drains. Using sync_start_all (not queue_resume)
          // because queue_resume alone just clears flags — without a
          // live thread, the queue would stay frozen.
          if (typeof api.sync_start_all === "function") {
            const res = await api.sync_start_all();
            if (res?.ok) {
              window._showToast?.("Resumed \u2014 starting queue.", "ok");
            } else {
              window._showToast?.(res?.error || "Resume failed.", "error");
            }
          }
        } else {
          if (typeof api.queue_pause === "function") {
            await api.queue_pause("both");
          }
          window._showToast?.(
            "Paused \u2014 current jobs finish first.", "warn");
        }
      } catch (e) { window._showToast?.("Error: " + e, "error"); }
    });


    // Sync Tasks queue popover: Pause toggles pause/resume, Cancel cancels.
    // If the queue is paused AND there's no live sync worker thread (cold
    // launch with items restored), clicking Resume has to START the
    // worker — queue_resume alone just clears the flag and the queue
    // stays frozen. Matches the global Pause button's behavior.
    const pauseSyncBtn = document.getElementById("btn-pause-sync-queue");
    pauseSyncBtn?.addEventListener("click", async () => {
      const api = window.pywebview?.api;
      if (!api?.queue_is_paused) { api?.sync_cancel?.(); return; }
      const s = _blinkState.sync;
      const threadAlive = s.running;
      const st = await api.queue_is_paused();
      if (st?.sync) {
        if (!threadAlive && s.count > 0 && typeof api.sync_start_all === "function") {
          const res = await api.sync_start_all();
          window._showToast?.(res?.ok
            ? "Sync resumed \u2014 starting queue."
            : (res?.error || "Resume failed."),
            res?.ok ? "ok" : "error");
        } else {
          await api.queue_resume("sync");
          window._showToast?.("Sync resumed.", "ok");
        }
      } else {
        await api.queue_pause("sync");
        window._showToast?.("Sync paused \u2014 finishing current channel.", "warn");
      }
    });
    document.getElementById("btn-cancel-sync-queue")?.addEventListener("click", async () => {
      const api = window.pywebview?.api;
      if (!api) return;
      const choice = await (window.askChoice
        ? askChoice({
            title: "Stop the sync pass?",
            message: "Pause keeps the queue. Clear Queue empties it — the " +
                     "current channel finishes either way.",
            buttons: [
              { label: "Pause", value: "pause", kind: "primary" },
              { label: "Clear Queue", value: "clear", kind: "danger" },
            ],
            cancel: "Cancel",
            cancelPlacement: "right",
          })
        : Promise.resolve(confirm("Clear the sync queue?") ? "clear" : null));
      if (choice === "clear") {
        const res = await api.sync_clear_queue?.();
        const n = res?.removed || 0;
        window._showToast?.(
          n > 0 ? `Sync queue cleared (${n} pending).`
                : "Sync cancel requested.", "warn");
      } else if (choice === "pause") {
        await api.queue_pause?.("sync");
        window._showToast?.("Sync paused \u2014 finishing current channel.", "warn");
      }
      // null → Cancel → no-op (dialog closed)
    });

    // GPU Tasks queue popover — mirror the Sync handlers.
    document.getElementById("btn-pause-gpu-queue")?.addEventListener("click", async () => {
      const api = window.pywebview?.api;
      if (!api?.queue_is_paused) { api?.transcribe_cancel_all?.(); return; }
      const st = await api.queue_is_paused();
      if (st?.gpu) {
        await api.queue_resume("gpu");
        window._showToast?.("GPU queue resumed.", "ok");
      } else {
        await api.queue_pause("gpu");
        window._showToast?.("GPU queue paused \u2014 current job will finish.", "warn");
      }
    });
    document.getElementById("btn-cancel-gpu-queue")?.addEventListener("click", async () => {
      const api = window.pywebview?.api;
      if (!api) return;
      const choice = await (window.askChoice
        ? askChoice({
            title: "Stop the GPU queue?",
            message: "Pause keeps the queue. Clear Queue empties it and " +
                     "cancels the current job.",
            buttons: [
              { label: "Pause", value: "pause", kind: "primary" },
              { label: "Clear Queue", value: "clear", kind: "danger" },
            ],
            cancel: "Cancel",
            cancelPlacement: "right",
          })
        : Promise.resolve(confirm("Clear the GPU queue?") ? "clear" : null));
      if (choice === "clear") {
        const res = await api.gpu_clear_queue?.();
        const n = res?.removed || 0;
        window._showToast?.(
          n > 0 ? `GPU queue cleared (${n} pending).`
                : "GPU queue cleared.", "warn");
      } else if (choice === "pause") {
        await api.queue_pause?.("gpu");
        window._showToast?.("GPU queue paused \u2014 current job will finish.", "warn");
      }
    });
  }

  // ─── Queue button blink animation ────────────────────────────────────
  //
  // Matches _blink_tick at YTArchiver.py:21049 — unified 700ms clock so
  // Sync + GPU buttons blink in phase. Running = blink, Paused = solid,
  // Idle = default bg.
  const _blinkState = {
    clockOn: false,
    timer: null,
    // `count` = items queued for this pipeline. Used by
    // _syncPauseButtonState so the global Pause button enables when
    // items are queued-but-paused-without-a-live-thread (the state after
    // a cold launch when QueueState.load() restored persisted items).
    sync: { running: false, paused: false, count: 0 },
    gpu: { running: false, paused: false, count: 0 },
  };
  // Expose for logs.js's renderQueues so it can mirror queue counts
  // into _blinkState the moment a fresh payload arrives.
  window._blinkState = _blinkState;
  window._syncPauseButtonState = _syncPauseButtonState;
  window._paintBlinkState = paintBlinkState;

  function initQueueBlink() {
    // Backend drives blink state via:
    // window.setQueueState({ sync: {running, paused}, gpu: {running, paused} })
    window.setQueueState = (state) => {
      if (state.sync) Object.assign(_blinkState.sync, state.sync);
      if (state.gpu) Object.assign(_blinkState.gpu, state.gpu);
      ensureBlinkRunning();
      paintBlinkState();
    };
    // Paint initial idle state (buttons default to gray)
    paintBlinkState();
    // NOTE: auto-start for preview-only mode was removed — it was firing in
    // native mode too because pywebview.api isn't ready at boot time.
    // Pause button: disabled when nothing running
    _syncPauseButtonState();
    // Re-check whenever state is pushed
    const origSet = window.setQueueState;
    window.setQueueState = (state) => {
      origSet(state);
      _syncPauseButtonState();
    };
    // Wrap renderQueues so queue counts AND paused flags are mirrored
    // into _blinkState every payload. Critical for cold launch: the
    // backend only pushes setQueueState via _on_queue_changed (i.e.
    // when state CHANGES after the window is ready) — it never emits
    // a baseline after set_window. So the frontend's first view of
    // paused-at-launch comes from the api.get_queues() payload, which
    // DOES carry sync_paused / gpu_paused. Without mirroring those
    // flags here, _blinkState.paused stays false at boot, the global
    // Pause button's "paused + items queued" branch never fires, and
    // the button sits disabled with 99+ items waiting.
    const origRenderQueues = window.renderQueues;
    if (typeof origRenderQueues === "function") {
      window.renderQueues = (queues) => {
        origRenderQueues(queues);
        _blinkState.sync.count = (queues?.sync || []).length;
        _blinkState.gpu.count = (queues?.gpu || []).length;
        // Paused flags may be missing on partial payloads (e.g. a
        // legacy caller that only passes {sync, gpu}) — only overwrite
        // when the key is present so we don't reset the state that
        // setQueueState already established.
        if (queues && "sync_paused" in queues) {
          _blinkState.sync.paused = !!queues.sync_paused;
        }
        if (queues && "gpu_paused" in queues) {
          _blinkState.gpu.paused = !!queues.gpu_paused;
        }
        _syncPauseButtonState();
        paintBlinkState();
      };
    }
  }

  function _syncPauseButtonState() {
    // Three states:
    // 1. Worker thread alive (running OR parked in _wait_if_paused) →
    // enabled. Click pauses/resumes an in-progress pass.
    // 2. Thread dead BUT items are queued AND queue is paused →
    // enabled. Click starts a new sync thread that will process
    // the restored queue. This covers the cold-launch case where
    // the user quit mid-pass and reopens with 99+ items pending.
    // 3. Thread dead AND (no items OR not paused) → disabled.
    // User clicks Sync Subbed to start fresh.
    const s = _blinkState.sync;
    const g = _blinkState.gpu;
    const anyAlive = s.running || g.running;
    const pausedWithItems =
      (s.paused && s.count > 0) || (g.paused && g.count > 0);
    const enable = anyAlive || pausedWithItems;
    const btn = document.getElementById("btn-pause");
    if (btn) {
      btn.disabled = !enable;
      btn.classList.toggle("disabled", !enable);
    }
  }

  function ensureBlinkRunning() {
    const anyActive = _blinkState.sync.running || _blinkState.gpu.running;
    if (anyActive && !_blinkState.timer) {
      _blinkState.clockOn = false;
      _blinkState.timer = setInterval(blinkTick, 700);
    } else if (!anyActive && _blinkState.timer) {
      clearInterval(_blinkState.timer);
      _blinkState.timer = null;
      _blinkState.clockOn = false;
      paintBlinkState(); // final solid paint
    }
  }

  function blinkTick() {
    _blinkState.clockOn = !_blinkState.clockOn;
    paintBlinkState();
  }

  function paintBlinkState() {
    // Three visual states, per user rule:
    // idle → grey (nothing running, nothing paused mid-pass)
    // running → green/red blink at clockOn cadence
    // paused → solid green/red when the worker thread is alive AND
    // paused. "Running" in _blinkState reflects "worker
    // thread is alive" (set by main.py _on_queue_changed),
    // so it stays true during pause-between-channels —
    // which means this cleanly distinguishes "actively
    // paused mid-pass" (solid) from "persisted paused flag
    // at idle" (grey).
    const syncBtn = document.getElementById("btn-sync-tasks");
    const gpuBtn = document.getElementById("btn-gpu-tasks");
    if (syncBtn) {
      const s = _blinkState.sync;
      let state = "idle";
      if (s.running && s.paused) state = "paused";
      else if (s.running) state = _blinkState.clockOn ? "on" : "off";
      syncBtn.dataset.blinkState = state;
    }
    if (gpuBtn) {
      const g = _blinkState.gpu;
      let state = "idle";
      if (g.running && g.paused) state = "paused";
      else if (g.running) state = _blinkState.clockOn ? "on" : "off";
      gpuBtn.dataset.blinkState = state;
    }
    // Global Pause button (Download tab). Three visual states:
    // running (ghost, pause icon) — a worker thread is alive,
    // nothing paused. Click pauses.
    // paused (solid, resume icon) — worker alive AND paused,
    // OR worker dead AND queue has items AND queue paused
    // (persisted-paused-at-launch). Click resumes — if the
    // thread is dead, the click handler starts a fresh one
    // that processes the restored queue.
    // disabled is set by _syncPauseButtonState when nothing to do.
    const pauseBtn = document.getElementById("btn-pause");
    if (pauseBtn) {
      const s = _blinkState.sync;
      const g = _blinkState.gpu;
      const syncActive = s.running && s.paused;
      const gpuActive = g.running && g.paused;
      const syncPausedWithItems = !s.running && s.paused && s.count > 0;
      const gpuPausedWithItems = !g.running && g.paused && g.count > 0;
      const anyPaused = syncActive || gpuActive ||
                        syncPausedWithItems || gpuPausedWithItems;
      pauseBtn.dataset.pauseState = anyPaused ? "paused" : "running";
      pauseBtn.title = anyPaused
        ? "Resume all queues"
        : "Pause all queues (current jobs finish first)";
      const svg = pauseBtn.querySelector("svg");
      if (svg) {
        const want = anyPaused ? "play" : "bars";
        if (svg.dataset.icon !== want) {
          svg.dataset.icon = want;
          svg.innerHTML = anyPaused
            ? '<path d="M4 3v10l9-5z"/>'
            : '<rect x="4" y="3" width="3" height="10" rx="0.5"/>' +
              '<rect x="9" y="3" width="3" height="10" rx="0.5"/>';
        }
      }
    }
    // Sync Tasks + GPU Tasks popover footer buttons — flip Pause ↔ Resume
    // whenever the corresponding pipeline is paused. "Paused" covers both
    // the thread-alive-but-paused case AND the cold-launch case where
    // items are queued but no thread is running yet; clicking Resume in
    // the latter case should start a fresh worker (handled in the click
    // handlers below). Also hide the Pause/Cancel pair entirely when the
    // queue is empty — nothing to act on means no buttons.
    _paintPopoverPauseBtn("btn-pause-sync-queue",
                           _blinkState.sync, "Sync");
    _paintPopoverPauseBtn("btn-pause-gpu-queue",
                           _blinkState.gpu, "GPU");
    // Sync popover: no sibling buttons in the footer — hide the entire
    // footer when there's nothing to act on so we don't show an empty
    // bar with just the border-top.
    const syncFooter = document.getElementById("sync-tasks-footer");
    if (syncFooter) {
      syncFooter.style.display = _blinkState.sync.count > 0 ? "" : "none";
    }
    // GPU popover: footer also hosts the Manual Transcribe folder icon,
    // which should stay visible. Only hide the Pause/Cancel pair.
    _setPopoverFooterBtnsVisible("btn-pause-gpu-queue",
                                  "btn-cancel-gpu-queue",
                                  _blinkState.gpu.count > 0);
  }

  // Show/hide a popover's Pause + Cancel button pair. When the queue is
  // empty the pair goes `display: none` so the popover footer collapses
  // around whatever sibling buttons remain (e.g. Manual Transcribe).
  function _setPopoverFooterBtnsVisible(pauseBtnId, cancelBtnId, visible) {
    const pb = document.getElementById(pauseBtnId);
    const cb = document.getElementById(cancelBtnId);
    if (pb) pb.style.display = visible ? "" : "none";
    if (cb) cb.style.display = visible ? "" : "none";
  }

  // Helper: paint one popover-footer pause button based on a pipeline's
  // blink state. Called by paintBlinkState for both Sync and GPU
  // popovers. Flips the label, title, and SVG icon.
  function _paintPopoverPauseBtn(btnId, state, label) {
    const btn = document.getElementById(btnId);
    if (!btn) return;
    const activelyPaused = state.running && state.paused;
    const deadPausedWithItems = !state.running && state.paused && state.count > 0;
    const isPaused = activelyPaused || deadPausedWithItems;
    const svg = btn.querySelector("svg");
    const span = btn.querySelector("span");
    if (span) span.textContent = isPaused ? "Resume" : "Pause";
    btn.title = isPaused
      ? `Resume ${label.toLowerCase()} queue`
      : `Pause ${label.toLowerCase()} queue (current job finishes first)`;
    btn.dataset.pauseState = isPaused ? "paused" : "running";
    if (svg) {
      const want = isPaused ? "play" : "bars";
      if (svg.dataset.icon !== want) {
        svg.dataset.icon = want;
        svg.innerHTML = isPaused
          ? '<path d="M3 2v10l8-5z"/>'
          : '<rect x="3" y="2" width="3" height="10" rx="0.5"/>' +
            '<rect x="8" y="2" width="3" height="10" rx="0.5"/>';
      }
    }
  }

  // ─── Settings tab ───────────────────────────────────────────────────
  //
  // Settings is now a full tab (#panel-settings), not a modal. Switching
  // to the Settings tab re-loads the current config values so the tab is
  // always in sync with what's on disk.
  function initSettingsTab() {
    const panel = document.getElementById("panel-settings");
    const save = document.getElementById("settings-save");
    const browseOut = document.getElementById("settings-browse-output");
    const browseVid = document.getElementById("settings-browse-video");
    const ytdlpBtn = document.getElementById("btn-ytdlp-update");
    const expBtn = document.getElementById("btn-export-channels");
    const impBtn = document.getElementById("btn-import-channels");
    const bkExpBtn = document.getElementById("btn-export-backup");
    const bkImpBtn = document.getElementById("btn-import-backup");
    if (!panel) return;

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
        document.getElementById("settings-whisper-model").value = s.whisper_model || "large-v3";
        document.getElementById("settings-default-res").value = s.default_resolution || "720";
        document.getElementById("settings-default-min").value = s.default_min_duration ?? 3;
        document.getElementById("settings-log-mode").value = s.log_mode || "Simple";
        // Startup knobs
        const stEl = document.getElementById("settings-disk-staleness");
        if (stEl) stEl.value = String(s.disk_scan_staleness_hours ?? 24);
        const paEl = document.getElementById("settings-preload-all");
        if (paEl) paEl.checked = !!s.browse_preload_all;
        const avgEl = document.getElementById("settings-show-avg-size");
        if (avgEl) avgEl.checked = (s.show_avg_size !== false);
        // Apply current toggle to the Subs table right away so opening
        // Settings doesn't require a save to see the effect.
        window._applySubsAvgVisibility?.(s.show_avg_size !== false);
        // Recent view radios — stored value is "list" | "grid".
        const rvMode = (s.recent_view_mode === "grid") ? "grid" : "list";
        const rvList = document.getElementById("settings-recent-view-list");
        const rvGrid = document.getElementById("settings-recent-view-grid");
        if (rvList) rvList.checked = (rvMode === "list");
        if (rvGrid) rvGrid.checked = (rvMode === "grid");
        window._applyRecentViewMode?.(rvMode);
        _updatePreloadHints();
        const vEl = document.getElementById("settings-ytdlp-version");
        if (vEl) vEl.textContent = "checking\u2026";
        try {
          const v = await api.ytdlp_version();
          if (vEl) vEl.textContent = v?.ok ? v.version : (v?.error || "not found");
        } catch { if (vEl) vEl.textContent = "check failed"; }
      } catch (e) { console.warn("settings load:", e); }
    }

    // Update hint text live when the toggle flips.
    document.getElementById("settings-preload-all")
      ?.addEventListener("change", _updatePreloadHints);

    // Apply Avg-column visibility live — no need to hit Save to preview.
    // Persistence still happens on Save; this just updates the current DOM.
    document.getElementById("settings-show-avg-size")
      ?.addEventListener("change", (e) => {
        window._applySubsAvgVisibility?.(!!e.target.checked);
      });

    // Recent view radios — live-preview the mode swap without needing
    // to hit Save. Persistence still goes through settings_save.
    const _rvApply = () => {
      const grid = document.getElementById("settings-recent-view-grid")?.checked;
      window._applyRecentViewMode?.(grid ? "grid" : "list");
    };
    document.getElementById("settings-recent-view-list")?.addEventListener("change", _rvApply);
    document.getElementById("settings-recent-view-grid")?.addEventListener("change", _rvApply);

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
    metaQueueAll?.addEventListener("click", async () => {
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
    });
    metaRefresh?.addEventListener("click", async () => {
      const api = window.pywebview?.api;
      if (!api?.metadata_queue_all) { window._showToast?.("Native mode required.", "warn"); return; }
      const ok = await askConfirm("Refresh views/likes",
        "Re-fetch view counts and like counts for every video on every channel?\n\n" +
        "This skips channels/videos that already have fresh metadata. Still slow " +
        "on a 100k-video archive.",
        { confirm: "Refresh" });
      if (!ok) return;
      const res = await api.metadata_queue_all(true);
      if (res?.ok) window._showToast?.(`Queued refresh for ${res.channels} channel(s).`, "ok");
      else window._showToast?.(res?.error || "Refresh failed.", "error");
    });

    save?.addEventListener("click", async () => {
      const api = window.pywebview?.api;
      const payload = {
        output_dir: document.getElementById("settings-output-dir").value,
        video_out_dir: document.getElementById("settings-video-dir").value,
        whisper_model: document.getElementById("settings-whisper-model").value,
        default_resolution: document.getElementById("settings-default-res").value,
        default_min_duration: parseInt(document.getElementById("settings-default-min").value) || 3,
        log_mode: document.getElementById("settings-log-mode").value,
        // Startup knobs
        disk_scan_staleness_hours:
          Math.max(0, parseInt(document.getElementById("settings-disk-staleness")?.value, 10) || 0),
        browse_preload_all:
          !!document.getElementById("settings-preload-all")?.checked,
        show_avg_size:
          !!document.getElementById("settings-show-avg-size")?.checked,
        recent_view_mode:
          document.getElementById("settings-recent-view-grid")?.checked ? "grid" : "list",
      };
      const res = await api?.settings_save?.(payload);
      if (res?.ok) window._showToast?.("Settings saved.", "ok");
      else window._showToast?.(res?.error || "Save failed.", "error");
    });

    browseOut?.addEventListener("click", async () => {
      const cur = document.getElementById("settings-output-dir").value;
      const res = await window.pywebview?.api?.pick_folder?.("Archive root", cur);
      if (res?.ok && res.path) document.getElementById("settings-output-dir").value = res.path;
    });
    browseVid?.addEventListener("click", async () => {
      const cur = document.getElementById("settings-video-dir").value;
      const res = await window.pywebview?.api?.pick_folder?.("Single-video downloads", cur);
      if (res?.ok && res.path) document.getElementById("settings-video-dir").value = res.path;
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
        window._showToast?.(`Added ${res.added} channels (${res.skipped} skipped).`, "ok");
        location.reload();
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
      const confirm1 = await askDanger("Restore backup",
        "Restoring a backup will OVERWRITE your current config, queue state, and journals.\n\n" +
        "A snapshot of the current config is saved to backups/ first, so you can roll back.",
        { confirm: "Pick ZIP\u2026" });
      if (!confirm1) return;
      const res = await window.pywebview?.api?.import_full_backup?.();
      if (res?.ok) {
        window._showToast?.(
          `Restored ${res.files_restored} files. Restart to apply.`,
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
    });
  }

  // ─── About dialog ────────────────────────────────────────────────────
  function initAboutDialog() {
    const bd = document.getElementById("about-backdrop");
    const openBtn = document.getElementById("btn-about");
    const closeBtn = document.getElementById("about-close");
    const body = document.getElementById("about-body");
    if (!bd) return;
    const show = async () => {
      bd.style.display = "flex";
      const api = window.pywebview?.api;
      if (!api?.about_info) {
        body.textContent = "Native mode only.";
        return;
      }
      try {
        const info = await api.about_info();
        body.innerHTML = `
          <div style="line-height:1.6;">
            <div><strong>${escapeHtml(info.app_name)}</strong> <span style="color:var(--c-dim)">${escapeHtml(info.app_version)}</span></div>
            <div style="margin-top:10px;color:var(--c-dim);font-size:11.5px;">
              <div>Channels subscribed: ${info.channels ?? "\u2014"}</div>
              <div>yt-dlp: ${escapeHtml(info.ytdlp_version || "\u2014")}</div>
              <div>Python: ${escapeHtml(info.python_version || "\u2014")}</div>
              <div style="margin-top:8px;">Config: <code style="font-size:11px;">${escapeHtml(info.config_path)}</code></div>
              <div>Archive root: <code style="font-size:11px;">${escapeHtml(info.output_dir || "\u2014")}</code></div>
            </div>
          </div>`;
      } catch (e) { body.textContent = "Error loading: " + e; }
    };
    const hide = () => { bd.style.display = "none"; };
    openBtn?.addEventListener("click", show);
    closeBtn?.addEventListener("click", hide);
    bd.addEventListener("click", (e) => { if (e.target === bd) hide(); });
  }

  // ─── Diagnostics dialog ──────────────────────────────────────────────
  function initDiagnosticsDialog() {
    const bd = document.getElementById("diag-backdrop");
    const openBtn = document.getElementById("btn-diagnostics");
    const closeBtn = document.getElementById("diag-close");
    const refreshBtn = document.getElementById("diag-refresh");
    const rowsEl = document.getElementById("diag-rows");
    const summaryEl = document.getElementById("diag-summary");
    if (!bd) return;

    async function run() {
      rowsEl.innerHTML = '<div class="browse-empty" style="padding:16px;">Running self-check\u2026</div>';
      summaryEl.textContent = "";
      const api = window.pywebview?.api;
      if (!api?.diagnostics_run) {
        rowsEl.innerHTML = '<div class="browse-empty" style="padding:16px;">Native mode required.</div>';
        return;
      }
      try {
        const res = await api.diagnostics_run();
        if (!res?.ok || !Array.isArray(res.rows)) {
          rowsEl.innerHTML = '<div class="browse-empty" style="padding:16px;">Self-check failed.</div>';
          return;
        }
        const frag = document.createDocumentFragment();
        let okN = 0, failN = 0;
        for (const r of res.rows) {
          const row = document.createElement("div");
          row.className = "diag-row" + (r.ok ? " diag-ok" : " diag-fail");
          row.innerHTML = `
            <span class="diag-dot"></span>
            <span class="diag-name"></span>
            <span class="diag-detail"></span>
          `;
          row.querySelector(".diag-name").textContent = r.name;
          row.querySelector(".diag-detail").textContent = r.detail || "";
          frag.appendChild(row);
          if (r.ok) okN++; else failN++;
        }
        rowsEl.innerHTML = "";
        rowsEl.appendChild(frag);
        summaryEl.textContent = failN === 0
          ? `All ${okN} checks passed`
          : `${okN} ok \u2014 ${failN} problem${failN === 1 ? "" : "s"}`;
      } catch (e) {
        rowsEl.innerHTML = `<div class="browse-empty" style="padding:16px;">Error: ${escapeHtml(String(e))}</div>`;
      }
    }

    const show = () => { bd.style.display = "flex"; run(); };
    const hide = () => { bd.style.display = "none"; };
    openBtn?.addEventListener("click", show);
    closeBtn?.addEventListener("click", hide);
    refreshBtn?.addEventListener("click", run);
    bd.addEventListener("click", (e) => { if (e.target === bd) hide(); });
  }

  // ─── Manual Transcribe dialog ────────────────────────────────────────
  function initManualTranscribe() {
    const backdrop = document.getElementById("manual-tx-backdrop");
    const pathEl = document.getElementById("manual-tx-path");
    const modelEl = document.getElementById("manual-tx-model");
    const openBtn = document.getElementById("btn-manual-transcribe");
    const browseBtn = document.getElementById("manual-tx-browse");
    const cancelBtn = document.getElementById("manual-tx-cancel");
    const confirmBtn = document.getElementById("manual-tx-confirm");
    if (!backdrop) return;

    const show = () => { backdrop.style.display = "flex"; };
    const hide = () => { backdrop.style.display = "none"; if (pathEl) pathEl.value = ""; };

    openBtn?.addEventListener("click", show);
    cancelBtn?.addEventListener("click", hide);
    backdrop.addEventListener("click", (e) => { if (e.target === backdrop) hide(); });

    // "Transcribe folder..." — recursively queue every untranscribed video
    // under a picked folder. Native folder picker handles the prompt.
    document.getElementById("btn-transcribe-folder")?.addEventListener("click", async () => {
      const api = window.pywebview?.api;
      if (!api?.transcribe_folder) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      // Manual → ask Whisper model (60s countdown auto-picks Settings default).
      const model = await (window._askWhisperModel?.("this folder"));
      if (model === null) return;
      const res = await api.transcribe_folder();
      if (res?.ok) {
        window._showToast?.("Walking folder \u2014 watch the log for queue counts.", "ok");
      } else if (!res?.cancelled) {
        window._showToast?.(res?.error || "Folder transcribe failed.", "error");
      }
    });

    browseBtn?.addEventListener("click", async () => {
      const api = window.pywebview?.api;
      if (!api?.pick_file) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      const res = await api.pick_file("Pick a video to transcribe", null,
                                       [("Video files (*.mp4;*.mkv;*.webm;*.mov)")]);
      if (res?.ok && res.path) {
        pathEl.value = res.path;
      }
    });

    confirmBtn?.addEventListener("click", async () => {
      const api = window.pywebview?.api;
      const path = pathEl?.value || "";
      if (!path) { window._showToast?.("Pick a video file first.", "warn"); return; }
      if (!api?.transcribe_enqueue) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      const title = path.split(/[\\/]/).pop().replace(/\.[^.]+$/, "");
      // Manual → ask Whisper model (60s countdown auto-picks Settings default).
      const model = await (window._askWhisperModel?.(`"${title}"`));
      if (model === null) return;
      const res = await api.transcribe_enqueue(path, title);
      if (res?.ok) {
        window._showToast?.("Queued for transcription.", "ok");
        hide();
      } else {
        window._showToast?.("Queue failed.", "error");
      }
    });
  }

  // ─── Queue popovers (Sync Tasks, GPU Tasks) ──────────────────────────
  //
  // Anchor to the icon button. Clicking the button toggles; clicking
  // outside closes. Escape closes. No backdrop dim — the popover is a
  // dropdown, not a modal.
  function initQueueModals() {
    const pairs = [
      ["btn-sync-tasks", "popover-sync-tasks"],
      ["btn-gpu-tasks", "popover-gpu-tasks"],
    ];
    for (const [btnId, popId] of pairs) {
      const btn = document.getElementById(btnId);
      const pop = document.getElementById(popId);
      if (!btn || !pop) continue;
      btn.addEventListener("click", (e) => {
        e.stopPropagation();
        const wasOpen = pop.classList.contains("open");
        // Close any other popover
        document.querySelectorAll(".queue-popover.open").forEach(p => p.classList.remove("open"));
        if (!wasOpen) {
          anchorPopover(pop, btn);
          pop.classList.add("open");
        }
      });
    }

    // Close on outside click
    document.addEventListener("click", (e) => {
      const open = document.querySelectorAll(".queue-popover.open");
      open.forEach(p => {
        if (!p.contains(e.target)) p.classList.remove("open");
      });
    });
    // Close on Escape
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape") {
        document.querySelectorAll(".queue-popover.open").forEach(p => p.classList.remove("open"));
      }
    });
    // Reposition on window resize
    window.addEventListener("resize", () => {
      for (const [btnId, popId] of pairs) {
        const btn = document.getElementById(btnId);
        const pop = document.getElementById(popId);
        if (btn && pop && pop.classList.contains("open")) {
          anchorPopover(pop, btn);
        }
      }
    });

    // ── Mid-queue model swap dropdown (GPU popover) ──────────────────────
    const swap = document.getElementById("gpu-model-swap");
    if (swap) {
      // Preload from settings so the dropdown reflects the real current model.
      (async () => {
        try {
          const s = await window.pywebview?.api?.settings_load?.();
          if (s?.whisper_model) swap.value = s.whisper_model;
        } catch { /* ignore */ }
      })();

      swap.addEventListener("change", async () => {
        const api = window.pywebview?.api;
        if (!api?.transcribe_swap_model) {
          window._showToast?.("Native mode required for model swap.", "warn");
          return;
        }
        const prev = swap.dataset.prev || swap.value;
        const ok = await askConfirm("Swap whisper model",
          `The current transcribe job (if any) will finish on the old model. ` +
          `The next job will use:\n\n ${swap.value}\n\nContinue?`,
          { confirm: "Swap" });
        if (!ok) {
          swap.value = prev;
          return;
        }
        const res = await api.transcribe_swap_model(swap.value);
        if (res?.ok) {
          window._showToast?.(`Model swapped to ${swap.value}.`, "ok");
          swap.dataset.prev = swap.value;
        } else {
          window._showToast?.(res?.error || "Swap failed.", "error");
          swap.value = prev;
        }
      });
      swap.addEventListener("focus", () => { swap.dataset.prev = swap.value; });
    }
  }

  function anchorPopover(pop, btn) {
    // Position the popover below the button, right-aligned to it.
    // Clamp to viewport edges.
    const br = btn.getBoundingClientRect();
    // Ensure measurable
    pop.style.visibility = "hidden";
    pop.style.display = "flex";
    const pr = pop.getBoundingClientRect();
    pop.style.display = "";
    pop.style.visibility = "";

    let top = br.bottom + 6;
    let left = br.right - pr.width;
    if (left < 8) left = 8;
    if (left + pr.width > window.innerWidth - 8) {
      left = window.innerWidth - pr.width - 8;
    }
    if (top + pr.height > window.innerHeight - 8) {
      top = br.top - pr.height - 6;
      if (top < 8) top = 8;
    }
    pop.style.top = top + "px";
    pop.style.left = left + "px";
  }

  // ─── Seed logs with test data ────────────────────────────────────────
  //
  // Dual mode:
  // - pywebview (desktop app) → Python bridge via window.pywebview.api
  // - browser preview → fetch sample.json (same data, static)
  async function seedLogs() {
    // Give pywebview a brief window to register its API before falling back.
    const pywebviewReady = () =>
      new Promise((resolve) => {
        if (window.pywebview && window.pywebview.api) {
          resolve(true);
          return;
        }
        let settled = false;
        const finish = (ok) => {
          if (settled) return;
          settled = true;
          resolve(ok);
        };
        window.addEventListener("pywebviewready",
          () => finish(true), { once: true });
        setTimeout(() => finish(!!(window.pywebview && window.pywebview.api)), 600);
      });

    const ready = await pywebviewReady();

    try {
      if (ready) {
        const api = window.pywebview.api;
        // Each API call is isolated — one failure does NOT cascade.
        const step = async (name, fn) => {
          try { await fn(); }
          catch (e) { console.error(`[seed] ${name} failed:`, e); }
        };

        await step("runtime_info", async () => {
          const info = await api.get_runtime_info();
          if (!info) return;
          console.info("[api] runtime_info:", info);
          const sel = document.getElementById("log-mode-select");
          if (sel && info.log_mode) sel.value = info.log_mode;
          document.body.dataset.logMode = info.log_mode || "Simple";
          // Pre-seed the Subs Avg column visibility so the upcoming
          // renderSubsTable step doesn't flash the column in and then
          // hide it. info.show_avg_size defaults true on missing key.
          window._applySubsAvgVisibility?.(info.show_avg_size !== false);
          // Pre-seed the Recent view mode so the upcoming
          // renderRecentTable step renders into the correct view and
          // the alternate frame is hidden before first paint.
          window._applyRecentViewMode?.(info.recent_view_mode || "list");
          // First-run wizard: no output_dir set → prompt for archive folder
          if (info.has_real_config === false || !info.output_dir) {
            try {
              const ok = await askConfirm(
                "Welcome to YT Archiver",
                "Looks like this is your first time (no archive folder configured yet).\n\nPick a folder where your YouTube downloads will be saved — this becomes the root of your archive.",
                { confirm: "Choose folder\u2026" });
              if (ok) {
                const picked = await api.pick_folder("Choose archive root");
                if (picked?.ok && picked.path) {
                  const saved = await api.set_parent_folder(picked.path);
                  if (saved?.ok) {
                    window._showToast?.("Archive root saved: " + picked.path, "ok");
                  } else if (saved?.write_blocked) {
                    window._showToast?.("Folder picked but write-gate off.", "warn");
                  }
                }
              }
            } catch (e) { console.warn("first-run prompt:", e); }
          }
        });

        await step("activity_log_history", async () => {
          const history = await api.get_activity_log_history();
          window.renderActivityLog(history || []);
          syncActivityLogVisibility();
        });

        await step("subs_channels", async () => {
          const subsData = await api.get_subs_channels();
          if (Array.isArray(subsData) && subsData.length === 2) {
            window.renderSubsTable(subsData[0], subsData[1]);
            window._primeBrowse(subsData[0]);
            window._populateIndexTable?.(subsData[0]);
          }
        });

        await step("recent_downloads", async () => {
          const rows = await api.get_recent_downloads();
          if (rows) window.renderRecentTable(rows);
        });

        await step("index_summary", async () => {
          const idx = await api.get_index_summary();
          if (idx) window._applyIndexSummary?.(idx);
        });

        await step("queues", async () => {
          const q = await api.get_queues();
          if (q) window.renderQueues(q);
        });

        await step("startup_ready", async () => {
          await api.startup_ready();
        });
      } else {
        // Browser preview fallback
        console.info("[logs] pywebview not detected — loading sample.json");
        const res = await fetch("./sample.json?t=" + Date.now(), { cache: "no-store" });
        const data = await res.json();
        console.info("[logs] sample.json loaded:", Object.keys(data),
                     "activity=" + (data.activity?.length),
                     "main=" + (data.main?.length));

        try { window.renderActivityLog(data.activity); }
        catch (e) { console.error("renderActivityLog failed:", e); }

        try { window.renderMainLog(data.main); }
        catch (e) { console.error("renderMainLog failed:", e); }

        if (data.subs) {
          try { window.renderSubsTable(data.subs, data.subs_total); }
          catch (e) { console.error("renderSubsTable failed:", e); }
          try { window._primeBrowse(data.subs); }
          catch (e) { console.error("_primeBrowse failed:", e); }
          try { window._populateIndexTable?.(data.subs); }
          catch (e) { console.error("_populateIndexTable failed:", e); }
        }
        if (data.recent) {
          try { window.renderRecentTable(data.recent); }
          catch (e) { console.error("renderRecentTable failed:", e); }
        }
        if (data.queues) {
          try { window.renderQueues(data.queues); }
          catch (e) { console.error("renderQueues failed:", e); }
        }

        const subsMini = data.main.slice(-4);
        for (const segs of subsMini) window.appendMiniLog("subs-mini-log", segs);
        for (const segs of subsMini) window.appendMiniLog("browse-mini-log", segs);
        for (const segs of subsMini) window.appendMiniLog("recent-mini-log", segs);
        for (const segs of subsMini) window.appendMiniLog("settings-mini-log", segs);

        // Simulate Python stream: append one line every 400ms
        let idx = 0;
        const streamLines = data.stream || [];
        const interval = setInterval(() => {
          if (idx >= streamLines.length) {
            clearInterval(interval);
            return;
          }
          window.appendMainLog(streamLines[idx]);
          idx++;
        }, 400);
      }
    } catch (e) {
      console.error("seedLogs failed:", e);
    }
  }

  // ─── Missing-folder reconcile flow ──────────────────────────────────
  async function _reconcileMissingFolders() {
    const api = window.pywebview?.api;
    if (!api?.check_channel_folders) return;
    let res;
    try { res = await api.check_channel_folders(); } catch { return; }
    const missing = (res && res.missing) || [];
    if (!missing.length) return;
    for (const ch of missing) {
      const choice = await askChoice?.(
        `"${ch.name}" — folder missing`,
        `This channel is subscribed but its folder is gone:\n\n` +
        ` ${ch.expected}\n\n` +
        "Locate the folder if you moved it, Remove the subscription if it's " +
        "no longer needed, or Skip to decide later.",
        [
          { label: "Locate\u2026", value: "locate", kind: "primary" },
          { label: "Remove", value: "remove", kind: "danger" },
          { label: "Skip", value: "skip", kind: "ghost" },
        ]);
      if (choice === "locate") {
        const pick = await api.subs_browse_for_channel_folder?.(ch.name);
        if (pick?.ok && pick.folder_name) {
          const r = await api.subs_relocate_channel?.(
            { name: ch.name, url: ch.url }, pick.folder_name);
          if (r?.ok) {
            window._showToast?.(
              `Relocated "${ch.name}" \u2192 ${pick.folder_name}`, "ok");
          } else {
            window._showToast?.(r?.error || "Relocate failed.", "error");
          }
        } else if (pick && !pick.cancelled) {
          window._showToast?.(pick.error || "Invalid folder.", "error");
        }
      } else if (choice === "remove") {
        await api.subs_remove_channel?.({ name: ch.name, url: ch.url });
        window._showToast?.(`Removed "${ch.name}" from subs.`, "warn");
      }
      // skip / cancel — leave as-is
    }
  }

  // ─── Boot ────────────────────────────────────────────────────────────
  // Custom tooltip — intercepts `title="..."` attributes, suppresses the
  // browser's default tooltip (by moving the title to `data-tooltip` on
  // first hover), and shows a dark-themed popup after a 400ms delay.
  // Matches YTArchiver.py:352 _ToolTip (delay=400, dark bg, wraplength).
  function _initCustomTooltips() {
    let timer = null;
    let bubble = null;
    let currentEl = null;

    const makeBubble = (text, x, y) => {
      const b = document.createElement("div");
      b.className = "custom-tooltip";
      b.textContent = text;
      b.style.left = x + "px";
      b.style.top = y + "px";
      document.body.appendChild(b);
      return b;
    };
    const hide = () => {
      if (timer) { clearTimeout(timer); timer = null; }
      if (bubble) { bubble.remove(); bubble = null; }
      currentEl = null;
    };

    document.addEventListener("mouseover", (e) => {
      const el = e.target.closest("[title], [data-tooltip]");
      if (!el || el === currentEl) return;
      // Move `title` to `data-tooltip` so the browser doesn't show its own.
      let text = el.getAttribute("data-tooltip");
      if (!text) {
        text = el.getAttribute("title") || "";
        if (!text) return;
        el.setAttribute("data-tooltip", text);
        el.removeAttribute("title");
      }
      hide();
      currentEl = el;
      timer = setTimeout(() => {
        const rect = el.getBoundingClientRect();
        let x = rect.left + rect.width / 2;
        let y = rect.bottom + 6;
        bubble = makeBubble(text, x, y);
        // Now that it's in the DOM, re-measure + clamp to viewport
        const br = bubble.getBoundingClientRect();
        if (br.right > window.innerWidth - 10) {
          bubble.style.left = (window.innerWidth - br.width - 10) + "px";
        }
        if (br.left < 10) bubble.style.left = "10px";
        if (br.bottom > window.innerHeight - 10) {
          // Flip above the element if no room below
          bubble.style.top = (rect.top - br.height - 6) + "px";
        }
        // Center-align horizontally
        const bbr = bubble.getBoundingClientRect();
        bubble.style.left = (parseFloat(bubble.style.left) -
                             (bbr.width / 2) + (rect.width / 2)) + "px";
      }, 400);
    });
    document.addEventListener("mouseout", (e) => {
      if (!currentEl) return;
      const to = e.relatedTarget;
      if (to && currentEl.contains(to)) return; // still inside
      hide();
    });
    // Also hide when the user starts clicking / typing
    document.addEventListener("mousedown", hide);
    document.addEventListener("keydown", hide);
  }

  // Global defocus — clicking any non-interactive element drops focus
  // back to body so stray input cursors + focus rings go away. Matches
  // YTArchiver.py:34371 _defocus_on_click.
  function _initGlobalDefocus() {
    document.addEventListener("mousedown", (e) => {
      const target = e.target;
      // Leave focus alone if click landed on a form control or anything
      // intentionally focusable (contenteditable, tabindex>=0 non-zero).
      if (target.closest(
        "input, textarea, select, button, a, [contenteditable='true'], " +
        ".bookmark-note, .search-viewer-body, .main-log, .activity-log, " +
        ".mini-log, .log-line, .ctx-menu"
      )) return;
      // Also respect user text-selection on long-form text (transcripts,
      // description drawer) — if they're actively selecting, don't defocus.
      const sel = window.getSelection?.();
      if (sel && sel.toString().length > 0) return;
      try {
        if (document.activeElement && document.activeElement !== document.body) {
          document.activeElement.blur();
        }
      } catch {}
    });
  }

  function boot() {
    try { initTabs(); } catch(e){console.error("initTabs:",e);}
    try { _initGlobalDefocus(); } catch(e){console.error("_initGlobalDefocus:",e);}
    try { _initCustomTooltips(); } catch(e){console.error("_initCustomTooltips:",e);}
    try { initSplitter(); } catch(e){console.error("initSplitter:",e);}
    try { initLogMode(); } catch(e){console.error("initLogMode:",e);}
    // Clicking the "Total: N TB" label triggers a fresh disk rescan.
    // Matches YTArchiver.py:4815 _total_disk_label.bind("<Button-1>", ...).
    // Old app pops _dark_askquestion("Refresh Sizes", "Rescan all channel folder sizes?")
    // before doing the walk — rescan is slow (walks every channel folder).
    document.getElementById("subs-total-size")?.addEventListener("click", async () => {
      const api = window.pywebview?.api;
      if (!api?.archive_rescan) return;
      const ok = await (window.askConfirm
        ? window.askConfirm("Refresh sizes",
            "Rescan all channel folder sizes?\n\nThis walks every channel folder on disk " +
            "and can take a minute or two on large archives.",
            { confirm: "Rescan" })
        : Promise.resolve(confirm("Rescan all channel folder sizes?")));
      if (!ok) return;
      window._showToast?.("Rescanning archive folder sizes\u2026", "ok");
      try {
        await api.archive_rescan();
        // After rescan completes, refresh the Subs table so totals update.
        if (api.get_subs_channels) {
          const subsData = await api.get_subs_channels();
          if (Array.isArray(subsData) && subsData.length === 2) {
            window.renderSubsTable(subsData[0], subsData[1]);
          }
        }
      } catch (e) { window._showToast?.("Rescan failed.", "error"); }
    });
    try { initBrowseSubmodes(); } catch(e){console.error("initBrowseSubmodes:",e);}
    try { initSubsContextMenu(); } catch(e){console.error("initSubsContextMenu:",e);}
    try { initRecentContextMenu(); } catch(e){console.error("initRecentContextMenu:",e);}
    try { initBrowseContextMenus(); } catch(e){console.error("initBrowseContextMenus:",e);}
    try { initEditChannelPanel(); } catch(e){console.error("initEditChannelPanel:",e);}
    try { initQueueModals(); } catch(e){console.error("initQueueModals:",e);}
    try { initBrowseSubmodeContent(); } catch(e){console.error("initBrowseSubmodeContent:",e);}
    try { initQueueBlink(); } catch(e){console.error("initQueueBlink:",e);}
    try { initSyncButton(); } catch(e){console.error("initSyncButton:",e);}
    try { initParentFolderPicker(); } catch(e){console.error("initParentFolderPicker:",e);}
    try { initColumnSort(); } catch(e){console.error("initColumnSort:",e);}
    try { initSearchView(); } catch(e){console.error("initSearchView:",e);}
    try { initManualTranscribe(); } catch(e){console.error("initManualTranscribe:",e);}
    try { initSettingsTab(); } catch(e){console.error("initSettingsTab:",e);}
    try { initAboutDialog(); } catch(e){console.error("initAboutDialog:",e);}
    try { initDiagnosticsDialog(); } catch(e){console.error("initDiagnosticsDialog:",e);}
    try { initWatchActions(); } catch(e){console.error("initWatchActions:",e);}
    try { initLogContextMenu(); } catch(e){console.error("initLogContextMenu:",e);}
    try { initUrlField(); } catch(e){console.error("initUrlField:",e);}
    try { initClearLog(); } catch(e){console.error("initClearLog:",e);}
    try { initSubsFilter(); } catch(e){console.error("initSubsFilter:",e);}
    try { initScanArchive(); } catch(e){console.error("initScanArchive:",e);}
    try { initIndexControls(); } catch(e){console.error("initIndexControls:",e);}
    try { initSettingsArchiveRoots(); } catch(e){console.error("initSettingsArchiveRoots:",e);}
    try { initSettingsSubTabs(); } catch(e){console.error("initSettingsSubTabs:",e);}
    try { initQueueAutoCheckboxes(); } catch(e){console.error("initQueueAutoCheckboxes:",e);}
    try { initRecentFilter(); } catch(e){console.error("initRecentFilter:",e);}
    try { initAutorunHistoryDialog(); } catch(e){console.error("initAutorunHistoryDialog:",e);}
    try { initDeferredLivestreams(); } catch(e){console.error("initDeferredLivestreams:",e);}
    try { initDragDropUrl(); } catch(e){console.error("initDragDropUrl:",e);}
    try { initKeyboardShortcuts(); } catch(e){console.error("initKeyboardShortcuts:",e);}
    try { initBookmarksExport(); } catch(e){console.error("initBookmarksExport:",e);}
    try { initGraphView(); } catch(e){console.error("initGraphView:",e);}
    try { initAutorun(); } catch(e){console.error("initAutorun:",e);}
    try { initLastSyncTicker(); } catch(e){console.error("initLastSyncTicker:",e);}
    try { persistSplitterOnResize(); } catch(e){console.error("persistSplitter:",e);}
    try { persistColumnWidths(); } catch(e){console.error("persistColumnWidths:",e);}
    try { observeActivityLog(); } catch(e){console.error("observeActivityLog:",e);}
    try { seedLogs(); } catch(e){console.error("seedLogs:",e);}
    // Run the missing-folder reconcile after seed so the Subs table is
    // already populated when any Remove/Relocate actions happen.
    setTimeout(() => { try { _reconcileMissingFolders(); } catch(e){} }, 1500);

    // Wire the "Queue Pending" button in the Subs header — left-click
    // queues channels with pending transcriptions/metadata, right-click
    // queues ALL channels. Matches YTArchiver.py:4817-4822 tooltip.
    const qpBtn = document.getElementById("btn-queue-pending");
    const qpCount = document.getElementById("queue-pending-count");

    // Update the badge count: sum channels with "\u2713 -N" transcribe cells
    // (i.e. channels that have completed transcription for some but have N
    // new videos pending). YTArchiver.py:5352 surfaces this in the Subs tab.
    const updateBadge = () => {
      if (!qpCount) return;
      let total = 0;
      const rows = window._subsAllRows || [];
      for (const r of rows) {
        // r.transcribe is like "A \u2713", "\u2713", "\u2014"
        // Pending count comes from the channel config, but we don't get
        // it in the rows payload directly. Approximation: count rows
        // whose transcribe cell is "A \u2713" (auto on + completed) as
        // "might have new pending" — use backend for exact count.
        if (r._pending_tx > 0 || r._pending_meta > 0) {
          total += 1;
        }
      }
      if (total > 0) {
        qpCount.hidden = false;
        qpCount.textContent = String(total);
      } else {
        qpCount.hidden = true;
      }
    };
    // Refresh whenever subs render. Hook via a MutationObserver on the
    // subs table body since re-renders are frequent.
    const _obsTarget = document.getElementById("subs-table-body");
    if (_obsTarget) {
      const obs = new MutationObserver(updateBadge);
      obs.observe(_obsTarget, { childList: true });
      updateBadge();
    }

    if (qpBtn) {
      qpBtn.addEventListener("click", async () => {
        const res = await window.pywebview?.api?.subs_queue_pending?.();
        if (res?.ok) {
          const parts = [];
          if (res.transcribe_queued) parts.push(`${res.transcribe_queued} for transcribe`);
          if (res.metadata_queued) parts.push(`${res.metadata_queued} for metadata`);
          window._showToast?.(parts.length
            ? `Queued ${parts.join(", ")}.`
            : "No pending channels.", parts.length ? "ok" : "warn");
        }
        setTimeout(updateBadge, 500);
      });
      qpBtn.addEventListener("contextmenu", async (e) => {
        e.preventDefault();
        const ok = await askConfirm?.(
          "Queue all channels",
          "Add ALL channels to the transcribe queue? This may take a long time for large libraries.",
          { confirm: "Queue all" });
        if (!ok) return;
        const res = await window.pywebview?.api?.subs_queue_all?.();
        if (res?.ok) {
          window._showToast?.(`Queued ${res.queued} channels.`, "ok");
        }
      });
      qpBtn.title = "Left-click: queue channels with pending transcriptions / metadata\nRight-click: queue ALL channels";
    }
  }

  // DOMContentLoaded may have already fired — check readyState and run immediately
  // if so. Otherwise wait for it.
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot, { once: true });
  } else {
    // Defer to next tick so the rest of the IIFE finishes setting up globals
    setTimeout(boot, 0);
  }
})();
