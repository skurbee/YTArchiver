/**
 * web/onboarding.js — setup wizard controller.
 *
 * Restores the onboarding lost in the tkinter -> pywebview migration:
 *   Step 1  Welcome
 *   Step 2  Pick archive folder   (api.pick_folder + api.set_parent_folder)
 *   Step 3  Choose traffic safety (api.onboarding_set_traffic)
 *   Step 4  Install dependencies  (api.onboarding_install_core / _whisper)
 *   Step 5  Done                  (api.onboarding_finish)
 *
 * Exposes:
 *   window._startOnboarding({force})  — show the wizard. seedLogs.js calls
 *       this on first run; Settings can call it with {force:true}.
 *   window._onboardingProgress(d)     — install progress sink, called from
 *       the Python side (OnboardingMixin._push_onboarding).
 */
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

  const esc = (s) => (window.YT?.util?.escapeHtml
    ? window.YT.util.escapeHtml(s) : String(s == null ? "" : s));
  const $ = (id) => document.getElementById(id);

  let _step = 1;
  let _folder = "";           // chosen/saved archive root
  let _coreBusy = false;
  let _whisperBusy = false;
  let _whisperChecking = false; // auto import-verify in flight
  let _deps = null;           // last probe snapshot
  let _traffic = null;        // live traffic status + projection
  let _trafficMode = "conservative";
  let _force = false;         // true when re-opened from Settings (dismissable)
  let _releaseFocusTrap = null;
  const INSTALL_TIMEOUT_MS = 10 * 60 * 1000;
  let _coreWatchdog = null;
  let _whisperWatchdog = null;

  const STEP_NEXT_LABEL = {
    1: "Get started", 2: "Continue", 3: "Continue", 4: "Continue", 5: "Finish"
  };
  const STEP_TITLE_ID = {
    1: "onb-intro-title",
    2: "onb-folder-title",
    3: "onb-traffic-title",
    4: "onb-deps-title",
    5: "onb-done-title",
  };

  function renderIntroCopy() {
    const title = $("onb-intro-title");
    const text = $("onb-intro-text");
    if (_force) {
      if (title) title.textContent = "Review YTArchiver setup";
      if (text) {
        text.textContent = "Review or change your current setup. Your existing choices stay in place unless you change them.";
      }
    } else {
      if (title) title.textContent = "Welcome to YTArchiver";
      if (text) text.textContent = "We'll choose your archive folder, traffic limits, and download tools.";
    }
  }

  function renderTraffic(traffic) {
    if (traffic) {
      _traffic = traffic;
      _trafficMode = traffic.mode || _trafficMode || "conservative";
    }
    document.querySelectorAll("#onb-traffic-grid .onb-traffic-card").forEach((card) => {
      const selected = card.dataset.trafficMode === _trafficMode;
      card.classList.toggle("selected", selected);
      card.setAttribute("aria-pressed", selected ? "true" : "false");
    });
    const custom = $("onb-traffic-custom");
    if (custom) custom.hidden = _trafficMode !== "custom";
    // A no-argument render is only a provisional card selection while the
    // save is pending.  Do not change Auto-sync until the backend returns the
    // newly committed traffic state.
    if (traffic) {
      window._setBudgetAutosyncAvailable?.(_trafficMode !== "unlimited");
    }
    const p = _traffic?.projection;
    const summary = $("onb-traffic-summary");
    if (summary) {
      let text = p?.recommendation || "Conservative is the safest starting point.";
      if (_trafficMode === "unlimited") {
        text = "Unlimited removes the hourly and daily limits. YTArchiver will "
          + "still space out YouTube requests and pause if YouTube starts "
          + "rejecting them. Auto-sync cannot use “When budget allows” in this mode.";
      } else if (p?.sweep?.channels) {
        text += ` One complete sweep is estimated at about ${p.sweep.units} operations.`;
      }
      summary.textContent = text;
      summary.classList.toggle("is-warning", _trafficMode === "unlimited" || p?.fits_complete_sweep === false);
    }
  }

  function customTrafficValues() {
    const number = (id, fallback) => {
      const value = parseInt($(id)?.value, 10);
      return Number.isFinite(value) ? value : fallback;
    };
    return {
      daily: number("onb-traffic-daily", 750),
      hourly: number("onb-traffic-hourly", 90),
      min_gap: number("onb-traffic-min-gap", 10),
      max_gap: number("onb-traffic-max-gap", 20),
    };
  }

  async function saveTraffic(mode) {
    const previousTraffic = _traffic;
    const previousMode = _trafficMode;
    _trafficMode = mode || previousMode;
    renderTraffic();
    if (!nativeBridgeUp()) {
      _traffic = previousTraffic;
      _trafficMode = previousMode;
      renderTraffic(previousTraffic);
      window._showToast?.(
        "YTArchiver is still starting. Try again in a moment.", "warn");
      return false;
    }
    try {
      const result = await bridgeCall(
        "onboarding_set_traffic", _trafficMode, customTrafficValues());
      if (result?.ok && result.youtube_traffic) {
        renderTraffic(result.youtube_traffic);
        if (result.budget_autosync_disabled) {
          window.dispatchEvent(new Event("autorun-state-changed"));
        }
        return true;
      } else {
        _traffic = previousTraffic;
        _trafficMode = previousMode;
        renderTraffic(previousTraffic);
        window._showToast?.(
          result?.error || "Could not save the traffic setting.", "error");
        return false;
      }
    } catch (e) {
      _traffic = previousTraffic;
      _trafficMode = previousMode;
      renderTraffic(previousTraffic);
      window._showToast?.("Could not save traffic safety setting: " + e, "error");
      return false;
    }
    return false;
  }

  // ── dep-row rendering ─────────────────────────────────────────────────
  // state: true → ✓, false → ✕, "pending" → spinner,
  //        null/undefined → neutral "–" (we genuinely haven't checked yet,
  //        so don't show a scary red ✗).
  function depRow(label, state, detail) {
    let icon;
    if (state === "pending") icon = '<span class="onb-spin"></span>';
    else if (state === true) icon = '<span class="onb-ok">✓</span>';
    else if (state === false) icon = '<span class="onb-no">✕</span>';
    else icon = '<span class="onb-unknown">–</span>';
    return `<div class="onb-dep-row">
      <span class="onb-dep-icon">${icon}</span>
      <span class="onb-dep-label">${esc(label)}</span>
      <span class="onb-dep-detail">${esc(detail || "")}</span>
    </div>`;
  }

  function renderCoreRows(deps) {
    const d = deps || {};
    const rows = [
      depRow("yt-dlp", d.ytdlp?.ok, d.ytdlp?.ok ? "installed" : "not found — needed to download videos"),
      depRow("ffmpeg", d.ffmpeg?.ok, d.ffmpeg?.ok ? "installed" : "not found — needed to mux/convert"),
      depRow("ffprobe", d.ffprobe?.ok, d.ffprobe?.ok ? "installed" : "comes with ffmpeg"),
    ];
    const el = $("onb-core-rows");
    if (el) el.innerHTML = rows.join("");
    // Update the Install button label/state.
    const btn = $("onb-install-core");
    const allOk = d.ytdlp?.ok && d.ffmpeg?.ok && d.ffprobe?.ok;
    if (btn) {
      btn.textContent = _coreBusy
        ? "Installing…"
        : (allOk ? "Reinstall" : "Install");
      btn.disabled = _coreBusy;
    }
  }

  function renderWhisperRows(deps) {
    const d = deps || {};
    const gpuTxt = d.gpu?.ok ? `GPU: ${d.gpu.name}` : "No NVIDIA GPU — will use CPU (slower)";
    const pyOk = d.python311?.ok;
    const wChecked = !!d.whisper?.checked;
    const wOk = !!d.whisper?.ok;
    // faster-whisper row: spinner while we auto-verify, ✓/✗ once verified,
    // neutral "–" if the import check genuinely hasn't run yet — never a
    // red ✗ just because the fast probe skipped the (slower) import test.
    let wState, wDetail;
    if (_whisperChecking) { wState = "pending"; wDetail = "checking…"; }
    else if (wChecked) { wState = wOk; wDetail = wOk ? "ready" : (pyOk ? "not installed" : "needs Python 3.11"); }
    else { wState = null; wDetail = "not checked yet"; }
    const rows = [
      depRow("Python 3.11", pyOk, pyOk ? "found" : "will be installed (per-user, no admin)"),
      depRow("faster-whisper + torch", wState, wDetail),
      depRow(d.gpu?.ok ? "GPU acceleration" : "CPU mode", d.gpu?.ok, gpuTxt),
    ];
    const el = $("onb-whisper-rows");
    if (el) el.innerHTML = rows.join("");
    const btn = $("onb-install-whisper");
    if (btn) {
      btn.textContent = _whisperBusy
        ? "Installing…"
        : (wOk ? "Reinstall" : "Install");
      btn.disabled = _whisperBusy;
    }
  }

  function renderCookieRows(deps) {
    const c = (deps && deps.cookies) || {};
    // "YouTube cookies" row: ✓ when signed in (or at least YT cookies found
    // in Firefox), ✗ when Firefox is missing or not signed into YouTube.
    let ytState, ytDetail;
    if (c.signed_in) { ytState = true; ytDetail = "signed into YouTube in Firefox"; }
    else if (c.has_yt_cookies) { ytState = true; ytDetail = "Firefox YouTube cookies found"; }
    else if (c.installed) { ytState = false; ytDetail = "Firefox found — not signed into YouTube"; }
    else { ytState = false; ytDetail = "needs Firefox (Chrome cookies aren't supported)"; }
    const rows = [
      depRow("Firefox", !!c.installed, c.installed ? "installed" : "not installed — get it from mozilla.org"),
      depRow("YouTube cookies", ytState, ytDetail),
    ];
    const el = $("onb-cookie-rows");
    if (el) el.innerHTML = rows.join("");
  }

  function renderDeps(deps) {
    deps = deps || _deps || {};
    // Preserve a previously-verified whisper result if the incoming probe
    // didn't run the (slower) import check — otherwise finishing a CORE
    // install would revert a confirmed faster-whisper ✓ back to neutral.
    if (_deps && _deps.whisper && _deps.whisper.checked &&
        deps.whisper && !deps.whisper.checked) {
      deps = Object.assign({}, deps, { whisper: _deps.whisper });
    }
    _deps = deps;
    renderCoreRows(_deps);
    renderWhisperRows(_deps);
    renderCookieRows(_deps);
  }

  // Auto-verify the whisper import the first time the user reaches the
  // dependencies step, so the faster-whisper row resolves to a real ✓/✗
  // on its own instead of sitting at a misleading ✗ until a manual Re-check.
  async function autoVerifyWhisper() {
    if (!nativeBridgeUp()) return;
    if (_whisperChecking) return;
    if (_deps && _deps.whisper && _deps.whisper.checked) return; // already verified
    _whisperChecking = true;
    renderWhisperRows(_deps);       // show the spinner immediately
    try {
      const r = await bridgeCall("onboarding_probe", true);
      if (r && r.ok && r.deps) {
        _deps = Object.assign({}, _deps || {}, r.deps);
      } else {
        throw new Error(r?.error || "The transcription check did not finish.");
      }
    } catch (e) {
      console.error("[onboarding] auto-verify whisper", e);
      window._showToast?.(`Could not verify transcription setup: ${e}`, "warn");
    } finally {
      _whisperChecking = false;
      renderWhisperRows(_deps);
      renderCookieRows(_deps);   // re-probe also refreshed cookie status
    }
  }

  function renderDoneSummary() {
    const d = _deps || {};
    const el = $("onb-done-summary");
    if (el) {
      el.innerHTML = [
        depRow("Archive folder", !!_folder, _folder || "not set"),
        depRow("Download tools", d.ytdlp?.ok && d.ffmpeg?.ok && d.ffprobe?.ok,
               (d.ytdlp?.ok && d.ffmpeg?.ok && d.ffprobe?.ok) ? "ready" : "missing — set up later in Settings"),
        depRow("AI transcription", d.whisper?.ok,
               d.whisper?.ok ? "ready" : "optional — not set up"),
        depRow("YouTube sign-in (Firefox)",
               !!(d.cookies && (d.cookies.signed_in || d.cookies.has_yt_cookies)),
               d.cookies && d.cookies.signed_in ? "signed in"
                 : (d.cookies && d.cookies.installed
                    ? "Firefox found — sign into YouTube"
                    : "install Firefox + sign into YouTube")),
        depRow("Traffic safety", _trafficMode !== "unlimited",
               _trafficMode.charAt(0).toUpperCase() + _trafficMode.slice(1)),
      ].join("");
    }
    const warn = $("onb-done-warn");
    if (warn) {
      warn.classList.remove("onb-msg-error");
      const coreMissing = !(d.ytdlp?.ok && d.ffmpeg?.ok && d.ffprobe?.ok);
      warn.innerHTML = coreMissing
        ? "⚠ The download tools aren't installed yet — sync/downloads "
          + "won't work until you install them (re-run setup from Settings)."
        : "";
    }
  }

  // ── step navigation ───────────────────────────────────────────────────
  function gotoStep(n, { focusHeading = true } = {}) {
    _step = n;
    document.querySelectorAll("#onboarding-overlay .onb-step").forEach((s) => {
      s.hidden = (parseInt(s.dataset.step, 10) !== n);
    });
    const titleId = STEP_TITLE_ID[n];
    const dialog = document.querySelector("#onboarding-overlay .onb-card");
    if (dialog && titleId) dialog.setAttribute("aria-labelledby", titleId);
    document.querySelectorAll("#onboarding-overlay .onb-dot").forEach((d) => {
      const dn = parseInt(d.dataset.step, 10);
      d.classList.toggle("done", dn < n);
      d.classList.toggle("active", dn === n);
    });
    const back = $("onb-back");
    if (back) back.hidden = (n === 1);
    const next = $("onb-next");
    if (next) {
      next.textContent = (n === 1 && _force)
        ? "Review setup"
        : (STEP_NEXT_LABEL[n] || "Continue");
      // Gate: step 2 needs a folder before moving on.
      next.disabled = (n === 2 && !_folder);
    }
    if (n === 5) renderDoneSummary();
    // Auto-verify the whisper stack when the deps step opens so its row
    // resolves to a real ✓/✗ without the user clicking Re-check.
    if (n === 4) autoVerifyWhisper();
    // Moving focus to the newly revealed heading makes the step change
    // explicit to keyboard and screen-reader users.  Initial opening is
    // handled by the focus trap so it can still restore the launch control.
    if (focusHeading && titleId) $(titleId)?.focus();
  }

  // ── progress sink (called from Python) ────────────────────────────────
  function _onboardingProgress(d) {
    try {
      if (!d || !d.phase) return;
      const isWhisper = (d.phase === "python" || d.phase === "whisper");
      const wrap = $(isWhisper ? "onb-whisper-progress" : "onb-core-progress");
      const fill = $(isWhisper ? "onb-whisper-fill" : "onb-core-fill");
      const msg = $(isWhisper ? "onb-whisper-msg" : "onb-core-msg");
      if (wrap) wrap.hidden = false;
      if (msg) {
        msg.textContent = d.msg || "";
        msg.classList.toggle("onb-msg-error", d.status === "error");
      }
      if (fill) {
        if (typeof d.pct === "number") {
          fill.classList.remove("onb-fill-indef");
          fill.style.width = Math.max(0, Math.min(100, d.pct)) + "%";
        } else if (d.status === "running") {
          // No percentage (pip steps) → indeterminate sweep.
          fill.classList.add("onb-fill-indef");
          fill.style.width = "100%";
        }
      }
      if (d.status === "done") {
        if (fill) { fill.classList.remove("onb-fill-indef"); fill.style.width = "100%"; }
        if (isWhisper) {
          _whisperBusy = false;
          _clearInstallWatchdog("whisper");
        } else {
          _coreBusy = false;
          _clearInstallWatchdog("core");
        }
        if (d.state && d.state.ytdlp) renderDeps(d.state);
        // Leave the final message visible briefly, then collapse the bar.
        if (msg) msg.textContent = d.ok ? "Done." : ("Failed: " + (d.error || d.msg || "unknown"));
        if (msg) msg.classList.toggle("onb-msg-error", !d.ok);
      }
    } catch (e) {
      console.error("[onboarding] progress error", e);
      window._showToast?.(`Setup progress update failed: ${e}`, "warn");
    }
  }

  // ── install actions ───────────────────────────────────────────────────
  function _clearInstallWatchdog(kind) {
    if (kind === "whisper") {
      if (_whisperWatchdog) clearTimeout(_whisperWatchdog);
      _whisperWatchdog = null;
    } else {
      if (_coreWatchdog) clearTimeout(_coreWatchdog);
      _coreWatchdog = null;
    }
  }

  function _startInstallWatchdog(kind) {
    _clearInstallWatchdog(kind);
    const isWhisper = kind === "whisper";
    const timer = setTimeout(async () => {
      const msg = $(isWhisper ? "onb-whisper-msg" : "onb-core-msg");
      // A transcription install can legitimately run longer than ten minutes.
      // Ask the backend whether the managed worker still exists before
      // offering Retry; a second click must never start a competing installer.
      try {
        const state = nativeBridgeUp()
          ? await bridgeCall("onboarding_state") : null;
        if (!state || state.installing?.[kind]) {
          if (msg) {
            msg.textContent = "Still installing…";
            msg.classList.remove("onb-msg-error");
          }
          _startInstallWatchdog(kind);
          return;
        }
        let deps = state.deps || {};
        if (isWhisper && !deps.whisper?.checked) {
          const verified = await bridgeCall("onboarding_probe", true);
          if (verified?.ok && verified.deps) deps = verified.deps;
        }
        const finished = isWhisper
          ? !!deps.whisper?.ok
          : !!(deps.ytdlp?.ok && deps.ffmpeg?.ok && deps.ffprobe?.ok);
        if (finished) {
          if (isWhisper) _whisperBusy = false;
          else _coreBusy = false;
          renderDeps(deps);
          const btn = $(isWhisper ? "onb-install-whisper" : "onb-install-core");
          const fill = $(isWhisper ? "onb-whisper-fill" : "onb-core-fill");
          if (btn) { btn.disabled = false; btn.textContent = "Reinstall"; }
          if (fill) {
            fill.classList.remove("onb-fill-indef");
            fill.style.width = "100%";
          }
          if (msg) {
            msg.textContent = "Done.";
            msg.classList.remove("onb-msg-error");
          }
          if (isWhisper) _whisperWatchdog = null;
          else _coreWatchdog = null;
          return;
        }
      } catch (_error) {
        if (msg) {
          msg.textContent = "Still waiting for the installer…";
          msg.classList.remove("onb-msg-error");
        }
        _startInstallWatchdog(kind);
        return;
      }
      if (isWhisper) _whisperBusy = false;
      else _coreBusy = false;
      const btn = $(isWhisper ? "onb-install-whisper" : "onb-install-core");
      const fill = $(isWhisper ? "onb-whisper-fill" : "onb-core-fill");
      if (btn) { btn.disabled = false; btn.textContent = "Retry"; }
      if (msg) {
        msg.textContent = "The installer stopped before it finished. Click Retry.";
        msg.classList.add("onb-msg-error");
      }
      if (fill) fill.classList.remove("onb-fill-indef");
      if (isWhisper) _whisperWatchdog = null;
      else _coreWatchdog = null;
    }, INSTALL_TIMEOUT_MS);
    if (isWhisper) _whisperWatchdog = timer;
    else _coreWatchdog = timer;
  }

  async function installCore() {
    if (!nativeBridgeUp() || _coreBusy) return;
    _coreBusy = true;
    _startInstallWatchdog("core");
    const btn = $("onb-install-core");
    if (btn) { btn.disabled = true; btn.textContent = "Installing…"; }
    const wrap = $("onb-core-progress"); if (wrap) wrap.hidden = false;
    try {
      const result = await bridgeCall("onboarding_install_core");
      if (!result?.ok || (!result?.started && !result?.running)) {
        throw new Error(result?.error || "The installer did not start.");
      }
      if (result.running) {
        window._showToast?.("Download tools are already being installed.", "warn");
      }
    }
    catch (e) {
      _clearInstallWatchdog("core");
      _onboardingProgress({
        phase: "core", status: "done", ok: false,
        error: e?.message || String(e), msg: e?.message || String(e),
      });
      _coreBusy = false;
      if (btn) { btn.disabled = false; btn.textContent = "Retry"; }
    }
  }

  async function installWhisper() {
    if (!nativeBridgeUp() || _whisperBusy) return;
    _whisperBusy = true;
    _startInstallWatchdog("whisper");
    const btn = $("onb-install-whisper");
    if (btn) { btn.disabled = true; btn.textContent = "Installing…"; }
    const wrap = $("onb-whisper-progress"); if (wrap) wrap.hidden = false;
    try {
      const result = await bridgeCall("onboarding_install_whisper");
      if (!result?.ok || (!result?.started && !result?.running)) {
        throw new Error(result?.error || "The installer did not start.");
      }
      if (result.running) {
        window._showToast?.("Transcription tools are already being installed.", "warn");
      }
    }
    catch (e) {
      _clearInstallWatchdog("whisper");
      _onboardingProgress({
        phase: "whisper", status: "done", ok: false,
        error: e?.message || String(e), msg: e?.message || String(e),
      });
      _whisperBusy = false;
      if (btn) { btn.disabled = false; btn.textContent = "Retry"; }
    }
  }

  async function recheck() {
    if (!nativeBridgeUp()) {
      window._showToast?.(
        "YTArchiver is still starting. Try again in a moment.", "warn");
      return;
    }
    const button = $("onb-recheck");
    if (button) button.disabled = true;
    try {
      const r = await bridgeCall("onboarding_probe", true);
      if (r && r.ok && r.deps) renderDeps(r.deps);
      else throw new Error(r?.error || "The setup check did not finish.");
    } catch (e) {
      console.error("[onboarding] recheck", e);
      window._showToast?.(`Setup re-check failed: ${e}`, "error");
    } finally {
      if (button) button.disabled = false;
    }
  }

  async function pickFolder() {
    if (!nativeBridgeUp()) return;
    const browse = $("onb-folder-browse");
    if (browse) browse.disabled = true;
    const hint = $("onb-folder-hint");
    try {
      const picked = await bridgeCall("pick_folder", "Choose archive folder");
      if (picked && picked.ok && picked.path) {
        // Save immediately so the rest of the app + a mid-wizard quit
        // still leaves a valid output_dir.
        const saved = await bridgeCall("set_parent_folder", picked.path);
        if (saved && saved.ok) {
          _folder = picked.path;
          const pe = $("onb-folder-path"); if (pe) pe.value = _folder;
          if (hint) { hint.textContent = "Saved."; hint.classList.remove("onb-msg-error"); }
          const next = $("onb-next"); if (next && _step === 2) next.disabled = false;
        } else if (hint) {
          hint.textContent = (saved && saved.error) || "Could not save folder.";
          hint.classList.add("onb-msg-error");
        }
      } else if (picked && picked.ok === false && picked.error && hint) {
        hint.textContent = "Folder picker failed: " + picked.error;
        hint.classList.add("onb-msg-error");
      }
    } catch (e) {
      if (hint) { hint.textContent = "Folder picker failed: " + String(e); hint.classList.add("onb-msg-error"); }
    } finally {
      if (browse) browse.disabled = false;
    }
  }

  async function finish() {
    const next = $("onb-next");
    if (next?.disabled) return false;
    if (next) next.disabled = true;
    let result;
    try {
      if (!nativeBridgeUp()) {
        result = {
          ok: false,
          error: "YTArchiver is still starting. Try again in a moment.",
        };
      } else {
        result = await bridgeCall("onboarding_finish");
      }
    } catch (e) {
      console.error("[onboarding] finish", e);
      result = { ok: false, error: e?.message || String(e) };
    }
    if (!result?.ok) {
      const msg = result?.error || "Setup completion could not be saved.";
      const warn = $("onb-done-warn");
      if (warn) {
        warn.textContent = `Setup is still open because it could not be saved: ${msg}`;
        warn.classList.add("onb-msg-error");
      }
      window._showToast?.(`Could not save setup completion: ${msg}`, "error");
      if (next) next.disabled = false;
      return false;
    }
    const ov = $("onboarding-overlay");
    if (ov) ov.hidden = true;
    document.removeEventListener("keydown", _escBlock, true);
    if (_releaseFocusTrap) {
      _releaseFocusTrap();
      _releaseFocusTrap = null;
    }
    return true;
  }

  // Hide the wizard WITHOUT finalizing setup. Used by the close (X) button
  // and by Escape when the wizard was re-opened from Settings ({force:true}).
  // Deliberately does NOT call onboarding_finish — only the Finish button
  // finalizes — so re-running setup and bailing leaves the existing
  // onboarded state untouched.
  function dismiss() {
    const ov = $("onboarding-overlay");
    if (ov) ov.hidden = true;
    document.removeEventListener("keydown", _escBlock, true);
    if (_releaseFocusTrap) {
      _releaseFocusTrap();
      _releaseFocusTrap = null;
    }
  }

  function _escBlock(e) {
    if (e.repeat) return;
    if (e.key === "Escape") {
      e.preventDefault();
      e.stopPropagation();
      // Genuine first-run stays trapped (deliberate — don't let setup be
      // skipped by a stray Esc). When re-opened from Settings ("Run setup
      // again"), Escape closes it like any other inspectable dialog.
      if (_force) dismiss();
    }
  }

  let _wired = false;
  function wireOnce() {
    if (_wired) return;
    _wired = true;
    $("onb-folder-browse")?.addEventListener("click", pickFolder);
    $("onb-install-core")?.addEventListener("click", installCore);
    $("onb-install-whisper")?.addEventListener("click", installWhisper);
    $("onb-recheck")?.addEventListener("click", recheck);
    document.querySelectorAll("#onb-traffic-grid .onb-traffic-card").forEach((card) => {
      card.addEventListener("click", () => saveTraffic(card.dataset.trafficMode));
    });
    ["onb-traffic-daily", "onb-traffic-hourly", "onb-traffic-min-gap",
      "onb-traffic-max-gap"].forEach((id) => {
      $(id)?.addEventListener("change", () => {
        if (_trafficMode === "custom") saveTraffic("custom");
      });
    });
    $("onb-back")?.addEventListener("click", () => { if (_step > 1) gotoStep(_step - 1); });
    $("onb-next")?.addEventListener("click", () => {
      if (_step < 5) gotoStep(_step + 1);
      else finish();
    });
    $("onb-close")?.addEventListener("click", dismiss);
  }

  // ── entry point ───────────────────────────────────────────────────────
  async function startOnboarding(opts) {
    const ov = $("onboarding-overlay");
    if (!ov) { console.warn("[onboarding] overlay element missing"); return; }
    wireOnce();
    _step = 1; _folder = ""; _whisperChecking = false;
    // Re-opened from Settings ("Run setup again") → dismissable
    // (show the X, allow Esc). Genuine first-run (no force) stays gated.
    _force = !!(opts && opts.force);
    const closeBtn = $("onb-close");
    if (closeBtn) closeBtn.hidden = !_force;
    renderIntroCopy();
    // Seed state from the backend (best-effort; the wizard still works if
    // this fails — the user can Browse + Re-check manually).
    try {
      if (nativeBridgeUp()) {
        const st = await bridgeCall("onboarding_state");
        if (st) {
          _coreBusy = !!st.installing?.core;
          _whisperBusy = !!st.installing?.whisper;
          const coreButton = $("onb-install-core");
          const whisperButton = $("onb-install-whisper");
          if (coreButton && _coreBusy) {
            coreButton.disabled = true;
            coreButton.textContent = "Installing…";
            _startInstallWatchdog("core");
          }
          if (whisperButton && _whisperBusy) {
            whisperButton.disabled = true;
            whisperButton.textContent = "Installing…";
            _startInstallWatchdog("whisper");
          }
          _folder = (st.output_dir || "");
          const pe = $("onb-folder-path"); if (pe) pe.value = _folder;
          renderDeps(st.deps || {});
          if (st.youtube_traffic) {
            _trafficMode = st.youtube_traffic.mode || "conservative";
            const cfg = st.youtube_traffic;
            const values = {
              "onb-traffic-daily": cfg.daily_limit,
              "onb-traffic-hourly": cfg.hourly_limit,
              "onb-traffic-min-gap": cfg.min_gap,
              "onb-traffic-max-gap": cfg.max_gap,
            };
            Object.entries(values).forEach(([id, value]) => {
              if ($(id) && value != null) $(id).value = String(value);
            });
            renderTraffic(st.youtube_traffic);
          }
        }
      }
    } catch (e) {
      console.error("[onboarding] state load", e);
      window._showToast?.(`Could not load setup state: ${e}`, "warn");
    }
    ov.hidden = false;
    document.addEventListener("keydown", _escBlock, true);
    gotoStep(1, { focusHeading: false });
    if (_releaseFocusTrap) _releaseFocusTrap();
    _releaseFocusTrap = window.YT?.modals?.activateFocusTrap?.(ov, {
      dialogSelector: ".onb-card",
      initialFocus: "#onb-intro-title",
    }) || null;
    console.info("[onboarding] wizard shown", opts || {});
  }

  // Wire the Settings "Run setup again" button independently of the
  // auto-show path, so it works even when onboarding never auto-triggered.
  function wireSettingsButton() {
    const b = $("btn-run-setup");
    if (b && !b._onbWired) {
      b._onbWired = true;
      b.addEventListener("click", () => startOnboarding({ force: true }));
    }
  }
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", wireSettingsButton);
  } else {
    wireSettingsButton();
  }

  window._startOnboarding = startOnboarding;
  window._onboardingProgress = _onboardingProgress;
})();
