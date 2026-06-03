/**
 * web/onboarding.js — first-run setup wizard controller.
 *
 * Restores the onboarding lost in the tkinter -> pywebview migration:
 *   Step 1  Welcome
 *   Step 2  Pick archive folder   (api.pick_folder + api.set_parent_folder)
 *   Step 3  Install dependencies  (api.onboarding_install_core / _whisper)
 *   Step 4  Done                  (api.onboarding_finish)
 *
 * Exposes:
 *   window._startOnboarding({force})  — show the wizard. seedLogs.js calls
 *       this on first run; Settings > Tools calls it with {force:true}.
 *   window._onboardingProgress(d)     — install progress sink, called from
 *       the Python side (OnboardingMixin._push_onboarding).
 */
(function () {
  "use strict";

  const esc = (s) => (window.YT?.util?.escapeHtml
    ? window.YT.util.escapeHtml(s) : String(s == null ? "" : s));
  const api = () => (window.pywebview && window.pywebview.api) || null;
  const $ = (id) => document.getElementById(id);

  let _step = 1;
  let _folder = "";           // chosen/saved archive root
  let _coreBusy = false;
  let _whisperBusy = false;
  let _whisperChecking = false; // auto import-verify in flight
  let _deps = null;           // last probe snapshot

  const STEP_NEXT_LABEL = { 1: "Get started", 2: "Continue", 3: "Continue", 4: "Finish" };

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
    if (btn && !_coreBusy) {
      const allOk = d.ytdlp?.ok && d.ffmpeg?.ok && d.ffprobe?.ok;
      btn.textContent = allOk ? "Reinstall" : "Install";
      btn.disabled = false;
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
    if (btn && !_whisperBusy) {
      btn.textContent = wOk ? "Reinstall" : "Install";
      btn.disabled = false;
    }
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
  }

  // Auto-verify the whisper import the first time the user reaches the
  // dependencies step, so the faster-whisper row resolves to a real ✓/✗
  // on its own instead of sitting at a misleading ✗ until a manual Re-check.
  async function autoVerifyWhisper() {
    const a = api();
    if (!a || !a.onboarding_probe) return;
    if (_whisperChecking) return;
    if (_deps && _deps.whisper && _deps.whisper.checked) return; // already verified
    _whisperChecking = true;
    renderWhisperRows(_deps);       // show the spinner immediately
    try {
      const r = await a.onboarding_probe(true);
      if (r && r.ok && r.deps) _deps = Object.assign({}, _deps || {}, r.deps);
    } catch (e) {
      console.error("[onboarding] auto-verify whisper", e);
    } finally {
      _whisperChecking = false;
      renderWhisperRows(_deps);
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
      ].join("");
    }
    const warn = $("onb-done-warn");
    if (warn) {
      const coreMissing = !(d.ytdlp?.ok && d.ffmpeg?.ok && d.ffprobe?.ok);
      warn.innerHTML = coreMissing
        ? "⚠ The download tools aren't installed yet — sync/downloads "
          + "won't work until you install them (re-run setup from Settings > Tools)."
        : "";
    }
  }

  // ── step navigation ───────────────────────────────────────────────────
  function gotoStep(n) {
    _step = n;
    document.querySelectorAll("#onboarding-overlay .onb-step").forEach((s) => {
      s.hidden = (parseInt(s.dataset.step, 10) !== n);
    });
    document.querySelectorAll("#onboarding-overlay .onb-dot").forEach((d) => {
      const dn = parseInt(d.dataset.step, 10);
      d.classList.toggle("done", dn < n);
      d.classList.toggle("active", dn === n);
    });
    const back = $("onb-back");
    if (back) back.hidden = (n === 1);
    const next = $("onb-next");
    if (next) {
      next.textContent = STEP_NEXT_LABEL[n] || "Continue";
      // Gate: step 2 needs a folder before moving on.
      next.disabled = (n === 2 && !_folder);
    }
    if (n === 4) renderDoneSummary();
    // Auto-verify the whisper stack when the deps step opens so its row
    // resolves to a real ✓/✗ without the user clicking Re-check.
    if (n === 3) autoVerifyWhisper();
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
        if (isWhisper) { _whisperBusy = false; } else { _coreBusy = false; }
        if (d.state && d.state.ytdlp) renderDeps(d.state);
        // Leave the final message visible briefly, then collapse the bar.
        if (msg) msg.textContent = d.ok ? "Done." : ("Failed: " + (d.error || d.msg || "unknown"));
        if (msg) msg.classList.toggle("onb-msg-error", !d.ok);
      }
    } catch (e) {
      console.error("[onboarding] progress error", e);
    }
  }

  // ── install actions ───────────────────────────────────────────────────
  async function installCore() {
    const a = api();
    if (!a || !a.onboarding_install_core || _coreBusy) return;
    _coreBusy = true;
    const btn = $("onb-install-core");
    if (btn) { btn.disabled = true; btn.textContent = "Installing…"; }
    const wrap = $("onb-core-progress"); if (wrap) wrap.hidden = false;
    try { await a.onboarding_install_core(); }
    catch (e) { _onboardingProgress({ phase: "core", status: "error", msg: String(e) }); _coreBusy = false; }
  }

  async function installWhisper() {
    const a = api();
    if (!a || !a.onboarding_install_whisper || _whisperBusy) return;
    _whisperBusy = true;
    const btn = $("onb-install-whisper");
    if (btn) { btn.disabled = true; btn.textContent = "Installing…"; }
    const wrap = $("onb-whisper-progress"); if (wrap) wrap.hidden = false;
    try { await a.onboarding_install_whisper(); }
    catch (e) { _onboardingProgress({ phase: "whisper", status: "error", msg: String(e) }); _whisperBusy = false; }
  }

  async function recheck() {
    const a = api();
    if (!a || !a.onboarding_probe) return;
    try {
      const r = await a.onboarding_probe(true);
      if (r && r.ok && r.deps) renderDeps(r.deps);
    } catch (e) { console.error("[onboarding] recheck", e); }
  }

  async function pickFolder() {
    const a = api();
    if (!a || !a.pick_folder) return;
    const browse = $("onb-folder-browse");
    if (browse) browse.disabled = true;
    const hint = $("onb-folder-hint");
    try {
      const picked = await a.pick_folder("Choose archive root");
      if (picked && picked.ok && picked.path) {
        // Save immediately so the rest of the app + a mid-wizard quit
        // still leaves a valid output_dir.
        const saved = await a.set_parent_folder(picked.path);
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
    const a = api();
    try { if (a && a.onboarding_finish) await a.onboarding_finish(); }
    catch (e) { console.error("[onboarding] finish", e); }
    const ov = $("onboarding-overlay");
    if (ov) ov.hidden = true;
    document.removeEventListener("keydown", _escBlock, true);
  }

  function _escBlock(e) {
    if (e.repeat) return;
    if (e.key === "Escape") { e.preventDefault(); e.stopPropagation(); }
  }

  let _wired = false;
  function wireOnce() {
    if (_wired) return;
    _wired = true;
    $("onb-folder-browse")?.addEventListener("click", pickFolder);
    $("onb-install-core")?.addEventListener("click", installCore);
    $("onb-install-whisper")?.addEventListener("click", installWhisper);
    $("onb-recheck")?.addEventListener("click", recheck);
    $("onb-back")?.addEventListener("click", () => { if (_step > 1) gotoStep(_step - 1); });
    $("onb-next")?.addEventListener("click", () => {
      if (_step < 4) gotoStep(_step + 1);
      else finish();
    });
  }

  // ── entry point ───────────────────────────────────────────────────────
  async function startOnboarding(opts) {
    const ov = $("onboarding-overlay");
    if (!ov) { console.warn("[onboarding] overlay element missing"); return; }
    wireOnce();
    _step = 1; _folder = ""; _coreBusy = false; _whisperBusy = false; _whisperChecking = false;
    // Seed state from the backend (best-effort; the wizard still works if
    // this fails — the user can Browse + Re-check manually).
    try {
      const a = api();
      if (a && a.onboarding_state) {
        const st = await a.onboarding_state();
        if (st) {
          _folder = (st.output_dir || "");
          const pe = $("onb-folder-path"); if (pe) pe.value = _folder;
          renderDeps(st.deps || {});
        }
      }
    } catch (e) { console.error("[onboarding] state load", e); }
    ov.hidden = false;
    document.addEventListener("keydown", _escBlock, true);
    gotoStep(1);
    console.info("[onboarding] wizard shown", opts || {});
  }

  // Wire the Settings > Tools "Run setup again" button independently of the
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
