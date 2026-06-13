/**
 * web/redownloadSampleModal.js — 10-sample confirm modal wiring.
 *
 * The Python redownload worker pauses after the first 10 videos and emits
 * a `__control__`-tagged log payload (kind=redownload_sample) with stats.
 * logs.js intercepts the control and re-dispatches as a `yt-control`
 * CustomEvent. This module listens for that event, paints the modal,
 * starts a 5-minute auto-continue countdown (pauses on hover), and calls
 * back `api.redownload_sample_confirm()` with the user's choice.
 *
 * Self-wired on script load — no init function needed; just include
 * the script tag before the modal can fire.
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

  const modal = () => document.getElementById("redwnl-sample-modal");
  const sub = () => document.getElementById("redwnl-sample-sub");
  const stats = () => document.getElementById("redwnl-sample-stats");
  const countdown = () => document.getElementById("redwnl-sample-countdown");
  const picker = () => document.getElementById("redwnl-sample-res-picker");

  let tickHandle = null;
  let remaining = 300;

  function stopTick() {
    if (tickHandle) { clearInterval(tickHandle); tickHandle = null; }
  }
  function startTick() {
    remaining = 300;
    const paint = () => {
      const m = Math.floor(remaining / 60);
      const s = String(remaining % 60).padStart(2, "0");
      const el = countdown();
      if (el) el.textContent = `Auto-continuing in ${m}:${s}…`;
    };
    paint();
    tickHandle = setInterval(() => {
      remaining -= 1;
      if (remaining <= 0) {
        stopTick();
        answer("continue");
        return;
      }
      paint();
    }, 1000);
  }

  function answer(choice) {
    stopTick();
    const m = modal();
    if (m) m.hidden = true;
    if (!nativeBridgeUp()) return;
    try { bridgeCall("redownload_sample_confirm", choice); }
    catch (e) { console.error("redownload_sample_confirm failed:", e); }
  }

  function showPicker(show) {
    const p = picker();
    if (p) p.hidden = !show;
  }

  window.addEventListener("yt-control", (ev) => {
    const d = ev && ev.detail;
    if (!d || d.kind !== "redownload_sample") return;
    const m = modal();
    if (!m) return;
    if (sub()) {
      sub().textContent =
        `${d.sample_n || 10} files redownloaded at ${d.res_label || ""}`;
    }
    if (stats()) {
      const dir = d.direction || "smaller";
      const pct = Math.round(Math.abs(d.avg_pct || 0));
      stats().textContent = `Average size change:  ${pct}% ${dir}`;
      stats().classList.toggle("larger", dir === "larger");
    }
    showPicker(false);
    m.hidden = false;
    startTick();
    // Pause the auto-continue countdown while the user is actively
    // interacting (hover); resume when pointer leaves. Protects a user
    // reading the stats from being rug-pulled by the 5-minute timer.
    const _pauseOnHover = () => { stopTick(); };
    const _resumeOnLeave = () => {
      if (!tickHandle && !m.hidden) startTick();
    };
    m.addEventListener("mouseenter", _pauseOnHover);
    m.addEventListener("mouseleave", _resumeOnLeave);
  });

  document.addEventListener("DOMContentLoaded", () => {
    const btnC = document.getElementById("redwnl-sample-continue");
    const btnX = document.getElementById("redwnl-sample-cancel");
    const btnR = document.getElementById("redwnl-sample-change");
    const btnResOk = document.getElementById("redwnl-sample-res-ok");
    const btnResBack = document.getElementById("redwnl-sample-res-back");
    if (btnC) btnC.addEventListener("click", () => answer("continue"));
    if (btnX) btnX.addEventListener("click", () => answer("cancel"));
    if (btnR) btnR.addEventListener("click", () => {
      // Swap to res-picker; keep the countdown ticking so an unattended
      // run still auto-continues if the user walked away.
      showPicker(true);
    });
    if (btnResOk) btnResOk.addEventListener("click", () => {
      const picked = document.querySelector(
        'input[name="redwnl-sample-res"]:checked');
      if (!picked) return;
      answer(picked.value);
    });
    if (btnResBack) btnResBack.addEventListener("click", () => {
      showPicker(false);
    });
  });
})();
