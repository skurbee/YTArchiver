/**
 * Redownload sample confirmation. The backend owns the deadline and defaults
 * to Cancel; an exact sample ID prevents late answers reaching a later job.
 */
(function () {
  "use strict";

  const modal = () => document.getElementById("redwnl-sample-modal");
  const countdown = () => document.getElementById("redwnl-sample-countdown");
  const picker = () => document.getElementById("redwnl-sample-res-picker");
  let active = null;
  let tickHandle = null;
  let releaseFocusTrap = null;

  function setBusy(busy) {
    modal()?.querySelectorAll("button, input").forEach(element => {
      element.disabled = busy;
    });
    modal()?.toggleAttribute("aria-busy", busy);
  }

  function close(sampleId) {
    if (active?.id !== sampleId) return;
    active = null;
    if (tickHandle !== null) clearInterval(tickHandle);
    tickHandle = null;
    releaseFocusTrap?.();
    releaseFocusTrap = null;
    const m = modal();
    if (m) m.hidden = true;
    setBusy(false);
  }

  function paintCountdown() {
    if (!active) return;
    const remaining = Math.max(0, Math.ceil((active.deadline - Date.now()) / 1000));
    if (countdown()) {
      const minutes = Math.floor(remaining / 60);
      const seconds = String(remaining % 60).padStart(2, "0");
      countdown().textContent = remaining > 0
        ? `Cancelling in ${minutes}:${seconds} unless you choose to continue…`
        : "Confirmation timed out. Waiting for cancellation…";
    }
    setBusy(active.answering || remaining === 0);
  }

  async function answer(choice) {
    const sample = active;
    if (!sample || sample.answering || Date.now() >= sample.deadline) return;
    if (!window.YT?.bridge?.isUp?.()) {
      window._showToast?.("The connection is unavailable. Your answer was not sent.", "error");
      return;
    }
    sample.answering = true;
    paintCountdown();
    try {
      const result = await window.YT.bridge.bridgeCall(
        "redownload_sample_confirm", choice, sample.id);
      if (active !== sample) return;
      if (result?.ok || result?.expired) {
        close(sample.id);
        if (result.expired) window._showToast?.("This sample confirmation has already closed.", "warn");
      } else {
        window._showToast?.(result?.error || "Your answer was not accepted. Try again.", "error");
      }
    } catch (error) {
      if (active === sample) window._showToast?.("Could not send your answer: " + error, "error");
    } finally {
      if (active === sample) {
        sample.answering = false;
        paintCountdown();
      }
    }
  }

  function showPicker(show) {
    if (picker()) picker().hidden = !show;
    const focusId = show ? "redwnl-sample-res-ok" : "redwnl-sample-cancel";
    document.getElementById(focusId)?.focus();
  }

  window.addEventListener("yt-control", event => {
    const data = event?.detail;
    if (data?.kind === "redownload_sample_closed") {
      close(data.sample_id);
      return;
    }
    if (data?.kind !== "redownload_sample") return;
    const deadline = Number(data.deadline_ts) * 1000;
    if (!data.sample_id || !Number.isFinite(deadline) || deadline <= Date.now()) return;
    const m = modal();
    if (!m) return;
    if (active) close(active.id);
    active = { id: data.sample_id, deadline, answering: false };
    const sub = document.getElementById("redwnl-sample-sub");
    if (sub) sub.textContent = `${data.sample_n || 10} files redownloaded at ${data.res_label || ""}`;
    const stats = document.getElementById("redwnl-sample-stats");
    if (stats) {
      const direction = data.direction || "smaller";
      stats.textContent = `Average size change: ${Math.round(Math.abs(data.avg_pct || 0))}% ${direction}`;
      stats.classList.toggle("larger", direction === "larger");
    }
    m.hidden = false;
    setBusy(false);
    showPicker(false);
    releaseFocusTrap = window.YT?.modals?.activateFocusTrap?.(m, {
      dialogSelector: ".yt-modal",
      initialFocus: "#redwnl-sample-cancel",
      onEscape: () => answer("cancel"),
    }) || null;
    paintCountdown();
    tickHandle = setInterval(paintCountdown, 250);
  });

  document.addEventListener("DOMContentLoaded", () => {
    const bind = (id, action) => document.getElementById(id)?.addEventListener("click", action);
    bind("redwnl-sample-continue", () => answer("continue"));
    bind("redwnl-sample-cancel", () => answer("cancel"));
    bind("redwnl-sample-change", () => showPicker(true));
    bind("redwnl-sample-res-ok", () => {
      const selected = document.querySelector('input[name="redwnl-sample-res"]:checked');
      if (selected) answer(selected.value);
    });
    bind("redwnl-sample-res-back", () => showPicker(false));
  });
})();
