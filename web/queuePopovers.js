/**
 * web/queuePopovers.js — Queue Tasks + GPU Tasks popover modals
 *
 * Exposed as window.initQueueModals; app.js boot calls it once.
 */
(function () {
  "use strict";

  const _browseState = window._browseState || {};
  const showContextMenu = window.showContextMenu || (() => {});
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

  // ─── Queue popovers (Sync Tasks, GPU Tasks) ──────────────────────────
  //
  // Anchor to the icon button. Clicking the button toggles; clicking
  // outside closes. Escape closes. No backdrop dim — the popover is a
  // dropdown, not a modal.
  function initQueueModals() {
    if (window._queueModalsInited) return;
    window._queueModalsInited = true;
    const pairs = [
      ["btn-sync-tasks", "popover-sync-tasks"],
      ["btn-gpu-tasks", "popover-gpu-tasks"],
    ];
    for (const [btnId, popId] of pairs) {
      const btn = document.getElementById(btnId);
      const pop = document.getElementById(popId);
      if (!btn || !pop) continue;
      btn.setAttribute("aria-expanded", "false");
      btn.addEventListener("click", (e) => {
        e.stopPropagation();
        _togglePopover(popId, btn);
      });
    }

    // Shared open/close toggle so the SAME popover can be summoned from
    // the toolbar buttons AND the global status-bar segments, anchored to
    // whichever element was clicked. Anchoring to the clicked element (not
    // always the toolbar button) matters because the status bar lives on
    // every tab, while the toolbar button is only visible on Download.
    function _togglePopover(popId, anchorEl) {
      const pop = document.getElementById(popId);
      if (!pop || !anchorEl) return;
      const wasOpen = pop.classList.contains("open");
      closeAllQueuePopovers();
      if (!wasOpen) {
        anchorPopover(pop, anchorEl);
        pop.classList.add("open");
        for (const [bId, pId] of pairs) {
          if (pId === popId) {
            document.getElementById(bId)?.setAttribute("aria-expanded", "true");
          }
        }
      }
    }
    // Public entry point for the status bar. `which` = "sync" | "gpu".
    window.toggleQueuePopover = function (which, anchorEl) {
      const popId = which === "gpu" ? "popover-gpu-tasks" : "popover-sync-tasks";
      const fallbackBtn = document.getElementById(
        which === "gpu" ? "btn-gpu-tasks" : "btn-sync-tasks");
      _togglePopover(popId, anchorEl || fallbackBtn);
    };

    // Close every open popover and clear the aria-expanded flag on its
    // trigger button. The flag does double duty: it's correct a11y state
    // AND the custom-tooltip system (uxPolish.js) skips any element that's
    // aria-expanded, so the button's tooltip can't render on top of the
    // popover it just opened.
    function closeAllQueuePopovers() {
      document.querySelectorAll(".queue-popover.open")
        .forEach(p => p.classList.remove("open"));
      for (const [bId] of pairs) {
        document.getElementById(bId)?.setAttribute("aria-expanded", "false");
      }
    }

    // Close on outside click — but ignore clicks inside modals /
    // context menus / dropdowns that originated FROM the popover.
    // Those attach to document.body so `popover.contains(target)`
    // is false even though the user is still interacting with the
    // popover's modal (audit: queuePopovers.js H191).
    document.addEventListener("click", (e) => {
      if (e.target.closest && e.target.closest(
          ".askq-backdrop, .ctx-menu, .context-menu, .dropdown-menu, .ask-confirm")) {
        return;
      }
      const open = document.querySelectorAll(".queue-popover.open");
      let anyClosed = false;
      open.forEach(p => {
        if (!p.contains(e.target)) { p.classList.remove("open"); anyClosed = true; }
      });
      // Reset the trigger flags if any popover actually closed here.
      if (anyClosed) {
        for (const [bId, pId] of pairs) {
          const p = document.getElementById(pId);
          if (p && !p.classList.contains("open")) {
            document.getElementById(bId)?.setAttribute("aria-expanded", "false");
          }
        }
      }
    });
    // Close on Escape
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape") closeAllQueuePopovers();
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
          const s = nativeBridgeUp() ? await bridgeCall("settings_load") : undefined;
          if (s?.whisper_model) swap.value = s.whisper_model;
        } catch { /* ignore */ }
      })();

      swap.addEventListener("change", async () => {
        if (!nativeBridgeUp()) {
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
        const res = await bridgeCall("transcribe_swap_model", swap.value);
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

  window.initQueueModals = initQueueModals;
})();
