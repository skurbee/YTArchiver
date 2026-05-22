/* ═══════════════════════════════════════════════════════════════════════
   uxPolish.js — global UX-polish helpers

   Extracted from app.js. Two small global behaviors that don't belong
   to any single tab and don't have feature-specific state:

     • Custom tooltip system — intercepts native `title=` attributes,
       suppresses the browser's default tooltip popup, and shows a
       dark-themed bubble after a 400ms hover delay. Matches the
       original tkinter app's _ToolTip class (delay=400, dark bg,
       viewport-clamped, flips above when no room below).

     • Global defocus — clicking a non-interactive element drops focus
       back to body so stray input cursors + focus rings vanish.
       Matches the original tkinter app's _defocus_on_click.

   Publishes:
     window.initCustomTooltips
     window.initGlobalDefocus

   Both are called from app.js boot(). No external dependencies.
   ═══════════════════════════════════════════════════════════════════════ */

(function () {
  "use strict";

  // Custom tooltip — intercepts `title="..."` attributes, suppresses the
  // browser's default tooltip (by moving the title to `data-tooltip` on
  // first hover), and shows a dark-themed popup after a 400ms delay.
  function initCustomTooltips() {
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
      // ALWAYS migrate any current `title` to `data-tooltip`. Earlier
      // versions only migrated on first hover, but elements whose
      // tooltip text changes dynamically (blink ticks rewriting
      // pauseBtn.title every 700ms) re-added the title without going
      // through the migration. Result: both the native browser
      // tooltip AND our custom bubble showed at the same time —
      // double popup. Migrating on every mouseover is cheap (a couple
      // attribute reads + one write) and guarantees the browser
      // tooltip never has a chance to fire.
      const titleAttr = el.getAttribute("title");
      if (titleAttr) {
        el.setAttribute("data-tooltip", titleAttr);
        el.removeAttribute("title");
      }
      const text = el.getAttribute("data-tooltip") || "";
      if (!text) return;
      hide();
      currentEl = el;
      timer = setTimeout(() => {
        // Bail if the element was removed from the DOM between
        // mouseover and the 400ms tooltip-show timer (audit:
        // uxPolish.js H141). Without this, mouseout never fires and
        // the bubble stays orphaned on screen.
        if (!document.contains(el)) return;
        const rect = el.getBoundingClientRect();
        let x = rect.left + rect.width / 2;
        let y = rect.bottom + 6;
        bubble = makeBubble(text, x, y);
        // Compute center FIRST, then clamp ONCE at the end so the
        // re-center doesn't push a right-edge tooltip back off-
        // screen (audit: uxPolish.js H135).
        const br = bubble.getBoundingClientRect();
        const _centered_left = (rect.left + rect.width / 2) - (br.width / 2);
        let _left = _centered_left;
        if (_left + br.width > window.innerWidth - 10) {
          _left = window.innerWidth - br.width - 10;
        }
        if (_left < 10) _left = 10;
        bubble.style.left = _left + "px";
        if (br.bottom > window.innerHeight - 10) {
          bubble.style.top = (rect.top - br.height - 6) + "px";
        }
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
  // back to body so stray input cursors + focus rings go away.
  function initGlobalDefocus() {
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

  window.initCustomTooltips = initCustomTooltips;
  window.initGlobalDefocus = initGlobalDefocus;
})();
