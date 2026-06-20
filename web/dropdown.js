/**
 * web/dropdown.js — custom <select> replacement.
 *
 * Chromium/pywebview silently drops the selected-value text on native
 * <select> elements once any non-trivial CSS styling is applied, so we
 * build our own dropdown out of divs + spans. The original <select>
 * stays in the DOM (hidden) so existing .value reads/writes keep
 * working; this widget just mirrors it.
 *
 * Self-wires on DOMContentLoaded + pywebviewready against any
 * `.settings-view .ctl-select`. Each enhanced <select> gets an
 * `_ytddRepaint` method attached so external code can force a label
 * refresh after programmatic .value changes (e.g. settings-load).
 *
 * Depends on: nothing. Loaded BEFORE app.js.
 */
(function () {
  "use strict";

  function enhanceSelect(sel) {
    if (!sel || sel.dataset.ytdd === "1") return; // idempotent

    const dd = document.createElement("div");
    dd.className = "yt-dd";
    const trigger = document.createElement("div");
    trigger.className = "yt-dd-trigger";
    trigger.tabIndex = 0;
    const valueEl = document.createElement("span");
    valueEl.className = "yt-dd-value";
    const caretEl = document.createElement("span");
    caretEl.className = "yt-dd-caret";
    caretEl.textContent = "▾"; // ▾
    trigger.appendChild(valueEl);
    trigger.appendChild(caretEl);
    dd.appendChild(trigger);

    const menu = document.createElement("div");
    menu.className = "yt-dd-menu";
    menu.hidden = true;
    dd.appendChild(menu);

    function paintMenu() {
      menu.innerHTML = "";
      const curIdx = sel.selectedIndex;
      Array.from(sel.options).forEach((opt, idx) => {
        const row = document.createElement("div");
        row.className = "yt-dd-option";
        if (idx === curIdx) {
          row.classList.add("selected");
        }
        row.textContent = opt.text || opt.value;
        row.dataset.idx = String(idx);
        row.addEventListener("click", (e) => {
          e.stopPropagation();
          const nextIdx = Number(row.dataset.idx);
          if (Number.isInteger(nextIdx)
              && nextIdx >= 0
              && nextIdx < sel.options.length) {
            sel.selectedIndex = nextIdx;
          }
          sel.dispatchEvent(new Event("change", { bubbles: true }));
          paintTrigger();
          closeMenu();
        });
        menu.appendChild(row);
      });
    }

    function paintTrigger() {
      const opt = sel.options[sel.selectedIndex] || null;
      const v = sel.value;
      valueEl.textContent = opt ? (opt.text || opt.value) : (v || "");
    }

    function openMenu() {
      paintMenu();
      menu.hidden = false;
      dd.classList.add("open");
      setTimeout(() => {
        document.addEventListener("click", onOutside, { once: true });
      }, 0);
    }
    function closeMenu() {
      menu.hidden = true;
      dd.classList.remove("open");
    }
    function onOutside(e) {
      if (!dd.contains(e.target)) closeMenu();
      else {
        setTimeout(() => {
          document.addEventListener("click", onOutside, { once: true });
        }, 0);
      }
    }

    trigger.addEventListener("click", (e) => {
      e.stopPropagation();
      if (dd.classList.contains("open")) closeMenu();
      else openMenu();
    });
    trigger.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        if (dd.classList.contains("open")) closeMenu();
        else openMenu();
      } else if (e.key === "Escape") {
        closeMenu();
      }
    });

    // Re-sync trigger label when the underlying <select> value is
    // programmatically changed (e.g. settings load populates it).
    sel.addEventListener("change", paintTrigger);
    sel._ytddRepaint = paintTrigger;

    paintTrigger();
    sel.parentNode.insertBefore(dd, sel);
    sel.classList.add("yt-dd-enhanced");
    sel.dataset.ytdd = "1";
  }

  function enhanceAllSettingsSelects() {
    document.querySelectorAll(
      ".settings-view .ctl-select").forEach((sel) => {
        try {
          enhanceSelect(sel);
        } catch (e) {
          console.warn("settings dropdown enhancement failed:", e);
          sel.classList.remove("yt-dd-enhanced");
          delete sel.dataset.ytdd;
        }
      });
  }

  document.addEventListener("DOMContentLoaded", enhanceAllSettingsSelects);
  window.addEventListener("pywebviewready", () => {
    enhanceAllSettingsSelects();
    // After load_settings() fires and sets select.value, ask the
    // triggers to repaint so the initial display matches config.
    document.querySelectorAll(".settings-view .ctl-select").forEach((s) => {
      if (s._ytddRepaint) s._ytddRepaint();
    });
  });

  // Expose for late-loaded selects that other code wants to enhance.
  window.enhanceSelect = enhanceSelect;
  window.enhanceAllSettingsSelects = enhanceAllSettingsSelects;
})();
