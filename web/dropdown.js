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
  let _dropdownSeq = 0;

  function syncAccessibleName(sel, trigger) {
    const labelledBy = sel.getAttribute("aria-labelledby");
    const ariaLabel = sel.getAttribute("aria-label");
    if (labelledBy) {
      trigger.setAttribute("aria-labelledby", labelledBy);
      trigger.removeAttribute("aria-label");
    } else if (ariaLabel) {
      trigger.setAttribute("aria-label", ariaLabel);
      trigger.removeAttribute("aria-labelledby");
    } else {
      const explicit = sel.id
        ? Array.from(document.querySelectorAll("label[for]")).find(
            label => label.htmlFor === sel.id)
        : null;
      const wrapped = sel.closest("label");
      const label = explicit || wrapped;
      const labelText = label?.textContent?.replace(/\s+/g, " ").trim();
      if (labelText) trigger.setAttribute("aria-label", labelText);
      else if (sel.title) trigger.setAttribute("aria-label", sel.title);
      else trigger.removeAttribute("aria-label");
      trigger.removeAttribute("aria-labelledby");
    }
    const describedBy = sel.getAttribute("aria-describedby");
    if (describedBy) trigger.setAttribute("aria-describedby", describedBy);
    else trigger.removeAttribute("aria-describedby");
    if (sel.title) trigger.title = sel.title;
    else trigger.removeAttribute("title");
  }

  function enhanceSelect(sel) {
    if (!sel || sel.dataset.ytdd === "1") return; // idempotent

    const dd = document.createElement("div");
    dd.className = "yt-dd";
    const widgetId = `yt-dd-${sel.id || ++_dropdownSeq}`;
    const trigger = document.createElement("div");
    trigger.className = "yt-dd-trigger";
    trigger.tabIndex = 0;
    trigger.setAttribute("role", "combobox");
    trigger.setAttribute("aria-haspopup", "listbox");
    trigger.setAttribute("aria-expanded", "false");
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
    menu.id = `${widgetId}-menu`;
    menu.setAttribute("role", "listbox");
    trigger.setAttribute("aria-controls", menu.id);
    menu.hidden = true;
    dd.appendChild(menu);
    let activeRow = null;

    function enabledRows() {
      return Array.from(menu.querySelectorAll(".yt-dd-option:not(.disabled)"));
    }

    function setActiveRow(row) {
      menu.querySelectorAll(".yt-dd-option.active").forEach((item) => {
        item.classList.remove("active");
      });
      activeRow = row || null;
      if (!activeRow) {
        trigger.removeAttribute("aria-activedescendant");
        return;
      }
      activeRow.classList.add("active");
      trigger.setAttribute("aria-activedescendant", activeRow.id);
      activeRow.scrollIntoView({ block: "nearest" });
    }

    function chooseRow(row) {
      if (!row || row.classList.contains("disabled")) return;
      const nextIdx = Number(row.dataset.idx);
      if (Number.isInteger(nextIdx)
          && nextIdx >= 0
          && nextIdx < sel.options.length) {
        sel.selectedIndex = nextIdx;
      }
      sel.dispatchEvent(new Event("change", { bubbles: true }));
      paintTrigger();
      closeMenu();
    }

    function paintMenu() {
      menu.innerHTML = "";
      const curIdx = sel.selectedIndex;
      Array.from(sel.options).forEach((opt, idx) => {
        if (opt.hidden) return;
        const row = document.createElement("div");
        row.className = "yt-dd-option";
        row.id = `${widgetId}-option-${idx}`;
        row.setAttribute("role", "option");
        if (opt.disabled) {
          row.classList.add("disabled");
          row.setAttribute("aria-disabled", "true");
        }
        if (idx === curIdx) {
          row.classList.add("selected");
        }
        row.setAttribute("aria-selected", idx === curIdx ? "true" : "false");
        const label = opt.text || opt.value;
        const tooltip = opt.dataset.tooltip || opt.title || "";
        const featured = opt.dataset.featured || "";
        row.textContent = label;
        if (featured) {
          row.classList.add("featured");
          row.dataset.featured = featured;
        }
        if (tooltip) {
          row.dataset.tooltip = tooltip;
          row.setAttribute("aria-label", `${label}. ${tooltip}`);
        }
        row.dataset.idx = String(idx);
        row.addEventListener("click", (e) => {
          e.stopPropagation();
          chooseRow(row);
        });
        row.addEventListener("mousemove", () => {
          if (!opt.disabled) setActiveRow(row);
        });
        menu.appendChild(row);
      });
    }

    function paintTrigger() {
      syncAccessibleName(sel, trigger);
      const opt = sel.options[sel.selectedIndex] || null;
      const v = sel.value;
      valueEl.textContent = opt ? (opt.text || opt.value) : (v || "");
      const disabled = !!sel.disabled;
      dd.classList.toggle("disabled", disabled);
      trigger.setAttribute("aria-disabled", disabled ? "true" : "false");
      trigger.tabIndex = disabled ? -1 : 0;
      if (disabled) closeMenu();
    }

    function positionMenu() {
      dd.classList.remove("open-up");
      menu.classList.remove("align-right");
      menu.style.maxHeight = "";
      menu.style.maxWidth = "";

      const clip = dd.closest(".settings-main");
      const clipRect = clip?.getBoundingClientRect?.();
      const topLimit = Math.max(4, clipRect?.top ?? 4);
      const bottomLimit = Math.min(
        window.innerHeight - 4,
        clipRect?.bottom ?? (window.innerHeight - 4));
      const leftLimit = Math.max(4, clipRect?.left ?? 4);
      const rightLimit = Math.min(
        window.innerWidth - 4,
        clipRect?.right ?? (window.innerWidth - 4));
      const triggerRect = trigger.getBoundingClientRect();
      const naturalHeight = Math.min(menu.scrollHeight || 0, 280);
      const spaceBelow = Math.max(0, bottomLimit - triggerRect.bottom - 4);
      const spaceAbove = Math.max(0, triggerRect.top - topLimit - 4);
      const openUp = naturalHeight > spaceBelow && spaceAbove > spaceBelow;
      dd.classList.toggle("open-up", openUp);
      const available = openUp ? spaceAbove : spaceBelow;
      menu.style.maxHeight = `${Math.max(48, Math.min(280, available))}px`;
      menu.style.maxWidth = `${Math.max(140, rightLimit - leftLimit)}px`;

      const menuRect = menu.getBoundingClientRect();
      if (menuRect.right > rightLimit
          && triggerRect.right - menuRect.width >= leftLimit) {
        menu.classList.add("align-right");
      }
    }

    function openMenu() {
      if (sel.disabled) return;
      paintMenu();
      menu.hidden = false;
      dd.classList.add("open");
      trigger.setAttribute("aria-expanded", "true");
      positionMenu();
      setActiveRow(menu.querySelector(".yt-dd-option.selected")
        || enabledRows()[0] || null);
      setTimeout(() => {
        document.addEventListener("click", onOutside, { once: true });
      }, 0);
    }
    function closeMenu() {
      menu.hidden = true;
      dd.classList.remove("open", "open-up");
      menu.classList.remove("align-right");
      trigger.setAttribute("aria-expanded", "false");
      setActiveRow(null);
    }
    function moveActive(delta) {
      const rows = enabledRows();
      if (!rows.length) return;
      let idx = rows.indexOf(activeRow);
      if (idx < 0) idx = 0;
      else idx = (idx + delta + rows.length) % rows.length;
      setActiveRow(rows[idx]);
    }
    function onOutside(e) {
      if (!dd.classList.contains("open")) return;
      if (!dd.contains(e.target)) closeMenu();
      else {
        setTimeout(() => {
          document.addEventListener("click", onOutside, { once: true });
        }, 0);
      }
    }

    trigger.addEventListener("click", (e) => {
      e.stopPropagation();
      if (sel.disabled) return;
      if (dd.classList.contains("open")) closeMenu();
      else openMenu();
    });
    trigger.addEventListener("keydown", (e) => {
      if (sel.disabled) return;
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        if (dd.classList.contains("open")) chooseRow(activeRow);
        else openMenu();
      } else if (e.key === "ArrowDown" || e.key === "ArrowUp") {
        e.preventDefault();
        if (!dd.classList.contains("open")) openMenu();
        moveActive(e.key === "ArrowDown" ? 1 : -1);
      } else if ((e.key === "Home" || e.key === "End")
          && dd.classList.contains("open")) {
        e.preventDefault();
        const rows = enabledRows();
        setActiveRow(e.key === "Home" ? rows[0] : rows[rows.length - 1]);
      } else if (e.key === "Escape") {
        e.preventDefault();
        closeMenu();
      } else if (e.key === "Tab") {
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
