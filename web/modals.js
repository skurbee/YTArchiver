/**
 * web/modals.js — dark dialog system.
 *
 * consolidates askQuestion / askChoice / askTextInput
 * out of app.js (lines 90-720). Adds a generic YT.modals.open()
 * primitive used by all three so the Esc-to-close + outside-click +
 * backdrop-management logic lives in ONE place.
 *
 * The old window.askQuestion / askChoice / askConfirm / askDanger /
 * askTextInput remain as global aliases so existing call sites in
 * app.js / logs.js keep working unchanged.
 *
 * Depends on: util.js, bridge.js
 * Loaded BEFORE logs.js + app.js.
 */
(function () {
  "use strict";

  window.YT = window.YT || {};
  const YT = window.YT;
  const escapeHtml = (YT.util && YT.util.escapeHtml) || (s => String(s ?? ""));
  const FOCUSABLE_SEL = [
    "a[href]",
    "button:not([disabled])",
    "input:not([disabled])",
    "select:not([disabled])",
    "textarea:not([disabled])",
    "[tabindex]:not([tabindex='-1'])",
  ].join(",");

  function visibleFocusables(root) {
    return Array.from(root.querySelectorAll(FOCUSABLE_SEL)).filter((el) => {
      if (!el || el.hidden) return false;
      const rects = el.getClientRects?.();
      return !rects || rects.length > 0;
    });
  }

  function prepareDialogSemantics(backdrop, dialogSelector) {
    const selector = dialogSelector || ".askq-dialog";
    const dialog = backdrop.matches?.(selector)
      ? backdrop
      : backdrop.querySelector(selector);
    if (!dialog) return null;
    dialog.setAttribute("role", "dialog");
    dialog.setAttribute("aria-modal", "true");
    const title = dialog.querySelector(".askq-header, .yt-modal-title");
    if (title) {
      if (!title.id) {
        title.id = `modal-title-${Math.random().toString(36).slice(2)}`;
      }
      dialog.setAttribute("aria-labelledby", title.id);
    }
    if (!dialog.hasAttribute("tabindex")) dialog.setAttribute("tabindex", "-1");
    return dialog;
  }

  function activateFocusTrap(backdrop, opts = {}) {
    if (!backdrop) return () => {};
    const previousFocus = document.activeElement;
    const dialog = prepareDialogSemantics(backdrop, opts.dialogSelector)
      || backdrop;
    const focusInitial = () => {
      const explicit = opts.initialFocus
        ? backdrop.querySelector(opts.initialFocus)
        : null;
      const target = explicit || visibleFocusables(dialog)[0] || dialog;
      try { target.focus(); } catch {}
    };
    const onKey = (e) => {
      if (e.key === "Escape" && typeof opts.onEscape === "function") {
        e.preventDefault();
        opts.onEscape();
        return;
      }
      if (e.key !== "Tab") return;
      const nodes = visibleFocusables(dialog);
      if (!nodes.length) {
        e.preventDefault();
        dialog.focus();
        return;
      }
      const first = nodes[0];
      const last = nodes[nodes.length - 1];
      if (e.shiftKey && document.activeElement === first) {
        e.preventDefault();
        last.focus();
      } else if (!e.shiftKey && document.activeElement === last) {
        e.preventDefault();
        first.focus();
      }
    };
    document.addEventListener("keydown", onKey, true);
    setTimeout(focusInitial, 30);
    return () => {
      document.removeEventListener("keydown", onKey, true);
      if (opts.restoreFocus === false) return;
      if (previousFocus && previousFocus.isConnected) {
        try { previousFocus.focus(); } catch {}
      }
    };
  }

  function bindStaticModal(modal, opts = {}) {
    if (!modal) return () => {};
    let releaseTrap = null;
    const sync = () => {
      if (!modal.hidden && isVisible(modal)) {
        if (!releaseTrap) releaseTrap = activateFocusTrap(modal, opts);
      } else if (releaseTrap) {
        releaseTrap();
        releaseTrap = null;
      }
    };
    const observer = new MutationObserver(sync);
    observer.observe(modal, {
      attributes: true,
      attributeFilter: ["hidden", "style", "class", "aria-hidden"],
    });
    sync();
    return () => {
      observer.disconnect();
      if (releaseTrap) releaseTrap();
    };
  }

  // ── Generic open() ──────────────────────────────────────────────
  // Returns a Promise that resolves with whatever resolveWith() is
  // called with from inside the body/buttons callbacks. Handles:
  //   - Esc → resolve(escapeValue)
  //   - outside-click → resolve(outsideClickValue)
  //   - cleanup animation
  //   - keydown listener add/remove
  //
  // Most callers compose this directly. askQuestion/askChoice/askTextInput
  // are pre-built wrappers.
  function openModal(opts) {
    const cfg = Object.assign({
      buildBody: null,        // (resolve) => element OR string
      bodyHtml: "",           // alternative to buildBody — raw HTML
      onKey: null,            // (e, resolve) => bool? (return true if handled)
      escapeValue: null,
      outsideClickValue: null,
      onMount: null,          // (root, resolve) => void
      onCleanup: null,        // () => void
      initialFocus: null,     // selector inside the backdrop
    }, opts || {});

    return new Promise((resolve) => {
      const backdrop = document.createElement("div");
      backdrop.className = "askq-backdrop";

      if (typeof cfg.buildBody === "function") {
        const built = cfg.buildBody(resolveOuter);
        if (built instanceof Node) backdrop.appendChild(built);
        else backdrop.innerHTML = String(built || "");
      } else {
        backdrop.innerHTML = cfg.bodyHtml || "";
      }

      document.body.appendChild(backdrop);
      const releaseFocusTrap = activateFocusTrap(backdrop, {
        initialFocus: cfg.initialFocus,
      });

      let _resolved = false;
      function resolveOuter(val) {
        if (_resolved) return;
        _resolved = true;
        try {
          if (typeof cfg.onCleanup === "function") cfg.onCleanup();
        } catch (e) {
          console.error("[modal cleanup]", e);
        }
        backdrop.style.animation = "askq-fade 0.12s ease-out reverse";
        setTimeout(() => backdrop.remove(), 120);
        document.removeEventListener("keydown", onKey);
        releaseFocusTrap();
        resolve(val);
      }

      function onKey(e) {
        if (cfg.onKey) {
          const handled = cfg.onKey(e, resolveOuter);
          if (handled) return;
        }
        if (e.key === "Escape") resolveOuter(cfg.escapeValue);
      }
      document.addEventListener("keydown", onKey);
      backdrop.addEventListener("click", (e) => {
        if (e.target === backdrop) resolveOuter(cfg.outsideClickValue);
      });

      if (cfg.onMount) {
        try { cfg.onMount(backdrop, resolveOuter); }
        catch (e) { console.error("[modal onMount]", e); }
      }
    });
  }

  function questionEnterDecision(danger, focusedAction) {
    if (focusedAction === "cancel") return "cancel";
    if (focusedAction === "confirm") return "confirm";
    return danger ? "ignore" : "confirm";
  }

  // ── askQuestion: title + body text + OK [+ Cancel] ───────────────
  function askQuestion(opts) {
    const cfg = Object.assign({
      title: "Confirm",
      message: "",
      bodyHtml: "",
      confirm: "OK",
      cancel: "Cancel",
      danger: false,
      noCancel: false,
    }, opts || {});

    return openModal({
      bodyHtml: `
        <div class="askq-dialog">
          <div class="askq-header"></div>
          <div class="askq-body"></div>
          <div class="askq-buttons">
            ${cfg.noCancel ? "" : '<button class="btn btn-ghost" data-act="cancel"></button>'}
            <button class="btn ${cfg.danger ? "btn-danger" : "btn-primary"}" data-act="confirm"></button>
          </div>
        </div>
      `,
      escapeValue: false,
      outsideClickValue: false,
      initialFocus: cfg.danger
        ? (cfg.noCancel ? ".askq-dialog" : '[data-act="cancel"]')
        : '[data-act="confirm"]',
      onKey: (e, resolveOuter) => {
        if (e.key !== "Enter") return false;

        // Respect the button the user actually selected.  In particular,
        // danger dialogs focus Cancel by default; the old unconditional
        // `true` here ran before the focused button's native click and turned
        // that visibly-safe default into approval.
        const focused = document.activeElement;
        const focusedAction = focused?.closest?.("[data-act]")?.dataset?.act;
        const decision = questionEnterDecision(cfg.danger, focusedAction);
        if (decision === "cancel") {
          e.preventDefault();
          e.stopImmediatePropagation?.();
          resolveOuter(false);
          return true;
        }
        if (decision === "confirm") {
          e.preventDefault();
          e.stopImmediatePropagation?.();
          resolveOuter(true);
          return true;
        }

        // A destructive action is never the implicit Enter default.  A
        // danger dialog without a Cancel button starts on the dialog itself;
        // the user must deliberately Tab/click to the affirmative button.
        if (cfg.danger) {
          e.preventDefault();
          e.stopImmediatePropagation?.();
          return true;
        }

        e.preventDefault();
        e.stopImmediatePropagation?.();
        resolveOuter(true);
        return true;
      },
      onMount: (root, resolveOuter) => {
        root.querySelector(".askq-header").textContent = cfg.title;
        const body = root.querySelector(".askq-body");
        if (cfg.bodyHtml) body.innerHTML = cfg.bodyHtml;
        else body.textContent = cfg.message;
        root.querySelector('[data-act="confirm"]').textContent = cfg.confirm;
        const cancelBtn = root.querySelector('[data-act="cancel"]');
        if (cancelBtn) cancelBtn.textContent = cfg.cancel;
        root.querySelector('[data-act="confirm"]').addEventListener(
          "click", () => resolveOuter(true));
        if (cancelBtn) cancelBtn.addEventListener(
          "click", () => resolveOuter(false));
      },
    });
  }

  // ── askChoice: title + body text + N action buttons + Cancel ────
  function askChoice(opts) {
    const cfg = Object.assign({
      title: "Choose",
      message: "",
      choices: [],
      buttons: null,
      cancel: "Cancel",
      cancelPlacement: "left",
      cancelKind: null,
      countdownSecs: 0,
      countdownLabel: "",
    }, opts || {});
    const choices = (cfg.buttons && cfg.buttons.length)
      ? cfg.buttons
      : (cfg.choices || []);
    const buttonsHtml = choices.map((c) => {
      const kind = c.kind || (c.primary ? "primary" : c.danger ? "danger" : "primary");
      const cls = kind === "primary" ? "btn btn-primary"
                : kind === "danger"  ? "btn btn-danger"
                                      : "btn btn-ghost";
      return `<button class="${cls}" data-value="${escapeHtml(c.value)}">${escapeHtml(c.label)}</button>`;
    }).join("");
    const hasCountdown = cfg.countdownSecs > 0;
    const compact = choices.length <= 1;
    const _cancelKind = cfg.cancelKind || (compact ? "ghost" : "danger");
    const _cancelCls = _cancelKind === "primary" ? "btn btn-primary"
                     : _cancelKind === "ghost"   ? "btn btn-ghost"
                                                  : "btn btn-danger";
    const cancelBtn = `<button class="${_cancelCls}" data-act="cancel"></button>`;
    const bodyHtml = compact ? `
      <div class="askq-dialog">
        <div class="askq-header"></div>
        <div class="askq-body"></div>
        ${hasCountdown ? '<div class="askq-countdown"></div>' : ""}
        <div class="askq-buttons askq-buttons-actions askq-buttons-inline">
          ${buttonsHtml}${cancelBtn}
        </div>
      </div>
    ` : `
      <div class="askq-dialog">
        <div class="askq-header"></div>
        <div class="askq-body"></div>
        ${hasCountdown ? '<div class="askq-countdown"></div>' : ""}
        <div class="askq-buttons askq-buttons-actions">
          ${buttonsHtml}
        </div>
        <div class="askq-buttons askq-buttons-cancel">
          ${cancelBtn}
        </div>
      </div>
    `;

    let countdownTimer = null;
    const clearCountdown = () => {
      if (countdownTimer) {
        clearInterval(countdownTimer);
        countdownTimer = null;
      }
    };
    const primary = choices.find(c => c.primary || c.kind === "primary");

    return openModal({
      bodyHtml,
      escapeValue: null,
      outsideClickValue: null,
      onKey: (e, resolveOuter) => {
        if (e.key !== "Enter") return false;

        // Enter follows the control the user actually focused.  This matters
        // when keyboard navigation moves from the primary choice to another
        // choice or to Cancel: the visible focus must win over the default.
        const focused = document.activeElement;
        const focusedChoice = focused?.closest?.("[data-value]");
        const focusedCancel = focused?.closest?.('[data-act="cancel"]');
        if (!focusedChoice && !focusedCancel && !primary) return false;

        e.preventDefault();
        e.stopImmediatePropagation?.();
        clearCountdown();
        if (focusedChoice) {
          resolveOuter(focusedChoice.dataset.value);
        } else if (focusedCancel) {
          resolveOuter(null);
        } else {
          // Preserve the documented Enter shortcut when focus is on the
          // dialog itself or another non-action element.
          resolveOuter(primary.value);
        }
        return true;
      },
      onCleanup: clearCountdown,
      onMount: (root, resolveOuter) => {
        root.querySelector(".askq-header").textContent = cfg.title;
        root.querySelector(".askq-body").textContent = cfg.message;
        root.querySelector('[data-act="cancel"]').textContent = cfg.cancel;
        const finish = (val) => {
          clearCountdown();
          resolveOuter(val);
        };
        root.querySelectorAll("[data-value]").forEach(b => {
          b.addEventListener("click", () => finish(b.dataset.value));
        });
        root.querySelector('[data-act="cancel"]').addEventListener(
          "click", () => finish(null));
        if (primary) {
          setTimeout(() => {
            const primaryButton = Array.from(root.querySelectorAll("[data-value]"))
              .find(btn => btn.dataset.value === String(primary.value));
            primaryButton?.focus();
          }, 30);
        }
        // Live countdown — auto-pick primary at zero.
        if (hasCountdown && primary) {
          let remaining = cfg.countdownSecs;
          const cdEl = root.querySelector(".askq-countdown");
          const baseLabel = cfg.countdownLabel
            || `Auto-selecting ${primary.label} in`;
          const render = () => {
            if (cdEl) cdEl.textContent = `${baseLabel} ${remaining}s…`;
          };
          render();
          countdownTimer = setInterval(() => {
            remaining -= 1;
            if (remaining <= 0) { finish(primary.value); return; }
            render();
          }, 1000);
        }
      },
    });
  }

  // ── askTextInput: title + body + one text field + OK/Cancel ─────
  function askTextInput(opts) {
    const cfg = Object.assign({
      title: "Enter text",
      message: "",
      placeholder: "",
      initial: "",
      confirm: "Save",
      cancel: "Cancel",
      allowEmpty: false,
    }, opts || {});

    return openModal({
      bodyHtml: `
        <div class="askq-dialog">
          <div class="askq-header"></div>
          <div class="askq-body"></div>
          <input type="text" class="askq-input" />
          <div class="askq-buttons">
            <button class="btn btn-ghost" data-act="cancel"></button>
            <button class="btn btn-primary" data-act="confirm"></button>
          </div>
        </div>
      `,
      escapeValue: null,
      outsideClickValue: null,
      onKey: null,    // Enter handled by save() onclick (input handles too)
      onMount: (root, resolveOuter) => {
        root.querySelector(".askq-header").textContent = cfg.title;
        root.querySelector(".askq-body").textContent = cfg.message;
        const input = root.querySelector(".askq-input");
        input.placeholder = cfg.placeholder || "";
        input.value = cfg.initial || "";
        root.querySelector('[data-act="cancel"]').textContent = cfg.cancel;
        root.querySelector('[data-act="confirm"]').textContent = cfg.confirm;
        const save = () => {
          const val = input.value || "";
          if (!cfg.allowEmpty && !val.trim()) { input.focus(); return; }
          resolveOuter(val);
        };
        root.querySelector('[data-act="confirm"]').addEventListener("click", save);
        root.querySelector('[data-act="cancel"]').addEventListener(
          "click", () => resolveOuter(null));
        input.addEventListener("keydown", (e) => {
          if (e.key === "Enter") { e.preventDefault(); save(); }
        });
        setTimeout(() => { input.focus(); input.select(); }, 30);
      },
    });
  }

  // ── Shortcut wrappers ───────────────────────────────────────────
  function askConfirm(title, message, opts) {
    return askQuestion(Object.assign({ title, message }, opts || {}));
  }
  function askDanger(title, message, confirmLabel) {
    return askQuestion({
      title, message,
      confirm: confirmLabel || "Remove",
      danger: true,
    });
  }

  // ── Expose ──────────────────────────────────────────────────────
  const escapeCloseEntries = [];
  let escapeCloseInstalled = false;

  function isVisible(el) {
    if (!el || !el.isConnected) return false;
    for (let node = el; node && node.nodeType === 1; node = node.parentElement) {
      if (node.hidden || node.getAttribute?.("aria-hidden") === "true") {
        return false;
      }
      const st = window.getComputedStyle ? window.getComputedStyle(node) : null;
      if (st && (st.display === "none" || st.visibility === "hidden")) {
        return false;
      }
      if (!st && node.style?.display === "none") return false;
    }
    return true;
  }

  function topVisibleEscapeEntry() {
    for (let i = escapeCloseEntries.length - 1; i >= 0; i--) {
      const entry = escapeCloseEntries[i];
      if (isVisible(entry.backdrop)) return entry;
    }
    return null;
  }

  function onSharedEscapeClose(e) {
    if (e.key !== "Escape") return;
    const entry = topVisibleEscapeEntry();
    if (!entry) return;
    const askOpen = Array.from(document.querySelectorAll(".askq-backdrop"))
      .some((backdrop) => backdrop !== entry.backdrop && isVisible(backdrop));
    if (askOpen) return;
    e.preventDefault();
    entry.close();
  }

  function registerEscapeClose(backdrop, closeFn) {
    if (!backdrop || typeof closeFn !== "function") return () => {};
    const entry = { backdrop, close: closeFn };
    escapeCloseEntries.push(entry);
    if (!escapeCloseInstalled) {
      document.addEventListener("keydown", onSharedEscapeClose);
      escapeCloseInstalled = true;
    }
    return () => {
      const idx = escapeCloseEntries.indexOf(entry);
      if (idx >= 0) escapeCloseEntries.splice(idx, 1);
    };
  }

  YT.modals = {
    open: openModal,
    ask: askQuestion,
    confirm: askConfirm,
    danger: askDanger,
    choice: askChoice,
    text: askTextInput,
    activateFocusTrap,
    bindStaticModal,
    registerEscapeClose,
    isVisible,
    _questionEnterDecision: questionEnterDecision,
  };

  document.addEventListener("DOMContentLoaded", () => {
    // Reusable dialogs live in the page from startup. Give them the same
    // focus containment and focus restoration as generated confirmations.
    const staticDialogs = [
      ["about-backdrop", "#about-close"],
      ["compress-dry-backdrop", "#compress-dry-res"],
      ["drift-backdrop", "#drift-channel"],
      ["repair-yt-backdrop", "#repair-yt-channel"],
      ["punct-restore-backdrop", "#punct-restore-channel"],
      ["provenance-backdrop", "#provenance-channel"],
      ["diag-backdrop", "#diag-refresh"],
      ["manual-tx-backdrop", "#manual-tx-browse"],
      ["autorun-history-backdrop", "#autorun-history-filter"],
      ["channel-editor-backdrop", "#channel-editor-close"],
    ];
    staticDialogs.forEach(([id, initialFocus]) => {
      bindStaticModal(document.getElementById(id), { initialFocus });
    });
    bindStaticModal(document.getElementById("redwnl-sample-modal"), {
      dialogSelector: ".yt-modal",
    });
  });

  // Back-compat globals — every existing app.js / logs.js call site
  // uses these. Patches 14-15 migrate to YT.modals.*.
  window.askQuestion = askQuestion;
  window.askConfirm = askConfirm;
  window.askDanger = askDanger;
  window.askChoice = askChoice;
  window.askTextInput = askTextInput;
})();
