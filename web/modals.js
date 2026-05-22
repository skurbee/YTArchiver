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

      let _resolved = false;
      function resolveOuter(val) {
        if (_resolved) return;
        _resolved = true;
        backdrop.style.animation = "askq-fade 0.12s ease-out reverse";
        setTimeout(() => backdrop.remove(), 120);
        document.removeEventListener("keydown", onKey);
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

  // ── askQuestion: title + body text + OK [+ Cancel] ───────────────
  function askQuestion(opts) {
    const cfg = Object.assign({
      title: "Confirm",
      message: "",
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
      onKey: (e, resolveOuter) => {
        if (e.key === "Enter") { resolveOuter(true); return true; }
        return false;
      },
      onMount: (root, resolveOuter) => {
        root.querySelector(".askq-header").textContent = cfg.title;
        root.querySelector(".askq-body").textContent = cfg.message;
        root.querySelector('[data-act="confirm"]').textContent = cfg.confirm;
        const cancelBtn = root.querySelector('[data-act="cancel"]');
        if (cancelBtn) cancelBtn.textContent = cfg.cancel;
        root.querySelector('[data-act="confirm"]').addEventListener(
          "click", () => resolveOuter(true));
        if (cancelBtn) cancelBtn.addEventListener(
          "click", () => resolveOuter(false));
        // Focus the safe button by default (Cancel for danger, Confirm
        // otherwise). Audit U-10: accidental Enter on a danger dialog
        // shouldn't auto-confirm.
        // BUT: if danger+noCancel (no cancel button rendered), the
        // fallback to focusing the confirm button defeats the safety
        // entirely — an Enter press auto-confirms the destructive
        // action. In that case focus the dialog root instead, so
        // Enter doesn't immediately commit (audit: modals.js H230).
        let focusTarget;
        if (cfg.danger && !cancelBtn) {
          focusTarget = root;
          try { root.setAttribute("tabindex", "-1"); } catch {}
        } else if (cfg.danger && cancelBtn) {
          focusTarget = cancelBtn;
        } else {
          focusTarget = root.querySelector('[data-act="confirm"]');
        }
        setTimeout(() => focusTarget?.focus(), 30);
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
        ${hasCountdown ? '<div class="askq-countdown" style="margin:8px 0 12px;font-size:12px;color:var(--c-dim);"></div>' : ""}
        <div class="askq-buttons askq-buttons-actions askq-buttons-inline">
          ${buttonsHtml}${cancelBtn}
        </div>
      </div>
    ` : `
      <div class="askq-dialog">
        <div class="askq-header"></div>
        <div class="askq-body"></div>
        ${hasCountdown ? '<div class="askq-countdown" style="margin:8px 0 12px;font-size:12px;color:var(--c-dim);"></div>' : ""}
        <div class="askq-buttons askq-buttons-actions">
          ${buttonsHtml}
        </div>
        <div class="askq-buttons askq-buttons-cancel">
          ${cancelBtn}
        </div>
      </div>
    `;

    let countdownTimer = null;
    const primary = choices.find(c => c.primary || c.kind === "primary");

    return openModal({
      bodyHtml,
      escapeValue: null,
      outsideClickValue: null,
      onKey: (e, resolveOuter) => {
        if (e.key === "Enter" && primary) {
          e.preventDefault();
          if (countdownTimer) clearInterval(countdownTimer);
          resolveOuter(primary.value);
          return true;
        }
        return false;
      },
      onMount: (root, resolveOuter) => {
        root.querySelector(".askq-header").textContent = cfg.title;
        root.querySelector(".askq-body").textContent = cfg.message;
        root.querySelector('[data-act="cancel"]').textContent = cfg.cancel;
        const finish = (val) => {
          if (countdownTimer) { clearInterval(countdownTimer); countdownTimer = null; }
          resolveOuter(val);
        };
        root.querySelectorAll("[data-value]").forEach(b => {
          b.addEventListener("click", () => finish(b.dataset.value));
        });
        root.querySelector('[data-act="cancel"]').addEventListener(
          "click", () => finish(null));
        if (primary) {
          setTimeout(() => root.querySelector(`[data-value="${primary.value}"]`)?.focus(), 30);
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
  YT.modals = {
    open: openModal,
    ask: askQuestion,
    confirm: askConfirm,
    danger: askDanger,
    choice: askChoice,
    text: askTextInput,
  };

  // Back-compat globals — every existing app.js / logs.js call site
  // uses these. Patches 14-15 migrate to YT.modals.*.
  window.askQuestion = askQuestion;
  window.askConfirm = askConfirm;
  window.askDanger = askDanger;
  window.askChoice = askChoice;
  window.askTextInput = askTextInput;
})();
