/**
 * web/toasts.js — toast notifications.
 *
 * Extracted from app.js. Exposes:
 *   window._showToast(msg | opts, kind?)
 *
 * Where `opts` is { msg, kind: "ok"|"error"|"warn", action: {label, onClick}, ttlMs }.
 *
 * Error-kind messages run through a sanitizer that strips Python exception
 * prefixes ("TypeError: …", "pywebview.JavascriptException: …") and caps
 * length, so raw tracebacks don't reach the user. The raw text still goes
 * to console.warn so devs can inspect it.
 *
 * Auto-dismiss is visibility-aware: the timer pauses while the window is
 * minimized / tab-switched, so a burst of queued toasts doesn't flash out
 * all at once when the user comes back.
 *
 * Depends on: nothing. Pure DOM helpers. Loaded BEFORE app.js so app.js's
 * call sites resolve `window._showToast` at run time.
 */
(function () {
  "use strict";

  // UI audit #37: sanitize raw exception strings before showing them
  // to the user. Strip Python traceback prefixes, drop wrapping quotes,
  // collapse whitespace, and cap length. Raw text still gets
  // console.warn'd for developer debugging.
  function _sanitizeErrorMsg(raw) {
    // Handle non-string inputs first. A naive `String(obj)` would give
    // "[object Object]" which is meaningless to the user. Try to
    // extract a `.message` (Error objects, rejected fetches) or a
    // `.error` field (backend convention) before falling back.
    if (raw == null) return "";
    if (typeof raw !== "string") {
      if (raw && typeof raw === "object") {
        const extracted = (raw.message || raw.error
                            || raw.detail || raw.msg);
        if (typeof extracted === "string" && extracted.trim()) {
          raw = extracted;
        } else {
          // Last resort: stringify safely. Truncated so we never dump
          // a giant blob into a toast.
          try {
            raw = JSON.stringify(raw).slice(0, 200);
          } catch {
            raw = "(unreadable error)";
          }
        }
      } else {
        raw = String(raw);
      }
    }
    let s = String(raw || "").trim();
    if (!s) return "";
    // Catch the residual case where something stringified to the
    // useless default before reaching us.
    if (s === "[object Object]" || s === "[object Promise]") {
      return "Something went wrong.";
    }
    // Pywebview wraps Python exceptions in this prefix.
    s = s.replace(/^pywebview\.[A-Za-z]+:\s*/i, "");
    // Strip common Python error class prefix like "TypeError: ", but
    // only when the rest of the message is human-readable (don't eat
    // an "Error:" the developer typed intentionally — heuristic:
    // require capital + lowercase + "Error" + ":" pattern).
    s = s.replace(/^[A-Z][A-Za-z]*Error:\s*/, "");
    // Strip a leading "Error:" prefix once if the surrounding caller
    // already added it.
    s = s.replace(/^Error:\s*/i, "");
    // Browser-side Error objects stringify as "Error: message". After
    // the strip above, sometimes a stray newline / json line sneaks
    // in — collapse to first line for a toast.
    const firstLine = s.split(/\r?\n/, 1)[0].trim();
    if (firstLine) s = firstLine;
    // Cap so a 10KB stack trace doesn't blow up the toast layout.
    if (s.length > 220) s = s.slice(0, 217) + "…";
    return s || "Something went wrong.";
  }

  // Usage: window._showToast("message", "ok" | "error" | "warn")
  // Usage: window._showToast({ msg, kind, action: {label, onClick}, ttlMs })
  window._showToast = function (msgOrOpts, kind) {
    const root = document.getElementById("toast-root");
    if (!root) return;
    const opts = typeof msgOrOpts === "string"
      ? { msg: msgOrOpts, kind }
      : (msgOrOpts || {});
    if (opts.kind === "error" && opts.msg) {
      try {
        const cleaned = _sanitizeErrorMsg(opts.msg);
        if (cleaned !== opts.msg) {
          console.warn("[toast/error raw]", opts.msg);
          opts.msg = cleaned;
        }
      } catch {}
    }
    // Errors get longer read-time so a multi-line message isn't
    // dismissed before the user finishes reading it (audit:
    // toasts.js L77). Was 4500ms, now 8000ms.
    const ttl = opts.ttlMs ?? (opts.kind === "error" ? 8000 : 2500);
    const el = document.createElement("div");
    el.className = "toast " + (opts.kind || "");
    const msgEl = document.createElement("span");
    msgEl.textContent = opts.msg || "";
    el.appendChild(msgEl);
    // Defer the action-button wiring until AFTER `dismiss` is defined
    // below (closure capture). Was: bare `el.remove()` here that
    // bypassed dismiss() — leaking the timer + the visibilitychange
    // listener registered later (audit: toasts.js H131).
    let _wireActionBtn = null;
    if (opts.action) {
      const btn = document.createElement("button");
      btn.className = "toast-action";
      btn.textContent = opts.action.label || "Undo";
      btn.style.cssText = "margin-left:10px;background:transparent;border:none;color:var(--c-log-blue);cursor:pointer;text-decoration:underline;font-size:inherit;";
      el.appendChild(btn);
      _wireActionBtn = (dismissFn) => {
        btn.addEventListener("click", () => {
          try { opts.action.onClick?.(); } catch {}
          dismissFn();
        });
      };
    }
    root.appendChild(el);
    // Visibility-aware auto-dismiss — pauses the countdown while the
    // window is hidden (minimized, tab-switched, locked screen) so
    // several queued toasts don't all flash out in a rapid burst when
    // the user returns to the app.
    let remaining = ttl;
    let startedAt = Date.now();
    let timer = null;
    const dismiss = () => {
      if (timer) { clearTimeout(timer); timer = null; }
      document.removeEventListener("visibilitychange", onVis);
      el.style.transition = "opacity 0.25s, transform 0.25s";
      el.style.opacity = "0";
      el.style.transform = "translateX(20px)";
      setTimeout(() => el.remove(), 300);
    };
    const schedule = () => {
      startedAt = Date.now();
      // Enforce a 400ms minimum after re-show so a toast that was
      // already nearly dismissed before visibilitychange doesn't
      // flash and immediately disappear when the user comes back
      // (audit: toasts.js L66).
      timer = setTimeout(dismiss, Math.max(remaining, 400));
    };
    const onVis = () => {
      if (document.visibilityState === "hidden") {
        if (timer) {
          clearTimeout(timer);
          timer = null;
          remaining = Math.max(0, remaining - (Date.now() - startedAt));
        }
      } else if (!timer && el.isConnected) {
        schedule();
      }
    };
    document.addEventListener("visibilitychange", onVis);
    if (document.visibilityState !== "hidden") schedule();
    if (_wireActionBtn) _wireActionBtn(dismiss);
  };
})();
