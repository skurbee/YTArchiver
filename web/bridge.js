/**
 * web/bridge.js — pywebview bridge surface, one place to wrap the
 * `window.pywebview.api` proxy + ready-gate.
 *
 * *   - YT.api.<method>(...) — proxy that auto-routes to
 *     window.pywebview.api.<method> when available, or shows a SINGLE
 *     "Native mode required" toast when not. Replaces 40+ verbatim
 *     copies of that toast string and 137+ inline `pywebview?.api`
 *     resolutions.
 *   - YT.bridge.ready — promise that resolves when pywebview is up.
 *     Replaces 4 separate polling loops in the legacy app.js that each
 *     polled 20× at 150ms.
 *   - YT.bridge.inFlight(fn) — debounces concurrent calls so a button
 *     click can't double-fire while the first request is pending.
 *   - YT.bridge.setReady(ready) — ungrays [data-needs-ready] buttons
 *     after the Python side signals stage 1 is done.
 *
 * Depends on: web/util.js
 * Loaded BEFORE logs.js and app.js.
 */
(function () {
  "use strict";

  window.YT = window.YT || {};
  const YT = window.YT;

  // ── Ready promise ────────────────────────────────────────────────
  // pywebview fires a 'pywebviewready' event when window.pywebview.api
  // is ready to use. Resolve our promise either on that event OR on a
  // poll loop fallback (some pywebview versions don't dispatch it).
  let _readyResolve = null;
  const readyPromise = new Promise((resolve) => { _readyResolve = resolve; });

  function _isBridgeUp() {
    return !!(window.pywebview && window.pywebview.api);
  }

  function _markReady() {
    if (_readyResolve) {
      _readyResolve(window.pywebview && window.pywebview.api);
      _readyResolve = null;
    }
  }

  if (_isBridgeUp()) {
    // Already up (script loaded after bridge — uncommon but possible).
    queueMicrotask(_markReady);
  } else {
    window.addEventListener("pywebviewready", _markReady, { once: true });
    // Fallback: poll a few times at increasing intervals in case the
    // event was missed. This caps total wait at ~6s before giving up
    // (the legacy code did 20×150ms = 3s; we double it).
    let _tries = 0;
    const _max = 40;
    const _check = () => {
      if (_isBridgeUp()) { _markReady(); return; }
      _tries += 1;
      if (_tries >= _max) {
        console.warn("[bridge] pywebview never came up — running in browser-only mode");
        _markReady();  // resolve to undefined so callers can fall back
        return;
      }
      setTimeout(_check, 150);
    };
    setTimeout(_check, 150);
  }

  // ── "Native mode required" toast (deduped) ───────────────────────
  let _lastToastAt = 0;
  function _toastNativeRequired(msg) {
    const text = msg || "Native mode required.";
    // Dedupe — repeated handler calls within 1s only show one toast.
    const now = Date.now();
    if (now - _lastToastAt < 1000) return;
    _lastToastAt = now;
    if (typeof window._showToast === "function") {
      window._showToast(text, "warn");
    } else {
      console.warn("[bridge] " + text);
    }
  }

  // ── YT.api proxy ────────────────────────────────────────────────
  // Reads ANY property name and returns either:
  //   - the real api.<method> bound function (when bridge is up)
  //   - a no-op fallback that shows the "Native mode required" toast
  //     and returns undefined (when bridge is down)
  //
  // Usage:
  //   YT.api.subs_list()
  //   YT.api.add_channel(url)
  //
  // Replaces the 137+ inline patterns:
  //   window.pywebview?.api?.foo?.(...)
  //   const api = window.pywebview?.api; if (api) api.foo(...)
  const _apiProxy = new Proxy({}, {
    get(_target, prop) {
      // Allow JS engines to probe these without firing the toast.
      if (typeof prop === "symbol") return undefined;
      if (prop === "then") return undefined;  // not a thenable
      if (prop === "__isProxy") return true;

      return function (...args) {
        const api = window.pywebview && window.pywebview.api;
        if (api && typeof api[prop] === "function") {
          return api[prop].apply(api, args);
        }
        _toastNativeRequired();
        return undefined;
      };
    },
  });

  // ── In-flight guard ─────────────────────────────────────────────
  // Wraps a function so concurrent calls are dropped while the first
  // is still pending. Used for button handlers that kick off async
  // API calls and shouldn't double-fire.
  function inFlight(fn) {
    let busy = false;
    return async function (...args) {
      if (busy) return;
      busy = true;
      try { return await fn.apply(this, args); }
      finally { busy = false; }
    };
  }

  // ── Ready-gate ──────────────────────────────────────────────────
  // Called from Python (evaluate_js) when Stage 1 of startup finishes.
  function setReady(ready) {
    const on = !!ready;
    document.querySelectorAll("[data-needs-ready]").forEach(el => {
      if (on) {
        el.removeAttribute("disabled");
        el.classList.remove("is-locked-pre-ready");
      } else {
        el.setAttribute("disabled", "");
        el.classList.add("is-locked-pre-ready");
      }
    });
  }

  // Bare-bones bridgeCall — the legacy app.js name. Kept for back-compat
  // until the migration completes; new code should use YT.api.* directly.
  function bridgeCall(method, ...args) {
    return _apiProxy[method](...args);
  }

  // ── Expose ──────────────────────────────────────────────────────
  YT.api = _apiProxy;
  YT.bridge = {
    ready: readyPromise,
    isUp: _isBridgeUp,
    inFlight,
    setReady,
    bridgeCall,
    _toastNativeRequired,
  };

  // Back-compat globals — legacy app.js still calls these directly.
  // Patches 14-15 will migrate to YT.bridge.*.
  window._setReady = setReady;

  // Default: lock until Python says otherwise.
  document.addEventListener("DOMContentLoaded", () => {
    setReady(false);
  });
})();
