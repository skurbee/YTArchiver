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
  YT.bootIssues = YT.bootIssues || [];

  function _bootIssueMessage(err) {
    if (!err) return "Unknown error";
    if (typeof err === "string") return err;
    if (err && typeof err.message === "string" && err.message.trim()) {
      return err.message.trim();
    }
    try { return JSON.stringify(err).slice(0, 240); }
    catch { return String(err); }
  }

  function _bootIssueDetails() {
    return YT.bootIssues.map((issue) => {
      const lines = [`[${issue.ts}] ${issue.name}: ${issue.message}`];
      if (issue.stack) lines.push(issue.stack);
      return lines.join("\n");
    }).join("\n\n");
  }

  function _ensureBootIssueBanner() {
    let banner = document.getElementById("boot-issue-banner");
    if (banner) return banner;
    banner = document.createElement("div");
    banner.id = "boot-issue-banner";
    banner.className = "boot-issue-banner";
    banner.hidden = true;
    banner.innerHTML = [
      '<div class="boot-issue-main">',
      '  <strong>UI started with issues</strong>',
      '  <span id="boot-issue-summary"></span>',
      '</div>',
      '<div class="boot-issue-actions">',
      '  <button type="button" id="boot-issue-copy" class="btn btn-thin">Copy details</button>',
      '  <button type="button" id="boot-issue-close" class="btn btn-thin">Dismiss</button>',
      '</div>',
    ].join("");
    const anchor = document.querySelector(".tab-row");
    if (anchor && anchor.parentNode) {
      anchor.parentNode.insertBefore(banner, anchor.nextSibling);
    } else {
      document.body.insertBefore(banner, document.body.firstChild);
    }
    banner.querySelector("#boot-issue-copy")?.addEventListener("click", async () => {
      const details = _bootIssueDetails();
      try {
        if (!navigator.clipboard?.writeText) {
          throw new Error("Clipboard API unavailable");
        }
        await navigator.clipboard?.writeText(details);
        window._showToast?.("Boot issue details copied.", "ok");
      } catch {
        console.warn("[boot issues]", details);
        window._showToast?.("Boot issue details written to console.", "warn");
      }
    });
    banner.querySelector("#boot-issue-close")?.addEventListener("click", () => {
      banner.hidden = true;
    });
    return banner;
  }

  function _renderBootIssues() {
    if (!YT.bootIssues.length || !document.body) return;
    const banner = _ensureBootIssueBanner();
    const summary = banner.querySelector("#boot-issue-summary");
    const names = YT.bootIssues.slice(-3).map((issue) => issue.name).join(", ");
    if (summary) {
      summary.textContent = `${YT.bootIssues.length} boot warning(s): ${names}`;
    }
    banner.hidden = false;
  }

  window._reportBootIssue = function (name, err, opts) {
    const issue = {
      name: String(name || "boot"),
      message: _bootIssueMessage(err),
      stack: err && err.stack ? String(err.stack) : "",
      level: (opts && opts.level) || "warn",
      ts: new Date().toISOString(),
    };
    YT.bootIssues.push(issue);
    console.error("[boot] " + issue.name + ":", err || issue.message);
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", _renderBootIssues, { once: true });
    } else {
      _renderBootIssues();
    }
    if (typeof window._showToast === "function") {
      window._showToast("UI started with issues. See the banner for details.", "warn");
    }
  };

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
        window._reportBootIssue?.(
          "pywebview bridge",
          "Native bridge did not become ready; desktop data may not load.",
          { level: "error" },
        );
        _markReady();  // resolve to undefined so callers can fall back
        return;
      }
      setTimeout(_check, 150);
    };
    setTimeout(_check, 150);
  }

  // ── "Native mode required" toast (deduped) ───────────────────────
  // Dedupe BY MESSAGE so two different error texts within 1s don't
  // silently swallow the second one (audit: bridge.js H146).
  const _lastToastAtBy = new Map();
  function _toastNativeRequired(msg) {
    const text = msg || "Native mode required.";
    const now = Date.now();
    const _prev = _lastToastAtBy.get(text) || 0;
    if (now - _prev < 1000) return;
    _lastToastAtBy.set(text, now);
    // Cap the map size so a flood of unique error strings can't grow
    // it unbounded.
    if (_lastToastAtBy.size > 50) {
      const _oldest = [..._lastToastAtBy.entries()].sort((a, b) => a[1] - b[1])[0];
      if (_oldest) _lastToastAtBy.delete(_oldest[0]);
    }
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
  // Cache returned function wrappers per property name so reference
  // identity is preserved across accesses — `YT.api.foo === YT.api.foo`
  // now holds, which matters for removeEventListener-style call sites
  // (audit: bridge.js:97-110).
  const API_FN_CACHE_LIMIT = 200;
  const _apiFnCache = new Map();
  const _apiProxy = new Proxy({}, {
    get(_target, prop) {
      // Allow JS engines to probe these without firing the toast.
      if (typeof prop === "symbol") return undefined;
      if (prop === "then") return undefined;  // not a thenable
      if (prop === "__isProxy") return true;

      const _cached = _apiFnCache.get(prop);
      if (_cached) return _cached;
      const _fn = function (...args) {
        const api = window.pywebview && window.pywebview.api;
        if (api && typeof api[prop] === "function") {
          return api[prop].apply(api, args);
        }
        _toastNativeRequired();
        return Promise.resolve({
          ok: false,
          error: "Native mode required",
          code: "NATIVE_BRIDGE_UNAVAILABLE",
        });
      };
      if (_apiFnCache.size >= API_FN_CACHE_LIMIT) {
        const oldest = _apiFnCache.keys().next().value;
        if (oldest !== undefined) _apiFnCache.delete(oldest);
      }
      _apiFnCache.set(prop, _fn);
      return _fn;
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
  let _readyState = false;
  function _applyReadyTo(el, on) {
    if (on) {
      el.removeAttribute("disabled");
      el.classList.remove("is-locked-pre-ready");
    } else {
      el.setAttribute("disabled", "");
      el.classList.add("is-locked-pre-ready");
    }
  }
  function setReady(ready) {
    _readyState = !!ready;
    document.querySelectorAll("[data-needs-ready]").forEach(el => {
      _applyReadyTo(el, _readyState);
    });
  }
  // MutationObserver so dynamically-added [data-needs-ready] elements
  // inherit the latest ready state. Without this, elements added
  // after setReady was last called stayed un-ready forever (audit:
  // bridge.js:131-140).
  if (typeof MutationObserver === "function") {
    try {
      const _mo = new MutationObserver(_records => {
        for (const rec of _records) {
          for (const node of rec.addedNodes) {
            if (node.nodeType !== 1) continue;
            // Fast bail for the hot path: log lines are added at
            // hundreds per second during heavy sync and never carry
            // [data-needs-ready]. Cheap class-based filter so the
            // expensive querySelectorAll only runs on potentially-
            // matching subtrees (audit: bridge.js H137).
            if (node.classList && (
                node.classList.contains("log-line")
                || node.classList.contains("activity-row"))) {
              continue;
            }
            if (node.hasAttribute && node.hasAttribute("data-needs-ready")) {
              _applyReadyTo(node, _readyState);
            }
            if (node.querySelector
                && node.querySelector("[data-needs-ready]")) {
              node.querySelectorAll("[data-needs-ready]").forEach(el => {
                _applyReadyTo(el, _readyState);
              });
            }
          }
        }
      });
      _mo.observe(document.body || document.documentElement, {
        childList: true, subtree: true,
      });
    } catch (_e) { /* MutationObserver unavailable on this WebView */ }
  }

  // Bare-bones bridgeCall — the legacy app.js name. Kept for back-compat
  // until the migration completes; new code should use YT.api.* directly.
  function bridgeCall(method, ...args) {
    // Defensive: if `method` resolves to undefined (e.g. someone
    // called `bridgeCall("then", ...)` while we serve "then" as
    // not-a-thenable), don't invoke undefined as a function (audit:
    // bridge.js:148).
    const fn = _apiProxy[method];
    return typeof fn === "function" ? fn(...args) : undefined;
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
