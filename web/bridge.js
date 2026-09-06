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
      '  <strong>Some features did not finish loading</strong>',
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
        window._showToast?.("Startup issue details copied.", "ok");
      } catch {
        console.warn("[boot issues]", details);
        window._showToast?.("Could not copy the startup issue details.", "warn");
      }
    });
    banner.querySelector("#boot-issue-close")?.addEventListener("click", () => {
      banner.hidden = true;
    });
    return banner;
  }

  function _renderBootIssues() {
    if (!document.body) return;
    if (!YT.bootIssues.length) {
      const banner = document.getElementById("boot-issue-banner");
      if (banner) banner.hidden = true;
      return;
    }
    const banner = _ensureBootIssueBanner();
    const summary = banner.querySelector("#boot-issue-summary");
    if (summary) {
      const count = YT.bootIssues.length;
      summary.textContent = `${count} startup ${count === 1 ? "issue" : "issues"}. `
        + "Some features may not be available.";
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
      window._showToast(
        "Some features did not finish loading. See the banner for details.", "warn");
    }
  };

  window._clearBootIssue = function (name) {
    YT.bootIssues = YT.bootIssues.filter(issue => issue.name !== name);
    _renderBootIssues();
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
          "App connection",
          "The app connection did not finish starting, so some data may not load.",
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
    const text = msg || "YTArchiver isn't ready yet. Try again in a moment.";
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
          error: "YTArchiver isn't ready yet. Try again in a moment.",
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

  // One truthful user-facing contract for every "Sync now" entry point.
  // The backend distinguishes newly queued, already queued, actively started,
  // and queued-behind-pause. Keeping the wording here prevents individual
  // screens from silently dropping a valid result or claiming an existing
  // task was added again.
  function reportSyncOneResult(result, fallbackName = "") {
    const name = String(result?.name || fallbackName || "").trim();
    let message;
    let kind;
    if (!result?.ok) {
      message = result?.error || "Sync failed to start.";
      kind = "error";
    } else if (result.started) {
      message = name ? `Sync started for "${name}".` : "Sync started.";
      kind = "ok";
    } else if (result.paused && result.queued) {
      message = name
        ? `Added "${name}" to the sync queue. The queue is paused.`
        : "Sync queued - queue is paused.";
      kind = "warn";
    } else if (result.paused) {
      message = name
        ? `"${name}" is already in the sync queue. The queue is paused.`
        : "Already queued - queue is paused.";
      kind = "warn";
    } else if (result.queued) {
      message = name
        ? `Added "${name}" to the sync queue.`
        : "Sync queued.";
      kind = "ok";
    } else {
      message = name
        ? `"${name}" is already in the sync queue.`
        : "Already queued.";
      kind = "warn";
    }
    window._showToast?.(message, kind);
    return { message, kind };
  }

  // ── Catalog-read coordinator ───────────────────────────────────
  // Several Browse/Settings screens read through the same serialized SQLite
  // connection. Starting another request whenever a user changes a sort or
  // revisits a tab does not make that connection faster; it only leaves more
  // invisible Python work queued behind it. Keep one request running in the
  // default catalog lane, retain only the newest follow-up for each screen,
  // and identify superseded results so callers never paint them. Long-running
  // disk-only maintenance may opt into a separate lane so it does not hold up
  // ordinary catalog screens. This deliberately does NOT promise cancellation:
  // a pywebview call already executing in Python must finish.
  const _catalogReadSlots = new Map();
  const _catalogReadRunning = new Map();
  let _catalogReadOrder = 0;

  function _catalogElapsed(ms) {
    const seconds = Math.max(0, Math.floor(Number(ms || 0) / 1000));
    if (seconds < 60) return `${seconds}s`;
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const tail = String(seconds % 60).padStart(2, "0");
    return hours
      ? `${hours}h ${minutes}m ${tail}s`
      : `${minutes}m ${tail}s`;
  }

  function _catalogSlowStatus(label, elapsedMs) {
    const elapsed = _catalogElapsed(elapsedMs);
    const events = window.YT?.eventState;
    const queue = events?.snapshot?.("queue-state") || {};
    const indicator = events?.snapshot?.("indicator") || {};
    const syncActive = !!queue?.sync?.running;
    const gpuActive = !!queue?.gpu?.running;
    if (syncActive || gpuActive) {
      return {
        state: "other-work",
        text: `Other downloads or processing are active; still loading ${label} (${elapsed}).`,
        announcement: `Other downloads or processing are active; still loading ${label}.`,
      };
    }
    if (indicator?.slot === "sweep" && String(indicator.text || "").trim()) {
      return {
        state: "maintenance",
        text: `Library maintenance is active; still loading ${label} (${elapsed}).`,
        announcement: `Library maintenance is active; still loading ${label}.`,
      };
    }
    return {
      state: "slow",
      text: `Still loading ${label} (${elapsed}).`,
      announcement: `Still loading ${label}.`,
    };
  }

  function _catalogWaitingStatus(slot, request, elapsedMs) {
    const elapsed = _catalogElapsed(elapsedMs);
    const running = _catalogReadRunning.get(slot.lane);
    const label = request.options.label || "library data";
    const blocker = running?.slot === slot
      ? `an earlier request for ${label}`
      : "another library view";
    const tail = elapsedMs < 1000 ? "…" : ` (${elapsed}).`;
    return {
      text: `Waiting to refresh ${label} while ${blocker} finishes${tail}`,
      announcement: `Waiting to refresh ${label} while ${blocker} finishes.`,
    };
  }

  function _catalogStatus(slot, request, phase) {
    if (request.generation !== slot.generation) return;
    const onStatus = request.options.onStatus;
    if (typeof onStatus !== "function") return;
    const phaseStartedAt = phase === "waiting"
      ? request.queuedAt
      : (request.startedAt || request.queuedAt);
    const elapsedMs = Math.max(0, Date.now() - phaseStartedAt);
    const label = request.options.label || "library data";
    const slow = phase === "slow"
      ? _catalogSlowStatus(label, elapsedMs)
      : null;
    const waiting = phase === "waiting"
      ? _catalogWaitingStatus(slot, request, elapsedMs)
      : null;
    const text = waiting?.text || slow?.text
      || (phase === "loading" ? `Loading ${label}…` : "");
    const announcementState = slow ? `slow:${slow.state}` : phase;
    const announce = request.announcementState !== announcementState;
    if (announce) request.announcementState = announcementState;
    const announcement = waiting?.announcement || slow?.announcement
      || (phase === "loading" ? `Loading ${label}.` : "");
    try {
      onStatus({
        phase,
        text,
        elapsedMs,
        generation: request.generation,
        announce,
        announcement,
      });
    } catch (error) {
      console.warn(`[catalog read: ${slot.key}] status callback failed`, error);
    }
  }

  function _startCatalogStatusTimers(slot, request) {
    const slowAfterMs = Math.max(0,
      Number(request.options.slowAfterMs ?? 3000));
    const tickMs = Math.max(25, Number(request.options.tickMs ?? 1000));
    const startSlowUpdates = () => {
      _catalogStatus(slot, request, "slow");
      request.slowInterval = setInterval(
        () => _catalogStatus(slot, request, "slow"), tickMs);
    };
    request.slowTimer = setTimeout(startSlowUpdates, slowAfterMs);
  }

  function _startCatalogWaitingTimer(slot, request) {
    const tickMs = Math.max(25, Number(request.options.tickMs ?? 1000));
    request.waitInterval = setInterval(
      () => _catalogStatus(slot, request, "waiting"), tickMs);
  }

  function _stopCatalogStatusTimers(request) {
    clearTimeout(request?.slowTimer);
    clearInterval(request?.slowInterval);
    clearInterval(request?.waitInterval);
    if (request) {
      request.slowTimer = null;
      request.slowInterval = null;
      request.waitInterval = null;
    }
  }

  function _nextCatalogReadSlot(lane) {
    let next = null;
    for (const slot of _catalogReadSlots.values()) {
      if (slot.lane !== lane) continue;
      if (!slot.queued) continue;
      if (!next || slot.queued.order > next.queued.order) next = slot;
    }
    return next;
  }

  async function _pumpCatalogRead(lane) {
    if (_catalogReadRunning.has(lane)) return;
    const slot = _nextCatalogReadSlot(lane);
    if (!slot) return;
    const request = slot.queued;
    slot.queued = null;
    slot.running = request;
    _catalogReadRunning.set(lane, { slot, request });
    _stopCatalogStatusTimers(request);
    request.startedAt = Date.now();
    _catalogStatus(slot, request, "loading");
    _startCatalogStatusTimers(slot, request);

    const context = Object.freeze({
      generation: request.generation,
      isCurrent: () => request.generation === slot.generation,
    });
    try {
      const value = await request.task(context);
      request.resolve({
        stale: request.generation !== slot.generation,
        skipped: false,
        value,
      });
    } catch (error) {
      if (request.generation !== slot.generation) {
        request.resolve({ stale: true, skipped: false, error });
      } else {
        request.reject(error);
      }
    } finally {
      _stopCatalogStatusTimers(request);
      if (request.generation === slot.generation) {
        _catalogStatus(slot, request, "done");
      }
      slot.running = null;
      _catalogReadRunning.delete(lane);
      // All of these screens ultimately share the same SQLite reader. Start
      // the most recently requested screen next, but retain one newest request
      // for every other screen so useful background refreshes are not lost.
      _pumpCatalogRead(lane);
    }
  }

  function catalogRead(key, task, options) {
    key = String(key || "").trim();
    if (!key) throw new TypeError("catalog read key is required");
    if (typeof task !== "function") {
      throw new TypeError("catalog read task must be a function");
    }
    options = options || {};
    const lane = String(options.lane || "catalog").trim() || "catalog";
    const slotId = `${lane}\u0000${key}`;
    let slot = _catalogReadSlots.get(slotId);
    if (!slot) {
      slot = { key, lane, generation: 0, running: null, queued: null };
      _catalogReadSlots.set(slotId, slot);
    }
    const generation = ++slot.generation;
    if (slot.queued) {
      _stopCatalogStatusTimers(slot.queued);
      slot.queued.resolve({ stale: true, skipped: true, value: undefined });
    }
    const promise = new Promise((resolve, reject) => {
      slot.queued = {
        generation,
        task,
        options,
        resolve,
        reject,
        queuedAt: Date.now(),
        startedAt: null,
        order: ++_catalogReadOrder,
        announcementState: null,
        slowTimer: null,
        slowInterval: null,
        waitInterval: null,
      };
    });
    if (_catalogReadRunning.has(lane)) {
      _catalogStatus(slot, slot.queued, "waiting");
      _startCatalogWaitingTimer(slot, slot.queued);
    }
    _pumpCatalogRead(lane);
    return promise;
  }

  function catalogReadBusy(key) {
    key = String(key || "").trim();
    for (const slot of _catalogReadSlots.values()) {
      if (slot.key === key && (slot.running || slot.queued)) return true;
    }
    return false;
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
    reportSyncOneResult,
    catalogRead,
    catalogReadBusy,
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
