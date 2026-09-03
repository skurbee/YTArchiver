/**
 * In-app navigation history for YTArchiver's single-page WebView UI.
 *
 * The browser's Back/Forward mouse buttons should move through app pages,
 * not navigate the WebView away from index.html. Each meaningful route is
 * stored as a same-document History API entry. A small in-memory fallback
 * keeps the buttons useful if a WebView does not expose pushState.
 */
(function () {
  "use strict";

  const MARKER = "ytarchiver-navigation-v1";
  const root = window.YT = window.YT || {};

  let initialized = false;
  let restoring = false;
  let browserHistory = false;
  let position = 0;
  let entries = [];

  function _activeSubview(panelId) {
    return document.querySelector(
      `#${panelId} .settings-subnav-btn.active[data-settings-view]`,
    )?.dataset.settingsView || null;
  }

  function _identity(item) {
    if (!item || typeof item !== "object") return "";
    return String(
      item.video_id || item.id || item.filepath || item.path
      || item.folder || item.name || item.title || "",
    );
  }

  function _captureRoute() {
    const tab = document.querySelector(".tab.active[data-tab]")?.dataset.tab
      || "download";
    const route = { tab };

    if (tab === "settings") {
      route.subview = _activeSubview("panel-settings");
    } else if (tab === "health") {
      route.subview = _activeSubview("panel-health");
    } else if (tab === "browse") {
      const state = window._browseState || {};
      route.browseSubmode = document.querySelector(
        "#panel-browse .submode-btn.active[data-submode]",
      )?.dataset.submode || state.submode || "channels";
      route.browseView = state.view || route.browseSubmode || "channels";
      // Keep object references in memory so same-session history can restore
      // a channel or video page without putting large objects in history.state.
      route.currentChannel = state.currentChannel || null;
      route.currentVideo = state.currentVideo || null;
      route.watchReturnTo = state.watchReturnTo || null;
    }
    return route;
  }

  function _routeKey(route) {
    if (!route) return "";
    return [
      route.tab || "download",
      route.subview || "",
      route.browseSubmode || "",
      route.browseView || "",
      _identity(route.currentChannel),
      _identity(route.currentVideo),
    ].join("|");
  }

  function _historyState(route, index) {
    return {
      marker: MARKER,
      position: index,
      tab: route.tab || "download",
      subview: route.subview || null,
      browseSubmode: route.browseSubmode || null,
      browseView: route.browseView || null,
    };
  }

  function _modalOpen() {
    const candidates = document.querySelectorAll(
      '[aria-modal="true"], .askq-backdrop, #onboarding-overlay',
    );
    return Array.from(candidates).some((el) => {
      if (el.hidden || el.getAttribute("aria-hidden") === "true") return false;
      if (window.YT?.modals?.isVisible) return window.YT.modals.isVisible(el);
      const style = window.getComputedStyle?.(el);
      return !style || (style.display !== "none" && style.visibility !== "hidden");
    });
  }

  function _click(selector) {
    const element = document.querySelector(selector);
    if (!element) return false;
    element.click();
    return true;
  }

  function _restoreBrowse(route) {
    const state = window._browseState;
    if (!state) return;

    const submode = route.browseSubmode || "channels";
    const currentSubmode = document.querySelector(
      "#panel-browse .submode-btn.active[data-submode]",
    )?.dataset.submode || state.submode || "channels";
    const intendedView = route.browseView || submode;
    if (currentSubmode === submode && state.view === intendedView
        && _identity(state.currentChannel) === _identity(route.currentChannel)
        && _identity(state.currentVideo) === _identity(route.currentVideo)) {
      // Returning from another top-level tab to the same Browse page should
      // keep the loaded Watch player and its playhead intact.
      return;
    }
    if (currentSubmode === submode && typeof window._browseGoBack === "function") {
      const normalWatchBack = state.view === "watch"
        && intendedView === "videos"
        && _identity(state.currentChannel) === _identity(route.currentChannel);
      const submodeWatchBack = state.view === "watch"
        && intendedView === submode
        && state.watchReturnTo === submode;
      const channelBack = submode === "channels"
        && state.view === "videos" && intendedView === "channels";
      if (normalWatchBack || submodeWatchBack || channelBack) {
        // Reuse Browse's established unwind path so grid scroll position and
        // selected-card orientation are restored exactly as with its Back UI.
        window._browseGoBack();
        return;
      }
    }
    _click(`#panel-browse .submode-btn[data-submode="${submode}"]`);

    const view = intendedView;
    if (view === submode ||
        (submode === "channels" && view === "channels")) {
      return;
    }

    if (route.currentChannel) state.currentChannel = route.currentChannel;
    if (route.currentVideo) state.currentVideo = route.currentVideo;

    if (view === "videos" && route.currentChannel) {
      try { window.loadVideosFor?.(route.currentChannel); } catch (_e) {}
      window.showView?.("videos");
      return;
    }

    if (view === "watch" && route.currentVideo) {
      const opened = window._openVideoInWatch?.(route.currentVideo);
      // The opener derives this from the current submode; restore the exact
      // prior return target after its synchronous route setup.
      state.watchReturnTo = route.watchReturnTo || state.watchReturnTo;
      Promise.resolve(opened).catch(() => {});
      return;
    }

    window.showView?.(view);
  }

  function _applyRoute(route) {
    if (!route) return;
    restoring = true;
    try {
      const tab = route.tab || "download";
      _click(`.tab[data-tab="${tab}"]`);

      if ((tab === "settings" || tab === "health") && route.subview) {
        _click(
          `#panel-${tab} .settings-subnav-btn[data-settings-view="${route.subview}"]`,
        );
      } else if (tab === "browse") {
        _restoreBrowse(route);
      }
    } finally {
      restoring = false;
    }
  }

  function record() {
    if (!initialized || restoring) return false;
    const route = _captureRoute();
    const current = entries[position];
    if (current && _routeKey(current) === _routeKey(route)) {
      // Refresh retained references without manufacturing a duplicate entry
      // when a programmatic refresh clicks the active tab again.
      entries[position] = route;
      return false;
    }

    entries = entries.slice(0, position + 1);
    entries.push(route);
    position = entries.length - 1;

    if (browserHistory) {
      try {
        window.history.pushState(_historyState(route, position), "");
      } catch (e) {
        console.warn("In-app browser history unavailable; using memory history:", e);
        browserHistory = false;
      }
    }
    return true;
  }

  function _go(delta) {
    if (!initialized) return false;
    // Do not change the page behind a blocking dialog. The mouse buttons can
    // be pressed accidentally while choosing or confirming an action.
    if (_modalOpen()) return false;
    const target = position + delta;
    if (target < 0 || target >= entries.length) return false;

    if (browserHistory) {
      if (delta < 0) window.history.back();
      else window.history.forward();
    } else {
      position = target;
      _applyRoute(entries[position]);
    }
    return true;
  }

  function _onPopState(event) {
    const state = event.state;
    if (!state || state.marker !== MARKER) return;
    const target = Number(state.position);
    if (!Number.isInteger(target) || !entries[target]) return;
    position = target;
    _applyRoute(entries[position]);
  }

  function _isHistoryButton(event) {
    return event.button === 3 || event.button === 4;
  }

  function _blockNativeMouseNavigation(event) {
    if (!_isHistoryButton(event)) return;
    event.preventDefault();
    event.stopImmediatePropagation();
  }

  function _useMouseHistory(event) {
    if (!_isHistoryButton(event)) return;
    event.preventDefault();
    event.stopImmediatePropagation();
    _go(event.button === 3 ? -1 : 1);
  }

  function initNavigationHistory() {
    if (initialized) return;
    initialized = true;
    entries = [_captureRoute()];
    position = 0;

    try {
      window.history.replaceState(_historyState(entries[0], 0), "");
      browserHistory = true;
    } catch (e) {
      browserHistory = false;
      console.warn("History API unavailable; using memory navigation history:", e);
    }

    window.addEventListener("popstate", _onPopState);
    // WebView2 reports mouse Back/Forward as buttons 3/4. Block its native
    // page navigation at capture time and drive only YTArchiver's route stack.
    window.addEventListener("mousedown", _blockNativeMouseNavigation, true);
    window.addEventListener("mouseup", _useMouseHistory, true);
    window.addEventListener("auxclick", _blockNativeMouseNavigation, true);
  }

  root.navigationHistory = {
    init: initNavigationHistory,
    record,
    back: () => _go(-1),
    forward: () => _go(1),
    get initialized() { return initialized; },
    // Read-only diagnostics used by focused browser tests.
    snapshot: () => ({
      position,
      length: entries.length,
      browserHistory,
      route: _captureRoute(),
    }),
  };
  window.initNavigationHistory = initNavigationHistory;
})();
