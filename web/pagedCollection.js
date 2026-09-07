/** Request ownership shared by the library's paged views.
 * Rendering, row mapping, deduplication, and cache policies belong to each view.
 * Native calls still use catalogRead; invalidation discards results, it does
 * not promise to cancel a Python call already running.
 */
(function () {
  "use strict";

  function create({ scope, label }) {
    let active = null;
    let generation = 0;
    let offset = 0;
    let hasMore = false;
    let pendingRefresh = null;

    const current = (request) => !!request && active === request;

    function invalidate() {
      generation++;
      active = null;
      pendingRefresh = null;
      hasMore = false;
    }

    function start(reset = false) {
      if (reset) {
        invalidate();
        offset = 0;
        hasMore = true;
      } else if (active || !hasMore) {
        return null;
      }
      active = { generation, offset, kind: "page" };
      return active;
    }

    function finish(request) {
      if (!current(request)) return false;
      active = null;
      const pending = pendingRefresh;
      const finishedGeneration = generation;
      pendingRefresh = null;
      if (pending) queueMicrotask(() => {
        if (generation === finishedGeneration) refresh(pending);
      });
      return true;
    }

    async function read(request, fetch, options = {}) {
      if (!current(request)) return null;
      const { onStatus, ...rest } = options;
      const outcome = await window.YT.bridge.catalogRead(scope, fetch, {
        label, ...rest,
        onStatus: onStatus ? (status) => {
          if (current(request)) onStatus(status);
        } : undefined,
      });
      if (!current(request) || outcome.stale) return null;
      const result = outcome.value;
      if (result?.error) throw new Error(result.error);
      if (!Array.isArray(result?.rows)) {
        throw new Error(`Couldn’t read ${label}. Please try again.`);
      }
      return result;
    }

    function position(request, nextOffset, more) {
      if (!current(request)) return false;
      if (!Number.isFinite(nextOffset) || nextOffset < 0) {
        throw new Error("Invalid library page offset.");
      }
      offset = nextOffset;
      hasMore = !!more;
      return true;
    }

    function commit(request, result) {
      if (!current(request)) return false;
      const proposed = result.next_offset == null
        ? request.offset + result.rows.length : Number(result.next_offset);
      // A malformed or empty final page cannot leave an endless load-more loop.
      const next = Number.isFinite(proposed) && proposed >= request.offset
        ? proposed : request.offset + result.rows.length;
      return position(request, next, result.has_more && next > request.offset);
    }

    async function refresh(task) {
      if (active) {
        pendingRefresh = task;
        return false;
      }
      const request = (active = { generation, offset, kind: "refresh" });
      try {
        return await task(request);
      } finally {
        finish(request);
      }
    }

    return Object.freeze({
      start, current, read, commit, position, finish, invalidate, refresh,
      get offset() { return offset; },
      get hasMore() { return hasMore; },
      get loading() { return !!active; },
    });
  }

  window.YT = window.YT || {};
  window.YT.pagedCollection = Object.freeze({ create });
})();
