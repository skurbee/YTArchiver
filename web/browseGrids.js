/* ═══════════════════════════════════════════════════════════════════════
   browseGrids.js — YTArchiver Browse tab grid renderers

   Extracted from logs.js (~500 lines). Owns the YouTube-style 3-view
   browse experience:
     • Channel grid (Browse landing) — banner + circular avatar + overlay
     • Video grid (inside a channel) — thumbnail cards, year/month groups
     • Lazy-load batching via shared IntersectionObserver
     • Image prefetch throttle (2 concurrent, capped at 40 cards)
     • Date/byte formatting helpers shared by both card types

   Publishes:
     window.renderChannelGrid
     window.renderVideoGrid
     window._buildVideoCard           — also called from recentTable.js
     window.renderBrowseTree          — legacy no-op, kept for old callers

   Reads:
     window._escapeHtml               — single canonical escapeHtml from util.js
   ═══════════════════════════════════════════════════════════════════════ */

(function () {
  "use strict";

  const escapeHtml = window._escapeHtml || ((s) => String(s ?? ""));

  /** Legacy tree renderer — no-op since Browse switched to the channel
      grid layout. Kept so existing call sites don't error. */
  window.renderBrowseTree = function (_rows) { /* no-op */ };

  // ─── Browse tab: YouTube-style 3-view flow ───────────────────────────

  // Deterministic gradient from a string (first letter → hue).
  function gradientFor(name) {
    const s = (name || "").trim();
    const first = (s[0] || "?").toUpperCase();
    const codepoint = first.charCodeAt(0);
    const hue = (codepoint * 47) % 360;
    const hue2 = (hue + 40) % 360;
    return `linear-gradient(135deg, hsl(${hue}, 55%, 28%) 0%, hsl(${hue2}, 60%, 18%) 100%)`;
  }

  /** Render the Channels grid (Browse tab landing view).
   *
   * YouTube-style: banner fills the card, circular PFP overlaps
   * bottom-left, name + stats in a dark-gradient overlay below.
   * Images use <img loading="lazy"> so 100+ channels don't fetch all
   * thumbnails on mount (fixes scroll lag the user reported).
   */
  window.renderChannelGrid = function (channels, onChannelClick) {
    const grid = document.getElementById("channel-grid");
    if (!grid) return;
    grid.innerHTML = "";
    const frag = document.createDocumentFragment();
    for (const c of channels) {
      const name = c.folder || c.name || "";
      const vids = c.n_vids || c.video_count || "—";
      const size = c.size || "";
      const first = (name[0] || "?").toUpperCase();

      const bannerUrl = c.banner_url || "";
      const avatarUrl = c.avatar_url || "";
      // Banner priority: explicit banner > avatar zoomed-to-fill > gradient.
      const bannerSrc = bannerUrl || avatarUrl || "";

      const card = document.createElement("div");
      card.className = "channel-card";
      if (!bannerSrc) {
        // Pure-gradient fallback gets the tinted bg directly.
        card.style.background = gradientFor(name);
      }
      if (typeof c.transcription_pending === "number") {
        card.dataset.pendingTx = String(c.transcription_pending);
      }
      if (typeof c.metadata_pending === "number") {
        card.dataset.pendingMeta = String(c.metadata_pending);
      }

      const bgImg = bannerSrc
        ? `<img class="channel-card-bg" src="${bannerSrc}" loading="lazy" decoding="async" alt="" />`
        : "";
      const avatarImg = avatarUrl
        ? `<img class="channel-avatar" src="${avatarUrl}" loading="lazy" decoding="async" alt="" />`
        : "";
      const letterHtml = (!bannerSrc && !avatarUrl)
        ? `<div class="channel-letter">${escapeHtml(first)}</div>`
        : "";

      card.innerHTML = `
        ${bgImg}
        ${letterHtml}
        <div class="channel-card-overlay">
          <div class="channel-card-name"></div>
          <div class="channel-card-meta"></div>
        </div>
        ${avatarImg}
      `;
      card.querySelector(".channel-card-name").textContent = name;
      card.querySelector(".channel-card-meta").textContent =
        `${vids}${vids && vids !== "—" ? " videos" : ""}${size ? " · " + size : ""}`;

      // Swap to gradient if the banner image fails to load.
      const bgEl = card.querySelector(".channel-card-bg");
      if (bgEl) {
        bgEl.addEventListener("error", () => {
          bgEl.remove();
          card.style.background = gradientFor(name);
          if (!avatarUrl && !card.querySelector(".channel-letter")) {
            const d = document.createElement("div");
            d.className = "channel-letter";
            d.textContent = first;
            card.insertBefore(d, card.firstChild);
          }
        }, { once: true });
      }

      card.addEventListener("click", () => onChannelClick && onChannelClick(c));
      frag.appendChild(card);
    }
    grid.appendChild(frag);

    // Prefetch banner + avatar images in the background, throttled to a
    // few parallel fetches at a time. By the time the user scrolls past
    // the first viewport, the later cards' images are already in the
    // browser cache and decoded — no more pop-in. Runs on idle time so
    // it never blocks main-thread scrolling.
    _prefetchChannelArt(channels);
  };

  let _prefetchQueue = [];
  let _prefetchActive = 0;
  // 2 concurrent is enough — more than this stacks decode work on
  // the main thread and creates the very lag we're trying to prevent.
  const PREFETCH_MAX_CONCURRENT = 2;
  // Only prefetch enough cards to fill the first couple of viewports.
  // Past that, let `<img loading="lazy">` pull the rest on demand as
  // the user scrolls. Prefetching all 100+ banners up-front floods the
  // decoder and makes scroll janky for the first 10-20 seconds.
  const PREFETCH_LIMIT = 40;

  function _prefetchChannelArt(channels) {
    // explicitly reset the module-level queue at the
    // top so a second renderChannelGrid call doesn't accidentally
    // append (safe no-op today — the assignment below replaces —
    // but defensive against any future refactor that changes the
    // assignment to a push).
    _prefetchQueue = [];
    const first = channels.slice(0, PREFETCH_LIMIT);
    const urls = [];
    for (const c of first) {
      if (c.banner_url) urls.push(c.banner_url);
    }
    for (const c of first) {
      if (c.avatar_url && c.avatar_url !== c.banner_url) urls.push(c.avatar_url);
    }
    _prefetchQueue = urls;
    for (let i = 0; i < PREFETCH_MAX_CONCURRENT; i++) _pumpPrefetch();
  }

  function _pumpPrefetch() {
    if (!_prefetchQueue.length) return;
    if (_prefetchActive >= PREFETCH_MAX_CONCURRENT) return;
    const url = _prefetchQueue.shift();
    if (!url) return;
    _prefetchActive++;
    const img = new Image();
    img.decoding = "async";
    img.fetchPriority = "low";
    const done = () => {
      _prefetchActive--;
      // Schedule the next pump inside an idle callback so decodes
      // only run when the main thread isn't busy rendering scrolls.
      const next = () => _pumpPrefetch();
      if (typeof requestIdleCallback === "function") {
        requestIdleCallback(next, { timeout: 500 });
      } else {
        setTimeout(next, 100);
      }
    };
    img.addEventListener("load", done, { once: true });
    img.addEventListener("error", done, { once: true });
    img.src = url;
  }

  /** Render the Videos grid (inside a channel). */
  function _buildVideoCard(v, onVideoClick) {
    const card = document.createElement("div");
    card.className = "video-card";
    // Flag visually + via data attr so CSS can fade the card, add a
    // strikethrough on the title, and overlay a corner badge.
    if (v.removed_from_yt) {
      card.classList.add("video-card-removed");
      card.dataset.removedFromYt = "1";
      card.title = "No longer on YouTube — uploader removed / privated / unlisted this video. Your local file is preserved.";
    }
    card.dataset.filepath = v.filepath || "";
    card.dataset.videoId = v.video_id || "";
    card.dataset.title = v.title || "";
    // Channel name is needed by the right-click "Refresh metadata" menu
    // so the backend can locate the channel's aggregated Metadata.jsonl.
    card.dataset.channel = v.channel || "";
    // Use a real <img> tag for thumbnails rather than CSS
    // `background: url(...)` — the image tag is far more forgiving of
    // http://127.0.0.1 URLs through pywebview's webview, and failed loads
    // trigger a clean `onerror` swap to the gradient placeholder.
    const hasThumb = !!v.thumbnail_url;
    const imgTag = hasThumb
      ? `<img class="video-thumb-img" src="${v.thumbnail_url}" alt=""
              loading="lazy" decoding="async" />`
      : "";
    const removedBadge = v.removed_from_yt
      ? '<span class="video-removed-badge" title="No longer on YouTube">✗ Removed from YT</span>'
      : "";
    card.innerHTML = `
      <div class="video-thumb" style="${hasThumb ? '' : `background: ${gradientFor(v.title)};`}">
        ${imgTag}
        ${hasThumb ? '' : '<span>&#9654;</span>'}
        ${removedBadge}
        <span class="video-duration-badge"></span>
      </div>
      <div class="video-card-body">
        <div class="video-card-title"></div>
        <div class="video-card-channel"></div>
        <div class="video-card-meta"></div>
      </div>
    `;
    // Swap to gradient placeholder if the thumb fails to load
    const imgEl = card.querySelector(".video-thumb-img");
    if (imgEl) {
      imgEl.addEventListener("error", () => {
        const tf = imgEl.parentElement;
        if (tf) {
          tf.style.background = gradientFor(v.title);
          imgEl.remove();
          const ph = document.createElement("span");
          ph.innerHTML = "&#9654;";
          tf.insertBefore(ph, tf.firstChild);
        }
      }, { once: true });
    }
    card.querySelector(".video-duration-badge").textContent = v.duration || "";
    card.querySelector(".video-card-title").textContent = v.title || "";
    // Channel line — opt-in via v.show_channel so contexts like the
    // Recent grid (many channels mixed together) get it, while the
    // Browse video grid (already scoped to one channel) doesn't show
    // a redundant channel name on every card.
    const chEl = card.querySelector(".video-card-channel");
    if (chEl) {
      if (v.show_channel && v.channel) {
        chEl.textContent = v.channel;
        chEl.style.display = "";
      } else {
        chEl.style.display = "none";
      }
    }
    const metaParts = [];
    // Pretty-print the upload date as "Nov 15, 2025" instead of the raw
    // "2025-11-15" that the backend emits. Accepts any ISO-ish YYYY-MM-DD
    // or date-time string; unparseable values fall back to verbatim.
    if (v.uploaded) metaParts.push(_fmtCardDate(v.uploaded));
    if (v.size_bytes) metaParts.push(_fmtBytes(v.size_bytes));
    if (v.views) metaParts.push(v.views + " views");
    if (v.tx_status === "transcribed") metaParts.push("transcribed");
    card.querySelector(".video-card-meta").textContent = metaParts.join(" · ");
    card.addEventListener("click", () => onVideoClick && onVideoClick(v));

    // (Hover-enlarge preview removed in v49.7 — intrusive on a dense
    // grid; covered the adjacent cards and interrupted normal browsing
    // flow without adding real information.)

    card.addEventListener("dblclick", (e) => {
      e.preventDefault();
      e.stopPropagation();
      if (v.filepath && window.pywebview?.api?.browse_open_video) {
        window.pywebview.api.browse_open_video(v.filepath);
      }
    });
    return card;
  }
  // Public alias so other renderers (e.g. renderRecentGrid) can reuse the
  // same card builder without duplicating markup + hover-preview logic.
  window._buildVideoCard = _buildVideoCard;

  function _yearOf(v) {
    // Prefer upload_ts (ms epoch from added_ts*1000); fall back to r.year
    if (typeof v.upload_ts === "number" && v.upload_ts > 0) {
      return new Date(v.upload_ts).getFullYear();
    }
    if (v.year) return Number(v.year);
    // Last resort: try to parse 'uploaded' (e.g. "2024-05-20")
    const m = (v.uploaded || "").match(/\b(19|20)\d{2}\b/);
    return m ? Number(m[0]) : null;
  }

  window.renderVideoGrid = function (videos, onVideoClick, opts) {
    const grid = document.getElementById("video-grid");
    if (!grid) return;
    grid.innerHTML = "";
    const groupByYear = !!(opts && opts.groupByYear);
    // `groupByMonth` only makes sense nested inside `groupByYear`;
    // without a year context, "March" alone is ambiguous across years.
    // Automatically treat month-grouping as implying year-grouping.
    const groupByMonth = !!(opts && opts.groupByMonth);
    const useYear = groupByYear || groupByMonth;

    // Lazy-load: on channels with 500+ videos, rendering all thumbnails at
    // once causes jank + high memory. Match YTArchiver.py:28108 (fires at
    // 85% scroll) by batching 60 cards per append cycle with an
    // IntersectionObserver sentinel.
    // Mirrors OLD `_grid_check_load_more` / `_grid_build_cards(reset=False)`.
    const BATCH = 60;
    const LAZY_THRESHOLD = 120; // only lazy-load when enough videos to matter
    const useLazy = !useYear && videos.length > LAZY_THRESHOLD;

    if (useLazy) {
      grid.classList.remove("video-grid-grouped");
      let cursor = 0;
      const appendBatch = () => {
        const end = Math.min(cursor + BATCH, videos.length);
        const frag = document.createDocumentFragment();
        for (let i = cursor; i < end; i++) {
          frag.appendChild(_buildVideoCard(videos[i], onVideoClick));
        }
        // Remove old sentinel if present, append batch, add new sentinel
        const oldSentinel = grid.querySelector(".video-grid-sentinel");
        oldSentinel?.remove();
        grid.appendChild(frag);
        cursor = end;
        if (cursor < videos.length) {
          const sentinel = document.createElement("div");
          sentinel.className = "video-grid-sentinel";
          sentinel.textContent = `… ${videos.length - cursor} more, scroll to load`;
          grid.appendChild(sentinel);
          _observeGridSentinel(sentinel, appendBatch);
        }
      };
      appendBatch();
      return;
    }

    if (!useYear) {
      grid.classList.remove("video-grid-grouped");
      const frag = document.createDocumentFragment();
      for (const v of videos) frag.appendChild(_buildVideoCard(v, onVideoClick));
      grid.appendChild(frag);
      return;
    }

    // Group in the order they arrive (caller already sorted).
    const buckets = new Map(); // year -> array of videos
    const order = []; // year order of first occurrence
    const unknown = [];
    for (const v of videos) {
      const y = _yearOf(v);
      if (y == null) { unknown.push(v); continue; }
      if (!buckets.has(y)) { buckets.set(y, []); order.push(y); }
      buckets.get(y).push(v);
    }
    if (unknown.length) { buckets.set("?", unknown); order.push("?"); }

    // Render: grid becomes a single column of <section>s each containing its
    // own internal grid of cards. CSS handles the visual gaps.
    grid.classList.add("video-grid-grouped");
    const frag = document.createDocumentFragment();
    for (const y of order) {
      const vids = buckets.get(y);
      const section = document.createElement("section");
      section.className = "video-grid-year-section";
      section.dataset.year = String(y);
      const head = document.createElement("header");
      head.className = "video-grid-year-head";
      const arrow = document.createElement("span");
      arrow.className = "vgy-arrow";
      arrow.textContent = "▾";
      const label = document.createElement("span");
      label.className = "vgy-label";
      label.textContent = (y === "?" ? "Unknown" : String(y));
      const count = document.createElement("span");
      count.className = "vgy-count";
      count.textContent = `(${vids.length})`;
      head.append(arrow, label, count);
      section.appendChild(head);

      const inner = document.createElement("div");
      inner.className = "video-grid-year-inner";
      if (groupByMonth && y !== "?") {
        // Second-level grouping: month within year. Month order
        // follows the caller's sort — videos arrive already sorted
        // so the first month we see is the newest (or oldest).
        const mBuckets = new Map();
        const mOrder = [];
        const mUnknown = [];
        for (const v of vids) {
          const m = _monthOf(v);
          if (m == null) { mUnknown.push(v); continue; }
          if (!mBuckets.has(m)) { mBuckets.set(m, []); mOrder.push(m); }
          mBuckets.get(m).push(v);
        }
        if (mUnknown.length) { mBuckets.set("?", mUnknown); mOrder.push("?"); }
        for (const m of mOrder) {
          const mVids = mBuckets.get(m);
          const mSec = document.createElement("section");
          mSec.className = "video-grid-month-section";
          mSec.dataset.month = String(m);
          const mHead = document.createElement("header");
          mHead.className = "video-grid-month-head";
          const mArrow = document.createElement("span");
          mArrow.className = "vgy-arrow";
          mArrow.textContent = "▾";
          const mLabel = document.createElement("span");
          mLabel.className = "vgy-label";
          mLabel.textContent = (m === "?" ? "Unknown" : _MONTH_NAMES[m] || `Month ${m}`);
          const mCount = document.createElement("span");
          mCount.className = "vgy-count";
          mCount.textContent = `(${mVids.length})`;
          mHead.append(mArrow, mLabel, mCount);
          mSec.appendChild(mHead);
          const mInner = document.createElement("div");
          mInner.className = "video-grid-year-inner";
          for (const v of mVids) mInner.appendChild(_buildVideoCard(v, onVideoClick));
          mSec.appendChild(mInner);
          mHead.addEventListener("click", () => {
            const collapsed = mSec.classList.toggle("collapsed");
            mArrow.textContent = collapsed ? "▸" : "▾";
          });
          inner.appendChild(mSec);
        }
      } else {
        for (const v of vids) inner.appendChild(_buildVideoCard(v, onVideoClick));
      }
      section.appendChild(inner);

      head.addEventListener("click", () => {
        const collapsed = section.classList.toggle("collapsed");
        arrow.textContent = collapsed ? "▸" : "▾"; // ▸ : ▾
      });

      frag.appendChild(section);
    }
    grid.appendChild(frag);
  };

  const _MONTH_NAMES = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December",
  };

  function _monthOf(v) {
    // Prefer the explicit `month` field (set from DB row's year/month
    // columns, which come from the yyyy/mm folder structure on disk);
    // fall back to parsing from upload_ts epoch if month is missing.
    if (v && typeof v.month === "number" && v.month >= 1 && v.month <= 12) {
      return v.month;
    }
    if (v && v.upload_ts) {
      try {
        const d = new Date(Number(v.upload_ts));
        if (!Number.isNaN(d.getTime())) return d.getMonth() + 1;
      } catch {}
    }
    return null;
  }

  function _fmtBytes(b) {
    if (!b) return "";
    const units = ["B","KB","MB","GB","TB"];
    let i = 0, v = b;
    while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
    return `${v.toFixed(i >= 2 ? 1 : 0)} ${units[i]}`;
  }

  // Pretty-print a date string (accepts YYYY-MM-DD or any Date-parseable
  // form) as e.g. "Nov 15, 2025". Returns the original string unchanged
  // if it can't be parsed so existing displays never regress to "NaN".
  const _MON_ABBR = ["Jan","Feb","Mar","Apr","May","Jun",
                     "Jul","Aug","Sep","Oct","Nov","Dec"];
  function _fmtCardDate(s) {
    if (!s) return "";
    // Prefer a strict YYYY-MM-DD match so we don't trip on Date's
    // timezone-shifted parsing of bare ISO dates (which can roll
    // "2025-11-15" into Nov 14 on negative-UTC offsets).
    const m = String(s).match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (m) {
      const y = Number(m[1]);
      const mo = Number(m[2]);
      const d = Number(m[3]);
      if (mo >= 1 && mo <= 12 && d >= 1 && d <= 31) {
        return `${_MON_ABBR[mo - 1]} ${d}, ${y}`;
      }
    }
    // Fallback — hand off to Date() for other formats (ISO with time, etc.)
    const dt = new Date(s);
    if (!isNaN(dt.getTime())) {
      return `${_MON_ABBR[dt.getMonth()]} ${dt.getDate()}, ${dt.getFullYear()}`;
    }
    return String(s);
  }

  // Shared IntersectionObserver so big channels don't make thousands of them.
  let _gridIO = null;
  const _gridSentinelCallbacks = new WeakMap();
  function _observeGridSentinel(sentinel, cb) {
    if (!_gridIO) {
      _gridIO = new IntersectionObserver((entries) => {
        for (const e of entries) {
          if (e.isIntersecting) {
            const fn = _gridSentinelCallbacks.get(e.target);
            if (fn) {
              _gridIO.unobserve(e.target);
              _gridSentinelCallbacks.delete(e.target);
              fn();
            }
          }
        }
      }, { rootMargin: "400px" });
    }
    _gridSentinelCallbacks.set(sentinel, cb);
    _gridIO.observe(sentinel);
  }
})();
