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

  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }
  function nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
  }

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

  function _buildChannelCard(c, index) {
    const name = c.folder || c.name || "";
    const vids = c.n_vids || c.video_count || "\u2014";
    const size = c.size || "";
    const first = (name[0] || "?").toUpperCase();

    const bannerUrl = c.banner_url || "";
    const avatarUrl = c.avatar_url || "";
    // Banner priority: explicit banner > avatar zoomed-to-fill > gradient.
    const bannerSrc = bannerUrl || avatarUrl || "";

    const card = document.createElement("div");
    card.className = "channel-card";
    card.dataset.channelIndex = String(index);
    card.setAttribute("role", "button");
    card.tabIndex = 0;
    card.setAttribute("aria-label", `Open channel ${name || "Untitled"}`);
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

    // Banner + avatar URLs come from per-channel metadata (potentially
    // user-influenced via custom configs); interpolating them into
    // innerHTML would XSS via " onerror=... or " /><script>. Build the
    // <img> elements via createElement + .src so the URL is treated as
    // a URL, not as HTML to parse.
    const letterHtml = (!bannerSrc && !avatarUrl)
      ? `<div class="channel-letter">${escapeHtml(first)}</div>`
      : "";

    card.innerHTML = `
      ${letterHtml}
      <div class="channel-card-overlay">
        <div class="channel-card-name"></div>
        <div class="channel-card-meta"></div>
      </div>
    `;

    if (bannerSrc) {
      const bgEl = document.createElement("img");
      bgEl.className = "channel-card-bg";
      bgEl.src = bannerSrc;
      bgEl.loading = "lazy";
      bgEl.decoding = "async";
      bgEl.alt = "";
      card.insertBefore(bgEl, card.firstChild);
    }
    if (avatarUrl) {
      const avEl = document.createElement("img");
      avEl.className = "channel-avatar";
      avEl.src = avatarUrl;
      avEl.loading = "lazy";
      avEl.decoding = "async";
      avEl.alt = "";
      card.appendChild(avEl);
    }
    card.querySelector(".channel-card-name").textContent = name;
    card.querySelector(".channel-card-meta").textContent =
      `${vids}${vids && vids !== "\u2014" ? " videos" : ""}${size ? " \u00b7 " + size : ""}`;

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
    return card;
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
    _unobserveGridSentinel(grid);
    grid.innerHTML = "";
    channels = Array.isArray(channels) ? channels : [];
    grid._channelItems = channels;
    grid._onChannelClick = onChannelClick;
    if (!grid._channelDelegated) {
      grid._channelDelegated = true;
      grid.addEventListener("click", (e) => {
        const card = e.target.closest(".channel-card");
        if (!card || !grid.contains(card)) return;
        const idx = Number(card.dataset.channelIndex);
        const ch = Number.isFinite(idx) ? grid._channelItems?.[idx] : null;
        if (ch && grid._onChannelClick) grid._onChannelClick(ch);
      });
      grid.addEventListener("keydown", (e) => {
        if (e.key !== "Enter" && e.key !== " ") return;
        const card = e.target.closest(".channel-card");
        if (!card || !grid.contains(card)) return;
        e.preventDefault();
        card.click();
      });
    }
    if (!channels.length) {
      grid.innerHTML = '<div class="browse-empty">No channels yet. Add one on the <b>Subs</b> tab to start archiving.</div>';
      return;
    }

    const CHANNEL_BATCH = 60;
    const CHANNEL_LAZY_THRESHOLD = 120;
    const useLazy = channels.length > CHANNEL_LAZY_THRESHOLD;
    let cursor = 0;
    const appendBatch = () => {
      const oldSentinel = Array.from(grid.children)
        .find(el => el.classList?.contains("video-grid-sentinel"));
      _clearGridSentinel(oldSentinel);
      const end = useLazy ? Math.min(cursor + CHANNEL_BATCH, channels.length)
                          : channels.length;
      const frag = document.createDocumentFragment();
      for (let i = cursor; i < end; i++) {
        frag.appendChild(_buildChannelCard(channels[i], i));
      }
      grid.appendChild(frag);
      cursor = end;
      if (useLazy && cursor < channels.length) {
        const sentinel = document.createElement("div");
        sentinel.className = "video-grid-sentinel";
        sentinel.textContent = `... ${channels.length - cursor} more, scroll to load`;
        grid.appendChild(sentinel);
        _observeGridSentinel(sentinel, appendBatch);
      }
    };
    appendBatch();

    // Prefetch banner + avatar images in the background, throttled to a
    // few parallel fetches at a time. By the time the user scrolls past
    // the first viewport, the later cards' images are already in the
    // browser cache and decoded. Runs on idle time so it never blocks
    // main-thread scrolling.
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
    // Validate URL scheme before prefetching — `new Image().src =
    // "javascript:..."` is a no-op but file:// or chrome-extension://
    // URLs sneak in from a poisoned backend payload could probe
    // local resources from the webview context (audit:
    // browseGrids.js:151). Allow only http/https + file:// for the
    // local thumbnail-server path.
    const _safeScheme = (u) => {
      const s = String(u || "").trim();
      if (!s) return false;
      const lo = s.toLowerCase();
      return lo.startsWith("http://")
          || lo.startsWith("https://")
          || lo.startsWith("file://")
          || lo.startsWith("/")              // server-relative
          || lo.startsWith("data:image/");   // inline base64 thumbs
    };
    const urls = [];
    for (const c of first) {
      if (c.banner_url && _safeScheme(c.banner_url)) urls.push(c.banner_url);
    }
    for (const c of first) {
      if (c.avatar_url && c.avatar_url !== c.banner_url
          && _safeScheme(c.avatar_url)) urls.push(c.avatar_url);
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
    card.setAttribute("role", "button");
    card.tabIndex = 0;
    const labelBits = [v.title || "Untitled video"];
    if (v.channel) labelBits.push(v.channel);
    if (v.uploaded) labelBits.push(v.uploaded);
    card.setAttribute("aria-label", labelBits.join(", "));
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
    // The img element is built via createElement (not innerHTML) so the
    // thumbnail_url can't be parsed as HTML — closes XSS via crafted urls.
    const hasThumb = !!v.thumbnail_url;
    const removedBadge = v.removed_from_yt
      ? '<span class="video-removed-badge" title="No longer on YouTube">✗ Removed from YT</span>'
      : "";
    card.innerHTML = `
      <div class="video-thumb" style="${hasThumb ? '' : `background: ${gradientFor(v.title)};`}">
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
    if (hasThumb) {
      const thumbWrap = card.querySelector(".video-thumb");
      if (thumbWrap) {
        const img = document.createElement("img");
        img.className = "video-thumb-img";
        img.src = v.thumbnail_url;
        img.alt = "";
        img.loading = "lazy";
        img.decoding = "async";
        thumbWrap.insertBefore(img, thumbWrap.firstChild);
      }
    }
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
    // Defer the single-click open by 220ms so a double-click can
    // cancel it before the Watch view opens — otherwise both
    // handlers fire and the external player AND in-app player
    // race each other (audit: browseGrids.js H151).
    let _clickTimer = null;
    card.addEventListener("click", () => {
      if (_clickTimer) clearTimeout(_clickTimer);
      _clickTimer = setTimeout(() => {
        _clickTimer = null;
        if (onVideoClick) onVideoClick(v);
      }, 220);
    });
    card.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        card.click();
      }
    });

    // (Hover-enlarge preview removed in v49.7 — intrusive on a dense
    // grid; covered the adjacent cards and interrupted normal browsing
    // flow without adding real information.)

    card.addEventListener("dblclick", (e) => {
      e.preventDefault();
      e.stopPropagation();
      if (_clickTimer) { clearTimeout(_clickTimer); _clickTimer = null; }
      if (v.filepath && nativeBridgeUp()) {
        bridgeCall("browse_open_video", v.filepath);
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

  function _clearGridSentinel(sentinel) {
    if (!sentinel) return;
    try { _gridIO?.unobserve?.(sentinel); } catch {}
    try { _gridSentinelCallbacks.delete(sentinel); } catch {}
    try { sentinel.remove(); } catch {}
  }

  function _renderVideoBatch(container, videos, onVideoClick, batchSize,
                             loadImmediately) {
    if (!videos.length) return;
    let cursor = 0;
    const appendBatch = () => {
      const oldSentinel = Array.from(container.children)
        .find(el => el.classList?.contains("video-grid-sentinel"));
      _clearGridSentinel(oldSentinel);

      const end = Math.min(cursor + batchSize, videos.length);
      const frag = document.createDocumentFragment();
      for (let i = cursor; i < end; i++) {
        frag.appendChild(_buildVideoCard(videos[i], onVideoClick));
      }
      container.appendChild(frag);
      cursor = end;
      if (cursor < videos.length) {
        const sentinel = document.createElement("div");
        sentinel.className = "video-grid-sentinel";
        sentinel.textContent = `... ${videos.length - cursor} more, scroll to load`;
        container.appendChild(sentinel);
        _observeGridSentinel(sentinel, appendBatch);
      }
    };

    if (loadImmediately) appendBatch();
    else {
      const sentinel = document.createElement("div");
      sentinel.className = "video-grid-sentinel";
      sentinel.textContent = `... ${videos.length} videos, scroll to load`;
      container.appendChild(sentinel);
      _observeGridSentinel(sentinel, appendBatch);
    }
  }

  window.renderVideoGrid = function (videos, onVideoClick, opts) {
    const grid = document.getElementById("video-grid");
    if (!grid) return;
    _unobserveGridSentinel(grid);
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
        // Remove old sentinel if present, append batch, add new
        // sentinel. Explicitly unobserve before remove so the shared
        // IntersectionObserver doesn't hold a dead-DOM reference
        // until GC (audit: browseGrids.js H162, H163).
        _unobserveGridSentinel(grid);
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
    let firstGroup = true;
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
        let firstMonth = firstGroup;
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
          _renderVideoBatch(mInner, mVids, onVideoClick, BATCH, firstMonth);
          firstMonth = false;
          mSec.appendChild(mInner);
          mHead.addEventListener("click", () => {
            const collapsed = mSec.classList.toggle("collapsed");
            mArrow.textContent = collapsed ? "▸" : "▾";
          });
          inner.appendChild(mSec);
        }
      } else {
        _renderVideoBatch(inner, vids, onVideoClick, BATCH, firstGroup);
      }
      section.appendChild(inner);
      firstGroup = false;

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
  function _unobserveGridSentinel(grid) {
    const oldSentinels = Array.from(
      grid?.querySelectorAll?.(".video-grid-sentinel") || []);
    for (const oldSentinel of oldSentinels) {
      _clearGridSentinel(oldSentinel);
    }
  }
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
