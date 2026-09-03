/* ═══════════════════════════════════════════════════════════════════════
   metadataTab.js — Health → Library channel-information section

   Owns the per-channel metadata refresh workflow:

     • Channel table rendering with sort (Views / Likes / Comments /
       Thumbs / Backfill / Still-on-YT / IDs / last-refresh column)
     • Per-row right-click context menus (refresh just this channel,
       skip, abort, etc.)
     • Bulk-refresh action buttons (Views, Comments, Backfill, Thumbs)
     • Reload + force-recheck for the "Still on YT" column

   Publishes:
     window.initMetadataTab
     window._initMetadataTab     (legacy alias kept for back-compat)

   Reads:
     window.pywebview.api (many endpoints)
     window.askConfirm / askDanger / askChoice / askTextInput / showContextMenu
     window._showToast / refreshSubsTable
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

  const askConfirm = window.askConfirm;
  const askDanger = window.askDanger;
  const askQuestion = window.askQuestion;
  const askChoice = window.askChoice;
  const askTextInput = window.askTextInput;
  const showContextMenu = window.showContextMenu || (() => {});

  function initMetadataTab() {
    // Re-init guard — multiple calls would stack duplicate global
    // listeners (mousedown/keydown/resize/scroll capture), each one
    // triggering on every event. After several re-inits the page
    // gets noticeably slower on scroll.
    if (window._metadataTabInited) return;
    window._metadataTabInited = true;
    const tbody = document.getElementById("metadata-tbody");
    const table = document.getElementById("metadata-table");
    const bAllViews = document.getElementById("btn-md-refresh-all-views");
    const bAllComments = document.getElementById("btn-md-refresh-all-comments");
    const bAllBackfill = document.getElementById("btn-md-backfill-all-ids");
    const bAllThumbs = document.getElementById("btn-md-refetch-all-thumbs");
    const bReload = document.getElementById("btn-md-reload");
    if (!tbody || !table) return;

    // Current dataset + sort state. Sort state persists across reloads.
    let _rows = [];
    let _sortKey = "views";   // matches data-sort-active in HTML
    let _sortDir = "asc";     // oldest-first by default
    // Whether the bulk thumbnail walk has returned at least once for the
    // currently-rendered rows. Used to swap the Thumbnails column between
    // "loading…" (spinner) and the real percentage. Cleared on each
    // refresh + force-recheck so the indicator shows up again.
    let _thumbsLoaded = false;
    let _thumbStatusInFlight = false;
    let _thumbStatusError = "";
    let _catalogLoadError = "";
    let _lastRowsLoadAt = 0;
    let _loadGen = 0;
    let _pendingRefreshTimer = null;

    // Instant-paint cache (perceived performance): remember the channel
    // rows across sessions so opening this tab paints last-known data
    // immediately instead of a full-table "Loading channels…" spinner
    // while the (sometimes multi-second) index-DB query runs.
    const _META_CACHE_KEY = "ytarchiver_meta_rows";
    function _loadCachedMeta() {
      try {
        const raw = localStorage.getItem(_META_CACHE_KEY);
        const arr = raw ? JSON.parse(raw) : null;
        if (Array.isArray(arr) && arr.length) return arr;
      } catch (e) { /* unavailable / corrupt */ }
      return null;
    }
    function _saveCachedMeta(rows) {
      try { localStorage.setItem(_META_CACHE_KEY, JSON.stringify(rows || [])); }
      catch (e) { /* quota / unavailable */ }
    }
    function _rowsHaveThumbStatus(rows) {
      return (rows || []).some((r) =>
        Object.prototype.hasOwnProperty.call(r || {}, "thumb_total")
        || Object.prototype.hasOwnProperty.call(r || {}, "thumb_with")
        || Object.prototype.hasOwnProperty.call(r || {}, "thumb_missing"));
    }
    function _mergeThumbStatus(thRes) {
      if (!thRes?.ok || !thRes.rows || typeof thRes.rows !== "object") {
        throw new Error(thRes?.error || "Thumbnail coverage is unavailable.");
      }
      const thMap = thRes?.rows || {};
      for (const r of _rows) {
        const key = (r.name || r.folder || "").toLowerCase();
        const t = thMap[key];
        if (t) {
          r.thumb_total = t.total || 0;
          r.thumb_with = t.with_thumb || 0;
          r.thumb_missing = t.missing || 0;
        }
      }
      _thumbStatusError = "";
      _thumbsLoaded = true;
      _saveCachedMeta(_rows);
    }
    const _scheduleMetadataRefresh = () => {
      if (_pendingRefreshTimer) clearTimeout(_pendingRefreshTimer);
      _pendingRefreshTimer = setTimeout(() => {
        _pendingRefreshTimer = null;
        try { window._refreshMetadataTab?.({ force: true }); } catch (e) {}
      }, 2000);
    };
    // Cutoff for the "Still on YT" column. bulk_refresh_views_likes only
    // started populating `removed_from_yt_ts` after 2026-05-13. Channels
    // whose `last_views_refresh_ts` predates that have no real signal —
    // the column shows "—" instead of a misleading "✓ 100%".
    const REMOVED_DETECTION_SINCE = 1778630400; // 2026-05-13 00:00 UTC

    // Format a timestamp relative to now ("2h ago", "3d ago", "never").
    // "Never" and >90d get a color class so they're easy to spot.
    const fmtRel = (ts) => {
      if (!ts || ts <= 0) return { text: "never", cls: "md-ts-never" };
      const now = Date.now() / 1000;
      const delta = Math.max(0, now - ts);
      const mins = Math.floor(delta / 60);
      const hrs = Math.floor(delta / 3600);
      const days = Math.floor(delta / 86400);
      let text;
      if (mins < 1) text = "just now";
      else if (mins < 60) text = `${mins}m ago`;
      else if (hrs < 24) text = `${hrs}h ago`;
      else if (days < 365) text = `${days}d ago`;
      else text = `${Math.floor(days / 365)}y ago`;
      const cls = days >= 90 ? "md-ts-old"
                : days >= 30 ? "" : "md-ts-fresh";
      return { text, cls };
    };

    // Must escape ' as &#39; — the data-identity attribute is wrapped in
    // single quotes (because its value is JSON, which contains "), so an
    // unescaped apostrophe in a folder name (e.g. "Don't Tell Comedy")
    // would break out of the attribute and inject markup.
    const escapeHtml = window.YT?.util?.escapeHtml || window._escapeHtml
      || ((s) => String(s ?? "").replace(/[&<>"']/g, c =>
          ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])));

    const fmtPct = (a, b, digits = 1) => {
      if (b <= 0) return "0%";
      if (a >= b) return "100%";
      const factor = 10 ** digits;
      const floored = Math.floor(((a / b) * 100) * factor) / factor;
      return `${floored.toFixed(digits)}%`;
    };

    // This page reports coverage from the catalog (IDs, transcripts, and
    // YouTube availability), so its visible video count must use that same
    // denominator. The disk-summary cache can legitimately be absent while
    // the catalog is populated, which previously made those channels show
    // "0 Videos" beside non-zero coverage. Keep a fallback for older cached
    // frontend rows that predate `id_total`.
    const trackedVideoCount = (r) => {
      if (r?.id_total !== null && r?.id_total !== undefined) {
        return Number(r.id_total) || 0;
      }
      return Number(r?.video_count) || 0;
    };

    const sortRows = (rows) => {
      const mult = _sortDir === "asc" ? 1 : -1;
      const kfn = {
        name: (r) => (r.name || "").toLowerCase(),
        videos: (r) => trackedVideoCount(r),
        // IDs column sorts by ratio of missing ids → worst-first
        // (highest missing fraction) when descending. Channels with
        // nothing on disk (id_total=0) sort as ratio=0 so they don't
        // dominate the "worst" position.
        ids: (r) => {
          const t = r.id_total || 0;
          const m = r.id_missing || 0;
          return t > 0 ? (m / t) : 0;
        },
        // same "missing ratio" pattern for the thumbs col.
        thumbs: (r) => {
          const t = r.thumb_total || 0;
          const w = r.thumb_with || 0;
          return t > 0 ? ((t - w) / t) : 0;
        },
        // Transcribed — sort by the not-transcribed ratio so the
        // least-covered channels float to the top when descending,
        // matching the ids/thumbs "worst-first" behavior.
        transcribed: (r) => {
          const t = r.tx_total || 0;
          const w = r.tx_transcribed || 0;
          return t > 0 ? ((t - w) / t) : 0;
        },
        // "Still on YT" column — sort by the removed-from-YT ratio so
        // channels hemorrhaging videos float to the top when sorting
        // descending. Channels with no on-disk videos sort as 0.
        onyt: (r) => {
          const t = r.id_total || 0;
          const rm = r.removed_from_yt || 0;
          return t > 0 ? (rm / t) : 0;
        },
        views: (r) => r.last_views_refresh_ts || 0,
        comments: (r) => r.last_comments_refresh_ts || 0,
      }[_sortKey] || ((r) => 0);
      const out = rows.slice().sort((a, b) => {
        const va = kfn(a), vb = kfn(b);
        if (va < vb) return -1 * mult;
        if (va > vb) return 1 * mult;
        // stable tiebreak on name
        const na = (a.name || "").toLowerCase();
        const nb = (b.name || "").toLowerCase();
        if (na < nb) return -1;
        if (na > nb) return 1;
        return 0;
      });
      return out;
    };

    // Aggregate archive-wide stats across every channel and paint the
    // 5-tile totals strip above the table. Hidden until rows arrive.
    const _renderTotals = () => {
      const totalsEl = document.getElementById("metadata-totals");
      if (!totalsEl) return;
      if (!_rows.length) {
        totalsEl.hidden = true;
        return;
      }
      let nChannels = _rows.length;
      let nVideos = 0;
      let idTot = 0, idWith = 0;
      let thTot = 0, thWith = 0;
      let txTot = 0, txWith = 0;
      // For Still-on-YT: only aggregate channels that have actually been
      // bulk-checked since the removed-from-YT detection shipped. Sum-
      // ming the others would dilute the percentage with channels whose
      // `removed_from_yt` is 0 only because nobody ever checked.
      let onTotChecked = 0, onRemovedChecked = 0;
      let onChannelsChecked = 0;
      for (const r of _rows) {
        nVideos += trackedVideoCount(r);
        idTot += (r.id_total || 0);
        idWith += (r.id_with_id || 0);
        thTot += (r.thumb_total || 0);
        thWith += (r.thumb_with || 0);
        txTot += (r.tx_total || 0);
        txWith += (r.tx_transcribed || 0);
        const _vts = r.last_views_refresh_ts || 0;
        if (_vts >= REMOVED_DETECTION_SINCE) {
          onTotChecked += (r.id_total || 0);
          onRemovedChecked += (r.removed_from_yt || 0);
          onChannelsChecked++;
        }
      }
      const fmt = (n) => n.toLocaleString();
      const pct = (a, b) => b > 0 ? ((a / b) * 100) : 0;
      // Color tier helper — mirrors the per-row column thresholds.
      const tier = (p) => p >= 90 ? "is-ok"
                       : p >= 50 ? "is-warn"
                       : "is-bad";

      // Tile renderer. Two-line layout: big primary value on top,
      // smaller "sub" value below. For percentage tiles, primary
      // is the percent and sub is the count fraction (fits even
      // in a narrow tile, unlike the previous one-line format
      // which clipped large six-digit counters mid-value.
      const setCard = (cardId, valId, primary, sub, klass) => {
        const card = document.getElementById(cardId);
        const val = document.getElementById(valId);
        if (val) {
          if (sub) {
            val.innerHTML =
              `<span class="md-total-primary"></span>`
              + `<span class="md-total-sub"></span>`;
            val.querySelector(".md-total-primary").textContent = primary;
            val.querySelector(".md-total-sub").textContent = sub;
          } else {
            val.textContent = primary;
          }
        }
        if (card) {
          card.classList.remove("is-ok", "is-warn", "is-bad");
          if (klass) card.classList.add(klass);
        }
      };

      const _chanEl = document.getElementById("md-tot-channels");
      if (_chanEl) _chanEl.textContent = fmt(nChannels);
      const _vidEl = document.getElementById("md-tot-videos");
      if (_vidEl) _vidEl.textContent = fmt(nVideos);

      // IDs tile.
      if (idTot > 0) {
        const p = pct(idWith, idTot);
        setCard("md-tot-card-ids", "md-tot-ids",
                fmtPct(idWith, idTot),
                `${fmt(idWith)} / ${fmt(idTot)}`,
                tier(p));
      } else {
        setCard("md-tot-card-ids", "md-tot-ids", "—", "", null);
      }
      // Thumbs tile.
      if (thTot > 0) {
        const p = pct(thWith, thTot);
        setCard("md-tot-card-thumbs", "md-tot-thumbs",
                fmtPct(thWith, thTot),
                `${fmt(thWith)} / ${fmt(thTot)}`,
                tier(p));
      } else {
        setCard("md-tot-card-thumbs", "md-tot-thumbs",
                _thumbsLoaded ? "—" : "loading…",
                "",
                null);
      }
      // Transcribed tile — archive-wide transcription coverage
      // (videos with a real transcript / total videos). Same %-tile
      // shape + color tiers as IDs / Thumbs.
      if (txTot > 0) {
        const p = pct(txWith, txTot);
        setCard("md-tot-card-transcribed", "md-tot-transcribed",
                fmtPct(txWith, txTot),
                `${fmt(txWith)} / ${fmt(txTot)}`,
                tier(p));
      } else {
        setCard("md-tot-card-transcribed", "md-tot-transcribed", "—", "", null);
      }
      // Still-on-YT tile. ONLY counts channels actually checked since
      // detection shipped (see filter above). Three states:
      //   - No channel checked yet → "—" + "(run views/likes refresh)"
      //   - Some checked, some/no removals → live percentage of checked
      //   - All checked, 0 removed → real 100%
      const _onyt_card = document.getElementById("md-tot-card-onyt");
      if (_onyt_card) {
        // Override the static title so the tooltip reflects scope.
        if (onChannelsChecked === 0) {
          _onyt_card.title = "Availability has not been checked yet. Run "
            + "Refresh views/likes to check which videos are still on YouTube.";
        } else if (onChannelsChecked >= nChannels) {
          const live = Math.max(0, onTotChecked - onRemovedChecked);
          _onyt_card.title = `All ${nChannels} channel(s) have been checked. `
            + `${fmt(live)} of ${fmt(onTotChecked)} tracked video(s) are `
            + "still on YouTube"
            + (onRemovedChecked
              ? `; ${fmt(onRemovedChecked)} are removed, private, or unlisted.`
              : "; no removed videos were found.");
        } else {
          _onyt_card.title = `${onChannelsChecked} of ${nChannels} channel(s) have been `
            + `checked. The other ${nChannels - onChannelsChecked} have `
            + `not been checked yet. Refresh views/likes for those channels `
            + `to include them.`;
        }
      }
      if (onChannelsChecked === 0) {
        setCard("md-tot-card-onyt", "md-tot-onyt",
                "—",
                "not yet checked",
                null);
      } else if (onTotChecked > 0 && onRemovedChecked > 0) {
        const live = Math.max(0, onTotChecked - onRemovedChecked);
        const p = pct(live, onTotChecked);
        // Never display 100% unless every tracked item is actually present.
        setCard("md-tot-card-onyt", "md-tot-onyt",
                fmtPct(live, onTotChecked),
                `${fmt(live)} / ${fmt(onTotChecked)} · `
                  + `${fmt(onRemovedChecked)} removed`
                  + (onChannelsChecked < nChannels
                     ? ` (${onChannelsChecked}/${nChannels})` : ""),
                tier(p));
      } else if (onTotChecked > 0) {
        // We DID check ≥1 channel and found zero removed across the
        // checked subset — real 100% (not a placeholder this time).
        setCard("md-tot-card-onyt", "md-tot-onyt",
                "100%",
                `${fmt(onTotChecked)} / ${fmt(onTotChecked)}`
                  + (onChannelsChecked < nChannels
                     ? ` (${onChannelsChecked}/${nChannels})` : ""),
                "is-ok");
      } else {
        setCard("md-tot-card-onyt", "md-tot-onyt", "—", "", null);
      }
      totalsEl.hidden = false;
    };

    const render = () => {
      _renderTotals();
      // Update th sort indicators.
      table.querySelectorAll("thead th").forEach(th => {
        if (!th.dataset.sort) {
          th.removeAttribute("data-sort-active");
          th.removeAttribute("data-sort-dir");
          th.removeAttribute("aria-sort");
          return;
        }
        if (th.dataset.sort === _sortKey) {
          th.setAttribute("data-sort-active", "");
          th.setAttribute("data-sort-dir", _sortDir);
          th.setAttribute("aria-sort",
            _sortDir === "asc" ? "ascending" : "descending");
        } else {
          th.removeAttribute("data-sort-active");
          th.removeAttribute("data-sort-dir");
          th.setAttribute("aria-sort", "none");
        }
      });
      if (!_rows.length) {
        tbody.innerHTML = '<tr><td colspan="9" class="md-empty">No channels configured.</td></tr>';
        return;
      }
      const sorted = sortRows(_rows);
      tbody.innerHTML = sorted.map(r => {
        const v = fmtRel(r.last_views_refresh_ts);
        const c = fmtRel(r.last_comments_refresh_ts);
        const ident = JSON.stringify({ folder: r.folder, url: r.url });
        // Video IDs status: compute icon + percentage + color class.
        // DB-only count — reflects what bulk views/likes refresh can
        // actually match. Most channels hover at 98-99% (a handful of
        // removed/privated videos YouTube no longer lists); only
        // genuine trouble channels drop below the warning threshold.
        const idTotal = r.id_total || 0;
        const idWith = r.id_with_id || 0;
        const idMissing = r.id_missing || 0;
        const idTriedFailed = r.id_tried_failed || 0;
        const idNotYet = Math.max(0, idMissing - idTriedFailed);
        // Threshold below which we turn the cell orange + flag with
        // a warning triangle. 90% catches the real outliers (e.g. a
        // late-night TV channel with mass DMCA takedowns) without
        // flagging typical 164/166 or 453/458 archives.
        const ID_WARN_THRESHOLD = 0.90;
        let idHtml;
        if (idTotal === 0) {
          idHtml = '<span class="md-id-dim" title="No downloaded videos are available to check">&mdash;</span>';
        } else {
          const pct = idWith / idTotal;
          const pctStr = fmtPct(idWith, idTotal);
          // Rich tooltip: show the split between "tried but
          // couldn't resolve" (probably genuinely unrecoverable)
          // vs "not yet attempted" (run Fix IDs to pick these up).
          let detail = `${idWith.toLocaleString()} of ${idTotal.toLocaleString()} video(s) have YouTube IDs`;
          if (idMissing > 0) {
            detail += ` — ${idMissing.toLocaleString()} missing`;
            if (idTriedFailed > 0 && idNotYet > 0) {
              detail += ` (${idTriedFailed.toLocaleString()} tried unsuccessfully, ${idNotYet.toLocaleString()} not yet attempted — run Fix IDs)`;
            } else if (idTriedFailed > 0) {
              detail += ` (all tried — likely renamed or removed from YouTube)`;
            } else {
              detail += ` (run Fix IDs to find them)`;
            }
          }
          if (idMissing === 0) {
            // 100% — keep the checkmark so "all good" is instantly
            // readable without parsing digits.
            idHtml = `<span class="md-id-ok" title="${detail}">\u2713 100%</span>`;
          } else if (idWith === 0) {
            idHtml = `<span class="md-id-bad" title="${detail}">\u2717 0%</span>`;
          } else if (pct < ID_WARN_THRESHOLD) {
            idHtml = `<span class="md-id-warn" title="${detail}">\u26A0 ${pctStr}</span>`;
          } else {
            // 90-99% — acceptable range. No warning; just show
            // the percentage in the neutral row color.
            idHtml = `<span class="md-id-neutral" title="${detail}">${pctStr}</span>`;
          }
        }
        // Stash the "needs fix" state on the button so the click
        // handler's menu can emphasize Fix IDs when relevant (and so
        // the icon picks up a warning color without extra DOM).
        const needsFix = (idTotal > 0 && idMissing > 0);

        // thumbnail coverage column. Same color/style
        // grammar as the Video IDs column so users can scan both at
        // once. Right-click context menu (or the row menu) is where
        // "Refetch missing thumbnails" lives.
        const thTotal = r.thumb_total || 0;
        const thWith = r.thumb_with || 0;
        const thMissing = Math.max(0, thTotal - thWith);
        const TH_WARN_THRESHOLD = 0.90;
        let thumbHtml;
        if (_thumbStatusError) {
          thumbHtml = `<span class="md-id-dim" title="${escapeHtml(_thumbStatusError)}">unavailable</span>`;
        } else if (!_thumbsLoaded) {
          // Bulk thumb walk still in flight — show a spinner so the user
          // knows the column is loading, not actually 0%. Was "—" before,
          // which was indistinguishable from "no on-disk videos".
          thumbHtml = '<span class="md-thumb-loading" title="Counting saved thumbnails…"><span class="md-spinner" aria-hidden="true"></span>loading…</span>';
        } else if (thTotal === 0) {
          thumbHtml = '<span class="md-id-dim" title="No on-disk videos to check">&mdash;</span>';
        } else {
          const pct = thWith / thTotal;
          const pctStr = fmtPct(thWith, thTotal);
          let detail = `${thWith.toLocaleString()} of ${thTotal.toLocaleString()} video(s) have a saved thumbnail`;
          if (thMissing > 0) {
            detail += ` \u2014 ${thMissing.toLocaleString()} missing. Right-click channel \u2192 "Refetch missing thumbnails"`;
          }
          if (thMissing === 0) {
            thumbHtml = `<span class="md-id-ok" title="${detail}">\u2713 100%</span>`;
          } else if (thWith === 0) {
            thumbHtml = `<span class="md-id-bad" title="${detail}">\u2717 0%</span>`;
          } else if (pct < TH_WARN_THRESHOLD) {
            thumbHtml = `<span class="md-id-warn" title="${detail}">\u26A0 ${pctStr}</span>`;
          } else {
            thumbHtml = `<span class="md-id-neutral" title="${detail}">${pctStr}</span>`;
          }
        }

        // Transcribed coverage column. Unlike IDs/thumbs (where a gap is
        // a fixable defect) low transcription is often just work-in-
        // progress, so we color by the same 90/50 tiers as the Transcribed
        // card but skip the ⚠/✗ symbols to avoid implying every non-100%
        // channel needs action. 100% still gets the ✓ for at-a-glance scan.
        const txTotal = r.tx_total || 0;
        const txWith = r.tx_transcribed || 0;
        let txHtml;
        if (txTotal === 0) {
          txHtml = '<span class="md-id-dim" title="No on-disk videos to check">&mdash;</span>';
        } else {
          const pctVal = (txWith / txTotal) * 100;
          const pctStr = fmtPct(txWith, txTotal);
          const detail = `${txWith.toLocaleString()} of ${txTotal.toLocaleString()} video(s) have a transcript`;
          if (txWith >= txTotal) {
            txHtml = `<span class="md-id-ok" title="${detail}">✓ 100%</span>`;
          } else if (pctVal >= 90) {
            txHtml = `<span class="md-id-neutral" title="${detail}">${pctStr}</span>`;
          } else if (pctVal >= 50) {
            txHtml = `<span class="md-id-warn" title="${detail}">${pctStr}</span>`;
          } else {
            txHtml = `<span class="md-id-bad" title="${detail}">${pctStr}</span>`;
          }
        }

        // "Still on YT" column. `removed_from_yt` is populated by
        // bulk_refresh_views_likes \u2014 files whose video_id disappeared
        // from YouTube's flat-playlist response between syncs. The
        // detection code shipped on 2026-05-13; any channel last
        // views-refreshed BEFORE that has no real signal to report
        // (its column was never populated by a sweep that knew to
        // look). Show "\u2014" + tooltip in that case rather than a
        // misleading "100%" \u2014 which used to be the bug.
        const onTotal = idTotal;
        const onRemoved = r.removed_from_yt || 0;
        const onLive = Math.max(0, onTotal - onRemoved);
        const _viewsTs = r.last_views_refresh_ts || 0;
        const _checkedRecently = _viewsTs >= REMOVED_DETECTION_SINCE;
        let onYtHtml;
        if (onTotal === 0) {
          onYtHtml = '<span class="md-id-dim" title="No tracked videos">&mdash;</span>';
        } else if (!_checkedRecently) {
          onYtHtml = `<span class="md-id-dim" title="Availability has not been checked yet. Run Refresh views/likes.">&mdash;</span>`;
        } else if (onRemoved === 0) {
          // We DID check, found zero removed \u2014 show a real 100%.
          onYtHtml = `<span class="md-id-ok" title="${onTotal.toLocaleString()} video(s) on disk, all still on YouTube as of the last bulk refresh.">\u2713 100%</span>`;
        } else {
          const pct = onLive / onTotal;
          const pctStr = fmtPct(onLive, onTotal);
          const detail = `${onLive.toLocaleString()} of ${onTotal.toLocaleString()} still on YouTube \u2014 ${onRemoved.toLocaleString()} removed / privated / unlisted by the uploader since download. Removed videos can't be metadata-refreshed; local files + IDs are preserved.`;
          if (pct < 0.50) {
            onYtHtml = `<span class="md-id-bad" title="${detail}">\u2717 ${pctStr} (${onLive.toLocaleString()}/${onTotal.toLocaleString()})</span>`;
          } else if (pct < 0.90) {
            onYtHtml = `<span class="md-id-warn" title="${detail}">\u26A0 ${pctStr} (${onLive.toLocaleString()}/${onTotal.toLocaleString()})</span>`;
          } else {
            onYtHtml = `<span class="md-id-neutral" title="${detail}">${pctStr} (${onLive.toLocaleString()}/${onTotal.toLocaleString()})</span>`;
          }
        }

        // Row is clickable (left OR right) anywhere \u2014 opens the action
        // picker. The chevron in the last cell is just a visual hint;
        // it has no click handler of its own. `needsFix` paints a
        // subtle yellow left-border on the row so attention-needing
        // channels still stand out at-a-glance now that the warn-colored
        // \u22EF button is gone.
        return `<tr data-identity='${escapeHtml(ident)}' class="md-row-clickable${needsFix ? ' md-row-needs-fix' : ''}" tabindex="0" aria-label="Actions for ${escapeHtml(r.name)}" title="Open channel actions">
          <td class="md-col-name">${escapeHtml(r.name)}</td>
          <td class="md-col-num">${trackedVideoCount(r).toLocaleString()}</td>
          <td class="md-col-ids">${idHtml}</td>
          <td class="md-col-ids">${thumbHtml}</td>
          <td class="md-col-ids">${txHtml}</td>
          <td class="md-col-onyt">${onYtHtml}</td>
          <td class="md-col-ts ${v.cls}">${v.text}</td>
          <td class="md-col-ts ${c.cls}">${c.text}</td>
          <td class="md-col-act"><span class="md-row-chev" aria-hidden="true">\u203A</span></td>
        </tr>`;
      }).join("");
    };

    const _paintMetadataCatalogStatus = (status) => {
      let note = document.getElementById("metadata-catalog-status");
      if (!note) {
        note = document.createElement("span");
        note.id = "metadata-catalog-status";
        note.className = "status-text metadata-catalog-status";
        note.setAttribute("role", "status");
        note.setAttribute("aria-live", "polite");
        bReload?.parentElement?.insertBefore(note, bReload);
      }
      if (status.phase === "done") {
        note.hidden = true;
        note.textContent = "";
        return;
      }
      note.hidden = false;
      note.setAttribute("aria-live", status.announce === false ? "off" : "polite");
      note.textContent = status.phase === "loading"
        ? "Refreshing channel details…"
        : status.text;
      const inline = document.getElementById("md-load-info");
      if (inline) inline.textContent = status.phase === "loading"
        ? ""
        : status.text;
    };

    window._refreshMetadataTab = async (opts = {}) => {
      if (!nativeBridgeUp()) {
        tbody.innerHTML = '<tr><td colspan="9" class="md-empty">YTArchiver isn\'t ready yet. Try again in a moment.</td></tr>';
        return;
      }
      const preferCache = !!opts.preferCache;
      const force = !!opts.force;
      const cacheTtlMs = 5 * 60 * 1000;
      const cachedForPaint = _rows.length ? _rows : _loadCachedMeta();
      if (cachedForPaint && cachedForPaint.length) {
        _rows = cachedForPaint;
        _thumbsLoaded = _thumbsLoaded || _rowsHaveThumbStatus(_rows);
        try { render(); } catch (e) { /* stale cache shape */ }
        if (preferCache && !force && _lastRowsLoadAt
            && (Date.now() - _lastRowsLoadAt) < cacheTtlMs
            && (_thumbsLoaded || _thumbStatusInFlight)) {
          return;
        }
      }
      const _myLoadGen = ++_loadGen;
      // Instant paint: if we already have rows (this session or persisted
      // from last), show them immediately and refresh in place. Only the
      // genuine first-ever load (no cache) falls back to a status row.
      const _cached = _rows.length ? _rows : _loadCachedMeta();
      if (_cached && _cached.length) {
        _rows = _cached;
        _thumbsLoaded = _thumbsLoaded || _rowsHaveThumbStatus(_rows);
        try { render(); } catch (e) { /* stale cache shape */ }
        if (table) table.classList.add("is-refreshing");
      } else {
        tbody.innerHTML = '<tr><td colspan="9" class="md-empty">'
          + 'Loading channels\u2026 <span id="md-load-info" class="md-load-info"></span>'
          + '</td></tr>';
      }
      // Mark thumbnails as not-yet-loaded so the column renders a
      // spinner until the bulk walk completes.
      _thumbsLoaded = false;
      _thumbStatusInFlight = false;
      _thumbStatusError = "";
      try {
        const outcome = await window.YT.bridge.catalogRead(
          "metadata",
          () => bridgeCall("get_channel_metadata_status"),
          {
            label: "channel metadata",
            onStatus: _paintMetadataCatalogStatus,
          });
        if (outcome.stale || _myLoadGen !== _loadGen) return;
        const rows = outcome.value;
        if (!Array.isArray(rows)) {
          throw new Error(rows?.error || "Channel information is unavailable.");
        }
        _rows = Array.isArray(rows) ? rows : [];
        _catalogLoadError = "";
        _lastRowsLoadAt = Date.now();
        _saveCachedMeta(_rows);
        if (table) table.classList.remove("is-refreshing");
        // Issue #154 (fix): thumbnail_status_bulk walks every channel
        // folder on disk (~100k probes on a 100-channel archive on a
        // network-pooled drive — multi-minute). DO NOT await it here.
        // Render the table immediately with a spinner in the Thumbnails
        // column, then kick the bulk walk in the background and patch
        // the column when it returns.
        if (nativeBridgeUp()) {
          _thumbStatusInFlight = true;
          window.YT.bridge.catalogRead(
            "metadata-background",
            () => bridgeCall("thumbnail_status_bulk"),
            {
              lane: "background",
              label: "thumbnail coverage",
              onStatus: (status) => {
                if (_myLoadGen === _loadGen) {
                  _paintMetadataCatalogStatus(status);
                }
              },
            }).then((thumbOutcome) => {
            if (thumbOutcome.stale || _myLoadGen !== _loadGen) {
              _thumbStatusInFlight = false;
              return;
            }
            _thumbStatusInFlight = false;
            try {
              _mergeThumbStatus(thumbOutcome.value);
            } catch (error) {
              _thumbsLoaded = true;
              _thumbStatusError = error?.message || String(error);
              window._showToast?.(
                `Couldn't load thumbnail coverage: ${_thumbStatusError}`,
                "warn");
            }
            // Re-render once thumb data is in. Cheap — same rows,
            // just rebuilt with the merged values.
            try { render(); } catch {}
          }).catch((error) => {
            if (_myLoadGen !== _loadGen) {
              _thumbStatusInFlight = false;
              return;
            }
            _thumbStatusInFlight = false;
            _thumbsLoaded = true;
            _thumbStatusError = error?.message || String(error);
            try { render(); } catch {}
          });
        } else {
          // No native API → no walk is going to happen. Don't sit on
          // a spinner forever; flip to loaded so the column shows "—".
          _thumbsLoaded = true;
        }
      } catch (e) {
        console.error("get_channel_metadata_status:", e);
        _catalogLoadError = e?.message || String(e);
        if (table) table.classList.remove("is-refreshing");
        // If cached rows are already on screen, keep them and just toast —
        // don't replace real (if stale) data with an error message.
        if (_rows.length) {
          window._showToast?.("Couldn't refresh metadata — showing cached data.", "warn");
        } else {
          tbody.innerHTML = `<tr><td colspan="9" class="md-empty">`
            + `Failed to load: ${escapeHtml(String(e))}</td></tr>`;
        }
        return;
      }
      render();
    };

    // Column header click → cycle sort. Same column toggles dir;
    // new column resets to a sensible default (asc for names, desc
    // for numbers + "refresh" columns where "most recent first"
    // reads more naturally).
    table.querySelectorAll("thead th[data-sort]").forEach(th => {
      th.tabIndex = 0;
      const activateSort = () => {
        const key = th.dataset.sort;
        if (key === _sortKey) {
          _sortDir = _sortDir === "asc" ? "desc" : "asc";
        } else {
          _sortKey = key;
          _sortDir = (key === "name") ? "asc" : "desc";
        }
        render();
      };
      th.addEventListener("click", activateSort);
      th.addEventListener("keydown", (event) => {
        if (event.key !== "Enter" && event.key !== " ") return;
        event.preventDefault();
        activateSort();
      });
    });

    // Per-row action handlers — routed from _openRowMenu after the
    // user picks an action in the askChoice dropdown. Each handler
    // hits a different backend endpoint, all of which enqueue onto
    // the sync queue (except refetch_thumbnails which spawns its own
    // background thread).
    //
    // Toast wording branches on res.paused: when the queue is paused,
    // we want the user to know the task was queued but the queue
    // won't auto-start. Surface message ends with "queue is paused —
    // resume to start." in that case.
    // Defensive: require strictly boolean true (not "true" / "false"
    // strings) so a backend regression that ever flips the field type
    // can't render the "queue paused" tail on every toast (audit:
    // metadataTab.js:565).
    const _pausedTail = (res) => res?.paused === true ? " Queue is paused — resume to start." : "";

    // Prompt the user to pick fast vs thorough backfill mode. Time
    // estimates derived from videoCount: fast is mostly catalog-walk
    // (constant-ish) + ffprobe (~0.07s/file local); thorough is
    // dominated by per-video upload_date fetches at ~0.75s/vid wall
    // clock with 4-wide parallelism, scoped to the candidate shortlist
    // (which we conservatively estimate at videoCount × 0.5 since
    // ~half the files are typically already-resolved and skip).
    //
    // Returns "fast" | "thorough" | null (cancelled).
    const _fmtMin = (sec) => {
      if (sec < 60) return `${Math.max(5, Math.round(sec / 5) * 5)}s`;
      const m = sec / 60;
      if (m < 2) return `~1 min`;
      if (m < 60) return `~${Math.round(m)} min`;
      const h = m / 60;
      return `~${h.toFixed(h < 3 ? 1 : 0)} hr`;
    };
    const _promptBackfillMode = async (videoCount, ctxLabel) => {
      const n = Math.max(1, Number(videoCount) || 1);
      const fastSec = 30 + n * 0.07;  // ffprobe is the variable cost
      const thoroughSec = fastSec + (n * 0.5 * 0.75);
      const fastTxt = _fmtMin(fastSec);
      const thoroughTxt = _fmtMin(thoroughSec);
      const msg = (ctxLabel ? ctxLabel + "\n\n" : "")
        + "Fast matches by title and duration. Try this first.\n\n"
        + "Thorough also checks upload dates. It is slower and uses more "
        + "YouTube requests; use it if Fast leaves videos unmatched.";
      const pick = await window.askChoice({
        title: "Fix missing video IDs",
        message: msg,
        choices: [
          { label: `Fast (${fastTxt})`, value: "fast", kind: "primary" },
          { label: `Thorough (${thoroughTxt})`, value: "thorough" },
        ],
      });
      return pick === "thorough" ? "thorough"
           : pick === "fast" ? "fast"
           : null;
    };
    const _rowForIdent = (ident) => {
      try {
        return _rows.find(r => ident.folder && r.folder === ident.folder)
          || _rows.find(r => ident.url && r.url === ident.url)
          || null;
      } catch {
        return null;
      }
    };
    const _runRowAct = async (act, ident, rowRefs) => {
      if (!nativeBridgeUp()) return;
      if (act === "views") {
        if (!nativeBridgeUp()) {
          window._showToast?.("YTArchiver isn't ready yet. Try again in a moment.", "warn"); return;
        }
        const pickN = parseInt(rowRefs, 10);
        const days = (Number.isFinite(pickN) && pickN > 0) ? pickN : null;
        try {
          const res = await bridgeCall(
            "metadata_refresh_views_channel", ident, days);
          if (!res?.ok) {
            window._showToast?.(res?.error || "Failed.", "error");
          } else {
            const scope = days ? ` (last ${days}d)` : " (all videos)";
            window._showToast?.(
              `Queued views/likes refresh${scope}.` + _pausedTail(res),
              res?.paused ? "warn" : "ok");
          }
        } catch (err) {
          window._showToast?.(String(err), "error");
        }
      } else if (act === "backfill") {
        if (!nativeBridgeUp()) {
          window._showToast?.("YTArchiver isn't ready yet. Try again in a moment.", "warn"); return;
        }
        // Look up this channel's video count from the cached _rows
        // so the dialog can show real time estimates.
        let _vc = 0;
        try {
          const _r = _rowForIdent(ident);
          _vc = _r?.video_count || 0;
        } catch {}
        const _mode = await _promptBackfillMode(
          _vc, `Channel: ${ident.folder || ident.url || ""}`);
        if (!_mode) return; // cancelled
        try {
          const res = await bridgeCall("metadata_backfill_ids_channel", ident, _mode);
          if (!res?.ok) {
            window._showToast?.(res?.error || "Failed.", "error");
          } else {
            const head = _mode === "thorough"
              ? "Queued missing-ID repair (thorough)."
              : "Queued missing-ID repair (fast).";
            window._showToast?.(head + _pausedTail(res),
              res?.paused ? "warn" : "ok");
          }
        } catch (err) {
          window._showToast?.(String(err), "error");
        }
      } else if (act === "thumbs") {
        // refetch missing thumbnails for one channel.
        if (!nativeBridgeUp()) {
          window._showToast?.("YTArchiver isn't ready yet. Try again in a moment.", "warn"); return;
        }
        try {
          const res = await bridgeCall("refetch_thumbnails", ident);
          if (res?.started) {
            window._showToast?.(
              "Thumbnail refetch started — check the log for progress.",
              "ok");
          } else if (res?.error) {
            window._showToast?.(res.error, "error");
          }
        } catch (err) {
          window._showToast?.(String(err), "error");
        }
      } else if (act === "transcribe") {
        const row = _rowForIdent(ident);
        const channelName = row?.name || ident.folder || "";
        if (!channelName) {
          window._showToast?.("Channel name unavailable.", "error");
          return;
        }
        const channelIdentity = {
          name: channelName,
          folder: row?.folder || ident.folder || "",
          url: row?.url || ident.url || "",
        };
        if (typeof window._askTranscribeChannel === "function") {
          await window._askTranscribeChannel(channelIdentity);
          return;
        }
        try {
          const res = await bridgeCall("chan_transcribe_all", channelIdentity);
          if (!res?.ok) {
            window._showToast?.(res?.error || "Transcribe failed to start.",
              "error");
          } else if (res?.needs_choice) {
            const pick = askChoice ? await askChoice({
              title: "Transcribe - " + channelName,
              message: "Where should transcript files be placed?",
              choices: [
                { label: `Follow organization (${res.org_label} folders)`,
                  value: "follow", primary: true },
                { label: "Combined (one file for entire channel)",
                  value: "combined" },
              ],
              countdownSecs: 60,
              countdownLabel: "Auto-selecting Follow organization in",
            }) : "follow";
            if (pick === null) return;
            const retry = await bridgeCall(
              "chan_transcribe_all", channelIdentity, pick === "combined");
            if (!retry?.ok) {
              window._showToast?.(
                retry?.error || "Transcribe failed to start.", "error");
            } else {
              window._showToast?.(
                `Queued ${retry?.queued || 0} video(s) for transcription.`,
                "ok");
            }
          } else {
            window._showToast?.(
              `Queued ${res?.queued || 0} video(s) for transcription.`,
              "ok");
          }
        } catch (err) {
          window._showToast?.(String(err), "error");
        }
      } else if (act === "comments") {
        if (!nativeBridgeUp()) {
          window._showToast?.("YTArchiver isn't ready yet. Try again in a moment.", "warn"); return;
        }
        // Caller passes the time-scope directly as the `arg` parameter
        // (set by the context-menu submenu's data-days attribute).
        // null = all videos. Numeric = N-day window.
        const pickN = parseInt(rowRefs, 10);
        const days = (Number.isFinite(pickN) && pickN > 0) ? pickN : null;
        try {
          const res = await bridgeCall("metadata_refresh_comments_channel", ident, days);
          if (!res?.ok) {
            window._showToast?.(res?.error || "Failed.", "error");
          } else {
            const head = days ? `Queued comments refresh (${days}d).`
                              : "Queued comments refresh (all).";
            window._showToast?.(head + _pausedTail(res),
              res?.paused ? "warn" : "ok");
          }
        } catch (err) {
          window._showToast?.(String(err), "error");
        }
      }
    };

    // Right-click-style dropdown for the action menu. Positioned at
    // the cursor, NOT a centered modal. Left + right click both open
    // the same dropdown at the click point. Active reference + close
    // helper — only one menu lives at a time.
    let _activeMenu = null;
    let _activeMenuTrigger = null;
    const _closeRowMenu = (restoreFocus = false) => {
      const trigger = _activeMenuTrigger;
      if (_activeMenu) {
        try { _activeMenu.remove(); } catch {}
        _activeMenu = null;
      }
      _activeMenuTrigger = null;
      if (restoreFocus && trigger?.isConnected) {
        try { trigger.focus(); } catch {}
      }
    };
    const _openRowMenu = (tr, clientX, clientY) => {
      _closeRowMenu();
      _activeMenuTrigger = tr;
      let ident = {};
      try { ident = JSON.parse(tr.dataset.identity || "{}"); } catch {}
      const row = _rowForIdent(ident);
      const needsFix = tr.classList.contains("md-row-needs-fix");
      const txTotal = row?.tx_total || 0;
      const txWith = row?.tx_transcribed || 0;
      const needsTranscribe = txTotal > 0 && txWith < txTotal;
      // Reorder so the most-likely action is FIRST — askChoice focuses
      // the first primary-kinded button for keyboard confirm. Since
      // the default kind is now primary (all green), ordering alone
      // picks the default without making the others visually secondary.
      // Build the dropdown DOM. Compact list, positioned at the
      // cursor \u2014 looks like a Windows right-click menu, NOT a
      // centered askChoice modal.
      const menu = document.createElement("div");
      menu.className = "md-context-menu";
      menu.setAttribute("role", "menu");
      const mkItem = (label, act, opts) => {
        opts = opts || {};
        const b = document.createElement("button");
        b.type = "button";
        b.className = "md-cm-item" + (opts.warn ? " md-cm-warn" : "");
        b.setAttribute("role", "menuitem");
        b.dataset.act = act;
        if (opts.days !== undefined) b.dataset.days = String(opts.days);
        b.textContent = label;
        return b;
      };
      const fixItem = mkItem("Fix missing video IDs", "backfill",
                             { warn: needsFix });
      const transcribeItem = mkItem("Transcribe missing", "transcribe",
                                    { warn: needsTranscribe });
      const thumbsItem = mkItem("Refetch missing thumbnails", "thumbs");
      // Views/likes supports the same recent-upload scoping as the Browse
      // channel menu. This keeps a large archive from defaulting to a full
      // historical pass when only current counts matter.
      const viewsWrap = document.createElement("div");
      viewsWrap.className = "md-cm-sub-wrap";
      const viewsHead = document.createElement("button");
      viewsHead.type = "button";
      viewsHead.className = "md-cm-item md-cm-has-sub";
      viewsHead.setAttribute("role", "menuitem");
      viewsHead.setAttribute("aria-haspopup", "menu");
      viewsHead.setAttribute("aria-expanded", "false");
      viewsHead.innerHTML = "Refresh views/likes<span class=\"md-cm-chev\">›</span>";
      const viewsSub = document.createElement("div");
      viewsSub.className = "md-cm-sub";
      viewsSub.setAttribute("role", "menu");
      viewsSub.setAttribute("aria-label", "Refresh views and likes range");
      viewsSub.appendChild(mkItem("Last week", "views", { days: 7 }));
      viewsSub.appendChild(mkItem("Last month", "views", { days: 30 }));
      viewsSub.appendChild(mkItem("Last year", "views", { days: 365 }));
      viewsSub.appendChild(mkItem("All videos", "views", { days: 0 }));
      viewsWrap.appendChild(viewsHead);
      viewsWrap.appendChild(viewsSub);
      // "Refresh comments" carries a hover-submenu with day-scope picks.
      const commentsWrap = document.createElement("div");
      commentsWrap.className = "md-cm-sub-wrap";
      const commentsHead = document.createElement("button");
      commentsHead.type = "button";
      commentsHead.className = "md-cm-item md-cm-has-sub";
      commentsHead.setAttribute("role", "menuitem");
      commentsHead.setAttribute("aria-haspopup", "menu");
      commentsHead.setAttribute("aria-expanded", "false");
      commentsHead.innerHTML = "Refresh comments<span class=\"md-cm-chev\">\u203a</span>";
      const commentsSub = document.createElement("div");
      commentsSub.className = "md-cm-sub";
      commentsSub.setAttribute("role", "menu");
      commentsSub.setAttribute("aria-label", "Refresh comments range");
      commentsSub.appendChild(mkItem("Last 7 days", "comments", { days: 7 }));
      commentsSub.appendChild(mkItem("Last 30 days", "comments", { days: 30 }));
      commentsSub.appendChild(mkItem("Last 90 days", "comments", { days: 90 }));
      commentsSub.appendChild(mkItem("All videos", "comments", { days: 0 }));
      commentsWrap.appendChild(commentsHead);
      commentsWrap.appendChild(commentsSub);
      // Mark the first choice `primary: true` so Enter confirms it
      // (askChoice uses this flag to pick the keyboard-focus target).
      // All three still render green — the primary flag only affects
      // auto-focus, not color, under the new default-to-primary kind.
      if (needsFix) {
        menu.appendChild(fixItem);
        if (needsTranscribe) menu.appendChild(transcribeItem);
        menu.appendChild(viewsWrap);
      } else {
        if (needsTranscribe) menu.appendChild(transcribeItem);
        menu.appendChild(viewsWrap);
        menu.appendChild(fixItem);
      }
      menu.appendChild(commentsWrap);
      menu.appendChild(thumbsItem);

      // Leaf-item click → close + dispatch. Clicking a submenu header
      // opens it and moves focus to the first choice, matching keyboard use.
      menu.addEventListener("click", async (ev) => {
        const btn = ev.target.closest(".md-cm-item");
        if (!btn) return;
        if (btn.classList.contains("md-cm-has-sub")) {
          const expanded = btn.getAttribute("aria-expanded") === "true";
          menu.querySelectorAll(".md-cm-has-sub").forEach(head =>
            head.setAttribute("aria-expanded", "false"));
          btn.setAttribute("aria-expanded", String(!expanded));
          if (!expanded) {
            btn.nextElementSibling?.querySelector(".md-cm-item")?.focus?.();
          }
          return;
        }
        const act = btn.dataset.act;
        const days = btn.dataset.days; // string or undefined
        _closeRowMenu();
        await _runRowAct(act, ident, days);
        // The refresh runs server-side (queued); re-pull the table shortly
        // after so a quick views/likes refresh on a small channel shows up
        // without the user manually hitting Reload. Harmless if the job is
        // still running — the row just re-renders with the current backend
        // state. (Full completion for big channels still needs a Reload.)
        _scheduleMetadataRefresh();
      });

      document.body.appendChild(menu);
      _activeMenu = menu;

      // Position at the click point, flipping if it would go off-screen.
      const margin = 4;
      const vw = window.innerWidth, vh = window.innerHeight;
      let x = clientX, y = clientY;
      const r = menu.getBoundingClientRect();
      if (x + r.width + margin > vw) x = Math.max(margin, vw - r.width - margin);
      if (y + r.height + margin > vh) y = Math.max(margin, vh - r.height - margin);
      menu.style.left = x + "px";
      menu.style.top = y + "px";

      setTimeout(() => {
        const first = menu.querySelector(
          ":scope > .md-cm-item, :scope > .md-cm-sub-wrap > .md-cm-has-sub");
        first?.focus?.();
      }, 0);
    };
    // Outside click / Escape / scroll / resize → close.
    document.addEventListener("mousedown", (e) => {
      if (!_activeMenu) return;
      if (_activeMenu.contains(e.target)) return;
      _closeRowMenu();
    });
    document.addEventListener("keydown", (e) => {
      if (_activeMenu && e.key === "Escape") {
        e.preventDefault();
        _closeRowMenu(true);
      } else if (_activeMenu && ["Enter", " ", "ArrowRight"].includes(e.key)
                 && document.activeElement?.classList?.contains("md-cm-has-sub")) {
        e.preventDefault();
        const head = document.activeElement;
        _activeMenu.querySelectorAll(".md-cm-has-sub").forEach(other =>
          other.setAttribute("aria-expanded", String(other === head)));
        head.nextElementSibling?.querySelector(".md-cm-item")?.focus?.();
      } else if (_activeMenu && e.key === "ArrowLeft"
                 && document.activeElement?.closest?.(".md-cm-sub")) {
        e.preventDefault();
        const sub = document.activeElement.closest(".md-cm-sub");
        const head = sub?.previousElementSibling;
        head?.setAttribute("aria-expanded", "false");
        head?.focus?.();
      } else if (_activeMenu && ["ArrowDown", "ArrowUp", "Home", "End"].includes(e.key)) {
        const activeSub = document.activeElement?.closest?.(".md-cm-sub");
        const items = activeSub
          ? Array.from(activeSub.querySelectorAll(":scope > .md-cm-item"))
          : Array.from(_activeMenu.querySelectorAll(
            ":scope > .md-cm-item, :scope > .md-cm-sub-wrap > .md-cm-has-sub"));
        if (!items.length) return;
        e.preventDefault();
        const current = items.indexOf(document.activeElement);
        let next = 0;
        if (e.key === "End") next = items.length - 1;
        else if (e.key === "Home") next = 0;
        else if (e.key === "ArrowUp") next = current <= 0 ? items.length - 1 : current - 1;
        else next = current < 0 || current >= items.length - 1 ? 0 : current + 1;
        items[next].focus();
      }
    });
    window.addEventListener("resize", _closeRowMenu);
    window.addEventListener("scroll", _closeRowMenu, true);
    // Left click anywhere on a row → open menu at the click point.
    tbody.addEventListener("click", (e) => {
      const tr = e.target.closest("tr.md-row-clickable");
      if (!tr) return;
      try {
        const sel = window.getSelection?.();
        // Only suppress the menu if the active text selection is
        // INSIDE this row — old check blocked the click even when
        // the selection was elsewhere in the page (audit:
        // metadataTab.js:818).
        if (sel && sel.toString().length > 0
            && typeof sel.containsNode === "function"
            && sel.containsNode(tr, true)) return;
      } catch {}
      _openRowMenu(tr, e.clientX, e.clientY);
    });
    // Right click anywhere on a row → open SAME menu at the click point.
    tbody.addEventListener("contextmenu", (e) => {
      const tr = e.target.closest("tr.md-row-clickable");
      if (!tr) return;
      e.preventDefault();
      _openRowMenu(tr, e.clientX, e.clientY);
    });
    tbody.addEventListener("keydown", (e) => {
      const tr = e.target.closest("tr.md-row-clickable");
      if (!tr) return;
      const openMenu = e.key === "Enter" || e.key === " "
        || e.key === "ContextMenu" || (e.shiftKey && e.key === "F10");
      if (!openMenu) return;
      e.preventDefault();
      const rect = tr.getBoundingClientRect();
      _openRowMenu(tr, rect.left + 24, rect.top + Math.min(24, rect.height / 2));
    });

    // Bulk buttons.
    if (bAllViews) {
      bAllViews.addEventListener("click", async () => {
        if (!nativeBridgeUp()) {
          window._showToast?.("YTArchiver isn't ready yet. Try again in a moment.", "warn"); return;
        }
        const pick = await (window.askChoice ? window.askChoice({
          title: "Refresh views/likes — all channels",
          message: "Choose videos by upload date. A shorter range is much faster on a large archive.",
          choices: [
            { label: "Last week", value: "7" },
            { label: "Last month", value: "30" },
            { label: "Last year", value: "365", kind: "primary" },
            { label: "All videos", value: "0" },
          ],
        }) : Promise.resolve("365"));
        if (pick === null) return;
        const parsed = parseInt(pick, 10);
        const days = (Number.isFinite(parsed) && parsed > 0) ? parsed : null;
        try {
          const res = await bridgeCall("metadata_queue_all", true, days);
          if (!res?.ok) {
            window._showToast?.(res?.error || "Failed.", "error");
          } else {
            const scope = days ? `last ${days}d` : "all videos";
            window._showToast?.(
              `Queued ${scope} for ${res.queued} channel(s).`
                + _pausedTail(res),
              res?.paused ? "warn" : "ok");
          }
        } catch (err) {
          window._showToast?.(String(err), "error");
        }
      });
    }
    if (bAllComments) {
      bAllComments.addEventListener("click", async () => {
        if (!nativeBridgeUp()) {
          window._showToast?.("YTArchiver isn't ready yet. Try again in a moment.", "warn"); return;
        }
        const pick = await (window.askChoice ? window.askChoice({
          title: "Refresh all comments",
          message: "Comments use the slow per-video yt-dlp path \u2014 scope to recent uploads when possible.",
          choices: [
            { label: "1 month", value: 30, kind: "primary" },
            { label: "1 year", value: 365 },
            { label: "All videos (slow!)", value: 0, kind: "ghost" },
          ],
        }) : Promise.resolve(30));
        if (pick === null || pick === undefined) return;
        // askChoice returns strings; coerce. 0 means all (no scope).
        const pickN = parseInt(pick, 10);
        const days = (Number.isFinite(pickN) && pickN > 0) ? pickN : 0;
        try {
          const res = await bridgeCall("metadata_refresh_comments_all", days);
          if (!res?.ok) {
            window._showToast?.(res?.error || "Failed.", "error");
          } else {
            window._showToast?.(
              `Queued for ${res.queued} channel(s).` + _pausedTail(res),
              res?.paused ? "warn" : "ok");
          }
        } catch (err) {
          window._showToast?.(String(err), "error");
        }
      });
    }
    if (bAllBackfill) {
      bAllBackfill.addEventListener("click", async () => {
        if (!nativeBridgeUp()) {
          window._showToast?.("YTArchiver isn't ready yet. Try again in a moment.", "warn"); return;
        }
        // Show how many channels would be queued BEFORE asking so the
        // user knows whether this is a big operation or a tiny no-op.
        // We don't have a dedicated preview method — just show the
        // aggregate count we're holding from the last tab render.
        const needing = _rows.filter(r => (r.id_total || 0) > 0 && (r.id_missing || 0) > 0).length;
        if (needing === 0) {
          if (_catalogLoadError) {
            window._showToast?.(
              `Couldn't verify video IDs: ${_catalogLoadError}`, "error");
            return;
          }
          window._showToast?.("All channels already have YouTube IDs.", "ok");
          return;
        }
        const pick = await (window.askChoice ? window.askChoice({
          title: "Fix missing video IDs?",
          message: `${needing} channel(s) have videos with missing YouTube IDs. Choose which channels to repair.`,
          choices: [
            { label: `Queue ${needing} channel(s)`, value: "missing", kind: "primary" },
            { label: "Queue ALL channels (force)", value: "all" },
          ],
        }) : Promise.resolve("missing"));
        if (!pick) return;
        // Sum total videos across affected channels so the time
        // estimate reflects the actual workload.
        const _targetChannels = _rows.filter(r =>
          pick === "all" ? true
            : ((r.id_total || 0) > 0 && (r.id_missing || 0) > 0));
        const _totalVideos = _targetChannels.reduce(
          (sum, r) => sum + (r.video_count || 0), 0);
        const _mode = await _promptBackfillMode(
          _totalVideos,
          `Running across ${_targetChannels.length} channel(s), `
          + `${_totalVideos.toLocaleString()} total videos. Time `
          + `estimates below are total — each channel runs sequentially.`);
        if (!_mode) return;
        try {
          const onlyMissing = (pick === "missing");
          const res = await bridgeCall("metadata_backfill_ids_all", onlyMissing, _mode);
          if (!res?.ok) {
            window._showToast?.(res?.error || "Failed.", "error");
          } else {
            const tail = _mode === "thorough" ? " (thorough)" : " (fast)";
            const msg = res.skipped_up_to_date
              ? `Queued ${res.queued} (${res.skipped_up_to_date} already OK)${tail}.`
              : `Queued ${res.queued} channel(s)${tail}.`;
            window._showToast?.(msg + _pausedTail(res),
              res?.paused ? "warn" : "ok");
          }
        } catch (err) {
          window._showToast?.(String(err), "error");
        }
      });
    }
    // Bulk thumbnail refetch — walks every channel sequentially in
    // the background. Doesn't touch the sync queue (thumbnails are
    // a cosmetic side-channel fetch). Aggregate the missing-thumb
    // count from the cached _rows so the confirmation dialog can
    // tell the user how big the job actually is.
    if (bAllThumbs) {
      bAllThumbs.addEventListener("click", async () => {
        if (!nativeBridgeUp()) {
          window._showToast?.("YTArchiver isn't ready yet. Try again in a moment.", "warn"); return;
        }
        const nMissing = _rows.reduce(
          (sum, r) => sum + Math.max(
            0, (r.thumb_total || 0) - (r.thumb_with || 0)),
          0);
        const nChannels = _rows.filter(r =>
          (r.thumb_total || 0) > (r.thumb_with || 0)).length;
        if (nMissing === 0) {
          if (_thumbStatusError || !_thumbsLoaded) {
            window._showToast?.(
              _thumbStatusError
                ? `Couldn't verify thumbnails: ${_thumbStatusError}`
                : "Thumbnail coverage is still loading.",
              _thumbStatusError ? "error" : "warn");
            return;
          }
          window._showToast?.(
            "Every channel already has all its thumbnails.", "ok");
          return;
        }
        const ok = await (window.askChoice ? window.askChoice({
          title: "Refetch missing thumbnails for all channels?",
          message: `${nMissing.toLocaleString()} thumbnail(s) missing `
            + `across ${nChannels} channel(s). Each missing thumbnail `
            + `is downloaded from its saved YouTube information. `
            + `Runs in the background; progress appears in the log. `
            + `(Some thumbnails may be unrecoverable if the source URL `
            + `404s or the video was removed from YouTube.)`,
          choices: [
            { label: "Start", value: "go", kind: "primary" },
          ],
        }) : Promise.resolve("go"));
        if (!ok) return;
        try {
          const res = await bridgeCall("refetch_thumbnails_all");
          if (!res?.ok) {
            window._showToast?.(res?.error || "Failed.", "error");
          } else {
            window._showToast?.(
              `Thumbnail refetch started across ${res.channels} channel(s) `
              + `— watch the log for progress.`, "ok");
          }
        } catch (err) {
          window._showToast?.(String(err), "error");
        }
      });
    }

    if (bReload) {
      bReload.addEventListener("click", () => {
        window._refreshMetadataTab?.({ force: true });
      });
    }

    // Force-recheck ALL stats — ignores every cache and walks/queries
    // fresh: video counts (archive_scan), Video IDs (DB GROUP BY),
    // Thumbnails (parallel disk walk).
    const bRecheckThumbs = document.getElementById("btn-md-recheck-thumbs");
    const recheckProgress = document.getElementById("md-recheck-progress");
    if (bRecheckThumbs) {
      bRecheckThumbs.addEventListener("click", async () => {
        if (bRecheckThumbs.disabled) return;
        if (!nativeBridgeUp()) {
          window._showToast?.("YTArchiver isn't ready yet. Try again in a moment.", "warn"); return;
        }
        bRecheckThumbs.disabled = true;
        if (recheckProgress) {
          recheckProgress.hidden = false;
          recheckProgress.textContent = " · rechecking…";
        }
        // Reset the loaded flag so the Thumbnails column shows the
        // spinner again while the force-walk runs. (The row data
        // itself is left intact — we don't blank it out — but the
        // spinner takes precedence so the user sees that work is
        // in progress.)
        const _myLoadGen = ++_loadGen;
        _thumbsLoaded = false;
        try { render(); } catch {}
        try {
          const outcome = await window.YT.bridge.catalogRead(
            "metadata-background",
            async (context) => {
              const metaRows = await bridgeCall(
                "get_channel_metadata_status", true);
              if (!context.isCurrent()) return null;
              const thRes = await bridgeCall("thumbnail_status_bulk", true);
              return { metaRows, thRes };
            },
            {
              lane: "background",
              label: "all metadata statistics",
              onStatus: (status) => {
                if (!recheckProgress || status.phase === "done") return;
                recheckProgress.textContent = ` · ${status.text}`;
              },
            });
          if (outcome.stale || _myLoadGen !== _loadGen || !outcome.value) return;
          const { metaRows, thRes } = outcome.value;
          if (!Array.isArray(metaRows)) {
            throw new Error(metaRows?.error || "Channel information is unavailable.");
          }
          if (!thRes?.ok) {
            throw new Error(thRes?.error || "Thumbnail coverage is unavailable.");
          }
          _rows = metaRows;
          _catalogLoadError = "";
          _lastRowsLoadAt = Date.now();
          _mergeThumbStatus(thRes);
          try { render(); } catch {}
          const thMap = thRes?.rows || {};
          const ch = Object.keys(thMap).length;
          window._showToast?.(
            `Rechecked ${ch} channel(s).`, "ok");
        } catch (e) {
          if (_myLoadGen !== _loadGen) return;
          // Stuck spinner is worse than a dash — flip back on error.
          _thumbsLoaded = true;
          try { render(); } catch {}
          window._showToast?.(String(e), "error");
        } finally {
          if (_myLoadGen === _loadGen && table) {
            table.classList.remove("is-refreshing");
          }
          bRecheckThumbs.disabled = false;
          if (recheckProgress) {
            recheckProgress.hidden = true;
            recheckProgress.textContent = "";
          }
        }
      });
    }

    // If Library is initially visible, pull channel information now.
    const metaView = document.getElementById("settings-view-library");
    if (metaView && !metaView.hidden) {
      // Re-check display inside the timeout — if the user clicks
      // away to another tab in the 400ms window, the fetch isn't
      // needed and would just spam the bridge (audit:
      // metadataTab.js:1077).
      setTimeout(() => {
        if (!metaView.hidden) {
          window._refreshMetadataTab?.();
        }
      }, 400);
    }
  }
  window._initMetadataTab = initMetadataTab;

  window.initMetadataTab = initMetadataTab;
})();
