/* ═══════════════════════════════════════════════════════════════════════
   metadataTab.js — Settings → Metadata tab

   Extracted from indexControls.js (Patch 15, v71.7). The biggest single
   chunk of indexControls.js — owns the per-channel metadata refresh
   workflow:

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

  const askConfirm = window.askConfirm;
  const askDanger = window.askDanger;
  const askQuestion = window.askQuestion;
  const askChoice = window.askChoice;
  const askTextInput = window.askTextInput;
  const showContextMenu = window.showContextMenu || (() => {});

  function initMetadataTab() {
    const tbody = document.getElementById("metadata-tbody");
    const table = document.getElementById("metadata-table");
    const bAllViews = document.getElementById("btn-md-refresh-all-views");
    const bAllComments = document.getElementById("btn-md-refresh-all-comments");
    const bAllBackfill = document.getElementById("btn-md-backfill-all-ids");
    const bAllThumbs = document.getElementById("btn-md-refetch-all-thumbs");
    const bReload = document.getElementById("btn-md-reload");
    if (!tbody || !table) return;

    const getApi = () => window.pywebview?.api;

    // Current dataset + sort state. Sort state persists across reloads.
    let _rows = [];
    let _sortKey = "views";   // matches data-sort-active in HTML
    let _sortDir = "asc";     // oldest-first by default
    // Whether the bulk thumbnail walk has returned at least once for the
    // currently-rendered rows. Used to swap the Thumbnails column between
    // "loading…" (spinner) and the real percentage. Cleared on each
    // refresh + force-recheck so the indicator shows up again.
    let _thumbsLoaded = false;
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

    const escapeHtml = (s) => String(s || "")
      .replace(/&/g, "&amp;").replace(/</g, "&lt;")
      .replace(/>/g, "&gt;").replace(/"/g, "&quot;");

    const sortRows = (rows) => {
      const mult = _sortDir === "asc" ? 1 : -1;
      const kfn = {
        name: (r) => (r.name || "").toLowerCase(),
        videos: (r) => r.video_count || 0,
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
      // For Still-on-YT: only aggregate channels that have actually been
      // bulk-checked since the removed-from-YT detection shipped. Sum-
      // ming the others would dilute the percentage with channels whose
      // `removed_from_yt` is 0 only because nobody ever checked.
      let onTotChecked = 0, onRemovedChecked = 0;
      let onChannelsChecked = 0;
      for (const r of _rows) {
        nVideos += (r.video_count || 0);
        idTot += (r.id_total || 0);
        idWith += (r.id_with_id || 0);
        thTot += (r.thumb_total || 0);
        thWith += (r.thumb_with || 0);
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
      // which clipped to "98,039/101,9… (96…").
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
                `${p.toFixed(1)}%`,
                `${fmt(idWith)} / ${fmt(idTot)}`,
                tier(p));
      } else {
        setCard("md-tot-card-ids", "md-tot-ids", "—", "", null);
      }
      // Thumbs tile.
      if (thTot > 0) {
        const p = pct(thWith, thTot);
        setCard("md-tot-card-thumbs", "md-tot-thumbs",
                `${p.toFixed(1)}%`,
                `${fmt(thWith)} / ${fmt(thTot)}`,
                tier(p));
      } else {
        setCard("md-tot-card-thumbs", "md-tot-thumbs",
                _thumbsLoaded ? "—" : "loading…",
                "",
                null);
      }
      // Still-on-YT tile. ONLY counts channels actually checked since
      // detection shipped (see filter above). Three states:
      //   - No channel checked yet → "—" + "(run views/likes refresh)"
      //   - Some checked, some/no removals → live percentage of checked
      //   - All checked, 0 removed → real 100%
      const _onyt_card = document.getElementById("md-tot-card-onyt");
      if (_onyt_card) {
        // Override the static title so the tooltip reflects scope.
        _onyt_card.title = onChannelsChecked === 0
          ? "No channels have been views/likes-refreshed since the "
            + "removed-from-YouTube detection shipped. Run a bulk "
            + "Refresh views/likes to populate this column."
          : `${onChannelsChecked} of ${nChannels} channel(s) have been `
            + `checked. The other ${nChannels - onChannelsChecked} have `
            + `no removal data yet — run Refresh views/likes on them to `
            + `include them in this number.`;
      }
      if (onChannelsChecked === 0) {
        setCard("md-tot-card-onyt", "md-tot-onyt",
                "—",
                "not yet checked",
                null);
      } else if (onTotChecked > 0 && onRemovedChecked > 0) {
        const live = Math.max(0, onTotChecked - onRemovedChecked);
        const p = pct(live, onTotChecked);
        // Bug fix v62.4: a tiny number of removals on a huge total
        // (e.g. 20 removed of 101,988 = 99.98%) used to round up to
        // "100.0%" via toFixed(1). The user then saw "100%" in the
        // card while per-row entries showed 99.8%/99.9% — a confusing
        // mismatch even though both sides agreed on the underlying
        // count. Bump to toFixed(2) whenever toFixed(1) would round
        // to "100.0" so the "we found N removed" signal stays in the
        // displayed percentage instead of getting hidden by rounding.
        let pStr = p.toFixed(1);
        if (pStr === "100.0" && p < 100) pStr = p.toFixed(2);
        setCard("md-tot-card-onyt", "md-tot-onyt",
                `${pStr}%`,
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
        if (th.dataset.sort === _sortKey) {
          th.setAttribute("data-sort-active", "");
          th.setAttribute("data-sort-dir", _sortDir);
        } else {
          th.removeAttribute("data-sort-active");
          th.removeAttribute("data-sort-dir");
        }
      });
      if (!_rows.length) {
        tbody.innerHTML = '<tr><td colspan="8" class="md-empty">No channels configured.</td></tr>';
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
          idHtml = '<span class="md-id-dim" title="No videos registered in the index DB for this channel">&mdash;</span>';
        } else {
          const pct = idWith / idTotal;
          const pctStr = (pct * 100).toFixed(1) + "%";
          // Rich tooltip: show the split between "tried but
          // couldn't resolve" (probably genuinely unrecoverable)
          // vs "not yet attempted" (run Fix IDs to pick these up).
          let detail = `${idWith.toLocaleString()} of ${idTotal.toLocaleString()} video(s) have resolvable video IDs`;
          if (idMissing > 0) {
            detail += ` — ${idMissing.toLocaleString()} missing`;
            if (idTriedFailed > 0 && idNotYet > 0) {
              detail += ` (${idTriedFailed.toLocaleString()} tried unsuccessfully, ${idNotYet.toLocaleString()} not yet attempted — run Fix IDs)`;
            } else if (idTriedFailed > 0) {
              detail += ` (all tried — likely renamed or removed from YouTube)`;
            } else {
              detail += ` (run Fix IDs to backfill)`;
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
        if (!_thumbsLoaded) {
          // Bulk thumb walk still in flight — show a spinner so the user
          // knows the column is loading, not actually 0%. Was "—" before,
          // which was indistinguishable from "no on-disk videos".
          thumbHtml = '<span class="md-thumb-loading" title="Counting thumbnail sidecars on disk…"><span class="md-spinner" aria-hidden="true"></span>loading…</span>';
        } else if (thTotal === 0) {
          thumbHtml = '<span class="md-id-dim" title="No on-disk videos to check">&mdash;</span>';
        } else {
          const pct = thWith / thTotal;
          const pctStr = (pct * 100).toFixed(1) + "%";
          let detail = `${thWith.toLocaleString()} of ${thTotal.toLocaleString()} video(s) have a thumbnail sidecar`;
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
          onYtHtml = `<span class="md-id-dim" title="No removed-from-YouTube data for this channel yet. Run 'Refresh views/likes' for this row (or the bulk button) to populate.">&mdash;</span>`;
        } else if (onRemoved === 0) {
          // We DID check, found zero removed \u2014 show a real 100%.
          onYtHtml = `<span class="md-id-ok" title="${onTotal.toLocaleString()} video(s) on disk, all still on YouTube as of the last bulk refresh.">\u2713 100%</span>`;
        } else {
          const pct = onLive / onTotal;
          const pctStr = (pct * 100).toFixed(1) + "%";
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
        return `<tr data-identity='${escapeHtml(ident)}' class="md-row-clickable${needsFix ? ' md-row-needs-fix' : ''}" title="Click for channel actions (or right-click)">
          <td class="md-col-name">${escapeHtml(r.name)}</td>
          <td class="md-col-num">${r.video_count.toLocaleString()}</td>
          <td class="md-col-ids">${idHtml}</td>
          <td class="md-col-ids">${thumbHtml}</td>
          <td class="md-col-onyt">${onYtHtml}</td>
          <td class="md-col-ts ${v.cls}">${v.text}</td>
          <td class="md-col-ts ${c.cls}">${c.text}</td>
          <td class="md-col-act"><span class="md-row-chev" aria-hidden="true">\u203A</span></td>
        </tr>`;
      }).join("");
    };

    // Compact elapsed formatter — mirrors backend utils.format_elapsed
    // (rule: never show raw seconds beyond 60 — fold into
    // "Xm YYs" / "Xh Ym YYs"). Used by the Loading... ticker so the
    // Metadata tab table doesn't sit silent during a slow DB query.
    const _fmtElapsed = (s) => {
      s = Math.max(0, Math.floor(s));
      if (s < 60) return s + "s";
      const h = Math.floor(s / 3600);
      const m = Math.floor((s % 3600) / 60);
      const ss = s % 60;
      const pad = (n) => (n < 10 ? "0" + n : "" + n);
      if (h) return `${h}h ${m}m ${pad(ss)}s`;
      return `${m}m ${pad(ss)}s`;
    };

    window._refreshMetadataTab = async () => {
      const api = getApi();
      if (!api?.get_channel_metadata_status) {
        tbody.innerHTML = '<tr><td colspan="8" class="md-empty">Native mode required.</td></tr>';
        return;
      }
      // Live "Loading channels..." with elapsed counter so the table
      // doesn't sit silent during a slow DB query (sometimes the
      // index DB lock is held by an in-flight sweep / ingest, in
      // which case this call blocks until they finish).
      const _t0 = Date.now();
      tbody.innerHTML = '<tr><td colspan="8" class="md-empty">'
        + 'Loading channels\u2026 <span id="md-load-info" class="md-load-info"></span>'
        + '</td></tr>';
      const _info = document.getElementById("md-load-info");
      const _paint = () => {
        if (!_info || !_info.isConnected) return;
        const el = (Date.now() - _t0) / 1000;
        let msg = `(${_fmtElapsed(el)})`;
        // After ~10s the call is unusually slow. Tell the user why
        // so they don't think the app is hung. After ~30s, escalate
        // — the DB is almost certainly contending with another op
        // (most often a startup-time backfill / sweep / preload that
        // hasn't finished yet on a large archive).
        if (el > 30) {
          msg += " \u00b7 still waiting on the index DB \u2014 a startup "
                + "task (backfill / sweep / preload) is likely holding "
                + "the lock; this clears once the green "
                + "\"Browse preload complete\" indicator appears";
        } else if (el > 10) {
          msg += " \u00b7 querying the index DB\u2026";
        }
        _info.textContent = msg;
      };
      _paint();
      const _ticker = setInterval(_paint, 1000);
      // Mark thumbnails as not-yet-loaded so the column renders a
      // spinner until the bulk walk completes.
      _thumbsLoaded = false;
      try {
        const rows = await api.get_channel_metadata_status();
        _rows = Array.isArray(rows) ? rows : [];
        // Issue #154 (fix): thumbnail_status_bulk walks every channel
        // folder on disk (~100k probes on a 100-channel archive on a
        // network-pooled drive — multi-minute). DO NOT await it here.
        // Render the table immediately with a spinner in the Thumbnails
        // column, then kick the bulk walk in the background and patch
        // the column when it returns.
        if (api?.thumbnail_status_bulk) {
          api.thumbnail_status_bulk().then((thRes) => {
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
            _thumbsLoaded = true;
            // Re-render once thumb data is in. Cheap — same rows,
            // just rebuilt with the merged values.
            try { render(); } catch {}
          }).catch(() => { _thumbsLoaded = true; try { render(); } catch {} });
        } else {
          // No native API → no walk is going to happen. Don't sit on
          // a spinner forever; flip to loaded so the column shows "—".
          _thumbsLoaded = true;
        }
      } catch (e) {
        console.error("get_channel_metadata_status:", e);
        clearInterval(_ticker);
        tbody.innerHTML = `<tr><td colspan="8" class="md-empty">`
          + `Failed to load: ${escapeHtml(String(e))}</td></tr>`;
        return;
      }
      clearInterval(_ticker);
      render();
    };

    // Column header click → cycle sort. Same column toggles dir;
    // new column resets to a sensible default (asc for names, desc
    // for numbers + "refresh" columns where "most recent first"
    // reads more naturally).
    table.querySelectorAll("thead th[data-sort]").forEach(th => {
      th.addEventListener("click", () => {
        const key = th.dataset.sort;
        if (key === _sortKey) {
          _sortDir = _sortDir === "asc" ? "desc" : "asc";
        } else {
          _sortKey = key;
          _sortDir = (key === "name") ? "asc" : "desc";
        }
        render();
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
    const _pausedTail = (res) => res?.paused ? " Queue is paused — resume to start." : "";

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
        + "Fast: match by title + duration only. Works great for "
        + "channels with stable titles and varied durations. "
        + "Doesn't help when YouTube renamed lots of titles AND "
        + "many videos share a duration.\n\n"
        + "Thorough: also re-fetches the upload date for every "
        + "candidate video so date can disambiguate renamed titles. "
        + "Hits YouTube hard — one request per candidate (~3s each, "
        + "parallelized 4-wide). Use when Fast left a lot unresolved.";
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
    const _runRowAct = async (act, ident, rowRefs) => {
      const api = getApi();
      if (!api) return;
      if (act === "views") {
        if (!api.metadata_refresh_views_channel) {
          window._showToast?.("Native mode required.", "warn"); return;
        }
        try {
          const res = await api.metadata_refresh_views_channel(ident);
          if (!res?.ok) {
            window._showToast?.(res?.error || "Failed.", "error");
          } else {
            window._showToast?.("Queued views/likes refresh." + _pausedTail(res),
              res?.paused ? "warn" : "ok");
          }
        } catch (err) {
          window._showToast?.(String(err), "error");
        }
      } else if (act === "backfill") {
        if (!api.metadata_backfill_ids_channel) {
          window._showToast?.("Native mode required.", "warn"); return;
        }
        // Look up this channel's video count from the cached _rows
        // so the dialog can show real time estimates.
        let _vc = 0;
        try {
          const _r = _rows.find(r => r.folder === ident.folder
                                   || r.url === ident.url);
          _vc = _r?.video_count || 0;
        } catch {}
        const _mode = await _promptBackfillMode(
          _vc, `Channel: ${ident.folder || ident.url || ""}`);
        if (!_mode) return; // cancelled
        try {
          const res = await api.metadata_backfill_ids_channel(ident, _mode);
          if (!res?.ok) {
            window._showToast?.(res?.error || "Failed.", "error");
          } else {
            const head = _mode === "thorough"
              ? "Queued video_id backfill (thorough)."
              : "Queued video_id backfill (fast).";
            window._showToast?.(head + _pausedTail(res),
              res?.paused ? "warn" : "ok");
          }
        } catch (err) {
          window._showToast?.(String(err), "error");
        }
      } else if (act === "thumbs") {
        // refetch missing thumbnails for one channel.
        if (!api.refetch_thumbnails) {
          window._showToast?.("Native mode required.", "warn"); return;
        }
        try {
          const res = await api.refetch_thumbnails(ident);
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
      } else if (act === "comments") {
        if (!api.metadata_refresh_comments_channel) {
          window._showToast?.("Native mode required.", "warn"); return;
        }
        // Caller passes the time-scope directly as the `arg` parameter
        // (set by the context-menu submenu's data-days attribute).
        // null = all videos. Numeric = N-day window.
        const pickN = parseInt(rowRefs, 10);
        const days = (Number.isFinite(pickN) && pickN > 0) ? pickN : null;
        try {
          const res = await api.metadata_refresh_comments_channel(ident, days);
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
    const _closeRowMenu = () => {
      if (_activeMenu) {
        try { _activeMenu.remove(); } catch {}
        _activeMenu = null;
      }
    };
    const _openRowMenu = (tr, clientX, clientY) => {
      _closeRowMenu();
      let ident = {};
      try { ident = JSON.parse(tr.dataset.identity || "{}"); } catch {}
      const needsFix = tr.classList.contains("md-row-needs-fix");
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
        b.dataset.act = act;
        if (opts.days !== undefined) b.dataset.days = String(opts.days);
        b.textContent = label;
        return b;
      };
      const fixItem = mkItem("Fix missing video IDs", "backfill",
                             { warn: needsFix });
      const viewsItem = mkItem("Refresh views/likes", "views");
      const thumbsItem = mkItem("Refetch missing thumbnails", "thumbs");
      // "Refresh comments" carries a hover-submenu with day-scope picks.
      const commentsWrap = document.createElement("div");
      commentsWrap.className = "md-cm-sub-wrap";
      const commentsHead = document.createElement("button");
      commentsHead.type = "button";
      commentsHead.className = "md-cm-item md-cm-has-sub";
      commentsHead.innerHTML = "Refresh comments<span class=\"md-cm-chev\">\u203a</span>";
      const commentsSub = document.createElement("div");
      commentsSub.className = "md-cm-sub";
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
        menu.appendChild(viewsItem);
      } else {
        menu.appendChild(viewsItem);
        menu.appendChild(fixItem);
      }
      menu.appendChild(commentsWrap);
      menu.appendChild(thumbsItem);

      // Leaf-item click → close + dispatch. Submenu header is hover-only.
      menu.addEventListener("click", async (ev) => {
        const btn = ev.target.closest(".md-cm-item");
        if (!btn) return;
        if (btn.classList.contains("md-cm-has-sub")) return;
        const act = btn.dataset.act;
        const days = btn.dataset.days; // string or undefined
        _closeRowMenu();
        await _runRowAct(act, ident, days);
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
        const first = menu.querySelector(".md-cm-item:not(.md-cm-has-sub)");
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
        _closeRowMenu();
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
        if (sel && sel.toString().length > 0) return;
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

    // Bulk buttons.
    if (bAllViews) {
      bAllViews.addEventListener("click", async () => {
        const api = getApi();
        if (!api?.metadata_queue_all) {
          window._showToast?.("Native mode required.", "warn"); return;
        }
        const ok = await (window.askChoice ? window.askChoice({
          title: "Refresh all views/likes?",
          message: "This will queue a views/likes refresh for every saved channel. Uses the fast bulk path — most channels finish in under a minute.",
          choices: [{ label: "Queue all", value: "go", kind: "primary" }],
        }) : Promise.resolve("go"));
        // askChoice resolves to a string from dataset.value, or null on
        // cancel. Any non-null string means the user clicked the
        // primary button, so proceed.
        if (!ok) return;
        try {
          const res = await api.metadata_queue_all(true);
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
    if (bAllComments) {
      bAllComments.addEventListener("click", async () => {
        const api = getApi();
        if (!api?.metadata_refresh_comments_all) {
          window._showToast?.("Native mode required.", "warn"); return;
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
          const res = await api.metadata_refresh_comments_all(days);
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
        const api = getApi();
        if (!api?.metadata_backfill_ids_all) {
          window._showToast?.("Native mode required.", "warn"); return;
        }
        // Show how many channels would be queued BEFORE asking so the
        // user knows whether this is a big operation or a tiny no-op.
        // We don't have a dedicated preview method — just show the
        // aggregate count we're holding from the last tab render.
        const needing = _rows.filter(r => (r.id_total || 0) > 0 && (r.id_missing || 0) > 0).length;
        if (needing === 0) {
          window._showToast?.("All channels already have video_ids \u2014 nothing to do.", "ok");
          return;
        }
        const pick = await (window.askChoice ? window.askChoice({
          title: "Fix missing video IDs?",
          message: `Will queue ${needing} channel(s) that currently have missing video_ids. Next step asks which match mode to use.`,
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
          const res = await api.metadata_backfill_ids_all(onlyMissing, _mode);
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
        const api = getApi();
        if (!api?.refetch_thumbnails_all) {
          window._showToast?.("Native mode required.", "warn"); return;
        }
        const nMissing = _rows.reduce(
          (sum, r) => sum + Math.max(
            0, (r.thumb_total || 0) - (r.thumb_with || 0)),
          0);
        const nChannels = _rows.filter(r =>
          (r.thumb_total || 0) > (r.thumb_with || 0)).length;
        if (nMissing === 0) {
          window._showToast?.(
            "Every channel already has all its thumbnails.", "ok");
          return;
        }
        const ok = await (window.askChoice ? window.askChoice({
          title: "Refetch missing thumbnails for all channels?",
          message: `${nMissing.toLocaleString()} thumbnail(s) missing `
            + `across ${nChannels} channel(s). Each missing thumbnail `
            + `is downloaded from the URL cached in metadata.jsonl. `
            + `Runs in the background; progress streams to the log. `
            + `(Some thumbnails may be unrecoverable if the source URL `
            + `404s or the video was removed from YouTube.)`,
          choices: [
            { label: "Start", value: "go", kind: "primary" },
          ],
        }) : Promise.resolve("go"));
        if (!ok) return;
        try {
          const res = await api.refetch_thumbnails_all();
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
        window._refreshMetadataTab?.();
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
        const api = getApi();
        if (!api?.thumbnail_status_bulk || !api?.get_channel_metadata_status) {
          window._showToast?.("Native mode required.", "warn"); return;
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
        _thumbsLoaded = false;
        try { render(); } catch {}
        const _t0 = Date.now();
        const _tick = setInterval(() => {
          if (!recheckProgress || recheckProgress.hidden) return;
          const sec = Math.floor((Date.now() - _t0) / 1000);
          recheckProgress.textContent = ` · rechecking ${sec}s…`;
        }, 1000);
        try {
          // Fire both in parallel — they read separate sources (DB
          // vs filesystem) so they don't contend.
          const [metaRows, thRes] = await Promise.all([
            api.get_channel_metadata_status(true),
            api.thumbnail_status_bulk(true),
          ]);
          _rows = Array.isArray(metaRows) ? metaRows : [];
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
          _thumbsLoaded = true;
          try { render(); } catch {}
          const ch = Object.keys(thMap).length;
          window._showToast?.(
            `Rechecked ${ch} channel(s).`, "ok");
        } catch (e) {
          // Stuck spinner is worse than a dash — flip back on error.
          _thumbsLoaded = true;
          try { render(); } catch {}
          window._showToast?.(String(e), "error");
        } finally {
          clearInterval(_tick);
          bRecheckThumbs.disabled = false;
          if (recheckProgress) {
            recheckProgress.hidden = true;
            recheckProgress.textContent = "";
          }
        }
      });
    }

    // If the user's initial panel is the Metadata tab (e.g. after a
    // restart where it was remembered as active), pull data now.
    const metaView = document.getElementById("settings-view-metadata");
    if (metaView && metaView.style.display !== "none") {
      setTimeout(() => window._refreshMetadataTab?.(), 400);
    }
  }
  window._initMetadataTab = initMetadataTab;

  window.initMetadataTab = initMetadataTab;
})();
