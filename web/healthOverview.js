/**
 * Health > Overview
 *
 * Builds a small live dashboard from existing read-only bridge methods. It
 * never starts a scan, queues work, saves settings, or fills unavailable data
 * with made-up zeroes. Each card links to the Health section that owns the
 * underlying detail or repair action.
 */
(function () {
  "use strict";

  const $ = (id) => document.getElementById(id);
  const CARD_IDS = {
    archive: ["health-overview-archive-value", "health-overview-archive-detail"],
    metadata: ["health-overview-metadata-value", "health-overview-metadata-detail"],
    index: ["health-overview-index-value", "health-overview-index-detail"],
    transcripts: ["health-overview-transcripts-value", "health-overview-transcripts-detail"],
    backup: ["health-overview-backup-value", "health-overview-backup-detail"],
    system: ["health-overview-system-value", "health-overview-system-detail"],
  };

  let refreshGeneration = 0;
  let initialized = false;
  let refreshInFlight = null;

  function bridgeCall(method, ...args) {
    return window.YT?.bridge?.bridgeCall?.(method, ...args);
  }

  function nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
  }

  function numberOrNull(value) {
    if (value === null || value === undefined || value === "") return null;
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  function formatCount(value) {
    return Math.max(0, Math.trunc(value)).toLocaleString();
  }

  function relativeTime(timestamp) {
    const ts = numberOrNull(timestamp);
    if (ts === null || ts <= 0) return null;
    const seconds = Math.max(0, Math.floor(Date.now() / 1000 - ts));
    if (seconds < 60) return "just now";
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
    if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
    const days = Math.floor(seconds / 86400);
    if (days < 365) return `${days}d ago`;
    return `${Math.floor(days / 365)}y ago`;
  }

  function cardFor(key) {
    const value = $(CARD_IDS[key]?.[0]);
    return value?.closest(".health-summary-card") || null;
  }

  function paintCard(key, value, detail, tone = "") {
    const ids = CARD_IDS[key];
    if (!ids) return;
    const valueEl = $(ids[0]);
    const detailEl = $(ids[1]);
    if (valueEl) valueEl.textContent = value;
    if (detailEl) detailEl.textContent = detail;
    const card = cardFor(key);
    if (card) {
      card.classList.remove("is-warn", "is-bad", "is-unavailable");
      if (tone) card.classList.add(`is-${tone}`);
    }
  }

  function paintUnavailable(key, label) {
    paintCard(key, "Unavailable", `${label} could not be read`, "unavailable");
  }

  function setText(id, value) {
    const element = $(id);
    if (element) element.textContent = value;
  }

  function backupAgeLabel(timestamp) {
    const ts = numberOrNull(timestamp);
    if (ts === null || ts <= 0) return null;
    const days = Math.max(0, Math.floor((Date.now() / 1000 - ts) / 86400));
    return {
      days,
      short: relativeTime(ts),
      long: days === 0 ? "today" : days === 1 ? "yesterday" : `${days} days ago`,
    };
  }

  function resetCards() {
    paintCard("archive", "Checking…", "Reading the library summary");
    paintCard("metadata", "Checking…", "Checking channel details");
    paintCard("index", "Checking…", "Reading searchable transcript totals");
    paintCard("transcripts", "Checking…", "Checking transcript coverage");
    paintCard("backup", "Checking…", "Reading the last successful backup time");
    paintCard("system", "Checking…", "Checking yt-dlp");
  }

  function navigateTo(target, requestedAnchor) {
    // Overview cards describe checks more precisely than the condensed
    // navigation. Map each check to its section on the Library page, or to
    // the related app preference when the control no longer lives in Health.
    const destinations = {
      "archive-files": { view: "library", anchor: "health-library-archive" },
      metadata: { view: "library", anchor: "health-library-metadata" },
      index: { view: "library", anchor: "health-library-index" },
      transcripts: { view: "library", anchor: "health-library-transcripts" },
      "backup-restore": { view: "backups" },
      backups: { view: "backups" },
      system: { tab: "settings", anchor: "settings-downloader-updates" },
      settings: { tab: "settings", anchor: "settings-downloader-updates" },
      library: { view: "library" },
      overview: { view: "overview" },
    };
    const destination = destinations[target] || { view: target };
    const anchor = requestedAnchor || destination.anchor;

    if (destination.tab === "settings") {
      document.querySelector('.tab[data-tab="settings"]')?.click();
    } else {
      document.querySelector(
        `#panel-health .settings-subnav-btn[data-settings-view="${destination.view}"]`,
      )?.click();
    }

    requestAnimationFrame(() => {
      const element = anchor ? document.getElementById(anchor) : null;
      if (element) {
        const disclosure = element.matches("details")
          ? element : element.closest("details");
        if (disclosure) disclosure.open = true;
        element.scrollIntoView({ block: "start" });
      } else {
        const panel = destination.tab === "settings"
          ? "#panel-settings" : "#panel-health";
        document.querySelector(`${panel} .settings-main`)?.scrollTo(0, 0);
      }
    });
  }

  function renderAttention(items, unavailableCount) {
    const list = $("health-attention-list");
    if (!list) return;
    list.replaceChildren();
    if (!items.length) {
      const empty = document.createElement("div");
      empty.className = "health-attention-empty";
      if (!unavailableCount) empty.classList.add("is-ok");
      empty.textContent = unavailableCount
        ? "No confirmed issues in the checks that finished."
        : "Nothing obvious needs attention in these checks.";
      list.appendChild(empty);
      return;
    }
    for (const item of items) {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "health-attention-item";
      button.textContent = item.text;
      button.addEventListener("click", () => navigateTo(item.target));
      list.appendChild(button);
    }
  }

  async function readOnly(method, {
    acceptErrorResult = false,
    args = [],
  } = {}) {
    try {
      const value = await bridgeCall(method, ...args);
      if (value === undefined || value === null) {
        throw new Error("No data returned");
      }
      if (!acceptErrorResult && value && typeof value === "object" && value.error) {
        throw new Error(String(value.error));
      }
      return { ok: true, value };
    } catch (error) {
      return { ok: false, error };
    }
  }

  async function refreshOverview() {
    if (refreshInFlight) return refreshInFlight;
    const generation = ++refreshGeneration;
    const status = $("health-overview-status");
    const attentionList = $("health-attention-list");
    resetCards();
    if (status) {
      status.textContent = "Checking current status…";
      status.classList.remove("is-warn");
    }
    if (attentionList) {
      attentionList.innerHTML = '<div class="health-attention-empty">Checking…</div>';
    }

    if (!nativeBridgeUp()) {
      for (const [key, label] of [
        ["archive", "Archive summary"],
        ["metadata", "Metadata status"],
        ["index", "Search index status"],
        ["transcripts", "Transcript coverage"],
        ["backup", "Backup status"],
        ["system", "yt-dlp status"],
      ]) paintUnavailable(key, label);
      setText("backup-age-display", "Backup status unavailable");
      setText("settings-ytdlp-version", "check unavailable");
      if (status) {
        status.textContent = "YTArchiver is not ready yet. Try Refresh in a moment.";
        status.classList.add("is-warn");
      }
      renderAttention([], 6);
      return;
    }

    refreshInFlight = (async () => {
      // All five methods are existing read-only status calls. Do not add a
      // maintenance/start/save endpoint to this list.
      const [settings, archive, index, metadata, ytdlp] = await Promise.all([
        readOnly("settings_load"),
        readOnly("get_index_summary"),
        readOnly("index_summary", { args: [true] }),
        readOnly("get_channel_metadata_status", { args: [false, true] }),
        readOnly("ytdlp_version", { acceptErrorResult: true }),
      ]);
      if (generation !== refreshGeneration) return;

      const attention = [];
      const unavailable = [];

      // Archive files — fast disk-cache summary from get_index_summary.
      if (archive.ok && archive.value && typeof archive.value === "object") {
        const cards = (archive.value.cards && typeof archive.value.cards === "object")
          ? archive.value.cards : archive.value;
        const videos = numberOrNull(cards.videos ?? archive.value.total_videos);
        const channels = numberOrNull(cards.channels);
        const physical = numberOrNull(cards.physical_copies);
        const sizeLabel = typeof cards.size_label === "string"
          ? cards.size_label.trim() : "";
        if (videos !== null || channels !== null || physical !== null || sizeLabel) {
          const primary = videos !== null
            ? `${formatCount(videos)} video${videos === 1 ? "" : "s"}`
            : channels !== null
              ? `${formatCount(channels)} channel${channels === 1 ? "" : "s"}`
              : sizeLabel;
          const parts = [];
          if (channels !== null && videos !== null) {
            parts.push(`${formatCount(channels)} channel${channels === 1 ? "" : "s"}`);
          }
          if (physical !== null) parts.push(`${formatCount(physical)} files on disk`);
          if (sizeLabel) parts.push(sizeLabel);
          paintCard("archive", primary, parts.join(" · ") || "Library summary available");
        } else {
          unavailable.push("archive summary");
          paintUnavailable("archive", "Archive summary");
        }
      } else {
        unavailable.push("archive summary");
        paintUnavailable("archive", "Archive summary");
      }

      // Metadata + transcript coverage — one shared per-channel snapshot.
      if (metadata.ok && Array.isArray(metadata.value)) {
        const rows = metadata.value;
        if (!rows.length) {
          paintCard("metadata", "No subscribed channels", "Nothing to refresh");
          paintCard("transcripts", "No subscribed channels", "No transcript coverage to report");
        } else {
          let missingIds = 0;
          let sawIdCounts = false;
          let staleViews = 0;
          let txTotal = 0;
          let txWith = 0;
          let sawTranscriptCounts = false;
          const ninetyDays = 90 * 86400;
          const now = Date.now() / 1000;
          for (const row of rows) {
            const missing = numberOrNull(row?.id_missing);
            if (missing !== null) {
              sawIdCounts = true;
              missingIds += Math.max(0, missing);
            }
            const viewsTs = numberOrNull(row?.last_views_refresh_ts) ?? 0;
            if (viewsTs <= 0 || now - viewsTs >= ninetyDays) staleViews += 1;
            const total = numberOrNull(row?.tx_total);
            const transcribed = numberOrNull(row?.tx_transcribed);
            if (total !== null && transcribed !== null) {
              sawTranscriptCounts = true;
              txTotal += Math.max(0, total);
              txWith += Math.max(0, Math.min(total, transcribed));
            }
          }

          if (sawIdCounts) {
            paintCard(
              "metadata",
              missingIds > 0
                ? `${formatCount(missingIds)} missing video ID${missingIds === 1 ? "" : "s"}`
                : "Video IDs look complete",
              staleViews > 0
                ? `${formatCount(staleViews)} channel${staleViews === 1 ? "" : "s"} never refreshed or 90+ days old`
                : "Every channel was refreshed within 90 days",
              missingIds > 0 || staleViews > 0 ? "warn" : "",
            );
          } else {
            paintUnavailable("metadata", "Metadata coverage");
            unavailable.push("metadata coverage");
          }
          if (missingIds > 0) {
            attention.push({
              target: "metadata",
              text: `${formatCount(missingIds)} tracked video${missingIds === 1 ? " is" : "s are"} missing a video ID.`,
            });
          }
          if (staleViews > 0) {
            attention.push({
              target: "metadata",
              text: `${formatCount(staleViews)} channel${staleViews === 1 ? " has" : "s have"} never been refreshed or were last refreshed 90+ days ago.`,
            });
          }

          if (sawTranscriptCounts) {
            const gap = Math.max(0, txTotal - txWith);
            paintCard(
              "transcripts",
              `${formatCount(txWith)} / ${formatCount(txTotal)} transcribed`,
              gap > 0
                ? `${formatCount(gap)} video${gap === 1 ? " is" : "s are"} not marked transcribed`
                : "All tracked videos are marked transcribed",
            );
          } else {
            paintUnavailable("transcripts", "Transcript coverage");
            unavailable.push("transcript coverage");
          }
        }
      } else {
        unavailable.push("metadata status", "transcript coverage");
        paintUnavailable("metadata", "Metadata status");
        paintUnavailable("transcripts", "Transcript coverage");
      }

      // Search-index totals — direct read-only catalog summary.
      if (index.ok && index.value && typeof index.value === "object") {
        const videos = numberOrNull(index.value.videos);
        const segments = numberOrNull(index.value.segments);
        const channels = numberOrNull(index.value.channels);
        if (videos !== null || segments !== null || channels !== null) {
          const primary = videos !== null
            ? `${formatCount(videos)} video${videos === 1 ? "" : "s"} indexed`
            : segments !== null
              ? `${formatCount(segments)} transcript segments`
              : `${formatCount(channels)} indexed channel${channels === 1 ? "" : "s"}`;
          const parts = [];
          if (segments !== null) parts.push(`${formatCount(segments)} transcript segments`);
          if (channels !== null) parts.push(`${formatCount(channels)} channels`);
          paintCard("index", primary, parts.join(" · ") || "Index summary available");
        } else {
          unavailable.push("search-index status");
          paintUnavailable("index", "Search index status");
        }
      } else {
        unavailable.push("search-index status");
        paintUnavailable("index", "Search index status");
      }

      // Backup status comes from settings_load.
      if (settings.ok && settings.value && typeof settings.value === "object") {
        const hasBackupField = Object.prototype.hasOwnProperty.call(
          settings.value, "last_backup_ts");
        if (hasBackupField) {
          const backupTs = numberOrNull(settings.value.last_backup_ts) ?? 0;
          const age = backupAgeLabel(backupTs);
          if (age) {
            const needsBackup = age.days >= 14;
            paintCard(
              "backup",
              `Last backup ${age.short}`,
              needsBackup ? "Consider creating a newer full app backup" : "Full app backup recorded",
              needsBackup ? "warn" : "",
            );
            setText(
              "backup-age-display",
              `${needsBackup ? "⚠ " : ""}Last backup: ${age.long}` +
                (needsBackup ? " — consider exporting soon" : ""),
            );
            if (needsBackup) {
              attention.push({
                target: "backup-restore",
                text: `The last full app backup was ${age.long}.`,
              });
            }
          } else {
            paintCard("backup", "No backup recorded", "Create a full app backup", "warn");
            setText("backup-age-display", "Last backup: never");
            attention.push({
              target: "backup-restore",
              text: "No successful full app backup is recorded.",
            });
          }
        } else {
          unavailable.push("backup status");
          paintUnavailable("backup", "Backup status");
          setText("backup-age-display", "Backup status unavailable");
        }
      } else {
        unavailable.push("backup status");
        paintUnavailable("backup", "Backup status");
        setText("backup-age-display", "Backup status unavailable");
      }

      // yt-dlp — version probe only; Update remains an explicit user action.
      if (ytdlp.ok && ytdlp.value && typeof ytdlp.value === "object") {
        const version = typeof ytdlp.value.version === "string"
          ? ytdlp.value.version.trim() : "";
        if (ytdlp.value.ok === true && version) {
          paintCard("system", `yt-dlp ${version}`, "YouTube download tool available");
          setText("settings-ytdlp-version", version);
        } else if (ytdlp.value.ok === false) {
          const detail = String(ytdlp.value.error || "yt-dlp is not available");
          paintCard("system", "yt-dlp needs attention", detail, "bad");
          setText("settings-ytdlp-version", detail);
          attention.push({ target: "system", text: "yt-dlp is not available." });
        } else {
          unavailable.push("yt-dlp status");
          paintUnavailable("system", "yt-dlp status");
          setText("settings-ytdlp-version", "check failed");
        }
      } else {
        unavailable.push("yt-dlp status");
        paintUnavailable("system", "yt-dlp status");
        setText("settings-ytdlp-version", "check failed");
      }

      const uniqueUnavailable = [...new Set(unavailable)];
      renderAttention(attention, uniqueUnavailable.length);
      if (status) {
        if (uniqueUnavailable.length) {
          status.textContent = `Checked what was available. Could not read: ${uniqueUnavailable.join(", ")}.`;
          status.classList.add("is-warn");
        } else {
          status.textContent = "All overview checks finished.";
          status.classList.remove("is-warn");
        }
      }
    })();

    try {
      await refreshInFlight;
    } finally {
      refreshInFlight = null;
    }
  }

  function initHealthOverview() {
    if (initialized || !$("settings-view-overview")) return;
    initialized = true;
    $("btn-health-overview-refresh")?.addEventListener("click", refreshOverview);
    document.querySelectorAll("#panel-health [data-health-target]").forEach((card) => {
      card.addEventListener("click", () => navigateTo(
        card.dataset.healthTarget, card.dataset.healthAnchor));
    });
    document.querySelector(
      '#panel-health .settings-subnav-btn[data-settings-view="overview"]')
      ?.addEventListener("click", refreshOverview);
    document.querySelector('.tab[data-tab="health"]')?.addEventListener("click", () => {
      setTimeout(() => {
        if (!$("settings-view-overview")?.hidden) refreshOverview();
      }, 0);
    });
    window.YT?.bridge?.ready?.then(() => {
      const panel = $("panel-health");
      if (panel?.classList.contains("active")
          && !$("settings-view-overview")?.hidden) refreshOverview();
    });
  }

  window.initHealthOverview = initHealthOverview;
  window._refreshHealthOverview = refreshOverview;
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initHealthOverview, { once: true });
  } else {
    queueMicrotask(initHealthOverview);
  }
})();
