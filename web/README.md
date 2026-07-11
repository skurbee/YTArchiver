# web/ — pywebview frontend

The HTML / CSS / JS that pywebview renders inside the YTArchiver window.

## File layout

The frontend is split into ~60 focused modules. Each module is wrapped in
its own IIFE and publishes a handful of `window.<name>` entry points; the
modules are loaded in dependency order by `index.html`. `app.js` is a
small boot orchestrator that calls each module's `init*` function.

### HTML

| File | Purpose |
|------|---------|
| `index.html`          | **Build artifact** — assembled at boot from the template + partials. Don't edit this directly; edit the source files below. |
| `index.template.html` | Shell with `<!-- @include partials/foo.html -->` markers. |
| `partials/tab-download.html`, `tab-subs.html`, `tab-browse.html`, `tab-health.html`, `tab-settings.html`, `onboarding.html` | Per-tab and onboarding markup. |
| `partials/popovers.html`, `dialogs.html`, `modals.html` | Floating overlays. |

The assembler is `backend/html_assembler.py`; it runs at `main.py`
startup and only rewrites `index.html` when generated content changes.

### CSS (loaded in cascade order)

| File | Purpose |
|------|---------|
| `styles.css`                | `:root` design-token vars + base, header, tab row, tab panels. **Must load first.** |
| `styles-settings.css`       | Settings page |
| `styles-download-controls.css` | Download tab toolbar |
| `styles-logs.css`           | Activity log + main log + tag classes |
| `styles-tabs-data.css`      | Subs table, data panels, queue popovers |
| `styles-browse.css`         | Browse tab framing + sub-modes + Index panel |
| `styles-browse-grids.css`   | Channel + Video grids |
| `styles-watch.css`          | Watch view + captions + metadata drawer |
| `styles-dialogs.css`        | Dark dialogs + toasts + redownload modal |

### JS — boot + rendering core

| File | Purpose |
|------|---------|
| `app.js`         | Tiny boot orchestrator — calls every module's `init*`. Owns the MutationObserver-cleanup pool. |
| `logs.js`        | Log infrastructure (`_logBatch`, scroll state, in-place row replace, mini-log mirror). |
| `watchView.js`   | Watch view + karaoke transcript + WebVTT captions + metadata drawer. |
| `browseGrids.js` | Channel grid + Video grid + `_buildVideoCard` (shared by Videos/Manual cards). |
| `tables.js`      | Subs channel table. |
| `queueRender.js` | Sync / GPU task popover row renderer (drag-reorder, right-click). |
| `metadataTab.js` | Settings → Metadata refresh-status table. |

### JS — foundation (load first)

| File | Purpose |
|------|---------|
| `util.js`        | Tiny shared utilities (`escapeHtml`, `escapeAttr`, `_formatTs`, `onceIdempotent`). |
| `bridge.js`      | `pywebview.api` readiness helper + `bridgeCall` plumbing. |
| `browseState.js` | Canonical `window._browseState` (loaded early so every extracted module captures the same object reference). |
| `toasts.js`      | Toast notifications + error-message sanitizer. |
| `modals.js`      | `askQuestion / askConfirm / askChoice / askDanger / askTextInput`. |
| `contextMenu.js` | Generic right-click menu builder. |
| `dropdown.js`    | Custom `<select>` replacement for Settings. |

### JS — shell + global wirings

| File | Purpose |
|------|---------|
| `chrome.js`      | Header version label + tab switching + paned-splitter drag. |
| `shortcuts.js`   | Global keyboard shortcuts (Ctrl+S, F11, 1-5, Escape, etc). |
| `queueBlink.js`  | Header Pause/Resume button + queue badge state machine. |
| `logContextMenu.js` | Right-click menu on every log surface (Copy, Save, Clear). |
| `uxPolish.js`    | Custom tooltip system + global click-defocus. |
| `smallInits.js`  | `initLastSyncTicker`, `initSubsFilter`, splitter persistence, archive-rescan push handler. |

### JS — per-tab modules

| File | Tab / area | Purpose |
|------|------------|---------|
| `downloadUrl.js`        | Download | Single-video URL input + Download button. |
| `downloadDragDrop.js`   | Download | Drag-and-drop a YouTube URL onto the tab. |
| `clearButton.js`        | Download | Consolidated Clear ▾ dropdown. |
| `editChannel.js`        | Subs     | Inline edit-channel panel (incl. ↻ Reset, Recheck, Continue Redownload). |
| `syncSubbed.js`         | header   | Sync Subbed button + pause/resume state. |
| `autoSync.js`           | header   | Auto-sync interval dropdown + countdown. |
| `liveDrawer.js`         | header   | Deferred-livestreams drawer. |
| `columnSort.js`         | Subs     | Clickable column-header sort. |
| `columnWidth.js`        | Subs     | Drag-to-resize column widths, persisted. |
| `browseView.js`         | Browse   | Sub-mode toggle + channel→video→watch flow + `filterCurrentView`. |
| `browseContent.js`      | Browse   | Video grid loader, search-result renderer, Whisper picker, `initQueueAutoCheckboxes`. |
| `browseSearch.js`       | Browse   | Search input + result list + viewer pane + sort dropdown + un-indexed banner. |
| `browseContextMenus.js` | Browse   | Right-click menus on channel and video cards. |
| `videosView.js`         | Browse   | Archive-wide Videos grid with lazy loading, sorting, and title/channel filtering. |
| `bookmarks.js`          | Browse   | Bookmarks sub-mode + week summary + redownload prompt. |
| `watchActions.js`       | Browse → Watch | Every interactive control on the watch view (incl. retranscribe). |
| `graphTab.js`           | Browse → Graph | Word-frequency charts + word cloud via Chart.js. |
| `settingsTab.js`        | Settings | Main settings form (archive paths, yt-dlp updater, backup export/import). |
| `settingsInfra.js`      | Settings | Sub-tab nav + Archive Roots panel. |
| `indexControls.js`      | Settings | Index sub-tab (Build / Rebuild / Stats). |
| `aboutDialog.js`        | Settings | About modal. |
| `diagnosticsDialog.js`  | Settings | Diagnostics modal. |
| `manualTranscribe.js`   | Settings | Manual Transcribe — pick a file, queue with Whisper. |
| `driftScanDialog.js`    | Settings | Transcript drift scan + fix modal. |
| `compressDryRunDialog.js` | Settings | Compress dry-run results modal. |
| `repairCaptionsDialog.js` | Settings | Repair YT auto-captions modal. |
| `punctRestoreDialog.js` | Settings | Restore transcript punctuation modal. |
| `provenanceDialog.js`   | Health   | Embed file tags modal (MP4 provenance + txt header IDs). |
| `queuePopovers.js`      | header   | Sync Tasks + GPU Tasks popover open/close. |
| `queuePending.js`       | Subs     | "Queue Pending" header button + badge count. |
| `refreshSizes.js`       | Subs     | Click-to-rescan on the Total: N TB label. |
| `removeChannel.js`      | anywhere | Shared two-step "Remove channel + optionally delete files" flow. |
| `autorunHistory.js`     | Settings | Autorun history full-view dialog. |
| `logMode.js`            | header   | Log mode dropdown (Simple / Verbose). |
| `scanArchive.js`        | Settings | Rescan archive button. |
| `activityLogVis.js`     | header   | Auto-hide the activity-log frame when empty. |
| `seedLogs.js`           | boot     | Initial log + queue + activity-log seed from the Python bridge. |
| `missingFolders.js`     | boot     | Reconcile flow when a channel folder is missing on disk. |
| `appDialogs.js`         | shared   | App-level dialogs (Metadata Already Downloaded, Close Confirm, Bookmark kind). |
| `redownloadSampleModal.js` | shared | "Redownload 10-sample" confirm modal. |

### Vendor

| File | Purpose |
|------|---------|
| `vendor/chart.umd.min.js` | Chart.js for Browse → Graph. |

## How pywebview wires Python ↔ JS

**JS → Python:** every `await window.pywebview.api.<method>(...)` call lands
in the `Api` class in `main.py`. The method name must match a method on the
Api class (or one of the mixins in `backend/api_mixins/`).

**Python → JS:** Python evaluates JS in the window via
`self._window.evaluate_js("window.<funcName>(<args>)")`. The frontend exposes
named globals via `window.<name> = ...` at the bottom of each module's IIFE.

**Log push:** the backend `LogStreamer` batches log lines and pushes them
every ~60ms via `window._logBatch(payload)` (defined in `logs.js`).

## Adding a UI feature

1. **HTML structure**: edit the relevant `partials/*.html` (or
   `index.template.html` for the outer shell). Use existing class
   conventions (`*-panel`, `*-table`, `*-grid`, `*-modal`).

2. **CSS**: add to the themed sheet that matches your feature
   (`styles-browse.css` for Browse-tab styling, etc.). Use the design
   tokens (`var(--c-bg)`, `var(--c-text)`, etc.) defined in `styles.css`.

3. **JS**: create a new module if the feature is a coherent unit, or
   add to an existing per-tab module. The pattern is:
   ```js
   (function () {
     "use strict";
     function initMyFeature() { /* ... */ }
     window.initMyFeature = initMyFeature;
   })();
   ```
   Then add `_safe("initMyFeature", () => window.initMyFeature?.());`
   to `app.js`'s `boot()`. Add a `<script>` tag at the bottom of
   `index.template.html` (between the existing module loads and
   `app.js`).

## Common patterns

### Showing a confirm dialog

```js
const ok = await askConfirm({
    title: "Delete this channel?",
    message: "All metadata stays; only the subscription is removed.",
    confirmLabel: "Delete",
    danger: true,
});
if (ok) {
    await window.pywebview.api.subs_remove({ name: channelName });
}
```

### Calling a Python API method

```js
try {
    const res = await window.pywebview.api.channel_list();
    renderRows(res);
} catch (e) {
    console.error("channel_list failed:", e);
}
```

### Receiving a push from Python

In Python:
```python
self._window.evaluate_js(
    f"window.renderSubsTable({json.dumps(rows)})"
)
```

In JS (any module that owns the rendering):
```js
window.renderSubsTable = function(rows) {
    // ... render the table from rows
};
```

## Testing

No automated test suite for the frontend. Run `python main.py` directly
and exercise the feature in the UI. The DevTools (F12 in pywebview)
work for inspecting elements and the console.

## Known accessibility gaps

Custom dropdowns lack ARIA labels; modals don't have focus traps.
Acceptable for a single-user desktop tool; not for public-web use.
