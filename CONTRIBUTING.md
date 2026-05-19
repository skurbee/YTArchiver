# Contributing to YTArchiver

YTArchiver is a Windows desktop tool for archiving YouTube channels with
local transcription and search. This document is for someone who has
just cloned the repo and wants to find their bearings.

## Quick start

1. **Install Python 3.13** (required for builds; 3.11 also needed for the
   Whisper transcribe subprocess).
2. **Install dependencies**: `pip install -r requirements.txt`
3. **Install external tools** on PATH:
   - `yt-dlp` (latest)
   - `ffmpeg` + `ffprobe`
4. **Run from source**:
   ```
   python main.py
   ```
   The pywebview window opens. No build step required for development.

## Project layout

```
YTArchiver/
├── main.py                 # Api class + window lifecycle + startup
├── YTArchiver.spec         # PyInstaller config (for builds only)
├── icon.ico
├── backend/                # All Python backend modules
│   ├── api_mixins/         # JS-callable methods (see api_mixins/README.md)
│   ├── version.py          # APP_VERSION + APP_VERSION_DATE
│   ├── sync/               # yt-dlp orchestration package
│   │   ├── core.py           # sync_channel — the giant per-channel loop
│   │   ├── sync_all.py       # multi-channel batch orchestrator
│   │   ├── sync_helpers.py   # file/format helpers
│   │   ├── log_rows.py       # activity-log row emission
│   │   ├── quickcheck.py     # fast "are there new uploads?" probe
│   │   ├── ytdlp_proc.py     # yt-dlp subprocess plumbing
│   │   ├── recent_track.py   # Recent-tab download tracking
│   │   ├── active_state.py   # in-flight sync-channel tracking
│   │   └── display_push.py   # sync-progress JSON for companion display
│   ├── transcribe/         # Whisper transcription package
│   │   ├── core.py             # TranscribeManager + worker loop
│   │   ├── helpers.py          # path/title/duration helpers
│   │   ├── punct_manager.py    # punctuation subprocess wrapper
│   │   ├── transcribe_vtt.py   # YT auto-captions fast-path
│   │   └── transcribe_files.py # .jsonl + .txt sidecar writers
│   ├── transcribe_paths.py # Path/format helpers (extracted Patch 6)
│   ├── metadata/           # Metadata package
│   │   ├── core.py             # title-match strategies + bulk pipeline
│   │   ├── fetcher.py          # per-video metadata fetch
│   │   ├── refresh.py          # re-export shim
│   │   ├── refresh_views.py    # bulk views/likes refresh
│   │   ├── refresh_comments.py # per-channel comment refresh
│   │   ├── refresh_fetch.py    # fill missing metadata
│   │   ├── _refresh_proxies.py # lazy proxies into core.py
│   │   ├── normalize.py        # title canonicalization
│   │   ├── scan.py             # per-channel video scan
│   │   └── thumbnails_ops.py   # thumbnail housekeeping
│   ├── metadata_io.py      # JSONL I/O helpers (extracted Patch 5)
│   ├── pause_helpers.py    # Shared pause/cancel guards
│   ├── index.py            # SQLite index entry — schema + register + reads
│   ├── index_search.py     # FTS5 + LIKE search (extracted Patch 17)
│   ├── index_graph.py      # word-frequency graph queries (Patch 17)
│   ├── index_bookmarks.py  # bookmark CRUD (Patch 20)
│   ├── index_maintenance.py # archive sweep + prune + FTS rebuild (Patch 20)
│   ├── html_assembler.py   # builds web/index.html from partials (Patch 19)
│   ├── queues.py           # Persistent multi-queue (sync/gpu/etc)
│   ├── compress.py         # AV1 NVENC encode pipeline
│   ├── redownload.py       # Resolution upgrade pipeline
│   ├── reorg.py            # Folder reorganization
│   ├── archive_scan.py     # Disk scan (counts + sizes)
│   ├── drift_scan.py       # Audit txt vs jsonl drift
│   ├── subs.py             # Subscription add/remove
│   ├── livestreams.py      # Livestream detection
│   ├── repair_captions.py  # YT caption repair
│   ├── punct_restore.py    # Restore punctuation on old transcripts
│   ├── thumbnails.py       # Thumbnail download + cache
│   ├── channel_art.py      # Channel banner + avatar
│   ├── ytarchiver_config.py # Config IO + view models
│   ├── view_format.py      # UI formatters (extracted Patch 7)
│   ├── text_utils.py       # Canonical normalize_title (Patch 1)
│   ├── fs_search.py        # Canonical VIDEO_EXTS + file walker (Patch 2)
│   ├── subprocess_util.py  # startupinfo + creationflags (Patch 2)
│   ├── process_runner.py   # ProcessRegistry + YtDlpRunner (Patch 3)
│   ├── utils.py            # Misc helpers (legacy grab-bag)
│   ├── log.py              # Logging bridge to LogStreamer
│   ├── log_stream.py       # Batched log emit to JS
│   ├── cmd_server.py       # HTTP cmd server (ArchivePlayer integration)
│   ├── local_fileserver.py # Local fileserver for video playback
│   ├── tray.py             # System tray
│   ├── autorun.py          # Scheduled sync runner
│   ├── window_state.py     # Save/restore window position
│   ├── net.py              # Network health check
│   ├── disk_watch.py       # Archive-drive health monitor
│   ├── temp_cleanup.py     # Startup partial-file cleanup
│   ├── channel_cache.py    # Channel ID cache (yt-dlp probe results)
│   ├── seen_filters.py     # Filter dedupe
│   ├── whisper_worker.py   # Persistent Whisper subprocess
│   └── punct_worker.py     # Persistent punctuation subprocess
└── web/                    # pywebview frontend (see web/README.md)
    ├── index.html             # Build artifact — assembled at boot
    ├── index.template.html    # Shell with @include markers
    ├── partials/              # Tab + dialog markup partials
    │   ├── tab-download.html, tab-subs.html, tab-settings.html,
    │   │   tab-browse.html, popovers.html, dialogs.html, modals.html
    ├── app.js              # Bootstrap + tab init orchestrator (~150 lines)
    ├── logs.js             # Log rendering (~900 lines)
    ├── watchView.js        # Watch view + karaoke + captions
    ├── browseGrids.js      # Channel grid + Video grid + card builder
    ├── tables.js           # Subs table + Recent list/grid
    ├── queueRender.js      # Sync/GPU task popover row builder
    ├── metadataTab.js      # Settings → Metadata refresh status
    ├── settingsTab.js, settingsInfra.js, indexControls.js
    ├── …~40 feature modules (see docs/PROJECT_MAP.md for full list)
    ├── styles.css             # vars + base (rest in styles-*.css)
    ├── styles-settings.css, styles-download-controls.css,
    │   styles-logs.css, styles-tabs-data.css, styles-browse.css,
    │   styles-browse-grids.css, styles-watch.css, styles-dialogs.css
    └── vendor/chart.umd.min.js
```

## Architecture

### Threading model

- **Main thread**: pywebview window event loop.
- **JS bridge thread(s)**: pywebview invokes Python on a worker pool when
  JS calls `pywebview.api.<method>`. Long-running handlers must offload
  to a background thread or the UI freezes.
- **Sync worker thread**: `Api._sync_thread`, spawned for sync passes.
- **GPU worker thread**: managed inside `TranscribeManager` — single
  Whisper subprocess held open across multiple videos.
- **Punctuation worker thread**: same shape as GPU but Python 3.11 subprocess.

State is shared via locks declared on the relevant objects (`Api._redwnl_lock`,
`QueueState._lock`, etc.). The cross-mixin `self.<attr>` contracts are
documented in `backend/api_mixins/README.md`.

### Data flow

1. User adds a channel URL → `subs.add_channel` writes to config.
2. Autorun scheduler (or manual "Sync" button) triggers `sync.sync_all`.
3. For each channel, `sync.sync_channel` runs yt-dlp, downloads new videos.
4. Each video lands → `transcribe.TranscribeManager.enqueue` for Whisper.
5. Whisper output → `punct_restore` for punctuation → `_write_jsonl_entry`
   stores per-segment timestamps.
6. `index.register_video` indexes the file in SQLite + FTS5.
7. User searches via UI → `index.search_segments` returns ranked snippets.

### Persistence

- **Config**: `%APPDATA%\YTArchiver\ytarchiver_config.json` (single file).
- **Index**: `<archive_root>\.ytarchiver_index.db` (SQLite + FTS5).
- **Queue state**: `%APPDATA%\YTArchiver\ytarchiver_queue.json` (debounced).
- **Auth token**: `%APPDATA%\YTArchiver\cmd_token` (Patch 1; cmd-server auth).
- **Transcripts**: `<channel>/{year}/{month}/<channel>.txt` (aggregated)
  + `<channel>/{year}/{month}/.<channel>.jsonl` (hidden, per-segment).
- **Thumbnails**: `<channel>/.Thumbnails/<title> [<vid>].jpg` (hidden).
- **Metadata**: `<channel>/.<channel> Metadata.jsonl` (hidden, per-video).

## Code style

- Python: PEP 8-ish. Type hints on new code.
- JS: ES2026 (no transpile). No `var`. Optional chaining `?.` welcome.
- Comments: explain WHY, not WHAT. The patch-history comments
  (`Patch N (vXX.Y)`) document non-obvious history.

## Building the exe

See [`docs/BUILD.md`](docs/BUILD.md).

## Submitting changes

This is a personal project. PRs are welcome but expect a real
review. Each PR should:
- Bump `APP_VERSION` in `backend/version.py` by 0.1 (see version rule).
- Be one concern per PR.
- Include a note explaining the WHY in the commit message.

### Version rule

Every git push bumps `APP_VERSION` by 0.1. Single-decimal versioning,
always carry the ten: `v37.9 + 0.1 = v38.0` (never `v37.10`).

## Known gaps

- Tests are a smoke suite only (`pytest tests/` exercises imports and
  package re-exports). End-to-end UI testing is still manual — run
  `python main.py` and exercise the flow you touched.
- `web/app.js` modularization is complete (10,218 → ~150 lines across
  ~50 focused modules). Remaining work is small-cleanup passes only.

## Where to learn more

- `backend/api_mixins/README.md` — the JS-callable API surface.
- `web/README.md` — frontend architecture.
- [`docs/BUILD.md`](docs/BUILD.md) — PyInstaller build workflow.
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — system architecture.
- [`docs/PROJECT_MAP.md`](docs/PROJECT_MAP.md) — file-by-file index.
