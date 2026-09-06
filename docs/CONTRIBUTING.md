# Contributing to YTArchiver

YTArchiver is a Windows desktop tool for archiving YouTube channels with
local transcription and search. This document is for someone who has
just cloned the repo and wants to find their bearings.

## Quick start

Run commands from the repository root on Windows x64. Install the exact
Python 3.13 patch version in `.python-version` and Node version in `.nvmrc`.
Browser tests use installed Chrome by default; see [BUILD.md](BUILD.md) for
browser selection and dependency setup.

The standard check without an executable build is:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/check.ps1 -Bootstrap -SkipBuild
```

This creates a temporary environment from the hash-locked Python dependencies,
runs the isolated checks, and removes that environment afterward. The floating
ranges in `docs/requirements.txt` are not the reproducible development or build
installation path. [BUILD.md](BUILD.md) also describes a reusable locked
environment for focused work.

Python 3.11 and the separate worker locks are needed for optional Whisper and
punctuation execution, not for packaging the desktop runtime. Downloads and
media processing additionally need yt-dlp, ffmpeg, and ffprobe. Dependency
diagnostics and **Run setup again** are under **Settings → About & troubleshooting**.

A source launch with `.venv\Scripts\python.exe main.py` opens the application
and uses the selected application-data profile. Native UI checks are a separate,
explicitly approved action: use disposable application data and archive folders,
or state specifically approved for that check. Do not close or replace a running
instance as part of an automated check.

## Project layout

```
YTArchiver/
├── main.py                 # Api class + window lifecycle + startup
├── YTArchiver.spec         # PyInstaller config (for builds only)
├── icon.ico
├── README.md               # GitHub landing page
├── LICENSE                 # MIT
├── pyproject.toml          # Project metadata + Python tool config
├── .python-version, .nvmrc # Exact quality-gate toolchain versions
├── requirements/           # Hash-locked desktop/build/dev/worker profiles
├── package.json            # Browser-test commands
├── package-lock.json       # Pinned browser-test dependencies
├── playwright.config.js    # Headless browser + fixture-test configuration
├── scripts/                # Windows gate, locks, import/HTML/bridge/build checks
├── tests/                  # Python, Node, and frontend/browser regressions
├── docs/                   # Project docs
│   ├── ARCHITECTURE.md     # System architecture
│   ├── BUILD.md            # PyInstaller build workflow
│   ├── CHANGELOG.md        # Release notes
│   ├── CONTRIBUTING.md     # This file
│   ├── PROJECT_MAP.md      # File-by-file index
│   └── requirements.txt    # Range-based convenience dependency list
├── backend/                # All Python backend modules
│   ├── api_mixins/         # JS-callable methods (see api_mixins/README.md)
│   ├── version.py          # APP_VERSION + APP_VERSION_DATE
│   ├── sync/               # yt-dlp orchestration package
│   │   ├── core.py           # sync_channel — the giant per-channel loop
│   │   ├── sync_all.py       # multi-channel batch orchestrator
│   │   ├── sync_helpers.py   # file/format helpers
│   │   ├── log_rows.py       # activity-log row emission
│   │   ├── quickcheck.py     # fast "are there new uploads?" probe
│   │   ├── options.py       # normalized sync options
│   │   ├── ytdlp_proc.py     # yt-dlp lookup/cookies/formats
│   │   ├── ytdlp_events.py   # yt-dlp output parsing
│   │   ├── ytdlp_session.py  # process launch/watchdog/finish
│   │   ├── recent_track.py   # Recent-download history tracking
│   │   ├── active_state.py   # in-flight sync-channel tracking
│   │   └── display_push.py   # sync-progress JSON for companion display
│   ├── transcribe/         # Whisper transcription package
│   │   ├── core.py             # TranscribeManager + worker loop
│   │   ├── helpers.py          # path/title/duration helpers
│   │   ├── paths.py            # Path/format helpers
│   │   ├── punct_manager.py    # punctuation subprocess wrapper
│   │   ├── transcribe_vtt.py   # YT auto-captions fast-path
│   │   └── transcribe_files.py # .jsonl + .txt sidecar writers
│   ├── metadata/           # Metadata package
│   │   ├── io.py               # JSONL I/O helpers
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
│   ├── services/           # AppServices + event bus + file ops
│   ├── pause_helpers.py    # Shared pause/cancel guards
│   ├── index.py            # SQLite index entry — schema + register + reads
│   ├── index_search.py     # FTS5 + LIKE search
│   ├── index_graph.py      # word-frequency graph queries
│   ├── index_bookmarks.py  # bookmark CRUD
│   ├── index_maintenance.py # archive sweep + prune + FTS rebuild
│   ├── html_assembler.py   # builds web/index.html from partials
│   ├── queues.py           # Persistent multi-queue (sync/gpu/etc)
│   ├── compress.py         # AV1 NVENC encode pipeline
│   ├── redownload.py       # Replace selected copies at a chosen resolution
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
│   ├── view_format.py      # UI formatters
│   ├── text_utils.py       # Canonical normalize_title
│   ├── fs_search.py        # Canonical VIDEO_EXTS + file walker
│   ├── subprocess_util.py  # startupinfo + creationflags
│   ├── process_runner.py   # ProcessRegistry + YtDlpRunner
│   ├── utils.py            # Misc helpers (legacy grab-bag)
│   ├── log.py              # Logging bridge to LogStreamer
│   ├── log_stream.py       # Batched log emit to JS
│   ├── cmd_server.py       # Loopback HTTP API for companion viewers
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
    │   ├── tab-download.html, tab-subs.html, tab-browse.html,
    │   │   tab-health.html, tab-settings.html, onboarding.html,
    │   │   popovers.html, dialogs.html, modals.html
    ├── app.js              # Bootstrap + tab init orchestrator
    ├── logs.js             # Log rendering
    ├── watchView.js        # Watch view + karaoke + captions
    ├── browseGrids.js      # Channel grid + Video grid + card builder
    ├── tables.js           # Optional compact Subs table
    ├── queueRender.js      # Sync/GPU task popover row builder
    ├── metadataTab.js      # Health → Library metadata and repair controls
    ├── settingsTab.js, settingsInfra.js, indexControls.js
    ├── …other feature modules (see docs/PROJECT_MAP.md for the full list)
    ├── styles.css             # vars + base (rest in styles-*.css)
    ├── styles-settings.css, styles-download-controls.css,
    │   styles-logs.css, styles-tabs-data.css, styles-browse.css,
    │   styles-browse-grids.css, styles-watch.css, styles-dialogs.css
    └── vendor/chart.umd.min.js
```

The default main tabs are Download, Browse, Health, and Settings. The optional
compact (Dense) Subs tab is hidden by default and can be enabled with **Show
compact Subs tab** in Settings. Channel management is also in **Browse → Channels**.
Health contains Overview, Library, and Backups. Settings is a single preferences
page. Search, Bookmarks, Graph, Videos, Manual, and Trash are Browse views; keep
those locations in mind when changing labels or navigation.

`web/index.html` is generated. Change `web/index.template.html` or the relevant
partial, then regenerate and review the HTML diff before checking the tree.
The exact regeneration and verification commands are in [BUILD.md](BUILD.md).

## Architecture

### Threading model

- **Main thread**: pywebview window event loop.
- **JS bridge threads**: calls to `pywebview.api.<method>` can overlap. Keep
  foreground queries bounded; a long query holding a shared lock can stall
  other views even while the window remains responsive. Long-running jobs
  use the application's supervised workers and cancellation controls.
- **Sync worker thread**: `Api._sync_thread`, spawned for sync passes.
- **Processing worker**: `TranscribeManager` runs queued transcription and
  compression work. Whisper uses a persistent Python 3.11 subprocess.
- **Punctuation worker**: a separate Python 3.11 subprocess handles punctuation
  restoration when requested.

State is shared via locks declared on the relevant objects (`Api._redwnl_lock`,
`QueueState._lock`, etc.). The cross-mixin `self.<attr>` contracts are
documented in `backend/api_mixins/README.md`.

### Data flow

1. User adds a channel URL → `subs.add_channel` writes to config.
2. Autorun scheduler (or manual "Sync" button) triggers `sync.sync_all`.
3. For each channel, `sync.sync_channel` runs yt-dlp, downloads new videos.
4. A downloaded file is registered in the video catalog. Enabled post-processing
   can use YouTube captions or queue Whisper and compression work.
5. Transcript writers store text and timed JSONL segments in the selected
   combined, year, month, or individual-video layout.
6. JSONL ingestion updates the SQLite transcript table and FTS5 index;
   `index.register_video` maintains the separate video catalog.
7. Search calls `index.search_fts` for transcript snippets and
   `index.search_video_titles` for video-title results.

### Persistence

- **Config**: `%APPDATA%\YTArchiver\ytarchiver_config.json` (single file).
- **Index**: `%APPDATA%\YTArchiver\transcription_index.db` (SQLite + FTS5).
- **Queue state**: `%APPDATA%\YTArchiver\ytarchiver_queue.json` plus
  `ytarchiver_queue_resuming.json` for current-task recovery. Exact task
  transitions use durable saves; routine updates can be debounced.
- **Auth token**: `%APPDATA%\YTArchiver\cmd_token` (cmd-server auth).
- **Transcripts**: channel-root, year, or month transcript files according to
  the channel's output preference; individual videos use their own sidecars.
  The timed JSONL sidecar shares the text file's stem with a leading dot.
- **Thumbnails**: `<channel>/.Thumbnails/<title> [<vid>].jpg` (hidden).
- **Metadata**: `<channel>/.<channel> Metadata.jsonl` (hidden, per-video).

## Code style

- Python: PEP 8-ish. Type hints on new code.
- JS: browser JavaScript without a transpilation step. Match the existing
  feature modules and supported WebView2 runtime; Node is used for checks.
- Comments: explain WHY, not WHAT. The patch-history comments
  (`Patch N (vXX.Y)`) document non-obvious history.

## Building the exe

See [`BUILD.md`](BUILD.md).

## Submitting changes

PRs are welcome and are reviewed before integration. Each PR should:

- Be one concern per PR.
- Explain the user-visible problem, resulting behavior, and verification.
- Include meaningful regression coverage where behavior changes.
- Preserve unrelated changes and keep public examples free of private paths
  or archive details.

### Version rule

Maintainer pushes require a version increment in `backend/version.py` and an
entry in `docs/CHANGELOG.md`; keep `APP_VERSION_DATE` aligned with the release.
Single-decimal versioning carries the ten: `v37.9 + 0.1 = v38.0`
(never `v37.10`). Coordinate the version with the maintainer. Local edits,
testing, building, committing, pushing, and publishing are separate actions;
do not infer permission for later steps from an earlier one.

## Verification

- Use the Windows gate above for the complete automated check without building.
  Omit `-SkipBuild` only when an executable build is intended.
- Run **one Python test file per fresh interpreter**, with disposable `APPDATA`
  and `LOCALAPPDATA` set before imports. Never use aggregate `pytest`,
  including `pytest tests/`. Test modules can change process-wide
  configuration and shutdown state. [BUILD.md](BUILD.md) has a safe focused
  test example, including `tests/test_backend_smoke.py`.
- Prefer `scripts/check.ps1` directly. `scripts/check.sh` is the Git Bash
  compatibility entry point that forwards arguments to the same Windows
  PowerShell gate; it does not run a separate aggregate Python suite.
- `scripts/smoke.py` is a legacy subset: it does not isolate its application-data
  environment, regenerates stale HTML, and skips JavaScript syntax checks if
  Node is unavailable. It is not a replacement for the gate.
- Node regressions are `tests/test_frontend*.js`. Browser behavior tests live in
  `tests/frontend/browser` and run with `npm run test:browser` against the real
  HTML and a fixture bridge. They run headlessly; native WebView/media behavior
  still benefits from a separately approved manual check.
- Edit the template or partials, regenerate HTML, and review that output before
  the gate. The gate verifies freshness without repairing the generated file.

## Where to learn more

- `../backend/api_mixins/README.md` — the JS-callable API surface.
- `../web/README.md` — frontend architecture.
- [`BUILD.md`](BUILD.md) — PyInstaller build workflow.
- [`ARCHITECTURE.md`](ARCHITECTURE.md) — system architecture.
- [`PROJECT_MAP.md`](PROJECT_MAP.md) — file-by-file index.
