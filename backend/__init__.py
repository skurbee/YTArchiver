"""
Backend package for YTArchiver.

The pywebview shell in ``main.py`` imports backend modules through
``from backend.<module> import ...``. This file is intentionally code-light;
its job is to keep a quick, accurate map of the current package shape.
For the exhaustive file-by-file index, see ``docs/PROJECT_MAP.md``.

Core packages:

  api_mixins/         JS-callable methods mixed into ``main.Api``.
                      The README in that package documents the shared
                      ``self.<attr>`` contracts and the AppServices migration.
  metadata/          Metadata refresh package: title matching, yt-dlp fetches,
                      bulk view/comment refresh, scan helpers, normalization,
                      thumbnails, and compatibility re-exports.
  services/          Dependency-injection layer for thinning ``main.Api``:
                      ``AppServices``, ``BridgeEventBus``, and file operation
                      helpers.
  sync/              yt-dlp orchestration package. ``core.py`` owns
                      ``sync_channel`` and the download loop; sibling modules
                      handle batch orchestration, options, progress/log rows,
                      quick checks, subprocess sessions, and active state.
  transcribe/        Whisper transcription package: manager/worker loop,
                      caption fast-path, transcript writers, punctuation
                      manager, and path/title helpers.

Top-level modules:

  archive_scan.py       Disk scans for channel counts, sizes, and recency.
  autorun.py            Recurring background sync scheduler and history.
  channel_art.py        Channel avatar/banner fetch and cache.
  channel_cache.py      Per-channel seen-video-id cache for fast sync probes.
  cmd_server.py         Localhost HTTP bridge for companion tools.
  compress.py           ffmpeg AV1/HEVC compression pipeline.
  deps_installer.py     Optional dependency installation helpers.
  disk_watch.py         Archive-drive writable/reachable monitor.
  drift_scan.py         File/index/transcript integrity checks.
  fs_search.py          Canonical video extension set and file walkers.
  html_assembler.py     Builds ``web/index.html`` from template partials.
  index.py              SQLite entry module: schema, registration, reads,
                        and re-exports from specialized index modules.
  index_bookmarks.py    Bookmark CRUD.
  index_graph.py        Word-frequency and graph queries.
  index_maintenance.py  Archive sweep, prune, and FTS rebuild operations.
  index_search.py       FTS5 and title-search query helpers.
  livestreams.py        Deferred livestream/premiere tracking.
  local_fileserver.py   Local file-serving layer for embedded playback.
  log.py                Logging adapter.
  log_stream.py         Batched Python-to-JS log transport.
  metadata/io.py        JSONL metadata/transcript sidecar I/O helpers.
  net.py                Network health probe.
  pause_helpers.py      Shared pause/cancel guards.
  process_runner.py     Process registry and yt-dlp runner helpers.
  punct_restore.py      Restore punctuation in existing transcripts.
  punct_worker.py       Punctuation subprocess entry point.
  queues.py             Persistent sync/GPU queue state.
  redownload.py         Resolution upgrade pipeline.
  reorg.py              Year/month folder reorganization.
  repair_captions.py    YouTube caption repair helpers.
  seen_filters.py       Persistent filter-hit dedupe.
  subprocess_util.py    Windows subprocess startup helpers.
  subs.py               Subscription CRUD and URL normalization.
  temp_cleanup.py       Startup cleanup of partial/intermediate files.
  text_utils.py         Title/text normalization helpers.
  thumbnails.py         Thumbnail download/cache helpers.
  transcribe/paths.py   Transcript path and naming helpers.
  tray.py               Windows system-tray controller.
  utils.py              Legacy shared helpers.
  version.py            ``APP_VERSION`` and ``APP_VERSION_DATE``.
  view_format.py        UI-facing formatting helpers.
  whisper_worker.py     Whisper subprocess entry point.
  window_state.py       Save/restore pywebview window state.
  ytarchiver_config.py  Config file loading, saving, and view models.
"""
