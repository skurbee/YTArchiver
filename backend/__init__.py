"""
backend/ — Python package containing every server-side module YTArchiver
uses at runtime. The pywebview shell (main.py) imports from here through
the `from backend.<module> import ...` form; the presence of this
`__init__.py` is what tells Python that `backend/` is a package and not
just a folder of loose scripts.

Module map (alphabetical):

  archive_scan.py      One-time + on-demand scans of the channel-folder
                       tree on disk. Produces the per-channel video
                       counts and total-size stats the Subs and Browse
                       tabs render.
  autorun.py           Schedules the recurring background sync passes
                       and writes their results to the activity log.
  channel_art.py       Fetches and caches channel avatar + banner
                       images for the Subs / Browse UIs.
  channel_cache.py     Per-channel cache of every video_id ever seen
                       on that channel — feeds the "is this new?"
                       check during a sync so we don't re-walk the
                       whole channel from yt-dlp every time.
  cmd_server.py        Tiny localhost HTTP endpoint other tools can
                       hit to read app state (version, current job,
                       etc.) without going through the GUI.
  compress.py          ffmpeg pipeline for AV1 / HEVC re-encodes that
                       shrink older downloads in-place. Driven by the
                       per-channel "compress" toggle.
  disk_watch.py        Filesystem watcher that notices when the Z:
                       drive disconnects, files are added/removed
                       manually, etc. Keeps the in-memory archive
                       scan in sync with reality.
  drift_scan.py        Periodic integrity check: every channel's
                       on-disk file list vs. the index DB, flagging
                       missing files / orphans.
  index.py             SQLite-backed video index. Every downloaded
                       video is registered here with its title,
                       upload date, duration, channel, file path,
                       transcription state, and metadata-fetch state.
                       This is the database the Browse / Search /
                       Recent tabs query.
  livestreams.py       Detection + deferral logic for YT livestream
                       and premiere URLs that aren't downloadable yet.
                       Maintains the "Deferred Livestreams" drawer.
  local_fileserver.py  Localhost static file server that serves the
                       archive's .mp4 / .vtt / .txt files to the
                       pywebview page (so the embedded player can use
                       HTTP URLs instead of file://).
  log_stream.py        The append-only log pipe between Python and
                       the JS UI. Backend writes segments, the
                       LogStreamer batches them every ~60ms and
                       evaluates `window._logBatch(payload)` in JS.
  metadata.py          yt-dlp metadata refresh (views / likes /
                       comments / description) for already-downloaded
                       videos. Writes the colored .txt sidecar that
                       Browse shows in the details panel.
  net.py               Network-down probe: parallel TCP-handshake
                       checks against a small set of hosts so the
                       pipeline can pause when the network goes
                       away and resume when it comes back. NOT a
                       general-purpose HTTP helper.
  punct_worker.py      Subprocess that runs the punctuation /
                       capitalization model over a transcript's raw
                       Whisper output. Stays in its own process so
                       loading the model doesn't bloat the main app.
  queues.py            The Sync / GPU queue state machines that the
                       UI's Sync Tasks / GPU Tasks popups display.
                       Persists to disk so queues survive a restart.
  redownload.py        Re-fetches an existing video at a higher
                       resolution / better format. Used by the
                       "Redownload" button on a video row.
  reorg.py             Year/Month folder re-shuffler — moves videos
                       into the right `YYYY/MM Month/` subfolder
                       once their upload_date is known.
  seen_filters.py      Persistent set of "skip these titles" filter
                       hits, so a duration / regex skip on one sync
                       doesn't re-trigger on every later sync.
  subs.py              Channel-subscription CRUD: add / remove /
                       rename / re-folder a tracked channel. Also
                       URL normalization so the same channel doesn't
                       get added twice with different URL spellings.
  sync.py              THE central download path — subprocess wrapper
                       around yt-dlp. Walks each channel, downloads
                       new videos, parses progress + DLTRACK lines,
                       feeds the activity log and the inline
                       metadata / transcribe queues.
  temp_cleanup.py      On-startup sweep that deletes .part / .ytdl /
                       intermediate-format leftovers from crashed
                       or cancelled past downloads.
  transcribe.py        Whisper transcription manager. Routes to the
                       fast auto-captions path when YT already has
                       captions, otherwise queues a Whisper job for
                       the Python-3.11 worker (whisper_worker.py).
                       Writes the per-video .txt and the merged
                       channel-wide `Transcript.txt`.
  tray.py              Windows system-tray icon + context menu.
                       Mirrors the current sync state into the
                       tooltip so users can see status without
                       opening the window.
  utils.py             Cross-module helpers: subprocess env, line
                       decoding (UTF-8 with cp1252 fallback),
                       MONTH_FOLDERS lookup table, etc.
  whisper_worker.py    The Python-3.11 subprocess that actually runs
                       faster-whisper. Kept separate so the main
                       app can stay on Python 3.13 while Whisper's
                       CUDA / CTranslate2 deps stay on 3.11.
  window_state.py      Saves / restores the pywebview window size,
                       position, and which tab was active across
                       app restarts.
  ytarchiver_config.py Reads / writes %APPDATA%\\YTArchiver\\
                       ytarchiver_config.json (channels, output_dir,
                       auto-sync settings, recent downloads, etc.).
                       The single source of truth for user settings.

The `log_stream` and `index` modules are the two everything else
ultimately leans on — most user-visible behavior is a backend module
either logging something via LogStreamer or reading/writing the
SQLite index.
"""
