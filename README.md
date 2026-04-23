# YTArchiver

Download, organize, transcribe, search, compress, and browse entire channels




## Features

### Downloading & Syncing
- **Channel subscriptions** — add channels to a sub list with per-channel settings
- **Selective archiving** — set a start date, grab the full history, or only pull new uploads
- **Auto-sync** — set a recurring interval (1hr, 3hr, 6hr, etc.) to automatically check all subbed channels for new videos
- **Duration filters** — exclude Shorts, livestreams, or long-form videos by setting min/max duration limits
- **Resolution control** — per-channel resolution settings (144p–1080p, or "best")
- **Redownload** — change a channel's resolution and retroactively redownload all videos at the new setting
- **Resumable downloads** — ID caching, so interrupted syncs pick back up without re-scanning
- **Cookie support** — uses Firefox cookies for age-restricted or member content, and helps avoid IP rate-limiting

### Organization
- **Folder sorting** — sort videos into `\YYYY\` or `\YYYY\MM\` folders, configurable per channel
- **Reorganize tool** — re-sort existing downloads into a new org structure at any time
- **Date Fix** — retroactively set file dates to the original YouTube upload date (fuzzy title matching, useful for migrating from other tools)

### Transcription
- **Auto-captions first** — pulls YouTube's built-in captions when available, with punctuation model cleanup
- **Whisper GPU fallback** — runs Whisper locally on GPU for videos without captions; model selectable per channel
- **Auto-transcribe** — per-channel toggle to automatically transcribe new videos after each sync
- **Transcript output** — clean `.txt` files with an option to follow the channel's folder org or combine into a single file per channel
- **Hidden JSONL sidecars** — per-word timestamps and video IDs stored alongside readable transcripts

### Browse Tab
- **Searchable transcript database** — full-text search across all transcribed channels
- **Embedded video player** — HTML5 video with a synced, scrolling transcript alongside
- **Click-to-seek** — click any word in the transcript to jump the video to that moment
- **Word frequency analysis** — frequency graphs (Year / Month / Week buckets) and word cloud visualizations
- **Recent downloads** — list or thumbnail-grid view of recently-archived videos, filterable

### Compression
- **AV1 NVENC encoding** — compress archived videos using AV1 hardware encoding (NVIDIA GPU)
- **Quality presets** — Generous, Average, and Below Average quality tiers with target bitrate-per-hour calculations
- **HQ downscale** — download at a higher resolution, then downscale for better quality at lower resolutions

### UI & Workflow
- **Four-tab layout** — Download, Subs, Browse, Settings (Recent now lives under Browse, Index under Settings)
- **Settings categories** — General / Performance / Appearance / Tools / Index sub-tabs
- **Simple / Verbose log modes** — toggle between a readable sync view and full yt-dlp output
- **Pause / resume** — pause active downloads or transcriptions mid-session and resume without losing progress
- **GPU task queue** — transcription and compression jobs run in their own reorderable queue, separate from downloads
- **System tray** — sits in the tray with separate indicators for downloads and GPU tasks; auto-sync controllable from the tray menu
- **Desktop notifications** — Windows notifications on sync completion, errors, etc.
- **Internet monitoring** — automatically pauses on connection loss, resumes when connectivity is restored
- **Drive monitoring** — automatically pauses on drive failure, resumes when drive is restored
- **Auto-update** — checks for new releases on GitHub at startup


## Tech Stack

Built on **yt-dlp** + **ffmpeg** for downloading, **OpenAI Whisper** (faster-whisper) for GPU transcription, **SQLite FTS5** for the transcript index, **Chart.js** for graphing, and a **pywebview** shell rendering an HTML/CSS/JS frontend.

> The previous tkinter-based build is archived as the **Tkinter legacy** release

EXE in releases has everything bundled. If you run from source, you'll need Python 3.13 with `pywebview`, `pystray`, and `Pillow` installed (`pip install pywebview pystray pillow`). Whisper stays out-of-tree and is invoked via a Python 3.11 venv at runtime.


Line by line, it's roughly 90% Claude-Code, 5% Copilot, 5% me :)

