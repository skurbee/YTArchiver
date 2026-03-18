# YTArchiver

A YouTube channel archiving tool built as a feature-rich alternative to
4K Video Downloader. Download, organize, transcribe, and compress entire channels
— then keep them in sync as new videos are published.

## Features

### Downloading & Channel Management
- **Channel subscription system** — subscribe to channels and sync them all with one
  click; first sync archives existing video IDs so only new uploads get downloaded
- **Selective archiving** — set a start date to only pull videos after a certain point,
  or grab the full channel history
- **Duration filters** — exclude Shorts, livestreams, or long-form videos by setting
  min/max duration limits
- **Resolution control** — pick resolution per channel
- **Redownload Ability** — Can change channel resolution, and retroactively redownload videos to match
- **Resumable downloads** — smart ID caching means interrupted syncs pick up where
  they left off without re-scanning the channel
- **Gradual downloading** — paced requests to avoid rate limiting and IP blocks
- **Cookie support** — uses Firefox cookies for age-restricted or member content

### Organization
- **Year/month folder structure** — option to sort videos into
  `YYYY/MM/` folders
- **Reorganize tool** — retroactively sort existing files into the correct folder
  structure based on their metadata. Can be changed & reapplied at any time.

### Transcription
- **Auto-captions first** — pulls YouTube's built-in captions when available, with
  punctuation model clean up
- **Whisper fallback** — runs Whisper locally (on GPU) for videos without
  captions or with poor auto-sub quality; model selectable per channel
- **Transcript output** — clean `.txt` files alongside each video; a hidden `.jsonl`
  sidecar records per-segment timestamps for future search-and-link functionality

### Compression
- **GPU encode queue** — separate queue for post-download compression/re-encoding,
  keeping downloads and encoding independent so neither blocks the other

### UI & Workflow
- **Three-tab layout** — Download, Subs, and Recent tabs, each with a mini activity
  log that mirrors the main log
- **Simple mode** — a minimal animated status view for distraction-free monitoring
- **Pause/resume** — pause active downloads mid-session and resume without losing
  progress
- **Multi-queue system** — sync, download, transcription, reorganization, and GPU
  tasks each run in their own queue
- **System tray** — sits in your tray while running; spinning indicator shows when
  work is in progress
- **Internet monitoring** — automatically pauses on connection loss and resumes when
  connectivity is restored

## Tech Stack

Packaged as a standalone `.exe` with all dependencies bundled (no Python install
needed). Built on yt-dlp + ffmpeg for downloading, OpenAI Whisper for transcription,
and tkinter for the UI. Developed primarily with Claude Code.


I suck at writing stuff so I asked claude to write this descripton. Not too bad I guess. previous version is my version lol
