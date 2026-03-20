# YTArchiver

A YouTube channel archiving tool built as a feature-rich alternative to
4KVideoDownloader. Download, organize, transcribe, and compress entire channels
— then keep them in sync as new videos are published.

## Features

### Downloading & Channel Management
- **Channel subscriptions** — subscribe to channels and sync them all with one
  click; first sync archives existing video IDs so only new uploads get downloaded
- **Selective archiving** — set a start date to only pull videos after a certain point,
  or grab the full channel history
- **Duration filters** — exclude Shorts, livestreams, or long-form videos by setting
  min/max duration limits
- **Resolution control** — pick resolution per channel
- **Redownload ability** — Can change channel resolution, and retroactively redownload videos to match
- **Resumable downloads** — ID caching means interrupted syncs pick up where
  they left off without re-scanning the channel
- **Gradual downloading** — paced requests to avoid rate limiting and IP blocks
- **Cookie support** — uses Firefox cookies for age-restricted or member content.

### Organization
- **Year/month folder structure** — option to sort videos into
  `YYYY/MM/` folders
- **Reorganize tool** — retroactively sort existing files into the correct folder
  structure based on their metadata. Can be changed & reapplied at any time.
- **Date Fix** — If you already have downloads from 4kVD, this can retroactively change their
  file date to the YT upload date (fuzzy title matching)

### Transcription
- **Auto-captions first** — pulls YouTube's built-in captions when available, with
  punctuation model clean up
- **Whisper fallback** — runs Whisper locally (on GPU) for videos without
  captions or with poor auto-sub quality; model selectable per channel
- **Transcript output** — clean `.txt` file that follows channel org structure; a hidden `.jsonl`
  sidecar records per-segment timestamps for future search-and-link functionality

### Compression
- **HQ Low Res** — Can opt to download at a higher resolution, and downscale for better
  quality low resolution downloads

### UI & Workflow
- **Three-tab layout** — Download, Subs, and Recent tabs, each with a mini activity
  log that mirrors the main log
- **Simple mode** — a minimal log view for easy monitoring
- **Pause/resume** — pause active downloads mid-session and resume without losing
  progress
- **Multi-queue system** — download tasks & GPU tasks (transcription, compression)
  each run in their own queue
- **System tray** — sits in your tray while running; spinning indicator shows when
  work is in progress
- **Internet monitoring** — automatically pauses on connection loss and resumes when
  connectivity is restored
- **Drive monitoring** — automatically pauses on drive failure, auto resumes when drive
  restored.

## Tech Stack

Built on yt-dlp + ffmpeg for downloading, OpenAI Whisper for transcription,
and tkinter for the UI. Developed primarily with Claude Code/Copilot.


If you use the .py file instead of the exe:

Just put the .ico in the same /dir as the .py file when you run. It's for the taskbar icon,
and I'm not sure what it'll do without it. Prob be fine, just sayin :)

Splitting this monolithic .py file into multiple smaller ones is in the todo list. IDK WHEN, but eventually :)

I suck at writing stuff so I asked claude to write this descripton. Not too bad I guess. lil too flowery but eh lol
