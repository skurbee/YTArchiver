"""
Synthesized log data for Phase 0 parity gate.

Exercises every tag category used by YTArchiver's activity log, main log,
and mini log. Format matches tkinter's insert(text, tag) pairing — here we
emit [text, tag] segments that app.js turns into styled <span>s.

When Phase 6 wires the real backend, this file goes away.
"""

from __future__ import annotations

import random
from typing import List, Tuple, Union

# A "segment" is [text, tag?]. Tag is a string (single class) or None.
Segment = List[Union[str, None]]


# ── Activity log generators ─────────────────────────────────────────────

def _metadata_row(time_str: str, date_str: str, channel: str,
                  fetched: int, skipped: int, errors: int, took: str,
                  tag: str) -> List[Segment]:
    """Format: [Metdta] 3:16pm, Apr 10 — ChannelName — N fetched · N skipped · N errors · took Ns"""
    return [
        ["[Metdta] ", tag],
        [f"{time_str}, {date_str} ", tag],
        ["\u2014 ", tag],
        [f"{channel} ", tag],
        ["\u2014 ", tag],
        [f"{fetched} fetched \u00b7 ", tag],
        [f"{skipped} skipped \u00b7 ", tag],
        [f"{errors} errors \u00b7 ", tag],
        [f"took {took}", tag],
    ]


def _sync_row(time_str: str, date_str: str, channel: str,
              downloaded: int, errors: int, took: str,
              tag: str) -> List[Segment]:
    """Format: [Sync] 5:30pm, Apr 10 — ChannelName — N downloaded · N errors · took 1m 20s"""
    return [
        ["[Sync] ", tag],
        [f"{time_str}, {date_str} ", tag],
        ["\u2014 ", tag],
        [f"{channel} ", tag],
        ["\u2014 ", tag],
        [f"{downloaded} downloaded \u00b7 ", tag],
        [f"{errors} errors \u00b7 ", tag],
        [f"took {took}", tag],
    ]


def _kind_cells(kind: str, time_str: str, date_str: str, channel: str,
                primary: str, secondary: str, errors: str, took: str,
                row_tag: str) -> List[dict]:
    """
    Emit a structured 9-cell row for the activity-log grid.

    The renderer places each cell into its own grid column so rows align
    vertically regardless of channel-name length, and the grid adapts to
    program width (channel column is the flex cell).
    """
    return {
        "kind": kind,
        "time_date": f"{time_str}, {date_str}",
        "channel": channel,
        "primary": primary,
        "secondary": secondary,
        "errors": errors,
        "took": took,
        "row_tag": row_tag or "", # "" = default color
    }


def generate_activity_log_history() -> List[dict]:
    """
    Return a list of activity-log entries, one per autorun pass.

    Matches YTArchiver's 7 activity-log kinds (line 22300-22326 in YTArchiver.py):
      Trnscr → hist_blue if transcribed > 0
      Metdta → hist_pink if fetched > 0 or refreshed > 0
      Manual → hist_green if downloaded > 0
      Auto → hist_green if downloaded > 0
      ReDwnl → hist_redwnl if replaced > 0 or "running..." in body
      Cmprss → hist_compress if compressed > 0
      Reorg → hist_reorg if moved/reorganized > 0

    Each entry = { segments: [...], alt: bool }
    """
    channels = [c[0] for c in SAMPLE_CHANNELS]
    entries: List[dict] = []
    alt = False

    def emit(kind, time_str, date_str, chan, primary, secondary, errors, took,
             work_triggered, positive_tag):
        # Project rule: only log a row when something actually happened.
        # Skip no-work entries entirely — they just add noise.
        if not work_triggered:
            return
        nonlocal alt
        row_tag = positive_tag
        cells = _kind_cells(kind, time_str, date_str, chan,
                            primary, secondary, errors, took, row_tag)
        entries.append({"cells": cells, "alt": alt})
        alt = not alt

    # Metdta — some with fetches/refreshes (pink), some without (default)
    for i in range(10):
        chan = channels[i]
        fetched = random.choice([0, 0, 0, 1, 3, 5, 8])
        refreshed = random.choice([0, 0, 0, 2, 6])
        skipped = random.randint(50, 5000)
        errors_n = random.choice([0, 0, 0, 0, 1, 2, 4])
        took_secs = random.randint(2, 90)
        took = f"took {took_secs}s" if took_secs < 60 else f"took {took_secs // 60}m {took_secs % 60}s"
        hour = 3 + (i // 2)
        minute = (i * 7) % 60
        had_work = fetched > 0 or refreshed > 0
        # Show whichever count is the work signal (matches real app dispatch)
        if fetched > 0:
            primary = f"{fetched} fetched"
        elif refreshed > 0:
            primary = f"{refreshed} refreshed"
        else:
            primary = "0 fetched"
        emit("Metdta", f"{hour}:{minute:02d}pm", "Apr 10", chan,
             primary, f"{skipped} skipped",
             f"{errors_n} errors", took, had_work, "hist_pink")

    # Trnscr — transcription summary (hist_blue when transcribed > 0)
    emit("Trnscr", "5:02pm", "Apr 10", channels[2],
         "14 transcribed", "", "0 errors", "took 6m 45s",
         True, "hist_blue")
    emit("Trnscr", "5:22pm", "Apr 10", channels[5],
         "0 transcribed", "", "0 errors", "nothing pending",
         False, "hist_blue")

    # Auto / Manual — sync (hist_green when downloaded > 0)
    emit("Auto", "5:30pm", "Apr 10", channels[4],
         "7 downloaded", "", "0 errors", "took 4m 20s",
         True, "hist_green")
    emit("Manual", "5:48pm", "Apr 10", channels[7],
         "2 downloaded", "", "1 errors", "took 1m 8s",
         True, "hist_green")

    # ReDwnl (hist_redwnl when replaced > 0 or running)
    emit("ReDwnl", "6:00pm", "Apr 10", channels[9],
         "3 replaced", "", "0 errors", "took 2m 10s",
         True, "hist_redwnl")

    # Cmprss (hist_compress when compressed > 0)
    emit("Cmprss", "6:12pm", "Apr 10", channels[11],
         "12 compressed", "", "0 errors", "took 18m 4s",
         True, "hist_compress")

    # Reorg (hist_reorg when moved > 0)
    emit("Reorg", "6:30pm", "Apr 10", channels[13],
         "48 moved", "", "0 errors", "took 12s",
         True, "hist_reorg")

    return entries


def generate_recent_downloads():
    """Return sample recent-downloads rows."""
    rows = []
    channels = [c[0] for c in SAMPLE_CHANNELS[:12]]
    titles = SAMPLE_TITLES[:50] if len(SAMPLE_TITLES) >= 50 else SAMPLE_TITLES * 2
    now_mins = 0
    for i in range(50):
        mins_ago = i * 7 + random.randint(0, 40)
        if mins_ago < 60:
            t = f"{mins_ago}m ago"
        elif mins_ago < 60*24:
            t = f"{mins_ago//60}h ago"
        else:
            t = f"{mins_ago//(60*24)}d ago"
        sec = random.randint(60, 45*60)
        dur = f"{sec//60}:{sec%60:02d}"
        mb = random.randint(3, 85)
        rows.append({
            "title": titles[i % len(titles)],
            "channel": channels[i % len(channels)],
            "time": t,
            "duration": dur,
            "size": f"{mb} MB",
        })
    return rows


def generate_queues():
    """Return sample sync queue + gpu queue state matching the popover format.

    Verb tense rule (matches YTArchiver): the RUNNING task (status="running",
    always pos 1) uses the present continuous ("Downloading", "Transcribing"),
    everything else in the queue uses the plain verb form ("Download",
    "Transcribe", "Metadata") — it hasn't started yet.
    """
    channels = [c[0] for c in SAMPLE_CHANNELS]
    sync = [
        {"name": f"Downloading {channels[0]}", "status": "running"},
        {"name": f"Metadata {channels[3]}", "status": "queued"},
        {"name": f"Download {channels[7]}", "status": "queued"},
        {"name": f"Download {channels[12]}", "status": "queued"},
    ]
    gpu = [
        {"name": f"Transcribing Season Finale Recap (25m 04s)",
         "status": "running"},
        {"name": f"Transcribe Weekly News Roundup (18m 22s)",
         "status": "queued"},
        {"name": f"Transcribe Product Teardown: Model X (9m 33s)",
         "status": "queued"},
    ]
    return {"sync": sync, "gpu": gpu}


# ── Main log generators ─────────────────────────────────────────────────

def _metadata_line(idx: int, total: int, title: str) -> List[Segment]:
    """[N/N] Metadata - Title - Channel"""
    return [
        ["[", "meta_bracket"],
        [f"{idx}", "dl_white"],
        ["/", "meta_bracket"],
        [f"{total}", "dl_white"],
        ["] ", "meta_bracket"],
        ["Metadata ", "simpleline_pink"],
        ["- ", "simpleline"],
        [title, "simpleline"],
        ["\n", None],
    ]


def _transcribing_line(idx: int, total: int, title: str, pct: int) -> List[Segment]:
    """[N/N] Transcribing "Title", 30%..."""
    return [
        ["[", "trans_bracket"],
        [f"{idx}", "dl_white"],
        ["/", "trans_bracket"],
        [f"{total}", "dl_white"],
        ["] ", "trans_bracket"],
        ["Transcribing ", "simpleline_blue"],
        [f'"{title}", ', "simpleline"],
        [f"{pct}%", "whisper_pct"],
        ["...", "whisper_dots"],
        ["\n", None],
    ]


def _done_line(idx: int, total: int, title: str,
               dur: str, took: str, realtime: str) -> List[Segment]:
    """[N/N] Title - (5m 56s) - done (Whisper, took 9sec, 36.6x realtime)"""
    return [
        ["[", "trans_bracket"],
        [f"{idx}", "dl_white"],
        ["/", "trans_bracket"],
        [f"{total}", "dl_white"],
        ["] ", "trans_bracket"],
        [f"{title}", "simpleline"],
        [" \u2014 ", "simpleline"],
        [f"({dur}) ", "simpleline"],
        ["\u2014 ", "simpleline"],
        ["done ", "simpleline_blue"],
        [f"(Whisper, took {took}, {realtime} realtime)", "simpleline_blue"],
        ["\n", None],
    ]


def _download_line(idx: int, total: int, title: str, pct: int) -> List[Segment]:
    """[N/N] Downloading "Title", 58%..."""
    return [
        ["[", "sync_bracket"],
        [f"{idx}", "dl_white"],
        ["/", "sync_bracket"],
        [f"{total}", "dl_white"],
        ["] ", "sync_bracket"],
        ["Downloading ", "simpleline_green"],
        [f'"{title}", ', "simpleline"],
        [f"{pct}%", "dlprogress_pct"],
        ["...", "trans_dots"],
        ["\n", None],
    ]


def _encode_line(idx: int, total: int, title: str, pct: int) -> List[Segment]:
    """[N/N] Encoding "Title", 42%..."""
    return [
        ["[", "compress_bracket"],
        [f"{idx}", "dl_white"],
        ["/", "compress_bracket"],
        [f"{total}", "dl_white"],
        ["] ", "compress_bracket"],
        ["Encoding ", "simpleline_compress"],
        [f'"{title}", ', "encode_prefix"],
        [f"{pct}%", "encode_pct"],
        ["...", "encode_dots"],
        ["\n", None],
    ]


def _header_line(text: str) -> List[Segment]:
    return [[text + "\n", "header"]]


def _summary_line(text: str) -> List[Segment]:
    return [[text + "\n", "summary"]]


def _update_sep() -> List[Segment]:
    return [["════════════════════════════════════════════════════════════════\n", "update_sep"]]


# Generic fictional channel names for preview / demo mode only — no real
# channels referenced. Used when the app runs without a backend (e.g.
# static HTML preview) so the Subs table renders with plausible-looking
# data. Replaced at runtime by `subs_load_channels()` from the real config.
SAMPLE_CHANNELS = [
    # (folder, res, min, max, compress, transcribe, metadata, last_sync, n_vids, size_gb)
    ("Alpha Tech Reviews", "720p", "3m", "—", False, True, True, "1d ago", 420, 55.0),
    ("Beta Gaming", "720p", "3m", "—", False, True, True, "1d ago", 910, 64.0),
    ("Comet Labs", "best", "3m", "—", True, True, True, "1d ago", 110, 16.0),
    ("Daily Explainer", "720p", "3m", "—", False, True, True, "1d ago", 500, 12.5),
    ("Echo Podcast", "720p", "3m", "—", False, True, True, "1d ago", 500, 14.0),
    ("Fable Documentary", "720p", "3m", "—", False, True, True, "1d ago", 80, 38.0),
    ("Global Music Weekly", "720p", "3m", "—", False, True, True, "1d ago", 165, 50.0),
    ("History Hour", "480p", "3m", "60m", False, True, True, "1d ago", 1390, 25.0),
    ("Indoor Gardening", "720p", "3m", "60m", False, True, True, "1d ago", 75, 6.0),
    ("Jumpstart Science", "best", "3m", "—", False, True, True, "1d ago", 40, 9.0),
    ("Kilo Retro Tech", "720p", "2m", "180m",False, True, True, "1d ago", 130, 59.0),
    ("Lima News 5", "720p", "3m", "—", False, True, True, "1d ago", 140, 19.0),
    ("Mike Bodycam", "720p", "—", "—", False, True, True, "1d ago", 395, 149.0),
    ("Novel Investigates", "720p", "3m", "—", False, True, True, "1d ago", 395, 31.8),
    ("Orbit Cold", "1080p", "3m", "60m", False, True, True, "1d ago", 505, 90.0),
    ("Paper Company", "720p", "3m", "60m", False, True, True, "1d ago", 470, 28.0),
    ("Quill Film Crew", "480p", "3m", "60m", False, True, True, "1d ago", 1295, 68.0),
    ("Radio Politics", "144p", "3m", "40m", False, True, True, "1d ago", 2800, 30.0),
    ("Sierra Decoder", "1080p", "3m", "—", False, True, True, "1d ago", 55, 10.8),
    ("Tango Detective", "720p", "3m", "—", False, True, True, "1d ago", 140, 45.8),
    ("Uniform Perks", "720p", "3m", "—", False, True, True, "1d ago", 80, 42.0),
    ("Victor Podcast", "1080p", "3m", "—", False, True, True, "1d ago", 45, 21.9),
    ("Whiskey MD", "720p", "3m", "—", False, True, True, "1d ago", 150, 27.0),
    ("Xenon reads", "720p", "3m", "—", False, True, True, "1d ago", 215, 56.0),
    ("Yankee Dashcam", "720p", "3m", "—", False, True, True, "1d ago", 600, 85.0),
    ("Zulu Crime Story", "720p", "3m", "—", False, True, True, "1d ago", 85, 10.0),
    ("Arctic Explorers", "720p", "3m", "—", False, True, True, "1d ago", 260, 48.0),
    ("Bayou Songbook", "720p", "3m", "—", False, True, True, "1d ago", 115, 57.0),
    ("Coastal Watch", "best", "—", "—", False, True, True, "1d ago", 165, 10.0),
    ("Digital Frame", "720p", "3m", "—", False, True, True, "1d ago", 40, 21.0),
    ("Element Review", "best", "3m", "60m", False, True, True, "1d ago", 35, 12.0),
    ("Fresh Work", "720p", "3m", "—", False, True, True, "1d ago", 50, 2.8),
    ("Green Info", "1080p", "3m", "60m", False, True, True, "1d ago", 450, 74.5),
    ("Hyper Discussion", "240p", "3m", "—", False, True, True, "1d ago", 2800, 110.0),
    ("Indy Finance", "1080p", "—", "—", False, True, True, "1d ago", 105, 13.4),
    ("Jade Horsing", "720p", "3m", "—", False, True, True, "1d ago", 60, 5.6),
    ("Kiwi Econ", "720p", "3m", "—", False, True, True, "1d ago", 250, 15.3),
    ("Loop Archive", "1080p", "—", "—", False, True, True, "1d ago", 43, 7.4),
    ("Midnight Talkshow", "144p", "3m", "—", False, True, True, "1d ago", 5860, 70.0),
    ("Nova Space Science", "480p", "3m", "—", False, True, True, "1d ago", 635, 9.9),
    ("Oak Weekly", "720p", "3m", "—", False, True, True, "1d ago", 690, 60.0),
    ("Pine Reporting", "720p", "3m", "—", False, True, True, "1d ago", 205, 22.3),
    ("Quant Legal", "360p", "3m", "—", False, True, True, "1d ago", 630, 28.0),
    ("Rose Shorts", "best", "3m", "—", False, True, True, "1d ago", 10, 2.0),
    ("Sable", "720p", "3m", "—", False, True, True, "1d ago", 45, 12.0),
    ("Tangent Answers", "720p", "3m", "—", False, True, True, "1d ago", 1020, 58.0),
    ("Ursa Tech", "480p", "3m", "60m", False, True, True, "1d ago", 815, 25.0),
    ("Vertex Aviation", "480p", "3m", "50m", False, True, True, "1d ago", 460, 26.0),
]


def generate_subs_channels():
    """Return subs-table row dicts for sample rendering."""
    out = []
    for (folder, res, mn, mx, comp, trans, meta, last_sync, nv, gb) in SAMPLE_CHANNELS:
        out.append({
            "folder": folder,
            "res": res,
            "min": mn,
            "max": mx,
            "compress": "\u2713" if comp else "\u2014",
            "transcribe": "\u2713" if trans else "\u2014",
            "metadata": "\u2713" if meta else "\u2014",
            "last_sync": last_sync,
            "n_vids": f"{nv:,}",
            "size": f"{gb:.1f} GB",
        })
    total = sum(gb for _, _, _, _, _, _, _, _, _, gb in SAMPLE_CHANNELS)
    return out, f"Total: {total/1024:.1f} TB" if total > 1024 else f"Total: {total:.1f} GB"


# Generic fictional episode titles used in the preview log samples so the
# log rendering tests render something that looks like real work. None of
# these refer to real videos, channels, or personalities.
SAMPLE_TITLES = [
    "Season Finale Recap",
    "The Art of Understanding",
    "Weekly News Roundup",
    "Product Teardown: Model X",
    "Behind the Camera",
    "Building a Better Inbox",
    "Market Report - Q3",
    "Tabletop Tactics Ep. 12",
    "Kitchen Experiments",
    "Field Notes from the Coast",
    "Unboxing the Latest",
    "Deep Dive: Climate Models",
    "Live Q&A Session",
    "Quick Take: Industry News",
    "The Morning Brief",
    "Creator Corner Ep. 47",
    "Road Trip Diaries",
    "Sunday Long Form",
    "Tech Made Simple",
    "Walk and Talk - Episode 3",
    "Interview: A Conversation",
    "Bookshelf Review",
    "The Essay Pilot",
    "Late Night Ramble",
    "Mini-Series Part 1",
    "Mini-Series Part 2",
    "Community Spotlight",
    "Hot Take Tuesday",
    "Lecture: Fundamentals",
    "Workshop Recording",
    "Bonus Content: Outtakes",
    "Afterthoughts and Q&A",
]


def stream_main_log_sample(initial: bool = True) -> List[List[Segment]]:
    """
    Returns a list of lines. Each line is a list of segments.

    initial=True → big bulk for first paint
    initial=False → smaller stream (simulates live output)
    """
    lines: List[List[Segment]] = []

    if initial:
        # Header
        lines.append(_header_line("=== Autorun: sync pass starting @ 3:12pm, Apr 10 ==="))
        lines.append(_summary_line(" 13 channels queued, 6 workers, 1 GPU lane"))
        lines.append([["\n", None]])

        # Interleave metadata fetches + transcription done lines (matches screenshot 09)
        for i in range(20):
            title = random.choice(SAMPLE_TITLES)
            meta_idx = 720 + i
            meta_total = 11088
            tr_idx = 1565 + i
            tr_total = 2523
            dur_min = random.randint(2, 12)
            dur_sec = random.randint(10, 59)
            took_sec = random.randint(5, 24)
            realtime = round(random.uniform(25.0, 45.0), 1)

            if i % 2 == 0:
                lines.append(_metadata_line(meta_idx, meta_total, title))
            else:
                lines.append(_done_line(
                    tr_idx, tr_total, title,
                    f"{dur_min}m {dur_sec}s",
                    f"{took_sec}sec",
                    f"{realtime}x",
                ))

        # A currently-running transcribe progress line
        lines.append(_transcribing_line(1601, 2523, "Season Finale Recap", 30))

        # Summary block
        lines.append([["\n", None]])
        lines.append(_update_sep())
        lines.append(_header_line("─── End of pass: 23 fetched · 11,065 skipped · 2 errors ───"))
        lines.append([["Elapsed: ", "dim"], ["4m 18s", "summary"], ["\n", None]])
        lines.append([["\n", None]])

        # Kick off a redownload block
        lines.append([["REDOWNLOAD MODE: ", "simplestatus_redwnl"], ["FilmFan · 3 items\n", "dl_white"]])
        for i in range(3):
            title = random.choice(SAMPLE_TITLES)
            lines.append([
                ["[", "redwnl_bracket"],
                [f"{i+1}", "dl_white"],
                ["/", "redwnl_bracket"],
                ["3", "dl_white"],
                ["] ", "redwnl_bracket"],
                ["Redownloading ", "simpleline_redwnl"],
                [f'"{title}"', "simpleline"],
                ["\n", None],
            ])

        # Compress block
        lines.append([["\n", None]])
        lines.append([["COMPRESS MODE: ", "simplestatus_compress"], ["GameReviews · 12 items\n", "dl_white"]])
        for i in range(4):
            title = random.choice(SAMPLE_TITLES)
            lines.append(_encode_line(i + 1, 12, title, 20 + i * 20))

        # Reorg block
        lines.append([["\n", None]])
        lines.append([["REORG MODE: ", "simplestatus_reorg"], ["TravelVlogs · 48 items\n", "dl_white"]])
        for i in range(3):
            title = random.choice(SAMPLE_TITLES)
            lines.append([
                ["[", "reorg_bracket"],
                [f"{i+1}", "dl_white"],
                ["/", "reorg_bracket"],
                ["48", "dl_white"],
                ["] ", "reorg_bracket"],
                ["Moving ", "simpleline_reorg"],
                [f'"{title}"', "simpleline"],
                ["\n", None],
            ])

        # An error / update example
        lines.append([["\n", None]])
        lines.append([["Error: ", "red"], ["network timeout retrieving metadata for channel 'FilmFan' — retrying in 30s\n", "red"]])
        lines.append([["\n", None]])
        lines.append([["UPDATE AVAILABLE: ", "update_head"], ["v42.3 — see Releases\n", "update_sep"]])
        lines.append([["\n", None]])

        # Livestream + filterskip
        lines.append([["[Live] ", "livestream"], ["Upcoming stream detected: NewsLive — \"Evening Briefing\" starts in 12 min\n", "dl_white"]])
        lines.append([["[Skip] ", "filterskip"], ["short video filtered (< 60s): \"ScienceWeekly short #4928\"\n", "filterskip_dim"]])

        return lines

    # initial=False → short stream for simulated live updates
    stream: List[List[Segment]] = []
    for i in range(60):
        title = random.choice(SAMPLE_TITLES)
        pick = i % 4
        if pick == 0:
            stream.append(_metadata_line(800 + i, 11088, title))
        elif pick == 1:
            pct = random.randint(10, 99)
            stream.append(_transcribing_line(1700 + i, 2523, title, pct))
        elif pick == 2:
            dur = f"{random.randint(2, 12)}m {random.randint(10, 59)}s"
            took = f"{random.randint(5, 24)}sec"
            rt = f"{round(random.uniform(25.0, 45.0), 1)}x"
            stream.append(_done_line(1700 + i, 2523, title, dur, took, rt))
        else:
            pct = random.randint(10, 95)
            stream.append(_download_line(900 + i, 4000, title, pct))
    return stream
