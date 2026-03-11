# YTArchiverTool
A personal project to replace 4KVideoDownloader+ with some features that I needed, built entirely with Claude-Code.

What started out as a simple script has turned into a somewhat complex YT-DLP GUI with some features I wanted added on top. I now use it in replacement of 4kVideoDownloader+ to download/archive YT channels.

Sub to a channel-

Choose resolution to download that channel at-

Limit shortest/longest video to cut out shorts/live-

Choose starting date to download-(if you don't want the first few years of a channel for example)

Can organize (and re-organize!) downloaded channels into Years/Months

can retroactively change file dates to YT upload dates- if you're switching over from 4kVD

Prioritizes slow & steady to be safe from YT IP ban

Uses Firefox for YT cookies (apparently getting them from chromium is harder?)

When downloading a large channel for the first time, it caches the video IDs. That way, if the download is cancelled or interupted, it doesn't have to re-scan the entire channel to find where it left off. On extremely large channels this can save 10+ minutes on resume.

you can of course still manually download single videos

Auto-Sync with log to show history

editable job-queue

Transcribe channels and output it to .txt files (following org structure that channel is in)

this is maybe 2% me and 98% claude-code :)



I will list found but unpatched issues here:

1. the current method of comparing file title to video title for transcriptions is good for backlogs. however, a lot of channels change video titles in the first few days. So whats happening:
   Video gets downloaded, auto-transcribe gets added to GPU task list-

   in the time it takes the user to start the GPU task list, channel changes video title-

   so instead of using the basically free YT autosub + punctuation model, it's falling back to whisper which takes ~50x longer to process    per video.

   not a huge deal, but maybe we need to check for transcripts on a per video ID basis, and not Title

2. 


