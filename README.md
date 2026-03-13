# YTArchiverTool
A personal project to replace 4KVideoDownloader+ with some extra features that I needed built in, built entirely with Claude-Code.

I put some pictures on the wiki.

What started out as a simple script has turned into a somewhat complex YT-DLP GUI with some features I wanted added on top. I now use it in replacement of 4kVideoDownloader+ to download/archive YT channels.

Add channel to sub list

Choose resolution to download that channel at

Set Max/Min duration limits to cut out shorts/live

Choose starting date to download-(if you don't want the first few years of a channel for example)

Compress after download- if you want to download @ 1080 then compress to a higher quality 360p output than yt gives, for example

Auto-sync subscribed channels timer, with seperate log to show activity and history

Can organize/re-organize downloaded channels into Years/Months folders

can retroactively change file dates to YT upload dates- if you're switching over from 4kVD

Prioritizes slow & steady to be safe from YT IP ban

Uses Firefox for YT cookies (apparently getting them from chromium is harder?)

When downloading a large channel for the first time, it caches the video IDs. That way, if the download is cancelled or interupted, it doesn't have to re-scan the entire channel to find where it left off. On extremely large channels this can save 10+ minutes on resume.

you can of course still manually download single videos

seperage sync-tasks and gpu-tasks editable queues

Transcribe full channels, and have the option to transcribe new downloads from that channel. Uses YT auto-captions + punctuation model when possible, falls back to whisper (with model options, per channel)


this is maybe 2% me and 98% claude-code :)



I will list found but unpatched issues here:

1. 

2. 


