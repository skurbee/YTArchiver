# YTArchiverTool
A personal project to replace 4KVideoDownloader+, with some extra features that I needed built in.
Built with Claude-Code and a lil copilot

I put some pictures on the wiki.

I now use it to download/transcribe/compress/archive full YT channels. 

USE:

Add channel to sub list

Choose resolution to download that channel at

Set Max/Min duration limits to cut out shorts/live

Choose starting date to download- if you don't want the first few years of a channel, for example

Compress after download- if you want to download @ 1080 then compress to a higher quality 360p output than yt gives, for example

Auto-sync subscribed channels timer, with seperate log to show activity and history

Can organize/re-organize downloaded channels into Years/Months folders

can retroactively change file dates to YT upload dates- if you're switching over from 4kVD

Prioritizes slow & steady to be safe from YT IP ban

Uses Firefox for YT cookies (apparently getting them from chromium is harder?)

When downloading a large channel for the first time, it caches the video IDs. That way, if the download is cancelled or interupted, it doesn't have to re-scan the entire channel to find where it left off. On extremely large channels this can save 10+ minutes on resume.

you can of course still manually download single videos

seperage sync-tasks and gpu-tasks editable queues

Transcribe full channels, and have the option to transcribe new downloads from that channel. Uses YT auto-captions + punctuation model when possible, falls back to whisper (with model options, per channel).
Outputs into 'readable' .txt files, and hidden .json files with the same info, but with timestamps and things included. 

on download fail, it pings a few things to test internet. if it's out, it auto-pauses everything until back online

Uses CUDA for all GPU stuff. 

this is maybe 5% me, 85% claude code, 10% copilot

I use it packed in an .exe with the icon & dependancies, but if the icon is just in the same /dir it'll still work. It's for the taskbar

Once I feel like this is truly """done""" I'll upload the .exe


