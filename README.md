# YTArchiverTool
Claude-Code + 30hrs of time + $150 in extra usage = 4kVideoDownloader+ replacement.


What started out as a simple script to re-date video files and trim ones over/under a certain length has turned into a somewhat complex YT-DLP GUI with some features added on top. I now use it in replacement of 4kVideoDownloader+.

Sub to a channel-
Choose res to download-
Limit shortest/longest video to cut out shorts/live-
Chose starting date to download-(if you don't want the first few years of a channel for example)
Can organize (and re-organize!) downloaded channels
Prioritizes slow & steady to be extra safe from YT IP ban
Uses Firefox for YT cookies (apparently getting them from chromium is harder?)

When downloading a large channel for the first time, it hashes the video IDs to a cache. It then only downloads a certain number of videos from that channel per day. It uses the cached video IDs to pick back up where it left off the next day, without having to re-scan the whole channel. 
This is currently set to a very aggressive 1000 a day, but that's for my use case of downloading 144p stuff lol

you can of course still manually download single videos


I didn't write any of this code (except for manually changing the label on things or changing padding or whatever)
All Claude-Code. Cost me about $150 in extra use credits, and about 30 real world hours testing and tweaking lol
