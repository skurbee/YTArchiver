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
All Claude-Code.



current (found) issues:
1. Simple log mode does not notify the user that a channel was skipped due to daily limit

2. last full sync timer sometimes resetting when single channel sync is performed

3. Channels that hit daily limit are not itemized in the log when running a full subbed channel sync, even in verbose log mode

4. Does not check that channel /dir still exists in any way (in case user deletes outside of program/manually)

5. When removing channel from sub list & removing it's videos from DL blocklist, it often fails to do so, and videos will not redownload if channel is added back.

6. It's supposed to run the subbed channels alphabetically, randomly does them out of order for some reason though. It feels like manually clicking sync-subbed does them in the right order, but when auto-sync fires it does not. Not sure if that's the exact correlation though.
