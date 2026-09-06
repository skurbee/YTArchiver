from __future__ import annotations

import subprocess
import unittest
from unittest import mock

from backend.sync import quickcheck

ROOT = "https://www.youtube.com/@ExampleChannel"
KNOWN = "KNOWNVIDEO1"
RECORDING = "RECORDING01"
LIVE = "LIVESTREAM1"
UPCOMING = "UPCOMING001"


class StreamChannelQuickcheckTests(unittest.TestCase):
    def setUp(self) -> None:
        self.cache = mock.patch.object(quickcheck, "_quickcheck_bad", {})
        self.cache.start()
        self.addCleanup(self.cache.stop)

    def probe(self, stdout, archived=(), *, url=ROOT, returncode=0, **kwargs):
        captured = []

        def run(command, **_kwargs):
            captured.append(command)
            # Model the missing Videos tab that motivated the regression.
            if command[-1].endswith("/videos"):
                return subprocess.CompletedProcess(command, 1, "", "No videos tab")
            return subprocess.CompletedProcess(command, returncode, stdout, "")

        with mock.patch.object(quickcheck, "find_yt_dlp", return_value="yt-dlp"), \
                mock.patch.object(quickcheck, "_find_cookie_source", return_value=[]), \
                mock.patch.object(quickcheck.youtube_traffic, "acquire",
                                  return_value={"ok": True}), \
                mock.patch("backend.youtube_session.handle_youtube_failure_text",
                           return_value=""), \
                mock.patch.object(quickcheck, "run_ytdlp", side_effect=run):
            result = quickcheck.quick_check_new_uploads(url, set(archived), **kwargs)
        return result, captured[0]

    def test_streams_only_channel_can_skip_after_recordings_are_archived(self):
        result, command = self.probe(f"{RECORDING}|||12000|||was_live\n", [RECORDING])
        self.assertFalse(result["has_new"])
        self.assertEqual(result["checked"], 1)
        self.assertEqual(command[-1], ROOT)

    def test_mixed_channel_finds_recording_after_archived_regular_upload(self):
        result, _command = self.probe(
            f"{KNOWN}|||600|||not_live\n{RECORDING}|||12000|||was_live\n", [KNOWN])
        self.assertTrue(result["has_new"])
        self.assertEqual(result["fresh_ids"], [RECORDING])

    def test_transient_streams_are_rechecked_when_they_become_recordings(self):
        for minimum in (0, 180):
            with self.subTest(minimum=minimum):
                result, command = self.probe(
                    f"{LIVE}|||0|||is_live\n{UPCOMING}|||0|||is_upcoming\n"
                    f"{RECORDING}|||0|||post_live\n", min_duration=minimum)
                self.assertFalse(result["has_new"])
                self.assertEqual(result["filtered_ids"], [])
                self.assertIn("%(id)s|||%(duration)s|||%(live_status)s", command)
                finished, _command = self.probe(
                    f"{LIVE}|||12000|||was_live\n", min_duration=minimum)
                self.assertEqual(finished["fresh_ids"], [LIVE])

    def test_short_finished_recording_obeys_duration_filter(self):
        result, _command = self.probe(f"{RECORDING}|||60|||was_live\n", min_duration=180)
        self.assertFalse(result["has_new"])
        self.assertEqual(result["filtered_ids"], [RECORDING])

    def test_partial_tab_failure_cannot_skip_channel(self):
        result, _command = self.probe(f"{KNOWN}|||600|||not_live\n", [KNOWN], returncode=1)
        self.assertTrue(result["has_new"])
        self.assertTrue(result["partial_probe"])

    def test_partial_result_retains_discovered_recording(self):
        result, _command = self.probe(f"{RECORDING}|||12000|||was_live\n", returncode=1)
        self.assertTrue(result["partial_probe"])
        self.assertEqual(result["fresh_ids"], [RECORDING])

    def test_overlapping_tabs_do_not_duplicate_video_ids(self):
        row = f"{RECORDING}|||12000|||was_live\n"
        result, _command = self.probe(row + row)
        self.assertEqual(result["checked"], 1)
        self.assertEqual(result["fresh_ids"], [RECORDING])

    def test_small_sample_does_not_cut_off_outer_channel_tabs(self):
        for count, expected in ((1, 3), (2, 3), (5, 5)):
            with self.subTest(count=count):
                _result, command = self.probe(
                    f"{KNOWN}|||600|||not_live\n", [KNOWN], check_count=count)
                self.assertEqual(command[command.index("--playlist-end") + 1], str(expected))

    def test_channel_tab_urls_use_root_including_old_permanent_urls(self):
        stable_root = "https://www.youtube.com/channel/UC" + "A" * 22
        for url, expected in ((ROOT + "/streams?view=0#top", ROOT),
                              (ROOT + "/videos/", ROOT),
                              ("@ExampleChannel", ROOT),
                              (stable_root + "/videos", stable_root)):
            with self.subTest(url=url):
                result, command = self.probe(f"{KNOWN}|||600|||not_live\n", [KNOWN], url=url)
                self.assertFalse(result["has_new"])
                self.assertEqual(command[-1], expected)

    def test_playlist_and_single_video_targets_keep_their_scope(self):
        for url in ("https://www.youtube.com/playlist?list=PLexample",
                    "https://www.youtube.com/watch?v=KNOWNVIDEO1"):
            with self.subTest(url=url):
                _result, command = self.probe(f"{KNOWN}\n", [KNOWN], url=url, check_count=1)
                self.assertEqual(command[-1], url)
                self.assertEqual(command[command.index("--playlist-end") + 1], "1")

    def test_empty_result_still_allows_full_sync(self):
        result, _command = self.probe("")
        self.assertTrue(result["has_new"])
        self.assertTrue(result["empty_probe"])


if __name__ == "__main__":
    unittest.main()
