from __future__ import annotations

import unittest
from unittest import mock

from backend import subs


class StreamChannelNameTests(unittest.TestCase):
    def _fetch_name(self, probe_output: str) -> str | None:
        proc = mock.Mock()
        proc.communicate.return_value = (probe_output + "\n", "")
        with mock.patch("backend.sync.find_yt_dlp", return_value="yt-dlp"), \
                mock.patch("backend.youtube_traffic.acquire", return_value={"ok": True}), \
                mock.patch("backend.process_runner.popen_ytdlp", return_value=proc), \
                mock.patch("backend.process_runner.PROCESS_REGISTRY.unregister"), \
                mock.patch("backend.sync._find_cookie_source") as cookies:
            name = subs.fetch_channel_display_name("https://www.youtube.com/@example")
        cookies.assert_not_called()
        return name

    def test_live_tab_title_uses_bare_channel_name(self) -> None:
        self.assertEqual(self._fetch_name("Example Channel - Live"), "Example Channel")

    def test_live_within_channel_name_is_preserved(self) -> None:
        self.assertEqual(self._fetch_name("Live Example Channel"), "Live Example Channel")
        self.assertEqual(self._fetch_name("Example - Live Sessions"), "Example - Live Sessions")


if __name__ == "__main__":
    unittest.main()
