from __future__ import annotations

import contextlib
import json
import tempfile
import threading
import unittest
from unittest import mock

from backend import channel_cache, channel_identity, process_runner, subs
from backend.sync import core as sync_core

CHANNEL_ID = "UC" + ("A" * 22)
OTHER_CHANNEL_ID = "UC" + ("B" * 22)
OLD_URL = "https://www.youtube.com/@OldHandle"
NEW_URL = "https://www.youtube.com/@NewHandle"


class ChannelIdentityParsingTests(unittest.TestCase):
    def test_channel_page_disclaimer_is_not_an_auth_verdict(self) -> None:
        text = (
            "ERROR: [youtube:tab] @OldHandle: Playlists that require "
            "authentication may not extract correctly without a successful "
            "webpage download"
        )

        self.assertTrue(channel_identity.is_channel_page_unavailable_error(text))
        self.assertFalse(channel_identity.is_channel_page_unavailable_error(
            "ERROR: cookies are no longer valid"))

    def test_channel_404_is_a_recovery_trigger(self) -> None:
        text = (
            "ERROR: [youtube:tab] @OldHandle: Unable to download webpage: "
            "HTTP Error 404: Not Found"
        )

        self.assertTrue(channel_identity.is_channel_page_unavailable_error(text))

    def test_parse_channel_track_keeps_exact_permanent_id(self) -> None:
        parsed = channel_identity.parse_channel_track_line(
            f"CHTRACK:::{CHANNEL_ID}:::@NewHandle:::"
            "https://www.youtube.com/@NewHandle:::New Name"
        )

        self.assertEqual(parsed, {
            "channel_id": CHANNEL_ID,
            "uploader_id": "@NewHandle",
            "uploader_url": NEW_URL,
            "channel_name": "New Name",
        })


class StaleHandleRecoveryTests(unittest.TestCase):
    def _rows(self, channel_id: str = CHANNEL_ID) -> list[dict[str, str]]:
        return [
            {
                "video_id": "AAAAAAAAAA1",
                "channel_id": channel_id,
                "uploader_id": "@NewHandle",
                "uploader_url": NEW_URL,
                "channel_name": "New Name",
            },
            {
                "video_id": "BBBBBBBBBB2",
                "channel_id": channel_id,
                "uploader_id": "@NewHandle",
                "uploader_url": NEW_URL,
                "channel_name": "New Name",
            },
        ]

    def test_legacy_recovery_requires_two_videos_and_candidate_id_match(self) -> None:
        committed = {
            "ok": True,
            "changed": True,
            "url_changed": True,
            "old_url": OLD_URL,
            "new_url": NEW_URL,
            "channel_id": CHANNEL_ID,
            "channel": {"name": "Saved Name", "url": NEW_URL,
                        "channel_id": CHANNEL_ID},
        }
        with mock.patch.object(
                channel_identity, "_known_video_ids",
                return_value=["AAAAAAAAAA1", "BBBBBBBBBB2"]), \
                mock.patch.object(
                    channel_identity, "_probe_context",
                    return_value={"ok": True}), \
                mock.patch.object(
                    channel_identity, "_probe_video_identities",
                    return_value=self._rows()), \
                mock.patch.object(
                    channel_identity, "_verify_candidate_url",
                    return_value=CHANNEL_ID) as verify, \
                mock.patch.object(
                    subs, "update_verified_channel_identity",
                    return_value=committed) as persist:
            result = channel_identity.recover_stale_channel(
                {"name": "Saved Name", "url": OLD_URL})

        self.assertTrue(result["ok"])
        self.assertTrue(result["verified"])
        verify.assert_called_once()
        self.assertEqual(verify.call_args.args[0], NEW_URL)
        self.assertTrue(verify.call_args.kwargs["context"]["ok"])
        self.assertEqual(
            persist.call_args.kwargs["channel_id"], CHANNEL_ID)
        self.assertEqual(persist.call_args.kwargs["current_url"], NEW_URL)

    def test_recovery_refuses_disagreeing_archived_videos(self) -> None:
        rows = self._rows()
        rows[1] = {**rows[1], "channel_id": OTHER_CHANNEL_ID}
        with mock.patch.object(
                channel_identity, "_known_video_ids",
                return_value=["AAAAAAAAAA1", "BBBBBBBBBB2"]), \
                mock.patch.object(
                    channel_identity, "_probe_context",
                    return_value={"ok": True}), \
                mock.patch.object(
                    channel_identity, "_probe_video_identities",
                    return_value=rows), \
                mock.patch.object(
                    channel_identity, "_verify_candidate_url") as verify, \
                mock.patch.object(
                    subs, "update_verified_channel_identity") as persist:
            result = channel_identity.recover_stale_channel(
                {"name": "Saved Name", "url": OLD_URL})

        self.assertFalse(result["ok"])
        verify.assert_not_called()
        persist.assert_not_called()

    def test_recovery_refuses_one_video_for_legacy_channel(self) -> None:
        with mock.patch.object(
                channel_identity, "_known_video_ids",
                return_value=["AAAAAAAAAA1", "BBBBBBBBBB2"]), \
                mock.patch.object(
                    channel_identity, "_probe_context",
                    return_value={"ok": True}), \
                mock.patch.object(
                    channel_identity, "_probe_video_identities",
                    return_value=self._rows()[:1]), \
                mock.patch.object(
                    subs, "update_verified_channel_identity") as persist:
            result = channel_identity.recover_stale_channel(
                {"name": "Saved Name", "url": OLD_URL})

        self.assertFalse(result["ok"])
        persist.assert_not_called()

    def test_saved_id_mismatch_is_never_repaired(self) -> None:
        with mock.patch.object(
                channel_identity, "_known_video_ids",
                return_value=["AAAAAAAAAA1"]), \
                mock.patch.object(
                    channel_identity, "_probe_context",
                    return_value={"ok": True}), \
                mock.patch.object(
                    channel_identity, "_probe_video_identities",
                    return_value=self._rows(OTHER_CHANNEL_ID)[:1]), \
                mock.patch.object(
                    channel_identity, "_verify_candidate_url") as verify, \
                mock.patch.object(
                    subs, "update_verified_channel_identity") as persist:
            result = channel_identity.recover_stale_channel({
                "name": "Saved Name",
                "url": OLD_URL,
                "channel_id": CHANNEL_ID,
            })

        self.assertFalse(result["ok"])
        verify.assert_not_called()
        persist.assert_not_called()

    def test_pause_after_candidate_check_prevents_config_commit(self) -> None:
        pause = threading.Event()

        def _verified_then_paused(_url, *, context):
            pause.set()
            return CHANNEL_ID

        with mock.patch.object(
                channel_identity, "_known_video_ids",
                return_value=["AAAAAAAAAA1", "BBBBBBBBBB2"]), \
                mock.patch.object(
                    channel_identity, "_probe_context",
                    return_value={"ok": True, "pause_event": pause}), \
                mock.patch.object(
                    channel_identity, "_probe_video_identities",
                    return_value=self._rows()), \
                mock.patch.object(
                    channel_identity, "_verify_candidate_url",
                    side_effect=_verified_then_paused), \
                mock.patch.object(
                    subs, "update_verified_channel_identity") as persist:
            result = channel_identity.recover_stale_channel(
                {"name": "Saved Name", "url": OLD_URL}, pause_event=pause)

        self.assertFalse(result["ok"])
        self.assertTrue(result["paused"])
        persist.assert_not_called()

    def test_skip_after_candidate_check_prevents_config_commit(self) -> None:
        skip = threading.Event()

        def _verified_then_skipped(_url, *, context):
            skip.set()
            return CHANNEL_ID

        with mock.patch.object(
                channel_identity, "_known_video_ids",
                return_value=["AAAAAAAAAA1", "BBBBBBBBBB2"]), \
                mock.patch.object(
                    channel_identity, "_probe_context",
                    return_value={"ok": True}), \
                mock.patch.object(
                    channel_identity, "_probe_video_identities",
                    return_value=self._rows()), \
                mock.patch.object(
                    channel_identity, "_verify_candidate_url",
                    side_effect=_verified_then_skipped), \
                mock.patch.object(
                    subs, "update_verified_channel_identity") as persist:
            result = channel_identity.recover_stale_channel(
                {"name": "Saved Name", "url": OLD_URL}, kill_current=skip)

        self.assertFalse(result["ok"])
        self.assertTrue(result["skipped"])
        persist.assert_not_called()


class InterruptibleProbeTests(unittest.TestCase):
    def test_probe_uses_utf8_registered_process_and_pause_stop_token(self) -> None:
        pause = threading.Event()
        proc = mock.Mock()

        def _supervise(_proc, **kwargs):
            pause.set()
            self.assertTrue(kwargs["cancel_event"].is_set())
            return process_runner.StreamingRunResult(
                -15, [], cancelled=True)

        with mock.patch.object(
                channel_identity.youtube_traffic, "acquire",
                return_value={"ok": True}), \
                mock.patch.object(
                    channel_identity, "popen_ytdlp", return_value=proc) as popen, \
                mock.patch.object(
                    channel_identity, "supervise_streaming_process",
                    side_effect=_supervise):
            result = channel_identity._run_identity_probe(
                ["yt-dlp", "url"],
                context={"pause_event": pause, "task_id": "task-1"},
                timeout=30,
                traffic_kind="channel_identity_verify",
            )

        self.assertFalse(result["ok"])
        self.assertTrue(result["paused"])
        kwargs = popen.call_args.kwargs
        self.assertEqual(kwargs["encoding"], "utf-8")
        self.assertEqual(kwargs["errors"], "replace")
        self.assertEqual(kwargs["env"]["PYTHONUTF8"], "1")
        self.assertEqual(kwargs["owner"], "sync")
        self.assertEqual(kwargs["task_id"], "task-1")
        self.assertEqual(kwargs["role"], "channel-identity")

    def test_real_traffic_governor_wait_is_interrupted_by_skip(self) -> None:
        skip = threading.Event()
        wakeup = threading.Event()
        timer = threading.Timer(0.02, skip.set)
        waiting = {
            "impossible": False,
            "allowed": False,
            "wait_seconds": 30.0,
            "wait_reason": "spacing",
        }
        with mock.patch.object(
                channel_identity.youtube_traffic, "load_config",
                return_value={}), \
                mock.patch.object(
                    channel_identity.youtube_traffic, "budget_override_active",
                    return_value=False), \
                mock.patch.object(
                    channel_identity.youtube_traffic, "circuit_state",
                    return_value={"active": False}), \
                mock.patch.object(
                    channel_identity.youtube_traffic, "eligibility",
                    return_value=waiting), \
                mock.patch.object(
                    channel_identity.youtube_traffic, "_read_events_locked"), \
                mock.patch.object(
                    channel_identity.youtube_traffic, "_set_wait_state"), \
                mock.patch.object(
                    channel_identity.youtube_traffic, "_override_wakeup", wakeup), \
                mock.patch.object(
                    channel_identity.youtube_traffic, "_reservations", {}), \
                mock.patch.object(channel_identity, "popen_ytdlp") as popen:
            timer.start()
            try:
                result = channel_identity._run_identity_probe(
                    ["yt-dlp", "url"],
                    context={"kill_current": skip},
                    timeout=30,
                    traffic_kind="channel_identity_verify",
                )
            finally:
                timer.cancel()
                timer.join(timeout=1)

        self.assertFalse(result["ok"])
        self.assertTrue(result["skipped"])
        popen.assert_not_called()


class CandidateUrlVerificationTests(unittest.TestCase):
    """The channel-identity check must not depend on one video's availability.

    Regression: verification ran `--playlist-end 1` WITHOUT `--flat-playlist`,
    so yt-dlp fully extracted the newest upload. A members-only, age-gated,
    geo-blocked, premiering or live newest video therefore failed the whole
    check and blocked that channel from syncing at all.
    """

    def _run(self, stdout: str):
        captured = {}

        def _probe(command, **kwargs):
            captured["command"] = command
            return {"ok": True, "stdout": stdout}

        with mock.patch.object(
                channel_identity, "_run_identity_probe", side_effect=_probe):
            resolved = channel_identity._verify_candidate_url(
                "https://www.youtube.com/@handle",
                context={"ok": True, "yt": "yt-dlp", "cookies": []},
            )
        return resolved, captured["command"]

    def test_verification_never_extracts_an_individual_video(self) -> None:
        _resolved, command = self._run(f"CHVERIFY:::{CHANNEL_ID}:::x")

        self.assertIn("--flat-playlist", command)
        self.assertIn("--ignore-errors", command)
        # Playlist-scoped print: the ID is read off the channel itself.
        self.assertIn(
            "playlist:CHVERIFY:::%(channel_id)s:::%(uploader_url)s", command)
        self.assertEqual(command[-1], "https://www.youtube.com/@handle")

    def test_streams_only_channel_verifies_without_a_videos_tab(self) -> None:
        def _probe(command, **kwargs):
            if command[-1].endswith("/videos"):
                return {
                    "ok": False, "stdout": "",
                    "error": "This channel does not have a videos tab",
                }
            self.assertEqual(command[-1], "https://www.youtube.com/@streams_only")
            return {
                "ok": True,
                "stdout": f"CHVERIFY:::{CHANNEL_ID}:::https://www.youtube.com/@streams_only",
            }

        with mock.patch.object(
                channel_identity, "_run_identity_probe", side_effect=_probe):
            resolved = channel_identity._verify_candidate_url(
                "@streams_only",
                context={"ok": True, "yt": "yt-dlp", "cookies": []},
            )

        self.assertEqual(resolved, CHANNEL_ID)

    def test_playlist_scoped_identity_is_accepted(self) -> None:
        resolved, _command = self._run(f"CHVERIFY:::{CHANNEL_ID}:::x")
        self.assertEqual(resolved, CHANNEL_ID)

    def test_entry_scoped_line_still_verifies_when_playlist_scope_is_empty(
            self) -> None:
        resolved, _command = self._run(
            f"CHVERIFY:::NA:::NA\nCHVERIFY:::{CHANNEL_ID}:::x")
        self.assertEqual(resolved, CHANNEL_ID)

    def test_unverifiable_channel_still_fails_closed(self) -> None:
        resolved, _command = self._run("ERROR: members-only content\n")
        self.assertEqual(resolved, "")


class LegacyIdentityPreflightTests(unittest.TestCase):
    def test_url_bound_history_is_verified_before_first_binding(self) -> None:
        repaired = {
            "ok": True,
            "channel": {"url": NEW_URL, "channel_id": CHANNEL_ID},
        }
        evidence = ["AAAAAAAAAA1", "BBBBBBBBBB2"]
        with mock.patch.object(
                channel_identity, "_known_video_ids", return_value=evidence), \
                mock.patch.object(
                    channel_identity, "recover_stale_channel",
                    return_value=repaired) as recover:
            result = channel_identity.preflight_channel_identity(
                {"name": "Saved Name", "url": OLD_URL})

        self.assertIs(result, repaired)
        self.assertEqual(
            recover.call_args.kwargs["evidence_video_ids"], evidence)

    def test_one_url_bound_video_starts_verified_legacy_binding(self) -> None:
        with mock.patch.object(
                channel_identity, "_known_video_ids",
                return_value=["AAAAAAAAAA1"]), \
                mock.patch.object(
                    channel_identity, "recover_stale_channel",
                    return_value={"ok": False, "error": "probe failed"},
                ) as recover:
            result = channel_identity.preflight_channel_identity(
                {"name": "Saved Name", "url": OLD_URL})

        # One video is enough to ATTEMPT the binding; recover_stale_channel
        # still has to prove the replacement URL resolves to that video's
        # permanent channel ID, so a failed probe fails closed.
        recover.assert_called_once()
        self.assertEqual(
            recover.call_args.kwargs["evidence_video_ids"], ["AAAAAAAAAA1"])
        self.assertFalse(result["ok"])

    def test_existing_archive_without_url_provenance_fails_closed(self) -> None:
        with mock.patch.object(
                channel_identity, "_known_video_ids", return_value=[]):
            result = channel_identity.preflight_channel_identity({
                "name": "Saved Name",
                "url": OLD_URL,
                "initialized": True,
            })

        self.assertFalse(result["ok"])

    def test_new_or_intentionally_rebound_url_may_first_bind(self) -> None:
        with mock.patch.object(
                channel_identity, "_known_video_ids", return_value=[]):
            new_result = channel_identity.preflight_channel_identity(
                {"name": "New", "url": OLD_URL})
        with mock.patch.object(
                channel_identity, "_known_video_ids") as known:
            rebind_result = channel_identity.preflight_channel_identity({
                "name": "Rebound",
                "url": OLD_URL,
                "initialized": True,
                "channel_identity_rebind_pending": True,
            })

        self.assertTrue(new_result["first_bind_allowed"])
        self.assertTrue(rebind_result["first_bind_allowed"])
        known.assert_not_called()


class VerifiedIdentityPersistenceTests(unittest.TestCase):
    def test_url_identity_key_ignores_cosmetics_but_not_id_case(self) -> None:
        self.assertEqual(
            subs._channel_url_identity_key(
                "youtube.com/@Foo/videos?view=0#ignored"),
            subs._channel_url_identity_key("https://m.youtube.com/@foo/"),
        )
        case_changed_id = "UC" + ("a" + ("A" * 21))
        self.assertNotEqual(
            subs._channel_url_identity_key(
                f"https://www.youtube.com/channel/{CHANNEL_ID}/videos"),
            subs._channel_url_identity_key(
                f"https://youtube.com/channel/{case_changed_id}"),
        )

    def test_verified_url_and_id_commit_together(self) -> None:
        cfg = {"channels": [{
            "name": "Saved Name",
            "url": OLD_URL,
            "channel_identity_rebind_pending": True,
        }]}
        with mock.patch.object(
                subs, "config_transaction",
                return_value=contextlib.nullcontext(cfg)), \
                mock.patch.object(channel_cache, "move_url") as move, \
                mock.patch("backend.archive_scan.invalidate_channel"):
            result = subs.update_verified_channel_identity(
                {"url": OLD_URL, "name": "Saved Name"},
                expected_url=OLD_URL,
                channel_id=CHANNEL_ID,
                current_url=NEW_URL,
            )

        self.assertTrue(result["url_changed"])
        self.assertEqual(cfg["channels"][0]["url"], NEW_URL)
        self.assertEqual(cfg["channels"][0]["channel_id"], CHANNEL_ID)
        self.assertNotIn(
            "channel_identity_rebind_pending", cfg["channels"][0])
        move.assert_called_once_with(OLD_URL, NEW_URL)

    def test_verified_update_refuses_saved_id_mismatch(self) -> None:
        cfg = {"channels": [{
            "name": "Saved Name",
            "url": OLD_URL,
            "channel_id": OTHER_CHANNEL_ID,
        }]}
        original = dict(cfg["channels"][0])
        with mock.patch.object(
                subs, "config_transaction",
                return_value=contextlib.nullcontext(cfg)):
            with self.assertRaises(subs.SubsError):
                subs.update_verified_channel_identity(
                    {"url": OLD_URL, "name": "Saved Name"},
                    expected_url=OLD_URL,
                    channel_id=CHANNEL_ID,
                    current_url=NEW_URL,
                )

        self.assertEqual(cfg["channels"][0], original)

    def test_verified_update_refuses_embedded_url_id_case_mismatch(self) -> None:
        case_changed_id = "UC" + ("a" + ("A" * 21))
        permanent_url = f"https://www.youtube.com/channel/{CHANNEL_ID}"
        cfg = {"channels": [{"name": "Saved Name", "url": permanent_url}]}
        original = dict(cfg["channels"][0])
        with mock.patch.object(
                subs, "config_transaction",
                return_value=contextlib.nullcontext(cfg)):
            with self.assertRaises(subs.SubsError):
                subs.update_verified_channel_identity(
                    {"url": permanent_url, "name": "Saved Name"},
                    expected_url=permanent_url,
                    channel_id=case_changed_id,
                    current_url=NEW_URL,
                )

        self.assertEqual(cfg["channels"][0], original)

    def test_verified_update_moves_exact_saved_cache_key(self) -> None:
        exact_saved_url = OLD_URL + "/videos/?view=0"
        cfg = {"channels": [{"name": "Saved Name", "url": exact_saved_url}]}
        with mock.patch.object(
                subs, "config_transaction",
                return_value=contextlib.nullcontext(cfg)), \
                mock.patch.object(channel_cache, "move_url") as move, \
                mock.patch("backend.archive_scan.invalidate_channel"):
            result = subs.update_verified_channel_identity(
                {"url": OLD_URL, "name": "Saved Name"},
                expected_url=OLD_URL,
                channel_id=CHANNEL_ID,
                current_url=NEW_URL,
            )

        self.assertTrue(result["url_changed"])
        move.assert_called_once_with(exact_saved_url, NEW_URL)

    def test_cosmetic_url_variant_saves_id_without_rewriting_url(self) -> None:
        saved_url = "https://youtube.com/@Foo/videos?view=0"
        cfg = {"channels": [{"name": "Saved Name", "url": saved_url}]}
        with mock.patch.object(
                subs, "config_transaction",
                return_value=contextlib.nullcontext(cfg)), \
                mock.patch.object(channel_cache, "move_url") as move:
            result = subs.update_verified_channel_identity(
                {"url": "https://www.youtube.com/@foo", "name": "Saved Name"},
                expected_url="https://www.youtube.com/@foo",
                channel_id=CHANNEL_ID,
                current_url="https://m.youtube.com/@FOO/",
            )

        self.assertFalse(result["url_changed"])
        self.assertEqual(cfg["channels"][0]["url"], saved_url)
        self.assertEqual(cfg["channels"][0]["channel_id"], CHANNEL_ID)
        move.assert_not_called()

    def test_permanent_url_learning_preserves_url_without_notice(self) -> None:
        permanent_url = f"https://www.youtube.com/channel/{CHANNEL_ID}"
        cfg = {"channels": [{"name": "Saved Name", "url": permanent_url}]}
        with mock.patch.object(
                subs, "config_transaction",
                return_value=contextlib.nullcontext(cfg)), \
                mock.patch.object(channel_cache, "move_url") as move:
            result = subs.update_verified_channel_identity(
                {"url": permanent_url, "name": "Saved Name"},
                expected_url=permanent_url,
                channel_id=CHANNEL_ID,
                current_url=NEW_URL,
            )

        self.assertFalse(result["url_changed"])
        self.assertEqual(cfg["channels"][0]["url"], permanent_url)
        self.assertNotIn("channel_id", cfg["channels"][0])
        move.assert_not_called()

    def test_manual_url_change_discards_old_permanent_id(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = {
                "output_dir": td,
                "channels": [{
                    "name": "Saved Name",
                    "folder": "Saved Name",
                    "url": OLD_URL,
                    "channel_id": CHANNEL_ID,
                    "resolution": "720",
                    "mode": "new",
                    "min_duration": 0,
                    "max_duration": 0,
                }],
            }
            payload = {
                "folder": "Saved Name",
                "url": "https://www.youtube.com/@DifferentChannel",
                "resolution": "720",
                "range": "subscribe",
                "folder_org": "flat",
                "auto_transcribe": False,
                "auto_metadata": True,
                "compress_enabled": False,
            }
            with mock.patch.object(subs, "load_config", return_value=cfg), \
                    mock.patch.object(
                        subs, "config_transaction",
                        return_value=contextlib.nullcontext(cfg)):
                result = subs.update_channel({"url": OLD_URL}, payload)

        self.assertNotIn("channel_id", result)
        self.assertNotIn("channel_id", cfg["channels"][0])
        self.assertTrue(result["channel_identity_rebind_pending"])

    def test_manual_cosmetic_url_change_preserves_permanent_id(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = {
                "output_dir": td,
                "channels": [{
                    "name": "Saved Name",
                    "folder": "Saved Name",
                    "url": "https://www.youtube.com/@SavedHandle",
                    "channel_id": CHANNEL_ID,
                    "resolution": "720",
                    "mode": "new",
                    "min_duration": 0,
                    "max_duration": 0,
                }],
            }
            payload = {
                "folder": "Saved Name",
                "url": "youtube.com/@savedhandle/videos?view=0",
                "resolution": "720",
                "range": "subscribe",
                "folder_org": "flat",
                "auto_transcribe": False,
                "auto_metadata": True,
                "compress_enabled": False,
            }
            with mock.patch.object(subs, "load_config", return_value=cfg), \
                    mock.patch.object(
                        subs, "config_transaction",
                        return_value=contextlib.nullcontext(cfg)):
                result = subs.update_channel(
                    {"url": "https://www.youtube.com/@SavedHandle"}, payload)

        self.assertEqual(result["channel_id"], CHANNEL_ID)
        self.assertEqual(cfg["channels"][0]["channel_id"], CHANNEL_ID)


class ChannelCacheMoveTests(unittest.TestCase):
    def setUp(self) -> None:
        self._old_cache = channel_cache._cache
        self._old_loaded = channel_cache._loaded
        channel_cache._loaded = True
        channel_cache._cache = {
            OLD_URL: {
                "last_refreshed": 10.0,
                "ids": ["AAAAAAAAAA1", "BBBBBBBBBB2"],
                "filtered_ids": {"0:60": ["CCCCCCCCCC3"]},
            },
            NEW_URL: {
                "last_refreshed": 20.0,
                "ids": ["BBBBBBBBBB2", "DDDDDDDDDD4"],
                "filtered_ids": {"0:60": ["EEEEEEEEEE5"]},
            },
        }

    def tearDown(self) -> None:
        channel_cache._cache = self._old_cache
        channel_cache._loaded = self._old_loaded

    def test_move_url_merges_ids_and_filter_history(self) -> None:
        with mock.patch.object(channel_cache, "_save_locked", return_value=True):
            self.assertTrue(channel_cache.move_url(OLD_URL, NEW_URL))

        self.assertNotIn(OLD_URL, channel_cache._cache)
        merged = channel_cache._cache[NEW_URL]
        self.assertEqual(merged["last_refreshed"], 20.0)
        self.assertEqual(merged["ids"], [
            "AAAAAAAAAA1", "BBBBBBBBBB2", "DDDDDDDDDD4"])
        self.assertEqual(merged["filtered_ids"]["0:60"], [
            "CCCCCCCCCC3", "EEEEEEEEEE5"])

    def test_move_url_restores_memory_when_save_fails(self) -> None:
        before = json.loads(json.dumps(channel_cache._cache))
        with mock.patch.object(channel_cache, "_save_locked", return_value=False):
            self.assertFalse(channel_cache.move_url(OLD_URL, NEW_URL))

        self.assertEqual(channel_cache._cache, before)


class SyncRetryTests(unittest.TestCase):
    def test_verified_handle_change_retries_exactly_once(self) -> None:
        first = sync_core.SyncResult(
            ok=False,
            reason="channel_page_unavailable",
            downloaded=0,
            errors=1,
            channel_tracks=[],
        )
        second = sync_core.SyncResult(
            ok=True,
            downloaded=0,
            errors=0,
            channel_tracks=[],
        )
        channel = {"name": "Saved Name", "url": OLD_URL, "task_id": "task-1"}
        repaired = {
            "ok": True,
            "url_changed": True,
            "old_url": OLD_URL,
            "new_url": NEW_URL,
            "channel_id": CHANNEL_ID,
            "channel": {
                "name": "Saved Name",
                "url": NEW_URL,
                "channel_id": CHANNEL_ID,
            },
        }
        stream = mock.Mock()
        with mock.patch.object(
                sync_core, "_sync_channel_impl",
                side_effect=[first, second]) as run, \
                mock.patch.object(
                    channel_identity, "recover_stale_channel",
                    return_value=repaired), \
                mock.patch.object(channel_identity, "emit_url_changed") as emit:
            result = sync_core.sync_channel(channel, stream)

        self.assertIs(result, second)
        self.assertEqual(run.call_count, 2)
        retried_channel = run.call_args_list[1].args[0]
        self.assertEqual(retried_channel["url"], NEW_URL)
        self.assertEqual(retried_channel["channel_id"], CHANNEL_ID)
        self.assertEqual(retried_channel["task_id"], "task-1")
        emit.assert_called_once_with(stream, repaired)

    def test_unverified_change_does_not_retry_or_emit_control(self) -> None:
        first = sync_core.SyncResult(
            ok=False,
            reason="channel_page_unavailable",
            downloaded=0,
            errors=1,
            channel_tracks=[],
        )
        stream = mock.Mock()
        with mock.patch.object(
                sync_core, "_sync_channel_impl", return_value=first) as run, \
                mock.patch.object(
                    channel_identity, "recover_stale_channel",
                    return_value={"ok": False, "error": "ID mismatch"}), \
                mock.patch.object(channel_identity, "emit_url_changed") as emit:
            result = sync_core.sync_channel(
                {"name": "Saved Name", "url": OLD_URL}, stream)

        self.assertIs(result, first)
        run.assert_called_once()
        emit.assert_not_called()
        stream.emit_error.assert_called_once()

    def test_already_committed_recovery_retries_without_false_error(self) -> None:
        first = sync_core.SyncResult(
            ok=False,
            reason="channel_page_unavailable",
            downloaded=0,
            errors=1,
            channel_tracks=[],
        )
        second = sync_core.SyncResult(
            ok=True,
            downloaded=0,
            errors=0,
            channel_tracks=[],
        )
        recovery = {
            "ok": True,
            "changed": False,
            "url_changed": False,
            "channel": {
                "name": "Saved Name",
                "url": NEW_URL,
                "channel_id": CHANNEL_ID,
            },
        }
        stream = mock.Mock()
        with mock.patch.object(
                sync_core, "_sync_channel_impl",
                side_effect=[first, second]) as run, \
                mock.patch.object(
                    channel_identity, "recover_stale_channel",
                    return_value=recovery), \
                mock.patch.object(channel_identity, "emit_url_changed") as emit:
            result = sync_core.sync_channel(
                {"name": "Saved Name", "url": OLD_URL}, stream)

        self.assertIs(result, second)
        self.assertEqual(run.call_count, 2)
        emit.assert_not_called()
        stream.emit_error.assert_not_called()


class ObservedIdentityTests(unittest.TestCase):
    def test_saved_channel_flags_disagreeing_markers_as_identity_mismatch(self) -> None:
        tracks = [
            {"channel_id": CHANNEL_ID, "uploader_url": NEW_URL},
            {"channel_id": OTHER_CHANNEL_ID, "uploader_url": NEW_URL},
        ]

        result = channel_identity.record_observed_identity(
            {"url": OLD_URL, "channel_id": CHANNEL_ID}, tracks)

        self.assertFalse(result["ok"])
        self.assertTrue(result["identity_mismatch"])


if __name__ == "__main__":
    unittest.main()
