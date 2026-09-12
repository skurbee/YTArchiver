"""Saved discovery is disposable; malformed or incomplete input never wins."""
import copy
import json
from concurrent.futures import ThreadPoolExecutor

import pytest

from backend.services import sidecar_store
from backend.sync import discovery_manifest as manifests

CHANNEL = "UC" + "A" * 22
OTHER_CHANNEL = "UC" + "B" * 22
URL = f"https://www.youtube.com/channel/{CHANNEL}/videos"


def video(video_id="fixture0001", **extra):
    return {
        "_type": "url", "ie_key": "Youtube", "id": video_id,
        "url": f"https://www.youtube.com/watch?v={video_id}",
        "channel_id": CHANNEL, "title": "Example video", "duration": 181,
        "live_status": "not_live", **extra,
    }


def playlist(entries=None, **extra):
    return {"_type": "playlist", "channel_id": CHANNEL, "id": CHANNEL,
            "entries": [video()] if entries is None else entries, **extra}


@pytest.fixture
def cache(tmp_path, monkeypatch):
    monkeypatch.setattr(manifests.ytarchiver_config, "APP_DATA_DIR", tmp_path / "profile")
    monkeypatch.setattr(manifests.ytarchiver_config, "config_is_writable", lambda: True)
    destination = str(tmp_path / "archive")
    key = manifests.context_key(CHANNEL, URL, destination)

    class Cache:
        path = manifests.manifest_path(key)

        def save(self, raw=None):
            return manifests.save_manifest(key, CHANNEL, URL, destination,
                                           playlist() if raw is None else raw)

        def load(self):
            return manifests.load_manifest(key, CHANNEL, URL, destination)

        def corrupt(self, mutate):
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            mutate(raw)
            self.path.write_text(json.dumps(raw), encoding="utf-8")

    result = Cache()
    result.key = key
    result.destination = destination
    return result


def test_roundtrip_is_standalone_playlist_with_stable_duration_but_no_transient_live_status(cache):
    raw = playlist([video(duration=179, live_status="was_live", is_live=False),
                    video("fixture0002", duration=None, live_status="is_upcoming",
                          is_upcoming=True)])
    stored = cache.save(raw)
    assert stored == cache.load()
    assert stored["_type"] == "playlist"
    assert stored["extractor"] == "youtube:tab"
    assert stored["extractor_key"] == "YoutubeTab"
    assert stored["entries"][0]["duration"] == 179
    assert stored["entries"][1]["duration"] is None
    assert all(flag not in entry for entry in stored["entries"]
               for flag in ("live_status", "is_live", "is_upcoming"))
    assert raw["entries"][0]["channel_id"] == CHANNEL


@pytest.mark.parametrize("status", [
    {"live_status": "is_live"}, {"live_status": "is_upcoming"},
    {"live_status": "post_live"}, {"is_live": True}, {"is_upcoming": True},
])
def test_unfinished_broadcast_duration_cannot_exclude_later_completed_video(cache, status):
    saved = cache.save(playlist([video(duration=0, **status)]))
    assert saved["entries"][0]["duration"] is None
    assert cache.load() == saved


def test_flat_shorts_url_is_validated_and_normalized(cache):
    saved = cache.save(playlist([video(url="https://www.youtube.com/shorts/fixture0001")]))
    assert saved["entries"][0]["url"] == "https://www.youtube.com/watch?v=fixture0001"
    assert cache.load() == saved
    assert cache.save(playlist([video(url="https://www.youtube.com/shorts/fixture0002")])) is None


def test_normalization_discards_paths_credentials_and_root_network_fallbacks(cache):
    raw = playlist([video(http_headers={"Authorization": "secret"},
                          formats=[{"url": "https://signed.invalid/?token=secret"}],
                          filepath="private path", url="https://youtube.com/watch?v=fixture0001&secret=1")],
                   title="private channel name", cookies="secret", webpage_url=URL,
                   original_url=URL, output_dir=cache.destination)
    stored = cache.save(raw)
    encoded = cache.path.read_text(encoding="utf-8")
    assert stored is not None
    assert stored["entries"][0]["url"] == "https://www.youtube.com/watch?v=fixture0001"
    assert all(value not in encoded for value in
               ("secret", "signed.invalid", "private", "webpage_url", "original_url", "output_dir"))
    assert cache.destination not in encoded


def test_nested_channel_playlists_flatten_and_deduplicate_in_discovery_order(cache):
    raw = playlist([playlist([video(), video("fixture0002")]),
                    playlist([video(), video("fixture0003")], id="uploads-tab")])
    saved = cache.save(raw)
    assert [entry["id"] for entry in saved["entries"]] == [
        "fixture0001", "fixture0002", "fixture0003"]
    assert cache.load() == saved


@pytest.mark.parametrize("bad", [
    None, "video", 1, [], {},
    video(id="bad"), video(_type="playlist", entries=None),
    video(ie_key="Generic"), video(_type="video"), video(url="file:///private/file"),
    video(url="https://www.youtube.com/watch?v=fixture0002"),
    video(url="https://www.youtube.com/watch?v=fixture0001&v=fixture0002"),
    video(channel_id=OTHER_CHANNEL), video(channel_id="NA"),
    video(duration=-1), video(duration="181"), video(duration=True),
    video(duration=float("nan")), video(duration=float("inf")),
    video(live_status="unknown"), video(is_live="false"), video(title={}),
])
def test_any_malformed_entry_rejects_whole_listing_without_replacing_prior(cache, bad):
    previous = cache.save()
    before = cache.path.read_bytes()
    assert cache.save(playlist([video("fixture0002"), bad])) is None
    assert cache.path.read_bytes() == before
    assert cache.load() == previous


@pytest.mark.parametrize("raw", [
    playlist(channel_id=OTHER_CHANNEL), playlist(id=OTHER_CHANNEL),
    playlist(channel_id=None, id="unproven"),
    playlist([playlist(channel_id=None, id="unproven")]),
    playlist([playlist(channel_id=OTHER_CHANNEL)]),
    {"_type": "playlist", "channel_id": CHANNEL, "entries": None},
])
def test_every_playlist_must_prove_the_same_permanent_channel(cache, raw):
    assert cache.save(raw) is None
    assert not cache.path.exists()


def test_channel_id_can_be_proven_by_playlist_id(cache):
    assert cache.save(playlist(channel_id=None)) is not None


@pytest.mark.parametrize("empty", [playlist([]), playlist([playlist([])]),
                                   playlist([playlist([]), playlist([playlist([])])])])
def test_empty_or_nested_empty_listing_never_replaces_prior_manifest(cache, empty):
    original = cache.save()
    before = cache.path.read_bytes()
    assert cache.save(empty) is None
    assert cache.path.read_bytes() == before
    assert cache.load() == original


@pytest.mark.parametrize("empty_entries", [[], [playlist([])]])
def test_empty_cached_manifest_is_rejected_on_load(cache, empty_entries):
    cache.save()
    cache.corrupt(lambda raw: raw.update(entries=empty_entries))
    assert cache.load() is None


@pytest.mark.parametrize("change", [
    lambda raw: raw.update(webpage_url=URL),
    lambda raw: raw.update(original_url=URL),
    lambda raw: raw["_ytarchiver_discovery"].update(schema=2),
    lambda raw: raw["_ytarchiver_discovery"].update(schema=True),
    lambda raw: raw["_ytarchiver_discovery"].update(context_key="b" * 64),
    lambda raw: raw["entries"][0].update(ie_key="Generic"),
    lambda raw: raw["entries"][0].update(http_headers={"Authorization": "secret"}),
    lambda raw: raw["entries"].append(None),
])
def test_load_rejects_tampered_or_incompatible_payload_before_ytdlp_uses_path(cache, change):
    cache.save()
    cache.corrupt(change)
    assert cache.load() is None


@pytest.mark.parametrize("payload", [b"", b"{", b"null", b"[]", b"\xff"])
def test_missing_or_corrupt_manifest_is_cache_miss(cache, payload):
    assert cache.load() is None
    cache.path.parent.mkdir(parents=True, exist_ok=True)
    cache.path.write_bytes(payload)
    assert cache.load() is None


def test_context_reuses_canonical_equivalents_but_separates_destination_and_tab(cache):
    canonical = manifests.context_key(CHANNEL, URL, cache.destination)
    equivalent = URL.replace("https://www.", "http://m.") + "/#ignored"
    assert manifests.context_key(CHANNEL, equivalent, cache.destination + "/.") == canonical
    assert manifests.context_key(CHANNEL, URL.replace("/videos", "/streams"), cache.destination) != canonical
    assert manifests.context_key(CHANNEL, URL, cache.destination + "-other") != canonical
    assert manifests.context_key(OTHER_CHANNEL, URL.replace(CHANNEL, OTHER_CHANNEL), cache.destination) != canonical
    cache.save()
    assert manifests.load_manifest(cache.key, CHANNEL, URL, cache.destination + "-other") is None


@pytest.mark.parametrize("channel,url,destination", [
    ("", URL, "C:/example"), ("not-permanent", URL, "C:/example"),
    (CHANNEL, "https://evil.invalid/channel/" + CHANNEL, "C:/example"),
    (CHANNEL, URL.replace(CHANNEL, OTHER_CHANNEL), "C:/example"),
    (CHANNEL, "https://www.youtube.com/watch?v=fixture0001", "C:/example"),
    (CHANNEL, URL, "relative/path"),
])
def test_context_rejects_ambiguous_identity_or_destination(channel, url, destination):
    with pytest.raises(ValueError):
        manifests.context_key(channel, url, destination)


def test_large_file_or_entry_count_falls_back_without_replacing_old_manifest(cache, monkeypatch):
    cache.save()
    before = cache.path.read_bytes()
    monkeypatch.setattr(manifests, "MAX_ENTRIES", 1)
    assert cache.save(playlist([video(), video("fixture0002")])) is None
    monkeypatch.setattr(manifests, "MAX_BYTES", len(before) - 1)
    assert cache.load() is None
    assert cache.save() is None
    assert cache.path.read_bytes() == before


def test_excessive_nesting_and_unmaterialized_iterator_are_rejected(cache):
    raw = playlist()
    for _ in range(manifests.MAX_DEPTH + 1):
        raw = playlist([raw])
    assert cache.save(raw) is None
    raw = playlist()
    raw["entries"] = iter([video()])
    assert cache.save(raw) is None


def test_replace_failure_preserves_old_listing_and_removes_stage(cache, monkeypatch):
    previous = cache.save()
    before = cache.path.read_bytes()

    def fail(*_):
        raise OSError("simulated replacement failure")

    monkeypatch.setattr(sidecar_store.os, "replace", fail)
    assert cache.save(playlist([video("fixture0002")])) is None
    assert cache.path.read_bytes() == before
    assert cache.load() == previous
    assert list(cache.path.parent.iterdir()) == [cache.path]


@pytest.mark.parametrize("freeze_during_stage", [False, True])
def test_restore_write_gate_protects_prior_manifest_and_invalidation(cache, monkeypatch,
                                                                   freeze_during_stage):
    cache.save()
    before = cache.path.read_bytes()
    calls = 0

    def writable():
        nonlocal calls
        calls += 1
        return freeze_during_stage and calls == 1

    monkeypatch.setattr(manifests.ytarchiver_config, "config_is_writable", writable)
    assert cache.save(playlist([video("fixture0002")])) is None
    manifests.invalidate(cache.key)
    assert cache.path.read_bytes() == before
    assert list(cache.path.parent.iterdir()) == [cache.path]


def test_concurrent_saves_publish_complete_json_only(cache):
    raws = [playlist([video(f"fixture{i:04d}")]) for i in range(12)]
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(cache.save, raws))
    assert all(result is not None for result in results)
    assert cache.load() in results
    assert list(cache.path.parent.iterdir()) == [cache.path]


def test_invalidate_can_remove_only_its_own_validated_cache_entry(cache, tmp_path):
    cache.save()
    unrelated = tmp_path / "unrelated.json"
    unrelated.write_text("preserve", encoding="utf-8")
    for invalid in ("../unrelated", str(unrelated), "", "A" * 64):
        manifests.invalidate(invalid)
        with pytest.raises(ValueError):
            manifests.manifest_path(invalid)
    assert unrelated.read_text(encoding="utf-8") == "preserve"
    assert cache.path.exists()
    manifests.invalidate(cache.key)
    assert not cache.path.exists()


def test_saved_listing_is_detached_from_callers_mutable_input(cache):
    raw = playlist()
    original = copy.deepcopy(raw)
    saved = cache.save(raw)
    assert raw == original
    raw["entries"].clear()
    assert len(saved["entries"]) == 1
    assert cache.load() == saved
