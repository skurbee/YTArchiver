"""Source-level regression checks for Patch 1 frontend async ownership.

The frontend is framework-free browser JavaScript and has no DOM unit-test
runner.  These checks protect the small ownership invariants that prevent a
late A response from repainting, closing, or opening over a newer B action;
``node --check`` remains the executable syntax check.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _source(name: str) -> str:
    return (ROOT / "web" / name).read_text(encoding="utf-8")


def test_channel_save_uses_captured_identity_and_editor_generation() -> None:
    source = _source("editChannel.js")

    assert "const _savedIdentity = _editingIdentity;" in source
    assert "const _saveGeneration = _editorGeneration;" in source
    assert "api.subs_update_channel(_savedIdentity, payload)" in source
    assert "payload.url, payload.folder, _savedIdentity || null" in source
    assert "_saveGeneration !== _editorGeneration" in source
    assert "const saveStillOwnsEditor =" in source


def test_search_opening_uses_one_canonical_watch_owner() -> None:
    content = _source("browseContent.js")
    search = _source("browseSearch.js")

    assert "window._reserveWatchOpenIntent" in content
    assert "window._isWatchOpenIntentCurrent" in content
    assert 'await window._openVideoInWatch({' in search
    search_open = search[
        search.index("async function _openResolvedSearchHit"):
        search.index("function _openSearchResultInWatch")
    ]
    assert 'bridgeCall("browse_get_transcript"' not in search_open
    bookmark_open = content[
        content.index("async function _openSearchHitInWatch"):
        content.index("function escapeForRegex")
    ]
    assert 'await window._openVideoInWatch({' in bookmark_open
    assert 'bridgeCall("browse_get_transcript"' not in bookmark_open


def test_search_distinguishes_failed_legs_and_keeps_title_text_raw() -> None:
    source = _source("browseSearch.js")

    assert "const legFailures = [txResult, tiResult]" in source
    assert 'counter.textContent = legFailures.length ? "Search incomplete"' in source
    assert '" · partial"' in source
    assert 'snippet: r.title || ""' in source
    assert 'snippet: escapeHtml(r.title || "")' not in source


def test_watch_and_graph_drop_stale_async_responses() -> None:
    watch = _source("watchView.js")
    graph = _source("graphTab.js")

    assert "if (!stillShowingRefreshTarget()) return;" in watch
    assert "requestSeq === _watchMetadataSeq" in watch
    assert "_watchVideoIdentity(window._watchCurrentVideo) === requestKey" in watch
    assert "let _graphRequestSeq = 0;" in graph
    assert "const requestType = _graphType;" in graph
    assert "if (!requestIsCurrent()) return;" in graph
    assert "drawGraphFromData(_graphLastData, requestType);" in graph
