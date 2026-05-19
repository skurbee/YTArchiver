"""
metadata.normalize — title normalization shims used by the metadata path.

Patch 19 phase M1 (v68.10): thin re-export shim. The canonical
`normalize_title` lives in `backend.text_utils` (Patch 11). This module
namespaces the metadata-specific helpers + variants so callers reading
the metadata package make sense of them.

Public surface (re-exported into legacy.py for back-compat):
    normalize_title          canonical (text_utils)
    normalize_title_loose    canonical (text_utils)
    _normalize_title_for_match    legacy.py-local wrapper, kept for
                                  back-compat — used by the playlist
                                  ↔ filename matcher
    _norm_title_for_match         legacy.py-local wrapper, kept for
                                  back-compat — used by backfill matcher
"""
from __future__ import annotations

from ..text_utils import normalize_title, normalize_title_loose


def _normalize_title_for_match(title: str) -> str:
    """Patch 11: thin wrapper around text_utils.normalize_title.

    Mode: lowercase + NFKC + unify-fullwidth-substitutions +
    strip-windows-illegal + collapse-whitespace. NO trailing-punct
    strip (the matcher distinguishes "title?" from "title").
    """
    return normalize_title(
        title,
        strip_trailing_punct=False,
        strip_windows_illegal=True,
    )


def _norm_title_for_match(s: str) -> str:
    """Patch 11: thin wrapper around text_utils.normalize_title.

    Mode: lowercase + NFKC + strip-id-bracket + alnum-only-collapse.
    Used by backfill_video_ids matching strategies so candidates
    compare apples-to-apples.
    """
    return normalize_title(
        s,
        strip_trailing_punct=False,
        strip_id_bracket=True,
        alnum_only=True,
    )


__all__ = [
    "normalize_title",
    "normalize_title_loose",
    "_normalize_title_for_match",
    "_norm_title_for_match",
]
