"""
Canonical text helpers — single source of truth for title normalization
and related string ops used across the backend.

consolidates four divergent `_norm_title` implementations
that existed in transcribe.py, repair_captions.py, metadata.py, and
index.py. Each had subtly different rules; cross-module lookups silently
mismatched on titles with punctuation or unicode. This module is THE
canonical normalizer — all other modules should import from here.

extended `normalize_title` with mode kwargs so the
three remaining metadata.py / index.py copies can route through here
without losing their (different) matching semantics. The flags compose:
  - strip_trailing_punct  -> drop ".?!" stacked at the end
  - strip_id_bracket      -> drop a trailing ` [video_id]` 11-char tag
  - alnum_only            -> collapse non-alphanumeric runs to one space
  - strip_windows_illegal -> remove the chars `<>:"/\\|?*` and the
                             common fullwidth unicode substitutions
                             yt-dlp writes when it sanitizes filenames
"""

from __future__ import annotations

import re
import unicodedata

_WS_RE = re.compile(r"\s+")
_TRAILING_PUNCT_RE = re.compile(r"[.?!]+$")
_ID_BRACKET_RE = re.compile(r"\s*\[[A-Za-z0-9_-]{11}\]\s*$")
_ALNUM_COLLAPSE_RE = re.compile(r"[^a-z0-9]+")
_WIN_ILLEGAL_RE = re.compile(r'[<>:"/\\|?*]')


def normalize_title(
    s: str,
    *,
    strip_trailing_punct: bool = True,
    strip_id_bracket: bool = False,
    alnum_only: bool = False,
    strip_windows_illegal: bool = False,
) -> str:
    """Normalize a video title for comparison/matching.

    Default rules (frozen — `_norm_title` callers in transcribe.py,
    drift_scan.py, repair_captions.py, channel_mixin.py, punct_restore.py
    all rely on these exact defaults):
      1. NFKC unicode normalization (collapses width variants, ligatures,
         half/full-width digits to canonical form).
      2. Strip leading/trailing whitespace.
      3. Lowercase.
      4. Collapse internal whitespace to single spaces.
      5. Strip trailing `.`, `?`, `!` (possibly stacked) so "Title." and
         "Title" key the same. This is critical for the retranscribe path
         where Whisper produces "title." and YouTube captions produce
         "title" — without this they don't match and duplicates appear.

    Optional modes (set kwargs to opt in):

      strip_id_bracket=True
        Drop a trailing ` [Abc12345_-x]` 11-char video-id tag if present.
        Used by the metadata.py title-fallback resolver where filenames
        may carry the id remnant but the YT-side title never does.

      alnum_only=True
        Collapse every non-alphanumeric run to a single space. Used by
        the metadata.py backfill matcher where punctuation, smart quotes,
        en-dashes, and filesystem-sanitized colons would otherwise
        prevent obvious matches.

      strip_windows_illegal=True
        Remove `<>:"/\\|?*` and unify the common fullwidth-substitution
        unicode chars yt-dlp writes when sanitizing filenames. Used by
        the metadata.py playlist↔filename matcher.

      strip_trailing_punct=False
        Opt out of the trailing-punct strip. The metadata.py matchers
        use this so "title?" and "title" can be distinguished.

    Empty string in → empty string out. Returns lowercase ASCII-or-unicode
    suitable for dict keys, set membership, and == comparison.
    """
    if not s:
        return ""
    t = unicodedata.normalize("NFKC", s).strip().lower()

    if strip_windows_illegal:
        # yt-dlp replaces path-reserved chars with unicode lookalikes when
        # it sanitizes filenames. The NFKC pass above already folds the
        # FULLWIDTH forms (＂＊：＜＞？｜／) back to ASCII, but the two glyphs
        # yt-dlp actually uses for `/` and `\` — BIG SOLIDUS (U+29F8 ⧸) and
        # REVERSE BIG SOLIDUS (U+29F9 ⧹) — have NO compatibility
        # decomposition and survive NFKC untouched. Without mapping them a
        # title's "11/23/1996" never matches the on-disk "11⧸23⧸1996", so
        # single-video binds silently failed on any title with a slash,
        # colon, etc. Map them explicitly before stripping.
        t = t.replace("⧸", "/").replace("⧹", "\\")
        t = t.replace("⁄", "/").replace("／", "/")
        t = t.replace("：", ":").replace("？", "?").replace("｜", "|")
        t = _WIN_ILLEGAL_RE.sub("", t)

    if strip_id_bracket:
        t = _ID_BRACKET_RE.sub("", t)

    if alnum_only:
        t = _ALNUM_COLLAPSE_RE.sub(" ", t)

    t = _WS_RE.sub(" ", t).strip()

    if strip_trailing_punct:
        t = _TRAILING_PUNCT_RE.sub("", t).rstrip()

    return t


def normalize_title_loose(s: str) -> str:
    """Stricter normalize — strips ALL punctuation, not just trailing,
    AND ASCII-folds accented letters so "café" matches "cafe".

    Useful for fuzzy matching where "title — pt 1" and "title pt 1" should
    key together. The trailing-punct-only `normalize_title` is the default;
    use this variant when you specifically need punctuation-insensitive
    matching across the whole title.

    audit: text_utils.py:108-122 — old code used NFKC which preserved
    non-ASCII letters, so this "stricter" form was actually LESS strict
    than the alnum_only normalizer. NFKD + ASCII-encode-with-ignore
    strips the combining marks, dropping "café" → "cafe".
    """
    if not s:
        return ""
    t = unicodedata.normalize("NFKD", s)
    # Drop combining marks (the NFKD-separated accents) but keep base
    # letters. Anything that can't round-trip to ASCII (CJK chars) is
    # preserved verbatim — folding to "?" or dropping would hurt
    # matching for transliterated channel names.
    _ascii = t.encode("ascii", "ignore").decode("ascii")
    # If ASCII-fold ate everything (pure non-ASCII title), fall back to
    # the original NFKC form so we still have something to match on.
    if not _ascii.strip():
        _ascii = unicodedata.normalize("NFKC", s)
    t = _ascii.strip().lower()
    # Drop everything that isn't a word char or whitespace, then collapse.
    t = re.sub(r"[^\w\s]+", " ", t)
    t = _WS_RE.sub(" ", t).strip()
    return t


_ID_RE_11 = re.compile(r"^[A-Za-z0-9_-]{11}$")
_ID_IN_FILENAME = re.compile(r"\[([A-Za-z0-9_-]{11})\]\s*$")


def extract_video_id(
    path: str,
    *,
    hint: str | None = None,
    conn=None,
    reject_alpha_only: bool = False,
    info_json_fallback: bool = False,
) -> str:
    """Extract an 11-char YouTube video id from an archive file path.

    Patch 11 consolidation of four divergent inline implementations that
    existed in transcribe.py (3 sites) and index.py:register_video.

    Strategy (each step skipped if prior step succeeded):
      1. `hint` if it matches the 11-char id shape.
      2. Trailing `[Abc12345_-x]` 11-char bracket on the filename stem
         (this is yt-dlp's `%(id)s` output format).
      3. If `conn` is provided: SELECT video_id FROM videos
         WHERE filepath=? COLLATE NOCASE LIMIT 1. Covers archives
         written by the classic tkinter app, which omitted the bracket.
         (This is also the path the sync-side captions probe takes for
         drop-in mode where the filename has been sanitized.)
      4. If `info_json_fallback=True`: read the .info.json sidecar that
         yt-dlp writes alongside the video and pull `data["id"]`.
         (Used by index.register_video; the captions probe never has
         time to wait on json IO, hence the conn path for that caller.)

    `reject_alpha_only` rejects matches that are entirely alphabetic
    (no digit / `_` / `-`). Some archives have filename suffixes like
    `[a-user-channel]` — 11 letters, valid pattern, but not a real
    YouTube id. Real ids are random picks so they essentially always
    include a digit or special char.

    Returns "" if no id can be resolved. Never raises.
    """
    import os

    def _ok(cand: str) -> bool:
        if not cand or not _ID_RE_11.fullmatch(cand):
            return False
        if reject_alpha_only and cand.isalpha():
            return False
        return True

    def _shape_ok(cand: str) -> bool:
        return bool(cand and _ID_RE_11.fullmatch(cand))

    if hint:
        h = hint.strip()
        # An explicit hint is AUTHORITATIVE (yt-dlp's DLTRACK %(id)s, or a
        # caller's known id) — validate by SHAPE only, never by the
        # `reject_alpha_only` heuristic. That heuristic exists to stop an
        # 11-letter *filename* bracket (a channel handle like `[SomeName]`)
        # from being mistaken for an id; it must NEVER reject a real
        # YouTube id handed to us directly, and real ids CAN be all
        # letters. Applying it to the hint silently dropped authoritative
        # ids → NULL video_id (a real contributor to the missing-id bug).
        if h and _ID_RE_11.fullmatch(h):
            return h

    try:
        name = os.path.basename(path or "")
    except Exception:
        name = path or ""

    if name:
        stem = os.path.splitext(name)[0]
        m = _ID_IN_FILENAME.search(stem)
        if m and _ok(m.group(1)):
            return m.group(1)

    if conn is not None and path:
        try:
            # Try BOTH normpath AND raw path. Rows in DB may have been
            # inserted with different slash direction (Z:\Foo/Bar vs
            # Z:\Foo\Bar). COLLATE NOCASE handles case but not slash
            # mixing (audit: text_utils.py:191-198). Two cheap UNIQUE
            # lookups is faster than scanning + normalizing every row.
            _norm = os.path.normpath(path)
            row = conn.execute(
                "SELECT video_id FROM videos WHERE filepath = ? "
                "COLLATE NOCASE LIMIT 1",
                (_norm,),
            ).fetchone()
            if row and row[0] and _shape_ok(str(row[0]).strip()):
                return str(row[0]).strip()
            if _norm != path:
                row = conn.execute(
                    "SELECT video_id FROM videos WHERE filepath = ? "
                    "COLLATE NOCASE LIMIT 1",
                    (path,),
                ).fetchone()
                if row and row[0] and _shape_ok(str(row[0]).strip()):
                    return str(row[0]).strip()
        except Exception:
            pass

    if info_json_fallback and path:
        try:
            import json as _json
            from pathlib import Path as _Path
            fp = _Path(path)
            # Sidecar naming preserves the FULL stem (incl. yt-dlp
            # format tags like ".f137"). The previous double
            # .with_suffix("").with_suffix(".info.json") collapsed
            # multi-dot stems and looked up the wrong filename first
            # (audit: text_utils.py:206-216). Use stem+suffix directly.
            info_json = fp.parent / (fp.stem + ".info.json")
            if info_json.is_file():
                with info_json.open("r", encoding="utf-8") as f:
                    data = _json.load(f)
                raw = (data.get("id") or "").strip()
                if _ok(raw):
                    return raw
        except Exception:
            pass

    return ""
