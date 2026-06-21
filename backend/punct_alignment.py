"""Helpers for mapping punctuation model character offsets back to words."""

from __future__ import annotations


def joined_text_and_word_ends(words: list[str]) -> tuple[str, list[int]]:
    """Return the exact joined text plus exclusive end offsets per word.

    The punctuation worker sends ``" ".join(words)`` to the model, so offsets
    should be derived from that exact string instead of reconstructed with
    ``len(word) + 1`` in a separate loop.
    """
    text = " ".join(words)
    offsets: list[int] = []
    pos = 0
    for word in words:
        start = text.find(word, pos)
        if start < 0:
            start = pos
        end = start + len(word)
        offsets.append(end)
        pos = end + 1
    return text, offsets
