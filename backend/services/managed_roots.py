"""One configuration snapshot of archive root membership and ownership.

No configuration imports or archive enumeration occur here. Containment
resolves paths at the time of the check so a changed link cannot reuse stale
authorization. Transaction-specific junction checks remain at their boundary.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ManagedRoots:
    paths: tuple[str, ...]

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> ManagedRoots:
        additional = config.get("tp_archive_roots") or []
        if not isinstance(additional, (list, tuple)):
            additional = []
        candidates = (config.get("output_dir"), config.get("video_out_dir"), *additional)
        paths: list[str] = []
        seen: set[str] = set()
        for value in candidates:
            path = str(value or "").strip()
            if not path:
                continue
            try:
                absolute = os.path.abspath(path)
                key = os.path.normcase(os.path.realpath(absolute))
            except (TypeError, ValueError, OSError):
                continue
            if key and key not in seen:
                seen.add(key)
                paths.append(absolute)
        return cls(tuple(paths))

    def owner_for(self, path: str) -> str:
        """Return the innermost resolved root, or empty when not contained."""
        if not path:
            return ""
        try:
            target = os.path.normcase(os.path.realpath(path))
        except (TypeError, ValueError, OSError):
            return ""
        matches: list[tuple[int, str]] = []
        for root in self.paths:
            try:
                resolved = os.path.realpath(root)
                key = os.path.normcase(resolved)
                if os.path.commonpath([target, key]) == key:
                    matches.append((len(key), resolved))
            except (TypeError, ValueError, OSError):
                continue
        return max(matches)[1] if matches else ""

    def contains(self, path: str) -> bool:
        return bool(self.owner_for(path))
