"""Content fingerprint for a Git working tree, including new source files."""
from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path


def source_fingerprint(root: Path) -> str:
    """Hash paths and current bytes; Git determines tracked/non-ignored scope."""
    output = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=root)
    digest = hashlib.sha256()
    for name in sorted(set(filter(None, output.split(b"\0")))):
        path = root / name.decode("utf-8", errors="surrogateescape")
        digest.update(name + b"\0")
        if path.is_file():
            digest.update(b"file\0")
            with path.open("rb") as stream:
                digest.update(hashlib.file_digest(stream, "sha256").digest())
        elif path.is_symlink():
            digest.update(b"link\0" + str(path.readlink()).encode("utf-8"))
        else:
            digest.update(b"absent\0")
        digest.update(b"\0")
    return digest.hexdigest()


if __name__ == "__main__":
    print(source_fingerprint(Path(__file__).resolve().parents[1]))
