"""Exercise build checks against small repositories without importing the app."""
import subprocess
from pathlib import Path

import pytest

from scripts.check_bridge_contract import backend_methods, missing_bridge_methods
from scripts.source_fingerprint import source_fingerprint


def write(root: Path, name: str, source: str) -> None:
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")


def test_detached_mixin_cannot_supply_bridge_endpoint(tmp_path):
    write(tmp_path, "main.py", "class Api: pass\n")
    write(tmp_path, "backend/api_mixins/detached.py", "class DetachedMixin:\n def orphan(self): pass\n")
    write(tmp_path, "web/calls.js", "YT.api.orphan();")
    assert missing_bridge_methods(tmp_path) == ["orphan"]


def test_inherited_aliases_and_shadowing_follow_actual_mro(tmp_path):
    write(tmp_path, "backend/api_mixins/base.py",
          "class Base:\n def kept(self): pass\n def hidden(self): pass\n")
    write(tmp_path, "backend/api_mixins/__init__.py",
          "from .base import Base as Exported\n")
    write(tmp_path, "main.py",
          "import backend.api_mixins as mixins\n"
          "class Api(mixins.Exported):\n hidden = None\n def own(self): pass\n")
    assert backend_methods(tmp_path) == {"kept", "own"}


def test_unknown_inherited_class_fails_check_instead_of_unioning_names(tmp_path):
    write(tmp_path, "main.py", "class Api(MissingMixin): pass\n")
    with pytest.raises(ValueError, match="Cannot resolve"):
        backend_methods(tmp_path)


def test_fingerprint_detects_new_file_edits_and_ignores_build_outputs(tmp_path):
    subprocess.run(["git", "init", "--quiet", str(tmp_path)], check=True)
    write(tmp_path, ".gitignore", "dist/\n")
    write(tmp_path, "new.py", "value = 1\n")
    before = source_fingerprint(tmp_path)
    write(tmp_path, "new.py", "value = 2\n")
    changed = source_fingerprint(tmp_path)
    assert changed != before
    write(tmp_path, "dist/build.bin", "generated")
    assert source_fingerprint(tmp_path) == changed
    subprocess.run(["git", "add", ".gitignore", "new.py"], cwd=tmp_path, check=True)
    assert source_fingerprint(tmp_path) == changed
    (tmp_path / "new.py").unlink()
    assert source_fingerprint(tmp_path) != changed
