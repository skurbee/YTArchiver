"""Exercise build checks against small repositories without importing the app."""
import subprocess
from pathlib import Path

import pytest

from scripts.check_bridge_contract import (
    backend_methods,
    missing_bridge_methods,
    unsafe_bridge_parameters,
)
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


def test_bridge_rejects_window_parameter_even_without_literal_frontend_call(tmp_path):
    write(tmp_path, "main.py",
          "class Api:\n def expirations(self, window='daily'): pass\n"
          " def _internal(self, window): pass\n")
    assert unsafe_bridge_parameters(tmp_path) == ["expirations: window"]


def test_bridge_parameter_check_respects_inherited_overrides_and_hidden_methods(tmp_path):
    write(tmp_path, "backend/base.py",
          "class Base:\n def corrected(self, window): pass\n"
          " def inherited(self, window, /): pass\n def hidden(self, window): pass\n")
    write(tmp_path, "main.py",
          "from backend.base import Base\nclass Api(Base):\n"
          " hidden = None\n def corrected(self, window_name): pass\n")
    assert unsafe_bridge_parameters(tmp_path) == ["inherited: window"]


def test_bridge_parameter_check_detects_unsafe_subclass_override(tmp_path):
    write(tmp_path, "backend/base.py", "class Base:\n def expirations(self, window_name): pass\n")
    write(tmp_path, "main.py",
          "from backend.base import Base\nclass Api(Base):\n"
          " def expirations(self, window='daily'): pass\n")
    assert unsafe_bridge_parameters(tmp_path) == ["expirations: window"]


def test_bridge_accepts_safe_window_name_parameter(tmp_path):
    write(tmp_path, "main.py", "class Api:\n def expirations(self, window_name='daily'): pass\n")
    assert unsafe_bridge_parameters(tmp_path) == []


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
