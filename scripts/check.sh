#!/usr/bin/env bash
# YTArchiver pre-flight gate — catches the "dumb little stuff" class that
# big manual audits miss: undefined names, broken re-exports / circular
# imports, and a red test suite.
#
# Run it yourself any time:   bash scripts/check.sh
# It is also invoked by the pre-push git hook.
#
# Exits non-zero on the first failing stage so a broken build can't ship.
set -u

cd "$(dirname "$0")/.." || exit 2

# Prefer the 3.13 build interpreter; fall back to whatever's on PATH.
PY="C:/Users/Scott/AppData/Local/Programs/Python/Python313/python.exe"
[ -x "$PY" ] || PY="$(command -v python || command -v py || echo python)"

fail() { echo ""; echo ">>> GATE FAILED: $1"; exit 1; }

echo "== [1/3] ruff — undefined names + redefinitions (real runtime bugs) =="
# Only the bug-class codes, not style: F821 undefined-name, F811 redefinition,
# F823 local-used-before-assignment, F706 return-outside-fn, F502/F522 bad %-format.
"$PY" -m ruff check . --select F821,F811,F823,F706,F502,F522 --quiet \
    || fail "ruff found a definite bug (see above)"

echo "== [2/3] import graph — every package + submodule loads cleanly =="
# Imports the real app surface the way main.py does, so a dropped re-export
# or a hard circular import fails HERE instead of in production.
"$PY" - <<'PYEOF' || fail "a module failed to import (re-export drop / circular import?)"
import importlib, sys
# Load the package roots first (their __init__ pulls submodules in the
# correct order), then sweep every remaining submodule individually.
import backend, backend.index, backend.sync, backend.metadata, backend.transcribe
import pkgutil
SKIP = {"backend.whisper_worker", "backend.punct_worker"}  # subprocess entrypoints: run on import
bad = []
for m in pkgutil.walk_packages(backend.__path__, "backend."):
    if m.name in SKIP:
        continue
    try:
        importlib.import_module(m.name)
    except Exception as e:
        bad.append(f"{m.name}: {e!r}")
if bad:
    print("IMPORT FAILURES:")
    for b in bad:
        print("  " + b)
    sys.exit(1)
print("  ok - all backend modules import")
PYEOF

echo "== [3/3] test suite =="
"$PY" -m pytest tests/ -q || fail "tests are red"

echo ""
echo ">>> GATE PASSED"
