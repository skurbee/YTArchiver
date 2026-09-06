#!/usr/bin/env bash
# Compatibility entry point for Git Bash and the pre-push hook.
# Keep backend imports and tests inside the isolated Windows quality gate.
set -eu
cd "$(dirname "$0")/.."
if command -v pwsh.exe >/dev/null 2>&1; then
    exec pwsh.exe -NoProfile -File scripts/check.ps1 "$@"
elif command -v powershell.exe >/dev/null 2>&1; then
    exec powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/check.ps1 "$@"
fi
echo "The YTArchiver gate requires Windows PowerShell. Run scripts/check.ps1 on Windows." >&2
exit 2
