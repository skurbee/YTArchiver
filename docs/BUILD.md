# Building YTArchiver

## Requirements

- **Python 3.13** specifically. The PyInstaller version on PATH is often
  bundled with 3.11 and produces a broken exe (~28 MB instead of the
  correct ~32 MB; silently misses `pywebview` modules at runtime).
  Always invoke PyInstaller via the Python 3.13 interpreter directly.

## Clean rebuild (always required)

Stale `.pyc` files inside `__pycache__` will cause PyInstaller to bundle
out-of-date bytecode. Clear them first:

```bash
rm -rf __pycache__ backend/__pycache__ backend/*/__pycache__ build dist
```

This catches every sub-package — `backend/sync/`, `backend/transcribe/`,
`backend/metadata/`, `backend/api_mixins/` — without having to enumerate them.

Then build:

```bash
py -3.13 -m PyInstaller YTArchiver.spec
```

Or with an explicit Python 3.13 path on Windows:

```bash
"%LOCALAPPDATA%/Programs/Python/Python313/python.exe" -m PyInstaller YTArchiver.spec
```

Output: `dist/YTArchiver.exe`. Expected size: **~32 MB**. If you see
**~28 MB**, you accidentally used Python 3.11 — clean and rebuild.

## Spec file

`YTArchiver.spec` handles everything — icon, hidden imports, data files.
Don't build with `pyinstaller --onefile main.py`; use the spec.

The icon is bundled both ways:
- In `datas=[('icon.ico', '.')]` so runtime code can `iconbitmap()` /
  pystray `Icon(...)`.
- In the `EXE(...)` `icon=` field for the Windows file icon.

## Distribution

Copy `dist/YTArchiver.exe` wherever. Portable single-file exe; no
installer needed. Same exe runs on any Windows 10/11 machine with the
external tools (yt-dlp, ffmpeg) on PATH.

## Testing the build

Always smoke-test the freshly-built exe before considering a release:
1. Double-click `dist/YTArchiver.exe`.
2. Confirm window opens, version header shows the expected `vXX.Y`.
3. Click through each tab — Subs, Browse, Settings, Recent — confirm
   no white screens or unhandled errors.
4. If pywebview's console (F12) shows errors, investigate before
   shipping.

For development, you don't need to build the exe — `python main.py`
is faster and identical behavior. The exe is only for distribution.

## CI

No CI is set up. Builds are manual. The version bump rule
(`APP_VERSION` +0.1 per push) is enforced by a pre-push git hook
locally — see `.git/hooks/pre-push`.
