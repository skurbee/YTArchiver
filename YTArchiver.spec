# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec for YTArchiver (pywebview build).
#
# Build with:
# py -3.13 -m PyInstaller YTArchiver.spec
#
# IMPORTANT: Build with Python 3.13, where the required pywebview runtime is
# installed. A different interpreter can produce an incomplete executable.
#
# Deps bundled next to the exe at runtime:
# - yt-dlp.exe (sync downloads)
# - ffmpeg.exe (compress + reorg)
# - icon.ico (window + tray icon)
# - web/ (HTML/CSS/JS shell)
# - backend/whisper_worker.py (runs under bundled/system Python 3.11)
#
# Whisper itself stays out-of-tree: we invoke Python 3.11's venv at runtime
# (find_python311 in backend/transcribe/) so we don't re-bundle CUDA + CTranslate2.

import os
from pathlib import Path

block_cipher = None

PROJECT_ROOT = Path(os.path.abspath(SPECPATH))

# Collect static data files shipped alongside the exe
datas = [
    (str(PROJECT_ROOT / 'web'), 'web'),
    (str(PROJECT_ROOT / 'backend' / 'whisper_worker.py'), 'backend'),
    # punct_worker.py is launched by path, so PyInstaller cannot discover
    # it through static analysis. Bundle it explicitly.
    (str(PROJECT_ROOT / 'backend' / 'punct_worker.py'), 'backend'),
]
# Optional: icon.ico (only if present)
icon_path = PROJECT_ROOT / 'icon.ico'
if icon_path.exists():
    datas.append((str(icon_path), '.'))

# If yt-dlp.exe / ffmpeg.exe / ffprobe.exe live next to main.py, bundle them too.
# ffprobe lets redownload.py detect videos already at the target resolution.
for _exe_name in ('yt-dlp.exe', 'ffmpeg.exe', 'ffprobe.exe'):
    _p = PROJECT_ROOT / _exe_name
    if _p.exists():
        datas.append((str(_p), '.'))

hiddenimports = [
    'webview',
    'webview.platforms.winforms',
    # pystray backends — pystray imports them lazily
    'pystray._win32',
    'PIL.Image',
    'PIL._tkinter_finder',
]

a = Analysis(
    ['main.py'],
    pathex=[str(PROJECT_ROOT)],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # The pywebview build does not use tkinter.
        'tkinter',
        'tkinter.ttk',
        'tkinter.messagebox',
        'tkinter.filedialog',
        # Transcription runs this stack in a separate Python 3.11 process.
        'faster_whisper',
        'ctranslate2',
        'torch',
        'transformers',
    ],
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='YTArchiver',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=str(icon_path) if icon_path.exists() else None,
)
