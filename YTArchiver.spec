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
import re
import runpy
from pathlib import Path

block_cipher = None

PROJECT_ROOT = Path(os.path.abspath(SPECPATH))

# backend/version.py is authoritative for both runtime/UI and Windows file
# metadata. Generate the PyInstaller resource in ignored build output so there
# is no second version string to drift.
_version_ns = runpy.run_path(str(PROJECT_ROOT / 'backend' / 'version.py'))
APP_VERSION = str(_version_ns['APP_VERSION']).lstrip('v')
APP_VERSION_DATE = str(_version_ns['APP_VERSION_DATE'])
_version_numbers = [int(piece) for piece in re.findall(r'\d+', APP_VERSION)[:4]]
_version_tuple = tuple((_version_numbers + [0, 0, 0, 0])[:4])
_resource_dir = PROJECT_ROOT / 'build' / 'build-metadata'
_resource_dir.mkdir(parents=True, exist_ok=True)
_version_resource = _resource_dir / 'version_info.txt'
_version_resource.write_text(
    "VSVersionInfo(\n"
    "  ffi=FixedFileInfo(\n"
    f"    filevers={_version_tuple!r}, prodvers={_version_tuple!r},\n"
    "    mask=0x3f, flags=0x0, OS=0x40004, fileType=0x1, subtype=0x0,\n"
    "    date=(0, 0)),\n"
    "  kids=[\n"
    "    StringFileInfo([StringTable('040904B0', [\n"
    "      StringStruct('CompanyName', 'YTArchiver contributors'),\n"
    "      StringStruct('FileDescription', 'YTArchiver desktop application'),\n"
    f"      StringStruct('FileVersion', {APP_VERSION!r}),\n"
    "      StringStruct('InternalName', 'YTArchiver'),\n"
    "      StringStruct('LegalCopyright', 'Licensed under the MIT License'),\n"
    "      StringStruct('OriginalFilename', 'YTArchiver.exe'),\n"
    "      StringStruct('ProductName', 'YTArchiver'),\n"
    f"      StringStruct('ProductVersion', {APP_VERSION!r}),\n"
    f"      StringStruct('Comments', {'Built from backend/version.py; ' + APP_VERSION_DATE!r}),\n"
    "    ])]),\n"
    "    VarFileInfo([VarStruct('Translation', [1033, 1200])])\n"
    "  ]\n"
    ")\n",
    encoding='utf-8',
)

# Collect static data files shipped alongside the exe
datas = [
    (str(PROJECT_ROOT / 'web'), 'web'),
    (str(PROJECT_ROOT / 'backend' / 'whisper_worker.py'), 'backend'),
    # punct_worker.py is launched by path, so PyInstaller cannot discover
    # it through static analysis. Bundle it explicitly.
    (str(PROJECT_ROOT / 'backend' / 'punct_worker.py'), 'backend'),
    (str(PROJECT_ROOT / 'backend' / 'punct_alignment.py'), 'backend'),
    # Reproducible optional-worker installation and distributed notices.
    (str(PROJECT_ROOT / 'requirements'), 'requirements'),
    (str(PROJECT_ROOT / 'licenses'), 'licenses'),
    (str(PROJECT_ROOT / 'THIRD_PARTY_NOTICES.md'), '.'),
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
    version=str(_version_resource),
)
