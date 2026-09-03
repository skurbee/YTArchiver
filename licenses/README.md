# Bundled license index

`THIRD_PARTY_NOTICES.md` maps distributed components to their licenses and
upstream sources. This directory includes the common license texts needed for
the desktop runtime:

- `MIT.txt`
- `BSD-3-Clause.txt`
- `UNLICENSE.txt`
- `LGPL-3.0.txt`

Some dependencies carry additional or component-specific terms. Their own
installed `.dist-info` license files remain authoritative. In particular:

- Pillow: `pillow-12.2.0.dist-info/licenses/LICENSE`
- PyInstaller: `pyinstaller-6.19.0.dist-info/licenses/COPYING.txt`
- typing-extensions / CPython: <https://www.python.org/psf/license/>
- Transformers: <https://www.apache.org/licenses/LICENSE-2.0>
- FFmpeg: <https://ffmpeg.org/legal.html>

The LGPL text incorporates GPLv3 by reference. The complete corresponding
GPLv3 text is available at <https://www.gnu.org/licenses/gpl-3.0.txt>.
