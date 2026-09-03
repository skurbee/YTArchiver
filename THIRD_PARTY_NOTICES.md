# YTArchiver third-party notices

YTArchiver is distributed under the MIT License. It also uses or distributes
the third-party software below. Copyright remains with each upstream project.
This notice is informational; the upstream license terms control.

## Desktop executable

| Component | Locked version | License |
|---|---:|---|
| pywebview | 6.2.1 | BSD-3-Clause |
| pystray | 0.19.5 | LGPL-3.0-only |
| Pillow | 12.2.0 | MIT-CMU and bundled image-codec notices |
| psutil | 7.0.0 | BSD-3-Clause |
| pythonnet | 3.0.5 | MIT |
| clr-loader | 0.2.10 | MIT |
| cffi | 2.0.0 | MIT |
| pycparser | 3.0 | BSD-3-Clause |
| Bottle | 0.13.4 | MIT |
| proxy-tools | 0.1.0 | MIT |
| six | 1.17.0 | MIT |
| typing-extensions | 4.13.0 | PSF-2.0 |
| Chart.js | 4.4.3 | MIT |
| PyInstaller bootloader | 6.19.0 | GPL-2.0-or-later with the PyInstaller bootloader exception |

The exact Windows artifacts and their SHA-256 values are in
`requirements/runtime.lock` and `requirements/build.lock`.

## Downloaded tools

The onboarding flow downloads these tools from their official distribution
locations and verifies the published SHA-256 before accepting them. They are
not maintained by the YTArchiver project.

| Component | Source | License |
|---|---|---|
| yt-dlp | yt-dlp GitHub releases | Unlicense |
| FFmpeg / ffprobe | Gyan Windows builds of FFmpeg | GPL/LGPL according to the selected FFmpeg build configuration |
| CPython 3.11 | python.org | PSF-2.0 |

## Optional transcription worker

The optional worker is installed separately under Python 3.11. Its CPU and
CUDA environments are separately locked in `requirements/worker-cpu.lock` and
`requirements/worker-cuda.lock`.

The principal projects are PyTorch (BSD-3-Clause), faster-whisper (MIT),
CTranslate2 (MIT), Hugging Face Transformers and Tokenizers (Apache-2.0), ONNX
Runtime (MIT), NumPy (BSD-3-Clause), PyAV (BSD-3-Clause), Jinja2/MarkupSafe
(BSD-3-Clause), Requests and its dependencies (Apache/BSD/MIT), and the
other exact transitive packages named in the worker locks. Pip preserves each
installed distribution's own license and metadata files next to that package.

## License texts

Common license texts and a license-location index are bundled under
`licenses/`. Dependency-specific notices shipped inside Python distributions
remain authoritative, especially Pillow's codec notices and PyInstaller's
bootloader exception.

Upstream project pages:

- <https://pywebview.flowrl.com/>
- <https://github.com/moses-palmer/pystray>
- <https://python-pillow.github.io/>
- <https://github.com/giampaolo/psutil>
- <https://www.chartjs.org/>
- <https://pyinstaller.org/>
- <https://github.com/yt-dlp/yt-dlp>
- <https://ffmpeg.org/legal.html>
- <https://www.python.org/psf/license/>
- <https://pytorch.org/>
- <https://github.com/SYSTRAN/faster-whisper>
- <https://github.com/OpenNMT/CTranslate2>
- <https://github.com/huggingface/transformers>
