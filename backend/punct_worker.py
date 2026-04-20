"""
Persistent punctuation-restoration worker process.

Spawned by transcribe.py (after whisper produces raw text) to add
commas / periods / capitalization using the
`oliverguhr/fullstop-punctuation-multilang-large` HuggingFace model.

Runs under Python 3.11 with CUDA PyTorch + transformers (same venv
whisper uses). Verbatim port of YTArchiver.py:8835 _PUNCT_SCRIPT.

Protocol:
    → stdin:  { "text": "raw whisper text no punctuation here" }
    ← stdout: { "status": "ready", "device": "cuda" }  (once at startup)
              { "status": "ok", "text": "Punctuated. Sentences here." }
              or  { "status": "error", "text": "reason" }
"""

import sys, json, io, re, os, logging

_out = sys.stdout
sys.stdout = io.StringIO()
sys.stderr = io.StringIO()

os.environ["HF_HUB_DISABLE_SYMLINKS_WARNING"] = "1"
os.environ["TRANSFORMERS_NO_ADVISORY_WARNINGS"] = "1"
os.environ["HF_HUB_DISABLE_TELEMETRY"] = "1"
os.environ["TOKENIZERS_PARALLELISM"] = "false"

for _name in ("transformers", "huggingface_hub", "safetensors",
              "transformers.modeling_utils"):
    logging.getLogger(_name).setLevel(logging.ERROR)

try:
    from transformers import pipeline as tf_pipeline
    import torch

    device_str = "cuda" if torch.cuda.is_available() else "cpu"
    pipe = tf_pipeline("ner", "oliverguhr/fullstop-punctuation-multilang-large",
                       aggregation_strategy="none",
                       device=0 if device_str == "cuda" else -1)

    _out.write(json.dumps({"status": "ready", "device": device_str}) + "\n")
    _out.flush()
except Exception as e:
    _out.write(json.dumps({"status": "error", "text": str(e)}) + "\n")
    _out.flush()
    sys.exit(1)

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        req = json.loads(line)
        text = req.get("text", "")
    except json.JSONDecodeError:
        continue
    if not text:
        _out.write(json.dumps({"status": "ok", "text": ""}) + "\n")
        _out.flush()
        continue

    try:
        cleaned = re.sub(r"(?<!\d)[.,;:!?](?!\d)", "", text)
        words = cleaned.split()
        if not words:
            _out.write(json.dumps({"status": "ok", "text": text}) + "\n")
            _out.flush()
            continue

        chunk_size = 230
        overlap = 5 if len(words) > chunk_size else 0

        def _chunk(lst, n, stride):
            for i in range(0, len(lst), n - stride):
                yield lst[i:i + n]

        batches = list(_chunk(words, chunk_size, overlap))
        if len(batches) > 1 and len(batches[-1]) <= overlap:
            batches.pop()

        tagged = []
        for batch in batches:
            ov = 0 if batch is batches[-1] else overlap
            text_chunk = " ".join(batch)
            result = pipe(text_chunk)
            char_index = 0
            result_index = 0
            for word in batch[:len(batch) - ov]:
                char_index += len(word) + 1
                label = "0"
                while result_index < len(result) and char_index > result[result_index]["end"]:
                    label = result[result_index]["entity"]
                    result_index += 1
                tagged.append((word, label))

        out = ""
        for word, label in tagged:
            out += word
            if label == "0":
                out += " "
            elif label in ".,?-:":
                out += label + " "
        out = out.strip()

        out = re.sub(r"([.!?]\s+)(\w)", lambda m: m.group(1) + m.group(2).upper(), out)
        if out:
            out = out[0].upper() + out[1:]

        _out.write(json.dumps({"status": "ok", "text": out}) + "\n")
        _out.flush()
    except Exception as e:
        _out.write(json.dumps({"status": "error", "text": str(e)}) + "\n")
        _out.flush()
