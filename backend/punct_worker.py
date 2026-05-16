"""
Persistent punctuation-restoration worker process.

Spawned by transcribe.py (after whisper produces raw text) to add
commas / periods / capitalization using the
`oliverguhr/fullstop-punctuation-multilang-large` HuggingFace model.

Runs under Python 3.11 with CUDA PyTorch + transformers (same venv
whisper uses). Verbatim port of YTArchiver.py:8835 _PUNCT_SCRIPT.

Protocol:
    → stdin: { "text": "raw whisper text no punctuation here" }
    ← stdout: { "status": "ready", "device": "cuda" } (once at startup)
              { "status": "ok", "text": "Punctuated. Sentences here." }
              or { "status": "error", "text": "reason" }
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
        # ── Step 1: strip pre-existing punctuation ─────────────────────
        # The model is trained to PREDICT punctuation between bare words.
        # If the input already has commas / periods (e.g. from yt-dlp's
        # auto-captions, which sometimes punctuate, sometimes don't),
        # those tokens confuse the tagger. So we strip every `. , ; : ! ?`
        # that isn't sandwiched between digits (so "3.14" and "1,000"
        # survive). The model then re-predicts from a clean slate.
        cleaned = re.sub(r"(?<!\d)[.,;:!?](?!\d)", "", text)
        words = cleaned.split()
        if not words:
            _out.write(json.dumps({"status": "ok", "text": text}) + "\n")
            _out.flush()
            continue

        # ── Step 2: split into overlapping chunks ──────────────────────
        # The punctuation model has a ~512-token context limit; 230 words
        # leaves plenty of headroom for sub-word tokenization. For long
        # transcripts we feed sequential chunks. The 5-word overlap lets
        # us drop the last 5 words of each non-final chunk — those words
        # might have been sentence-final but the model couldn't see the
        # next chunk's context, so we re-predict them in the next chunk
        # where they get proper following context.
        chunk_size = 230
        overlap = 5 if len(words) > chunk_size else 0

        def _chunk(lst, n, stride):
            # Sliding window: step by (chunk_size - overlap) so windows
            # share `overlap` words at their boundaries.
            for i in range(0, len(lst), n - stride):
                yield lst[i:i + n]

        batches = list(_chunk(words, chunk_size, overlap))
        # Defensive: if the very last window happens to consist of
        # nothing BUT overlap tail (≤5 words), the previous chunk
        # already covered those, so drop the redundant window.
        if len(batches) > 1 and len(batches[-1]) <= overlap:
            batches.pop()

        # ── Step 3: tag each word with its predicted punctuation ───────
        # The model returns "entity" labels positioned by CHARACTER
        # offsets inside the joined chunk text. We walk both at once:
        #   char_index    — running byte offset into the chunk text as
        #                   we step through words (incrementing by
        #                   `len(word) + 1` for the trailing space)
        #   result_index  — pointer into the model's entity list
        # When the model's entity ends at or before char_index, that
        # entity's label belongs to the word we just consumed. "0" is
        # the model's "no punctuation here" label.
        tagged = []
        for batch in batches:
            # Drop the last `overlap` words of every non-final batch —
            # they get re-tagged in the next batch with full right
            # context.
            ov = 0 if batch is batches[-1] else overlap
            text_chunk = " ".join(batch)
            result = pipe(text_chunk)
            char_index = 0
            result_index = 0
            for word in batch[:len(batch) - ov]:
                char_index += len(word) + 1  # +1 for the joining space
                label = "0"
                while result_index < len(result) and char_index > result[result_index]["end"]:
                    label = result[result_index]["entity"]
                    result_index += 1
                tagged.append((word, label))

        # ── Step 4: rebuild punctuated text from (word, label) pairs ──
        # Label "0" means "just a space after this word". Any of
        # `. , ? - :` (a subset of what the model emits — we keep the
        # ones that make sense in a transcript) is appended right after
        # the word, then a space.
        out = ""
        for word, label in tagged:
            out += word
            if label == "0":
                out += " "
            elif label in ".,?-:":
                out += label + " "
        out = out.strip()

        # ── Step 5: capitalize sentence starts ─────────────────────────
        # The model doesn't restore casing — it only predicts
        # punctuation — so we capitalize the first letter after any
        # `. ! ?` and the very first character of the whole transcript.
        out = re.sub(r"([.!?]\s+)(\w)", lambda m: m.group(1) + m.group(2).upper(), out)
        if out:
            out = out[0].upper() + out[1:]

        _out.write(json.dumps({"status": "ok", "text": out}) + "\n")
        _out.flush()
    except Exception as e:
        _out.write(json.dumps({"status": "error", "text": str(e)}) + "\n")
        _out.flush()
