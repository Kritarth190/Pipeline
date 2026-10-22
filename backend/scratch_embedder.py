"""
Inference for the from-scratch embedder trained with tools/train_embedder.py.

The trained model is a tokenizer plus a small table of word-piece vectors, so
running it needs only numpy and `tokenizers` (no PyTorch, no network, no API
token). It loads in milliseconds and a free 512 MB server can run it.

Model folder layout (default: backend/models/scratch-embedder/):
    tokenizer.json   word-piece tokenizer learned on your own data
    weights.npz      emb: (vocab, dim) vectors, tok_w: (vocab,) importance logits
    meta.json        {"dim": 128, "vocab_size": 16000, "max_len": 512, ...}

A text is embedded as the importance-weighted mean of its token vectors,
L2-normalised, exactly as during training.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np

from rag_prep import Embedder, _l2_normalize

DEFAULT_MODEL_DIR = Path(__file__).resolve().parent / "models" / "scratch-embedder"
REQUIRED_FILES = ("tokenizer.json", "weights.npz", "meta.json")


def model_dir() -> Path:
    return Path(os.environ.get("SCRATCH_EMBEDDER_DIR") or DEFAULT_MODEL_DIR)


def scratch_model_info(path: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    """Return the model's meta.json if a complete model folder exists, else None."""
    path = path or model_dir()
    if not all((path / name).is_file() for name in REQUIRED_FILES):
        return None
    try:
        return json.loads((path / "meta.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _softplus(x: np.ndarray) -> np.ndarray:
    # log(1 + e^x), stable for large |x|; keeps every token weight positive
    return np.logaddexp(0.0, x)


class ScratchEmbedder(Embedder):
    name = "scratch"

    def __init__(self, path: Optional[Path] = None):
        path = Path(path) if path else model_dir()
        meta = scratch_model_info(path)
        if meta is None:
            raise RuntimeError(
                f"No trained embedder found in '{path}'. Train one with tools/train_embedder.py "
                "and copy the output folder there (see the README), or untick 'Create embeddings'."
            )
        from tokenizers import Tokenizer

        self._tok = Tokenizer.from_file(str(path / "tokenizer.json"))
        self._tok.no_padding()
        self._tok.enable_truncation(max_length=int(meta.get("max_len", 512)))

        with np.load(path / "weights.npz") as weights:
            self._emb = weights["emb"].astype(np.float32)
            self._tok_w = _softplus(weights["tok_w"].astype(np.float32))

        if self._emb.shape[0] != self._tok_w.shape[0]:
            raise RuntimeError("Corrupt model: embedding table and token weights differ in size")
        self.dim = int(self._emb.shape[1])
        self.model = meta.get("name", "scratch-embedder")

    def embed(self, texts: List[str]) -> np.ndarray:
        encodings = self._tok.encode_batch(list(texts), add_special_tokens=False)
        out = np.zeros((len(texts), self.dim), dtype=np.float32)
        for row, enc in enumerate(encodings):
            ids = np.asarray(enc.ids, dtype=np.int64)
            if ids.size == 0:
                continue  # nothing to embed; stays a zero vector
            weights = self._tok_w[ids]
            out[row] = (weights[:, None] * self._emb[ids]).sum(axis=0) / weights.sum()
        return _l2_normalize(out)
