"""
RAG preparation: text cleaning, token-aware chunking and embedding.

Turns a tabular dataset (Kaggle / Hugging Face / any CSV) into a JSONL file of
chunks that can be loaded straight into a vector store:

    {"chunk_id": "12::0", "doc_id": "12", "chunk_index": 0, "text": "...",
     "token_count": 241, "char_start": 0, "char_end": 1180,
     "metadata": {"label": "sports"}, "embedding": [0.01, ...]}

Designed for small hosts (e.g. Render free tier, 512 MB RAM):
  * tokenizing uses the lightweight `tokenizers` library (no PyTorch)
  * embeddings come from the Hugging Face Inference API (HF_TOKEN) or,
    optionally, a local ONNX model through `fastembed`
  * rows are streamed to disk in batches, never held in memory all at once
"""

from __future__ import annotations

import hashlib
import html
import json
import logging
import math
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "BAAI/bge-small-en-v1.5"
MAX_CHUNK_TOKENS = 512  # most BERT-style embedders truncate beyond this


# ============================================================================
# CONFIG
# ============================================================================

@dataclass
class RAGConfig:
    text_columns: List[str] = field(default_factory=list)   # empty -> auto-detect
    id_column: Optional[str] = None                          # None -> row number
    metadata_columns: Optional[List[str]] = None             # None -> auto, [] -> none
    tokenizer_name: str = DEFAULT_MODEL
    chunk_tokens: int = 256
    chunk_overlap: int = 32
    min_doc_tokens: int = 5
    embed: bool = True
    embedder: str = "hf_api"                                 # hf_api | fastembed
    embedding_model: str = DEFAULT_MODEL
    batch_size: int = 32
    max_rows: int = 5000
    dedupe: bool = True

    @classmethod
    def from_dict(cls, raw: Optional[Dict[str, Any]], max_rows_cap: int = 20000) -> "RAGConfig":
        """Build a config from untrusted user input, clamping every value."""
        raw = raw or {}
        cfg = cls()

        def str_list(value) -> List[str]:
            if not isinstance(value, list):
                return []
            return [str(v) for v in value if isinstance(v, (str, int))][:20]

        def clamp_int(value, default, lo, hi) -> int:
            try:
                return max(lo, min(hi, int(value)))
            except (TypeError, ValueError):
                return default

        cfg.text_columns = str_list(raw.get("text_columns"))
        cfg.id_column = str(raw["id_column"]) if raw.get("id_column") else None
        if raw.get("metadata_columns") is not None:
            cfg.metadata_columns = str_list(raw.get("metadata_columns"))
        cfg.chunk_tokens = clamp_int(raw.get("chunk_tokens"), cfg.chunk_tokens, 32, MAX_CHUNK_TOKENS)
        cfg.chunk_overlap = clamp_int(raw.get("chunk_overlap"), cfg.chunk_overlap, 0, cfg.chunk_tokens // 2)
        cfg.max_rows = clamp_int(raw.get("max_rows"), cfg.max_rows, 1, max_rows_cap)
        cfg.batch_size = clamp_int(raw.get("batch_size"), cfg.batch_size, 1, 128)
        cfg.embed = bool(raw.get("embed", True))
        cfg.dedupe = bool(raw.get("dedupe", True))
        if raw.get("embedder") in ("hf_api", "fastembed"):
            cfg.embedder = raw["embedder"]
        for key in ("tokenizer_name", "embedding_model"):
            value = raw.get(key)
            # Hugging Face model ids look like "org/name" or "name"
            if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][\w.\-]*(/[A-Za-z0-9][\w.\-]*)?", value):
                setattr(cfg, key, value)
        # Keep the tokenizer aligned with the embedder unless the user chose otherwise
        if "tokenizer_name" not in raw:
            cfg.tokenizer_name = cfg.embedding_model
        return cfg


# ============================================================================
# TEXT CLEANING
# ============================================================================

_TAG_RE = re.compile(r"<[^>]+>")
_CTRL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_SPACE_RE = re.compile(r"[ \t\r\f\v]+")
_BLANKS_RE = re.compile(r"\n{3,}")


def clean_text(value: Any) -> str:
    """Normalise one text value: unescape HTML, strip tags/control chars, tidy whitespace."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    text = html.unescape(str(value))
    text = _TAG_RE.sub(" ", text)
    text = _CTRL_RE.sub("", text)
    text = _SPACE_RE.sub(" ", text)
    text = re.sub(r" ?\n ?", "\n", text)
    text = _BLANKS_RE.sub("\n\n", text)
    return text.strip()


# ============================================================================
# TOKENIZER + CHUNKER
# ============================================================================

class TextTokenizer:
    """
    Wraps a Hugging Face `tokenizers` tokenizer. Falls back to whitespace
    tokens if the model's tokenizer can't be downloaded, and records that in
    `kind` so the job metrics can warn about it.
    """

    def __init__(self, name: str):
        self.name = name
        self._tok = None
        self.kind = "whitespace"
        try:
            from tokenizers import Tokenizer

            tok = Tokenizer.from_pretrained(name)
            tok.no_truncation()
            tok.no_padding()
            self._tok = tok
            self.kind = "huggingface"
        except Exception as exc:  # network error, unknown model, missing package
            logger.warning("Tokenizer '%s' unavailable (%s); using whitespace tokens", name, exc)

    def offsets(self, text: str) -> List[Tuple[int, int]]:
        """Character span of every token (special tokens excluded)."""
        if self._tok is not None:
            enc = self._tok.encode(text, add_special_tokens=False)
            return [tuple(o) for o in enc.offsets]
        return [m.span() for m in re.finditer(r"\S+", text)]

    def count(self, text: str) -> int:
        return len(self.offsets(text))


_tokenizer_cache: Dict[str, TextTokenizer] = {}


def get_tokenizer(name: str) -> TextTokenizer:
    """Load each tokenizer once per process; a failed (whitespace) load is retried next time."""
    cached = _tokenizer_cache.get(name)
    if cached is not None:
        return cached
    tok = TextTokenizer(name)
    if tok.kind == "huggingface":
        _tokenizer_cache[name] = tok
    return tok


def chunk_text(
    text: str, tokenizer: TextTokenizer, chunk_tokens: int, overlap: int, min_doc_tokens: int = 1
) -> List[Dict[str, Any]]:
    """
    Split `text` into windows of `chunk_tokens` tokens with `overlap` tokens of
    overlap. Chunk text is sliced from the original string using token
    offsets, so it is exact (no detokenizing artefacts like lowercasing).
    """
    spans = tokenizer.offsets(text)
    n = len(spans)
    if n < min_doc_tokens:
        return []

    step = max(1, chunk_tokens - overlap)
    chunks = []
    start = 0
    while start < n:
        end = min(start + chunk_tokens, n)
        char_start, char_end = spans[start][0], spans[end - 1][1]
        piece = text[char_start:char_end].strip()
        if piece:
            chunks.append(
                {
                    "text": piece,
                    "token_count": end - start,
                    "char_start": char_start,
                    "char_end": char_end,
                }
            )
        if end == n:
            break
        start += step
    return chunks


# ============================================================================
# EMBEDDERS
# ============================================================================

class Embedder:
    name: str = "base"
    model: str = ""

    def embed(self, texts: List[str]) -> np.ndarray:  # pragma: no cover - interface
        raise NotImplementedError


def _l2_normalize(arr: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(arr, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return arr / norms


class HFAPIEmbedder(Embedder):
    """Hosted embeddings through the Hugging Face Inference API. Needs HF_TOKEN."""

    name = "hf_api"

    def __init__(self, model: str, token: Optional[str] = None, retries: int = 4):
        token = token or os.environ.get("HF_TOKEN")
        if not token:
            raise RuntimeError(
                "HF_TOKEN is not set. Create a free access token at "
                "huggingface.co/settings/tokens and add it as an environment variable."
            )
        from huggingface_hub import InferenceClient

        self.model = model
        self.retries = retries
        self._client = InferenceClient(token=token)

    def embed(self, texts: List[str]) -> np.ndarray:
        last_error: Optional[Exception] = None
        for attempt in range(self.retries):
            try:
                out = np.asarray(self._client.feature_extraction(texts, model=self.model), dtype=np.float32)
                if out.ndim == 3:  # token-level output -> mean pool
                    out = out.mean(axis=1)
                if out.ndim == 1:
                    out = out.reshape(1, -1)
                if out.shape[0] != len(texts):
                    raise RuntimeError(f"Expected {len(texts)} embeddings, got {out.shape[0]}")
                return _l2_normalize(out)
            except Exception as exc:  # rate limit, cold model, transient network
                last_error = exc
                wait = 2 ** attempt
                logger.warning("Embedding attempt %d/%d failed: %s (retry in %ss)", attempt + 1, self.retries, exc, wait)
                time.sleep(wait)
        raise RuntimeError(f"Embedding failed after {self.retries} attempts: {last_error}")


class FastEmbedEmbedder(Embedder):
    """Local ONNX embeddings (no PyTorch). Needs `pip install fastembed`; uses ~200 MB RAM."""

    name = "fastembed"

    def __init__(self, model: str):
        try:
            from fastembed import TextEmbedding
        except ImportError as exc:
            raise RuntimeError("fastembed is not installed. Run: pip install fastembed") from exc
        self.model = model
        self._model = TextEmbedding(model_name=model)

    def embed(self, texts: List[str]) -> np.ndarray:
        return _l2_normalize(np.asarray(list(self._model.embed(texts)), dtype=np.float32))


def get_embedder(cfg: RAGConfig) -> Embedder:
    if cfg.embedder == "fastembed":
        return FastEmbedEmbedder(cfg.embedding_model)
    return HFAPIEmbedder(cfg.embedding_model)


# ============================================================================
# DATAFRAME HELPERS
# ============================================================================

def load_text_table(path: str, max_rows: int) -> pd.DataFrame:
    """Read a CSV/TXT upload. A plain text file (one document per line) becomes a `text` column."""
    try:
        df = pd.read_csv(path, nrows=max_rows)
        if len(df.columns) > 1 or str(path).lower().endswith(".csv"):
            return df
    except Exception:
        pass
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        lines = [line.rstrip("\n") for _, line in zip(range(max_rows), fh)]
    return pd.DataFrame({"text": [ln for ln in lines if ln.strip()]})


def analyze_text_columns(df: pd.DataFrame, sample: int = 500) -> Dict[str, Any]:
    """Describe each column and suggest which ones hold text worth embedding."""
    head = df.head(sample)
    columns = []
    for col in df.columns:
        series = head[col]
        is_texty = not pd.api.types.is_numeric_dtype(series) and not pd.api.types.is_bool_dtype(series)
        avg_chars = float(series.dropna().astype(str).str.len().mean()) if is_texty and series.notna().any() else 0.0
        columns.append(
            {
                "name": str(col),
                "dtype": str(df[col].dtype),
                "avg_chars": round(avg_chars, 1),
                "unique_ratio": round(float(series.nunique() / max(len(series), 1)), 3),
                "text_candidate": bool(is_texty and avg_chars >= 40),
            }
        )
    suggested = [c["name"] for c in sorted(columns, key=lambda c: -c["avg_chars"]) if c["text_candidate"]][:3]
    id_guess = next((c["name"] for c in columns if c["name"].lower() in ("id", "doc_id", "uuid") and c["unique_ratio"] > 0.99), None)
    return {"columns": columns, "suggested_text_columns": suggested, "suggested_id_column": id_guess, "rows_sampled": len(head)}


def _json_safe(value: Any) -> Any:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if math.isnan(float(value)) else float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _auto_metadata_columns(df: pd.DataFrame, text_cols: List[str], id_col: Optional[str]) -> List[str]:
    """Keep short, non-text columns (labels, dates, numbers) as chunk metadata."""
    picked = []
    for col in df.columns:
        if col in text_cols or col == id_col:
            continue
        series = df[col].head(200)
        if pd.api.types.is_numeric_dtype(series) or pd.api.types.is_bool_dtype(series):
            picked.append(col)
        elif series.dropna().astype(str).str.len().mean() <= 80:
            picked.append(col)
        if len(picked) >= 10:
            break
    return picked


def _iter_documents(df: pd.DataFrame, text_cols: List[str], id_col: Optional[str], meta_cols: List[str]) -> Iterator[Dict[str, Any]]:
    for pos, row in enumerate(df.itertuples(index=False, name=None)):
        record = dict(zip(df.columns, row))
        parts = [clean_text(record[c]) for c in text_cols]
        text = "\n\n".join(p for p in parts if p)
        doc_id = str(record[id_col]) if id_col and pd.notna(record[id_col]) else str(pos)
        yield {"doc_id": doc_id, "text": text, "metadata": {c: _json_safe(record[c]) for c in meta_cols}}


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def build_rag_dataset(
    df: pd.DataFrame,
    cfg: RAGConfig,
    output_path: str,
    embedder: Optional[Embedder] = None,
    progress_cb: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    """
    Clean -> chunk -> (embed) -> write JSONL. Returns run statistics.
    `embedder` can be injected (used by the tests); otherwise one is built from cfg.
    """
    started = time.time()
    df = df.head(cfg.max_rows)
    if df.empty:
        raise ValueError("The dataset has no rows")

    analysis = analyze_text_columns(df)
    text_cols = [c for c in cfg.text_columns if c in df.columns] or analysis["suggested_text_columns"]
    if not text_cols:
        raise ValueError(
            "No text column found. Choose at least one column that contains the text to embed."
        )
    id_col = cfg.id_column if cfg.id_column in df.columns else analysis["suggested_id_column"]
    meta_cols = (
        [c for c in cfg.metadata_columns if c in df.columns and c not in text_cols]
        if cfg.metadata_columns is not None
        else _auto_metadata_columns(df, text_cols, id_col)
    )

    tokenizer = get_tokenizer(cfg.tokenizer_name)
    if cfg.embed and embedder is None:
        embedder = get_embedder(cfg)

    stats = {
        "mode": "rag",
        "rows_read": len(df),
        "documents": 0,
        "documents_skipped_empty": 0,
        "documents_skipped_duplicate": 0,
        "chunks": 0,
        "total_tokens": 0,
        "text_columns": text_cols,
        "id_column": id_col,
        "metadata_columns": meta_cols,
        "tokenizer": cfg.tokenizer_name,
        "tokenizer_kind": tokenizer.kind,
        "embedding_model": embedder.model if embedder else None,
        "embedding_dim": None,
        "chunk_tokens": cfg.chunk_tokens,
        "chunk_overlap": cfg.chunk_overlap,
    }
    if tokenizer.kind != "huggingface":
        stats["warning"] = (
            f"Tokenizer '{cfg.tokenizer_name}' could not be loaded, so whitespace words were used "
            "instead of real tokens. Token counts are approximate."
        )

    seen_hashes = set()
    pending: List[Dict[str, Any]] = []
    total_docs = len(df)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    def flush(fh) -> None:
        if not pending:
            return
        if embedder is not None:
            vectors = embedder.embed([c["text"] for c in pending])
            stats["embedding_dim"] = int(vectors.shape[1])
            for chunk, vec in zip(pending, vectors):
                chunk["embedding"] = [round(float(x), 6) for x in vec]
        for chunk in pending:
            fh.write(json.dumps(chunk, ensure_ascii=False) + "\n")
        stats["chunks"] += len(pending)
        pending.clear()

    with open(out, "w", encoding="utf-8") as fh:
        for done, doc in enumerate(_iter_documents(df, text_cols, id_col, meta_cols), start=1):
            text = doc["text"]
            if not text:
                stats["documents_skipped_empty"] += 1
            else:
                digest = hashlib.sha1(text.encode("utf-8")).hexdigest()
                if cfg.dedupe and digest in seen_hashes:
                    stats["documents_skipped_duplicate"] += 1
                else:
                    seen_hashes.add(digest)
                    pieces = chunk_text(text, tokenizer, cfg.chunk_tokens, cfg.chunk_overlap, cfg.min_doc_tokens)
                    if not pieces:
                        stats["documents_skipped_empty"] += 1
                    for k, piece in enumerate(pieces):
                        pending.append(
                            {
                                "chunk_id": f"{doc['doc_id']}::{k}",
                                "doc_id": doc["doc_id"],
                                "chunk_index": k,
                                **piece,
                                "metadata": doc["metadata"],
                            }
                        )
                        stats["total_tokens"] += piece["token_count"]
                    if pieces:
                        stats["documents"] += 1
            if len(pending) >= cfg.batch_size:
                flush(fh)
                if progress_cb:
                    progress_cb({"progress": round(done / total_docs, 3), "chunks": stats["chunks"]})
        flush(fh)

    if stats["chunks"] == 0:
        raise ValueError("No chunks were produced. Check the selected text columns.")

    stats["avg_chunk_tokens"] = round(stats["total_tokens"] / stats["chunks"], 1)
    stats["processing_time"] = round(time.time() - started, 2)
    stats["output_format"] = "jsonl"
    return stats
