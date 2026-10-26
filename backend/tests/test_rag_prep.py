"""Unit tests for rag_prep.py: cleaning, chunking, config validation, dataset building."""

import json
import math

import numpy as np
import pandas as pd
import pytest

import rag_prep
from rag_prep import (RAGConfig, TextTokenizer, analyze_text_columns, build_rag_dataset, chunk_text,
                      clean_text, load_text_table)
from conftest import FakeEmbedder


class WhitespaceTokenizer(TextTokenizer):
    """Offline tokenizer: one token per word (what the app falls back to without the hub)."""
    def __init__(self):
        self.name, self._tok, self.kind = "whitespace", None, "whitespace"


@pytest.fixture(autouse=True)
def offline_tokenizer(monkeypatch):
    # Never touch the Hugging Face hub from unit tests
    monkeypatch.setattr(rag_prep, "get_tokenizer", lambda name: WhitespaceTokenizer())


# ---------------------------------------------------------------- clean_text
class TestCleanText:
    def test_strips_html_and_unescapes(self):
        assert clean_text("<p>Fish &amp; <b>chips</b></p>") == "Fish & chips"

    def test_collapses_whitespace_but_keeps_paragraphs(self):
        assert clean_text("a   b\t\tc\n\n\n\nd") == "a b c\n\nd"

    def test_removes_control_characters(self):
        assert clean_text("he\x00llo\x07") == "hello"

    @pytest.mark.parametrize("value", [None, float("nan"), "", "   \n "])
    def test_empty_values(self, value):
        assert clean_text(value) == ""

    def test_numbers_become_text(self):
        assert clean_text(12.5) == "12.5"

    def test_comparison_operators_are_not_mistaken_for_html_tags(self):
        # "x < 5 and y > 3" is ordinary text (maths, code, forum posts); only real tags may be removed
        assert "5 and y" in clean_text("if x < 5 and y > 3 then stop")


# ---------------------------------------------------------------- chunk_text
class TestChunkText:
    tok = WhitespaceTokenizer()

    def test_short_text_is_one_chunk(self):
        chunks = chunk_text("one two three", self.tok, 10, 2)
        assert [c["text"] for c in chunks] == ["one two three"]
        assert chunks[0]["token_count"] == 3

    def test_window_size_and_overlap(self):
        text = " ".join(str(i) for i in range(100))
        chunks = chunk_text(text, self.tok, 30, 10)
        assert all(c["token_count"] <= 30 for c in chunks)
        first, second = chunks[0]["text"].split(), chunks[1]["text"].split()
        assert first[-10:] == second[:10]

    def test_every_token_is_covered(self):
        text = " ".join(str(i) for i in range(137))
        chunks = chunk_text(text, self.tok, 32, 8)
        seen = set()
        for c in chunks:
            seen.update(c["text"].split())
        assert seen == {str(i) for i in range(137)}

    def test_chunk_text_is_exact_slice_of_original(self):
        text = "Alpha,  Beta!  Gamma?\nDelta"
        for c in chunk_text(text, self.tok, 2, 0):
            assert text[c["char_start"]:c["char_end"]].strip() == c["text"]

    def test_below_min_doc_tokens_is_skipped(self):
        assert chunk_text("a b", self.tok, 10, 0, min_doc_tokens=5) == []

    def test_empty_text(self):
        assert chunk_text("", self.tok, 10, 2) == []

    def test_no_overlap_gives_disjoint_chunks(self):
        chunks = chunk_text(" ".join(map(str, range(20))), self.tok, 5, 0)
        assert [c["token_count"] for c in chunks] == [5, 5, 5, 5]

    def test_exact_multiple_does_not_emit_empty_tail(self):
        chunks = chunk_text(" ".join(map(str, range(10))), self.tok, 5, 0)
        assert len(chunks) == 2

    def test_overlap_equal_to_chunk_size_terminates(self):
        # from_dict prevents this, but chunk_text must never loop forever
        chunks = chunk_text(" ".join(map(str, range(12))), self.tok, 4, 4)
        assert len(chunks) >= 3


# ---------------------------------------------------------------- RAGConfig
class TestRAGConfig:
    def test_defaults(self):
        cfg = RAGConfig.from_dict({})
        assert (cfg.chunk_tokens, cfg.chunk_overlap, cfg.embed) == (256, 32, True)

    def test_values_are_clamped(self):
        cfg = RAGConfig.from_dict({"chunk_tokens": 99999, "chunk_overlap": 99999, "batch_size": 0,
                                   "max_rows": 10 ** 9}, max_rows_cap=500)
        assert cfg.chunk_tokens == 512
        assert cfg.chunk_overlap == 256          # at most half a chunk
        assert cfg.batch_size == 1
        assert cfg.max_rows == 500

    def test_garbage_numbers_fall_back_to_defaults(self):
        cfg = RAGConfig.from_dict({"chunk_tokens": "abc", "max_rows": None})
        assert cfg.chunk_tokens == 256 and cfg.max_rows == 5000

    @pytest.mark.parametrize("bad", ["../evil", "a/b/c", "x y", "", "/etc/passwd", ".hidden", "a;rm -rf"])
    def test_unsafe_model_ids_are_ignored(self, bad):
        cfg = RAGConfig.from_dict({"embedding_model": bad})
        assert cfg.embedding_model == rag_prep.DEFAULT_MODEL

    def test_valid_model_id_is_accepted_and_tokenizer_follows(self):
        cfg = RAGConfig.from_dict({"embedding_model": "sentence-transformers/all-MiniLM-L6-v2"})
        assert cfg.tokenizer_name == "sentence-transformers/all-MiniLM-L6-v2"

    def test_unknown_embedder_is_ignored(self):
        assert RAGConfig.from_dict({"embedder": "evil"}).embedder == "hf_api"
        assert RAGConfig.from_dict({"embedder": "scratch"}).embedder == "scratch"

    def test_column_lists_are_sanitised(self):
        cfg = RAGConfig.from_dict({"text_columns": ["a", {"x": 1}, 5, None], "metadata_columns": "oops"})
        assert cfg.text_columns == ["a", "5"]
        assert cfg.metadata_columns == []

    @pytest.mark.parametrize("raw", [None, {}, [], "", 0])
    def test_empty_inputs_are_fine(self, raw):
        assert RAGConfig.from_dict(raw).chunk_tokens == 256

    @pytest.mark.parametrize("raw", [["a"], "text", 5, True])
    def test_non_dict_config_is_handled_without_crashing(self, raw):
        # The API passes whatever JSON the client sent as `config`
        RAGConfig.from_dict(raw)


# ---------------------------------------------------------------- analyze / load
class TestAnalyze:
    def test_suggests_long_text_columns_only(self):
        df = pd.DataFrame({"id": range(5), "title": ["t"] * 5, "body": ["x" * 200] * 5, "n": [1.5] * 5})
        info = analyze_text_columns(df)
        assert info["suggested_text_columns"] == ["body"]
        assert info["suggested_id_column"] == "id"

    def test_numeric_column_is_never_text(self):
        info = analyze_text_columns(pd.DataFrame({"n": [10 ** 12] * 5}))
        assert info["suggested_text_columns"] == []

    def test_empty_frame(self):
        assert analyze_text_columns(pd.DataFrame({"a": []}))["suggested_text_columns"] == []


class TestLoadTextTable:
    def test_csv(self, tmp_path):
        p = tmp_path / "a.csv"
        p.write_text("id,text\n1,hello world\n2,bye\n")
        assert list(load_text_table(str(p), 10).columns) == ["id", "text"]

    def test_max_rows(self, tmp_path):
        p = tmp_path / "a.csv"
        p.write_text("t\n" + "\n".join(f"row {i}" for i in range(50)))
        assert len(load_text_table(str(p), 7)) == 7

    def test_plain_text_is_one_document_per_line(self, tmp_path):
        p = tmp_path / "a.txt"
        p.write_text("first doc\nsecond doc\n\nthird doc\n")
        df = load_text_table(str(p), 10)
        assert list(df.columns) == ["text"] and len(df) == 3

    def test_empty_file_gives_empty_frame(self, tmp_path):
        p = tmp_path / "a.csv"
        p.write_text("")
        assert len(load_text_table(str(p), 10)) == 0

    def test_ragged_csv_is_not_silently_turned_into_lines(self, tmp_path):
        # A CSV with a row that has too many fields breaks the parser. The header line must not
        # quietly become a "document" (that would pollute the dataset with a row of column names).
        p = tmp_path / "bad.csv"
        p.write_text("id,text\n1,hello\n2,a,b,c,d\n3,fine\n")
        df = load_text_table(str(p), 10)
        assert "id,text" not in df.iloc[:, 0].astype(str).tolist()


# ---------------------------------------------------------------- build_rag_dataset
def _docs(n=6, words=80):
    return pd.DataFrame({"id": range(n), "text": [" ".join(f"w{(i + j) % 40}" for j in range(words)) for i in range(n)],
                         "label": ["x", "y"] * (n // 2)})


def _read(path):
    return [json.loads(l) for l in open(path, encoding="utf-8")]


class TestBuildRagDataset:
    def cfg(self, **kw):
        base = dict(text_columns=["text"], id_column="id", chunk_tokens=32, chunk_overlap=4, embed=False, batch_size=5)
        base.update(kw)
        return RAGConfig(**base)

    def test_writes_chunks_with_metadata_and_ids(self, tmp_path):
        out = tmp_path / "o.jsonl"
        stats = build_rag_dataset(_docs(), self.cfg(), str(out))
        rows = _read(out)
        assert stats["chunks"] == len(rows) and stats["documents"] == 6
        assert rows[0]["chunk_id"] == "0::0" and rows[0]["metadata"] == {"label": "x"}
        assert "embedding" not in rows[0]

    def test_embeddings_are_attached_in_batches(self, tmp_path):
        emb = FakeEmbedder()
        out = tmp_path / "o.jsonl"
        stats = build_rag_dataset(_docs(), self.cfg(embed=True), str(out), embedder=emb)
        rows = _read(out)
        assert all(len(r["embedding"]) == 8 for r in rows)
        assert stats["embedding_dim"] == 8 and sum(emb.calls) == len(rows)
        assert max(emb.calls) <= 5 + 4   # a batch is flushed once it reaches batch_size (plus one doc's chunks)

    def test_duplicates_and_empties_are_skipped(self, tmp_path):
        df = pd.DataFrame({"id": [1, 2, 3, 4], "text": ["same text here " * 10, "same text here " * 10, None, ""]})
        stats = build_rag_dataset(df, self.cfg(), str(tmp_path / "o.jsonl"))
        assert (stats["documents"], stats["documents_skipped_duplicate"], stats["documents_skipped_empty"]) == (1, 1, 2)

    def test_dedupe_can_be_turned_off(self, tmp_path):
        df = pd.DataFrame({"id": [1, 2], "text": ["same text here " * 10] * 2})
        stats = build_rag_dataset(df, self.cfg(dedupe=False), str(tmp_path / "o.jsonl"))
        assert stats["documents"] == 2

    def test_empty_dataframe_raises(self, tmp_path):
        with pytest.raises(ValueError):
            build_rag_dataset(pd.DataFrame({"text": []}), self.cfg(), str(tmp_path / "o.jsonl"))

    def test_no_text_column_raises_helpful_error(self, tmp_path):
        with pytest.raises(ValueError, match="text column"):
            build_rag_dataset(pd.DataFrame({"n": [1, 2, 3]}), RAGConfig(embed=False), str(tmp_path / "o.jsonl"))

    def test_auto_detects_text_when_unspecified(self, tmp_path):
        stats = build_rag_dataset(_docs(), RAGConfig(embed=False, chunk_tokens=32, chunk_overlap=4), str(tmp_path / "o.jsonl"))
        assert stats["text_columns"] == ["text"]

    def test_unknown_text_column_falls_back_to_detection(self, tmp_path):
        stats = build_rag_dataset(_docs(), self.cfg(text_columns=["nope"]), str(tmp_path / "o.jsonl"))
        assert stats["text_columns"] == ["text"]

    def test_max_rows(self, tmp_path):
        stats = build_rag_dataset(_docs(10), self.cfg(max_rows=3), str(tmp_path / "o.jsonl"))
        assert stats["rows_read"] == 3

    def test_metadata_is_json_safe(self, tmp_path):
        df = _docs(2)
        df["score"] = [np.float32(1.5), np.nan]
        df["when"] = pd.to_datetime(["2026-01-01", "2026-01-02"])
        df["n"] = np.array([1, 2], dtype=np.int64)
        build_rag_dataset(df, self.cfg(metadata_columns=["score", "when", "n"]), str(tmp_path / "o.jsonl"))
        by_doc = {}
        for r in _read(tmp_path / "o.jsonl"):
            by_doc.setdefault(r["doc_id"], r["metadata"])
        assert by_doc["0"]["score"] == 1.5 and by_doc["1"]["score"] is None
        assert by_doc["0"]["n"] == 1 and by_doc["0"]["when"].startswith("2026-01-01")

    def test_metadata_empty_list_means_none(self, tmp_path):
        build_rag_dataset(_docs(), self.cfg(metadata_columns=[]), str(tmp_path / "o.jsonl"))
        assert _read(tmp_path / "o.jsonl")[0]["metadata"] == {}

    def test_progress_callback_reports_fractions(self, tmp_path):
        seen = []
        build_rag_dataset(_docs(10), self.cfg(batch_size=2), str(tmp_path / "o.jsonl"), progress_cb=seen.append)
        assert seen and all(0 < p["progress"] <= 1 for p in seen)

    def test_chunk_ids_are_unique_even_if_id_column_is_not(self, tmp_path):
        df = pd.DataFrame({"id": [7, 7, 8], "text": [f"unique words number {i} " * 12 for i in range(3)]})
        build_rag_dataset(df, self.cfg(), str(tmp_path / "o.jsonl"))
        ids = [r["chunk_id"] for r in _read(tmp_path / "o.jsonl")]
        assert len(ids) == len(set(ids)), f"duplicate chunk_ids: {sorted(ids)}"

    def test_id_column_with_missing_values(self, tmp_path):
        df = pd.DataFrame({"id": [1.0, None], "text": ["first doc words " * 12, "second doc words " * 12]})
        build_rag_dataset(df, self.cfg(), str(tmp_path / "o.jsonl"))
        ids = {r["doc_id"] for r in _read(tmp_path / "o.jsonl")}
        assert len(ids) == 2

    def test_float_ids_do_not_get_a_trailing_dot_zero(self, tmp_path):
        # pandas turns an int column with a missing value into floats: ids should still read 1, 2
        df = pd.DataFrame({"id": [1.0, 2.0, None], "text": [f"doc {i} words " * 12 for i in range(3)]})
        build_rag_dataset(df, self.cfg(), str(tmp_path / "o.jsonl"))
        assert not any(r["doc_id"].endswith(".0") for r in _read(tmp_path / "o.jsonl"))

    def test_unicode_text_round_trips(self, tmp_path):
        df = pd.DataFrame({"id": [1], "text": ["नमस्ते दुनिया 你好世界 héllo " * 10]})
        build_rag_dataset(df, self.cfg(), str(tmp_path / "o.jsonl"))
        assert "नमस्ते" in _read(tmp_path / "o.jsonl")[0]["text"]

    def test_tokenizer_fallback_is_reported(self, tmp_path):
        stats = build_rag_dataset(_docs(), self.cfg(), str(tmp_path / "o.jsonl"))
        assert stats["tokenizer_kind"] == "whitespace" and "warning" in stats
