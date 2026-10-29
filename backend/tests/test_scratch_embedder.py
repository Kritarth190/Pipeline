"""Unit tests for scratch_embedder.py (numpy inference). Builds a tiny model by hand: no torch needed."""

import json

import numpy as np
import pytest
from tokenizers import Tokenizer, models, normalizers, pre_tokenizers, trainers

from scratch_embedder import ScratchEmbedder, scratch_model_info


def make_model(path, dim=8, seed=0, max_len=512):
    corpus = ["red green blue yellow purple orange", "cat dog bird fish horse cow"] * 30
    tok = Tokenizer(models.WordPiece(unk_token="[UNK]"))
    tok.normalizer = normalizers.BertNormalizer(lowercase=True)
    tok.pre_tokenizer = pre_tokenizers.BertPreTokenizer()
    tok.train_from_iterator(corpus, trainers.WordPieceTrainer(vocab_size=100, special_tokens=["[PAD]", "[UNK]"]))
    path.mkdir(parents=True, exist_ok=True)
    tok.save(str(path / "tokenizer.json"))
    v = tok.get_vocab_size()
    rng = np.random.default_rng(seed)
    np.savez(path / "weights.npz", emb=rng.normal(size=(v, dim)).astype(np.float16),
             tok_w=np.zeros(v, dtype=np.float16))
    (path / "meta.json").write_text(json.dumps({"name": "test-model", "dim": dim, "vocab_size": v, "max_len": max_len}))
    return path


@pytest.fixture()
def model_dir(tmp_path):
    return make_model(tmp_path / "m")


def test_loads_and_reports_info(model_dir):
    assert scratch_model_info(model_dir)["name"] == "test-model"
    e = ScratchEmbedder(model_dir)
    assert e.dim == 8 and e.model == "test-model"


def test_vectors_are_unit_length(model_dir):
    out = ScratchEmbedder(model_dir).embed(["red green", "cat dog bird", "red"])
    assert out.shape == (3, 8)
    assert np.allclose(np.linalg.norm(out, axis=1), 1.0, atol=1e-5)


def test_is_deterministic(model_dir):
    e = ScratchEmbedder(model_dir)
    assert np.array_equal(e.embed(["red green blue"]), e.embed(["red green blue"]))


def test_batch_and_single_results_agree(model_dir):
    e = ScratchEmbedder(model_dir)
    batch = e.embed(["red green", "cat dog"])
    assert np.allclose(batch[1], e.embed(["cat dog"])[0], atol=1e-6)


def test_case_is_ignored(model_dir):
    e = ScratchEmbedder(model_dir)
    assert np.allclose(e.embed(["RED Green"]), e.embed(["red green"]))


def test_word_order_does_not_matter_by_design(model_dir):
    e = ScratchEmbedder(model_dir)
    assert np.allclose(e.embed(["red green blue"]), e.embed(["blue red green"]), atol=1e-6)


def test_empty_text_gives_zero_vector_not_nan(model_dir):
    out = ScratchEmbedder(model_dir).embed(["", "   "])
    assert np.all(np.isfinite(out)) and np.allclose(out, 0)


def test_unknown_words_do_not_crash(model_dir):
    out = ScratchEmbedder(model_dir).embed(["zzzz qqqq xxxx"])
    assert np.all(np.isfinite(out))


def test_empty_batch(model_dir):
    assert ScratchEmbedder(model_dir).embed([]).shape == (0, 8)


def test_long_text_is_truncated_not_rejected(tmp_path):
    e = ScratchEmbedder(make_model(tmp_path / "m", max_len=16))
    long_text = " ".join(["red"] * 5000)
    assert np.allclose(e.embed([long_text]), e.embed([" ".join(["red"] * 16)]), atol=1e-5)


def test_missing_model_gives_actionable_error(tmp_path):
    assert scratch_model_info(tmp_path / "nope") is None
    with pytest.raises(RuntimeError, match="train_embedder"):
        ScratchEmbedder(tmp_path / "nope")


def test_incomplete_model_folder_is_not_available(model_dir):
    (model_dir / "weights.npz").unlink()
    assert scratch_model_info(model_dir) is None


def test_corrupt_meta_is_not_available(model_dir):
    (model_dir / "meta.json").write_text("{not json")
    assert scratch_model_info(model_dir) is None


def test_mismatched_weights_are_rejected(model_dir):
    np.savez(model_dir / "weights.npz", emb=np.zeros((10, 8), dtype=np.float16), tok_w=np.zeros(7, dtype=np.float16))
    with pytest.raises(RuntimeError, match="Corrupt"):
        ScratchEmbedder(model_dir)


def test_corrupt_weights_file_gives_clear_error(model_dir):
    (model_dir / "weights.npz").write_bytes(b"not a zip")
    with pytest.raises(Exception):
        ScratchEmbedder(model_dir)


def test_model_dir_env_override(monkeypatch, model_dir):
    from scratch_embedder import model_dir as resolve
    monkeypatch.setenv("SCRATCH_EMBEDDER_DIR", str(model_dir))
    assert str(resolve()) == str(model_dir)
    assert scratch_model_info() is not None


def test_dataset_pipeline_uses_scratch_embedder(model_dir, monkeypatch, tmp_path):
    import pandas as pd
    import rag_prep
    from rag_prep import RAGConfig, build_rag_dataset

    class WS(rag_prep.TextTokenizer):
        def __init__(self): self.name, self._tok, self.kind = "ws", None, "whitespace"

    monkeypatch.setattr(rag_prep, "get_tokenizer", lambda n: WS())
    monkeypatch.setenv("SCRATCH_EMBEDDER_DIR", str(model_dir))
    df = pd.DataFrame({"id": [1, 2], "text": ["red green blue " * 20, "cat dog bird " * 20]})
    cfg = RAGConfig(text_columns=["text"], embedder="scratch", chunk_tokens=32, chunk_overlap=4)
    stats = build_rag_dataset(df, cfg, str(tmp_path / "o.jsonl"))
    rows = [json.loads(l) for l in open(tmp_path / "o.jsonl")]
    assert stats["embedding_dim"] == 8 and stats["embedding_model"] == "test-model"
    assert len(rows[0]["embedding"]) == 8
