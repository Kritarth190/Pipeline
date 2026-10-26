"""Unit tests for dataset_sources.py with the network mocked out."""

import pandas as pd
import pytest

import dataset_sources as ds
from dataset_sources import DatasetImportError


class FakeResponse:
    def __init__(self, payload=None, status=200, text=""):
        self._payload, self.status_code, self.text = payload, status, text
        self.ok = status < 400

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


def install_fake_hub(monkeypatch, total=250, splits=None, calls=None):
    splits = splits if splits is not None else [{"config": "plain", "split": "test"}, {"config": "plain", "split": "train"}]

    def fake_get(url, params=None, headers=None, timeout=None):
        if calls is not None:
            calls.append((url.rsplit("/", 1)[-1], dict(params)))
        if url.endswith("/splits"):
            return FakeResponse({"splits": splits})
        offset, length = int(params["offset"]), int(params["length"])
        rows = [{"row_idx": i, "row": {"text": f"doc {i}", "meta": {"k": i}, "tags": ["a", "b"]}}
                for i in range(offset, min(offset + length, total))]
        return FakeResponse({"rows": rows, "num_rows_total": total})

    monkeypatch.setattr(ds.requests, "get", fake_get)


def test_fetches_pages_in_parallel_and_caps_rows(monkeypatch, tmp_path):
    install_fake_hub(monkeypatch, total=250)
    info = ds.fetch_huggingface("user/data", str(tmp_path / "o.csv"), max_rows=230)
    df = pd.read_csv(tmp_path / "o.csv")
    assert len(df) == 230 and info["rows"] == 230 and info["rows_available"] == 250
    assert list(df["text"][:3]) == ["doc 0", "doc 1", "doc 2"]          # order preserved across pages
    assert df["text"].tolist() == [f"doc {i}" for i in range(230)]


def test_prefers_train_split(monkeypatch, tmp_path):
    install_fake_hub(monkeypatch, total=5)
    assert ds.fetch_huggingface("imdb", str(tmp_path / "o.csv"), max_rows=5)["split"] == "train"


def test_nested_values_become_json_strings(monkeypatch, tmp_path):
    install_fake_hub(monkeypatch, total=3)
    ds.fetch_huggingface("imdb", str(tmp_path / "o.csv"), max_rows=3)
    df = pd.read_csv(tmp_path / "o.csv")
    assert df["meta"][0] == '{"k": 0}' and df["tags"][0] == '["a", "b"]'


def test_fewer_rows_than_requested(monkeypatch, tmp_path):
    install_fake_hub(monkeypatch, total=7)
    assert ds.fetch_huggingface("imdb", str(tmp_path / "o.csv"), max_rows=1000)["rows"] == 7


def test_never_asks_for_more_than_100_rows_per_request(monkeypatch, tmp_path):
    calls = []
    install_fake_hub(monkeypatch, total=500, calls=calls)
    ds.fetch_huggingface("imdb", str(tmp_path / "o.csv"), max_rows=500)
    assert max(int(p["length"]) for name, p in calls if name == "rows") <= 100


def test_user_supplied_config_and_split_skip_lookup(monkeypatch, tmp_path):
    calls = []
    install_fake_hub(monkeypatch, total=5, calls=calls)
    ds.fetch_huggingface("imdb", str(tmp_path / "o.csv"), config="plain", split="test", max_rows=5)
    assert all(name != "splits" for name, _ in calls)


@pytest.mark.parametrize("bad", ["", "../etc/passwd", "a/b/c", "a b", "x;y", None, ".hidden"])
def test_invalid_hf_ids_rejected_before_any_request(monkeypatch, tmp_path, bad):
    monkeypatch.setattr(ds.requests, "get", lambda *a, **k: pytest.fail("network must not be touched"))
    with pytest.raises(DatasetImportError):
        ds.fetch_huggingface(bad, str(tmp_path / "o.csv"))


@pytest.mark.parametrize("status,needle", [(404, "not found"), (401, "authentication"), (403, "authentication"),
                                           (500, "500")])
def test_http_errors_become_friendly_messages(monkeypatch, tmp_path, status, needle):
    monkeypatch.setattr(ds.requests, "get", lambda *a, **k: FakeResponse(None, status, "boom"))
    with pytest.raises(DatasetImportError, match=f"(?i){needle}"):
        ds.fetch_huggingface("imdb", str(tmp_path / "o.csv"))


def test_network_failure_is_reported(monkeypatch, tmp_path):
    def boom(*a, **k):
        raise ds.requests.ConnectionError("offline")
    monkeypatch.setattr(ds.requests, "get", boom)
    with pytest.raises(DatasetImportError, match="Could not reach"):
        ds.fetch_huggingface("imdb", str(tmp_path / "o.csv"))


def test_dataset_without_splits(monkeypatch, tmp_path):
    install_fake_hub(monkeypatch, splits=[])
    with pytest.raises(DatasetImportError, match="no viewable splits"):
        ds.fetch_huggingface("imdb", str(tmp_path / "o.csv"))


def test_hf_token_is_sent_when_configured(monkeypatch, tmp_path):
    seen = {}

    def fake_get(url, params=None, headers=None, timeout=None):
        seen.update(headers or {})
        return FakeResponse({"splits": [{"config": "c", "split": "train"}], "rows": [{"row": {"a": 1}}], "num_rows_total": 1})

    monkeypatch.setenv("HF_TOKEN", "hf_secret")
    monkeypatch.setattr(ds.requests, "get", fake_get)
    ds.fetch_huggingface("imdb", str(tmp_path / "o.csv"), max_rows=1)
    assert seen["Authorization"] == "Bearer hf_secret"


# ------------------------------------------------------------------ Kaggle
def test_kaggle_requires_credentials(monkeypatch, tmp_path):
    monkeypatch.delenv("KAGGLE_USERNAME", raising=False)
    monkeypatch.delenv("KAGGLE_KEY", raising=False)
    with pytest.raises(DatasetImportError, match="not configured"):
        ds.fetch_kaggle("owner/name", str(tmp_path / "o.csv"))


@pytest.mark.parametrize("bad", ["", "justone", "a/b/c", "../x/y", "a b/c", None])
def test_kaggle_slug_validation(monkeypatch, tmp_path, bad):
    monkeypatch.setenv("KAGGLE_USERNAME", "u")
    monkeypatch.setenv("KAGGLE_KEY", "k")
    with pytest.raises(DatasetImportError, match="owner/dataset"):
        ds.fetch_kaggle(bad, str(tmp_path / "o.csv"))
