"""
Fetch datasets from Hugging Face and Kaggle and save them as a capped CSV.

Hugging Face: uses the public datasets-server "rows" API with plain HTTP, so
no `datasets`/`pyarrow` install is needed (important on small hosts).
Kaggle: uses the official `kaggle` package; needs KAGGLE_USERNAME and
KAGGLE_KEY environment variables.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

HF_ROWS_API = "https://datasets-server.huggingface.co"
PAGE_SIZE = 100  # the rows API allows at most 100 rows per request
MAX_KAGGLE_FILE_BYTES = 200 * 1024 * 1024

_HF_ID = re.compile(r"^[A-Za-z0-9][\w.\-]*(/[A-Za-z0-9][\w.\-]*)?$")
_KAGGLE_SLUG = re.compile(r"^[A-Za-z0-9][\w.\-]*/[A-Za-z0-9][\w.\-]*$")


class DatasetImportError(Exception):
    """A problem the user can fix (bad id, private dataset, missing credentials...)."""


def _hf_headers() -> Dict[str, str]:
    token = os.environ.get("HF_TOKEN")
    return {"Authorization": f"Bearer {token}"} if token else {}


def _hf_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    try:
        resp = requests.get(f"{HF_ROWS_API}{path}", params=params, headers=_hf_headers(), timeout=30)
    except requests.RequestException as exc:
        raise DatasetImportError(f"Could not reach Hugging Face: {exc}") from exc
    if resp.status_code == 404:
        raise DatasetImportError("Dataset not found, or it is private/gated. Check the id (e.g. 'imdb' or 'user/name').")
    if resp.status_code in (401, 403):
        raise DatasetImportError("This dataset needs authentication. Set HF_TOKEN on the server and accept the dataset's terms.")
    if not resp.ok:
        try:
            detail = resp.json().get("error", resp.text[:200])
        except ValueError:
            detail = resp.text[:200]
        raise DatasetImportError(f"Hugging Face returned {resp.status_code}: {detail}")
    return resp.json()


def _flatten(value: Any) -> Any:
    """CSV cells must be scalars: nested lists/dicts become JSON strings."""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def fetch_huggingface(
    dataset_id: str,
    out_csv: str,
    config: Optional[str] = None,
    split: Optional[str] = None,
    max_rows: int = 2000,
) -> Dict[str, Any]:
    if not _HF_ID.match(dataset_id or ""):
        raise DatasetImportError("Invalid Hugging Face dataset id")

    # Pick config/split (prefer 'train') unless the user gave them
    if not config or not split:
        splits = _hf_get("/splits", {"dataset": dataset_id}).get("splits", [])
        if not splits:
            raise DatasetImportError("This dataset has no viewable splits")
        if config:
            splits = [s for s in splits if s["config"] == config] or splits
        chosen = next((s for s in splits if s["split"] == "train"), splits[0])
        config = config or chosen["config"]
        split = split or chosen["split"]

    def page(offset: int) -> List[Dict[str, Any]]:
        length = min(PAGE_SIZE, max_rows - offset)
        data = _hf_get(
            "/rows",
            {"dataset": dataset_id, "config": config, "split": split, "offset": offset, "length": length},
        )
        return [{k: _flatten(v) for k, v in r["row"].items()} for r in data.get("rows", [])]

    first = _hf_get("/rows", {"dataset": dataset_id, "config": config, "split": split, "offset": 0, "length": 1})
    available = int(first.get("num_rows_total", max_rows))
    target = min(max_rows, available)
    offsets = list(range(0, target, PAGE_SIZE))

    with ThreadPoolExecutor(max_workers=5) as pool:
        pages = list(pool.map(page, offsets))
    rows = [r for p in pages for r in p][:target]
    if not rows:
        raise DatasetImportError("No rows were returned for this dataset/split")

    pd.DataFrame(rows).to_csv(out_csv, index=False)
    return {"source": "huggingface", "dataset": dataset_id, "config": config, "split": split,
            "rows": len(rows), "rows_available": available}


def fetch_kaggle(
    dataset_slug: str,
    out_csv: str,
    file_name: Optional[str] = None,
    max_rows: int = 2000,
) -> Dict[str, Any]:
    if not _KAGGLE_SLUG.match(dataset_slug or ""):
        raise DatasetImportError("Kaggle dataset must look like 'owner/dataset-name'")
    if not (os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY")):
        raise DatasetImportError("Kaggle is not configured on the server (KAGGLE_USERNAME / KAGGLE_KEY).")

    try:
        # Importing `kaggle` authenticates immediately, so it must stay lazy.
        from kaggle.api.kaggle_api_extended import KaggleApi
    except ImportError as exc:
        raise DatasetImportError("The 'kaggle' package is not installed on the server") from exc

    api = KaggleApi()
    try:
        api.authenticate()
        listing = api.dataset_list_files(dataset_slug)
    except Exception as exc:
        raise DatasetImportError(f"Kaggle error: {exc}") from exc

    csvs = [f for f in listing.files if str(f.name).lower().endswith(".csv")]
    if file_name:
        csvs = [f for f in csvs if f.name == file_name]
    if not csvs:
        raise DatasetImportError("No CSV file found in this Kaggle dataset" + (f" named '{file_name}'" if file_name else ""))
    target = max(csvs, key=lambda f: getattr(f, "total_bytes", 0) or 0) if not file_name else csvs[0]
    size = getattr(target, "total_bytes", 0) or 0
    if size > MAX_KAGGLE_FILE_BYTES:
        raise DatasetImportError(
            f"'{target.name}' is {size / 1e6:.0f} MB, larger than the {MAX_KAGGLE_FILE_BYTES // 1024 // 1024} MB limit. "
            "Pick a smaller file or download and upload a sample."
        )

    tmp = tempfile.mkdtemp(prefix="kaggle_")
    try:
        try:
            api.dataset_download_file(dataset_slug, target.name, path=tmp, force=True, quiet=True)
        except Exception as exc:
            raise DatasetImportError(f"Kaggle download failed: {exc}") from exc
        downloaded = next(Path(tmp).glob("*"), None)
        if downloaded is None:
            raise DatasetImportError("Kaggle returned no file")
        if downloaded.suffix.lower() == ".zip":
            import zipfile

            with zipfile.ZipFile(downloaded) as zf:
                zf.extractall(tmp)
            downloaded = next((p for p in Path(tmp).rglob("*.csv")), None)
            if downloaded is None:
                raise DatasetImportError("The downloaded archive has no CSV")
        df = pd.read_csv(downloaded, nrows=max_rows, encoding_errors="replace")
        df.to_csv(out_csv, index=False)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    return {"source": "kaggle", "dataset": dataset_slug, "file": target.name, "rows": len(df)}
