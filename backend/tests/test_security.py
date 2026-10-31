"""Tests for the hardening added in the security review: rate limits, password policy,
enumeration, storage quota, zip-slip, and CSV/JSONL formula injection."""

import importlib
import os
import uuid
import zipfile
from pathlib import Path

import pytest

from conftest import TMP, sensor_csv, upload


# ============================================================ password policy / enumeration
class TestRegistration:
    def test_short_password_is_rejected(self, client):
        name = f"u_{uuid.uuid4().hex[:8]}"
        r = client.post("/api/register", json={"username": name, "email": f"{name}@x.com", "password": "short1"})
        assert r.status_code == 400 and "8 characters" in r.get_json()["message"]

    def test_duplicate_username_and_email_give_the_same_message(self, client, user):
        by_username = client.post("/api/register", json={"username": user["username"], "email": "fresh@x.com", "password": "longenough1"})
        by_email = client.post("/api/register", json={"username": "fresh_" + uuid.uuid4().hex[:6],
                                                       "email": f"{user['username']}@x.com", "password": "longenough1"})
        assert by_username.status_code == by_email.status_code == 400
        assert by_username.get_json()["message"] == by_email.get_json()["message"], \
            "a different message per field lets someone enumerate registered usernames/emails"


# ============================================================ CSV / JSONL formula injection
class TestFormulaInjection:
    def test_classic_pipeline_output_defangs_formula_like_cells(self, client, user):
        from conftest import BACKEND
        # 'status' is a free-text column the pipeline passes through unchanged
        rows = ["timestamp,machine_id,temperature,pressure,vibration,rpm,status"]
        for i in range(40):
            rows.append(f"2026-01-01 00:00:{i:02d},M1,50.{i},10.{i},1.{i},1500,=cmd|'/C calc'!A1" if i == 0
                        else f"2026-01-01 00:00:{i:02d},M1,50.{i},10.{i},1.{i},1500,normal")
        fid = upload(client, user["headers"], "\n".join(rows) + "\n", "s.csv").get_json()["file"]["id"]
        job = client.post("/api/process", headers=user["headers"], json={"file_id": fid, "config": {}}).get_json()["job"]
        d = client.get(f"/api/jobs/{job['id']}/download", headers=user["headers"]).data.decode()
        row = d.split("\n")[1]
        assert ",=cmd" not in row, f"a formula-leading cell was written unescaped: {row!r}"
        assert ",'=cmd" in row, f"expected the cell defanged with a leading quote: {row!r}"

    def test_rag_chunks_defang_formula_like_text(self, client, user):
        import json
        csv = "id,text\n1,\"=1+1 plus some trailing words to make a real chunk of real length here\"\n"
        fid = upload(client, user["headers"], csv, "t.csv").get_json()["file"]["id"]
        r = client.post("/api/rag/process", headers=user["headers"],
                        json={"file_id": fid, "config": {"text_columns": ["text"], "id_column": "id", "embed": False}})
        assert r.status_code == 202
        from conftest import wait_for_job
        job = wait_for_job(client, user["headers"])
        rows = [json.loads(l) for l in client.get(f"/api/jobs/{job['id']}/download", headers=user["headers"]).data.decode().splitlines()]
        assert not rows[0]["text"].startswith("=1+1"), f"chunk text starts with an unescaped formula: {rows[0]['text']!r}"
        assert rows[0]["text"].startswith("'=1+1")


# ============================================================ per-user storage quota
class TestStorageQuota:
    def test_uploads_beyond_the_quota_are_refused(self, A, client, user, monkeypatch):
        monkeypatch.setattr(A, "MAX_USER_STORAGE_BYTES", 100)
        ok = upload(client, user["headers"], "a" * 50, "a.csv")
        assert ok.status_code == 201
        blocked = upload(client, user["headers"], "a" * 200, "b.csv")
        assert blocked.status_code == 400 and "storage limit" in blocked.get_json()["message"].lower()

    def test_quota_is_per_user(self, A, client, user, other_user, monkeypatch):
        monkeypatch.setattr(A, "MAX_USER_STORAGE_BYTES", 100)
        upload(client, user["headers"], "a" * 90, "a.csv")
        # a different user still has their own quota, unaffected by the first user's usage
        r = upload(client, other_user["headers"], "a" * 90, "b.csv")
        assert r.status_code == 201


# ============================================================ zip-slip
class TestZipSlip:
    def test_malicious_zip_entries_are_refused(self, tmp_path):
        from dataset_sources import _safe_extract, DatasetImportError

        evil = tmp_path / "evil.zip"
        with zipfile.ZipFile(evil, "w") as zf:
            zf.writestr("../../escaped.txt", "pwned")
        dest = tmp_path / "dest"
        dest.mkdir()
        with zipfile.ZipFile(evil) as zf, pytest.raises(DatasetImportError):
            _safe_extract(zf, str(dest))
        assert not (tmp_path / "escaped.txt").exists()

    def test_well_behaved_zip_still_extracts(self, tmp_path):
        from dataset_sources import _safe_extract

        good = tmp_path / "good.zip"
        with zipfile.ZipFile(good, "w") as zf:
            zf.writestr("data.csv", "a,b\n1,2\n")
        dest = tmp_path / "dest"
        dest.mkdir()
        with zipfile.ZipFile(good) as zf:
            _safe_extract(zf, str(dest))
        assert (dest / "data.csv").read_text() == "a,b\n1,2\n"


# ============================================================ rate limiting (own process, limiter enabled)
class TestRateLimiting:
    @pytest.fixture()
    def limited_client(self, monkeypatch):
        """A second import of app.py with rate limiting turned on, isolated from the shared `A`/`client`
        fixtures (which run with RATELIMIT_ENABLED=false so the rest of the suite isn't throttled)."""
        work = TMP / f"ratelimit_{uuid.uuid4().hex[:8]}"
        work.mkdir()
        monkeypatch.chdir(work)
        monkeypatch.setenv("DATABASE_URL", f"sqlite:///{work}/db.sqlite")
        monkeypatch.setenv("RATELIMIT_ENABLED", "true")
        monkeypatch.setenv("SCRATCH_EMBEDDER_DIR", str(work / "no-model"))
        import app as shared_app_module
        import sys
        sys.modules.pop("app", None)
        app_module = importlib.import_module("app")
        yield app_module.app.test_client()
        sys.modules["app"] = shared_app_module  # restore the shared module for later tests

    def test_login_is_throttled_after_repeated_attempts(self, limited_client):
        statuses = [limited_client.post("/api/login", json={"username": "nobody", "password": "wrong1234"}).status_code
                   for _ in range(15)]
        assert 429 in statuses, f"10/minute limit on /api/login never kicked in: {statuses}"

    def test_register_is_throttled_after_repeated_attempts(self, limited_client):
        statuses = []
        for i in range(15):
            r = limited_client.post("/api/register", json={"username": f"flood_{i}", "email": f"flood_{i}@x.com", "password": "longenough1"})
            statuses.append(r.status_code)
        assert 429 in statuses, f"10/hour limit on /api/register never kicked in: {statuses}"
