"""API tests: every endpoint through Flask's test client, using a real (temporary) database."""

import datetime as dt
import io
import json
import os
import subprocess
import sys
import textwrap
import uuid
from pathlib import Path

import jwt
import pytest

from conftest import FakeEmbedder, sensor_csv, text_csv, upload, wait_for_job, TMP, BACKEND


def new_file(client, user, content=None, name="data.csv"):
    r = upload(client, user["headers"], sensor_csv() if content is None else content, name)
    assert r.status_code == 201, r.get_json()
    return r.get_json()["file"]["id"]


def run_sync(client, user, file_id, config=None):
    return client.post("/api/process", headers=user["headers"], json={"file_id": file_id, "config": config or {}})


def run_rag(client, user, file_id, **cfg):
    cfg = {"text_columns": ["text"], "id_column": "id", "embed": False, "chunk_tokens": 32, "chunk_overlap": 4, **cfg}
    return client.post("/api/rag/process", headers=user["headers"], json={"file_id": file_id, "config": cfg})


# ============================================================ health / config / CORS
class TestBasics:
    def test_health(self, client):
        r = client.get("/api/health")
        assert r.status_code == 200 and r.get_json()["status"] == "healthy"

    def test_default_config(self, client):
        cfg = client.get("/api/config/defaults").get_json()
        assert cfg["missing_value_threshold"] == 0.3 and cfg["validation_enabled"] is True

    def test_cors_allows_the_configured_frontend(self, client):
        r = client.options("/api/login", headers={"Origin": "https://app.example.com",
                                                  "Access-Control-Request-Method": "POST",
                                                  "Access-Control-Request-Headers": "content-type,authorization"})
        assert r.headers.get("Access-Control-Allow-Origin") == "https://app.example.com"

    def test_cors_allows_local_dev_servers(self, client):
        for origin in ("http://localhost:3000", "http://localhost:5173"):
            r = client.options("/api/login", headers={"Origin": origin, "Access-Control-Request-Method": "POST"})
            assert r.headers.get("Access-Control-Allow-Origin") == origin

    def test_cors_blocks_other_sites(self, client):
        r = client.options("/api/login", headers={"Origin": "https://evil.example.net", "Access-Control-Request-Method": "POST"})
        assert "Access-Control-Allow-Origin" not in r.headers

    def test_unknown_api_route(self, client):
        assert client.get("/api/nope").status_code == 404

    def test_errors_are_returned_as_json(self, client, user):
        # The frontend always calls response.json(); an HTML error page turns every failure into "Network error"
        r = client.get("/api/does-not-exist")
        assert r.is_json, f"404 returned {r.content_type}"

    def test_oversized_upload_is_rejected_with_json(self, A, client, user, monkeypatch):
        monkeypatch.setitem(A.app.config, "MAX_CONTENT_LENGTH", 2000)
        r = upload(client, user["headers"], "x" * 10_000, "big.csv")
        assert r.status_code == 413
        assert r.is_json, "413 came back as an HTML page, so the frontend shows 'Network error during upload'"


# ============================================================ auth
class TestAuth:
    def test_register_and_login(self, client):
        name = f"u_{uuid.uuid4().hex[:8]}"
        r = client.post("/api/register", json={"username": name, "email": f"{name}@x.com", "password": "secret1"})
        assert r.status_code == 201 and "password" not in json.dumps(r.get_json())
        assert "hash" not in json.dumps(r.get_json())
        ok = client.post("/api/login", json={"username": name, "password": "secret1"})
        assert ok.status_code == 200 and ok.get_json()["token"]

    @pytest.mark.parametrize("body", [{}, {"username": "a"}, {"username": "a", "email": "a@x.com"},
                                      {"username": "", "email": "a@x.com", "password": "p"},
                                      {"username": "a", "email": "a@x.com", "password": ""}])
    def test_register_requires_all_fields(self, client, body):
        assert client.post("/api/register", json=body).status_code == 400

    def test_duplicate_username_and_email(self, client, user):
        r1 = client.post("/api/register", json={"username": user["username"], "email": "fresh@x.com", "password": "p"})
        r2 = client.post("/api/register", json={"username": "fresh_" + uuid.uuid4().hex[:6],
                                                "email": f"{user['username']}@x.com", "password": "p"})
        assert r1.status_code == 400 and r2.status_code == 400

    def test_email_duplicates_ignore_case(self, client, user):
        r = client.post("/api/register", json={"username": "x_" + uuid.uuid4().hex[:6],
                                               "email": f"{user['username']}@X.COM".upper(), "password": "p"})
        assert r.status_code == 400, "the same email address can be registered twice by changing its case"

    def test_invalid_email_is_rejected(self, client):
        r = client.post("/api/register", json={"username": "m_" + uuid.uuid4().hex[:6], "email": "not-an-email", "password": "p"})
        assert r.status_code == 400

    def test_wrong_password_and_unknown_user_look_the_same(self, client, user):
        a = client.post("/api/login", json={"username": user["username"], "password": "wrong"})
        b = client.post("/api/login", json={"username": "ghost_" + uuid.uuid4().hex, "password": "wrong"})
        assert a.status_code == b.status_code == 401 and a.get_json() == b.get_json()

    def test_login_requires_fields(self, client):
        assert client.post("/api/login", json={"username": "x"}).status_code == 400

    def test_malformed_json_body_is_a_client_error_not_a_crash(self, client):
        r = client.post("/api/login", data="{not json", content_type="application/json")
        assert 400 <= r.status_code < 500

    def test_protected_routes_need_a_token(self, client):
        for method, path in [("get", "/api/files"), ("get", "/api/jobs"), ("post", "/api/process"),
                             ("post", "/api/rag/process"), ("get", "/api/embedders"), ("post", "/api/datasets/import"),
                             ("get", "/api/files/1/columns"), ("get", "/api/jobs/1/download"), ("delete", "/api/files/1")]:
            assert getattr(client, method)(path).status_code == 401, path

    def test_bad_and_foreign_tokens(self, A, client):
        for token in ["garbage", "Bearer garbage", jwt.encode({"user_id": 1}, "other-secret", algorithm="HS256")]:
            assert client.get("/api/files", headers={"Authorization": token}).status_code == 401

    def test_expired_token(self, A, client, user):
        token = jwt.encode({"user_id": user["id"], "exp": dt.datetime.utcnow() - dt.timedelta(minutes=1)},
                           A.app.config["SECRET_KEY"], algorithm="HS256")
        r = client.get("/api/files", headers={"Authorization": f"Bearer {token}"})
        assert r.status_code == 401 and "expired" in r.get_json()["message"].lower()

    def test_unsigned_token_is_refused(self, A, client, user):
        token = jwt.encode({"user_id": user["id"]}, key=None, algorithm="none")
        assert client.get("/api/files", headers={"Authorization": f"Bearer {token}"}).status_code == 401

    def test_token_for_deleted_user(self, A, client):
        token = jwt.encode({"user_id": 10 ** 9}, A.app.config["SECRET_KEY"], algorithm="HS256")
        assert client.get("/api/files", headers={"Authorization": f"Bearer {token}"}).status_code == 401

    def test_token_without_user_id_claim(self, A, client):
        token = jwt.encode({"foo": 1}, A.app.config["SECRET_KEY"], algorithm="HS256")
        r = client.get("/api/files", headers={"Authorization": f"Bearer {token}"})
        assert r.status_code == 401, f"token with no user_id gave {r.status_code}"

    def test_password_is_stored_hashed(self, A, user):
        with A.app.app_context():
            row = A.User.query.get(user["id"])
            assert row.password_hash != user["password"] and row.check_password(user["password"])


# ============================================================ files
class TestFiles:
    def test_upload_list_delete(self, A, client, user):
        fid = new_file(client, user)
        files = client.get("/api/files", headers=user["headers"]).get_json()["files"]
        assert [f["id"] for f in files] == [fid] and files[0]["status"] == "uploaded" and files[0]["file_size"] > 0
        assert client.delete(f"/api/files/{fid}", headers=user["headers"]).status_code == 200
        assert client.get("/api/files", headers=user["headers"]).get_json()["files"] == []

    def test_delete_removes_the_file_from_disk(self, A, client, user):
        fid = new_file(client, user)
        with A.app.app_context():
            path = A.db.session.get(A.UploadedFile, fid).file_path
        assert os.path.exists(path)
        client.delete(f"/api/files/{fid}", headers=user["headers"])
        assert not os.path.exists(path)

    @pytest.mark.parametrize("name", ["data.CSV", "notes.txt", "Data Set (1).csv"])
    def test_accepted_names(self, client, user, name):
        assert upload(client, user["headers"], "a,b\n1,2\n", name).status_code == 201

    @pytest.mark.parametrize("name", ["virus.exe", "page.html", "data.csv.exe", "csv"])
    def test_rejected_extensions(self, client, user, name):
        assert upload(client, user["headers"], "a,b\n", name).status_code == 400

    def test_no_file_and_empty_filename(self, client, user):
        assert client.post("/api/files/upload", headers=user["headers"]).status_code == 400
        assert upload(client, user["headers"], "x", "").status_code == 400

    def test_path_traversal_in_filename(self, A, client, user):
        r = upload(client, user["headers"], "a,b\n1,2\n", "../../../etc/evil.csv")
        assert r.status_code == 201
        with A.app.app_context():
            path = A.db.session.get(A.UploadedFile, r.get_json()["file"]["id"]).file_path
        assert os.path.dirname(path) == A.app.config["UPLOAD_FOLDER"]

    def test_non_ascii_filename_keeps_a_usable_name(self, client, user):
        r = upload(client, user["headers"], "a,b\n1,2\n", "डेटा.csv")
        assert r.status_code == 201
        assert r.get_json()["file"]["filename"].endswith(".csv"), r.get_json()["file"]["filename"]

    def test_same_name_uploaded_twice_in_a_row_gets_separate_files(self, A, client, user):
        a = upload(client, user["headers"], "a,b\n1,1\n", "same.csv").get_json()["file"]["id"]
        b = upload(client, user["headers"], "a,b\n2,2\n", "same.csv").get_json()["file"]["id"]
        with A.app.app_context():
            pa, pb = (A.db.session.get(A.UploadedFile, i).file_path for i in (a, b))
        assert pa != pb, "second upload overwrote the first (same second, same name)"

    def test_users_only_see_their_own_files(self, client, user, other_user):
        fid = new_file(client, user)
        assert client.get("/api/files", headers=other_user["headers"]).get_json()["files"] == []
        assert client.delete(f"/api/files/{fid}", headers=other_user["headers"]).status_code == 404
        assert client.get(f"/api/files/{fid}/columns", headers=other_user["headers"]).status_code == 404

    def test_delete_unknown_file(self, client, user):
        assert client.delete("/api/files/999999", headers=user["headers"]).status_code == 404

    def test_a_processed_file_can_be_deleted(self, client, user):
        # The UI offers a delete button on processed files, whose job rows point at them
        fid = new_file(client, user)
        assert run_sync(client, user, fid).status_code == 200
        r = client.delete(f"/api/files/{fid}", headers=user["headers"])
        assert r.status_code == 200, f"delete failed with {r.status_code}: {r.get_data(as_text=True)[:200]}"

    def test_deleting_a_file_removes_its_results_too(self, A, client, user):
        fid = new_file(client, user)
        run_sync(client, user, fid)
        with A.app.app_context():
            out = A.ProcessedJob.query.filter_by(file_id=fid).first().output_path
        client.delete(f"/api/files/{fid}", headers=user["headers"])
        assert not os.path.exists(out), "processed output stays on disk after its source file is deleted"

    def test_columns_endpoint(self, client, user):
        fid = new_file(client, user, text_csv(10), "t.csv")
        info = client.get(f"/api/files/{fid}/columns", headers=user["headers"]).get_json()
        assert info["suggested_text_columns"] == ["text"] and info["suggested_id_column"] == "id"

    def test_columns_of_a_binary_file(self, client, user):
        fid = new_file(client, user, bytes(range(256)) * 20, "bin.csv")
        r = client.get(f"/api/files/{fid}/columns", headers=user["headers"])
        assert r.status_code in (200, 400) and r.is_json


# ============================================================ classic pipeline
class TestSyncProcess:
    def test_happy_path(self, client, user):
        fid = new_file(client, user)
        r = run_sync(client, user, fid)
        body = r.get_json()
        assert r.status_code == 200 and body["job"]["status"] == "completed"
        m = body["job"]["metrics"]
        assert m["total_records"] == 200 and m["valid_records"] + m["invalid_records"] == 200
        assert client.get("/api/files", headers=user["headers"]).get_json()["files"][0]["status"] == "processed"

    def test_download_is_a_csv_with_engineered_columns(self, client, user):
        fid = new_file(client, user)
        job_id = run_sync(client, user, fid).get_json()["job"]["id"]
        d = client.get(f"/api/jobs/{job_id}/download", headers=user["headers"])
        header = d.data.decode().splitlines()[0]
        assert d.status_code == 200 and "temperature_normalized" in header and "Content-Disposition" in d.headers

    def test_user_config_beats_suggestions(self, client, user):
        fid = new_file(client, user)
        job = run_sync(client, user, fid, {"normalization_enabled": False, "outlier_std_threshold": 2.5}).get_json()["job"]
        assert job["config"]["normalization_enabled"] is False and job["config"]["outlier_std_threshold"] == 2.5

    def test_requires_file_id_and_ownership(self, client, user, other_user):
        assert client.post("/api/process", headers=user["headers"], json={}).status_code == 400
        assert run_sync(client, user, 999999).status_code == 404
        fid = new_file(client, user)
        assert run_sync(client, other_user, fid).status_code == 404

    @pytest.mark.parametrize("bad_config", [[1, 2], "oops", 5])
    def test_non_object_config_is_a_400(self, client, user, bad_config):
        fid = new_file(client, user)
        assert run_sync(client, user, fid, bad_config).status_code == 400

    def test_nonsense_threshold_is_a_400(self, client, user):
        fid = new_file(client, user)
        assert run_sync(client, user, fid, {"missing_value_threshold": "abc"}).status_code == 400

    def test_garbage_file_fails_cleanly(self, client, user):
        fid = new_file(client, user, bytes(range(256)) * 50, "junk.csv")
        r = run_sync(client, user, fid)
        assert r.status_code == 500 and r.is_json
        job = client.get("/api/jobs", headers=user["headers"]).get_json()["jobs"][0]
        assert job["status"] == "failed" and job["error_message"]
        assert client.get("/api/files", headers=user["headers"]).get_json()["files"][0]["status"] == "error"

    def test_header_only_csv_is_reported_not_faked(self, client, user):
        fid = new_file(client, user, "temperature,pressure\n", "empty.csv")
        r = run_sync(client, user, fid)
        assert r.status_code != 200, "an empty upload 'succeeded' (the pipeline invented sample data for it)"

    def test_text_only_csv_works(self, client, user):
        fid = new_file(client, user, text_csv(30), "t.csv")
        assert run_sync(client, user, fid).status_code == 200

    def test_processing_twice_keeps_both_results_intact(self, client, user):
        fid = new_file(client, user)
        j1 = run_sync(client, user, fid, {"outlier_detection_enabled": False, "normalization_enabled": False}).get_json()["job"]["id"]
        before = client.get(f"/api/jobs/{j1}/download", headers=user["headers"]).data
        run_sync(client, user, fid, {"outlier_detection_enabled": True, "normalization_enabled": True})
        after = client.get(f"/api/jobs/{j1}/download", headers=user["headers"]).data
        assert before == after, "re-processing a file overwrote the output of the earlier job"

    def test_jobs_are_listed_newest_first_with_filename(self, client, user):
        fid = new_file(client, user, name="mine.csv")
        run_sync(client, user, fid)
        run_sync(client, user, fid)
        jobs = client.get("/api/jobs", headers=user["headers"]).get_json()["jobs"]
        assert len(jobs) == 2 and jobs[0]["id"] > jobs[1]["id"] and jobs[0]["filename"] == "mine.csv"

    def test_jobs_and_downloads_are_private(self, client, user, other_user):
        fid = new_file(client, user)
        job_id = run_sync(client, user, fid).get_json()["job"]["id"]
        assert client.get("/api/jobs", headers=other_user["headers"]).get_json()["jobs"] == []
        assert client.get(f"/api/jobs/{job_id}/download", headers=other_user["headers"]).status_code == 404

    def test_download_of_failed_or_missing_job(self, client, user):
        fid = new_file(client, user, bytes(range(256)) * 50, "junk.csv")
        run_sync(client, user, fid)
        job_id = client.get("/api/jobs", headers=user["headers"]).get_json()["jobs"][0]["id"]
        assert client.get(f"/api/jobs/{job_id}/download", headers=user["headers"]).status_code == 404
        assert client.get("/api/jobs/999999/download", headers=user["headers"]).status_code == 404


# ============================================================ RAG jobs
class TestRag:
    def test_chunks_only_job(self, A, client, user):
        fid = new_file(client, user, text_csv(30), "t.csv")
        r = run_rag(client, user, fid)
        assert r.status_code == 202 and r.get_json()["job"]["status"] == "pending"
        job = wait_for_job(client, user["headers"])
        assert job["status"] == "completed", job
        assert job["metrics"]["documents"] == 30 and job["metrics"]["chunks"] >= 30 and job["metrics"]["embedding_dim"] is None
        d = client.get(f"/api/jobs/{job['id']}/download", headers=user["headers"])
        rows = [json.loads(l) for l in d.data.decode().splitlines()]
        assert d.status_code == 200 and len(rows) == job["metrics"]["chunks"] and "embedding" not in rows[0]
        assert "jsonl" in d.headers["Content-Disposition"]
        assert client.get("/api/files", headers=user["headers"]).get_json()["files"][0]["status"] == "processed"

    def test_job_with_embeddings(self, A, client, user, monkeypatch):
        import rag_prep
        monkeypatch.setenv("HF_TOKEN", "fake")
        monkeypatch.setattr(rag_prep, "get_embedder", lambda cfg: FakeEmbedder(dim=8))
        fid = new_file(client, user, text_csv(12), "t.csv")
        assert run_rag(client, user, fid, embed=True, embedder="hf_api").status_code == 202
        job = wait_for_job(client, user["headers"])
        assert job["status"] == "completed" and job["metrics"]["embedding_dim"] == 8
        rows = [json.loads(l) for l in client.get(f"/api/jobs/{job['id']}/download", headers=user["headers"]).data.decode().splitlines()]
        assert len(rows[0]["embedding"]) == 8

    def test_embeddings_without_credentials_or_model_are_refused_up_front(self, client, user):
        fid = new_file(client, user, text_csv(5), "t.csv")
        r1 = run_rag(client, user, fid, embed=True, embedder="hf_api")
        r2 = run_rag(client, user, fid, embed=True, embedder="scratch")
        assert r1.status_code == 400 and "HF_TOKEN" in r1.get_json()["message"]
        assert r2.status_code == 400 and "embedder" in r2.get_json()["message"]
        # ...and a refused request must not leave a stuck "processing" file behind
        assert client.get("/api/files", headers=user["headers"]).get_json()["files"][0]["status"] == "uploaded"

    def test_embedder_failure_marks_job_and_file_failed(self, A, client, user, monkeypatch):
        import rag_prep

        class Boom(FakeEmbedder):
            def embed(self, texts):
                raise RuntimeError("embedding service is down")

        monkeypatch.setenv("HF_TOKEN", "fake")
        monkeypatch.setattr(rag_prep, "get_embedder", lambda cfg: Boom())
        fid = new_file(client, user, text_csv(6), "t.csv")
        run_rag(client, user, fid, embed=True, embedder="hf_api")
        job = wait_for_job(client, user["headers"])
        assert job["status"] == "failed" and "down" in job["error_message"]
        assert client.get("/api/files", headers=user["headers"]).get_json()["files"][0]["status"] == "error"
        assert client.get(f"/api/jobs/{job['id']}/download", headers=user["headers"]).status_code == 404

    def test_failed_job_leaves_no_partial_output_behind(self, A, client, user, monkeypatch):
        import rag_prep

        class Boom(FakeEmbedder):
            def embed(self, texts):
                raise RuntimeError("down")

        monkeypatch.setenv("HF_TOKEN", "fake")
        monkeypatch.setattr(rag_prep, "get_embedder", lambda cfg: Boom())
        fid = new_file(client, user, text_csv(6), "t.csv")
        with A.app.app_context():
            fname = A.db.session.get(A.UploadedFile, fid).filename.rsplit(".", 1)[0]
        run_rag(client, user, fid, embed=True, embedder="hf_api")
        wait_for_job(client, user["headers"])
        leftovers = list(Path(A.app.config["PROCESSED_FOLDER"]).glob(f"rag_{fname}*"))
        assert leftovers == [], f"half-written output left on disk: {leftovers}"

    def test_a_failed_job_can_be_retried(self, client, user, monkeypatch):
        fid = new_file(client, user, "text\n" + "\n".join(["x"] * 3), "tiny.csv")
        run_rag(client, user, fid, text_columns=["text"], id_column=None)
        assert wait_for_job(client, user["headers"])["status"] == "failed"
        again = run_rag(client, user, fid, text_columns=["text"], id_column=None, chunk_tokens=32)
        assert again.status_code == 202          # not blocked by the earlier failure

    def test_only_one_job_at_a_time_per_user(self, A, client, user):
        fid = new_file(client, user, text_csv(5), "t.csv")
        with A.app.app_context():
            A.db.session.add(A.ProcessedJob(user_id=user["id"], file_id=fid, status="running"))
            A.db.session.commit()
        assert run_rag(client, user, fid).status_code == 409

    def test_validation_and_ownership(self, client, user, other_user):
        assert client.post("/api/rag/process", headers=user["headers"], json={}).status_code == 400
        assert client.post("/api/rag/process", headers=user["headers"], json={"file_id": 999999}).status_code == 404
        fid = new_file(client, user, text_csv(5), "t.csv")
        assert run_rag(client, other_user, fid).status_code == 404

    @pytest.mark.parametrize("bad_config", [["a"], "text", 5])
    def test_non_object_config_does_not_crash_the_server(self, client, user, bad_config):
        fid = new_file(client, user, text_csv(5), "t.csv")
        r = client.post("/api/rag/process", headers=user["headers"], json={"file_id": fid, "config": bad_config})
        assert r.status_code in (200, 202, 400), f"got {r.status_code}"

    def test_dataset_without_text_fails_with_a_helpful_message(self, client, user):
        fid = new_file(client, user, "a,b\n1,2\n3,4\n", "nums.csv")
        run_rag(client, user, fid, text_columns=[], id_column=None)
        job = wait_for_job(client, user["headers"])
        assert job["status"] == "failed" and "text column" in job["error_message"]

    def test_rerunning_a_file_keeps_the_first_result(self, client, user):
        fid = new_file(client, user, text_csv(20), "t.csv")
        run_rag(client, user, fid, chunk_tokens=32)
        j1 = wait_for_job(client, user["headers"])
        run_rag(client, user, fid, chunk_tokens=64)
        j2 = wait_for_job(client, user["headers"])
        assert j1["metrics"]["chunks"] != j2["metrics"]["chunks"]
        rows = client.get(f"/api/jobs/{j1['id']}/download", headers=user["headers"]).data.decode().splitlines()
        assert len(rows) == j1["metrics"]["chunks"], "the earlier job's download now contains the later run's data"

    def test_max_rows_is_capped_by_the_server(self, A, client, user, monkeypatch):
        monkeypatch.setattr(A, "MAX_RAG_ROWS", 7)
        fid = new_file(client, user, text_csv(30), "t.csv")
        run_rag(client, user, fid, max_rows=10 ** 6)
        assert wait_for_job(client, user["headers"])["metrics"]["rows_read"] == 7

    def test_embedders_endpoint(self, client, user, monkeypatch):
        info = client.get("/api/embedders", headers=user["headers"]).get_json()
        assert info["scratch"]["available"] is False and info["hf_api"]["available"] is False
        monkeypatch.setenv("HF_TOKEN", "x")
        assert client.get("/api/embedders", headers=user["headers"]).get_json()["hf_api"]["available"] is True


# ============================================================ dataset import
class TestImport:
    def post(self, client, user, **body):
        return client.post("/api/datasets/import", headers=user["headers"], json=body)

    def test_validation(self, client, user):
        assert self.post(client, user).status_code == 400
        assert self.post(client, user, source="ftp", dataset="x").status_code == 400
        assert self.post(client, user, source="huggingface").status_code == 400

    def test_huggingface_import_creates_an_uploaded_file(self, A, client, user, monkeypatch):
        seen = {}

        def fake(dataset_id, out, config, split, max_rows):
            seen.update(dataset=dataset_id, rows=max_rows)
            Path(out).write_text("text\nhello world\n")
            return {"source": "huggingface", "dataset": dataset_id, "rows": 1}

        monkeypatch.setattr(A, "fetch_huggingface", fake)
        r = self.post(client, user, source="huggingface", dataset="user/some-data", max_rows=50)
        assert r.status_code == 201 and r.get_json()["file"]["filename"] == "user_some-data.csv"
        assert seen == {"dataset": "user/some-data", "rows": 50}
        assert len(client.get("/api/files", headers=user["headers"]).get_json()["files"]) == 1

    @pytest.mark.parametrize("asked,expected", [(10 ** 9, 20000), (0, 1), (-5, 1), ("many", 2000), (None, 2000)])
    def test_row_limit_is_clamped(self, A, client, user, monkeypatch, asked, expected):
        seen = {}

        def fake(dataset_id, out, config, split, max_rows):
            seen["rows"] = max_rows
            Path(out).write_text("t\nx\n")
            return {"dataset": dataset_id, "rows": 1}

        monkeypatch.setattr(A, "fetch_huggingface", fake)
        self.post(client, user, source="huggingface", dataset="imdb", max_rows=asked)
        assert seen["rows"] == expected

    def test_import_errors_are_shown_and_leave_nothing_behind(self, A, client, user, monkeypatch):
        def fake(*a, **k):
            raise A.DatasetImportError("Dataset not found")

        monkeypatch.setattr(A, "fetch_huggingface", fake)
        r = self.post(client, user, source="huggingface", dataset="nope")
        assert r.status_code == 400 and r.get_json()["message"] == "Dataset not found"
        assert client.get("/api/files", headers=user["headers"]).get_json()["files"] == []

    def test_unexpected_errors_are_json_500s(self, A, client, user, monkeypatch):
        monkeypatch.setattr(A, "fetch_huggingface", lambda *a, **k: 1 / 0)
        r = self.post(client, user, source="huggingface", dataset="imdb")
        assert r.status_code == 500 and r.is_json

    def test_kaggle_without_credentials(self, client, user, monkeypatch):
        monkeypatch.delenv("KAGGLE_USERNAME", raising=False)
        monkeypatch.delenv("KAGGLE_KEY", raising=False)
        r = self.post(client, user, source="kaggle", dataset="owner/name")
        assert r.status_code == 400 and "not configured" in r.get_json()["message"]

    def test_pasted_links_are_understood(self, A, client, user, monkeypatch):
        seen = {}

        def fake(dataset_id, out, config, split, max_rows):
            seen["id"] = dataset_id
            Path(out).write_text("t\nx\n")
            return {"dataset": dataset_id, "rows": 1}

        monkeypatch.setattr(A, "fetch_huggingface", fake)
        r = self.post(client, user, source="huggingface", dataset="https://huggingface.co/datasets/stanfordnlp/imdb")
        assert r.status_code == 201 and seen["id"] == "stanfordnlp/imdb"


# ============================================================ process-level behaviour
class TestStartup:
    def run_py(self, code, env):
        full = {**os.environ, "PYTHONPATH": str(BACKEND), **env}
        return subprocess.run([sys.executable, "-c", textwrap.dedent(code)], cwd=env.get("_CWD", TMP),
                              env={k: v for k, v in full.items() if k != "_CWD"},
                              capture_output=True, text=True, timeout=90)

    @pytest.mark.parametrize("url", ["postgres://u:p@h:5432/db", "postgresql://u:p@h:5432/db",
                                     "postgresql+psycopg://u:p@h:5432/db"])
    def test_postgres_urls_use_the_installed_psycopg2_driver(self, url, tmp_path):
        r = self.run_py("""
            import sqlalchemy
            from unittest import mock
            with mock.patch.object(sqlalchemy.engine.Engine, "connect", side_effect=RuntimeError("offline"), create=True):
                pass
            from sqlalchemy.engine import make_url
            import re, os
            u = re.sub(r'^postgres(ql)?(\\+\\w+)?://', 'postgresql+psycopg2://', os.environ['DATABASE_URL'])
            print(make_url(u).get_dialect().driver)
        """, {"DATABASE_URL": url, "_CWD": str(tmp_path)})
        assert r.stdout.strip().endswith("psycopg2"), r.stderr[-400:]

    def test_the_real_app_module_normalises_postgres_urls(self, tmp_path):
        # Import the actual app.py with a postgres URL: the engine must be created (psycopg2 installed)
        # and fail only because the host does not exist, not because a driver module is missing.
        r = self.run_py("""
            try:
                import app
            except Exception as e:
                print(type(e).__module__, type(e).__name__, str(e)[:120].replace("\\n", " "))
        """, {"DATABASE_URL": "postgresql://u:p@127.0.0.1:1/db", "SECRET_KEY": "x", "_CWD": str(tmp_path)})
        assert "ModuleNotFoundError" not in r.stdout + r.stderr, r.stdout + r.stderr[-300:]
        assert "OperationalError" in r.stdout

    def test_missing_secret_key_does_not_crash_and_is_random(self, tmp_path):
        code = "import app; print(app.app.config['SECRET_KEY'])"
        env = {"DATABASE_URL": f"sqlite:///{tmp_path}/a.db", "_CWD": str(tmp_path)}
        a = self.run_py(code, {**env, "SECRET_KEY": ""})
        b = self.run_py(code, {**env, "SECRET_KEY": ""})
        ka, kb = a.stdout.strip().splitlines()[-1], b.stdout.strip().splitlines()[-1]
        assert len(ka) >= 32 and ka != kb and "66b6e7a5" not in ka

    def test_no_hardcoded_secrets_in_the_source(self):
        for f in BACKEND.glob("*.py"):
            text = f.read_text()
            assert "66b6e7a5f5a1d1657699" not in text, f"old leaked key still in {f.name}"

    def test_jobs_left_running_by_a_restart_are_marked_failed(self, tmp_path):
        env = {"DATABASE_URL": f"sqlite:///{tmp_path}/r.db", "SECRET_KEY": "x", "_CWD": str(tmp_path)}
        first = self.run_py("""
            import app
            with app.app.app_context():
                u = app.User(username="a", email="a@x.com"); u.set_password("p")
                app.db.session.add(u); app.db.session.commit()
                f = app.UploadedFile(user_id=u.id, filename="f.csv", original_filename="f.csv", file_path="/x", status="processing")
                app.db.session.add(f); app.db.session.commit()
                app.db.session.add(app.ProcessedJob(user_id=u.id, file_id=f.id, status="running")); app.db.session.commit()
            print("seeded")
        """, env)
        assert "seeded" in first.stdout, first.stderr[-400:]
        second = self.run_py("""
            import app
            with app.app.app_context():
                j = app.ProcessedJob.query.first(); f = app.UploadedFile.query.first()
                print(j.status, "|", j.error_message, "|", f.status)
        """, env)
        status, message, file_status = [x.strip() for x in second.stdout.strip().splitlines()[-1].split("|")]
        assert status == "failed" and "restart" in message and file_status == "uploaded"


# ============================================================ frontend <-> backend contract
def test_every_url_the_frontend_calls_exists_on_the_backend(A):
    import re
    jsx = (BACKEND.parent / "frontend" / "src" / "App.jsx").read_text()
    paths = set(re.findall(r"\$\{API_BASE_URL\}(/[^`'\"]*)", jsx))
    assert len(paths) >= 8
    adapter = A.app.url_map.bind("localhost")
    missing = []
    for raw in sorted(paths):
        path = "/api" + re.sub(r"\$\{[^}]+\}", "1", raw)
        for method in ("GET", "POST", "DELETE"):
            try:
                adapter.match(path, method=method)
                break
            except Exception:
                continue
        else:
            missing.append(path)
    assert not missing, f"frontend calls endpoints the backend does not have: {missing}"
