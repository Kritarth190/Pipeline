"""
Shared test setup.

The app reads its configuration when it is imported, so everything is set up
BEFORE the first import: a throw-away working directory (uploads/, processed/,
data_versions/ and the SQLite file all live there), a known SECRET_KEY, a
FRONTEND_URL for the CORS tests, and no HF_TOKEN / trained model unless a test
adds one.
"""

import os
import sys
import tempfile
import time
import uuid
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
REPO = BACKEND.parent
sys.path.insert(0, str(BACKEND))

TMP = Path(tempfile.mkdtemp(prefix="pipeline_tests_")).resolve()
os.chdir(TMP)
os.environ["DATABASE_URL"] = f"sqlite:///{TMP}/test.db"
os.environ["SECRET_KEY"] = "test-secret-key-test-secret-key-1234"
os.environ["FRONTEND_URL"] = "https://app.example.com"
os.environ.pop("HF_TOKEN", None)
os.environ["SCRATCH_EMBEDDER_DIR"] = str(TMP / "no-model")
# The suite makes far more requests per minute than a real client ever would; rate limiting
# itself is covered by its own tests (test_rate_limiting.py), which enable it explicitly.
os.environ["RATELIMIT_ENABLED"] = "false"


@pytest.fixture(scope="session")
def A():
    """The imported app module (app.py)."""
    import app as app_module
    from sqlalchemy import event

    # Production uses PostgreSQL, which enforces foreign keys. SQLite does not unless
    # asked, so turn it on to make tests behave like production.
    def _fk_on(dbapi_conn, _record):
        dbapi_conn.execute("PRAGMA foreign_keys=ON")

    with app_module.app.app_context():
        app_module.db.engine.dispose()
        event.listen(app_module.db.engine, "connect", _fk_on)
    return app_module


@pytest.fixture()
def client(A):
    return A.app.test_client()


@pytest.fixture()
def user(client):
    """A freshly registered, logged-in user: {'headers', 'id', 'username', 'password'}."""
    name = f"user_{uuid.uuid4().hex[:10]}"
    password = "pw-" + uuid.uuid4().hex[:8]
    r = client.post("/api/register", json={"username": name, "email": f"{name}@x.com", "password": password})
    assert r.status_code == 201, r.get_json()
    token = client.post("/api/login", json={"username": name, "password": password}).get_json()["token"]
    return {"headers": {"Authorization": f"Bearer {token}"}, "id": r.get_json()["user"]["id"],
            "username": name, "password": password}


@pytest.fixture()
def other_user(client):
    name = f"other_{uuid.uuid4().hex[:10]}"
    client.post("/api/register", json={"username": name, "email": f"{name}@x.com", "password": "pw1234567"})
    token = client.post("/api/login", json={"username": name, "password": "pw1234567"}).get_json()["token"]
    return {"headers": {"Authorization": f"Bearer {token}"}, "username": name}


def upload(client, headers, content, filename="data.csv"):
    import io
    data = content.encode() if isinstance(content, str) else content
    return client.post("/api/files/upload", headers=headers,
                       data={"file": (io.BytesIO(data), filename)}, content_type="multipart/form-data")


def text_csv(rows=40, words=60):
    """A CSV of long text documents with an id and a label column."""
    lines = ["id,text,label"]
    for i in range(rows):
        body = " ".join(f"word{(i * 7 + j) % 50}" for j in range(words))
        lines.append(f'{i},"Document {i} {body}",{"a" if i % 2 else "b"}')
    return "\n".join(lines) + "\n"


def sensor_csv(rows=200):
    import numpy as np
    rng = np.random.default_rng(1)
    lines = ["timestamp,machine_id,temperature,pressure,vibration,rpm"]
    for i in range(rows):
        lines.append(f"2026-01-01 00:{i // 60:02d}:{i % 60:02d},M{i % 3},{rng.normal(75, 5):.2f},"
                     f"{rng.normal(30, 3):.2f},{rng.exponential(2):.3f},{rng.normal(1500, 100):.1f}")
    return "\n".join(lines) + "\n"


def wait_for_job(client, headers, job_id=None, timeout=60):
    """Poll /api/jobs until the (given or newest) job finishes; returns the job dict."""
    deadline = time.time() + timeout
    job = None
    while time.time() < deadline:
        jobs = client.get("/api/jobs", headers=headers).get_json()["jobs"]
        job = next((j for j in jobs if j["id"] == job_id), None) if job_id else (jobs[0] if jobs else None)
        if job and job["status"] in ("completed", "failed"):
            return job
        time.sleep(0.2)
    raise AssertionError(f"job did not finish in {timeout}s: {job}")


class FakeEmbedder:
    """Deterministic stand-in for a real embedder: no network, no model."""
    name = "fake"
    model = "fake-embedder"

    def __init__(self, dim=8):
        self.dim = dim
        self.calls = []

    def embed(self, texts):
        import numpy as np
        self.calls.append(len(texts))
        out = np.zeros((len(texts), self.dim), dtype=np.float32)
        for i, t in enumerate(texts):
            for ch in t:
                out[i, ord(ch) % self.dim] += 1.0
        norms = np.linalg.norm(out, axis=1, keepdims=True)
        norms[norms == 0] = 1
        return out / norms


@pytest.fixture(autouse=True)
def offline_tokenizer(monkeypatch):
    """Never download a tokenizer from the Hugging Face hub during tests: use whitespace tokens."""
    import rag_prep

    class WhitespaceTokenizer(rag_prep.TextTokenizer):
        def __init__(self):
            self.name, self._tok, self.kind = "whitespace", None, "whitespace"

    monkeypatch.setattr(rag_prep, "get_tokenizer", lambda name: WhitespaceTokenizer())
