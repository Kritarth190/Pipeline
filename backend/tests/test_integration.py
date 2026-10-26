"""Integration tests: a real gunicorn server over HTTP, the real trainer, a real frontend build."""

import json
import os
import shutil
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest
import requests

from conftest import BACKEND, text_csv

ROOT = BACKEND.parent


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    work = tmp_path_factory.mktemp("srv")
    port = free_port()
    env = {**os.environ, "DATABASE_URL": f"sqlite:///{work}/db.sqlite", "SECRET_KEY": "integration",
           "FRONTEND_URL": "https://app.example.com", "PYTHONPATH": str(BACKEND),
           "SCRATCH_EMBEDDER_DIR": str(work / "model")}
    env.pop("HF_TOKEN", None)
    log = open(work / "server.log", "w")
    proc = subprocess.Popen(["gunicorn", "app:app", "--timeout", "120", "-b", f"127.0.0.1:{port}"],
                            cwd=work, env={**env, "PYTHONPATH": str(BACKEND)}, stdout=log, stderr=log)
    base = f"http://127.0.0.1:{port}/api"
    for _ in range(60):
        try:
            if requests.get(f"{base}/health", timeout=1).ok:
                break
        except requests.RequestException:
            time.sleep(0.5)
    else:
        proc.kill()
        pytest.fail("server did not start:\n" + (work / "server.log").read_text()[-1500:])
    yield {"base": base, "work": work, "env": env}
    proc.terminate()
    proc.wait(10)


@pytest.fixture(scope="module")
def session(server):
    name = "i_" + uuid.uuid4().hex[:8]
    b = server["base"]
    assert requests.post(f"{b}/register", json={"username": name, "email": f"{name}@x.com", "password": "pw12345"}).status_code == 201
    token = requests.post(f"{b}/login", json={"username": name, "password": "pw12345"}).json()["token"]
    s = requests.Session()
    s.headers["Authorization"] = f"Bearer {token}"
    return s


def wait(session, base, timeout=120):
    end = time.time() + timeout
    while time.time() < end:
        job = session.get(f"{base}/jobs").json()["jobs"][0]
        if job["status"] in ("completed", "failed"):
            return job
        time.sleep(1)
    pytest.fail("job never finished")


def upload(session, base, content, name="data.csv"):
    r = session.post(f"{base}/files/upload", files={"file": (name, content)})
    assert r.status_code == 201, r.text
    return r.json()["file"]["id"]


def test_server_serves_cors_and_json(server):
    b = server["base"]
    r = requests.options(f"{b}/login", headers={"Origin": "https://app.example.com", "Access-Control-Request-Method": "POST"})
    assert r.headers.get("Access-Control-Allow-Origin") == "https://app.example.com"
    assert requests.get(f"{b}/health").json()["status"] == "healthy"


def test_classic_pipeline_over_http(server, session):
    b = server["base"]
    from conftest import sensor_csv
    fid = upload(session, b, sensor_csv())
    r = session.post(f"{b}/process", json={"file_id": fid, "config": {}})
    assert r.status_code == 200, r.text
    d = session.get(f"{b}/jobs/{r.json()['job']['id']}/download")
    assert d.status_code == 200 and d.text.count("\n") > 50


def test_rag_flow_train_install_embed_retrieve(server, session):
    pytest.importorskip("torch")
    b, work = server["base"], server["work"]

    # 1. two-topic corpus -> chunks (no embeddings) over HTTP
    docs = ["cats purr meow whiskers kitten feline fur", "dogs bark fetch puppy canine leash tail",
            "stocks bonds market equity dividend portfolio", "interest inflation bank loan mortgage credit"]
    rows = ["id,text,label"]
    for i in range(600):
        base = docs[i % 4]
        rows.append(f"{i},{base} {base.split()[i % 5]} sample {i},{'pets' if i % 4 < 2 else 'money'}")
    fid = upload(session, b, "\n".join(rows) + "\n", "topics.csv")
    cfg = {"text_columns": ["text"], "id_column": "id", "metadata_columns": ["label"], "embed": False,
           "chunk_tokens": 32, "chunk_overlap": 4}
    assert session.post(f"{b}/rag/process", json={"file_id": fid, "config": cfg}).status_code == 202
    job = wait(session, b)
    assert job["status"] == "completed", job
    chunks = session.get(f"{b}/jobs/{job['id']}/download").text
    jsonl = work / "chunks.jsonl"
    jsonl.write_text(chunks)

    # 2. train the real model with the real script
    out = work / "model"
    r = subprocess.run([sys.executable, str(ROOT / "tools" / "train_embedder.py"), str(jsonl), "--out", str(out),
                        "--group-key", "metadata.label", "--dim", "32", "--vocab", "500", "--epochs", "6", "--batch", "32"],
                       capture_output=True, text=True, timeout=600)
    assert r.returncode == 0, r.stdout[-800:] + r.stderr[-800:]
    assert {p.name for p in out.iterdir()} >= {"tokenizer.json", "weights.npz", "meta.json"}

    # 3. the running server must see the model (it was started before the model existed)
    info = session.get(f"{b}/embedders").json()
    assert info["scratch"]["available"] is True, info

    # 4. embed job through the server, then check retrieval quality
    cfg.update(embed=True, embedder="scratch")
    assert session.post(f"{b}/rag/process", json={"file_id": fid, "config": cfg}).status_code == 202
    job = wait(session, b)
    assert job["status"] == "completed", job
    assert job["metrics"]["embedding_dim"] == 32
    recs = [json.loads(l) for l in session.get(f"{b}/jobs/{job['id']}/download").text.splitlines()]

    import numpy as np
    sys.path.insert(0, str(BACKEND))
    from scratch_embedder import ScratchEmbedder
    emb = ScratchEmbedder(out)
    M = np.array([r["embedding"] for r in recs], dtype=np.float32)
    assert np.allclose(np.linalg.norm(M, axis=1), 1.0, atol=1e-3)
    q = emb.embed(["kitten purr and meow"])[0]
    top = np.argsort(-(M @ q))[:10]
    assert sum(recs[i]["metadata"]["label"] == "pets" for i in top) >= 8, "retrieval does not separate topics"


def test_frontend_builds():
    if not shutil.which("npm"):
        pytest.skip("npm not available")
    fe = ROOT / "frontend"
    if not (fe / "node_modules").exists():
        r = subprocess.run(["npm", "install", "--no-audit", "--no-fund"], cwd=fe, capture_output=True, text=True, timeout=600)
        assert r.returncode == 0, r.stderr[-800:]
    r = subprocess.run(["npm", "run", "build"], cwd=fe, capture_output=True, text=True, timeout=600,
                       env={**os.environ, "VITE_API_URL": "https://api.example.com/api"})
    assert r.returncode == 0, (r.stdout + r.stderr)[-1500:]
