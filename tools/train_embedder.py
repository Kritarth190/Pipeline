#!/usr/bin/env python3
"""
Train a small text embedder from scratch on your own data.

Input : a JSONL file of chunks, e.g. the file downloaded from the app's
        "RAG Prep" tab (run it once with "Create embeddings" unticked).
        Each line needs a "text" and a group key (default "doc_id").
Output: a folder with tokenizer.json, weights.npz and meta.json. Copy that
        folder to  backend/models/scratch-embedder/  and the app can embed with it.

How it learns (contrastive learning, no labels needed): two pieces of text
from the same group (two chunks of one document, or two random halves of one
chunk) are pulled together, and everything else in the batch is pushed apart.
The model is a learned tokenizer + one vector per word-piece + a learned
importance weight per word-piece, averaged. That is small, fast and exportable
to plain numpy, but it is a bag-of-words model: it captures topical similarity
from your data, not grammar or word order.

Usage:
    pip install -r tools/requirements-train.txt
    python tools/train_embedder.py chunks.jsonl --out my-embedder
"""

import argparse
import json
import math
import random
import sys
import time
from pathlib import Path

import numpy as np

MAX_LEN_EXPORT = 512


def get_path(record, key):
    """Read 'doc_id' or a nested key like 'metadata.category' from a record."""
    value = record
    for part in key.split("."):
        if not isinstance(value, dict) or part not in value:
            return None
        value = value[part]
    return value


def load_chunks(path, group_key):
    texts, groups = [], []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            text = (rec.get("text") or "").strip()
            group = get_path(rec, group_key)
            if text and group is not None:
                texts.append(text)
                groups.append(str(group))
    return texts, groups


def train_tokenizer(texts, vocab_size):
    from tokenizers import Tokenizer, models, normalizers, pre_tokenizers, trainers

    tok = Tokenizer(models.WordPiece(unk_token="[UNK]"))
    tok.normalizer = normalizers.BertNormalizer(lowercase=True)
    tok.pre_tokenizer = pre_tokenizers.BertPreTokenizer()
    trainer = trainers.WordPieceTrainer(
        vocab_size=vocab_size, min_frequency=2, special_tokens=["[PAD]", "[UNK]"]
    )
    tok.train_from_iterator(texts, trainer)
    tok.no_padding()
    tok.enable_truncation(max_length=MAX_LEN_EXPORT)
    return tok


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("chunks", help="JSONL file of chunks")
    ap.add_argument("--out", default="scratch-embedder", help="output folder")
    ap.add_argument("--name", default="scratch-embedder", help="model name stored in meta.json")
    ap.add_argument("--group-key", default="doc_id",
                    help="field whose equal values mark related text, e.g. doc_id or metadata.category")
    ap.add_argument("--dim", type=int, default=128)
    ap.add_argument("--vocab", type=int, default=16000)
    ap.add_argument("--epochs", type=int, default=20)
    ap.add_argument("--batch", type=int, default=256)
    ap.add_argument("--lr", type=float, default=5e-3)
    ap.add_argument("--temperature", type=float, default=0.05)
    ap.add_argument("--train-len", type=int, default=256, help="max tokens per text while training")
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    import torch
    import torch.nn as nn
    import torch.nn.functional as F

    random.seed(args.seed)
    np.random.seed(args.seed)
    torch.manual_seed(args.seed)
    device = "cuda" if torch.cuda.is_available() else "cpu"

    texts, group_ids = load_chunks(args.chunks, args.group_key)
    if len(texts) < 200:
        sys.exit(f"Only {len(texts)} usable chunks. You need at least a few hundred (thousands is better).")
    print(f"Loaded {len(texts)} chunks, {len(set(group_ids))} groups, device={device}")

    print("Training tokenizer...")
    tok = train_tokenizer(texts, args.vocab)
    vocab_size = tok.get_vocab_size()
    encoded = [e.ids[: args.train_len] for e in tok.encode_batch(texts, add_special_tokens=False)]

    groups = {}
    for ids, g in zip(encoded, group_ids):
        if ids:
            groups.setdefault(g, []).append(ids)
    keys = sorted(groups)
    random.shuffle(keys)
    n_eval = min(max(len(keys) // 20, 0), 2000)
    eval_keys, train_keys = (keys[:n_eval], keys[n_eval:]) if n_eval >= 50 else ([], keys)
    print(f"Groups: {len(train_keys)} train, {len(eval_keys)} held out for evaluation")

    class BagEncoder(nn.Module):
        """Importance-weighted mean of word-piece vectors, L2-normalised."""

        def __init__(self, vocab, dim):
            super().__init__()
            self.emb = nn.Embedding(vocab, dim, padding_idx=0)
            self.tok_w = nn.Embedding(vocab, 1, padding_idx=0)
            nn.init.normal_(self.emb.weight, 0.0, 0.1)
            nn.init.zeros_(self.tok_w.weight)

        def forward(self, ids, mask):
            w = F.softplus(self.tok_w(ids).squeeze(-1)) * mask
            v = (self.emb(ids) * w.unsqueeze(-1)).sum(1) / w.sum(1, keepdim=True).clamp_min(1e-6)
            return F.normalize(v, dim=-1)

    def pad(batch):
        longest = max(len(x) for x in batch)
        ids = torch.zeros(len(batch), longest, dtype=torch.long)
        for i, x in enumerate(batch):
            ids[i, : len(x)] = torch.tensor(x)
        return ids.to(device), (ids != 0).float().to(device)

    def drop_view(ids, keep=0.6):
        kept = [t for t in ids if random.random() < keep]
        return kept or [random.choice(ids)]

    def make_pair(members, train=True):
        if len(members) >= 2:
            a, b = random.sample(members, 2)
            return (drop_view(a, 0.9), drop_view(b, 0.9)) if train else (a, b)
        return (drop_view(members[0]), drop_view(members[0])) if train else (members[0], drop_view(members[0]))

    model = BagEncoder(vocab_size, args.dim).to(device)
    opt = torch.optim.AdamW(model.parameters(), lr=args.lr, weight_decay=0.0)
    steps_per_epoch = math.ceil(len(train_keys) / args.batch)
    sched = torch.optim.lr_scheduler.OneCycleLR(opt, max_lr=args.lr, total_steps=steps_per_epoch * args.epochs)

    @torch.no_grad()
    def evaluate():
        if not eval_keys:
            return None
        model.eval()
        rng_state = random.getstate()
        random.seed(1234)
        q_ids, t_ids = zip(*[make_pair(groups[k], train=False) for k in eval_keys])
        random.setstate(rng_state)
        q = model(*pad(list(q_ids)))
        t = model(*pad(list(t_ids)))
        ranks = (q @ t.T).argsort(dim=1, descending=True)
        target = torch.arange(len(eval_keys), device=device).unsqueeze(1)
        pos = (ranks == target).float().argmax(dim=1) + 1
        model.train()
        n = len(eval_keys)
        return {"candidates": n, "recall@1": float((pos <= 1).float().mean()),
                "recall@5": float((pos <= 5).float().mean()), "mrr": float((1.0 / pos.float()).mean()),
                "random_recall@1": 1.0 / n}

    print("Training...")
    started = time.time()
    model.train()
    for epoch in range(1, args.epochs + 1):
        random.shuffle(train_keys)
        total = 0.0
        for start in range(0, len(train_keys), args.batch):
            batch_keys = train_keys[start:start + args.batch]
            if len(batch_keys) < 2:
                continue
            pairs = [make_pair(groups[k]) for k in batch_keys]
            a = model(*pad([p[0] for p in pairs]))
            b = model(*pad([p[1] for p in pairs]))
            logits = a @ b.T / args.temperature
            labels = torch.arange(len(batch_keys), device=device)
            loss = (F.cross_entropy(logits, labels) + F.cross_entropy(logits.T, labels)) / 2
            opt.zero_grad()
            loss.backward()
            opt.step()
            sched.step()
            total += float(loss)
        msg = f"epoch {epoch:>3}/{args.epochs}  loss {total / steps_per_epoch:.4f}"
        if epoch % 5 == 0 or epoch == args.epochs:
            res = evaluate()
            if res:
                msg += f"  held-out recall@1 {res['recall@1']:.3f} recall@5 {res['recall@5']:.3f}"
        print(msg, flush=True)

    result = evaluate()
    model.eval()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    tok.save(str(out / "tokenizer.json"))
    emb = model.emb.weight.detach().cpu().numpy().astype(np.float16)
    tok_w = model.tok_w.weight.detach().cpu().numpy().reshape(-1).astype(np.float16)
    np.savez(out / "weights.npz", emb=emb, tok_w=tok_w)

    # Check the exported numpy version gives the same vectors as the trained model
    sample = texts[:8]
    enc = tok.encode_batch(sample, add_special_tokens=False)
    with torch.no_grad():
        ref = model(*pad([e.ids[: MAX_LEN_EXPORT] or [1] for e in enc])).cpu().numpy()
    emb32, w32 = emb.astype(np.float32), np.logaddexp(0.0, tok_w.astype(np.float32))
    got = []
    for e in enc:
        ids = np.asarray(e.ids or [1])
        w = w32[ids]
        v = (w[:, None] * emb32[ids]).sum(0) / w.sum()
        got.append(v / max(np.linalg.norm(v), 1e-9))
    cos = float(np.mean([float(r @ g) for r, g in zip(ref, got)]))
    print(f"Export check: mean cosine between trained model and numpy version = {cos:.4f} (should be ~1.0)")

    meta = {
        "name": args.name, "format": 1, "dim": args.dim, "vocab_size": vocab_size,
        "max_len": MAX_LEN_EXPORT, "pooling": "softplus-weighted mean",
        "trained_on": {"chunks": len(texts), "groups": len(keys), "epochs": args.epochs,
                       "group_key": args.group_key, "seconds": round(time.time() - started, 1)},
        "heldout_eval": result,
    }
    (out / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    size_mb = sum(f.stat().st_size for f in out.iterdir()) / 1e6
    print(f"\nSaved model to '{out}' ({size_mb:.1f} MB). Copy that folder to backend/models/scratch-embedder/")
    if result:
        print(f"Held-out retrieval: recall@1 {result['recall@1']:.3f} (random would be {result['random_recall@1']:.4f}), "
              f"recall@5 {result['recall@5']:.3f}")


if __name__ == "__main__":
    main()
