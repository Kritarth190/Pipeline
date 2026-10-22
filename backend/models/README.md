# Trained embedder goes here

Put the folder produced by `tools/train_embedder.py` in `backend/models/scratch-embedder/`
so it contains these three files:

    tokenizer.json
    weights.npz
    meta.json

The backend loads it automatically (override the location with the `SCRATCH_EMBEDDER_DIR`
environment variable). Until a model is here, the app shows "not installed" for the
from-scratch embedder and you can still export chunks without embeddings.
