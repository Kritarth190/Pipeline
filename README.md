# Industrial Data Pipeline - Full Stack Application

A complete web application for processing industrial sensor data through a configurable ML pipeline with user authentication and file management.

## Features

- 🔐 User authentication (Register/Login)
- 📁 File upload and management
- ⚙️ Configurable data processing pipeline
- 📊 Real-time processing metrics
- 💾 Download processed results
- 🎯 Multi-stage data processing:
  - Validation
  - Data Cleaning
  - Outlier Detection
  - Normalization
  - Feature Engineering

## Quick Start

### Prerequisites

- Python 3.8+
- Node.js 14+
- pip
- npm

### Backend Setup
```bash
cd backend
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

Backend runs on: http://localhost:5000

### Frontend Setup
```bash
cd frontend
npm install
npm run dev
```

Frontend runs on: http://localhost:3000

## RAG preparation and the from-scratch embedder

The **RAG Prep** tab turns a text dataset (uploaded, or imported from Hugging Face / Kaggle) into
token-sized chunks, optionally with embeddings, as a `.jsonl` file you can load into a vector store.

Embeddings come from a small model you train yourself on your own data (free, and it runs on the
Render free tier because inference is plain numpy):

1. **Export chunks.** In RAG Prep, pick your dataset, untick *Create embeddings*, build, then download
   the `.jsonl` from the Results tab.
2. **Train** (a few minutes on a laptop or free Google Colab, no GPU needed):
   ```bash
   pip install -r tools/requirements-train.txt
   python tools/train_embedder.py chunks.jsonl --out my-embedder
   ```
   It prints held-out retrieval scores at the end. Use `--group-key metadata.<column>` if a column
   (e.g. a category) marks which chunks belong together; the default groups chunks by `doc_id`.
3. **Install.** Copy the files from `my-embedder/` into `backend/models/scratch-embedder/`, commit and push.
4. **Embed.** Run RAG Prep again with *Trained from scratch (free)* selected.

Good to know:
- It is a bag-of-words model: it captures topical similarity in *your* data, not grammar or word order,
  and it will not match large pretrained models. Use thousands of chunks or more.
- Queries must be embedded with the same model (`from scratch_embedder import ScratchEmbedder`).
  If you retrain, re-embed everything.

## Security

The backend has a few protections worth knowing about when you deploy it:

- **Rate limiting.** Login, registration, dataset imports and RAG jobs are capped per IP
  (e.g. 10 login attempts/minute, 10 registrations/hour) to slow brute-forcing and job-spam.
  It's on by default; set `RATELIMIT_ENABLED=false` to disable it, or `RATELIMIT_STORAGE_URI`
  (e.g. a Redis URL) to share limits across more than one gunicorn worker/dyno — the default
  in-memory store is per-process.
- **Per-user storage quota.** Each account is capped at `MAX_USER_STORAGE_BYTES` (default
  500 MB) across uploads and imports combined, so one user can't fill the free tier's disk.
- **Password policy.** Registration requires at least 8 characters.
- **No account enumeration.** A duplicate username and a duplicate email return the same
  "Username or email already registered" message.
- **Formula-injection defused.** Text that would be read as a spreadsheet formula (starts
  with `=`, `+`, `-` or `@`) is prefixed with `'` in every CSV/JSONL export, so opening a
  download in Excel/Sheets can't execute it.
- **Safe zip extraction.** Kaggle dataset archives are extracted with a path check that
  refuses any entry that would land outside the target folder ("zip slip").
- **Scrubbed error messages.** Exceptions shown to users (and stored on failed jobs) have the
  server's internal file paths stripped out first.

Always set a real `SECRET_KEY` in production — without one the app falls back to a random key
per process, which invalidates every login on restart and (with more than one worker) can make
tokens fail unpredictably depending on which worker issued them.

## Usage

1. Register a new account
2. Login with your credentials
3. Upload CSV/TXT files
4. Configure pipeline settings
5. Process your data
6. View metrics and download results

## API Endpoints

- `POST /api/register` - Register user
- `POST /api/login` - Login user
- `POST /api/files/upload` - Upload file
- `GET /api/files` - List files
- `DELETE /api/files/<id>` - Delete file
- `POST /api/process` - Process file
- `GET /api/jobs` - List jobs
- `GET /api/jobs/<id>/download` - Download result
- `POST /api/datasets/import` - Import a Hugging Face / Kaggle dataset
- `GET /api/files/<id>/columns` - List columns and suggest text columns
- `GET /api/embedders` - Which embedders the server can run
- `POST /api/rag/process` - Start a RAG chunking/embedding job

## Tech Stack

**Backend:**
- Flask
- SQLAlchemy
- JWT Authentication
- Pandas/NumPy

**Frontend:**
- React
- Vite
- Tailwind CSS (via inline classes)
- Lucide Icons

## License

MIT

## Author

Industrial Data Pipeline Team