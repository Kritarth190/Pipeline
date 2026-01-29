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