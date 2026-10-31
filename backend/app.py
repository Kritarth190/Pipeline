from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
import jwt
from datetime import datetime, timedelta
from functools import wraps
from dataclasses import asdict
import pandas as pd
import os
import re
import json
from pathlib import Path
import secrets
import threading
import logging
import uuid
from werkzeug.exceptions import HTTPException

from rag_prep import RAGConfig, build_rag_dataset, analyze_text_columns, load_text_table, csv_safe
from dataset_sources import fetch_huggingface, fetch_kaggle, normalize_dataset_id, DatasetImportError
from scratch_embedder import scratch_model_info
from industrial_pipeline import (
    IndustrialDataPipeline, 
    PipelineConfig, 
    DatabaseConfig,
    DataAnalyzer
)

logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
# Never keep a real secret in the source code. If SECRET_KEY isn't set we use a
# random one for this process (logins are invalidated on every restart).
_secret = os.environ.get('SECRET_KEY')
if not _secret:
    logging.warning("SECRET_KEY is not set - using a temporary random key. Set it in your environment.")
    _secret = secrets.token_hex(32)
app.config['SECRET_KEY'] = _secret
# Use PostgreSQL in production, SQLite locally
DATABASE_URL = os.environ.get('DATABASE_URL', 'sqlite:///pipeline_users.db')

# Normalise any Postgres URL (postgres://, postgresql://, postgresql+psycopg://, ...) to the
# psycopg2 driver we install. Newer SQLAlchemy defaults plain postgresql:// to psycopg v3,
# which is not installed here and crashes the app on start.
DATABASE_URL = re.sub(r'^postgres(ql)?(\+\w+)?://', 'postgresql+psycopg2://', DATABASE_URL)

app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['UPLOAD_FOLDER'] = os.path.abspath('./uploads')
app.config['PROCESSED_FOLDER'] = os.path.abspath('./processed')
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max file size
# Hard ceiling on rows per RAG job / dataset import (protects small free-tier hosts)
MAX_RAG_ROWS = int(os.environ.get('MAX_RAG_ROWS', '20000'))
# Hard ceiling on bytes one account may have on disk at once (uploads + imports), so a
# single user can't fill the free tier's small, ephemeral disk.
MAX_USER_STORAGE_BYTES = int(os.environ.get('MAX_USER_STORAGE_BYTES', str(500 * 1024 * 1024)))

db = SQLAlchemy(app)

# Rate limiting: brute-force login/registration and job-spamming cost real money or quota
# on free-tier hosts (and on Kaggle/HF for dataset imports), so sensitive routes are capped
# per IP. The default in-memory store is per-process: on a multi-worker/multi-dyno deploy,
# set RATELIMIT_STORAGE_URI (e.g. to the same Redis most hosts already offer) for it to be
# shared; a single free-tier gunicorn worker works fine with the default. Set
# RATELIMIT_ENABLED=false to turn it off entirely (the test suite does this).
app.config['RATELIMIT_ENABLED'] = os.environ.get('RATELIMIT_ENABLED', 'true').lower() != 'false'
limiter = Limiter(
    get_remote_address,
    app=app,
    storage_uri=os.environ.get('RATELIMIT_STORAGE_URI', 'memory://'),
    default_limits=[],
)

# Create necessary directories
Path(app.config['UPLOAD_FOLDER']).mkdir(parents=True, exist_ok=True)
Path(app.config['PROCESSED_FOLDER']).mkdir(parents=True, exist_ok=True)
Path("./data_versions").mkdir(parents=True, exist_ok=True)
# CORS configuration
FRONTEND_URL = os.environ.get("FRONTEND_URL")

CORS(app, resources={
    r"/api/*": {
        "origins": [
            "http://localhost:3000",
            "http://localhost:5173",
            *([FRONTEND_URL] if FRONTEND_URL else [])
        ],
        "methods": ["GET", "POST", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"]
    }
})

# ============================================================================
# DATABASE MODELS
# ============================================================================

class User(db.Model):
    """User model for authentication"""
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Relationships
    files = db.relationship('UploadedFile', backref='user', lazy=True, cascade='all, delete-orphan')
    processed_jobs = db.relationship('ProcessedJob', backref='user', lazy=True, cascade='all, delete-orphan')
    
    def set_password(self, password):
        self.password_hash = generate_password_hash(password)
    
    def check_password(self, password):
        return check_password_hash(self.password_hash, password)
    
    def to_dict(self):
        return {
            'id': self.id,
            'username': self.username,
            'email': self.email,
            'created_at': self.created_at.isoformat()
        }


class UploadedFile(db.Model):
    """Track uploaded files"""
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    filename = db.Column(db.String(255), nullable=False)
    original_filename = db.Column(db.String(255), nullable=False)
    file_path = db.Column(db.String(512), nullable=False)
    file_size = db.Column(db.Integer)
    upload_date = db.Column(db.DateTime, default=datetime.utcnow)
    status = db.Column(db.String(50), default='uploaded')
    
    def to_dict(self):
        return {
            'id': self.id,
            'filename': self.original_filename,
            'file_size': self.file_size,
            'upload_date': self.upload_date.isoformat(),
            'status': self.status
        }


class ProcessedJob(db.Model):
    """Track processing jobs and results"""
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    file_id = db.Column(db.Integer, db.ForeignKey('uploaded_file.id'))
    processed_date = db.Column(db.DateTime, default=datetime.utcnow)
    output_path = db.Column(db.String(512))
    metrics = db.Column(db.Text)
    config = db.Column(db.Text)
    status = db.Column(db.String(50), default='pending')
    error_message = db.Column(db.Text)
    
    def to_dict(self):
        return {
            'id': self.id,
            'file_id': self.file_id,
            'processed_date': self.processed_date.isoformat(),
            'metrics': json.loads(self.metrics) if self.metrics else None,
            'config': json.loads(self.config) if self.config else None,
            'status': self.status,
            'error_message': self.error_message
        }


# ============================================================================
# HELPERS AND ERROR HANDLING
# ============================================================================

EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')
MIN_PASSWORD_LENGTH = 8


def scrub_error(exc):
    """Message safe to show a user: real server paths removed, exception type kept."""
    text = str(exc)
    for folder in (app.config['UPLOAD_FOLDER'], app.config['PROCESSED_FOLDER'], os.getcwd()):
        text = text.replace(folder + os.sep, '').replace(folder, '')
    return text


def sanitize_for_csv(df):
    """Return a copy of df with every text cell passed through csv_safe.

    Checks is_object_dtype too, not just dtype == object: pandas' newer string dtype
    (pd.StringDtype, used when a column is all-string) does not equal `object`, so that
    check alone silently skips those columns.
    """
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].map(csv_safe)
    return df


def _stamp():
    """Unique-per-call stamp for file names (a second-resolution time alone collides)."""
    return f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"


def _body():
    """JSON body as a dict (empty dict if missing, malformed or not an object)."""
    data = request.get_json(silent=True)
    return data if isinstance(data, dict) else {}


def _remove_quietly(path):
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except OSError:
        logger.warning("Could not remove %s", path)


@app.errorhandler(HTTPException)
def json_http_error(e):
    # The frontend always parses JSON; Flask's default HTML error pages show up as "Network error"
    messages = {413: 'File is too large (limit is 50 MB)'}
    return jsonify({'message': messages.get(e.code, e.description or e.name)}), e.code


@app.errorhandler(Exception)
def json_server_error(e):
    logger.exception("Unhandled error")
    return jsonify({'message': 'Internal server error'}), 500


# ============================================================================
# AUTHENTICATION DECORATOR
# ============================================================================

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        
        if not token:
            return jsonify({'message': 'Token is missing'}), 401
        
        try:
            if token.startswith('Bearer '):
                token = token[7:]
            
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
            user_id = data.get('user_id')
            if not isinstance(user_id, int):
                return jsonify({'message': 'Invalid token'}), 401
            current_user = db.session.get(User, user_id)
            
            if not current_user:
                return jsonify({'message': 'User not found'}), 401
                
        except jwt.ExpiredSignatureError:
            return jsonify({'message': 'Token has expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'message': 'Invalid token'}), 401
        
        return f(current_user, *args, **kwargs)
    
    return decorated


# ============================================================================
# API ROUTES - AUTHENTICATION
# ============================================================================

@app.route('/api/register', methods=['POST'])
@limiter.limit('10 per hour')
def register():
    """Register a new user"""
    data = _body()
    username = str(data.get('username') or '').strip()
    email = str(data.get('email') or '').strip().lower()
    password = data.get('password') or ''

    if not username or not email or not password:
        return jsonify({'message': 'Missing required fields'}), 400

    if not EMAIL_RE.match(email):
        return jsonify({'message': 'Please enter a valid email address'}), 400

    if len(password) < MIN_PASSWORD_LENGTH:
        return jsonify({'message': f'Password must be at least {MIN_PASSWORD_LENGTH} characters'}), 400

    # One generic message for both clashes: telling the two apart lets someone enumerate
    # which usernames/emails are already registered.
    taken = (User.query.filter_by(username=username).first()
             or User.query.filter(db.func.lower(User.email) == email).first())
    if taken:
        return jsonify({'message': 'Username or email already registered'}), 400

    user = User(username=username, email=email)
    user.set_password(password)
    
    db.session.add(user)
    db.session.commit()
    
    return jsonify({
        'message': 'User registered successfully',
        'user': user.to_dict()
    }), 201


@app.route('/api/login', methods=['POST'])
@limiter.limit('10 per minute')
def login():
    """Login user and return JWT token"""
    data = _body()
    
    if not data.get('username') or not data.get('password'):
        return jsonify({'message': 'Missing credentials'}), 400
    
    user = User.query.filter_by(username=data['username']).first()
    
    if not user or not user.check_password(data['password']):
        return jsonify({'message': 'Invalid credentials'}), 401
    
    token = jwt.encode({
        'user_id': user.id,
        'exp': datetime.utcnow() + timedelta(days=7)
    }, app.config['SECRET_KEY'], algorithm='HS256')
    
    return jsonify({
        'token': token,
        'user': user.to_dict()
    }), 200


# ============================================================================
# API ROUTES - FILE MANAGEMENT
# ============================================================================

def _user_storage_used(user_id):
    total = db.session.query(db.func.coalesce(db.func.sum(UploadedFile.file_size), 0)) \
        .filter_by(user_id=user_id).scalar()
    return int(total or 0)


@app.route('/api/files/upload', methods=['POST'])
@token_required
@limiter.limit('30 per minute')
def upload_file(current_user):
    """Upload a file"""
    if 'file' not in request.files:
        return jsonify({'message': 'No file provided'}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({'message': 'No file selected'}), 400

    if not file.filename.lower().endswith(('.csv', '.txt')):
        return jsonify({'message': 'Only CSV and TXT files are allowed'}), 400

    used = _user_storage_used(current_user.id)
    if used >= MAX_USER_STORAGE_BYTES:
        return jsonify({'message': 'Storage limit reached. Delete some files before uploading more.'}), 400

    # secure_filename drops non-ASCII letters (even the whole name), so keep the extension separately
    stem, ext = os.path.splitext(file.filename)
    original_filename = f"{secure_filename(stem) or 'file'}{ext.lower()}"
    filename = f"{current_user.id}_{_stamp()}_{original_filename}"
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)

    file.save(file_path)
    file_size = os.path.getsize(file_path)

    # Re-check after writing: Content-Length can't be trusted ahead of time (chunked uploads,
    # or a client that lies), so this is the real guard against a user quietly busting the quota.
    if used + file_size > MAX_USER_STORAGE_BYTES:
        _remove_quietly(file_path)
        return jsonify({'message': 'Storage limit reached. Delete some files before uploading more.'}), 400

    uploaded_file = UploadedFile(
        user_id=current_user.id,
        filename=filename,
        original_filename=original_filename,
        file_path=file_path,
        file_size=file_size
    )

    db.session.add(uploaded_file)
    db.session.commit()

    return jsonify({
        'message': 'File uploaded successfully',
        'file': uploaded_file.to_dict()
    }), 201


@app.route('/api/files', methods=['GET'])
@token_required
def get_files(current_user):
    """Get all files for current user"""
    files = UploadedFile.query.filter_by(user_id=current_user.id).order_by(UploadedFile.upload_date.desc()).all()
    return jsonify({
        'files': [f.to_dict() for f in files]
    }), 200


@app.route('/api/files/<int:file_id>', methods=['DELETE'])
@token_required
def delete_file(current_user, file_id):
    """Delete a file"""
    file_obj = UploadedFile.query.filter_by(id=file_id, user_id=current_user.id).first()
    
    if not file_obj:
        return jsonify({'message': 'File not found'}), 404
    
    # Results of this file go with it (and their rows reference the file, so they must be removed first)
    jobs = ProcessedJob.query.filter_by(file_id=file_obj.id).all()
    paths = [file_obj.file_path] + [j.output_path for j in jobs]
    for job in jobs:
        db.session.delete(job)
    db.session.delete(file_obj)
    db.session.commit()
    for path in paths:
        _remove_quietly(path)
    
    return jsonify({'message': 'File deleted successfully'}), 200


# ============================================================================
# API ROUTES - PIPELINE PROCESSING WITH SMART DETECTION
# ============================================================================

def _validate_pipeline_config(cfg):
    """Return an error message if the user's pipeline config is unusable, else None."""
    if not isinstance(cfg, dict):
        return "'config' must be an object"
    for key in ('validation_enabled', 'outlier_detection_enabled', 'normalization_enabled',
                'feature_engineering_enabled'):
        if key in cfg and not isinstance(cfg[key], bool):
            return f"'{key}' must be true or false"
    for key, low, high in (('missing_value_threshold', 0.0, 1.0), ('outlier_std_threshold', 0.1, 100.0)):
        if key in cfg:
            v = cfg[key]
            if isinstance(v, bool) or not isinstance(v, (int, float)) or not low <= v <= high:
                return f"'{key}' must be a number between {low} and {high}"
    return None


@app.route('/api/process', methods=['POST'])
@token_required
def process_data(current_user):
    """Process uploaded file through pipeline with smart parameter detection"""
    data = _body()
    
    if not data.get('file_id'):
        return jsonify({'message': 'File ID is required'}), 400
    
    pipeline_config_data = data.get('config', {})
    if pipeline_config_data is None:
        pipeline_config_data = {}
    config_error = _validate_pipeline_config(pipeline_config_data)
    if config_error:
        return jsonify({'message': config_error}), 400
    
    file_obj = UploadedFile.query.filter_by(id=data['file_id'], user_id=current_user.id).first()
    
    if not file_obj:
        return jsonify({'message': 'File not found'}), 404
    
    file_obj.status = 'processing'
    db.session.commit()
    
    try:
        # Read the uploaded file
        input_data = pd.read_csv(file_obj.file_path)
        if input_data.empty:
            raise ValueError('The file has no data rows')
        
        # ============ SMART PARAMETER DETECTION ============
        # Analyze the uploaded data
        analysis = DataAnalyzer.analyze_dataframe(input_data)
        
        # Get smart configuration suggestions
        smart_config = DataAnalyzer.suggest_pipeline_config(analysis)
        
        logger.info("Smart detection for '%s' (user %s): %d rows x %d cols, features=%s, suggestions=%s",
                    file_obj.original_filename, current_user.username,
                    analysis['total_rows'], analysis['total_columns'],
                    [k for k, v in analysis['detected_features'].items() if v],
                    smart_config['reasons'])

        # Merge user config with smart suggestions (user config takes priority)
        tunable = ['validation_enabled', 'outlier_detection_enabled', 'normalization_enabled',
                   'feature_engineering_enabled', 'missing_value_threshold', 'outlier_std_threshold']
        final_config = {k: pipeline_config_data.get(k, smart_config[k]) for k in tunable}

        # Create pipeline configuration
        config = PipelineConfig(
            batch_size=len(input_data),
            **final_config,
            enable_versioning=True,
            version_storage_path=f"./data_versions/user_{current_user.id}",
            database=DatabaseConfig(
                use_postgres=False,
                use_mongodb=False
            )
        )
        
        # Run pipeline with user's data
        pipeline = IndustrialDataPipeline(config)
        processed_data, metrics = pipeline.run(input_data)
        
        logger.info("Processed '%s': %d/%d valid records in %.2fs, quality %.2f%%, %d output columns",
                    file_obj.original_filename, metrics.valid_records, metrics.total_records,
                    metrics.processing_time, metrics.data_quality_score * 100, len(processed_data.columns))

        # Save processed data
        output_filename = f"processed_{_stamp()}_{file_obj.filename}"
        output_path = os.path.join(app.config['PROCESSED_FOLDER'], output_filename)
        sanitize_for_csv(processed_data).to_csv(output_path, index=False)
        
        # Create job record with analysis info
        job = ProcessedJob(
            user_id=current_user.id,
            file_id=file_obj.id,
            output_path=output_path,
            metrics=json.dumps({
                'total_records': metrics.total_records,
                'valid_records': metrics.valid_records,
                'invalid_records': metrics.invalid_records,
                'processing_time': metrics.processing_time,
                'data_quality_score': metrics.data_quality_score,
                'throughput': metrics.throughput,
                'stage_times': metrics.stage_times,
                # Add analysis info
                'data_analysis': {
                    'input_columns': analysis['columns'],
                    'numeric_columns': len(analysis['numeric_columns']),
                    'categorical_columns': len(analysis['categorical_columns']),
                    'detected_features': analysis['detected_features'],
                    'output_columns': len(processed_data.columns)
                }
            }),
            config=json.dumps(final_config),
            status='completed'
        )
        
        db.session.add(job)
        file_obj.status = 'processed'
        db.session.commit()
        
        return jsonify({
            'message': 'Processing completed successfully',
            'job': job.to_dict(),
            'analysis': {
                'detected_features': analysis['detected_features'],
                'suggestions': smart_config['reasons']
            }
        }), 200
        
    except Exception as e:
        logger.exception("Processing failed for file %s", file_obj.id)

        file_obj.status = 'error'
        
        job = ProcessedJob(
            user_id=current_user.id,
            file_id=file_obj.id,
            status='failed',
            error_message=scrub_error(e)
        )

        db.session.add(job)
        db.session.commit()

        return jsonify({
            'message': 'Processing failed',
            'error': scrub_error(e)
        }), 500


@app.route('/api/jobs', methods=['GET'])
@token_required
def get_jobs(current_user):
    """Get all processing jobs for current user"""
    jobs = ProcessedJob.query.filter_by(user_id=current_user.id).order_by(ProcessedJob.processed_date.desc()).all()
    
    result = []
    for job in jobs:
        job_dict = job.to_dict()
        if job.file_id:
            file_obj = db.session.get(UploadedFile, job.file_id)
            if file_obj:
                job_dict['filename'] = file_obj.original_filename
        result.append(job_dict)
    
    return jsonify({'jobs': result}), 200


@app.route('/api/jobs/<int:job_id>/download', methods=['GET'])
@token_required
def download_processed(current_user, job_id):
    """Download processed data"""
    job = ProcessedJob.query.filter_by(id=job_id, user_id=current_user.id).first()
    
    if not job:
        return jsonify({'message': 'Job not found'}), 404
    
    if not job.output_path or not os.path.exists(job.output_path):
        return jsonify({'message': 'Processed file not found'}), 404
    
    if job.output_path.endswith('.jsonl'):
        return send_file(
            job.output_path,
            as_attachment=True,
            download_name=f"rag_chunks_{job.id}.jsonl",
            mimetype='application/x-ndjson'
        )

    return send_file(
        job.output_path,
        as_attachment=True,
        download_name=f"processed_{job.id}.csv"
    )


# ============================================================================
# API ROUTES - RAG PREPARATION (tokenizer + embedder) AND DATASET IMPORT
# ============================================================================

# Only one RAG job runs at a time so a small host never runs out of memory.
_rag_slot = threading.Semaphore(1)


def _save_imported_csv(current_user, label, writer):
    """Create the UploadedFile row for a dataset fetched from a remote source."""
    if _user_storage_used(current_user.id) >= MAX_USER_STORAGE_BYTES:
        raise DatasetImportError('Storage limit reached. Delete some files before importing more.')
    safe_label = secure_filename(label) or 'dataset'
    filename = f"{current_user.id}_{_stamp()}_{safe_label}.csv"
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    info = writer(file_path)
    record = UploadedFile(
        user_id=current_user.id,
        filename=filename,
        original_filename=f"{safe_label}.csv",
        file_path=file_path,
        file_size=os.path.getsize(file_path)
    )
    db.session.add(record)
    db.session.commit()
    return record, info


@app.route('/api/datasets/import', methods=['POST'])
@token_required
@limiter.limit('10 per hour')
def import_dataset(current_user):
    """Import a dataset from Hugging Face or Kaggle as an uploaded file."""
    data = _body()
    source = data.get('source')
    dataset_id = normalize_dataset_id(source, str(data.get('dataset') or ''))
    try:
        max_rows = max(1, min(int(data.get('max_rows', 2000)), MAX_RAG_ROWS))
    except (TypeError, ValueError):
        max_rows = 2000

    if source not in ('huggingface', 'kaggle') or not dataset_id:
        return jsonify({'message': "Provide 'source' (huggingface or kaggle) and 'dataset'"}), 400

    try:
        if source == 'huggingface':
            record, info = _save_imported_csv(
                current_user, dataset_id.replace('/', '_'),
                lambda p: fetch_huggingface(dataset_id, p, data.get('config') or None,
                                            data.get('split') or None, max_rows))
        else:
            record, info = _save_imported_csv(
                current_user, dataset_id.replace('/', '_'),
                lambda p: fetch_kaggle(dataset_id, p, data.get('file') or None, max_rows))
    except DatasetImportError as e:
        return jsonify({'message': str(e)}), 400
    except Exception as e:
        logger.exception("Dataset import failed")
        return jsonify({'message': f'Import failed: {e}'}), 500

    return jsonify({'message': 'Dataset imported', 'file': record.to_dict(), 'import': info}), 201


@app.route('/api/files/<int:file_id>/columns', methods=['GET'])
@token_required
def file_columns(current_user, file_id):
    """List a file's columns and suggest which ones hold text for the RAG step."""
    file_obj = UploadedFile.query.filter_by(id=file_id, user_id=current_user.id).first()
    if not file_obj:
        return jsonify({'message': 'File not found'}), 404
    try:
        df = load_text_table(file_obj.file_path, 500)
        return jsonify(analyze_text_columns(df)), 200
    except Exception as e:
        return jsonify({'message': f'Could not read file: {scrub_error(e)}'}), 400


def _run_rag_job(job_id, file_path, output_path, cfg):
    """Background worker: runs the RAG preparation and records the result."""
    with app.app_context():
        job = db.session.get(ProcessedJob, job_id)
        file_obj = db.session.get(UploadedFile, job.file_id)
        _rag_slot.acquire()
        try:
            job.status = 'running'
            db.session.commit()

            def on_progress(p):
                job.metrics = json.dumps(p)
                db.session.commit()

            df = load_text_table(file_path, cfg.max_rows)
            stats = build_rag_dataset(df, cfg, output_path, progress_cb=on_progress)

            job.metrics = json.dumps(stats)
            job.output_path = output_path
            job.status = 'completed'
            file_obj.status = 'processed'
        except Exception as e:
            logger.exception("RAG job %s failed", job_id)
            db.session.rollback()
            job = db.session.get(ProcessedJob, job_id)
            file_obj = db.session.get(UploadedFile, job.file_id)
            job.status = 'failed'
            job.error_message = scrub_error(e)
            file_obj.status = 'error'
            _remove_quietly(output_path)  # never leave a half-written file behind
        finally:
            _rag_slot.release()
            db.session.commit()
            db.session.remove()


@app.route('/api/embedders', methods=['GET'])
@token_required
def list_embedders(current_user):
    """Which embedders this server can actually run right now."""
    info = scratch_model_info()
    return jsonify({
        'scratch': {'available': info is not None,
                    'dim': info.get('dim') if info else None,
                    'name': info.get('name') if info else None},
        'hf_api': {'available': bool(os.environ.get('HF_TOKEN'))}
    }), 200


@app.route('/api/rag/process', methods=['POST'])
@token_required
@limiter.limit('20 per hour')
def process_rag(current_user):
    """Start a background job: clean -> tokenize/chunk -> embed -> JSONL."""
    data = _body()
    if not data.get('file_id'):
        return jsonify({'message': 'File ID is required'}), 400

    file_obj = UploadedFile.query.filter_by(id=data['file_id'], user_id=current_user.id).first()
    if not file_obj:
        return jsonify({'message': 'File not found'}), 404

    busy = ProcessedJob.query.filter(
        ProcessedJob.user_id == current_user.id,
        ProcessedJob.status.in_(['pending', 'running'])
    ).first()
    if busy:
        return jsonify({'message': 'You already have a job running. Wait for it to finish.'}), 409

    if data.get('config') is not None and not isinstance(data['config'], dict):
        return jsonify({'message': "'config' must be an object"}), 400
    cfg = RAGConfig.from_dict(data.get('config'), max_rows_cap=MAX_RAG_ROWS)
    if cfg.embed and cfg.embedder == 'scratch' and scratch_model_info() is None:
        return jsonify({'message': 'No trained from-scratch embedder is installed on the server. '
                                   'Train one with tools/train_embedder.py, add it to backend/models/scratch-embedder, '
                                   'or turn embeddings off.'}), 400
    if cfg.embed and cfg.embedder == 'hf_api' and not os.environ.get('HF_TOKEN'):
        return jsonify({'message': 'The server has no HF_TOKEN set, so it cannot create embeddings. '
                                   'Add HF_TOKEN, or turn embeddings off to export chunks only.'}), 400

    output_path = os.path.join(app.config['PROCESSED_FOLDER'], f"rag_{_stamp()}_{file_obj.filename.rsplit('.', 1)[0]}.jsonl")
    job = ProcessedJob(
        user_id=current_user.id,
        file_id=file_obj.id,
        config=json.dumps({**asdict(cfg), 'mode': 'rag'}),
        metrics=json.dumps({'mode': 'rag', 'progress': 0}),
        status='pending'
    )
    file_obj.status = 'processing'
    db.session.add(job)
    db.session.commit()

    threading.Thread(target=_run_rag_job, args=(job.id, file_obj.file_path, output_path, cfg), daemon=True).start()
    return jsonify({'message': 'RAG job started', 'job': job.to_dict()}), 202


# ============================================================================
# API ROUTES - CONFIGURATION
# ============================================================================

@app.route('/api/config/defaults', methods=['GET'])
def get_default_config():
    """Get default pipeline configuration"""
    default_config = {
        'validation_enabled': True,
        'outlier_detection_enabled': True,
        'normalization_enabled': True,
        'feature_engineering_enabled': True,
        'missing_value_threshold': 0.3,
        'outlier_std_threshold': 3.0
    }
    return jsonify(default_config), 200


@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat()
    }), 200


# ============================================================================
# INITIALIZATION
# ============================================================================

# Runs on import so it also works under gunicorn (which never executes __main__)
with app.app_context():
    db.create_all()
    # A restart kills background threads, so jobs left "running" can never finish.
    stale = ProcessedJob.query.filter(ProcessedJob.status.in_(['pending', 'running'])).all()
    for stale_job in stale:
        stale_job.status = 'failed'
        stale_job.error_message = 'Interrupted by a server restart. Please run it again.'
        if stale_job.file_id:
            f = db.session.get(UploadedFile, stale_job.file_id)
            if f:
                f.status = 'uploaded'
    db.session.commit()


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
