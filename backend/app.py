from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
import jwt
from datetime import datetime, timedelta
from functools import wraps
import pandas as pd
import os
import io
import json
from pathlib import Path
import traceback

# Import your original pipeline
import sys
sys.path.append('.')
from industrial_pipeline import (
    IndustrialDataPipeline, 
    PipelineConfig, 
    DatabaseConfig,
    StreamConfig,
    MLConfig,
    MonitoringConfig,
    DataAnalyzer  # Import the smart analyzer
)

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'your-secret-key-change-this-in-production')
# Use PostgreSQL in production, SQLite locally
DATABASE_URL = os.environ.get('DATABASE_URL', 'sqlite:///pipeline_users.db')

# Fix for SQLAlchemy (Render uses postgres:// but SQLAlchemy needs postgresql://)
if DATABASE_URL and DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['UPLOAD_FOLDER'] = './uploads'
app.config['PROCESSED_FOLDER'] = './processed'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max file size

db = SQLAlchemy(app)

# Create necessary directories
Path(app.config['UPLOAD_FOLDER']).mkdir(parents=True, exist_ok=True)
Path(app.config['PROCESSED_FOLDER']).mkdir(parents=True, exist_ok=True)
FRONTEND_URL = os.environ.get('FRONTEND_URL', 'http://localhost:3000')

CORS(app, resources={
    r"/api/*": {
        "origins": [
            "http://localhost:3000",
            "http://localhost:5173",
            FRONTEND_URL,
            "https://pipeline-kappa-topaz.vercel.app/"  # Allow all Vercel deployments
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
            current_user = User.query.get(data['user_id'])
            
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
def register():
    """Register a new user"""
    data = request.get_json()
    
    if not data or not data.get('username') or not data.get('email') or not data.get('password'):
        return jsonify({'message': 'Missing required fields'}), 400
    
    if User.query.filter_by(username=data['username']).first():
        return jsonify({'message': 'Username already exists'}), 400
    
    if User.query.filter_by(email=data['email']).first():
        return jsonify({'message': 'Email already exists'}), 400
    
    user = User(username=data['username'], email=data['email'])
    user.set_password(data['password'])
    
    db.session.add(user)
    db.session.commit()
    
    return jsonify({
        'message': 'User registered successfully',
        'user': user.to_dict()
    }), 201


@app.route('/api/login', methods=['POST'])
def login():
    """Login user and return JWT token"""
    data = request.get_json()
    
    if not data or not data.get('username') or not data.get('password'):
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

@app.route('/api/files/upload', methods=['POST'])
@token_required
def upload_file(current_user):
    """Upload a file"""
    if 'file' not in request.files:
        return jsonify({'message': 'No file provided'}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({'message': 'No file selected'}), 400
    
    if not file.filename.lower().endswith(('.csv', '.txt')):
        return jsonify({'message': 'Only CSV and TXT files are allowed'}), 400
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    original_filename = secure_filename(file.filename)
    filename = f"{current_user.id}_{timestamp}_{original_filename}"
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    
    file.save(file_path)
    file_size = os.path.getsize(file_path)
    
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
    
    if os.path.exists(file_obj.file_path):
        os.remove(file_obj.file_path)
    
    db.session.delete(file_obj)
    db.session.commit()
    
    return jsonify({'message': 'File deleted successfully'}), 200


# ============================================================================
# API ROUTES - PIPELINE PROCESSING WITH SMART DETECTION
# ============================================================================

@app.route('/api/process', methods=['POST'])
@token_required
def process_data(current_user):
    """Process uploaded file through pipeline with smart parameter detection"""
    data = request.get_json()
    
    if not data or not data.get('file_id'):
        return jsonify({'message': 'File ID is required'}), 400
    
    file_obj = UploadedFile.query.filter_by(id=data['file_id'], user_id=current_user.id).first()
    
    if not file_obj:
        return jsonify({'message': 'File not found'}), 404
    
    file_obj.status = 'processing'
    db.session.commit()
    
    try:
        # Read the uploaded file
        input_data = pd.read_csv(file_obj.file_path)
        
        # ============ SMART PARAMETER DETECTION ============
        # Analyze the uploaded data
        analysis = DataAnalyzer.analyze_dataframe(input_data)
        
        # Get smart configuration suggestions
        smart_config = DataAnalyzer.suggest_pipeline_config(analysis)
        
        # Debug output - show what was detected
        print(f"\n{'='*70}")
        print(f"SMART PARAMETER DETECTION")
        print(f"{'='*70}")
        print(f"File: {file_obj.original_filename}")
        print(f"User: {current_user.username}")
        print(f"\n--- DATA ANALYSIS ---")
        print(f"Rows: {analysis['total_rows']}")
        print(f"Columns: {analysis['total_columns']}")
        print(f"Column Names: {analysis['columns']}")
        print(f"Numeric Columns: {analysis['numeric_columns']}")
        print(f"Categorical Columns: {analysis['categorical_columns']}")
        print(f"DateTime Columns: {analysis['datetime_columns']}")
        
        if analysis['missing_values']:
            print(f"\n--- MISSING VALUES ---")
            for col, pct in analysis['missing_values'].items():
                print(f"  {col}: {pct:.1%}")
        
        print(f"\n--- DETECTED FEATURES ---")
        for feature, present in analysis['detected_features'].items():
            if present:
                print(f"  ✓ {feature}")
        
        print(f"\n--- SMART CONFIGURATION ---")
        for key, value in smart_config.items():
            if key != 'reasons':
                print(f"  {key}: {value}")
        
        if smart_config['reasons']:
            print(f"\n--- CONFIGURATION REASONS ---")
            for reason in smart_config['reasons']:
                print(f"  • {reason}")
        
        print(f"{'='*70}\n")
        
        # Get user's configuration or use smart suggestions
        pipeline_config_data = data.get('config', {})
        
        # Merge user config with smart suggestions (user config takes priority)
        final_config = {
            'validation_enabled': pipeline_config_data.get('validation_enabled', smart_config['validation_enabled']),
            'outlier_detection_enabled': pipeline_config_data.get('outlier_detection_enabled', smart_config['outlier_detection_enabled']),
            'normalization_enabled': pipeline_config_data.get('normalization_enabled', smart_config['normalization_enabled']),
            'feature_engineering_enabled': pipeline_config_data.get('feature_engineering_enabled', smart_config['feature_engineering_enabled']),
            'missing_value_threshold': pipeline_config_data.get('missing_value_threshold', smart_config['missing_value_threshold']),
            'outlier_std_threshold': pipeline_config_data.get('outlier_std_threshold', smart_config['outlier_std_threshold'])
        }
        
        # Create pipeline configuration
        config = PipelineConfig(
            batch_size=len(input_data),
            validation_enabled=final_config['validation_enabled'],
            outlier_detection_enabled=final_config['outlier_detection_enabled'],
            normalization_enabled=final_config['normalization_enabled'],
            feature_engineering_enabled=final_config['feature_engineering_enabled'],
            missing_value_threshold=final_config['missing_value_threshold'],
            outlier_std_threshold=final_config['outlier_std_threshold'],
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
        
        # Debug output after processing
        print(f"\n{'='*70}")
        print(f"PROCESSING COMPLETED")
        print(f"{'='*70}")
        print(f"Input Records: {metrics.total_records}")
        print(f"Valid Records: {metrics.valid_records}")
        print(f"Invalid Records: {metrics.invalid_records}")
        print(f"Processing Time: {metrics.processing_time:.2f}s")
        print(f"Data Quality: {metrics.data_quality_score:.2%}")
        print(f"Throughput: {metrics.throughput:.0f} records/sec")
        print(f"\nOutput Shape: {len(processed_data)} rows × {len(processed_data.columns)} columns")
        print(f"Output Columns (first 10): {list(processed_data.columns)[:10]}...")
        print(f"{'='*70}\n")
        
        # Save processed data
        output_filename = f"processed_{file_obj.filename}"
        output_path = os.path.join(app.config['PROCESSED_FOLDER'], output_filename)
        processed_data.to_csv(output_path, index=False)
        
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
        # Enhanced error handling with traceback
        error_details = traceback.format_exc()
        
        print(f"\n{'='*70}")
        print(f"PROCESSING ERROR")
        print(f"{'='*70}")
        print(f"Error: {str(e)}")
        print(f"\nFull Traceback:")
        print(error_details)
        print(f"{'='*70}\n")
        
        file_obj.status = 'error'
        
        job = ProcessedJob(
            user_id=current_user.id,
            file_id=file_obj.id,
            status='failed',
            error_message=str(e)
        )
        
        db.session.add(job)
        db.session.commit()
        
        return jsonify({
            'message': 'Processing failed',
            'error': str(e)
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
            file_obj = UploadedFile.query.get(job.file_id)
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
    
    return send_file(
        job.output_path,
        as_attachment=True,
        download_name=f"processed_{job.id}.csv"
    )


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

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    print("\n" + "="*70)
    print("Industrial Data Pipeline API Server")
    print("="*70)
    print("Server starting on http://0.0.0.0:5000")
    print("Features:")
    print("  ✓ Smart Parameter Detection")
    print("  ✓ User Authentication")
    print("  ✓ File Upload & Processing")
    print("  ✓ Automatic Data Analysis")
    print("="*70 + "\n")
    app.run(debug=True, host='0.0.0.0', port=5000)