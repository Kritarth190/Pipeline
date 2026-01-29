"""
Production-Grade Industrial Data Pipeline
Features: Database Integration, Stream Processing, ML Framework Integration, Monitoring
UPDATED: Smart Parameter Detection for Any CSV File
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any, Callable
import json
import logging
from dataclasses import dataclass, asdict, field
from abc import ABC, abstractmethod
import pickle
import hashlib
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
import threading
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    postgres_url: str = "postgresql://user:password@localhost:5432/industrial_data"
    mongodb_url: str = "mongodb://localhost:27017/"
    use_postgres: bool = False
    use_mongodb: bool = False
    table_name: str = "sensor_data"


@dataclass
class StreamConfig:
    """Stream processing configuration"""
    enabled: bool = False
    buffer_size: int = 1000
    flush_interval: int = 5
    num_workers: int = 4


@dataclass
class MLConfig:
    """ML framework configuration"""
    mlflow_tracking_uri: str = "http://localhost:5000"
    experiment_name: str = "industrial_pipeline"
    model_registry_name: str = "industrial_model"
    auto_log: bool = True
    save_artifacts: bool = True


@dataclass
class MonitoringConfig:
    """Monitoring configuration"""
    prometheus_port: int = 8000
    enable_metrics: bool = False
    alert_thresholds: Dict[str, float] = field(default_factory=lambda: {
        'error_rate': 0.05,
        'processing_time': 60.0,
        'data_quality_score': 0.8
    })


@dataclass
class PipelineConfig:
    """Main pipeline configuration"""
    batch_size: int = 1000
    validation_enabled: bool = True
    outlier_detection_enabled: bool = True
    normalization_enabled: bool = True
    feature_engineering_enabled: bool = True
    missing_value_threshold: float = 0.3
    outlier_std_threshold: float = 3.0
    enable_versioning: bool = True
    version_storage_path: str = "./data_versions"
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    streaming: StreamConfig = field(default_factory=StreamConfig)
    ml: MLConfig = field(default_factory=MLConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)


@dataclass
class PipelineMetrics:
    """Enhanced metrics with monitoring"""
    total_records: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    processing_time: float = 0.0
    stage_times: Dict[str, float] = field(default_factory=dict)
    data_quality_score: float = 1.0
    error_count: int = 0
    throughput: float = 0.0


# ============================================================================
# SMART DATA ANALYZER
# ============================================================================

class DataAnalyzer:
    """Automatically analyze and detect data characteristics"""
    
    @staticmethod
    def analyze_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze DataFrame and detect its characteristics"""
        analysis = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'columns': list(df.columns),
            'numeric_columns': [],
            'categorical_columns': [],
            'datetime_columns': [],
            'missing_values': {},
            'data_types': {},
            'column_stats': {},
            'detected_features': {}
        }
        
        for col in df.columns:
            # Detect data types
            analysis['data_types'][col] = str(df[col].dtype)
            
            # Check for missing values
            missing_pct = df[col].isna().sum() / len(df)
            if missing_pct > 0:
                analysis['missing_values'][col] = missing_pct
            
            # Categorize columns
            if pd.api.types.is_numeric_dtype(df[col]):
                analysis['numeric_columns'].append(col)
                # Get statistics for numeric columns
                analysis['column_stats'][col] = {
                    'min': float(df[col].min()) if not df[col].isna().all() else None,
                    'max': float(df[col].max()) if not df[col].isna().all() else None,
                    'mean': float(df[col].mean()) if not df[col].isna().all() else None,
                    'std': float(df[col].std()) if not df[col].isna().all() else None,
                    'median': float(df[col].median()) if not df[col].isna().all() else None
                }
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                analysis['datetime_columns'].append(col)
            else:
                # Try to convert to datetime
                try:
                    pd.to_datetime(df[col], errors='raise')
                    analysis['datetime_columns'].append(col)
                except:
                    analysis['categorical_columns'].append(col)
                    # Get unique values for categorical
                    unique_vals = df[col].nunique()
                    analysis['column_stats'][col] = {
                        'unique_values': int(unique_vals),
                        'most_common': str(df[col].mode()[0]) if len(df[col].mode()) > 0 else None
                    }
        
        # Detect common industrial data patterns
        analysis['detected_features'] = DataAnalyzer._detect_features(df, analysis)
        
        return analysis
    
    @staticmethod
    def _detect_features(df: pd.DataFrame, analysis: Dict) -> Dict[str, bool]:
        """Detect common industrial data features"""
        features = {
            'has_timestamp': False,
            'has_sensor_data': False,
            'has_machine_id': False,
            'has_status': False,
            'has_temperature': False,
            'has_pressure': False,
            'is_time_series': False
        }
        
        columns_lower = [col.lower() for col in df.columns]
        
        # Check for timestamp
        timestamp_keywords = ['timestamp', 'time', 'date', 'datetime']
        features['has_timestamp'] = any(kw in col for col in columns_lower for kw in timestamp_keywords)
        
        # Check for machine/device ID
        id_keywords = ['machine', 'device', 'sensor', 'equipment', 'id']
        features['has_machine_id'] = any(kw in col for col in columns_lower for kw in id_keywords)
        
        # Check for sensor data
        sensor_keywords = ['temperature', 'pressure', 'vibration', 'rpm', 'speed', 'voltage', 'current']
        features['has_sensor_data'] = any(kw in col for col in columns_lower for kw in sensor_keywords)
        
        # Check for specific sensors
        features['has_temperature'] = any('temp' in col for col in columns_lower)
        features['has_pressure'] = any('press' in col for col in columns_lower)
        
        # Check for status/state
        status_keywords = ['status', 'state', 'mode', 'condition']
        features['has_status'] = any(kw in col for col in columns_lower for kw in status_keywords)
        
        # Check if it's time series data
        features['is_time_series'] = features['has_timestamp'] and len(df) > 10
        
        return features
    
    @staticmethod
    def suggest_pipeline_config(analysis: Dict) -> Dict[str, Any]:
        """Suggest optimal pipeline configuration based on data analysis"""
        suggestions = {
            'validation_enabled': True,
            'outlier_detection_enabled': False,
            'normalization_enabled': False,
            'feature_engineering_enabled': False,
            'missing_value_threshold': 0.3,
            'outlier_std_threshold': 3.0,
            'reasons': []
        }
        
        # Enable outlier detection if we have numeric sensor data
        if len(analysis['numeric_columns']) > 3:
            suggestions['outlier_detection_enabled'] = True
            suggestions['reasons'].append("Multiple numeric columns detected - enabling outlier detection")
        
        # Enable normalization if we have sensor data with different scales
        if analysis['detected_features'].get('has_sensor_data'):
            suggestions['normalization_enabled'] = True
            suggestions['reasons'].append("Sensor data detected - enabling normalization")
        
        # Enable feature engineering for time series data
        if analysis['detected_features'].get('is_time_series'):
            suggestions['feature_engineering_enabled'] = True
            suggestions['reasons'].append("Time series data detected - enabling feature engineering")
        
        # Adjust missing value threshold based on data quality
        missing_cols = len(analysis['missing_values'])
        if missing_cols > 0:
            avg_missing = sum(analysis['missing_values'].values()) / missing_cols
            if avg_missing > 0.2:
                suggestions['missing_value_threshold'] = 0.5
                suggestions['reasons'].append(f"High missing values detected ({avg_missing:.1%}) - adjusting threshold")
        
        return suggestions


# ============================================================================
# DATA VERSIONING
# ============================================================================

class DataVersionManager:
    """Version control for processed data"""
    
    def __init__(self, storage_path: str):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.manifest_file = self.storage_path / "manifest.json"
        self.manifest = self._load_manifest()
    
    def _load_manifest(self) -> Dict:
        """Load version manifest"""
        if self.manifest_file.exists():
            with open(self.manifest_file, 'r') as f:
                return json.load(f)
        return {'versions': []}
    
    def _save_manifest(self):
        """Save version manifest"""
        with open(self.manifest_file, 'w') as f:
            json.dump(self.manifest, f, indent=2)
    
    def save_version(self, data: pd.DataFrame, metadata: Dict) -> str:
        """Save a new data version"""
        data_hash = hashlib.md5(str(data.values.tobytes()).encode()).hexdigest()
        version_id = f"v_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{data_hash[:8]}"
        
        version_path = self.storage_path / version_id
        version_path.mkdir(exist_ok=True)
        
        data.to_csv(version_path / "data.csv", index=False)
        
        version_metadata = {
            'version_id': version_id,
            'timestamp': datetime.now().isoformat(),
            'records': len(data),
            'columns': list(data.columns),
            'hash': data_hash,
            **metadata
        }
        
        with open(version_path / "metadata.json", 'w') as f:
            json.dump(version_metadata, f, indent=2)
        
        self.manifest['versions'].append(version_metadata)
        self._save_manifest()
        
        logger.info(f"Saved data version: {version_id}")
        return version_id
    
    def load_version(self, version_id: str) -> Tuple[pd.DataFrame, Dict]:
        """Load a specific data version"""
        version_path = self.storage_path / version_id
        
        if not version_path.exists():
            raise ValueError(f"Version {version_id} not found")
        
        data = pd.read_csv(version_path / "data.csv")
        
        with open(version_path / "metadata.json", 'r') as f:
            metadata = json.load(f)
        
        return data, metadata
    
    def list_versions(self) -> List[Dict]:
        """List all available versions"""
        return self.manifest['versions']


# ============================================================================
# PIPELINE STAGES
# ============================================================================

class PipelineStage(ABC):
    """Abstract base class for pipeline stages"""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"Stage.{name}")
    
    @abstractmethod
    def process(self, data: pd.DataFrame, config: PipelineConfig) -> pd.DataFrame:
        """Process data through this stage"""
        pass
    
    def log_info(self, message: str):
        self.logger.info(f"[{self.name}] {message}")
    
    def log_error(self, message: str):
        self.logger.error(f"[{self.name}] {message}")


class DataIngestionStage(PipelineStage):
    """Smart data ingestion with automatic parameter detection"""
    
    def __init__(self):
        super().__init__("Data Ingestion")
        self.data_analysis = None
    
    def process(self, data: pd.DataFrame, config: PipelineConfig) -> pd.DataFrame:
        # Check if user provided real data
        if data is not None and not data.empty:
            self.log_info(f"Processing uploaded data: {len(data)} rows × {len(data.columns)} columns")
            
            # Analyze the data
            self.data_analysis = DataAnalyzer.analyze_dataframe(data)
            
            # Log detected features
            self.log_info(f"Detected columns: {self.data_analysis['columns']}")
            self.log_info(f"Numeric columns: {len(self.data_analysis['numeric_columns'])}")
            self.log_info(f"Categorical columns: {len(self.data_analysis['categorical_columns'])}")
            self.log_info(f"DateTime columns: {len(self.data_analysis['datetime_columns'])}")
            
            # Log detected features
            features = self.data_analysis['detected_features']
            detected = [k.replace('has_', '').replace('is_', '') for k, v in features.items() if v]
            if detected:
                self.log_info(f"Detected features: {', '.join(detected)}")
            
            # Log data quality
            if self.data_analysis['missing_values']:
                self.log_info(f"Columns with missing values: {len(self.data_analysis['missing_values'])}")
            
            return data
        
        # Only generate synthetic data if no data provided
        self.log_info("No data provided - generating synthetic sample data")
        data = self._generate_sample_data(config.batch_size)
        self.log_info(f"Generated {len(data)} synthetic records")
        return data
    
    def _generate_sample_data(self, n_records: int) -> pd.DataFrame:
        """Generate realistic industrial sensor data"""
        np.random.seed(int(time.time()) % 2**32)
        
        modes = np.random.choice(['normal', 'high_load', 'maintenance'], n_records, p=[0.7, 0.2, 0.1])
        
        base_temp = np.where(modes == 'normal', 75, np.where(modes == 'high_load', 85, 65))
        base_pressure = np.where(modes == 'normal', 30, np.where(modes == 'high_load', 40, 20))
        
        return pd.DataFrame({
            'timestamp': pd.date_range(start=datetime.now() - timedelta(hours=1), periods=n_records, freq='1s'),
            'machine_id': np.random.choice(['M001', 'M002', 'M003', 'M004', 'M005'], n_records),
            'temperature': base_temp + np.random.normal(0, 5, n_records),
            'pressure': base_pressure + np.random.normal(0, 3, n_records),
            'vibration': np.random.exponential(2, n_records) * (1 + (modes == 'high_load') * 0.5),
            'rpm': 1500 + np.random.normal(0, 100, n_records) + (modes == 'high_load') * 200,
            'power_consumption': 150 + np.random.normal(0, 15, n_records) + (modes == 'high_load') * 50,
            'humidity': np.random.uniform(30, 70, n_records),
            'oil_pressure': np.random.normal(45, 5, n_records),
            'operating_mode': modes,
            'status': np.random.choice(['normal', 'warning', 'critical'], n_records, p=[0.85, 0.12, 0.03])
        })


class ValidationStage(PipelineStage):
    """Smart validation with automatic column detection"""
    
    def __init__(self):
        super().__init__("Validation")
        self.quality_score = 1.0
    
    def process(self, data: pd.DataFrame, config: PipelineConfig) -> pd.DataFrame:
        if not config.validation_enabled:
            return data
        
        initial_count = len(data)
        quality_issues = []
        
        self.log_info(f"Validating {initial_count} records")
        
        # Detect and convert datetime columns
        for col in data.columns:
            if 'time' in col.lower() or 'date' in col.lower():
                try:
                    data[col] = pd.to_datetime(data[col], errors='coerce')
                    invalid = data[col].isna().sum()
                    if invalid > 0:
                        quality_issues.append(f"{col}_invalid:{invalid}")
                        self.log_info(f"Found {invalid} invalid datetime values in {col}")
                except:
                    pass
        
        # Validate numeric columns (flexible - works with any numeric data)
        numeric_cols = data.select_dtypes(include=[np.number]).columns
        
        valid_mask = pd.Series([True] * len(data))
        
        for col in numeric_cols:
            # Check for inf and extreme outliers
            col_valid = np.isfinite(data[col])
            
            # Check for reasonable ranges (very permissive)
            if col_valid.sum() > 0:
                q1 = data.loc[col_valid, col].quantile(0.01)
                q99 = data.loc[col_valid, col].quantile(0.99)
                range_span = q99 - q1
                
                # Only flag extreme outliers (beyond 100x the normal range)
                if range_span > 0:
                    extreme_low = q1 - (range_span * 100)
                    extreme_high = q99 + (range_span * 100)
                    col_valid &= (data[col] >= extreme_low) & (data[col] <= extreme_high)
            
            invalid_count = (~col_valid).sum()
            if invalid_count > 0:
                quality_issues.append(f"{col}_invalid:{invalid_count}")
                self.log_info(f"Found {invalid_count} invalid values in {col}")
            
            valid_mask &= col_valid
        
        # Remove invalid rows
        data = data[valid_mask].copy()
        removed = initial_count - len(data)
        
        self.quality_score = len(data) / initial_count if initial_count > 0 else 1.0
        
        self.log_info(f"Validation complete: Quality score {self.quality_score:.3f}")
        if removed > 0:
            self.log_info(f"Removed {removed} invalid records ({removed/initial_count*100:.1f}%)")
        
        return data


class DataCleaningStage(PipelineStage):
    """Smart data cleaning with advanced imputation"""
    
    def __init__(self):
        super().__init__("Data Cleaning")
    
    def process(self, data: pd.DataFrame, config: PipelineConfig) -> pd.DataFrame:
        self.log_info(f"Cleaning {len(data)} records")
        
        data = self._handle_missing_values(data, config)
        
        duplicates = data.duplicated().sum()
        if duplicates > 0:
            self.log_info(f"Removing {duplicates} duplicate records")
            data = data.drop_duplicates().copy()
        
        return data
    
    def _handle_missing_values(self, data: pd.DataFrame, config: PipelineConfig) -> pd.DataFrame:
        """Smart missing value handling for any dataset"""
        numeric_cols = data.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            missing_pct = data[col].isna().sum() / len(data)
            
            if missing_pct > config.missing_value_threshold:
                self.log_info(f"Dropping column {col} (missing: {missing_pct:.1%})")
                data = data.drop(columns=[col])
            elif missing_pct > 0:
                # Use forward fill for time-series-like data, median for others
                if 'time' in col.lower() or 'date' in col.lower():
                    data[col] = data[col].ffill().bfill()
                else:
                    # Check if data looks like sensor/sequential data
                    try:
                        is_sequential = data[col].autocorr() > 0.5 if len(data) > 10 else False
                    except:
                        is_sequential = False
                    
                    if is_sequential:
                        data[col] = data[col].ffill().bfill()
                        self.log_info(f"Forward-filled {col} (sequential data, missing: {missing_pct:.1%})")
                    else:
                        median_val = data[col].median()
                        data[col] = data[col].fillna(median_val)
                        self.log_info(f"Median-filled {col} (missing: {missing_pct:.1%})")
        
        return data


class OutlierDetectionStage(PipelineStage):
    """Statistical outlier detection and removal"""
    
    def __init__(self):
        super().__init__("Outlier Detection")
    
    def process(self, data: pd.DataFrame, config: PipelineConfig) -> pd.DataFrame:
        if not config.outlier_detection_enabled:
            return data
        
        initial_count = len(data)
        numeric_cols = data.select_dtypes(include=[np.number]).columns
        
        # Reset index to ensure alignment
        data = data.reset_index(drop=True)
        
        # Initialize mask with correct index
        outlier_mask = pd.Series([False] * len(data), index=data.index)
        
        for col in numeric_cols:
            if col in data.columns and data[col].std() > 0:
                z_scores = np.abs((data[col] - data[col].mean()) / data[col].std())
                col_outliers = z_scores > config.outlier_std_threshold
                outlier_count = col_outliers.sum()
                
                if outlier_count > 0:
                    self.log_info(f"{col}: {outlier_count} outliers detected")
                    outlier_mask = outlier_mask | col_outliers
        
        data = data[~outlier_mask].copy()
        removed = initial_count - len(data)
        
        if removed > 0:
            self.log_info(f"Removed {removed} outlier records ({removed/initial_count*100:.1f}%)")
        
        return data


class NormalizationStage(PipelineStage):
    """Smart normalization for any numeric data"""
    
    def __init__(self):
        super().__init__("Normalization")
        self.scalers = {}
    
    def process(self, data: pd.DataFrame, config: PipelineConfig) -> pd.DataFrame:
        if not config.normalization_enabled:
            return data
        
        # Get all numeric columns automatically
        numeric_cols = data.select_dtypes(include=[np.number]).columns
        
        self.log_info(f"Normalizing {len(numeric_cols)} numeric columns")
        
        for col in numeric_cols:
            if col in data.columns:
                # Skip if all values are the same
                if data[col].nunique() <= 1:
                    continue
                
                min_val = data[col].min()
                max_val = data[col].max()
                
                if max_val > min_val:
                    data[f'{col}_normalized'] = (data[col] - min_val) / (max_val - min_val)
                    self.scalers[col] = {'min': min_val, 'max': max_val}
                    self.log_info(f"Normalized {col}: [{min_val:.2f}, {max_val:.2f}]")
        
        return data


class FeatureEngineeringStage(PipelineStage):
    """Smart feature engineering based on detected columns"""
    
    def __init__(self):
        super().__init__("Feature Engineering")
    
    def process(self, data: pd.DataFrame, config: PipelineConfig) -> pd.DataFrame:
        if not config.feature_engineering_enabled:
            return data
        
        self.log_info("Creating engineered features")
        initial_cols = len(data.columns)
        
        # Time-based features (detect datetime columns automatically)
        datetime_cols = data.select_dtypes(include=['datetime64']).columns
        for col in datetime_cols:
            data[f'{col}_hour'] = data[col].dt.hour
            data[f'{col}_day_of_week'] = data[col].dt.dayofweek
            data[f'{col}_is_weekend'] = data[col].dt.dayofweek.isin([5, 6]).astype(int)
            self.log_info(f"Created time features from {col}")
        
        # Also check for string columns that might be datetime
        for col in data.select_dtypes(include=['object']).columns:
            if 'time' in col.lower() or 'date' in col.lower():
                try:
                    dt_col = pd.to_datetime(data[col], errors='coerce')
                    if dt_col.notna().sum() > len(data) * 0.8:  # If 80% valid
                        data[f'{col}_hour'] = dt_col.dt.hour
                        data[f'{col}_day_of_week'] = dt_col.dt.dayofweek
                        self.log_info(f"Created time features from {col}")
                except:
                    pass
        
        # Interaction features for numeric columns
        numeric_cols = data.select_dtypes(include=[np.number]).columns.tolist()
        
        # Create ratios for related columns (limit to avoid explosion)
        for i, col1 in enumerate(numeric_cols[:5]):  # Limit to first 5 columns
            for col2 in numeric_cols[i+1:i+3]:  # Create max 2 ratios per column
                if col2 in data.columns:
                    try:
                        data[f'{col1}_{col2}_ratio'] = data[col1] / (data[col2] + 1e-6)
                    except:
                        pass
        
        # Rolling statistics for first few numeric columns
        if len(data) >= 10:
            for col in numeric_cols[:5]:  # Limit to first 5 to avoid too many features
                try:
                    window_size = min(10, len(data)//2)
                    data[f'{col}_rolling_mean'] = data[col].rolling(window=window_size, min_periods=1).mean()
                    data[f'{col}_rolling_std'] = data[col].rolling(window=window_size, min_periods=1).std()
                except:
                    pass
        
        # Categorical encoding (automatic)
        categorical_cols = data.select_dtypes(include=['object']).columns
        for col in categorical_cols:
            if data[col].nunique() < 20:  # Only encode if reasonable number of categories
                try:
                    data[f'{col}_encoded'] = pd.Categorical(data[col]).codes
                    self.log_info(f"Encoded categorical column: {col}")
                except:
                    pass
        
        new_features = len(data.columns) - initial_cols
        self.log_info(f"Created {new_features} new features")
        
        return data


# ============================================================================
# DATABASE CONNECTORS
# ============================================================================

class DatabaseConnector(ABC):
    """Abstract database connector"""
    
    @abstractmethod
    def connect(self):
        pass
    
    @abstractmethod
    def save_data(self, data: pd.DataFrame, table_name: str):
        pass
    
    @abstractmethod
    def load_data(self, query: str) -> pd.DataFrame:
        pass


class PostgreSQLConnector(DatabaseConnector):
    """PostgreSQL database connector"""
    
    def __init__(self, connection_url: str):
        self.connection_url = connection_url
        self.engine = None
    
    def connect(self):
        try:
            import sqlalchemy
            self.engine = sqlalchemy.create_engine(self.connection_url)
            logger.info("Connected to PostgreSQL")
        except ImportError:
            logger.info("SQLAlchemy not installed. PostgreSQL features disabled.")
            self.engine = None
        except Exception as e:
            logger.error(f"PostgreSQL connection error: {e}")
            self.engine = None
    
    def save_data(self, data: pd.DataFrame, table_name: str):
        if self.engine:
            data.to_sql(table_name, self.engine, if_exists='append', index=False)
            logger.info(f"Saved {len(data)} records to PostgreSQL table '{table_name}'")
        else:
            logger.info("PostgreSQL not connected. Skipping save.")
    
    def load_data(self, query: str) -> pd.DataFrame:
        if self.engine:
            return pd.read_sql(query, self.engine)
        else:
            logger.warning("PostgreSQL not connected. Returning empty DataFrame.")
            return pd.DataFrame()


class MongoDBConnector(DatabaseConnector):
    """MongoDB database connector"""
    
    def __init__(self, connection_url: str):
        self.connection_url = connection_url
        self.client = None
        self.db = None
    
    def connect(self):
        try:
            import pymongo
            self.client = pymongo.MongoClient(self.connection_url)
            self.db = self.client['industrial_data']
            logger.info("Connected to MongoDB")
        except ImportError:
            logger.info("PyMongo not installed. MongoDB features disabled.")
            self.client = None
            self.db = None
        except Exception as e:
            logger.error(f"MongoDB connection error: {e}")
            self.client = None
            self.db = None
    
    def save_data(self, data: pd.DataFrame, collection_name: str):
        if self.db:
            records = data.to_dict('records')
            self.db[collection_name].insert_many(records)
            logger.info(f"Saved {len(records)} records to MongoDB collection '{collection_name}'")
        else:
            logger.info("MongoDB not connected. Skipping save.")
    
    def load_data(self, query: Dict) -> pd.DataFrame:
        if self.db:
            cursor = self.db['sensor_data'].find(query)
            return pd.DataFrame(list(cursor))
        else:
            logger.warning("MongoDB not connected. Returning empty DataFrame.")
            return pd.DataFrame()


# ============================================================================
# STREAM PROCESSING
# ============================================================================

class StreamProcessor:
    """Real-time stream processing with buffer"""
    
    def __init__(self, config: StreamConfig, pipeline):
        self.config = config
        self.pipeline = pipeline
        self.buffer = queue.Queue(maxsize=config.buffer_size)
        self.workers = []
        self.running = False
    
    def start(self):
        """Start stream processing workers"""
        self.running = True
        
        for i in range(self.config.num_workers):
            worker = threading.Thread(target=self._worker, args=(i,), daemon=True)
            worker.start()
            self.workers.append(worker)
        
        # Start flush scheduler
        flush_thread = threading.Thread(target=self._flush_scheduler, daemon=True)
        flush_thread.start()
        
        logger.info(f"Started {self.config.num_workers} stream processing workers")
    
    def _worker(self, worker_id: int):
        """Worker thread for processing buffered data"""
        while self.running:
            try:
                batch = self.buffer.get(timeout=1)
                logger.info(f"Worker {worker_id} processing batch of {len(batch)} records")
                self.pipeline.run(batch)
                self.buffer.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
    
    def _flush_scheduler(self):
        """Periodic buffer flush"""
        while self.running:
            time.sleep(self.config.flush_interval)
            logger.info(f"Buffer status: {self.buffer.qsize()} / {self.config.buffer_size}")
    
    def add_data(self, data: pd.DataFrame):
        """Add data to processing buffer"""
        try:
            self.buffer.put(data, timeout=5)
        except queue.Full:
            logger.warning("Buffer full, data dropped")
    
    def stop(self):
        """Stop stream processing"""
        self.running = False
        self.buffer.join()
        logger.info("Stream processing stopped")


# ============================================================================
# ML INTEGRATION
# ============================================================================

class MLFlowIntegration:
    """MLflow experiment tracking integration"""
    
    def __init__(self, config: MLConfig):
        self.config = config
        self.mlflow_available = False
        self._setup()
    
    def _setup(self):
        """Setup MLflow tracking"""
        try:
            import mlflow
            self.mlflow = mlflow
            mlflow.set_tracking_uri(self.config.mlflow_tracking_uri)
            mlflow.set_experiment(self.config.experiment_name)
            self.mlflow_available = True
            logger.info(f"MLflow tracking configured: {self.config.mlflow_tracking_uri}")
        except ImportError:
            logger.info("MLflow not installed. ML tracking features disabled.")
            self.mlflow_available = False
        except Exception as e:
            logger.warning(f"MLflow setup error: {e}")
            self.mlflow_available = False
    
    def log_metrics(self, metrics: Dict[str, float], step: int = 0):
        """Log metrics to MLflow"""
        if self.mlflow_available:
            with self.mlflow.start_run():
                for key, value in metrics.items():
                    self.mlflow.log_metric(key, value, step=step)
    
    def log_artifact(self, filepath: str):
        """Log artifact to MLflow"""
        if self.mlflow_available:
            with self.mlflow.start_run():
                self.mlflow.log_artifact(filepath)
    
    def log_params(self, params: Dict[str, Any]):
        """Log parameters to MLflow"""
        if self.mlflow_available:
            with self.mlflow.start_run():
                self.mlflow.log_params(params)


# ============================================================================
# MONITORING
# ============================================================================

class PrometheusMonitor:
    """Prometheus metrics monitoring"""
    
    def __init__(self, config: MonitoringConfig):
        self.config = config
        self.metrics = {}
        self.prometheus_available = False
        self._setup()
    
    def _setup(self):
        """Setup Prometheus metrics"""
        try:
            from prometheus_client import Counter, Gauge, Histogram, start_http_server
            
            self.records_processed = Counter('pipeline_records_processed', 'Total records processed')
            self.processing_time = Histogram('pipeline_processing_seconds', 'Processing time')
            self.data_quality = Gauge('pipeline_data_quality', 'Data quality score')
            self.error_count = Counter('pipeline_errors', 'Pipeline errors')
            
            if self.config.enable_metrics:
                start_http_server(self.config.prometheus_port)
                logger.info(f"Prometheus metrics server started on port {self.config.prometheus_port}")
            
            self.prometheus_available = True
        except ImportError:
            logger.info("Prometheus client not installed. Monitoring features disabled.")
            self.prometheus_available = False
        except Exception as e:
            logger.warning(f"Prometheus setup error: {e}")
            self.prometheus_available = False
    
    def record_processing(self, count: int, time_seconds: float, quality: float):
        """Record processing metrics"""
        if self.prometheus_available:
            self.records_processed.inc(count)
            self.processing_time.observe(time_seconds)
            self.data_quality.set(quality)
    
    def record_error(self):
        """Record an error"""
        if self.prometheus_available:
            self.error_count.inc()
    
    def check_alerts(self, metrics: PipelineMetrics):
        """Check if metrics exceed alert thresholds"""
        alerts = []
        
        error_rate = metrics.error_count / max(metrics.total_records, 1)
        if error_rate > self.config.alert_thresholds['error_rate']:
            alerts.append(f"High error rate: {error_rate:.2%}")
        
        if metrics.processing_time > self.config.alert_thresholds['processing_time']:
            alerts.append(f"Slow processing: {metrics.processing_time:.1f}s")
        
        if metrics.data_quality_score < self.config.alert_thresholds['data_quality_score']:
            alerts.append(f"Low data quality: {metrics.data_quality_score:.2f}")
        
        if alerts:
            logger.warning(f"ALERTS: {', '.join(alerts)}")
        
        return alerts


# ============================================================================
# MAIN PIPELINE
# ============================================================================

class IndustrialDataPipeline:
    """Main orchestration pipeline"""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.metrics = PipelineMetrics()
        
        # Initialize stages
        self.stages = [
            DataIngestionStage(),
            ValidationStage(),
            DataCleaningStage(),
            OutlierDetectionStage(),
            NormalizationStage(),
            FeatureEngineeringStage()
        ]
        
        # Initialize components
        self.version_manager = DataVersionManager(config.version_storage_path) if config.enable_versioning else None
        self.ml_integration = MLFlowIntegration(config.ml)
        self.monitor = PrometheusMonitor(config.monitoring)
        
        # Database connectors
        self.db_connectors = []
        if config.database.use_postgres:
            pg = PostgreSQLConnector(config.database.postgres_url)
            pg.connect()
            self.db_connectors.append(pg)
        
        if config.database.use_mongodb:
            mongo = MongoDBConnector(config.database.mongodb_url)
            mongo.connect()
            self.db_connectors.append(mongo)
        
        # Stream processor
        self.stream_processor = None
        if config.streaming.enabled:
            self.stream_processor = StreamProcessor(config.streaming, self)
            self.stream_processor.start()
        
        logger.info("Pipeline initialized")
    
    def run(self, input_data: Optional[pd.DataFrame] = None) -> Tuple[pd.DataFrame, PipelineMetrics]:
        """Execute the complete pipeline"""
        start_time = time.time()
        
        try:
            data = input_data if input_data is not None else pd.DataFrame()
            self.metrics.total_records = len(data) if not data.empty else self.config.batch_size
            
            # Execute each stage
            for stage in self.stages:
                stage_start = time.time()
                
                try:
                    data = stage.process(data, self.config)
                    stage_time = time.time() - stage_start
                    self.metrics.stage_times[stage.name] = stage_time
                    
                    logger.info(f"{stage.name} completed in {stage_time:.2f}s, {len(data)} records")
                    
                except Exception as e:
                    self.metrics.error_count += 1
                    self.monitor.record_error()
                    logger.error(f"Error in {stage.name}: {e}")
                    raise
            
            # Update metrics
            self.metrics.valid_records = len(data)
            self.metrics.invalid_records = self.metrics.total_records - self.metrics.valid_records
            self.metrics.processing_time = time.time() - start_time
            self.metrics.throughput = self.metrics.valid_records / max(self.metrics.processing_time, 0.001)
            
            # Get quality score from validation stage
            validation_stage = next((s for s in self.stages if isinstance(s, ValidationStage)), None)
            if validation_stage:
                self.metrics.data_quality_score = validation_stage.quality_score
            
            # Save version
            if self.version_manager:
                version_metadata = {
                    'metrics': asdict(self.metrics),
                    'config': asdict(self.config)
                }
                version_id = self.version_manager.save_version(data, version_metadata)
            
            # Save to databases
            for connector in self.db_connectors:
                connector.save_data(data, self.config.database.table_name)
            
            # Log to MLflow
            ml_metrics = {
                'records_processed': self.metrics.valid_records,
                'processing_time': self.metrics.processing_time,
                'data_quality': self.metrics.data_quality_score,
                'throughput': self.metrics.throughput
            }
            self.ml_integration.log_metrics(ml_metrics)
            
            # Record monitoring metrics
            self.monitor.record_processing(
                self.metrics.valid_records,
                self.metrics.processing_time,
                self.metrics.data_quality_score
            )
            
            # Check alerts
            self.monitor.check_alerts(self.metrics)
            
            logger.info(f"Pipeline completed: {self.metrics.valid_records} records in {self.metrics.processing_time:.2f}s")
            
            return data, self.metrics
            
        except Exception as e:
            self.metrics.error_count += 1
            logger.error(f"Pipeline failed: {e}")
            raise
    
    def print_summary(self):
        """Print pipeline execution summary"""
        print("\n" + "="*70)
        print("PIPELINE EXECUTION SUMMARY")
        print("="*70)
        print(f"Total Records:        {self.metrics.total_records:,}")
        print(f"Valid Records:        {self.metrics.valid_records:,}")
        print(f"Invalid Records:      {self.metrics.invalid_records:,}")
        print(f"Processing Time:      {self.metrics.processing_time:.2f}s")
        print(f"Throughput:           {self.metrics.throughput:.0f} records/sec")
        print(f"Data Quality Score:   {self.metrics.data_quality_score:.2%}")
        print(f"Errors:               {self.metrics.error_count}")
        print("\nStage Execution Times:")
        print("-" * 70)
        for stage, duration in self.metrics.stage_times.items():
            print(f"  {stage:<30} {duration:>8.2f}s")
        print("="*70 + "\n")
    
    def export_results(self, data: pd.DataFrame, output_path: str = "./output"):
        """Export processed data to various formats"""
        output_dir = Path(output_path)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Export to CSV
        csv_path = output_dir / f"processed_data_{timestamp}.csv"
        data.to_csv(csv_path, index=False)
        logger.info(f"Exported to CSV: {csv_path}")
        
        # Export to JSON
        json_path = output_dir / f"processed_data_{timestamp}.json"
        data.to_json(json_path, orient='records', indent=2)
        logger.info(f"Exported to JSON: {json_path}")
        
        # Export metrics
        metrics_path = output_dir / f"metrics_{timestamp}.json"
        with open(metrics_path, 'w') as f:
            json.dump(asdict(self.metrics), f, indent=2)
        logger.info(f"Exported metrics: {metrics_path}")
        
        return {
            'csv': str(csv_path),
            'json': str(json_path),
            'metrics': str(metrics_path)
        }


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

def main():
    """Main execution function"""
    
    # Create custom configuration
    config = PipelineConfig(
        batch_size=1000,
        validation_enabled=True,
        outlier_detection_enabled=True,
        normalization_enabled=True,
        feature_engineering_enabled=True,
        enable_versioning=True,
        version_storage_path="./data_versions",
        database=DatabaseConfig(
            use_postgres=False,
            use_mongodb=False
        ),
        streaming=StreamConfig(
            enabled=False
        ),
        ml=MLConfig(
            auto_log=True,
            save_artifacts=True
        ),
        monitoring=MonitoringConfig(
            enable_metrics=False
        )
    )
    
    # Initialize pipeline
    pipeline = IndustrialDataPipeline(config)
    
    # Run pipeline (will generate synthetic data if no input provided)
    processed_data, metrics = pipeline.run()
    
    # Print summary
    pipeline.print_summary()
    
    # Export results
    export_paths = pipeline.export_results(processed_data)
    print(f"\nResults exported to:")
    for format_type, path in export_paths.items():
        print(f"  {format_type}: {path}")
    
    # Display sample of processed data
    print("\nSample of Processed Data (first 5 rows):")
    print(processed_data.head())
    
    print("\nProcessed Data Info:")
    print(processed_data.info())
    
    # List saved versions
    if config.enable_versioning:
        versions = pipeline.version_manager.list_versions()
        print(f"\nData Versions Saved: {len(versions)}")
        if versions:
            latest = versions[-1]
            print(f"Latest Version: {latest['version_id']}")
            print(f"  - Timestamp: {latest['timestamp']}")
            print(f"  - Records: {latest['records']}")


if __name__ == "__main__":
    main()