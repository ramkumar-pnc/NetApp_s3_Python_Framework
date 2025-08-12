"""
Modularized NetApp S3 Object Store Framework
Separated modules for Python and PySpark operations with clean interfaces
"""

import logging
import os
import json
from typing import Dict, List, Optional, Union, Any, Tuple, Protocol
from dataclasses import dataclass, field
from datetime import datetime
import tempfile
from pathlib import Path
from abc import ABC, abstractmethod
import pandas as pd

import s3fs
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

# ================================
# Core Configuration Module
# ================================

@dataclass
class S3Config:
    """Configuration class for S3 connection settings"""
    endpoint_url: str
    access_key: str
    secret_key: str
    bucket_name: Optional[str] = None
    region: str = "us-east-1"
    use_ssl: bool = True
    verify_ssl: bool = True
    client_kwargs: Dict[str, Any] = field(default_factory=dict)
    config_kwargs: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_env(cls, prefix: str = "NETAPP_S3") -> "S3Config":
        """Create config from environment variables"""
        return cls(
            endpoint_url=os.getenv(f"{prefix}_ENDPOINT_URL"),
            access_key=os.getenv(f"{prefix}_ACCESS_KEY"),
            secret_key=os.getenv(f"{prefix}_SECRET_KEY"),
            bucket_name=os.getenv(f"{prefix}_BUCKET"),
            region=os.getenv(f"{prefix}_REGION", "us-east-1"),
            use_ssl=os.getenv(f"{prefix}_USE_SSL", "true").lower() == "true",
            verify_ssl=os.getenv(f"{prefix}_VERIFY_SSL", "true").lower() == "true"
        )

    @classmethod
    def from_file(cls, config_path: str) -> "S3Config":
        """Create config from JSON file"""
        with open(config_path, 'r') as f:
            config_data = json.load(f)
        return cls(**config_data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary"""
        return {
            'endpoint_url': self.endpoint_url,
            'access_key': self.access_key,
            'secret_key': self.secret_key,
            'bucket_name': self.bucket_name,
            'region': self.region,
            'use_ssl': self.use_ssl,
            'verify_ssl': self.verify_ssl,
            'client_kwargs': self.client_kwargs,
            'config_kwargs': self.config_kwargs
        }

# ================================
# Logger Module
# ================================

class LoggerMixin:
    """Mixin class for consistent logging across modules"""
    
    def _setup_logging(self, name: str = None) -> logging.Logger:
        """Setup comprehensive logging"""
        logger_name = name or f"{__name__}.{self.__class__.__name__}"
        logger = logging.getLogger(logger_name)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

# ================================
# Base Connection Module
# ================================

class BaseS3Connection(LoggerMixin):
    """Base S3 connection handler"""
    
    def __init__(self, config: S3Config):
        self.config = config
        self.logger = self._setup_logging()
        self.fs = None
        self._connect()

    def _connect(self):
        """Establish connection to NetApp S3"""
        try:
            self.fs = s3fs.S3FileSystem(
                key=self.config.access_key,
                secret=self.config.secret_key,
                endpoint_url=self.config.endpoint_url,
                use_ssl=self.config.use_ssl,
                client_kwargs={
                    'verify': self.config.verify_ssl,
                    'region_name': self.config.region,
                    **self.config.client_kwargs
                },
                config_kwargs=self.config.config_kwargs
            )
            self.logger.info(f"Successfully connected to NetApp S3: {self.config.endpoint_url}")
        except Exception as e:
            self.logger.error(f"Failed to connect to NetApp S3: {e}")
            raise

    def get_filesystem(self) -> s3fs.S3FileSystem:
        """Get the underlying filesystem object"""
        return self.fs

    def test_connection(self) -> bool:
        """Test the S3 connection"""
        try:
            self.fs.ls('/')
            self.logger.info("S3 connection test successful")
            return True
        except Exception as e:
            self.logger.error(f"S3 connection test failed: {e}")
            return False

# ================================
# Bucket Operations Module
# ================================

class BucketOperations(LoggerMixin):
    """Module for S3 bucket operations"""
    
    def __init__(self, connection: BaseS3Connection):
        self.fs = connection.get_filesystem()
        self.logger = self._setup_logging("BucketOperations")

    def list_buckets(self) -> List[str]:
        """List all available buckets"""
        try:
            buckets = self.fs.ls('/')
            bucket_names = [bucket.split('/')[-1] for bucket in buckets]
            self.logger.info(f"Found {len(bucket_names)} buckets")
            return bucket_names
        except Exception as e:
            self.logger.error(f"Failed to list buckets: {e}")
            raise

    def create_bucket(self, bucket_name: str) -> bool:
        """Create a new bucket"""
        try:
            if self.bucket_exists(bucket_name):
                self.logger.warning(f"Bucket {bucket_name} already exists")
                return True
            
            self.fs.mkdir(bucket_name)
            self.logger.info(f"Successfully created bucket: {bucket_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to create bucket {bucket_name}: {e}")
            return False

    def delete_bucket(self, bucket_name: str, force: bool = False) -> bool:
        """Delete a bucket (optionally with all contents)"""
        try:
            if not self.bucket_exists(bucket_name):
                self.logger.warning(f"Bucket {bucket_name} does not exist")
                return True

            if force:
                # Use ObjectOperations to delete all objects first
                from . import ObjectOperations  # Avoid circular import
                obj_ops = ObjectOperations(BaseS3Connection(self.fs))
                objects = obj_ops.list_objects(bucket_name)
                for obj in objects:
                    obj_ops.delete_object(bucket_name, obj)

            self.fs.rmdir(bucket_name)
            self.logger.info(f"Successfully deleted bucket: {bucket_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to delete bucket {bucket_name}: {e}")
            return False

    def bucket_exists(self, bucket_name: str) -> bool:
        """Check if bucket exists"""
        try:
            return self.fs.exists(bucket_name)
        except Exception as e:
            self.logger.error(f"Failed to check bucket existence {bucket_name}: {e}")
            return False

    def get_bucket_info(self, bucket_name: str) -> Optional[Dict]:
        """Get bucket information"""
        try:
            if not self.bucket_exists(bucket_name):
                return None
            
            info = self.fs.info(bucket_name)
            return {
                'name': bucket_name,
                'type': info.get('type'),
                'size': info.get('size', 0)
            }
        except Exception as e:
            self.logger.error(f"Failed to get bucket info for {bucket_name}: {e}")
            return None

# ================================
# Object Operations Module
# ================================

class ObjectOperations(LoggerMixin):
    """Module for S3 object operations"""
    
    def __init__(self, connection: BaseS3Connection):
        self.fs = connection.get_filesystem()
        self.logger = self._setup_logging("ObjectOperations")

    def list_objects(self, bucket_name: str, prefix: str = "", 
                    recursive: bool = True) -> List[str]:
        """List objects in bucket with optional prefix filtering"""
        try:
            path = f"{bucket_name}/{prefix}" if prefix else bucket_name
            
            if recursive:
                objects = self.fs.find(path)
            else:
                objects = self.fs.ls(path)
            
            # Remove bucket name from paths and filter files only
            clean_objects = []
            for obj in objects:
                if self.fs.isfile(obj):
                    clean_path = obj.replace(f"{bucket_name}/", "")
                    clean_objects.append(clean_path)
            
            self.logger.info(f"Found {len(clean_objects)} objects in {bucket_name}")
            return clean_objects
        except Exception as e:
            self.logger.error(f"Failed to list objects in {bucket_name}: {e}")
            return []

    def upload_file(self, local_path: str, bucket_name: str, 
                   object_key: str, overwrite: bool = False) -> bool:
        """Upload local file to S3 bucket"""
        try:
            s3_path = f"{bucket_name}/{object_key}"
            
            if not overwrite and self.fs.exists(s3_path):
                self.logger.warning(f"Object {s3_path} already exists")
                return False
            
            self.fs.put(local_path, s3_path)
            self.logger.info(f"Successfully uploaded {local_path} to {s3_path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to upload {local_path} to {bucket_name}/{object_key}: {e}")
            return False

    def download_file(self, bucket_name: str, object_key: str, 
                     local_path: str, overwrite: bool = False) -> bool:
        """Download file from S3 to local path"""
        try:
            s3_path = f"{bucket_name}/{object_key}"
            
            if not overwrite and os.path.exists(local_path):
                self.logger.warning(f"Local file {local_path} already exists")
                return False
            
            if not self.fs.exists(s3_path):
                self.logger.error(f"Object {s3_path} does not exist")
                return False
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            self.fs.get(s3_path, local_path)
            self.logger.info(f"Successfully downloaded {s3_path} to {local_path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to download {bucket_name}/{object_key}: {e}")
            return False

    def delete_object(self, bucket_name: str, object_key: str) -> bool:
        """Delete object from bucket"""
        try:
            s3_path = f"{bucket_name}/{object_key}"
            
            if not self.fs.exists(s3_path):
                self.logger.warning(f"Object {s3_path} does not exist")
                return True
            
            self.fs.rm(s3_path)
            self.logger.info(f"Successfully deleted object: {s3_path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to delete object {s3_path}: {e}")
            return False

    def copy_object(self, source_bucket: str, source_key: str,
                   dest_bucket: str, dest_key: str) -> bool:
        """Copy object from one location to another"""
        try:
            source_path = f"{source_bucket}/{source_key}"
            dest_path = f"{dest_bucket}/{dest_key}"
            
            if not self.fs.exists(source_path):
                self.logger.error(f"Source object {source_path} does not exist")
                return False
            
            self.fs.copy(source_path, dest_path)
            self.logger.info(f"Successfully copied {source_path} to {dest_path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to copy {source_bucket}/{source_key}: {e}")
            return False

    def get_object_info(self, bucket_name: str, object_key: str) -> Optional[Dict]:
        """Get object metadata and information"""
        try:
            s3_path = f"{bucket_name}/{object_key}"
            
            if not self.fs.exists(s3_path):
                self.logger.error(f"Object {s3_path} does not exist")
                return None
            
            info = self.fs.info(s3_path)
            return {
                'name': info.get('name'),
                'size': info.get('size'),
                'type': info.get('type'),
                'last_modified': info.get('LastModified'),
                'etag': info.get('ETag'),
                'content_type': info.get('ContentType')
            }
        except Exception as e:
            self.logger.error(f"Failed to get info for {bucket_name}/{object_key}: {e}")
            return None

    def object_exists(self, bucket_name: str, object_key: str) -> bool:
        """Check if object exists"""
        try:
            s3_path = f"{bucket_name}/{object_key}"
            return self.fs.exists(s3_path)
        except Exception as e:
            self.logger.error(f"Failed to check object existence {bucket_name}/{object_key}: {e}")
            return False

# ================================
# Python Data Operations Module
# ================================

class PythonDataOperations(LoggerMixin):
    """Module for Python-specific data operations"""
    
    def __init__(self, connection: BaseS3Connection):
        self.fs = connection.get_filesystem()
        self.logger = self._setup_logging("PythonDataOperations")

    def read_text_file(self, bucket_name: str, object_key: str) -> Optional[str]:
        """Read text file content"""
        try:
            s3_path = f"{bucket_name}/{object_key}"
            with self.fs.open(s3_path, 'r') as f:
                content = f.read()
            self.logger.info(f"Successfully read text file: {s3_path}")
            return content
        except Exception as e:
            self.logger.error(f"Failed to read text file {bucket_name}/{object_key}: {e}")
            return None

    def write_text_file(self, content: str, bucket_name: str, object_key: str) -> bool:
        """Write text content to file"""
        try:
            s3_path = f"{bucket_name}/{object_key}"
            with self.fs.open(s3_path, 'w') as f:
                f.write(content)
            self.logger.info(f"Successfully wrote text file: {s3_path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to write text file {bucket_name}/{object_key}: {e}")
            return False

    def read_json_file(self, bucket_name: str, object_key: str) -> Optional[Dict]:
        """Read JSON file content"""
        try:
            content = self.read_text_file(bucket_name, object_key)
            if content:
                return json.loads(content)
            return None
        except Exception as e:
            self.logger.error(f"Failed to read JSON file {bucket_name}/{object_key}: {e}")
            return None

    def write_json_file(self, data: Dict, bucket_name: str, object_key: str) -> bool:
        """Write data to JSON file"""
        try:
            content = json.dumps(data, indent=2)
            return self.write_text_file(content, bucket_name, object_key)
        except Exception as e:
            self.logger.error(f"Failed to write JSON file {bucket_name}/{object_key}: {e}")
            return False

    def read_binary_file(self, bucket_name: str, object_key: str) -> Optional[bytes]:
        """Read binary file content"""
        try:
            s3_path = f"{bucket_name}/{object_key}"
            with self.fs.open(s3_path, 'rb') as f:
                content = f.read()
            self.logger.info(f"Successfully read binary file: {s3_path}")
            return content
        except Exception as e:
            self.logger.error(f"Failed to read binary file {bucket_name}/{object_key}: {e}")
            return None

    def write_binary_file(self, data: bytes, bucket_name: str, object_key: str) -> bool:
        """Write binary data to file"""
        try:
            s3_path = f"{bucket_name}/{object_key}"
            with self.fs.open(s3_path, 'wb') as f:
                f.write(data)
            self.logger.info(f"Successfully wrote binary file: {s3_path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to write binary file {bucket_name}/{object_key}: {e}")
            return False

    # Pandas Integration
    def read_csv_to_pandas(self, bucket_name: str, object_key: str, 
                          **read_options) -> Optional[pd.DataFrame]:
        """Read CSV file to Pandas DataFrame"""
        try:
            s3_path = f"{bucket_name}/{object_key}"
            with self.fs.open(s3_path, 'rb') as f:
                df = pd.read_csv(f, **read_options)
            self.logger.info(f"Successfully read CSV to Pandas: {s3_path}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to read CSV to Pandas {bucket_name}/{object_key}: {e}")
            return None

    def write_pandas_to_csv(self, df: pd.DataFrame, bucket_name: str,
                           object_key: str, **write_options) -> bool:
        """Write Pandas DataFrame to CSV file"""
        try:
            s3_path = f"{bucket_name}/{object_key}"
            with self.fs.open(s3_path, 'w') as f:
                df.to_csv(f, index=False, **write_options)
            self.logger.info(f"Successfully wrote Pandas DataFrame to CSV: {s3_path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to write Pandas DataFrame to CSV {bucket_name}/{object_key}: {e}")
            return False

    def read_parquet_to_pandas(self, bucket_name: str, object_key: str,
                              **read_options) -> Optional[pd.DataFrame]:
        """Read Parquet file to Pandas DataFrame"""
        try:
            s3_path = f"{bucket_name}/{object_key}"
            with self.fs.open(s3_path, 'rb') as f:
                df = pd.read_parquet(f, **read_options)
            self.logger.info(f"Successfully read Parquet to Pandas: {s3_path}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to read Parquet to Pandas {bucket_name}/{object_key}: {e}")
            return None

    def write_pandas_to_parquet(self, df: pd.DataFrame, bucket_name: str,
                               object_key: str, **write_options) -> bool:
        """Write Pandas DataFrame to Parquet file"""
        try:
            s3_path = f"{bucket_name}/{object_key}"
            with self.fs.open(s3_path, 'wb') as f:
                df.to_parquet(f, index=False, **write_options)
            self.logger.info(f"Successfully wrote Pandas DataFrame to Parquet: {s3_path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to write Pandas DataFrame to Parquet {bucket_name}/{object_key}: {e}")
            return False

    def read_excel_to_pandas(self, bucket_name: str, object_key: str,
                            **read_options) -> Optional[pd.DataFrame]:
        """Read Excel file to Pandas DataFrame"""
        try:
            s3_path = f"{bucket_name}/{object_key}"
            with self.fs.open(s3_path, 'rb') as f:
                df = pd.read_excel(f, **read_options)
            self.logger.info(f"Successfully read Excel to Pandas: {s3_path}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to read Excel to Pandas {bucket_name}/{object_key}: {e}")
            return None

# ================================
# PySpark Data Operations Module
# ================================

class PySparkDataOperations(LoggerMixin):
    """Module for PySpark-specific data operations"""
    
    def __init__(self, connection: BaseS3Connection, spark: Optional[SparkSession] = None):
        self.fs = connection.get_filesystem()
        self.config = connection.config
        self.spark = spark or self._get_or_create_spark()
        self.logger = self._setup_logging("PySparkDataOperations")
        self._configure_spark_s3()

    def _get_or_create_spark(self) -> SparkSession:
        """Get existing SparkSession or create new one"""
        try:
            return SparkSession.getActiveSession() or SparkSession.builder \
                .appName("NetAppS3Framework") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
                .getOrCreate()
        except Exception as e:
            self.logger.error(f"Failed to create SparkSession: {e}")
            raise

    def _configure_spark_s3(self):
        """Configure Spark for S3 access"""
        try:
            conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            conf.set("fs.s3a.endpoint", self.config.endpoint_url)
            conf.set("fs.s3a.access.key", self.config.access_key)
            conf.set("fs.s3a.secret.key", self.config.secret_key)
            conf.set("fs.s3a.path.style.access", "true")
            conf.set("fs.s3a.connection.ssl.enabled", str(self.config.use_ssl).lower())
            
            if not self.config.verify_ssl:
                conf.set("fs.s3a.connection.ssl.enabled", "false")
            
            self.logger.info("Successfully configured Spark for S3 access")
        except Exception as e:
            self.logger.error(f"Failed to configure Spark S3 settings: {e}")
            raise

    def read_parquet_to_spark(self, bucket_name: str, object_key: str, 
                             **read_options) -> Optional[DataFrame]:
        """Read Parquet file(s) into Spark DataFrame"""
        try:
            s3_path = f"s3a://{bucket_name}/{object_key}"
            df = self.spark.read.parquet(s3_path, **read_options)
            self.logger.info(f"Successfully read Parquet file to Spark: {s3_path}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to read Parquet file to Spark {bucket_name}/{object_key}: {e}")
            return None

    def write_spark_to_parquet(self, df: DataFrame, bucket_name: str, 
                              object_key: str, mode: str = "overwrite", 
                              **write_options) -> bool:
        """Write Spark DataFrame to Parquet file(s)"""
        try:
            s3_path = f"s3a://{bucket_name}/{object_key}"
            df.write.mode(mode).parquet(s3_path, **write_options)
            self.logger.info(f"Successfully wrote Spark DataFrame to Parquet: {s3_path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to write Spark DataFrame to Parquet {bucket_name}/{object_key}: {e}")
            return False

    def read_csv_to_spark(self, bucket_name: str, object_key: str,
                         schema: Optional[StructType] = None,
                         **read_options) -> Optional[DataFrame]:
        """Read CSV file(s) into Spark DataFrame"""
        try:
            s3_path = f"s3a://{bucket_name}/{object_key}"
            reader = self.spark.read.option("header", "true").option("inferSchema", "true")
            
            if schema:
                reader = reader.schema(schema)
            
            for key, value in read_options.items():
                reader = reader.option(key, value)
            
            df = reader.csv(s3_path)
            self.logger.info(f"Successfully read CSV file to Spark: {s3_path}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to read CSV file to Spark {bucket_name}/{object_key}: {e}")
            return None

    def write_spark_to_csv(self, df: DataFrame, bucket_name: str,
                          object_key: str, mode: str = "overwrite",
                          **write_options) -> bool:
        """Write Spark DataFrame to CSV file(s)"""
        try:
            s3_path = f"s3a://{bucket_name}/{object_key}"
            writer = df.write.mode(mode).option("header", "true")
            
            for key, value in write_options.items():
                writer = writer.option(key, value)
            
            writer.csv(s3_path)
            self.logger.info(f"Successfully wrote Spark DataFrame to CSV: {s3_path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to write Spark DataFrame to CSV {bucket_name}/{object_key}: {e}")
            return False

    def read_json_to_spark(self, bucket_name: str, object_key: str,
                          **read_options) -> Optional[DataFrame]:
        """Read JSON file(s) into Spark DataFrame"""
        try:
            s3_path = f"s3a://{bucket_name}/{object_key}"
            df = self.spark.read.json(s3_path, **read_options)
            self.logger.info(f"Successfully read JSON file to Spark: {s3_path}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to read JSON file to Spark {bucket_name}/{object_key}: {e}")
            return None

    def write_spark_to_json(self, df: DataFrame, bucket_name: str,
                           object_key: str, mode: str = "overwrite") -> bool:
        """Write Spark DataFrame to JSON file(s)"""
        try:
            s3_path = f"s3a://{bucket_name}/{object_key}"
            df.write.mode(mode).json(s3_path)
            self.logger.info(f"Successfully wrote Spark DataFrame to JSON: {s3_path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to write Spark DataFrame to JSON {bucket_name}/{object_key}: {e}")
            return False

    def read_delta_to_spark(self, bucket_name: str, object_key: str,
                           **read_options) -> Optional[DataFrame]:
        """Read Delta table into Spark DataFrame"""
        try:
            s3_path = f"s3a://{bucket_name}/{object_key}"
            df = self.spark.read.format("delta").load(s3_path, **read_options)
            self.logger.info(f"Successfully read Delta table to Spark: {s3_path}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to read Delta table to Spark {bucket_name}/{object_key}: {e}")
            return None

    def write_spark_to_delta(self, df: DataFrame, bucket_name: str,
                            object_key: str, mode: str = "overwrite",
                            **write_options) -> bool:
        """Write Spark DataFrame to Delta table"""
        try:
            s3_path = f"s3a://{bucket_name}/{object_key}"
            df.write.format("delta").mode(mode).save(s3_path, **write_options)
            self.logger.info(f"Successfully wrote Spark DataFrame to Delta: {s3_path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to write Spark DataFrame to Delta {bucket_name}/{object_key}: {e}")
            return False

    def get_spark_session(self) -> SparkSession:
        """Get the Spark session"""
        return self.spark

# ================================
# Utility Operations Module
# ================================

class UtilityOperations(LoggerMixin):
    """Module for utility operations and batch processing"""
    
    def __init__(self, connection: BaseS3Connection):
        self.fs = connection.get_filesystem()
        self.logger = self._setup_logging("UtilityOperations")

    def get_bucket_size(self, bucket_name: str) -> int:
        """Get total size of all objects in bucket"""
        try:
            total_size = 0
            objects = self.fs.find(bucket_name)
            
            for obj in objects:
                if self.fs.isfile(obj):
                    info = self.fs.info(obj)
                    total_size += info.get('size', 0)
            
            self.logger.info(f"Total size of bucket {bucket_name}: {total_size} bytes")
            return total_size
        except Exception as e:
            self.logger.error(f"Failed to get bucket size for {bucket_name}: {e}")
            return 0

    def sync_local_to_s3(self, local_dir: str, bucket_name: str, 
                        s3_prefix: str = "", exclude_patterns: List[str] = None) -> bool:
        """Sync local directory to S3 bucket"""
        try:
            exclude_patterns = exclude_patterns or []
            local_path = Path(local_dir)
            
            if not local_path.exists():
                self.logger.error(f"Local directory {local_dir} does not exist")
                return False
            
            success_count = 0
            total_count = 0
            
            for file_path in local_path.rglob('*'):
                if file_path.is_file():
                    # Check exclusion patterns
                    if any(pattern in str(file_path) for pattern in exclude_patterns):
                        continue
                    
                    relative_path = file_path.relative_to(local_path)
                    s3_key = f"{s3_prefix}/{relative_path}".lstrip('/')
                    
                    total_count += 1
                    try:
                        self.fs.put(str(file_path), f"{bucket_name}/{s3_key}")
                        success_count += 1
                    except Exception as e:
                        self.logger.error(f"Failed to upload {file_path}: {e}")
            
            self.logger.info(f"Sync completed: {success_count}/{total_count} files uploaded")
            return success_count == total_count
        except Exception as e:
            self.logger.error(f"Failed to sync {local_dir} to {bucket_name}: {e}")
            return False

    def sync_s3_to_local(self, bucket_name: str, local_dir: str,
                        s3_prefix: str = "", exclude_patterns: List[str] = None) -> bool:
        """Sync S3 bucket/prefix to local directory"""
        try:
            exclude_patterns = exclude_patterns or []
            local_path = Path(local_dir)
            local_path.mkdir(parents=True, exist_ok=True)
            
            s3_path = f"{bucket_name}/{s3_prefix}" if s3_prefix else bucket_name
            objects = self.fs.find(s3_path)
            
            success_count = 0
            total_count = 0
            
            for obj in objects:
                if self.fs.isfile(obj):
                    # Check exclusion patterns
                    if any(pattern in obj for pattern in exclude_patterns):
                        continue
                    
                    # Extract relative path from S3 object
                    rel_path = obj.replace(f"{bucket_name}/", "")
                    if s3_prefix:
                        rel_path = rel_path.replace(f"{s3_prefix}/", "")
                    
                    local_file = local_path / rel_path
                    local_file.parent.mkdir(parents=True, exist_ok=True)
                    
                    total_count += 1
                    try:
                        self.fs.get(obj, str(local_file))
                        success_count += 1
                    except Exception as e:
                        self.logger.error(f"Failed to download {obj}: {e}")
            
            self.logger.info(f"Sync completed: {success_count}/{total_count} files downloaded")
            return success_count == total_count
        except Exception as e:
            self.logger.error(f"Failed to sync {bucket_name} to {local_dir}: {e}")
            return False

    def batch_delete_objects(self, bucket_name: str, object_keys: List[str]) -> Dict[str, bool]:
        """Delete multiple objects in batch"""
        results = {}
        try:
            for key in object_keys:
                s3_path = f"{bucket_name}/{key}"
                try:
                    if self.fs.exists(s3_path):
                        self.fs.rm(s3_path)
                        results[key] = True
                        self.logger.debug(f"Successfully deleted: {s3_path}")
                    else:
                        results[key] = True  # Consider non-existent as success
                        self.logger.debug(f"Object does not exist: {s3_path}")
                except Exception as e:
                    results[key] = False
                    self.logger.error(f"Failed to delete {s3_path}: {e}")
            
            success_count = sum(results.values())
            self.logger.info(f"Batch delete completed: {success_count}/{len(object_keys)} objects deleted")
            return results
        except Exception as e:
            self.logger.error(f"Failed batch delete operation: {e}")
            return {key: False for key in object_keys}

    def get_object_stats(self, bucket_name: str, prefix: str = "") -> Dict[str, Any]:
        """Get statistics about objects in bucket/prefix"""
        try:
            path = f"{bucket_name}/{prefix}" if prefix else bucket_name
            objects = self.fs.find(path)
            
            stats = {
                'total_objects': 0,
                'total_size': 0,
                'file_types': {},
                'largest_file': {'name': '', 'size': 0},
                'smallest_file': {'name': '', 'size': float('inf')},
                'avg_size': 0
            }
            
            sizes = []
            for obj in objects:
                if self.fs.isfile(obj):
                    info = self.fs.info(obj)
                    size = info.get('size', 0)
                    
                    stats['total_objects'] += 1
                    stats['total_size'] += size
                    sizes.append(size)
                    
                    # Track file types
                    ext = Path(obj).suffix.lower()
                    stats['file_types'][ext] = stats['file_types'].get(ext, 0) + 1
                    
                    # Track largest file
                    if size > stats['largest_file']['size']:
                        stats['largest_file'] = {'name': obj, 'size': size}
                    
                    # Track smallest file
                    if size < stats['smallest_file']['size']:
                        stats['smallest_file'] = {'name': obj, 'size': size}
            
            if sizes:
                stats['avg_size'] = sum(sizes) / len(sizes)
            
            if stats['smallest_file']['size'] == float('inf'):
                stats['smallest_file'] = {'name': '', 'size': 0}
            
            self.logger.info(f"Object stats for {bucket_name}/{prefix}: {stats['total_objects']} objects, {stats['total_size']} bytes")
            return stats
        except Exception as e:
            self.logger.error(f"Failed to get object stats for {bucket_name}/{prefix}: {e}")
            return {}

# ================================
# Main Framework Interface
# ================================

class NetAppS3Framework(LoggerMixin):
    """
    Main framework interface that orchestrates all modules
    Provides both Python and PySpark capabilities
    """
    
    def __init__(self, config: S3Config, spark: Optional[SparkSession] = None):
        self.config = config
        self.logger = self._setup_logging("NetAppS3Framework")
        
        # Initialize connection
        self.connection = BaseS3Connection(config)
        
        # Initialize modules
        self.buckets = BucketOperations(self.connection)
        self.objects = ObjectOperations(self.connection)
        self.python_data = PythonDataOperations(self.connection)
        self.utilities = UtilityOperations(self.connection)
        
        # Initialize PySpark module only if requested
        self._pyspark_data = None
        if spark is not None:
            self._pyspark_data = PySparkDataOperations(self.connection, spark)
        
        self.logger.info("NetApp S3 Framework initialized successfully")

    @property
    def pyspark_data(self) -> PySparkDataOperations:
        """Lazy initialization of PySpark operations"""
        if self._pyspark_data is None:
            self._pyspark_data = PySparkDataOperations(self.connection)
            self.logger.info("PySpark operations module initialized")
        return self._pyspark_data

    def test_connection(self) -> bool:
        """Test the S3 connection"""
        return self.connection.test_connection()

    def get_framework_info(self) -> Dict[str, Any]:
        """Get information about the framework and its modules"""
        return {
            'config': {
                'endpoint_url': self.config.endpoint_url,
                'region': self.config.region,
                'use_ssl': self.config.use_ssl
            },
            'modules': {
                'buckets': 'BucketOperations',
                'objects': 'ObjectOperations',
                'python_data': 'PythonDataOperations',
                'pyspark_data': 'PySparkDataOperations' if self._pyspark_data else 'Not initialized',
                'utilities': 'UtilityOperations'
            },
            'connection_status': self.test_connection()
        }

    def cleanup(self):
        """Cleanup resources"""
        try:
            if hasattr(self, 'connection') and self.connection:
                # s3fs doesn't require explicit cleanup
                pass
            
            if self._pyspark_data and hasattr(self._pyspark_data, 'spark'):
                # Don't stop spark as it might be used by other processes
                pass
                
            self.logger.info("Framework cleanup completed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

# ================================
# Factory Functions
# ================================

def create_python_framework(config: S3Config) -> NetAppS3Framework:
    """Factory function to create framework optimized for Python operations"""
    return NetAppS3Framework(config, spark=None)

def create_pyspark_framework(config: S3Config, spark: Optional[SparkSession] = None) -> NetAppS3Framework:
    """Factory function to create framework with PySpark capabilities"""
    return NetAppS3Framework(config, spark=spark)

def create_hybrid_framework(config: S3Config, spark: Optional[SparkSession] = None) -> NetAppS3Framework:
    """Factory function to create framework with both Python and PySpark capabilities"""
    return NetAppS3Framework(config, spark=spark)

# ================================
# Example Usage and Testing
# ================================

if __name__ == "__main__":
    # Example configuration
    config = S3Config(
        endpoint_url="https://your-netapp-s3-endpoint.com",
        access_key="your-access-key",
        secret_key="your-secret-key",
        bucket_name="test-bucket"
    )
    
    # Example 1: Python-only framework
    print("=== Python Framework Example ===")
    with create_python_framework(config) as python_framework:
        # Test connection
        if python_framework.test_connection():
            print("Connection successful!")
            
            # Bucket operations
            buckets = python_framework.buckets.list_buckets()
            print(f"Available buckets: {buckets}")
            
            # Object operations
            if buckets:
                objects = python_framework.objects.list_objects(buckets[0])
                print(f"Objects in {buckets[0]}: {len(objects)}")
            
            # Python data operations
            # df = python_framework.python_data.read_csv_to_pandas("bucket", "file.csv")
            
            # Utility operations
            # stats = python_framework.utilities.get_object_stats("bucket")
    
    # Example 2: PySpark framework
    print("\n=== PySpark Framework Example ===")
    with create_pyspark_framework(config) as pyspark_framework:
        # Test connection
        if pyspark_framework.test_connection():
            print("Connection successful!")
            
            # PySpark data operations
            # df = pyspark_framework.pyspark_data.read_parquet_to_spark("bucket", "data/*.parquet")
            # if df:
            #     df.show(5)
            #     pyspark_framework.pyspark_data.write_spark_to_parquet(df, "bucket", "output/data.parquet")
            
            # Get Spark session for custom operations
            spark = pyspark_framework.pyspark_data.get_spark_session()
            print(f"Spark version: {spark.version}")
    
    # Example 3: Hybrid framework (both Python and PySpark)
    print("\n=== Hybrid Framework Example ===")
    with create_hybrid_framework(config) as hybrid_framework:
        # Framework info
        info = hybrid_framework.get_framework_info()
        print(f"Framework info: {info}")
        
        # Use both Python and PySpark operations
        # pandas_df = hybrid_framework.python_data.read_csv_to_pandas("bucket", "small_file.csv")
        # spark_df = hybrid_framework.pyspark_data.read_parquet_to_spark("bucket", "large_data/*.parquet")
        
        # Utility operations
        # stats = hybrid_framework.utilities.get_object_stats("bucket")
        # print(f"Bucket stats: {stats}")

    print("\nAll examples completed successfully!")