# NetApp S3 Framework Documentation

## Table of Contents

1. [Overview](#overview)
2. [Installation](#installation)
3. [Quick Start](#quick-start)
4. [Configuration](#configuration)
5. [API Reference](#api-reference)
6. [Usage Examples](#usage-examples)
7. [Error Handling](#error-handling)
8. [Best Practices](#best-practices)
9. [Testing](#testing)
10. [Troubleshooting](#troubleshooting)

---

## Overview

The NetApp S3 Framework is a comprehensive Python library designed to interact with NetApp StorageGRID S3 object storage. It provides a simple, maintainable interface for reading, writing, and deleting files in multiple formats including text, CSV, and Parquet.

### Key Features

- **Multiple file format support**: Text, CSV, Parquet
- **SSL/TLS handling**: Automatic SSL configuration for NetApp environments
- **Robust error handling**: Comprehensive exception handling and logging
- **Memory efficient**: Streaming support for large files
- **Type hints**: Full type annotation for better IDE support
- **Configurable**: Environment variable and configuration file support
- **Logging**: Built-in logging with configurable levels
- **Retry logic**: Automatic retry for transient failures

### Supported Operations

| Operation | Text Files | CSV Files | Parquet Files |
|-----------|------------|-----------|---------------|
| Read      | ✅         | ✅        | ✅           |
| Write     | ✅         | ✅        | ✅           |
| Delete    | ✅         | ✅        | ✅           |
| List      | ✅         | ✅        | ✅           |
| Stream    | ✅         | ✅        | ❌           |

---

## Installation

### Prerequisites

- Python 3.7+
- NetApp StorageGRID access credentials
- Network access to NetApp S3 endpoint

### Required Packages

```bash
pip install s3fs pandas pyarrow python-dotenv typing-extensions
```

### Optional Packages

```bash
pip install pytest pytest-mock  # For testing
pip install black flake8       # For code formatting
```

---

## Quick Start

### Basic Usage

```python
from netapp_s3_framework import NetAppS3Client
import pandas as pd

# Initialize client
client = NetAppS3Client(
    endpoint_url="https://your-netapp-endpoint.com",
    access_key="your-access-key",
    secret_key="your-secret-key"
)

# Read a CSV file
df = client.read_csv("my-bucket", "data/sales.csv")

# Write a text file
client.write_text("my-bucket", "logs/output.txt", "Hello World!")

# List files
files = client.list_files("my-bucket", prefix="data/")
```

### Environment Variables

Create a `.env` file:

```
NETAPP_ENDPOINT=https://your-netapp-endpoint.com
NETAPP_ACCESS_KEY=your-access-key
NETAPP_SECRET_KEY=your-secret-key
NETAPP_DEFAULT_BUCKET=my-default-bucket
NETAPP_SSL_VERIFY=false
NETAPP_LOG_LEVEL=INFO
```

```python
# Client will automatically load from environment
client = NetAppS3Client.from_env()
```

---

## Configuration

### Configuration Options

```python
from netapp_s3_framework import NetAppS3Config

config = NetAppS3Config(
    endpoint_url="https://your-endpoint.com",
    access_key="your-key",
    secret_key="your-secret",
    region_name="us-east-1",
    ssl_verify=False,
    default_bucket="my-bucket",
    max_retries=3,
    retry_delay=1.0,
    chunk_size=8192,
    log_level="INFO"
)

client = NetAppS3Client(config=config)
```

### Configuration File

Create `netapp_s3_config.yaml`:

```yaml
endpoint_url: "https://your-endpoint.com"
access_key: "your-key"
secret_key: "your-secret"
region_name: "us-east-1"
ssl_verify: false
default_bucket: "my-bucket"
max_retries: 3
retry_delay: 1.0
chunk_size: 8192
log_level: "INFO"
```

```python
client = NetAppS3Client.from_config_file("netapp_s3_config.yaml")
```

---

## API Reference

### NetAppS3Client Class

#### Constructor

```python
class NetAppS3Client:
    def __init__(
        self,
        endpoint_url: str = None,
        access_key: str = None,
        secret_key: str = None,
        config: NetAppS3Config = None,
        **kwargs
    ):
        """
        Initialize NetApp S3 client.
        
        Args:
            endpoint_url: NetApp S3 endpoint URL
            access_key: S3 access key
            secret_key: S3 secret key
            config: Configuration object
            **kwargs: Additional configuration options
        """
```

#### Class Methods

```python
@classmethod
def from_env(cls) -> 'NetAppS3Client':
    """Create client from environment variables."""

@classmethod
def from_config_file(cls, config_path: str) -> 'NetAppS3Client':
    """Create client from configuration file."""
```

#### File Operations

##### Text Files

```python
def read_text(
    self, 
    bucket: str, 
    key: str, 
    encoding: str = 'utf-8'
) -> Optional[str]:
    """
    Read text file from S3.
    
    Args:
        bucket: S3 bucket name
        key: Object key/path
        encoding: Text encoding (default: utf-8)
        
    Returns:
        File content as string, None if error
        
    Raises:
        FileNotFoundError: If file doesn't exist
        PermissionError: If access denied
        UnicodeDecodeError: If encoding error
    """

def write_text(
    self, 
    bucket: str, 
    key: str, 
    content: str, 
    encoding: str = 'utf-8'
) -> bool:
    """
    Write text file to S3.
    
    Args:
        bucket: S3 bucket name
        key: Object key/path
        content: Text content to write
        encoding: Text encoding (default: utf-8)
        
    Returns:
        True if successful, False otherwise
    """

def read_text_lines(
    self, 
    bucket: str, 
    key: str, 
    encoding: str = 'utf-8'
) -> Optional[List[str]]:
    """
    Read text file as list of lines.
    
    Args:
        bucket: S3 bucket name
        key: Object key/path
        encoding: Text encoding (default: utf-8)
        
    Returns:
        List of lines, None if error
    """
```

##### CSV Files

```python
def read_csv(
    self, 
    bucket: str, 
    key: str, 
    **pandas_kwargs
) -> Optional[pd.DataFrame]:
    """
    Read CSV file into pandas DataFrame.
    
    Args:
        bucket: S3 bucket name
        key: Object key/path
        **pandas_kwargs: Additional arguments for pd.read_csv()
        
    Returns:
        pandas DataFrame, None if error
    """

def write_csv(
    self, 
    bucket: str, 
    key: str, 
    dataframe: pd.DataFrame, 
    **pandas_kwargs
) -> bool:
    """
    Write pandas DataFrame as CSV file.
    
    Args:
        bucket: S3 bucket name
        key: Object key/path
        dataframe: pandas DataFrame to write
        **pandas_kwargs: Additional arguments for df.to_csv()
        
    Returns:
        True if successful, False otherwise
    """

def append_csv(
    self, 
    bucket: str, 
    key: str, 
    dataframe: pd.DataFrame
) -> bool:
    """
    Append DataFrame to existing CSV file.
    
    Args:
        bucket: S3 bucket name
        key: Object key/path
        dataframe: pandas DataFrame to append
        
    Returns:
        True if successful, False otherwise
    """
```

##### Parquet Files

```python
def read_parquet(
    self, 
    bucket: str, 
    key: str, 
    **pandas_kwargs
) -> Optional[pd.DataFrame]:
    """
    Read Parquet file into pandas DataFrame.
    
    Args:
        bucket: S3 bucket name
        key: Object key/path
        **pandas_kwargs: Additional arguments for pd.read_parquet()
        
    Returns:
        pandas DataFrame, None if error
    """

def write_parquet(
    self, 
    bucket: str, 
    key: str, 
    dataframe: pd.DataFrame, 
    **pandas_kwargs
) -> bool:
    """
    Write pandas DataFrame as Parquet file.
    
    Args:
        bucket: S3 bucket name
        key: Object key/path
        dataframe: pandas DataFrame to write
        **pandas_kwargs: Additional arguments for df.to_parquet()
        
    Returns:
        True if successful, False otherwise
    """
```

##### General Operations

```python
def delete_file(self, bucket: str, key: str) -> bool:
    """
    Delete file from S3.
    
    Args:
        bucket: S3 bucket name
        key: Object key/path
        
    Returns:
        True if successful, False otherwise
    """

def file_exists(self, bucket: str, key: str) -> bool:
    """
    Check if file exists in S3.
    
    Args:
        bucket: S3 bucket name
        key: Object key/path
        
    Returns:
        True if file exists, False otherwise
    """

def get_file_info(self, bucket: str, key: str) -> Optional[Dict]:
    """
    Get file metadata.
    
    Args:
        bucket: S3 bucket name
        key: Object key/path
        
    Returns:
        Dictionary with file info, None if error
    """

def list_files(
    self, 
    bucket: str, 
    prefix: str = '', 
    max_files: int = 1000
) -> List[str]:
    """
    List files in bucket.
    
    Args:
        bucket: S3 bucket name
        prefix: Key prefix filter
        max_files: Maximum number of files to return
        
    Returns:
        List of file keys
    """

def list_buckets(self) -> List[str]:
    """
    List all accessible buckets.
    
    Returns:
        List of bucket names
    """
```

##### Streaming Operations

```python
def stream_text_file(
    self, 
    bucket: str, 
    key: str, 
    chunk_size: int = None
) -> Iterator[str]:
    """
    Stream text file line by line.
    
    Args:
        bucket: S3 bucket name
        key: Object key/path
        chunk_size: Read chunk size
        
    Yields:
        Text lines
    """

def stream_csv_file(
    self, 
    bucket: str, 
    key: str, 
    chunk_size: int = 1000
) -> Iterator[pd.DataFrame]:
    """
    Stream CSV file in chunks.
    
    Args:
        bucket: S3 bucket name
        key: Object key/path
        chunk_size: Number of rows per chunk
        
    Yields:
        pandas DataFrame chunks
    """
```

---

## Usage Examples

### Text File Operations

```python
from netapp_s3_framework import NetAppS3Client

client = NetAppS3Client.from_env()

# Write text file
success = client.write_text(
    bucket="logs", 
    key="application.log", 
    content="Application started successfully"
)

# Read text file
log_content = client.read_text("logs", "application.log")
print(log_content)

# Read as lines
log_lines = client.read_text_lines("logs", "application.log")
for line in log_lines:
    print(f"Log: {line}")

# Stream large text file
for line in client.stream_text_file("logs", "large_log.txt"):
    if "ERROR" in line:
        print(f"Found error: {line}")
```

### CSV File Operations

```python
import pandas as pd

# Create sample data
df = pd.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35],
    'city': ['New York', 'London', 'Tokyo']
})

# Write CSV
client.write_csv("data", "users.csv", df, index=False)

# Read CSV
users_df = client.read_csv("data", "users.csv")
print(users_df.head())

# Read CSV with custom parameters
large_df = client.read_csv(
    "data", 
    "large_dataset.csv",
    chunksize=1000,  # Read in chunks
    usecols=['name', 'age'],  # Only specific columns
    dtype={'age': 'int32'}  # Specify data types
)

# Append to existing CSV
new_users = pd.DataFrame({
    'name': ['David', 'Eve'],
    'age': [28, 32],
    'city': ['Paris', 'Sydney']
})
client.append_csv("data", "users.csv", new_users)

# Stream large CSV file
total_rows = 0
for chunk in client.stream_csv_file("data", "huge_dataset.csv", chunk_size=5000):
    total_rows += len(chunk)
    # Process chunk
    processed_chunk = chunk[chunk['age'] > 25]
    print(f"Processed {len(processed_chunk)} rows")

print(f"Total rows processed: {total_rows}")
```

### Parquet File Operations

```python
# Write Parquet (more efficient than CSV)
client.write_parquet("data", "users.parquet", df)

# Read Parquet
users_parquet = client.read_parquet("data", "users.parquet")

# Write with compression
client.write_parquet(
    "data", 
    "compressed_data.parquet", 
    large_df,
    compression='snappy',
    index=False
)

# Read specific columns from Parquet
specific_cols = client.read_parquet(
    "data", 
    "large_dataset.parquet",
    columns=['name', 'age']
)
```

### File Management

```python
# Check if file exists
if client.file_exists("data", "users.csv"):
    print("File exists!")

# Get file information
file_info = client.get_file_info("data", "users.csv")
if file_info:
    print(f"File size: {file_info['size']} bytes")
    print(f"Last modified: {file_info['last_modified']}")

# List files with prefix
csv_files = client.list_files("data", prefix="reports/", max_files=100)
print(f"Found {len(csv_files)} CSV files")

# Delete file
if client.delete_file("data", "old_file.csv"):
    print("File deleted successfully")

# List all buckets
buckets = client.list_buckets()
print("Available buckets:", buckets)
```

### Batch Operations

```python
def process_all_csv_files(client, bucket, prefix):
    """Process all CSV files in a directory"""
    csv_files = [f for f in client.list_files(bucket, prefix) if f.endswith('.csv')]
    
    results = []
    for file_key in csv_files:
        try:
            df = client.read_csv(bucket, file_key)
            if df is not None:
                # Process dataframe
                summary = {
                    'file': file_key,
                    'rows': len(df),
                    'columns': len(df.columns),
                    'size_mb': client.get_file_info(bucket, file_key)['size'] / (1024*1024)
                }
                results.append(summary)
        except Exception as e:
            print(f"Error processing {file_key}: {e}")
    
    # Save summary as new CSV
    summary_df = pd.DataFrame(results)
    client.write_csv(bucket, f"{prefix}/summary.csv", summary_df)
    
    return summary_df

# Usage
summary = process_all_csv_files(client, "analytics", "reports/2024/")
```

---

## Error Handling

### Exception Types

The framework defines custom exceptions for better error handling:

```python
class NetAppS3Error(Exception):
    """Base exception for NetApp S3 operations"""
    pass

class ConnectionError(NetAppS3Error):
    """Connection-related errors"""
    pass

class AuthenticationError(NetAppS3Error):
    """Authentication/authorization errors"""
    pass

class FileNotFoundError(NetAppS3Error):
    """File not found errors"""
    pass

class InvalidFormatError(NetAppS3Error):
    """File format errors"""
    pass
```

### Error Handling Examples

```python
from netapp_s3_framework.exceptions import NetAppS3Error, FileNotFoundError

try:
    df = client.read_csv("data", "nonexistent.csv")
except FileNotFoundError:
    print("File not found, creating default dataset")
    df = pd.DataFrame()  # Create empty DataFrame
except NetAppS3Error as e:
    print(f"S3 operation failed: {e}")
    df = None
except Exception as e:
    print(f"Unexpected error: {e}")
    df = None

# Using context manager for automatic cleanup
with client.get_context() as ctx:
    try:
        df = ctx.read_csv("data", "important.csv")
        processed_df = process_data(df)
        ctx.write_csv("results", "processed.csv", processed_df)
    except Exception as e:
        ctx.rollback()  # Cleanup partial operations
        raise
```

### Retry Logic

```python
# Configure retry behavior
client = NetAppS3Client(
    endpoint_url="https://your-endpoint.com",
    access_key="your-key",
    secret_key="your-secret",
    max_retries=5,
    retry_delay=2.0,
    backoff_factor=2  # Exponential backoff
)

# Retry specific operations
@client.with_retry(max_attempts=3)
def reliable_upload(df):
    return client.write_parquet("data", "critical.parquet", df)

success = reliable_upload(important_dataframe)
```

---

## Best Practices

### 1. Configuration Management

```python
# ✅ Good: Use environment variables for credentials
client = NetAppS3Client.from_env()

# ✅ Good: Use configuration files for complex setups
client = NetAppS3Client.from_config_file("config.yaml")

# ❌ Avoid: Hardcoding credentials
client = NetAppS3Client(
    endpoint_url="https://endpoint.com",
    access_key="HARDCODED_KEY",  # Don't do this!
    secret_key="HARDCODED_SECRET"
)
```

### 2. File Format Selection

```python
# ✅ Good: Use Parquet for large datasets
client.write_parquet("data", "large_dataset.parquet", big_df)

# ✅ Good: Use CSV for small, human-readable data  
client.write_csv("reports", "summary.csv", small_df)

# ✅ Good: Use text files for logs and simple data
client.write_text("logs", "app.log", log_message)
```

### 3. Memory Management

```python
# ✅ Good: Stream large files
total_sum = 0
for chunk in client.stream_csv_file("data", "huge.csv", chunk_size=10000):
    total_sum += chunk['amount'].sum()

# ❌ Avoid: Loading huge files into memory
# huge_df = client.read_csv("data", "huge.csv")  # May cause OOM
```

### 4. Error Handling

```python
# ✅ Good: Specific error handling
try:
    df = client.read_csv("data", "file.csv")
except FileNotFoundError:
    df = create_default_dataframe()
except InvalidFormatError:
    df = client.read_csv("data", "file.csv", sep=';')  # Try different separator

# ✅ Good: Validate data before writing
if not df.empty and all(col in df.columns for col in required_columns):
    client.write_parquet("data", "validated.parquet", df)
```

### 5. Performance Optimization

```python
# ✅ Good: Use appropriate chunk sizes
for chunk in client.stream_csv_file("data", "file.csv", chunk_size=5000):
    process_chunk(chunk)

# ✅ Good: Use compression for large files
client.write_parquet("data", "compressed.parquet", df, compression='snappy')

# ✅ Good: Specify data types to save memory
df = client.read_csv("data", "file.csv", dtype={'id': 'int32', 'amount': 'float32'})
```

---

## Testing

### Unit Tests

```python
import pytest
from unittest.mock import Mock, patch
from netapp_s3_framework import NetAppS3Client

@pytest.fixture
def mock_client():
    with patch('netapp_s3_framework.s3fs.S3FileSystem'):
        client = NetAppS3Client(
            endpoint_url="https://test.com",
            access_key="test_key",
            secret_key="test_secret"
        )
        return client

def test_read_text_success(mock_client):
    # Mock successful text read
    mock_client.fs.open.return_value.__enter__.return_value.read.return_value = "test content"
    
    result = mock_client.read_text("test-bucket", "test.txt")
    
    assert result == "test content"
    mock_client.fs.open.assert_called_once_with("test-bucket/test.txt", 'r', encoding='utf-8')

def test_read_text_file_not_found(mock_client):
    # Mock file not found
    mock_client.fs.exists.return_value = False
    
    result = mock_client.read_text("test-bucket", "nonexistent.txt")
    
    assert result is None

def test_write_csv_success(mock_client):
    import pandas as pd
    
    df = pd.DataFrame({'a': [1, 2, 3]})
    mock_client.fs.open.return_value.__enter__.return_value = Mock()
    
    result = mock_client.write_csv("test-bucket", "test.csv", df)
    
    assert result is True
```

### Integration Tests

```python
import pytest
import pandas as pd
from netapp_s3_framework import NetAppS3Client

@pytest.fixture(scope="session")
def integration_client():
    """Real client for integration testing (requires test environment)"""
    return NetAppS3Client.from_env()

@pytest.fixture
def test_bucket():
    return "test-bucket-integration"

def test_csv_roundtrip(integration_client, test_bucket):
    """Test write then read CSV"""
    # Create test data
    original_df = pd.DataFrame({
        'name': ['Alice', 'Bob'],
        'age': [25, 30],
        'score': [95.5, 87.3]
    })
    
    key = "test_data/roundtrip.csv"
    
    try:
        # Write CSV
        assert integration_client.write_csv(test_bucket, key, original_df, index=False)
        
        # Read CSV back
        read_df = integration_client.read_csv(test_bucket, key)
        
        # Verify data integrity
        assert read_df is not None
        assert len(read_df) == len(original_df)
        assert list(read_df.columns) == list(original_df.columns)
        pd.testing.assert_frame_equal(read_df, original_df)
        
    finally:
        # Cleanup
        integration_client.delete_file(test_bucket, key)

def test_large_file_streaming(integration_client, test_bucket):
    """Test streaming large CSV file"""
    # Create large test dataset
    large_df = pd.DataFrame({
        'id': range(50000),
        'value': np.random.randn(50000)
    })
    
    key = "test_data/large_file.csv"
    
    try:
        # Write large file
        assert integration_client.write_csv(test_bucket, key, large_df, index=False)
        
        # Stream read and verify
        total_rows = 0
        for chunk in integration_client.stream_csv_file(test_bucket, key, chunk_size=5000):
            total_rows += len(chunk)
            assert 'id' in chunk.columns
            assert 'value' in chunk.columns
        
        assert total_rows == 50000
        
    finally:
        integration_client.delete_file(test_bucket, key)
```

### Running Tests

```bash
# Run unit tests
pytest tests/unit/ -v

# Run integration tests (requires test environment)
pytest tests/integration/ -v --env=test

# Run all tests with coverage
pytest --cov=netapp_s3_framework --cov-report=html

# Run performance tests
pytest tests/performance/ -v --benchmark-only
```

---

## Troubleshooting

### Common Issues and Solutions

#### 1. SSL Certificate Errors

**Problem**: `SSL: CERTIFICATE_VERIFY_FAILED` or similar SSL errors

**Solution**:
```python
# Option 1: Disable SSL verification
client = NetAppS3Client(
    endpoint_url="https://your-endpoint.com",
    access_key="your-key",
    secret_key="your-secret",
    ssl_verify=False
)

# Option 2: Specify certificate bundle
client = NetAppS3Client(
    endpoint_url="https://your-endpoint.com",
    access_key="your-key", 
    secret_key="your-secret",
    ssl_cert_path="/path/to/cert.pem"
)
```

#### 2. Connection Timeouts

**Problem**: Operations timeout or hang

**Solution**:
```python
client = NetAppS3Client(
    endpoint_url="https://your-endpoint.com",
    access_key="your-key",
    secret_key="your-secret",
    timeout=60,  # Increase timeout
    max_retries=5,  # Increase retry attempts
    retry_delay=2.0  # Add delay between retries
)
```

#### 3. Memory Issues with Large Files

**Problem**: Out of memory errors when reading large files

**Solution**:
```python
# Use streaming instead of loading entire file
total_rows = 0
for chunk in client.stream_csv_file("bucket", "large_file.csv", chunk_size=1000):
    # Process chunk instead of entire file
    processed = process_chunk(chunk)
    total_rows += len(chunk)
```

#### 4. Authentication Errors

**Problem**: `403 Forbidden` or `401 Unauthorized` errors

**Solution**:
```python
# Verify credentials
print(f"Using endpoint: {client.endpoint_url}")
print(f"Access key: {client.access_key[:8]}...")

# Test connection
try:
    buckets = client.list_buckets()
    print(f"Successfully connected. Buckets: {buckets}")
except Exception as e:
    print(f"Connection failed: {e}")
```

#### 5. File Format Issues

**Problem**: `ParserError` or format-related exceptions

**Solution**:
```python
# For CSV files with issues
try:
    df = client.read_csv("bucket", "problematic.csv")
except Exception as e:
    # Try different parameters
    df = client.read_csv("bucket", "problematic.csv", 
                        sep=';',  # Different separator
                        encoding='latin1',  # Different encoding
                        error_bad_lines=False)  # Skip bad lines
```

### Debugging

#### Enable Debug Logging

```python
import logging

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('netapp_s3_framework')
logger.setLevel(logging.DEBUG)

client = NetAppS3Client.from_env()
```

#### Inspect S3 Operations

```python
# Enable detailed S3 operation logging
client = NetAppS3Client(
    endpoint_url="https://your-endpoint.com",
    access_key="your-key",
    secret_key="your-secret",
    debug=True,  # Enable debug mode
    log_level="DEBUG"
)

# Operations will now show detailed logs
df = client.read_csv("bucket", "file.csv")
```

### Performance Monitoring

```python
import time
from netapp_s3_framework.monitoring import performance_monitor

@performance_monitor
def bulk_upload():
    for i in range(100):
        df = pd.DataFrame({'data': range(1000)})
        client.write_parquet("bucket", f"data_{i}.parquet", df)

# Run with monitoring
bulk_upload()
# Output: Operation completed in 45.2 seconds. Average: 0.45s per file
```

---

## Framework Architecture

### Core Components

```
netapp_s3_framework/
├── __init__.py              # Package initialization
├── client.py               # Main NetAppS3Client class
├── config.py               # Configuration management
├── exceptions.py           # Custom exceptions
├── operations/            
│   ├── __init__.py
│   ├── text_ops.py        # Text file operations
│   ├── csv_ops.py         # CSV file operations
│   └── parquet_ops.py     # Parquet file operations
├── utils/
│   ├── __init__.py
│   ├── ssl_handler.py     # SSL configuration
│   ├── retry.py           # Retry logic
│   └── logging.py         # Logging utilities
└── tests/
    ├── unit/              # Unit tests
    ├── integration/       # Integration tests
    └── fixtures/          # Test fixtures
```

### Extension Points

The framework is designed to be extensible:

```python
from netapp_s3_framework.operations.base import BaseOperation

class JSONOperation(BaseOperation):
    """Custom JSON file operations"""
    
    def read_json(self, bucket: str, key: str) -> dict:
        with self.fs.open(f"{bucket}/{key}", 'r') as f:
            return json.load(f)
    
    def write_json(self, bucket: str, key: str, data: dict) -> bool:
        try:
            with self.fs.open(f"{bucket}/{key}", 'w') as f:
                json.dump(data, f)
            return True
        except Exception as e:
            self.logger.error(f"Failed to write JSON: {e}")
            return False

# Register custom operation
client.register_operation('json', JSONOperation)

# Use custom operation
data = client.json.read_json("bucket", "config.json")
```

---

## License and Support

### License
This framework is released under the MIT License.

### Support
For issues and questions:
1. Check this documentation
2. Review common troubleshooting steps
3. Enable debug logging to diagnose issues
4. Create detailed bug reports with logs and configurations

### Contributing
1. Follow PEP 8 style guidelines
2. Add unit tests for new features
3. Update documentation for API changes
4. Use type hints for all public methods

---

## Complete Implementation

### Core Framework Files

#### `netapp_s3_framework/__init__.py`

```python
"""
NetApp S3 Framework
A comprehensive Python library for interacting with NetApp StorageGRID S3 object storage.
"""

from .client import NetAppS3Client
from .config import NetAppS3Config
from .exceptions import (
    NetAppS3Error,
    ConnectionError,
    AuthenticationError,
    FileNotFoundError,
    InvalidFormatError
)

__version__ = "1.0.0"
__author__ = "Your Organization"
__email__ = "support@yourorg.com"

__all__ = [
    'NetAppS3Client',
    'NetAppS3Config',
    'NetAppS3Error',
    'ConnectionError',
    'AuthenticationError', 
    'FileNotFoundError',
    'InvalidFormatError'
]
```

#### `netapp_s3_framework/client.py`

```python
"""
Main NetApp S3 Client implementation
"""

import os
import logging
from typing import Optional, List, Dict, Iterator, Union, Any
import pandas as pd
import s3fs
import ssl
import urllib3
from urllib3.exceptions import InsecureRequestWarning

from .config import NetAppS3Config
from .exceptions import NetAppS3Error, ConnectionError, FileNotFoundError
from .operations import TextOperations, CSVOperations, ParquetOperations
from .utils import setup_logging, retry_on_failure

class NetAppS3Client:
    """
    NetApp StorageGRID S3 client with support for text, CSV, and Parquet files.
    
    This client provides a high-level interface for interacting with NetApp S3
    storage, handling SSL configuration, retries, and multiple file formats.
    """
    
    def __init__(
        self,
        endpoint_url: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        config: Optional[NetAppS3Config] = None,
        **kwargs
    ):
        """
        Initialize NetApp S3 client.
        
        Args:
            endpoint_url: NetApp S3 endpoint URL
            access_key: S3 access key
            secret_key: S3 secret key
            config: Configuration object
            **kwargs: Additional configuration options
            
        Raises:
            ConnectionError: If unable to connect to S3
            ValueError: If required parameters are missing
        """
        # Use provided config or create from parameters
        if config is None:
            if not all([endpoint_url, access_key, secret_key]):
                raise ValueError("endpoint_url, access_key, and secret_key are required")
            
            config = NetAppS3Config(
                endpoint_url=endpoint_url,
                access_key=access_key,
                secret_key=secret_key,
                **kwargs
            )
        
        self.config = config
        self.logger = setup_logging(config.log_level)
        
        # Suppress SSL warnings if verification is disabled
        if not config.ssl_verify:
            urllib3.disable_warnings(InsecureRequestWarning)
        
        # Initialize S3 filesystem
        self.fs = self._create_s3_filesystem()
        
        # Initialize operation handlers
        self.text = TextOperations(self.fs, self.logger, self.config)
        self.csv = CSVOperations(self.fs, self.logger, self.config)
        self.parquet = ParquetOperations(self.fs, self.logger, self.config)
        
        # Test connection
        self._test_connection()
        
        self.logger.info(f"NetApp S3 client initialized successfully for {config.endpoint_url}")
    
    @classmethod
    def from_env(cls) -> 'NetAppS3Client':
        """
        Create client from environment variables.
        
        Expected environment variables:
        - NETAPP_ENDPOINT: S3 endpoint URL
        - NETAPP_ACCESS_KEY: Access key
        - NETAPP_SECRET_KEY: Secret key
        - NETAPP_DEFAULT_BUCKET: Default bucket (optional)
        - NETAPP_SSL_VERIFY: SSL verification (optional, default: false)
        - NETAPP_LOG_LEVEL: Logging level (optional, default: INFO)
        
        Returns:
            NetAppS3Client instance
            
        Raises:
            ValueError: If required environment variables are missing
        """
        from dotenv import load_dotenv
        load_dotenv()
        
        required_vars = ['NETAPP_ENDPOINT', 'NETAPP_ACCESS_KEY', 'NETAPP_SECRET_KEY']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")
        
        config = NetAppS3Config(
            endpoint_url=os.getenv('NETAPP_ENDPOINT'),
            access_key=os.getenv('NETAPP_ACCESS_KEY'),
            secret_key=os.getenv('NETAPP_SECRET_KEY'),
            default_bucket=os.getenv('NETAPP_DEFAULT_BUCKET'),
            ssl_verify=os.getenv('NETAPP_SSL_VERIFY', 'false').lower() != 'false',
            log_level=os.getenv('NETAPP_LOG_LEVEL', 'INFO'),
            region_name=os.getenv('NETAPP_REGION', 'us-east-1'),
            max_retries=int(os.getenv('NETAPP_MAX_RETRIES', '3')),
            retry_delay=float(os.getenv('NETAPP_RETRY_DELAY', '1.0')),
            chunk_size=int(os.getenv('NETAPP_CHUNK_SIZE', '8192'))
        )
        
        return cls(config=config)
    
    @classmethod
    def from_config_file(cls, config_path: str) -> 'NetAppS3Client':
        """
        Create client from configuration file.
        
        Args:
            config_path: Path to YAML configuration file
            
        Returns:
            NetAppS3Client instance
        """
        config = NetAppS3Config.from_file(config_path)
        return cls(config=config)
    
    def _create_s3_filesystem(self) -> s3fs.S3FileSystem:
        """Create and configure S3 filesystem object."""
        client_kwargs = {
            'region_name': self.config.region_name,
            'verify': self.config.ssl_verify
        }
        
        # Handle custom SSL context if needed
        if not self.config.ssl_verify:
            client_kwargs['verify'] = False
        elif self.config.ssl_cert_path:
            client_kwargs['verify'] = self.config.ssl_cert_path
        
        try:
            fs = s3fs.S3FileSystem(
                endpoint_url=self.config.endpoint_url,
                key=self.config.access_key,
                secret=self.config.secret_key,
                use_ssl=True,
                client_kwargs=client_kwargs
            )
            return fs
            
        except Exception as e:
            self.logger.error(f"Failed to create S3 filesystem: {e}")
            raise ConnectionError(f"Unable to connect to NetApp S3: {e}")
    
    def _test_connection(self):
        """Test S3 connection by listing buckets."""
        try:
            self.fs.ls('/')
            self.logger.debug("S3 connection test successful")
        except Exception as e:
            self.logger.error(f"S3 connection test failed: {e}")
            raise ConnectionError(f"Unable to connect to S3: {e}")
    
    # Text file operations
    def read_text(self, bucket: str, key: str, encoding: str = 'utf-8') -> Optional[str]:
        """Read text file from S3."""
        return self.text.read(bucket, key, encoding)
    
    def write_text(self, bucket: str, key: str, content: str, encoding: str = 'utf-8') -> bool:
        """Write text file to S3."""
        return self.text.write(bucket, key, content, encoding)
    
    def read_text_lines(self, bucket: str, key: str, encoding: str = 'utf-8') -> Optional[List[str]]:
        """Read text file as list of lines."""
        return self.text.read_lines(bucket, key, encoding)
    
    def stream_text_file(self, bucket: str, key: str, chunk_size: Optional[int] = None) -> Iterator[str]:
        """Stream text file line by line."""
        return self.text.stream(bucket, key, chunk_size or self.config.chunk_size)
    
    # CSV file operations
    def read_csv(self, bucket: str, key: str, **pandas_kwargs) -> Optional[pd.DataFrame]:
        """Read CSV file into pandas DataFrame."""
        return self.csv.read(bucket, key, **pandas_kwargs)
    
    def write_csv(self, bucket: str, key: str, dataframe: pd.DataFrame, **pandas_kwargs) -> bool:
        """Write pandas DataFrame as CSV file."""
        return self.csv.write(bucket, key, dataframe, **pandas_kwargs)
    
    def append_csv(self, bucket: str, key: str, dataframe: pd.DataFrame) -> bool:
        """Append DataFrame to existing CSV file."""
        return self.csv.append(bucket, key, dataframe)
    
    def stream_csv_file(self, bucket: str, key: str, chunk_size: int = 1000) -> Iterator[pd.DataFrame]:
        """Stream CSV file in chunks."""
        return self.csv.stream(bucket, key, chunk_size)
    
    # Parquet file operations
    def read_parquet(self, bucket: str, key: str, **pandas_kwargs) -> Optional[pd.DataFrame]:
        """Read Parquet file into pandas DataFrame."""
        return self.parquet.read(bucket, key, **pandas_kwargs)
    
    def write_parquet(self, bucket: str, key: str, dataframe: pd.DataFrame, **pandas_kwargs) -> bool:
        """Write pandas DataFrame as Parquet file."""
        return self.parquet.write(bucket, key, dataframe, **pandas_kwargs)
    
    # General file operations
    def delete_file(self, bucket: str, key: str) -> bool:
        """Delete file from S3."""
        s3_path = f"{bucket}/{key}"
        try:
            if self.fs.exists(s3_path):
                self.fs.rm(s3_path)
                self.logger.info(f"Deleted file: {s3_path}")
                return True
            else:
                self.logger.warning(f"File not found for deletion: {s3_path}")
                return False
        except Exception as e:
            self.logger.error(f"Failed to delete file {s3_path}: {e}")
            return False
    
    def file_exists(self, bucket: str, key: str) -> bool:
        """Check if file exists in S3."""
        s3_path = f"{bucket}/{key}"
        try:
            return self.fs.exists(s3_path)
        except Exception as e:
            self.logger.error(f"Failed to check file existence {s3_path}: {e}")
            return False
    
    def get_file_info(self, bucket: str, key: str) -> Optional[Dict]:
        """Get file metadata."""
        s3_path = f"{bucket}/{key}"
        try:
            info = self.fs.info(s3_path)
            return {
                'size': info.get('size', 0),
                'last_modified': info.get('LastModified'),
                'etag': info.get('ETag'),
                'content_type': info.get('ContentType'),
                'path': s3_path
            }
        except Exception as e:
            self.logger.error(f"Failed to get file info for {s3_path}: {e}")
            return None
    
    def list_files(self, bucket: str, prefix: str = '', max_files: int = 1000) -> List[str]:
        """List files in bucket."""
        try:
            if prefix:
                path = f"{bucket}/{prefix}"
            else:
                path = bucket
            
            files = self.fs.ls(path, maxkeys=max_files)
            # Remove bucket prefix from file paths
            cleaned_files = [f.replace(f"{bucket}/", "") for f in files if f != bucket]
            
            self.logger.debug(f"Listed {len(cleaned_files)} files from {path}")
            return cleaned_files
            
        except Exception as e:
            self.logger.error(f"Failed to list files in {bucket}/{prefix}: {e}")
            return []
    
    def list_buckets(self) -> List[str]:
        """List all accessible buckets."""
        try:
            buckets = self.fs.ls('/')
            self.logger.debug(f"Listed {len(buckets)} buckets")
            return buckets
        except Exception as e:
            self.logger.error(f"Failed to list buckets: {e}")
            return []
    
    def get_bucket_info(self, bucket: str) -> Optional[Dict]:
        """Get bucket information."""
        try:
            info = self.fs.info(bucket)
            return {
                'name': bucket,
                'creation_date': info.get('CreationDate'),
                'size': info.get('size', 0)
            }
        except Exception as e:
            self.logger.error(f"Failed to get bucket info for {bucket}: {e}")
            return None
    
    # Utility methods
    def copy_file(self, src_bucket: str, src_key: str, dst_bucket: str, dst_key: str) -> bool:
        """Copy file between S3 locations."""
        src_path = f"{src_bucket}/{src_key}"
        dst_path = f"{dst_bucket}/{dst_key}"
        
        try:
            self.fs.copy(src_path, dst_path)
            self.logger.info(f"Copied file from {src_path} to {dst_path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to copy file from {src_path} to {dst_path}: {e}")
            return False
    
    def move_file(self, src_bucket: str, src_key: str, dst_bucket: str, dst_key: str) -> bool:
        """Move file between S3 locations."""
        if self.copy_file(src_bucket, src_key, dst_bucket, dst_key):
            return self.delete_file(src_bucket, src_key)
        return False
    
    @retry_on_failure(max_attempts=3)
    def sync_directory(self, local_dir: str, bucket: str, prefix: str = '') -> bool:
        """Sync local directory to S3."""
        import os
        from pathlib import Path
        
        local_path = Path(local_dir)
        if not local_path.exists():
            self.logger.error(f"Local directory does not exist: {local_dir}")
            return False
        
        try:
            uploaded_count = 0
            for file_path in local_path.rglob('*'):
                if file_path.is_file():
                    relative_path = file_path.relative_to(local_path)
                    s3_key = f"{prefix}/{relative_path}".lstrip('/')
                    
                    # Determine file type and upload accordingly
                    if file_path.suffix.lower() == '.csv':
                        df = pd.read_csv(file_path)
                        success = self.write_csv(bucket, s3_key, df)
                    elif file_path.suffix.lower() == '.parquet':
                        df = pd.read_parquet(file_path)
                        success = self.write_parquet(bucket, s3_key, df)
                    else:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                        success = self.write_text(bucket, s3_key, content)
                    
                    if success:
                        uploaded_count += 1
            
            self.logger.info(f"Synced {uploaded_count} files from {local_dir} to s3://{bucket}/{prefix}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to sync directory {local_dir}: {e}")
            return False
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        try:
            # Cleanup resources if needed
            self.logger.debug("NetApp S3 client context closed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
    
    def __repr__(self):
        """String representation."""
        return f"NetAppS3Client(endpoint='{self.config.endpoint_url}', buckets={len(self.list_buckets())})"
```

#### `netapp_s3_framework/config.py`

```python
"""
Configuration management for NetApp S3 client
"""

import os
import yaml
from typing import Optional, Any, Dict
from dataclasses import dataclass, field

@dataclass
class NetAppS3Config:
    """Configuration class for NetApp S3 client."""
    
    endpoint_url: str
    access_key: str
    secret_key: str
    region_name: str = 'us-east-1'
    default_bucket: Optional[str] = None
    ssl_verify: bool = False
    ssl_cert_path: Optional[str] = None
    max_retries: int = 3
    retry_delay: float = 1.0
    backoff_factor: float = 2.0
    timeout: int = 60
    chunk_size: int = 8192
    log_level: str = 'INFO'
    debug: bool = False
    
    # Advanced options
    connection_pool_size: int = 10
    max_connections: int = 10
    use_threads: bool = True
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.endpoint_url:
            raise ValueError("endpoint_url is required")
        if not self.access_key:
            raise ValueError("access_key is required")
        if not self.secret_key:
            raise ValueError("secret_key is required")
        
        # Ensure endpoint URL has proper format
        if not self.endpoint_url.startswith(('http://', 'https://')):
            self.endpoint_url = f"https://{self.endpoint_url}"
    
    @classmethod
    def from_file(cls, config_path: str) -> 'NetAppS3Config':
        """Load configuration from YAML file."""
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        return cls(**config_data)
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'NetAppS3Config':
        """Create configuration from dictionary."""
        return cls(**config_dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'endpoint_url': self.endpoint_url,
            'access_key': self.access_key,
            'secret_key': '***',  # Mask secret for security
            'region_name': self.region_name,
            'default_bucket': self.default_bucket,
            'ssl_verify': self.ssl_verify,
            'ssl_cert_path': self.ssl_cert_path,
            'max_retries': self.max_retries,
            'retry_delay': self.retry_delay,
            'timeout': self.timeout,
            'chunk_size': self.chunk_size,
            'log_level': self.log_level,
            'debug': self.debug
        }
    
    def save_to_file(self, config_path: str, mask_secrets: bool = True):
        """Save configuration to YAML file."""
        config_dict = self.to_dict()
        if not mask_secrets:
            config_dict['secret_key'] = self.secret_key
        
        with open(config_path, 'w') as f:
            yaml.dump(config_dict, f, default_flow_style=False)
    
    def validate(self) -> bool:
        """Validate configuration parameters."""
        checks = [
            (self.max_retries >= 0, "max_retries must be non-negative"),
            (self.retry_delay >= 0, "retry_delay must be non-negative"),
            (self.timeout > 0, "timeout must be positive"),
            (self.chunk_size > 0, "chunk_size must be positive"),
            (self.log_level in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], 
             "log_level must be valid logging level")
        ]
        
        for check, message in checks:
            if not check:
                raise ValueError(message)
        
        return True
```

#### `netapp_s3_framework/exceptions.py`

```python
"""
Custom exceptions for NetApp S3 framework
"""

class NetAppS3Error(Exception):
    """Base exception for NetApp S3 operations."""
    
    def __init__(self, message: str, error_code: str = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code

class ConnectionError(NetAppS3Error):
    """Raised when connection to S3 fails."""
    pass

class AuthenticationError(NetAppS3Error):
    """Raised when authentication fails."""
    pass

class FileNotFoundError(NetAppS3Error):
    """Raised when requested file is not found."""
    pass

class InvalidFormatError(NetAppS3Error):
    """Raised when file format is invalid or unsupported."""
    pass

class PermissionError(NetAppS3Error):
    """Raised when access is denied."""
    pass

class QuotaExceededError(NetAppS3Error):
    """Raised when storage quota is exceeded."""
    pass

class TimeoutError(NetAppS3Error):
    """Raised when operation times out."""
    pass

class ValidationError(NetAppS3Error):
    """Raised when data validation fails."""
    pass
```

---

## Installation and Setup Guide

### Step 1: Install Dependencies

```bash
pip install s3fs pandas pyarrow python-dotenv pyyaml typing-extensions
```

### Step 2: Create Project Structure

```
your_project/
├── netapp_s3_framework/     # Copy the framework files here
├── config/
│   ├── .env                 # Environment variables
│   └── config.yaml          # Configuration file
├── examples/                # Usage examples
├── tests/                   # Test files
└── requirements.txt         # Dependencies
```

### Step 3: Configure Environment Variables

Create `.env` file:

```bash
# NetApp StorageGRID Configuration
NETAPP_ENDPOINT=https://your-netapp-storagegrid-endpoint.com
NETAPP_ACCESS_KEY=your-tenant-access-key
NETAPP_SECRET_KEY=your-tenant-secret-key
NETAPP_DEFAULT_BUCKET=your-default-bucket
NETAPP_SSL_VERIFY=false
NETAPP_LOG_LEVEL=INFO
NETAPP_REGION=us-east-1
NETAPP_MAX_RETRIES=3
NETAPP_RETRY_DELAY=1.0
NETAPP_CHUNK_SIZE=8192
```

### Step 4: Basic Usage Script

Create `example_usage.py`:

```python
#!/usr/bin/env python3
"""
NetApp S3 Framework usage example
"""

import pandas as pd
import numpy as np
from netapp_s3_framework import NetAppS3Client

def main():
    # Initialize client from environment variables
    print("Initializing NetApp S3 client...")
    client = NetAppS3Client.from_env()
    
    # Test connection
    buckets = client.list_buckets()
    print(f"Connected successfully! Available buckets: {buckets}")
    
    # Use first bucket or default bucket
    bucket_name = client.config.default_bucket or buckets[0] if buckets else "test-bucket"
    print(f"Using bucket: {bucket_name}")
    
    # Example 1: Text file operations
    print("\n=== Text File Operations ===")
    
    # Write text file
    log_message = f"Application started at {pd.Timestamp.now()}\nSystem initialized successfully"
    success = client.write_text(bucket_name, "logs/app.log", log_message)
    print(f"Text file written: {success}")
    
    # Read text file
    content = client.read_text(bucket_name, "logs/app.log")
    print(f"Text content: {content}")
    
    # Example 2: CSV file operations
    print("\n=== CSV File Operations ===")
    
    # Create sample DataFrame
    df = pd.DataFrame({
        'id': range(1, 101),
        'name': [f'User_{i}' for i in range(1, 101)],
        'score': np.random.randint(60, 100, 100),
        'timestamp': pd.date_range('2024-01-01', periods=100, freq='1H')
    })
    
    # Write CSV file
    success = client.write_csv(bucket_name, "data/users.csv", df, index=False)
    print(f"CSV file written: {success}")
    
    # Read CSV file
    df_read = client.read_csv(bucket_name, "data/users.csv")
    if df_read is not None:
        print(f"CSV file read successfully: {df_read.shape}")
        print(f"First few rows:\n{df_read.head()}")
    
    # Example 3: Parquet file operations
    print("\n=== Parquet File Operations ===")
    
    # Write Parquet file (more efficient for large datasets)
    success = client.write_parquet(bucket_name, "data/users.parquet", df, compression='snappy')
    print(f"Parquet file written: {success}")
    
    # Read Parquet file
    df_parquet = client.read_parquet(bucket_name, "data/users.parquet")
    if df_parquet is not None:
        print(f"Parquet file read successfully: {df_parquet.shape}")
    
    # Example 4: File management
    print("\n=== File Management ===")
    
    # List files
    files = client.list_files(bucket_name, prefix="data/")
    print(f"Files in data/ directory: {files}")
    
    # Get file info
    file_info = client.get_file_info(bucket_name, "data/users.csv")
    if file_info:
        print(f"CSV file size: {file_info['size']} bytes")
    
    # Check if file exists
    exists = client.file_exists(bucket_name, "data/users.parquet")
    print(f"Parquet file exists: {exists}")
    
    # Example 5: Streaming large files
    print("\n=== Streaming Operations ===")
    
    # Create larger dataset for streaming example
    large_df = pd.DataFrame({
        'id': range(1, 10001),
        'value': np.random.randn(10000),
        'category': np.random.choice(['A', 'B', 'C'], 10000)
    })
    
    # Write large CSV
    client.write_csv(bucket_name, "data/large_dataset.csv", large_df, index=False)
    
    # Stream read in chunks
    total_rows = 0
    chunk_count = 0
    
    for chunk in client.stream_csv_file(bucket_name, "data/large_dataset.csv", chunk_size=1000):
        total_rows += len(chunk)
        chunk_count += 1
        
        if chunk_count <= 3:  # Show first 3 chunks
            print(f"Chunk {chunk_count}: {len(chunk)} rows, mean value: {chunk['value'].mean():.3f}")
    
    print(f"Streamed total rows: {total_rows} in {chunk_count} chunks")
    
    # Cleanup example files (optional)
    print("\n=== Cleanup ===")
    cleanup_files = [
        "logs/app.log",
        "data/users.csv", 
        "data/users.parquet",
        "data/large_dataset.csv"
    ]
    
    for file_key in cleanup_files:
        deleted = client.delete_file(bucket_name, file_key)
        print(f"Deleted {file_key}: {deleted}")
    
    print("\nExample completed successfully!")

if __name__ == "__main__":
    main()
```

### Step 5: Run the Example

```bash
python example_usage.py
```

---

## Production Deployment Considerations

### Security Best Practices

1. **Credential Management**
   ```bash
   # Use environment variables or secure vaults
   export NETAPP_ACCESS_KEY="your-access-key"
   export NETAPP_SECRET_KEY="your-secret-key"
   
   # Never commit credentials to version control
   echo "*.env" >> .gitignore
   echo "config/*.yaml" >> .gitignore
   ```

2. **SSL Configuration**
   ```python
   # For production, use proper SSL certificates
   client = NetAppS3Client(
       endpoint_url="https://secure-netapp-endpoint.com",
       access_key=os.environ['NETAPP_ACCESS_KEY'],
       secret_key=os.environ['NETAPP_SECRET_KEY'],
       ssl_verify=True,  # Always verify SSL in production
       ssl_cert_path="/path/to/certificate.pem"
   )
   ```

### Performance Optimization

1. **Connection Pooling**
   ```python
   config = NetAppS3Config(
       endpoint_url="https://your-endpoint.com",
       access_key="your-key",
       secret_key="your-secret",
       connection_pool_size=20,  # Increase for high concurrency
       max_connections=20,
       use_threads=True
   )
   ```

2. **Batch Operations**
   ```python
   def batch_upload_files(client, file_list, bucket):
       """Upload multiple files efficiently"""
       from concurrent.futures import ThreadPoolExecutor
       
       def upload_file(file_info):
           return client.write_csv(bucket, file_info['key'], file_info['data'])
       
       with ThreadPoolExecutor(max_workers=5) as executor:
           results = list(executor.map(upload_file, file_list))
       
       return results
   ```

### Monitoring and Logging

1. **Structured Logging**
   ```python
   import structlog
   
   # Configure structured logging
   structlog.configure(
       processors=[
           structlog.stdlib.filter_by_level,
           structlog.stdlib.add_logger_name,
           structlog.stdlib.add_log_level,
           structlog.stdlib.PositionalArgumentsFormatter(),
           structlog.processors.TimeStamper(fmt="iso"),
           structlog.processors.StackInfoRenderer(),
           structlog.processors.format_exc_info,
           structlog.processors.JSONRenderer()
       ],
       context_class=dict,
       logger_factory=structlog.stdlib.LoggerFactory(),
       wrapper_class=structlog.stdlib.BoundLogger,
       cache_logger_on_first_use=True,
   )
   ```

2. **Performance Metrics**
   ```python
   import time
   from functools import wraps
   
   def monitor_performance(func):
       """Decorator to monitor function performance"""
       @wraps(func)
       def wrapper(*args, **kwargs):
           start_time = time.time()
           try:
               result = func(*args, **kwargs)
               duration = time.time() - start_time
               logger.info(f"{func.__name__} completed", duration=duration, success=True)
               return result
           except Exception as e:
               duration = time.time() - start_time
               logger.error(f"{func.__name__} failed", duration=duration, error=str(e))
               raise
       return wrapper
   
   # Apply to client methods
   client.read_csv = monitor_performance(client.read_csv)
   ```

---

## Complete Operations Implementation

### `netapp_s3_framework/operations/__init__.py`

```python
"""
Operations module for different file types
"""

from .text_ops import TextOperations
from .csv_ops import CSVOperations  
from .parquet_ops import ParquetOperations
from .base import BaseOperation

__all__ = [
    'TextOperations',
    'CSVOperations', 
    'ParquetOperations',
    'BaseOperation'
]
```

### `netapp_s3_framework/operations/base.py`

```python
"""
Base operation class for common functionality
"""

import logging
from typing import Optional, Any
from abc import ABC, abstractmethod
from ..exceptions import NetAppS3Error, FileNotFoundError
from ..utils import retry_on_failure

class BaseOperation(ABC):
    """Base class for all file operations."""
    
    def __init__(self, fs, logger: logging.Logger, config):
        self.fs = fs
        self.logger = logger
        self.config = config
    
    def _get_s3_path(self, bucket: str, key: str) -> str:
        """Generate S3 path from bucket and key."""
        return f"{bucket}/{key}"
    
    def _check_file_exists(self, bucket: str, key: str) -> bool:
        """Check if file exists and raise appropriate exception if not."""
        s3_path = self._get_s3_path(bucket, key)
        try:
            exists = self.fs.exists(s3_path)
            if not exists:
                raise FileNotFoundError(f"File not found: {s3_path}")
            return True
        except Exception as e:
            if "not found" in str(e).lower():
                raise FileNotFoundError(f"File not found: {s3_path}")
            raise NetAppS3Error(f"Error checking file existence: {e}")
    
    @retry_on_failure(max_attempts=3)
    def _safe_operation(self, operation_func, *args, **kwargs):
        """Execute operation with retry logic and error handling."""
        try:
            return operation_func(*args, **kwargs)
        except Exception as e:
            self.logger.error(f"Operation failed: {e}")
            raise
    
    @abstractmethod
    def read(self, bucket: str, key: str, **kwargs) -> Any:
        """Abstract read method to be implemented by subclasses."""
        pass
    
    @abstractmethod  
    def write(self, bucket: str, key: str, data: Any, **kwargs) -> bool:
        """Abstract write method to be implemented by subclasses."""
        pass
```

### `netapp_s3_framework/operations/text_ops.py`

```python
"""
Text file operations
"""

from typing import Optional, List, Iterator
from .base import BaseOperation
from ..exceptions import FileNotFoundError, NetAppS3Error

class TextOperations(BaseOperation):
    """Handle text file operations."""
    
    def read(self, bucket: str, key: str, encoding: str = 'utf-8') -> Optional[str]:
        """Read text file from S3."""
        s3_path = self._get_s3_path(bucket, key)
        
        try:
            self._check_file_exists(bucket, key)
            
            with self.fs.open(s3_path, 'r', encoding=encoding) as f:
                content = f.read()
            
            self.logger.debug(f"Read text file: {s3_path} ({len(content)} characters)")
            return content
            
        except FileNotFoundError:
            self.logger.warning(f"Text file not found: {s3_path}")
            return None
        except UnicodeDecodeError as e:
            self.logger.error(f"Encoding error reading {s3_path}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Failed to read text file {s3_path}: {e}")
            return None
    
    def write(self, bucket: str, key: str, content: str, encoding: str = 'utf-8') -> bool:
        """Write text file to S3."""
        s3_path = self._get_s3_path(bucket, key)
        
        try:
            with self.fs.open(s3_path, 'w', encoding=encoding) as f:
                f.write(content)
            
            self.logger.info(f"Wrote text file: {s3_path} ({len(content)} characters)")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to write text file {s3_path}: {e}")
            return False
    
    def read_lines(self, bucket: str, key: str, encoding: str = 'utf-8') -> Optional[List[str]]:
        """Read text file as list of lines."""
        s3_path = self._get_s3_path(bucket, key)
        
        try:
            self._check_file_exists(bucket, key)
            
            with self.fs.open(s3_path, 'r', encoding=encoding) as f:
                lines = [line.rstrip('\n\r') for line in f]
            
            self.logger.debug(f"Read text file lines: {s3_path} ({len(lines)} lines)")
            return lines
            
        except FileNotFoundError:
            self.logger.warning(f"Text file not found: {s3_path}")
            return None
        except Exception as e:
            self.logger.error(f"Failed to read text file lines {s3_path}: {e}")
            return None
    
    def append(self, bucket: str, key: str, content: str, encoding: str = 'utf-8') -> bool:
        """Append content to text file."""
        s3_path = self._get_s3_path(bucket, key)
        
        try:
            # Read existing content if file exists
            existing_content = ""
            if self.fs.exists(s3_path):
                existing_content = self.read(bucket, key, encoding)
                if existing_content is None:
                    existing_content = ""
            
            # Append new content
            new_content = existing_content + content
            
            return self.write(bucket, key, new_content, encoding)
            
        except Exception as e:
            self.logger.error(f"Failed to append to text file {s3_path}: {e}")
            return False
    
    def stream(self, bucket: str, key: str, chunk_size: int = 8192) -> Iterator[str]:
        """Stream text file line by line."""
        s3_path = self._get_s3_path(bucket, key)
        
        try:
            self._check_file_exists(bucket, key)
            
            with self.fs.open(s3_path, 'r') as f:
                for line in f:
                    yield line.rstrip('\n\r')
                    
        except FileNotFoundError:
            self.logger.warning(f"Text file not found for streaming: {s3_path}")
            return
        except Exception as e:
            self.logger.error(f"Failed to stream text file {s3_path}: {e}")
            return
```

### `netapp_s3_framework/operations/csv_ops.py`

```python
"""
CSV file operations
"""

import pandas as pd
from typing import Optional, Iterator
from io import StringIO
from .base import BaseOperation
from ..exceptions import FileNotFoundError, InvalidFormatError

class CSVOperations(BaseOperation):
    """Handle CSV file operations."""
    
    def read(self, bucket: str, key: str, **pandas_kwargs) -> Optional[pd.DataFrame]:
        """Read CSV file into pandas DataFrame."""
        s3_path = self._get_s3_path(bucket, key)
        
        try:
            self._check_file_exists(bucket, key)
            
            with self.fs.open(s3_path, 'r') as f:
                df = pd.read_csv(f, **pandas_kwargs)
            
            self.logger.info(f"Read CSV file: {s3_path} ({df.shape[0]} rows, {df.shape[1]} columns)")
            return df
            
        except FileNotFoundError:
            self.logger.warning(f"CSV file not found: {s3_path}")
            return None
        except pd.errors.EmptyDataError:
            self.logger.warning(f"CSV file is empty: {s3_path}")
            return pd.DataFrame()
        except pd.errors.ParserError as e:
            self.logger.error(f"CSV parsing error in {s3_path}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Failed to read CSV file {s3_path}: {e}")
            return None
    
    def write(self, bucket: str, key: str, dataframe: pd.DataFrame, **pandas_kwargs) -> bool:
        """Write pandas DataFrame as CSV file."""
        s3_path = self._get_s3_path(bucket, key)
        
        try:
            if dataframe.empty:
                self.logger.warning(f"Writing empty DataFrame to {s3_path}")
            
            with self.fs.open(s3_path, 'w') as f:
                dataframe.to_csv(f, **pandas_kwargs)
            
            self.logger.info(f"Wrote CSV file: {s3_path} ({dataframe.shape[0]} rows, {dataframe.shape[1]} columns)")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to write CSV file {s3_path}: {e}")
            return False
    
    def append(self, bucket: str, key: str, dataframe: pd.DataFrame) -> bool:
        """Append DataFrame to existing CSV file."""
        s3_path = self._get_s3_path(bucket, key)
        
        try:
            if self.fs.exists(s3_path):
                # Read existing data
                existing_df = self.read(bucket, key)
                if existing_df is not None:
                    # Combine with new data
                    combined_df = pd.concat([existing_df, dataframe], ignore_index=True)
                    return self.write(bucket, key, combined_df, index=False)
                else:
                    # If read failed, just write new data
                    return self.write(bucket, key, dataframe, index=False)
            else:
                # File doesn't exist, create new one
                return self.write(bucket, key, dataframe, index=False)
                
        except Exception as e:
            self.logger.error(f"Failed to append to CSV file {s3_path}: {e}")
            return False
    
    def stream(self, bucket: str, key: str, chunk_size: int = 1000) -> Iterator[pd.DataFrame]:
        """Stream CSV file in chunks."""
        s3_path = self._get_s3_path(bucket, key)
        
        try:
            self._check_file_exists(bucket, key)
            
            with self.fs.open(s3_path, 'r') as f:
                # Use pandas chunking capability
                chunk_iter = pd.read_csv(f, chunksize=chunk_size)
                chunk_count = 0
                
                for chunk in chunk_iter:
                    chunk_count += 1
                    self.logger.debug(f"Streaming chunk {chunk_count}: {len(chunk)} rows")
                    yield chunk
                    
        except FileNotFoundError:
            self.logger.warning(f"CSV file not found for streaming: {s3_path}")
            return
        except Exception as e:
            self.logger.error(f"Failed to stream CSV file {s3_path}: {e}")
            return
    
    def get_column_info(self, bucket: str, key: str) -> Optional[dict]:
        """Get column information without reading entire file."""
        try:
            # Read just the first few rows to get column info
            sample_df = self.read(bucket, key, nrows=5)
            if sample_df is not None:
                column_info = {
                    'columns': sample_df.columns.tolist(),
                    'dtypes': sample_df.dtypes.to_dict(),
                    'column_count': len(sample_df.columns)
                }
                return column_info
            return None
        except Exception as e:
            self.logger.error(f"Failed to get column info: {e}")
            return None
    
    def validate_csv(self, bucket: str, key: str, required_columns: list = None) -> dict:
        """Validate CSV file structure and content."""
        validation_result = {
            'is_valid': False,
            'errors': [],
            'warnings': [],
            'row_count': 0,
            'column_count': 0,
            'columns': []
        }
        
        try:
            df = self.read(bucket, key)
            if df is None:
                validation_result['errors'].append("Could not read CSV file")
                return validation_result
            
            validation_result['row_count'] = len(df)
            validation_result['column_count'] = len(df.columns)
            validation_result['columns'] = df.columns.tolist()
            
            # Check for required columns
            if required_columns:
                missing_cols = set(required_columns) - set(df.columns)
                if missing_cols:
                    validation_result['errors'].append(f"Missing required columns: {list(missing_cols)}")
            
            # Check for empty DataFrame
            if df.empty:
                validation_result['warnings'].append("CSV file is empty")
            
            # Check for duplicate columns
            if len(df.columns) != len(set(df.columns)):
                validation_result['errors'].append("Duplicate column names found")
            
            # Check for null values
            null_counts = df.isnull().sum()
            null_columns = null_counts[null_counts > 0].to_dict()
            if null_columns:
                validation_result['warnings'].append(f"Null values found in columns: {null_columns}")
            
            validation_result['is_valid'] = len(validation_result['errors']) == 0
            
        except Exception as e:
            validation_result['errors'].append(f"Validation failed: {str(e)}")
        
        return validation_result
```

### `netapp_s3_framework/operations/parquet_ops.py`

```python
"""
Parquet file operations
"""

import pandas as pd
from typing import Optional, List
from .base import BaseOperation
from ..exceptions import FileNotFoundError, InvalidFormatError

class ParquetOperations(BaseOperation):
    """Handle Parquet file operations."""
    
    def read(self, bucket: str, key: str, **pandas_kwargs) -> Optional[pd.DataFrame]:
        """Read Parquet file into pandas DataFrame."""
        s3_path = self._get_s3_path(bucket, key)
        
        try:
            self._check_file_exists(bucket, key)
            
            with self.fs.open(s3_path, 'rb') as f:
                df = pd.read_parquet(f, **pandas_kwargs)
            
            self.logger.info(f"Read Parquet file: {s3_path} ({df.shape[0]} rows, {df.shape[1]} columns)")
            return df
            
        except FileNotFoundError:
            self.logger.warning(f"Parquet file not found: {s3_path}")
            return None
        except Exception as e:
            self.logger.error(f"Failed to read Parquet file {s3_path}: {e}")
            return None
    
    def write(self, bucket: str, key: str, dataframe: pd.DataFrame, **pandas_kwargs) -> bool:
        """Write pandas DataFrame as Parquet file."""
        s3_path = self._get_s3_path(bucket, key)
        
        try:
            if dataframe.empty:
                self.logger.warning(f"Writing empty DataFrame to {s3_path}")
            
            # Set default compression if not specified
            if 'compression' not in pandas_kwargs:
                pandas_kwargs['compression'] = 'snappy'
            
            with self.fs.open(s3_path, 'wb') as f:
                dataframe.to_parquet(f, **pandas_kwargs)
            
            self.logger.info(f"Wrote Parquet file: {s3_path} ({dataframe.shape[0]} rows, {dataframe.shape[1]} columns)")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to write Parquet file {s3_path}: {e}")
            return False
    
    def append(self, bucket: str, key: str, dataframe: pd.DataFrame) -> bool:
        """Append DataFrame to existing Parquet file."""
        s3_path = self._get_s3_path(bucket, key)
        
        try:
            if self.fs.exists(s3_path):
                # Read existing data
                existing_df = self.read(bucket, key)
                if existing_df is not None:
                    # Combine with new data
                    combined_df = pd.concat([existing_df, dataframe], ignore_index=True)
                    return self.write(bucket, key, combined_df)
                else:
                    # If read failed, just write new data
                    return self.write(bucket, key, dataframe)
            else:
                # File doesn't exist, create new one
                return self.write(bucket, key, dataframe)
                
        except Exception as e:
            self.logger.error(f"Failed to append to Parquet file {s3_path}: {e}")
            return False
    
    def read_columns(self, bucket: str, key: str, columns: List[str]) -> Optional[pd.DataFrame]:
        """Read only specific columns from Parquet file."""
        return self.read(bucket, key, columns=columns)
    
    def read_filtered(self, bucket: str, key: str, filters: List[tuple]) -> Optional[pd.DataFrame]:
        """Read Parquet file with filters applied."""
        return self.read(bucket, key, filters=filters)
    
    def get_metadata(self, bucket: str, key: str) -> Optional[dict]:
        """Get Parquet file metadata without reading data."""
        s3_path = self._get_s3_path(bucket, key)
        
        try:
            import pyarrow.parquet as pq
            
            with self.fs.open(s3_path, 'rb') as f:
                parquet_file = pq.ParquetFile(f)
                metadata = parquet_file.metadata
                schema = parquet_file.schema
                
                info = {
                    'num_rows': metadata.num_rows,
                    'num_columns': metadata.num_columns,
                    'num_row_groups': metadata.num_row_groups,
                    'file_size': metadata.serialized_size,
                    'columns': [field.name for field in schema],
                    'column_types': {field.name: str(field.type) for field in schema},
                    'compression': str(metadata.row_group(0).column(0).compression) if metadata.num_row_groups > 0 else 'unknown'
                }
                
                return info
                
        except ImportError:
            self.logger.error("pyarrow not available for metadata extraction")
            return None
        except Exception as e:
            self.logger.error(f"Failed to get Parquet metadata {s3_path}: {e}")
            return None
    
    def optimize_file(self, bucket: str, key: str, output_key: str = None) -> bool:
        """Optimize Parquet file by rewriting with better compression/partitioning."""
        if output_key is None:
            output_key = key
        
        try:
            df = self.read(bucket, key)
            if df is not None:
                # Write with optimal settings
                return self.write(bucket, output_key, df, 
                                compression='snappy',
                                index=False,
                                engine='pyarrow')
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to optimize Parquet file: {e}")
            return False
```

### `netapp_s3_framework/utils/__init__.py`

```python
"""
Utility functions and decorators
"""

from .logging import setup_logging
from .retry import retry_on_failure
from .ssl_handler import create_ssl_context
from .validators import validate_bucket_name, validate_key

__all__ = [
    'setup_logging',
    'retry_on_failure', 
    'create_ssl_context',
    'validate_bucket_name',
    'validate_key'
]
```

### `netapp_s3_framework/utils/logging.py`

```python
"""
Logging utilities
"""

import logging
import sys
from typing import Optional

def setup_logging(log_level: str = 'INFO', log_format: Optional[str] = None) -> logging.Logger:
    """
    Set up logging for the NetApp S3 framework.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Custom log format string
        
    Returns:
        Configured logger instance
    """
    if log_format is None:
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Create logger
    logger = logging.getLogger('netapp_s3_framework')
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Avoid duplicate handlers
    if not logger.handlers:
        # Create console handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(getattr(logging, log_level.upper()))
        
        # Create formatter
        formatter = logging.Formatter(log_format)
        handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(handler)
    
    return logger

def configure_s3fs_logging(log_level: str = 'WARNING'):
    """Configure s3fs library logging to reduce noise."""
    s3fs_logger = logging.getLogger('s3fs')
    s3fs_logger.setLevel(getattr(logging, log_level.upper()))
    
    boto_logger = logging.getLogger('botocore')
    boto_logger.setLevel(getattr(logging, log_level.upper()))
```

### `netapp_s3_framework/utils/retry.py`

```python
"""
Retry logic utilities
"""

import time
import functools
import logging
from typing import Callable, Any, Type, Tuple

logger = logging.getLogger(__name__)

def retry_on_failure(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,)
):
    """
    Decorator to retry function execution on failure.
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff_factor: Factor to multiply delay by after each failure
        exceptions: Tuple of exceptions to catch and retry on
        
    Returns:
        Decorator function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            current_delay = delay
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_attempts - 1:
                        # Last attempt, don't delay and re-raise
                        break
                    
                    logger.warning(
                        f"Attempt {attempt + 1}/{max_attempts} failed for {func.__name__}: {e}. "
                        f"Retrying in {current_delay:.1f}s..."
                    )
                    
                    time.sleep(current_delay)
                    current_delay *= backoff_factor
            
            # All attempts failed, raise the last exception
            raise last_exception
        
        return wrapper
    return decorator

class RetryableOperation:
    """Class-based retry mechanism for complex operations."""
    
    def __init__(self, max_attempts: int = 3, delay: float = 1.0, backoff_factor: float = 2.0):
        self.max_attempts = max_attempts
        self.delay = delay
        self.backoff_factor = backoff_factor
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def execute(self, operation: Callable, *args, **kwargs) -> Any:
        """Execute operation with retry logic."""
        current_delay = self.delay
        last_exception = None
        
        for attempt in range(self.max_attempts):
            try:
                result = operation(*args, **kwargs)
                if attempt > 0:
                    self.logger.info(f"Operation succeeded on attempt {attempt + 1}")
                return result
                
            except Exception as e:
                last_exception = e
                if attempt == self.max_attempts - 1:
                    break
                
                self.logger.warning(
                    f"Attempt {attempt + 1}/{self.max_attempts} failed: {e}. "
                    f"Retrying in {current_delay:.1f}s..."
                )
                
                time.sleep(current_delay)
                current_delay *= self.backoff_factor
        
        # All attempts failed
        self.logger.error(f"All {self.max_attempts} attempts failed. Last error: {last_exception}")
        raise last_exception
```

### `netapp_s3_framework/utils/validators.py`

```python
"""
Validation utilities
"""

import re
from typing import Optional

def validate_bucket_name(bucket_name: str) -> bool:
    """
    Validate S3 bucket name according to AWS rules.
    
    Args:
        bucket_name: Bucket name to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not bucket_name:
        return False
    
    # Length check
    if len(bucket_name) < 3 or len(bucket_name) > 63:
        return False
    
    # Pattern check
    pattern = r'^[a-z0-9][a-z0-9\-\.]*[a-z0-9]
    if not re.match(pattern, bucket_name):
        return False
    
    # Additional checks
    if '..' in bucket_name or '.-' in bucket_name or '-.' in bucket_name:
        return False
    
    # IP address check
    ip_pattern = r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}
    if re.match(ip_pattern, bucket_name):
        return False
    
    return True

def validate_key(key: str) -> bool:
    """
    Validate S3 object key.
    
    Args:
        key: Object key to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not key:
        return False
    
    # Length check (AWS limit is 1024 characters)
    if len(key) > 1024:
        return False
    
    # Check for invalid characters (basic check)
    invalid_chars = ['\0', '\r', '\n']
    for char in invalid_chars:
        if char in key:
            return False
    
    return True

def sanitize_key(key: str) -> str:
    """
    Sanitize object key by removing/replacing invalid characters.
    
    Args:
        key: Original key
        
    Returns:
        Sanitized key
    """
    # Remove/replace problematic characters
    sanitized = key.replace('\\', '/')
    sanitized = re.sub(r'[^\w\-_\./]', '_', sanitized)
    
    # Remove multiple consecutive slashes
    sanitized = re.sub(r'/+', '/', sanitized)
    
    # Remove leading/trailing slashes
    sanitized = sanitized.strip('/')
    
    # Ensure it's not empty
    if not sanitized:
        sanitized = 'unnamed_file'
    
    return sanitized

def validate_pandas_kwargs(operation: str, **kwargs) -> dict:
    """
    Validate and filter pandas kwargs for specific operations.
    
    Args:
        operation: Operation type ('read_csv', 'to_csv', 'read_parquet', 'to_parquet')
        **kwargs: Keyword arguments to validate
        
    Returns:
        Filtered and validated kwargs
    """
    valid_kwargs = {}
    
    if operation == 'read_csv':
        allowed_params = [
            'sep', 'delimiter', 'header', 'names', 'index_col', 'usecols', 
            'dtype', 'skiprows', 'skipfooter', 'nrows', 'na_values', 
            'encoding', 'chunksize', 'parse_dates', 'date_parser'
        ]
        valid_kwargs = {k: v for k, v in kwargs.items() if k in allowed_params}
    
    elif operation == 'to_csv':
        allowed_params = [
            'sep', 'na_rep', 'columns', 'header', 'index', 'encoding',
            'compression', 'quoting', 'quotechar', 'line_terminator', 'date_format'
        ]
        valid_kwargs = {k: v for k, v in kwargs.items() if k in allowed_params}
    
    elif operation == 'read_parquet':
        allowed_params = [
            'columns', 'filters', 'use_nullable_dtypes', 'dtype_backend'
        ]
        valid_kwargs = {k: v for k,