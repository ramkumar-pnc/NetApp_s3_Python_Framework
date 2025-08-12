"""
Enterprise Apache Iceberg Framework - Python Implementation
===========================================================

A comprehensive, modularized framework for implementing enterprise-grade 
Apache Iceberg tables using Apache Spark as the catalog engine.

Author: Enterprise Data Engineering Team
Version: 1.0.0
Date: August 2025
"""

import json
import logging
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


# =============================================================================
# CONFIGURATION CLASSES
# =============================================================================

class WriteMode(Enum):
    """Enumeration for different write modes."""
    APPEND = "append"
    OVERWRITE = "overwrite"
    UPSERT = "upsert"
    DELETE = "delete"


class EvolutionMode(Enum):
    """Schema evolution modes."""
    STRICT = "strict"
    PERMISSIVE = "permissive"
    MERGE = "merge"


@dataclass
class InputConfig:
    """Configuration for input data sources."""
    source_type: str
    source_path: str
    format: str
    compression_codec: Optional[str] = None
    read_options: Dict[str, Any] = field(default_factory=dict)
    streaming: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        # Set default read options
        default_read_options = {
            "multiline": False,
            "infer_schema": True,
            "header": True,
            "delimiter": ",",
            "timestamp_format": "yyyy-MM-dd HH:mm:ss",
            "date_format": "yyyy-MM-dd"
        }
        self.read_options = {**default_read_options, **self.read_options}
        
        # Set default streaming options
        default_streaming = {
            "enabled": False,
            "trigger": "processingTime='10 seconds'",
            "checkpoint_location": "/tmp/checkpoint",
            "max_files_per_trigger": 1000
        }
        self.streaming = {**default_streaming, **self.streaming}


@dataclass
class SchemaConfig:
    """Configuration for schema management."""
    enable_auto_evolution: bool = True
    evolution_mode: EvolutionMode = EvolutionMode.PERMISSIVE
    allow_column_addition: bool = True
    allow_column_deletion: bool = False
    allow_type_widening: bool = True
    required_columns: List[str] = field(default_factory=list)
    default_values: Dict[str, Any] = field(default_factory=dict)
    schema_registry_url: Optional[str] = None


@dataclass
class PartitionSpec:
    """Partition specification for Iceberg tables."""
    column: str
    transform: str = "identity"  # identity, bucket, truncate, year, month, day, hour


@dataclass
class SortOrder:
    """Sort order specification for Iceberg tables."""
    column: str
    direction: str = "asc"  # asc, desc
    null_order: str = "first"  # first, last


@dataclass
class TableConfig:
    """Configuration for Iceberg table creation and management."""
    table_name: str
    database: str
    table_location: str
    partition_specs: List[PartitionSpec] = field(default_factory=list)
    sort_orders: List[SortOrder] = field(default_factory=list)
    table_properties: Dict[str, str] = field(default_factory=dict)
    primary_keys: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        # Set default table properties
        default_properties = {
            "write.format.default": "parquet",
            "write.target-file-size-bytes": "134217728",  # 128MB
            "write.distribution-mode": "hash",
            "commit.retry.num-retries": "4",
            "commit.retry.min-wait-ms": "100",
            "write.spark.fanout.enabled": "true"
        }
        self.table_properties = {**default_properties, **self.table_properties}


@dataclass
class CatalogConfig:
    """Configuration for Spark catalog integration."""
    catalog_name: str
    catalog_impl: str = "org.apache.iceberg.spark.SparkCatalog"
    warehouse: str = ""
    catalog_type: str = "hadoop"  # hive, hadoop, nessie, rest
    uri: Optional[str] = None
    authentication: Dict[str, str] = field(default_factory=dict)


@dataclass
class MaintenanceConfig:
    """Configuration for table optimization and maintenance."""
    compaction: Dict[str, Any] = field(default_factory=dict)
    expire_snapshots: Dict[str, Any] = field(default_factory=dict)
    rewrite_manifests: Dict[str, Any] = field(default_factory=dict)
    cleanup_orphan_files: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        # Set defaults for maintenance operations
        self.compaction = {
            "enabled": True,
            "target_file_size_bytes": 134217728,  # 128MB
            "min_input_files": 5,
            **self.compaction
        }
        
        self.expire_snapshots = {
            "enabled": True,
            "retain_last": 7,
            "max_snapshot_age_ms": 432000000,  # 5 days
            **self.expire_snapshots
        }
        
        self.rewrite_manifests = {
            "enabled": True,
            "target_manifest_size_bytes": 8388608,  # 8MB
            **self.rewrite_manifests
        }
        
        self.cleanup_orphan_files = {
            "enabled": True,
            "older_than_ms": 259200000,  # 3 days
            **self.cleanup_orphan_files
        }


@dataclass
class IcebergFrameworkConfig:
    """Main configuration class for the Iceberg framework."""
    input_config: InputConfig
    schema_config: SchemaConfig
    table_config: TableConfig
    catalog_config: CatalogConfig
    maintenance_config: MaintenanceConfig
    job_name: str = "iceberg_framework_job"
    log_level: str = "INFO"


# =============================================================================
# VALIDATION AND RESULT CLASSES
# =============================================================================

@dataclass
class ValidationResult:
    """Result of validation operations."""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExecutionResult:
    """Result of framework execution."""
    success: bool
    message: str
    execution_time: float
    records_processed: int = 0
    files_processed: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


# =============================================================================
# EXCEPTION CLASSES
# =============================================================================

class IcebergFrameworkError(Exception):
    """Base exception for Iceberg framework."""
    pass


class ConfigurationError(IcebergFrameworkError):
    """Configuration-related errors."""
    pass


class InputError(IcebergFrameworkError):
    """Input data-related errors."""
    pass


class SchemaError(IcebergFrameworkError):
    """Schema-related errors."""
    pass


class TableError(IcebergFrameworkError):
    """Table operation-related errors."""
    pass


# =============================================================================
# CORE MODULES
# =============================================================================

class InputHandler:
    """Handles various input data formats and sources."""
    
    def __init__(self, config: InputConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def validate_input(self) -> ValidationResult:
        """Validate input configuration and accessibility."""
        errors = []
        warnings = []
        
        try:
            # Validate source path exists (for file-based sources)
            if self.config.source_type in ["file", "s3", "hdfs", "adls", "gcs"]:
                if not self.config.source_path:
                    errors.append("Source path is required for file-based sources")
                
                # Check if path exists (basic validation)
                if self.config.source_type == "file":
                    path = Path(self.config.source_path)
                    if not path.exists():
                        errors.append(f"Source path does not exist: {self.config.source_path}")
            
            # Validate format
            supported_formats = ["parquet", "json", "csv", "avro", "orc", "delta"]
            if self.config.format.lower() not in supported_formats:
                errors.append(f"Unsupported format: {self.config.format}")
            
            # Validate streaming configuration if enabled
            if self.config.streaming.get("enabled", False):
                if not self.config.streaming.get("checkpoint_location"):
                    errors.append("Checkpoint location is required for streaming")
            
            return ValidationResult(
                is_valid=len(errors) == 0,
                errors=errors,
                warnings=warnings
            )
            
        except Exception as e:
            self.logger.error(f"Error validating input: {str(e)}")
            return ValidationResult(
                is_valid=False,
                errors=[f"Validation error: {str(e)}"]
            )
    
    def read_data(self, spark: SparkSession) -> DataFrame:
        """Read data from the configured source."""
        try:
            self.logger.info(f"Reading data from {self.config.source_path} with format {self.config.format}")
            
            # Build read options
            reader = spark.read.format(self.config.format)
            
            # Apply read options
            for option, value in self.config.read_options.items():
                if option == "infer_schema":
                    reader = reader.option("inferSchema", value)
                else:
                    reader = reader.option(option, value)
            
            # Handle compression
            if self.config.compression_codec:
                reader = reader.option("compression", self.config.compression_codec)
            
            # Read the data
            if self.config.streaming.get("enabled", False):
                df = self._read_streaming(spark, reader)
            else:
                df = reader.load(self.config.source_path)
            
            # Add metadata columns
            df = df.withColumn("_input_file_name", lit(self.config.source_path))
            df = df.withColumn("_processing_timestamp", current_timestamp())
            
            self.logger.info(f"Successfully read data. Row count: {df.count()}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error reading data: {str(e)}")
            raise InputError(f"Failed to read data: {str(e)}")
    
    def _read_streaming(self, spark: SparkSession, reader) -> DataFrame:
        """Configure streaming read."""
        stream_reader = spark.readStream.format(self.config.format)
        
        # Apply read options for streaming
        for option, value in self.config.read_options.items():
            stream_reader = stream_reader.option(option, value)
        
        # Apply streaming-specific options
        max_files = self.config.streaming.get("max_files_per_trigger", 1000)
        stream_reader = stream_reader.option("maxFilesPerTrigger", max_files)
        
        return stream_reader.load(self.config.source_path)
    
    def infer_schema(self, spark: SparkSession) -> StructType:
        """Infer schema from input data."""
        try:
            sample_df = self.read_data(spark).limit(1000)
            return sample_df.schema
        except Exception as e:
            self.logger.error(f"Error inferring schema: {str(e)}")
            raise SchemaError(f"Failed to infer schema: {str(e)}")


class SchemaManager:
    """Manages schema operations including evolution and validation."""
    
    def __init__(self, config: SchemaConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def validate_schema(self, data: DataFrame, target_schema: StructType) -> ValidationResult:
        """Validate data against target schema."""
        errors = []
        warnings = []
        
        try:
            current_schema = data.schema
            current_columns = {field.name: field.dataType for field in current_schema.fields}
            target_columns = {field.name: field.dataType for field in target_schema.fields}
            
            # Check required columns
            for required_col in self.config.required_columns:
                if required_col not in current_columns:
                    errors.append(f"Required column missing: {required_col}")
            
            # Check data type compatibility
            for col_name, col_type in current_columns.items():
                if col_name in target_columns:
                    if not self._is_compatible_type(col_type, target_columns[col_name]):
                        if self.config.allow_type_widening:
                            warnings.append(f"Type widening for column {col_name}: {col_type} -> {target_columns[col_name]}")
                        else:
                            errors.append(f"Incompatible type for column {col_name}: {col_type} vs {target_columns[col_name]}")
            
            return ValidationResult(
                is_valid=len(errors) == 0,
                errors=errors,
                warnings=warnings
            )
            
        except Exception as e:
            self.logger.error(f"Error validating schema: {str(e)}")
            return ValidationResult(
                is_valid=False,
                errors=[f"Schema validation error: {str(e)}"]
            )
    
    def evolve_schema(self, current_schema: StructType, new_schema: StructType) -> StructType:
        """Evolve schema based on configuration."""
        try:
            if self.config.evolution_mode == EvolutionMode.STRICT:
                return self._strict_evolution(current_schema, new_schema)
            elif self.config.evolution_mode == EvolutionMode.PERMISSIVE:
                return self._permissive_evolution(current_schema, new_schema)
            else:  # MERGE
                return self._merge_evolution(current_schema, new_schema)
                
        except Exception as e:
            self.logger.error(f"Error evolving schema: {str(e)}")
            raise SchemaError(f"Failed to evolve schema: {str(e)}")
    
    def _strict_evolution(self, current_schema: StructType, new_schema: StructType) -> StructType:
        """Strict schema evolution - no changes allowed."""
        if current_schema != new_schema:
            raise SchemaError("Schema changes not allowed in strict mode")
        return current_schema
    
    def _permissive_evolution(self, current_schema: StructType, new_schema: StructType) -> StructType:
        """Permissive schema evolution - allow additions and compatible changes."""
        current_fields = {field.name: field for field in current_schema.fields}
        new_fields = {field.name: field for field in new_schema.fields}
        
        evolved_fields = []
        
        # Keep existing fields
        for field_name, field in current_fields.items():
            if field_name in new_fields:
                # Check compatibility
                new_field = new_fields[field_name]
                if self._is_compatible_type(field.dataType, new_field.dataType):
                    evolved_fields.append(new_field)
                else:
                    evolved_fields.append(field)  # Keep original type
            else:
                if not self.config.allow_column_deletion:
                    evolved_fields.append(field)
        
        # Add new fields if allowed
        if self.config.allow_column_addition:
            for field_name, field in new_fields.items():
                if field_name not in current_fields:
                    evolved_fields.append(field)
        
        return StructType(evolved_fields)
    
    def _merge_evolution(self, current_schema: StructType, new_schema: StructType) -> StructType:
        """Merge evolution - intelligent merging of schemas."""
        # Implementation for merge evolution logic
        return self._permissive_evolution(current_schema, new_schema)
    
    def _is_compatible_type(self, source_type, target_type) -> bool:
        """Check if source type is compatible with target type."""
        # Simplified compatibility check
        if source_type == target_type:
            return True
        
        # Add more sophisticated type compatibility logic here
        compatible_types = {
            IntegerType(): [StringType()],
            StringType(): [IntegerType(), TimestampType()]
        }
        
        return target_type in compatible_types.get(source_type, [])
    
    def apply_default_values(self, data: DataFrame) -> DataFrame:
        """Apply default values for missing columns."""
        df = data
        for column, default_value in self.config.default_values.items():
            if column not in df.columns:
                df = df.withColumn(column, lit(default_value))
        return df


class IcebergTableManager:
    """Manages Iceberg table operations."""
    
    def __init__(self, config: TableConfig, spark: SparkSession):
        self.config = config
        self.spark = spark
        self.logger = logging.getLogger(self.__class__.__name__)
        self.full_table_name = f"{config.database}.{config.table_name}"
    
    def create_table_if_not_exists(self, schema: StructType) -> bool:
        """Create Iceberg table if it doesn't exist."""
        try:
            # Check if table exists
            if self._table_exists():
                self.logger.info(f"Table {self.full_table_name} already exists")
                return False
            
            self.logger.info(f"Creating Iceberg table: {self.full_table_name}")
            
            # Build CREATE TABLE statement
            create_sql = self._build_create_table_sql(schema)
            
            # Execute table creation
            self.spark.sql(create_sql)
            
            self.logger.info(f"Successfully created table: {self.full_table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating table: {str(e)}")
            raise TableError(f"Failed to create table: {str(e)}")
    
    def write_data(self, data: DataFrame, write_mode: WriteMode = WriteMode.APPEND) -> ExecutionResult:
        """Write data to Iceberg table."""
        start_time = time.time()
        
        try:
            self.logger.info(f"Writing data to {self.full_table_name} with mode: {write_mode.value}")
            
            writer = data.write.format("iceberg")
            
            # Configure write mode
            if write_mode == WriteMode.APPEND:
                writer = writer.mode("append")
            elif write_mode == WriteMode.OVERWRITE:
                writer = writer.mode("overwrite")
            elif write_mode == WriteMode.UPSERT:
                return self._perform_upsert(data)
            elif write_mode == WriteMode.DELETE:
                return self._perform_delete(data)
            
            # Apply table properties
            for prop, value in self.config.table_properties.items():
                writer = writer.option(prop, value)
            
            # Write the data
            writer.saveAsTable(self.full_table_name)
            
            execution_time = time.time() - start_time
            record_count = data.count()
            
            self.logger.info(f"Successfully wrote {record_count} records in {execution_time:.2f} seconds")
            
            return ExecutionResult(
                success=True,
                message=f"Data written successfully to {self.full_table_name}",
                execution_time=execution_time,
                records_processed=record_count
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Error writing data: {str(e)}")
            return ExecutionResult(
                success=False,
                message=f"Failed to write data: {str(e)}",
                execution_time=execution_time
            )
    
    def _perform_upsert(self, data: DataFrame) -> ExecutionResult:
        """Perform upsert operation using MERGE."""
        if not self.config.primary_keys:
            raise TableError("Primary keys must be defined for upsert operations")
        
        start_time = time.time()
        
        try:
            # Create temporary view
            temp_view = f"temp_{self.config.table_name}_{int(time.time())}"
            data.createOrReplaceTempView(temp_view)
            
            # Build merge condition
            merge_conditions = []
            for pk in self.config.primary_keys:
                merge_conditions.append(f"target.{pk} = source.{pk}")
            
            merge_condition = " AND ".join(merge_conditions)
            
            # Build update and insert clauses
            columns = data.columns
            update_clause = ", ".join([f"target.{col} = source.{col}" for col in columns])
            insert_columns = ", ".join(columns)
            insert_values = ", ".join([f"source.{col}" for col in columns])
            
            # Execute MERGE statement
            merge_sql = f"""
            MERGE INTO {self.full_table_name} AS target
            USING {temp_view} AS source
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
            """
            
            self.spark.sql(merge_sql)
            
            execution_time = time.time() - start_time
            record_count = data.count()
            
            # Clean up temp view
            self.spark.sql(f"DROP VIEW IF EXISTS {temp_view}")
            
            return ExecutionResult(
                success=True,
                message=f"Upsert completed for {self.full_table_name}",
                execution_time=execution_time,
                records_processed=record_count
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Error performing upsert: {str(e)}")
            return ExecutionResult(
                success=False,
                message=f"Upsert failed: {str(e)}",
                execution_time=execution_time
            )
    
    def _perform_delete(self, data: DataFrame) -> ExecutionResult:
        """Perform delete operation."""
        if not self.config.primary_keys:
            raise TableError("Primary keys must be defined for delete operations")
        
        start_time = time.time()
        
        try:
            # Create temporary view
            temp_view = f"temp_delete_{self.config.table_name}_{int(time.time())}"
            data.createOrReplaceTempView(temp_view)
            
            # Build delete condition
            delete_conditions = []
            for pk in self.config.primary_keys:
                delete_conditions.append(f"{pk} IN (SELECT {pk} FROM {temp_view})")
            
            delete_condition = " AND ".join(delete_conditions)
            
            # Execute DELETE statement
            delete_sql = f"DELETE FROM {self.full_table_name} WHERE {delete_condition}"
            self.spark.sql(delete_sql)
            
            execution_time = time.time() - start_time
            record_count = data.count()
            
            # Clean up temp view
            self.spark.sql(f"DROP VIEW IF EXISTS {temp_view}")
            
            return ExecutionResult(
                success=True,
                message=f"Delete completed for {self.full_table_name}",
                execution_time=execution_time,
                records_processed=record_count
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Error performing delete: {str(e)}")
            return ExecutionResult(
                success=False,
                message=f"Delete failed: {str(e)}",
                execution_time=execution_time
            )
    
    def _table_exists(self) -> bool:
        """Check if table exists."""
        try:
            self.spark.sql(f"DESCRIBE TABLE {self.full_table_name}")
            return True
        except:
            return False
    
    def _build_create_table_sql(self, schema: StructType) -> str:
        """Build CREATE TABLE SQL statement."""
        # Build column definitions
        columns = []
        for field in schema.fields:
            col_def = f"{field.name} {self._spark_to_iceberg_type(field.dataType)}"
            if not field.nullable:
                col_def += " NOT NULL"
            columns.append(col_def)
        
        columns_sql = ",\n  ".join(columns)
        
        # Build partitioning
        partition_sql = ""
        if self.config.partition_specs:
            partitions = []
            for spec in self.config.partition_specs:
                if spec.transform == "identity":
                    partitions.append(spec.column)
                else:
                    partitions.append(f"{spec.transform}({spec.column})")
            partition_sql = f"PARTITIONED BY ({', '.join(partitions)})"
        
        # Build table properties
        properties = []
        for prop, value in self.config.table_properties.items():
            properties.append(f"'{prop}' = '{value}'")
        
        properties_sql = ""
        if properties:
            properties_sql = f"TBLPROPERTIES ({', '.join(properties)})"
        
        # Build complete CREATE TABLE statement
        create_sql = f"""
        CREATE TABLE {self.full_table_name} (
          {columns_sql}
        ) USING ICEBERG
        {partition_sql}
        LOCATION '{self.config.table_location}'
        {properties_sql}
        """
        
        return create_sql
    
    def _spark_to_iceberg_type(self, spark_type) -> str:
        """Convert Spark data type to Iceberg type string."""
        type_mapping = {
            "StringType": "string",
            "IntegerType": "int",
            "LongType": "long",
            "DoubleType": "double",
            "FloatType": "float",
            "BooleanType": "boolean",
            "TimestampType": "timestamp",
            "DateType": "date",
            "BinaryType": "binary"
        }
        
        spark_type_name = type(spark_type).__name__
        return type_mapping.get(spark_type_name, "string")
    
    def optimize_table(self) -> ExecutionResult:
        """Optimize Iceberg table."""
        start_time = time.time()
        
        try:
            self.logger.info(f"Optimizing table: {self.full_table_name}")
            
            # Perform compaction
            self.spark.sql(f"CALL system.rewrite_data_files('{self.full_table_name}')")
            
            # Rewrite manifests
            self.spark.sql(f"CALL system.rewrite_manifests('{self.full_table_name}')")
            
            execution_time = time.time() - start_time
            
            self.logger.info(f"Table optimization completed in {execution_time:.2f} seconds")
            
            return ExecutionResult(
                success=True,
                message=f"Table optimization completed for {self.full_table_name}",
                execution_time=execution_time
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Error optimizing table: {str(e)}")
            return ExecutionResult(
                success=False,
                message=f"Table optimization failed: {str(e)}",
                execution_time=execution_time
            )


class MaintenanceManager:
    """Manages table maintenance operations."""
    
    def __init__(self, config: MaintenanceConfig, spark: SparkSession):
        self.config = config
        self.spark = spark
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def perform_maintenance(self, table_name: str) -> ExecutionResult:
        """Perform all enabled maintenance operations."""
        start_time = time.time()
        results = []
        
        try:
            self.logger.info(f"Starting maintenance for table: {table_name}")
            
            # Compaction
            if self.config.compaction.get("enabled", False):
                result = self._perform_compaction(table_name)
                results.append(result)
            
            # Expire snapshots
            if self.config.expire_snapshots.get("enabled", False):
                result = self._expire_snapshots(table_name)
                results.append(result)
            
            # Rewrite manifests
            if self.config.rewrite_manifests.get("enabled", False):
                result = self._rewrite_manifests(table_name)
                results.append(result)
            
            # Cleanup orphan files
            if self.config.cleanup_orphan_files.get("enabled", False):
                result = self._cleanup_orphan_files(table_name)
                results.append(result)
            
            execution_time = time.time() - start_time
            
            # Check if all operations succeeded
            all_success = all(result.success for result in results)
            
            return ExecutionResult(
                success=all_success,
                message=f"Maintenance {'completed' if all_success else 'completed with errors'} for {table_name}",
                execution_time=execution_time,
                metadata={"operation_results": [r.__dict__ for r in results]}
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Error performing maintenance: {str(e)}")
            return ExecutionResult(
                success=False,
                message=f"Maintenance failed: {str(e)}",
                execution_time=execution_time
            )
    
    def _perform_compaction(self, table_name: str) -> ExecutionResult:
        """Perform file compaction."""
        start_time = time.time()
        
        try:
            target_size = self.config.compaction.get("target_file_size_bytes", 134217728)
            min_files = self.config.compaction.get("min_input_files", 5)
            
            sql = f"""
            CALL system.rewrite_data_files(
                table => '{table_name}',
                options => map(
                    'target-file-size-bytes', '{target_size}',
                    'min-input-files', '{min_files}'
                )
            )"""
            
            self.spark.sql(sql)
            
            execution_time = time.time() - start_time
            self.logger.info(f"Compaction completed for {table_name}")
            
            return ExecutionResult(
                success=True,
                message=f"Compaction completed for {table_name}",
                execution_time=execution_time
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Error performing compaction: {str(e)}")
            return ExecutionResult(
                success=False,
                message=f"Compaction failed: {str(e)}",
                execution_time=execution_time
            )
    
    def _expire_snapshots(self, table_name: str) -> ExecutionResult:
        """Expire old snapshots."""
        start_time = time.time()
        
        try:
            retain_last = self.config.expire_snapshots.get("retain_last", 7)
            max_age_ms = self.config.expire_snapshots.get("max_snapshot_age_ms", 432000000)
            
            # Calculate timestamp for expiration
            expire_timestamp = datetime.now() - timedelta(milliseconds=max_age_ms)
            expire_ts_str = expire_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            
            sql = f"""
            CALL system.expire_snapshots(
                table => '{table_name}',
                older_than => timestamp '{expire_ts_str}',
                retain_last => {retain_last}
            )"""
            
            self.spark.sql(sql)
            
            execution_time = time.time() - start_time
            self.logger.info(f"Snapshot expiration completed for {table_name}")
            
            return ExecutionResult(
                success=True,
                message=f"Snapshot expiration completed for {table_name}",
                execution_time=execution_time
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Error expiring snapshots: {str(e)}")
            return ExecutionResult(
                success=False,
                message=f"Snapshot expiration failed: {str(e)}",
                execution_time=execution_time
            )
    
    def _rewrite_manifests(self, table_name: str) -> ExecutionResult:
        """Rewrite manifest files."""
        start_time = time.time()
        
        try:
            target_size = self.config.rewrite_manifests.get("target_manifest_size_bytes", 8388608)
            
            sql = f"""
            CALL system.rewrite_manifests(
                table => '{table_name}',
                options => map('target-manifest-size-bytes', '{target_size}')
            )"""
            
            self.spark.sql(sql)
            
            execution_time = time.time() - start_time
            self.logger.info(f"Manifest rewrite completed for {table_name}")
            
            return ExecutionResult(
                success=True,
                message=f"Manifest rewrite completed for {table_name}",
                execution_time=execution_time
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Error rewriting manifests: {str(e)}")
            return ExecutionResult(
                success=False,
                message=f"Manifest rewrite failed: {str(e)}",
                execution_time=execution_time
            )
    
    def _cleanup_orphan_files(self, table_name: str) -> ExecutionResult:
        """Cleanup orphaned files."""
        start_time = time.time()
        
        try:
            older_than_ms = self.config.cleanup_orphan_files.get("older_than_ms", 259200000)
            
            # Calculate timestamp for cleanup
            cleanup_timestamp = datetime.now() - timedelta(milliseconds=older_than_ms)
            cleanup_ts_str = cleanup_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            
            sql = f"""
            CALL system.remove_orphan_files(
                table => '{table_name}',
                older_than => timestamp '{cleanup_ts_str}'
            )"""
            
            self.spark.sql(sql)
            
            execution_time = time.time() - start_time
            self.logger.info(f"Orphan file cleanup completed for {table_name}")
            
            return ExecutionResult(
                success=True,
                message=f"Orphan file cleanup completed for {table_name}",
                execution_time=execution_time
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Error cleaning up orphan files: {str(e)}")
            return ExecutionResult(
                success=False,
                message=f"Orphan file cleanup failed: {str(e)}",
                execution_time=execution_time
            )


class SparkCatalogIntegrator:
    """Integrates with Spark catalog for Iceberg operations."""
    
    def __init__(self, config: CatalogConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def configure_spark_session(self, spark_builder) -> SparkSession:
        """Configure Spark session with Iceberg catalog settings."""
        try:
            # Configure Iceberg extensions
            spark_builder = spark_builder.config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            )
            
            # Configure catalog
            catalog_prefix = f"spark.sql.catalog.{self.config.catalog_name}"
            
            spark_builder = spark_builder.config(
                f"{catalog_prefix}",
                self.config.catalog_impl
            )
            
            if self.config.catalog_type == "hadoop":
                spark_builder = spark_builder.config(
                    f"{catalog_prefix}.type",
                    "hadoop"
                ).config(
                    f"{catalog_prefix}.warehouse",
                    self.config.warehouse
                )
            elif self.config.catalog_type == "hive":
                spark_builder = spark_builder.config(
                    f"{catalog_prefix}.type",
                    "hive"
                ).config(
                    f"{catalog_prefix}.uri",
                    self.config.uri or "thrift://localhost:9083"
                )
            elif self.config.catalog_type == "rest":
                spark_builder = spark_builder.config(
                    f"{catalog_prefix}.type",
                    "rest"
                ).config(
                    f"{catalog_prefix}.uri",
                    self.config.uri
                )
            
            # Configure authentication if provided
            for auth_key, auth_value in self.config.authentication.items():
                spark_builder = spark_builder.config(
                    f"{catalog_prefix}.{auth_key}",
                    auth_value
                )
            
            # Additional Iceberg optimizations
            spark_builder = (spark_builder
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.adaptive.skewJoin.enabled", "true")
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            )
            
            spark = spark_builder.getOrCreate()
            
            # Set current catalog
            spark.sql(f"USE CATALOG {self.config.catalog_name}")
            
            self.logger.info(f"Spark session configured with Iceberg catalog: {self.config.catalog_name}")
            return spark
            
        except Exception as e:
            self.logger.error(f"Error configuring Spark session: {str(e)}")
            raise ConfigurationError(f"Failed to configure Spark session: {str(e)}")


# =============================================================================
# MAIN FRAMEWORK ORCHESTRATOR
# =============================================================================

class IcebergFramework:
    """Main framework orchestrator that coordinates all modules."""
    
    def __init__(self, config: IcebergFrameworkConfig):
        self.config = config
        self.logger = self._setup_logging()
        
        # Initialize modules
        self.input_handler = InputHandler(config.input_config)
        self.schema_manager = SchemaManager(config.schema_config)
        self.catalog_integrator = SparkCatalogIntegrator(config.catalog_config)
        
        # Spark session will be initialized during execution
        self.spark = None
        self.table_manager = None
        self.maintenance_manager = None
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration."""
        logger = logging.getLogger("IcebergFramework")
        logger.setLevel(getattr(logging, self.config.log_level.upper()))
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def execute(self) -> ExecutionResult:
        """Execute the complete Iceberg framework pipeline."""
        overall_start_time = time.time()
        
        try:
            self.logger.info(f"Starting Iceberg framework execution: {self.config.job_name}")
            
            # Step 1: Initialize Spark session
            self._initialize_spark()
            
            # Step 2: Validate input
            validation_result = self.input_handler.validate_input()
            if not validation_result.is_valid:
                return ExecutionResult(
                    success=False,
                    message=f"Input validation failed: {'; '.join(validation_result.errors)}",
                    execution_time=time.time() - overall_start_time
                )
            
            # Step 3: Read input data
            input_data = self.input_handler.read_data(self.spark)
            
            # Step 4: Handle schema operations
            current_schema = input_data.schema
            
            # Check if table exists and get existing schema
            if self.table_manager._table_exists():
                existing_table_df = self.spark.table(self.table_manager.full_table_name)
                existing_schema = existing_table_df.schema
                
                # Validate and evolve schema
                schema_validation = self.schema_manager.validate_schema(input_data, existing_schema)
                if not schema_validation.is_valid and self.config.schema_config.evolution_mode == EvolutionMode.STRICT:
                    return ExecutionResult(
                        success=False,
                        message=f"Schema validation failed: {'; '.join(schema_validation.errors)}",
                        execution_time=time.time() - overall_start_time
                    )
                
                # Evolve schema if needed
                evolved_schema = self.schema_manager.evolve_schema(existing_schema, current_schema)
                current_schema = evolved_schema
            
            # Step 5: Create table if it doesn't exist
            table_created = self.table_manager.create_table_if_not_exists(current_schema)
            
            # Step 6: Apply default values
            processed_data = self.schema_manager.apply_default_values(input_data)
            
            # Step 7: Write data to table
            if self.config.input_config.streaming.get("enabled", False):
                write_result = self._handle_streaming_write(processed_data)
            else:
                write_result = self.table_manager.write_data(processed_data, WriteMode.APPEND)
            
            if not write_result.success:
                return write_result
            
            # Step 8: Perform optimization if configured
            optimization_result = None
            if self.config.table_config.table_properties.get("auto-optimize", "false").lower() == "true":
                optimization_result = self.table_manager.optimize_table()
            
            # Step 9: Perform maintenance if configured
            maintenance_result = None
            if any(self.config.maintenance_config.__dict__.values()):
                maintenance_result = self.maintenance_manager.perform_maintenance(
                    self.table_manager.full_table_name
                )
            
            execution_time = time.time() - overall_start_time
            
            # Compile final result
            metadata = {
                "table_created": table_created,
                "records_processed": write_result.records_processed,
                "optimization_performed": optimization_result is not None,
                "maintenance_performed": maintenance_result is not None
            }
            
            if optimization_result:
                metadata["optimization_result"] = optimization_result.__dict__
            
            if maintenance_result:
                metadata["maintenance_result"] = maintenance_result.__dict__
            
            self.logger.info(f"Framework execution completed successfully in {execution_time:.2f} seconds")
            
            return ExecutionResult(
                success=True,
                message=f"Framework execution completed successfully",
                execution_time=execution_time,
                records_processed=write_result.records_processed,
                metadata=metadata
            )
            
        except Exception as e:
            execution_time = time.time() - overall_start_time
            self.logger.error(f"Framework execution failed: {str(e)}")
            return ExecutionResult(
                success=False,
                message=f"Framework execution failed: {str(e)}",
                execution_time=execution_time
            )
        finally:
            # Cleanup
            if self.spark:
                if not self.config.input_config.streaming.get("enabled", False):
                    self.spark.stop()
    
    def _initialize_spark(self):
        """Initialize Spark session and related managers."""
        try:
            spark_builder = SparkSession.builder.appName(self.config.job_name)
            self.spark = self.catalog_integrator.configure_spark_session(spark_builder)
            
            # Initialize managers that depend on Spark
            self.table_manager = IcebergTableManager(self.config.table_config, self.spark)
            self.maintenance_manager = MaintenanceManager(self.config.maintenance_config, self.spark)
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Spark session: {str(e)}")
            raise ConfigurationError(f"Spark initialization failed: {str(e)}")
    
    def _handle_streaming_write(self, data: DataFrame) -> ExecutionResult:
        """Handle streaming data write operations."""
        try:
            trigger_config = self.config.input_config.streaming.get("trigger", "processingTime='10 seconds'")
            checkpoint_location = self.config.input_config.streaming.get("checkpoint_location")
            
            # Configure streaming write
            stream_writer = (data.writeStream
                .format("iceberg")
                .outputMode("append")
                .option("checkpointLocation", checkpoint_location)
                .option("path", self.config.table_config.table_location)
                .trigger(processingTime=trigger_config.split("'")[1])
            )
            
            # Apply table properties
            for prop, value in self.config.table_config.table_properties.items():
                stream_writer = stream_writer.option(prop, value)
            
            # Start the streaming query
            streaming_query = stream_writer.toTable(self.table_manager.full_table_name)
            
            self.logger.info(f"Streaming write started for table: {self.table_manager.full_table_name}")
            
            return ExecutionResult(
                success=True,
                message="Streaming write started successfully",
                execution_time=0,
                records_processed=0,
                metadata={"streaming_query_id": streaming_query.id}
            )
            
        except Exception as e:
            self.logger.error(f"Error starting streaming write: {str(e)}")
            return ExecutionResult(
                success=False,
                message=f"Streaming write failed: {str(e)}",
                execution_time=0
            )


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def load_config_from_file(config_path: str) -> IcebergFrameworkConfig:
    """Load framework configuration from JSON file."""
    try:
        with open(config_path, 'r') as file:
            config_data = json.load(file)
        
        # Parse configuration sections
        input_config = InputConfig(**config_data["input_config"])
        schema_config = SchemaConfig(**config_data["schema_config"])
        table_config = TableConfig(**config_data["table_config"])
        catalog_config = CatalogConfig(**config_data["catalog_config"])
        maintenance_config = MaintenanceConfig(**config_data.get("maintenance_config", {}))
        
        return IcebergFrameworkConfig(
            input_config=input_config,
            schema_config=schema_config,
            table_config=table_config,
            catalog_config=catalog_config,
            maintenance_config=maintenance_config,
            job_name=config_data.get("job_name", "iceberg_framework_job"),
            log_level=config_data.get("log_level", "INFO")
        )
        
    except Exception as e:
        raise ConfigurationError(f"Failed to load configuration from {config_path}: {str(e)}")


def create_sample_config() -> Dict[str, Any]:
    """Create a sample configuration dictionary."""
    return {
        "job_name": "iceberg_data_pipeline",
        "log_level": "INFO",
        "input_config": {
            "source_type": "file",
            "source_path": "/path/to/input/data",
            "format": "parquet",
            "compression_codec": "snappy",
            "read_options": {
                "multiline": False,
                "infer_schema": True,
                "header": True
            },
            "streaming": {
                "enabled": False,
                "trigger": "processingTime='30 seconds'",
                "checkpoint_location": "/path/to/checkpoint",
                "max_files_per_trigger": 1000
            }
        },
        "schema_config": {
            "enable_auto_evolution": True,
            "evolution_mode": "permissive",
            "allow_column_addition": True,
            "allow_column_deletion": False,
            "allow_type_widening": True,
            "required_columns": ["id", "timestamp"],
            "default_values": {
                "created_at": "current_timestamp()",
                "status": "active"
            }
        },
        "table_config": {
            "table_name": "my_iceberg_table",
            "database": "default",
            "table_location": "s3a://my-bucket/warehouse/my_iceberg_table",
            "partition_specs": [
                {"column": "year", "transform": "year"},
                {"column": "month", "transform": "month"}
            ],
            "sort_orders": [
                {"column": "timestamp", "direction": "desc"}
            ],
            "primary_keys": ["id"],
            "table_properties": {
                "write.format.default": "parquet",
                "write.target-file-size-bytes": "134217728",
                "write.distribution-mode": "hash"
            }
        },
        "catalog_config": {
            "catalog_name": "iceberg_catalog",
            "catalog_impl": "org.apache.iceberg.spark.SparkCatalog",
            "warehouse": "s3a://my-bucket/warehouse",
            "catalog_type": "hadoop"
        },
        "maintenance_config": {
            "compaction": {
                "enabled": True,
                "target_file_size_bytes": 134217728,
                "min_input_files": 5
            },
            "expire_snapshots": {
                "enabled": True,
                "retain_last": 7,
                "max_snapshot_age_ms": 432000000
            },
            "rewrite_manifests": {
                "enabled": True,
                "target_manifest_size_bytes": 8388608
            },
            "cleanup_orphan_files": {
                "enabled": True,
                "older_than_ms": 259200000
            }
        }
    }


def validate_configuration(config: IcebergFrameworkConfig) -> ValidationResult:
    """Validate the framework configuration."""
    errors = []
    warnings = []
    
    try:
        # Validate input configuration
        if not config.input_config.source_path:
            errors.append("Input source path is required")
        
        if not config.input_config.format:
            errors.append("Input format is required")
        
        # Validate table configuration
        if not config.table_config.table_name:
            errors.append("Table name is required")
        
        if not config.table_config.database:
            errors.append("Database name is required")
        
        if not config.table_config.table_location:
            errors.append("Table location is required")
        
        # Validate catalog configuration
        if not config.catalog_config.catalog_name:
            errors.append("Catalog name is required")
        
        if not config.catalog_config.warehouse and config.catalog_config.catalog_type == "hadoop":
            errors.append("Warehouse location is required for Hadoop catalog")
        
        # Validate streaming configuration
        if config.input_config.streaming.get("enabled", False):
            if not config.input_config.streaming.get("checkpoint_location"):
                errors.append("Checkpoint location is required for streaming")
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )
        
    except Exception as e:
        return ValidationResult(
            is_valid=False,
            errors=[f"Configuration validation error: {str(e)}"]
        )


# =============================================================================
# EXAMPLE USAGE
# =============================================================================

if __name__ == "__main__":
    # Example usage of the framework
    
    # Create sample configuration
    sample_config = create_sample_config()
    
    # Save sample configuration to file
    config_file = "iceberg_config.json"
    with open(config_file, 'w') as file:
        json.dump(sample_config, file, indent=2)
    
    print(f"Sample configuration saved to {config_file}")
    
    # Load configuration from file
    try:
        config = load_config_from_file(config_file)
        
        # Validate configuration
        validation_result = validate_configuration(config)
        if not validation_result.is_valid:
            print("Configuration validation failed:")
            for error in validation_result.errors:
                print(f"  - {error}")
            exit(1)
        
        # Initialize and execute framework
        framework = IcebergFramework(config)
        result = framework.execute()
        
        if result.success:
            print(f"Framework execution successful!")
            print(f"Records processed: {result.records_processed}")
            print(f"Execution time: {result.execution_time:.2f} seconds")
        else:
            print(f"Framework execution failed: {result.message}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        exit(1)