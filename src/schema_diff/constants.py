#!/usr/bin/env python3
"""Constants for schema-diff operations.

This module centralizes all magic numbers, default values, and configuration constants
used throughout the schema-diff codebase.
"""
from __future__ import annotations

# ──────────────────────────────────────────────────────────────────────────────
# Sampling and Record Processing
# ──────────────────────────────────────────────────────────────────────────────

# Default number of records to sample for schema inference
DEFAULT_SAMPLE_SIZE = 1000

# Safety limit for --all-records to prevent memory issues
MAX_RECORD_SAFETY_LIMIT = 1_000_000

# Maximum number of records to process in a single batch
MAX_BATCH_SIZE = 10_000

# Default record index for single record processing
DEFAULT_RECORD_INDEX = 0

# ──────────────────────────────────────────────────────────────────────────────
# Caching and Performance
# ──────────────────────────────────────────────────────────────────────────────

# Default cache TTL in seconds (1 hour)
DEFAULT_CACHE_TTL_SECONDS = 3600

# Maximum cache size in MB
MAX_CACHE_SIZE_MB = 100

# Cache cleanup interval in seconds
CACHE_CLEANUP_INTERVAL = 1800  # 30 minutes

# ──────────────────────────────────────────────────────────────────────────────
# Code Quality and Complexity Limits
# ──────────────────────────────────────────────────────────────────────────────

# Maximum acceptable function complexity (AST nodes)
MAX_FUNCTION_COMPLEXITY = 100

# Maximum acceptable nesting depth
MAX_NESTING_DEPTH = 4

# Maximum acceptable function length in lines
MAX_FUNCTION_LENGTH = 50

# Maximum acceptable line length
MAX_LINE_LENGTH = 120

# ──────────────────────────────────────────────────────────────────────────────
# Schema Processing
# ──────────────────────────────────────────────────────────────────────────────

# Maximum depth for recursive schema traversal
MAX_SCHEMA_DEPTH = 20

# Default array element index for path representation
DEFAULT_ARRAY_INDEX = 0

# Maximum number of union types to display
MAX_UNION_TYPES_DISPLAY = 5

# ──────────────────────────────────────────────────────────────────────────────
# File and I/O Operations
# ──────────────────────────────────────────────────────────────────────────────

# Default buffer size for file operations
DEFAULT_BUFFER_SIZE = 8192

# Maximum file size for in-memory processing (100MB)
MAX_IN_MEMORY_FILE_SIZE = 100 * 1024 * 1024

# Default timeout for external commands in seconds
DEFAULT_COMMAND_TIMEOUT = 300  # 5 minutes

# ──────────────────────────────────────────────────────────────────────────────
# BigQuery and SQL
# ──────────────────────────────────────────────────────────────────────────────

# Maximum number of columns in a BigQuery table
MAX_BIGQUERY_COLUMNS = 10_000

# Default BigQuery dataset location
DEFAULT_BIGQUERY_LOCATION = "US"

# Maximum SQL identifier length
MAX_SQL_IDENTIFIER_LENGTH = 128

# ──────────────────────────────────────────────────────────────────────────────
# Validation and Error Handling
# ──────────────────────────────────────────────────────────────────────────────

# Maximum number of validation errors to collect before stopping
MAX_VALIDATION_ERRORS = 100

# Default retry attempts for transient failures
DEFAULT_RETRY_ATTEMPTS = 3

# Default retry delay in seconds
DEFAULT_RETRY_DELAY = 1.0

# Exponential backoff multiplier for retries
RETRY_BACKOFF_MULTIPLIER = 2.0

# ──────────────────────────────────────────────────────────────────────────────
# Output and Formatting
# ──────────────────────────────────────────────────────────────────────────────

# Default output directory for generated files
DEFAULT_OUTPUT_DIR = "output"

# Maximum width for console output
MAX_CONSOLE_WIDTH = 120

# Default indentation for nested output
DEFAULT_INDENT_SIZE = 2

# Maximum number of items to show in lists before truncating
MAX_LIST_DISPLAY_ITEMS = 10

# ──────────────────────────────────────────────────────────────────────────────
# GCS and Cloud Operations
# ──────────────────────────────────────────────────────────────────────────────

# Default GCS download timeout in seconds
DEFAULT_GCS_TIMEOUT = 600  # 10 minutes

# Maximum GCS file size for download (1GB)
MAX_GCS_FILE_SIZE = 1024 * 1024 * 1024

# GCS operation retry attempts
GCS_RETRY_ATTEMPTS = 5

# ──────────────────────────────────────────────────────────────────────────────
# Migration Analysis
# ──────────────────────────────────────────────────────────────────────────────

# Threshold for considering a field change as "critical"
CRITICAL_FIELD_CHANGE_THRESHOLD = 0.1  # 10% of fields

# Threshold for considering a schema change as "major"
MAJOR_SCHEMA_CHANGE_THRESHOLD = 0.05  # 5% of fields

# Maximum number of field changes to show in detail
MAX_DETAILED_FIELD_CHANGES = 50

# ──────────────────────────────────────────────────────────────────────────────
# Supported Formats and Extensions
# ──────────────────────────────────────────────────────────────────────────────

# Supported data file extensions
SUPPORTED_DATA_EXTENSIONS = {".json", ".jsonl", ".ndjson", ".gz"}

# Supported schema file extensions
SUPPORTED_SCHEMA_EXTENSIONS = {".json", ".sql", ".proto", ".yml", ".yaml"}

# Supported compression formats
SUPPORTED_COMPRESSION_FORMATS = {"gzip", "bzip2", "xz"}

# ──────────────────────────────────────────────────────────────────────────────
# Regular Expressions and Patterns
# ──────────────────────────────────────────────────────────────────────────────

# Pattern for detecting BigQuery table references
BIGQUERY_TABLE_PATTERN = r"^([a-zA-Z0-9_-]+):([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)$"

# Pattern for detecting GCS URLs
GCS_URL_PATTERN = r"^gs://([a-zA-Z0-9_-]+)/(.+)$"

# Pattern for detecting SQL identifiers
SQL_IDENTIFIER_PATTERN = r"^[a-zA-Z_][a-zA-Z0-9_]*$"

# ──────────────────────────────────────────────────────────────────────────────
# Version and Compatibility
# ──────────────────────────────────────────────────────────────────────────────

# Minimum Python version required
MIN_PYTHON_VERSION = (3, 9)

# JSON Schema version to use for generation
DEFAULT_JSON_SCHEMA_VERSION = "http://json-schema.org/draft-07/schema#"

# Default Spark schema version
DEFAULT_SPARK_VERSION = "3.0"

# ──────────────────────────────────────────────────────────────────────────────
# Logging and Debugging
# ──────────────────────────────────────────────────────────────────────────────

# Default log level
DEFAULT_LOG_LEVEL = "INFO"

# Maximum log file size in MB
MAX_LOG_FILE_SIZE = 50

# Number of log files to keep in rotation
LOG_FILE_BACKUP_COUNT = 5

# Log format string
DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# ──────────────────────────────────────────────────────────────────────────────
# Export all constants
# ──────────────────────────────────────────────────────────────────────────────

__all__ = [
    # Sampling and Record Processing
    "DEFAULT_SAMPLE_SIZE",
    "MAX_RECORD_SAFETY_LIMIT",
    "MAX_BATCH_SIZE",
    "DEFAULT_RECORD_INDEX",
    # Caching and Performance
    "DEFAULT_CACHE_TTL_SECONDS",
    "MAX_CACHE_SIZE_MB",
    "CACHE_CLEANUP_INTERVAL",
    # Code Quality and Complexity Limits
    "MAX_FUNCTION_COMPLEXITY",
    "MAX_NESTING_DEPTH",
    "MAX_FUNCTION_LENGTH",
    "MAX_LINE_LENGTH",
    # Schema Processing
    "MAX_SCHEMA_DEPTH",
    "DEFAULT_ARRAY_INDEX",
    "MAX_UNION_TYPES_DISPLAY",
    # File and I/O Operations
    "DEFAULT_BUFFER_SIZE",
    "MAX_IN_MEMORY_FILE_SIZE",
    "DEFAULT_COMMAND_TIMEOUT",
    # BigQuery and SQL
    "MAX_BIGQUERY_COLUMNS",
    "DEFAULT_BIGQUERY_LOCATION",
    "MAX_SQL_IDENTIFIER_LENGTH",
    # Validation and Error Handling
    "MAX_VALIDATION_ERRORS",
    "DEFAULT_RETRY_ATTEMPTS",
    "DEFAULT_RETRY_DELAY",
    "RETRY_BACKOFF_MULTIPLIER",
    # Output and Formatting
    "DEFAULT_OUTPUT_DIR",
    "MAX_CONSOLE_WIDTH",
    "DEFAULT_INDENT_SIZE",
    "MAX_LIST_DISPLAY_ITEMS",
    # GCS and Cloud Operations
    "DEFAULT_GCS_TIMEOUT",
    "MAX_GCS_FILE_SIZE",
    "GCS_RETRY_ATTEMPTS",
    # Migration Analysis
    "CRITICAL_FIELD_CHANGE_THRESHOLD",
    "MAJOR_SCHEMA_CHANGE_THRESHOLD",
    "MAX_DETAILED_FIELD_CHANGES",
    # Supported Formats and Extensions
    "SUPPORTED_DATA_EXTENSIONS",
    "SUPPORTED_SCHEMA_EXTENSIONS",
    "SUPPORTED_COMPRESSION_FORMATS",
    # Regular Expressions and Patterns
    "BIGQUERY_TABLE_PATTERN",
    "GCS_URL_PATTERN",
    "SQL_IDENTIFIER_PATTERN",
    # Version and Compatibility
    "MIN_PYTHON_VERSION",
    "DEFAULT_JSON_SCHEMA_VERSION",
    "DEFAULT_SPARK_VERSION",
    # Logging and Debugging
    "DEFAULT_LOG_LEVEL",
    "MAX_LOG_FILE_SIZE",
    "LOG_FILE_BACKUP_COUNT",
    "DEFAULT_LOG_FORMAT",
]
