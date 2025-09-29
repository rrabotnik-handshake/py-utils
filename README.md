# schema-diff

**Compare schemas across JSON/NDJSON data, JSON Schema, Spark schemas, SQL DDL, dbt models, BigQuery live tables, and Protobuf definitions.**

A powerful tool for schema comparison, migration analysis, and DDL generation with support for:

- ✅ **Cross-format comparison** - Compare any schema format against any other
- ✅ **Large file support** - Streaming processing with intelligent sampling
- ✅ **BigQuery integration** - Live table access and DDL generation
- ✅ **Migration analysis** - Comprehensive reports for schema changes
- ✅ **Schema generation** - Create schemas from data in multiple formats
- ✅ **Intelligent detection** - Auto-detect file types and comparison modes

## 🚀 Quick Start

```bash
# Install
pip install -e .

# Install with optional features
pip install -e ".[bigquery]"      # BigQuery DDL + live tables
pip install -e ".[gcs]"           # Google Cloud Storage support
pip install -e ".[validation]"    # Schema validation
pip install -e ".[bigquery,gcs,validation]"  # All features
```

## 🔄 Schema Comparison

```bash
# Basic data comparison
schema-diff file1.json file2.json

# Cross-format comparison
schema-diff data.json schema.sql --right sql
schema-diff data.ndjson spark_schema.txt --right spark
schema-diff data.json my-project:dataset.table --right bigquery

# Advanced analysis
schema-diff dataset1.json.gz dataset2.json.gz --all-records --migration-analysis report.md
schema-diff data.json schema.json --right jsonschema --fields user_id email profile.name
```

## 🏗️ Schema Generation

```bash
# Generate schemas from data
schema-diff generate data.json --format json_schema
schema-diff generate data.json --format bigquery_ddl --table-name users
schema-diff generate data.json --format spark --output
```

## ☁️ Google Cloud Storage (GCS) Support

**Seamlessly work with files stored in Google Cloud Storage using `gs://` paths or HTTPS URLs.**

```bash
# Compare GCS files directly (gs:// format)
schema-diff gs://my-bucket/old-data.json gs://my-bucket/new-data.json

# Compare GCS files using HTTPS URLs
schema-diff https://storage.cloud.google.com/bucket/file1.json https://storage.googleapis.com/bucket/file2.json

# Compare GCS file with local file
schema-diff gs://my-bucket/data.json local-schema.sql --right sql

# Generate schema from GCS file
schema-diff generate gs://my-bucket/production-data.json.gz --format bigquery_ddl --output

# Compare GCS file with BigQuery live table
schema-diff gs://my-bucket/data.json my-project:dataset.table --right bigquery
```

### 🔧 GCS Features

- **✅ Automatic download** - Files are cached locally in `./data/` directory
- **✅ Smart caching** - Reuses downloaded files unless `--force-download` is used
- **✅ Metadata inspection** - Use `--gcs-info gs://bucket/file` to view object details
- **✅ Multiple URL formats** - Supports `gs://`, `https://storage.cloud.google.com/`, and `https://storage.googleapis.com/`
- **✅ All formats supported** - Works with JSON, NDJSON, compressed files, schemas
- **✅ Authentication** - Uses your existing `gcloud auth` or service account credentials

### 📋 GCS Setup

```bash
# Install GCS support
pip install -e ".[gcs]"

# Authenticate (choose one method)
gcloud auth application-default login                    # Interactive login
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json # Service account

# Set default project (required)
gcloud config set project YOUR_PROJECT_ID               # Set default project
# OR
export GOOGLE_CLOUD_PROJECT=YOUR_PROJECT_ID             # Environment variable
```

### 🔧 Common Authentication Issues

**Error: "Project was not passed and could not be determined from the environment"**

```bash
# Fix: Set your default GCP project
gcloud config set project YOUR_PROJECT_ID

# Verify configuration
gcloud config list
```

### 🛠️ GCS Commands

```bash
# Get file information (gs:// format)
schema-diff --gcs-info gs://my-bucket/data.json

# Get file information (HTTPS format)
schema-diff --gcs-info https://storage.cloud.google.com/my-bucket/data.json

# Force re-download (bypass cache)
schema-diff gs://bucket/file1.json gs://bucket/file2.json --force-download

# Generate schema with caching
schema-diff generate gs://bucket/large-dataset.json.gz --format spark --all-records
```

## ☁️ BigQuery DDL

```bash
# Generate DDL for BigQuery tables
schema-diff ddl my-project:dataset.table
schema-diff ddl-batch my-project:dataset table1 table2 table3
schema-diff ddl-dataset my-project:dataset --out-dir ./ddl
```

## 📊 Migration Analysis

```bash
# Generate comprehensive migration reports
schema-diff old_schema.json new_schema.json --output
schema-diff old_data.json new_data.json --output --output-format json
```

## 📚 Documentation

- **Complete Documentation:** [`src/schema_diff/README.md`](src/schema_diff/README.md)
- **Command Reference:** All commands, options, and examples
- **Optional Dependencies:** BigQuery, validation, and development features
- **Configuration:** File-based and environment configuration
- **Troubleshooting:** Common issues and performance tips

## 🎯 Key Features

### Supported Formats

- **Data:** JSON, NDJSON, JSONL (with .gz compression)
- **Schemas:** JSON Schema, Spark, SQL DDL, BigQuery DDL, dbt, Protobuf
- **Live Sources:** BigQuery tables, dbt manifests

### Analysis Capabilities

- **Intelligent Sampling:** Efficient processing of large datasets
- **Type Inference:** Comprehensive type detection and union handling
- **Nested Structures:** Deep analysis of objects and arrays
- **Path Tracking:** Field location changes and restructuring
- **Presence Analysis:** Nullable vs required field detection

### Output Options

- **Human-readable Reports:** Clean, organized diff output
- **JSON Export:** Machine-readable comparison results
- **Migration Analysis:** Comprehensive change impact reports
- **Schema Dumps:** Normalized schema representations

## 🔧 Optional Dependencies

- **`[bigquery]`** - Live BigQuery table access, DDL generation, SQL highlighting
- **`[gcs]`** - Google Cloud Storage file download and caching support
- **`[validation]`** - Schema validation for generated outputs
- **`[dev]`** - Testing framework for contributors

Install only what you need to keep the tool lightweight and fast!
