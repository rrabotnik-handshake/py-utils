# schema-diff

**Compare schemas across JSON/NDJSON data, JSON Schema, Spark schemas, SQL DDL, dbt models, BigQuery live tables, and Protobuf definitions.**

A powerful tool for schema comparison, migration analysis, DDL generation, and advanced schema analytics with support for:

- ✅ **Cross-format comparison** - Compare any schema format against any other
- ✅ **Large file support** - Streaming processing with intelligent sampling
- ✅ **BigQuery integration** - Live table access and DDL generation
- ✅ **Migration analysis** - Comprehensive reports for schema changes
- ✅ **Schema generation** - Create schemas from data in multiple formats (JSON Schema, SQL DDL, BigQuery DDL, Spark, OpenAPI)
- ✅ **Advanced analytics** - Schema complexity analysis, pattern detection, and improvement suggestions
- ✅ **Intelligent detection** - Auto-detect file types and comparison modes
- ✅ **Google Cloud Storage** - Native support for GCS files with caching

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
schema-diff compare file1.json file2.json

# Cross-format comparison
schema-diff compare data.json schema.sql --right sql
schema-diff compare data.ndjson spark_schema.txt --right spark
schema-diff compare data.json my-project:dataset.table --right bigquery

# Advanced analysis with migration reports
schema-diff compare dataset1.json.gz dataset2.json.gz --all-records --output
schema-diff compare data.json schema.json --right jsonschema --fields user_id email profile.name

# Field filtering and sampling
schema-diff compare large_file1.json.gz large_file2.json.gz --sample-size 5000 --fields user_id profile
```

## 🏗️ Schema Generation

```bash
# Generate schemas from data (6 formats supported)
schema-diff generate data.json --format json_schema
schema-diff generate data.json --format bigquery_ddl --table-name users
schema-diff generate data.json --format spark --output
schema-diff generate data.json --format sql_ddl --table-name products
schema-diff generate data.json --format openapi --required-fields id name
schema-diff generate data.json --format bigquery_json --validate

# Advanced generation options
schema-diff generate large_dataset.json.gz --all-records --format bigquery_ddl --output
```

## ☁️ Google Cloud Storage (GCS) Support

**Seamlessly work with files stored in Google Cloud Storage using `gs://` paths or HTTPS URLs.**

```bash
# Compare GCS files directly (gs:// format)
schema-diff compare gs://my-bucket/old-data.json gs://my-bucket/new-data.json

# Compare GCS files using HTTPS URLs
schema-diff compare https://storage.cloud.google.com/bucket/file1.json https://storage.googleapis.com/bucket/file2.json

# Compare GCS file with local file
schema-diff compare gs://my-bucket/data.json local-schema.sql --right sql

# Generate schema from GCS file
schema-diff generate gs://my-bucket/production-data.json.gz --format bigquery_ddl --output

# Compare GCS file with BigQuery live table
schema-diff compare gs://my-bucket/data.json my-project:dataset.table --right bigquery
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
schema-diff config --gcs-info gs://my-bucket/data.json

# Get file information (HTTPS format)
schema-diff config --gcs-info https://storage.cloud.google.com/my-bucket/data.json

# Force re-download (bypass cache)
schema-diff compare gs://bucket/file1.json gs://bucket/file2.json --force-download

# Generate schema with caching
schema-diff generate gs://bucket/large-dataset.json.gz --format spark --all-records
```

## ☁️ BigQuery DDL

```bash
# Generate DDL for BigQuery tables
schema-diff ddl table my-project:dataset.table
schema-diff ddl batch my-project:dataset.table1 my-project:dataset.table2
schema-diff ddl dataset my-project:dataset --output
```

## 📊 Advanced Schema Analysis

```bash
# Comprehensive schema analysis
schema-diff analyze schema.json --all --output

# Specific analysis types
schema-diff analyze data.json --type data --complexity --patterns
schema-diff analyze spark_schema.txt --type spark --suggestions
schema-diff analyze my-project:dataset.table --type sql --report

# Multiple output formats
schema-diff analyze schema.json --all --format markdown --output
schema-diff analyze data.json --complexity --format json
```

## 📊 Migration Analysis

```bash
# Generate comprehensive migration reports
schema-diff compare old_schema.json new_schema.json --output
schema-diff compare old_data.json new_data.json --output
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
- **Schemas:** JSON Schema, Spark, SQL DDL, BigQuery DDL, dbt (manifest/yml/model), Protobuf
- **Live Sources:** BigQuery tables, dbt manifests
- **Generation:** JSON Schema, SQL DDL, BigQuery DDL, Spark, BigQuery JSON, OpenAPI 3.0

### Analysis Capabilities

- **Intelligent Sampling:** Efficient processing of large datasets
- **Type Inference:** Comprehensive type detection and union handling
- **Nested Structures:** Deep analysis of objects and arrays
- **Path Tracking:** Field location changes and restructuring
- **Presence Analysis:** Nullable vs required field detection
- **Schema Analytics:** Complexity analysis, pattern detection, improvement suggestions
- **Field Filtering:** Compare only specific fields across schemas

### Output Options

- **Human-readable Reports:** Clean, organized diff output with color coding
- **JSON Export:** Machine-readable comparison results
- **Migration Analysis:** Comprehensive change impact reports with markdown formatting
- **Schema Generation:** Multiple output formats (JSON Schema, SQL DDL, BigQuery DDL, Spark, OpenAPI)
- **Advanced Analytics:** Schema complexity metrics, pattern analysis, improvement suggestions
- **File Output:** Organized output directory structure with timestamps

## 🔧 Optional Dependencies

- **`[bigquery]`** - Live BigQuery table access, DDL generation, SQL highlighting (`google-cloud-bigquery`, `pygments`)
- **`[gcs]`** - Google Cloud Storage file download and caching support (`google-cloud-storage`)
- **`[validation]`** - Schema validation for generated outputs (`sqlparse`, `jsonschema`)
- **`[dev]`** - Testing framework and development tools (`pytest`, `pydocstyle`)

**Additional (experimental):**

- **`[modern-cli]`** - Enhanced CLI with `typer` and `rich` (future feature)
- **`[models]`** - Pydantic-based schema models (future feature)

Install only what you need to keep the tool lightweight and fast!

```bash
# Common combinations
pip install -e ".[bigquery,gcs]"          # Cloud features
pip install -e ".[validation,dev]"        # Quality assurance
pip install -e ".[bigquery,gcs,validation]"  # Production ready
```
