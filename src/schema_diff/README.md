# schema-diff

**Compare schemas across multiple formats and generate schema documentation.**

## Table of Contents

- [🚀 Quick Start](#-quick-start)
- [📋 Common Use Cases](#-common-use-cases)
- [📊 Example Output](#-example-output)
- [🔧 Advanced Usage](#-advanced-usage)
- [📦 Supported Formats](#-supported-formats)
- [🔧 Command Reference](#-command-reference)
- [📊 Output Sections](#-output-sections)
- [🔄 Migration Analysis](#-migration-analysis)
- [🛠️ Configuration](#️-configuration)
- [🔍 Troubleshooting](#-troubleshooting)

## 🚀 Quick Start

```bash
# Install
pip install -e .

# Basic comparison (most common use case)
schema-diff compare file1.json file2.json

# Compare with different formats
schema-diff compare data.json schema.sql --right sql
schema-diff compare data.json my-project:dataset.table --right bigquery

# Generate schema from data
schema-diff generate data.json --format json_schema
```

## 📋 Common Use Cases

### Compare Data Files

```bash
# Compare two JSON files
schema-diff compare old_data.json new_data.json

# Compare compressed files
schema-diff compare data1.json.gz data2.ndjson.gz

# Quick check (first record only)
schema-diff compare file1.json file2.json --first-record
```

### Compare Data vs Schema

```bash
# Data vs JSON Schema
schema-diff compare data.json schema.json --right jsonschema

# Data vs SQL schema
schema-diff compare data.json schema.sql --right sql --right-table users

# Data vs BigQuery table
schema-diff compare data.json my-project:dataset.table --right bigquery
```

### Compare Schemas

```bash
# JSON Schema vs SQL
schema-diff compare schema.json db.sql --left jsonschema --right sql

# BigQuery table vs BigQuery table
schema-diff compare project:dataset.table1 project:dataset.table2

# Spark schema vs data
schema-diff compare data.json spark_schema.txt --left data --right spark
```

### Generate Schemas

```bash
# Generate JSON Schema
schema-diff generate data.json --format json_schema

# Generate BigQuery DDL
schema-diff generate data.json --format bigquery_ddl --table-name my_table

# Generate SQL DDL
schema-diff generate data.json --format sql_ddl
```

## 📊 Example Output

```
=== Schema diff (types only, old_schema.json → new_schema.json) ===

-- Only in old_schema.json -- (2)
  deprecated_field
  old_version

-- Only in new_schema.json -- (3)
  new_feature
  updated_timestamp
  version

-- Presence mismatches (NULL/required) -- (1)
  user_id: required → nullable

-- Type mismatches -- (2)
  is_active: bool → int
  score: float → str
```

---

## 🔧 Advanced Usage

### Installation Options

```bash
# Install with optional features
pip install -e ".[bigquery]"      # BigQuery DDL generation + live table access
pip install -e ".[gcs]"           # Google Cloud Storage support
pip install -e ".[validation]"    # Schema validation for generated schemas
pip install -e ".[dev]"          # Development tools (pytest)

# Install multiple features
pip install -e ".[bigquery,gcs,validation]"  # BigQuery + GCS + validation
```

### Advanced Comparison Options

```bash
# Process all records (comprehensive analysis)
schema-diff compare file1.json file2.json --all-records

# Compare only specific fields
schema-diff compare file1.json file2.json --fields headline full_name industry

# Show sampled records
schema-diff compare file1.json file2.json -k 5 --show-samples

# Save results to file
schema-diff compare file1.json file2.json --output
```

### BigQuery DDL Generation

```bash
# Single table DDL
schema-diff ddl table my-project:dataset.table

# Real-world example
schema-diff ddl table 'handshake-production:coresignal.linkedin_member_us_snapshot'

# Batch DDL generation
schema-diff ddl batch my-project:dataset.table1 my-project:dataset.table2

# Entire dataset DDL
schema-diff ddl dataset my-project:dataset

# Save DDL to files
schema-diff ddl table my-project:dataset.table --output
schema-diff ddl dataset my-project:dataset --output
```

---

## 📦 Supported Formats

### Data Files

- **JSON:** `.json` files with single objects or arrays
- **NDJSON/JSONL:** `.ndjson`, `.jsonl` files with one JSON object per line
- **Compressed:** All formats support `.gz` compression
- **Large Files:** Streaming support for files of any size
- **Google Cloud Storage:** `gs://bucket/path`, `https://storage.cloud.google.com/bucket/path`

### Schema Formats

- **JSON Schema:** Draft-07 compatible schemas
- **Spark Schema:** Output from `df.printSchema()` with deep nested parsing
- **SQL DDL:** PostgreSQL and BigQuery DDL including `ARRAY<...>`, `STRUCT<...>`
- **dbt:** `manifest.json`, `schema.yml`, and model `.sql` files
- **BigQuery Live:** Direct table access via `project:dataset.table` references
- **Protobuf:** `.proto` files with explicit message selection

### Auto-Detection

`schema-diff` automatically detects file types:

- **Extension-based:** `.sql`, `.yml`, `.txt`, `.proto`, `.json`
- **Content-based:** Analyzes file contents to determine format
- **No configuration needed** for most comparisons

---

## 🔧 Command Reference

### Main Commands

- `compare` - Compare two schemas or data files
- `generate` - Generate schema from data file
- `ddl` - Generate BigQuery DDL from live tables
- `config` - Configuration management
- `analyze` - Advanced schema analysis

### Compare Command

```bash
schema-diff compare [OPTIONS] file1 file2
```

**Key Options:**

- `--first-record` - Compare only the first record
- `--all-records` - Process all records (no sampling)
- `--fields [FIELDS ...]` - Compare only specific fields
- `--left/--right {data,json_schema,spark,sql,bigquery,protobuf,dbt-manifest}` - Specify file types
- `--output` - Save results to file

### Generate Command

```bash
schema-diff generate [OPTIONS] data_file
```

**Key Options:**

- `--format {json_schema,sql_ddl,bigquery_ddl,spark}` - Output format
- `--table-name TABLE_NAME` - Table name for DDL formats
- `--required-fields [FIELDS ...]` - Mark fields as required

### DDL Command

```bash
schema-diff ddl {table,batch,dataset} [OPTIONS] ...
```

**Examples:**

- `schema-diff ddl table my-project:dataset.table`
- `schema-diff ddl batch table1 table2 table3`
- `schema-diff ddl dataset my-project:dataset`

---

## 📊 Output Sections

`schema-diff` provides comprehensive comparison results organized into clear sections:

### Common Fields

Fields that appear in both schemas, showing their types and any differences between them.

### Only in Source / Only in Target

Fields that exist in only one of the compared schemas, helping identify added or removed fields.

### Type Mismatches

Fields with the same name but different data types between schemas (e.g., `bool → int`, `str → float`).

### Presence mismatches (NULL/required)

Differences in field optionality and nullability constraints (e.g., `required → nullable`, `nullable → required`).

### Path Changes

Fields that appear in different locations or structures between schemas, useful for understanding structural changes.

### When is Presence/Nullability Shown?

The "Presence mismatches (NULL/required)" section is **only displayed** for **schema-to-schema** comparisons where nullability constraints are explicitly defined and meaningful.

**Automatically skipped for:**

- ✅ **Data-to-data** comparisons - Nullability depends on which records were sampled, making it unreliable noise
- ✅ **Data-to-schema** comparisons - Data files don't have explicit nullability declarations, only actual data presence

**Shown for:**

- ✅ **Schema-to-schema** comparisons - Both sides have explicit `NULL`/`NOT NULL` or `required`/`optional` declarations
- Examples: SQL → SQL, BigQuery → BigQuery, JSON Schema → JSON Schema, dbt → dbt

---

## 🔄 Migration Analysis

The migration analysis feature generates comprehensive reports for schema migrations based on comparison results:

### Report Sections

- **📊 Migration Overview** - High-level summary with field counts and compatibility metrics
- **🎯 Compatibility Summary** - Overall migration complexity rating
- **❌ Critical Issues** - Breaking changes requiring immediate attention
- **⚠️ Warnings** - Changes that may impact your system
- **💡 Migration Recommendation** - Actionable steps and timeline guidance
- **📊 Complete Schema Comparison** - Full technical comparison with detailed field analysis

### Usage

```bash
# Generate migration analysis report
schema-diff compare old_data.json new_data.json --output

# Migration analysis is automatically generated when using --output
```

---

## 🛠️ Configuration

`schema-diff` uses optional dependencies to keep the core installation lightweight while providing extended functionality:

### 🔧 Core Installation (Default)

```bash
pip install -e .
```

**Includes:** Basic schema comparison, data analysis, JSON/Spark/SQL/dbt/Protobuf support
**Dependencies:** `ijson`, `deepdiff`, `pyyaml`, `protobuf`

### ☁️ BigQuery Features

```bash
pip install -e ".[bigquery]"
```

**Adds:** Live BigQuery table access, DDL generation, SQL syntax highlighting
**Use when:** Working with BigQuery tables, generating DDL, need colored SQL output
**Dependencies:** `google-cloud-bigquery`, `pygments`

### 🌐 Google Cloud Storage (GCS) Support

```bash
pip install -e ".[gcs]"
```

**Adds:** GCS file download, caching, metadata inspection, HTTPS URL support
**Use when:** Working with files stored in Google Cloud Storage
**Dependencies:** `google-cloud-storage`

### ✅ Schema Validation

```bash
pip install -e ".[validation]"
```

**Adds:** Validation of generated schemas (JSON Schema, SQL DDL, etc.)
**Use when:** Generating schemas and want syntax validation
**Dependencies:** `sqlparse`, `jsonschema`

### 🧪 Development Tools

```bash
pip install -e ".[dev]"
```

**Adds:** Testing framework for contributors
**Use when:** Contributing to the project or running tests
**Dependencies:** `pytest`

### 🚨 Missing Dependency Errors

If you see errors like:

- `ModuleNotFoundError: No module named 'google.cloud'` → Install `[bigquery]` or `[gcs]`
- `ImportError: cannot import name 'bigquery'` → Install `[bigquery]`
- `Google Cloud Storage support requires 'google-cloud-storage'` → Install `[gcs]`
- Schema validation warnings → Install `[validation]` for better validation

---

## 🔧 Command Reference

### Main Command: `schema-diff`

The `schema-diff` tool uses subcommands for different operations:

- `compare` - Compare two schemas or data files (default)
- `generate` - Generate schema from data file
- `ddl` - Generate BigQuery DDL from live tables
- `config` - Configuration management
- `analyze` - Advanced schema analysis and insights

### Subcommand: `compare`

Compare two schemas or data files.

```bash
schema-diff compare [OPTIONS] file1 file2
```

#### Positional Arguments

- `file1` - Left input (data or schema)
- `file2` - Right input (data or schema)

#### Record Selection & Sampling

- `--first-record` - Compare only the first record from each DATA file
- `--sample-size SAMPLE_SIZE` - Number of records to sample (default: 1000)
- `-k, --samples N` - Alias for --sample-size
- `--all-records` - Process ALL records instead of sampling (may be memory intensive)
- `--record RECORD` - Process specific record number
- `--seed SEED` - Random seed for reproducible sampling
- `--show-samples` - Print the chosen/sampled records
- `--both-modes` - Run two comparisons: chosen record(s) AND random sampled records

#### Output Control

- `--no-color` - Disable ANSI colors
- `--show-common` - Print the sorted list of fields that appear in both sides
- `--fields [FIELDS ...]` - Compare only specific fields (space-separated list)

#### Export Options

- `--json-out JSON_OUT` - Write diff JSON to this path
- `--output` - Save comparison results and migration analysis to ./output directory

#### GCS Options

- `--force-download` - Force re-download of GCS files even if they exist locally
- `--gcs-info` - Show GCS file information and exit

#### Schema Type Selection

- `--left {data,json_schema,jsonschema,spark,sql,protobuf,dbt-manifest,dbt-yml,dbt-model,bigquery}` - Left file type (auto-detected if not specified)
- `--right {data,json_schema,jsonschema,spark,sql,protobuf,dbt-manifest,dbt-yml,dbt-model,bigquery}` - Right file type (auto-detected if not specified)

#### Schema-Specific Options

- `--table TABLE` - BigQuery table name (for BigQuery live table comparisons)
- `--right-table RIGHT_TABLE` - Table name for right-side SQL schema (alias for --table)
- `--model MODEL` - dbt model name (for dbt manifest/yml comparisons)
- `--right-message RIGHT_MESSAGE` - Protobuf message to use for file2

#### Direct Schema Options

- `--json-schema JSON_SCHEMA.json` - Compare DATA file1 vs a JSON Schema file
- `--spark-schema SPARK_SCHEMA.txt` - Compare DATA file1 vs a Spark-style schema text
- `--sql-schema SCHEMA.sql` - Compare DATA file1 vs a SQL schema
- `--sql-table TABLE` - Table name to select if --sql-schema has multiple tables

#### Inference Options

- `--infer-datetimes` - Treat ISO-like strings as timestamp/date/time on the DATA side

#### Google Cloud Storage (GCS) Options

- `--force-download` - Re-download GCS files even if they exist locally (default: use cached files)

### Subcommand: `generate`

Generate schema in various formats from a data file.

```bash
schema-diff generate [OPTIONS] data_file
```

#### Options

- `--format {json_schema,sql_ddl,bigquery_ddl,spark,bigquery_json,openapi}` - Output schema format (default: json_schema)
- `--output` - Save schema to ./output/schemas/ directory with auto-generated filename
- `--table-name TABLE_NAME` - Table name for SQL DDL formats (default: generated_table)
- `--sample-size SAMPLE_SIZE` - Number of records to sample (default: 1000)
- `--all-records` - Process all records for comprehensive schema
- `--required-fields [REQUIRED_FIELDS ...]` - Field paths that should be marked as required/NOT NULL
- `--validate` - Validate generated schema syntax (default: enabled)
- `--no-validate` - Skip schema validation
- `--force-download` - Force re-download of GCS files even if they exist locally
- `--gcs-info` - Show GCS file information and exit

### Subcommand: `ddl`

Generate BigQuery DDL from live tables.

```bash
schema-diff ddl {table,batch,dataset} [OPTIONS] ...
```

#### DDL Subcommands

##### `ddl table` - Single Table DDL

Generate DDL for a single BigQuery table.

```bash
schema-diff ddl table [OPTIONS] table_ref
```

**Arguments:**

- `table_ref` - BigQuery table reference (project:dataset.table)

**Options:**

- `--output` - Save DDL to ./output directory

**Examples:**

```bash
# Generate DDL to stdout
schema-diff ddl table 'handshake-production:coresignal.linkedin_member_us_snapshot'

# Save to file
schema-diff ddl table 'my-project:analytics.users' --output
```

##### `ddl batch` - Multiple Tables DDL

Generate DDL for multiple tables.

```bash
schema-diff ddl batch [OPTIONS] table_ref [table_ref ...]
```

**Arguments:**

- `table_ref` - BigQuery table references (project:dataset.table)

**Options:**

- `--output` - Save DDL files to ./output directory

**Examples:**

```bash
# Generate DDL for multiple tables
schema-diff ddl batch 'my-project:dataset.users' 'my-project:dataset.orders' 'my-project:dataset.products'

# Save all to files
schema-diff ddl batch 'my-project:dataset.table1' 'my-project:dataset.table2' --output
```

##### `ddl dataset` - Dataset DDL

Generate DDL for all tables in a BigQuery dataset.

```bash
schema-diff ddl dataset [OPTIONS] dataset_ref
```

**Arguments:**

- `dataset_ref` - BigQuery dataset reference (project:dataset)

**Options:**

- `--output` - Save DDL files to ./output directory

**Examples:**

```bash
# Generate DDL for all tables in a dataset
schema-diff ddl dataset 'my-project:analytics'

# Save all table DDLs to files
schema-diff ddl dataset 'handshake-production:coresignal' --output
```

### Subcommand: `config`

Configuration management and GCS utilities.

```bash
schema-diff config {show,init} [OPTIONS] ...
```

#### Config Subcommands

##### `config show` - Display Configuration

Display current configuration values from files and environment.

```bash
schema-diff config show
```

##### `config init` - Create Configuration

Create a new configuration file with default values.

```bash
schema-diff config init [OPTIONS] [config_path]
```

**Arguments:**

- `config_path` - Path for new config file (default: schema-diff.yml)

**Options:**

- `--force` - Overwrite existing config file

#### GCS Information

Get metadata about Google Cloud Storage objects:

```bash
schema-diff config --gcs-info gs://bucket/file.json
schema-diff config --gcs-info https://storage.cloud.google.com/bucket/file.json
```

### Subcommand: `analyze`

Perform advanced schema analysis including complexity metrics, pattern detection, and improvement suggestions.

```bash
schema-diff analyze [OPTIONS] schema_file
```

#### Positional Arguments

- `schema_file` - Schema or data file to analyze

#### Analysis Types

- `--complexity` - Show complexity analysis (nesting depth, type distribution, field counts)
- `--patterns` - Show pattern analysis (repeated structures, semantic patterns, naming conventions)
- `--suggestions` - Show improvement suggestions (optimization recommendations, best practices)
- `--report` - Generate comprehensive analysis report
- `--all` - Show all analysis types (equivalent to --complexity --patterns --suggestions --report)

#### Schema Type Options

- `--type {data,json_schema,jsonschema,spark,sql,protobuf,dbt-manifest,dbt-yml,dbt-model,bigquery}` - Schema type (auto-detected if not specified)
- `--table TABLE` - BigQuery table name (for SQL schemas)
- `--model MODEL` - dbt model name (for dbt schemas)
- `--message MESSAGE` - Protobuf message name (for protobuf schemas)

#### Data Processing Options

- `--sample-size SAMPLE_SIZE` - Number of records to sample for data files (default: 1000)
- `--all-records` - Process all records (no sampling limit)

#### Output Options

- `--format {text,json,markdown}` - Output format (default: text)
- `--output` - Save analysis to output directory

#### Examples

```bash
# Basic complexity analysis
schema-diff analyze user_schema.json --complexity

# Comprehensive analysis with all metrics
schema-diff analyze data.json --type data --all --output

# Pattern analysis for Spark schema
schema-diff analyze spark_schema.txt --type spark --patterns

# Generate markdown report
schema-diff analyze schema.json --report --format markdown --output

# Analyze BigQuery table schema
schema-diff analyze my_table.sql --type sql --table users --suggestions
```

---

## 📊 Output Sections

`schema-diff` provides comprehensive comparison results organized into clear sections:

### Common Fields

Fields that appear in both schemas, showing their types and any differences between them.

### Only in Source / Only in Target

Fields that exist in only one of the compared schemas, helping identify added or removed fields.

### Type Mismatches

Fields with the same name but different data types between schemas (e.g., `bool → int`, `str → float`).

### Presence mismatches (NULL/required)

Differences in field optionality and nullability constraints (e.g., `required → nullable`, `nullable → required`).

### Path Changes

Fields that appear in different locations or structures between schemas, useful for understanding structural changes.

---

## 🔄 Migration Analysis

The migration analysis feature generates comprehensive reports for schema migrations based purely on the schema comparison results:

### Report Sections

- **📊 Migration Overview** - High-level summary with field counts and compatibility metrics
- **🎯 Compatibility Summary** - Overall migration complexity rating with emoji-coded indicators
- **❌ Critical Issues** - Breaking changes requiring immediate attention
- **⚠️ Warnings** - Changes that may impact your system
- **💡 Migration Recommendation** - Actionable steps and timeline guidance
- **📊 Complete Schema Comparison** - Full technical comparison with collapsible sections, color-coded differences, and detailed field analysis

### Analysis Logic

The analysis is data-driven and uses common patterns to categorize changes:

- **Critical Fields** - IDs, keys, names, emails, status fields
- **Metadata Fields** - Created/updated timestamps, audit fields
- **Type Conflicts** - Always flagged as breaking changes
- **Structure Changes** - Field moves and restructuring
- **Nullability Changes** - Optionality and presence differences

---

## 🎯 Philosophy & Behavior

### Sampling Strategy

- **Default:** Smart sampling (1000 records) for performance
- **Comprehensive:** `--all-records` for complete analysis
- **Targeted:** `--first-record` or `--record N` for specific records

### Type Inference

- **Conservative:** Infers the most specific type that fits all samples
- **Union Types:** Handles mixed types gracefully (e.g., `union(int|str)`)
- **Nested Structures:** Deep analysis of objects and arrays
- **Null Handling:** Distinguishes between missing fields and null values

### Intelligent Schema Analysis

`schema-diff` provides intelligent analysis and clear output formatting:

- **Type Detection:** Automatically infers types from data samples with support for complex nested structures
- **Format Support:** Works across multiple schema formats (JSON Schema, SQL DDL, Spark, BigQuery, dbt, Protobuf)
- **Smart Comparison:** Separates type changes from nullability changes for clearer understanding
- **Large File Support:** Handles files of any size with streaming and sampling capabilities

### Presence vs. Types

- **Data Files:** Field presence indicates optionality
- **Schema Files:** Explicit nullable/required declarations
- **Alignment:** Matches presence constraints with type nullability

### Path Normalization

- **Dot Notation:** `parent.child.grandchild`
- **Array Elements:** `array[].field` for consistent representation
- **Nested Objects:** Flattened for comparison while preserving structure

---

## 🛠️ Configuration

### Configuration Files

`schema-diff` supports configuration files for default settings:

```yaml
# schema-diff.yml
bigquery:
  project: my-default-project
  dataset: my-default-dataset

sampling:
  default_samples: 1000
  max_memory_mb: 512

output:
  color: auto
  show_common: false
```

### Environment Variables

- `SCHEMA_DIFF_PROJECT` - Default BigQuery project
- `SCHEMA_DIFF_DATASET` - Default BigQuery dataset
- `NO_COLOR` - Disable colored output

### File Discovery

Configuration files are automatically discovered in:

1. Current directory: `schema-diff.yml` or `schema-diff.yaml`
2. Home directory: `~/.schema-diff.yml`
3. System config: `/etc/schema-diff.yml`

---

## 🔍 Troubleshooting

### Common Issues

**Large Files / Memory Usage:**

```bash
# Use sampling instead of --all-records
schema-diff compare large1.json.gz large2.json.gz -k 5000

# Process specific records only
schema-diff compare large1.json.gz large2.json.gz --first-record
```

**BigQuery Authentication:**

```bash
# Set up application default credentials
gcloud auth application-default login

# Or use service account key
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

**Google Cloud Storage (GCS) Issues:**

```bash
# Install GCS support
pip install -e ".[gcs]"

# Set up authentication
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID

# Alternative: Use environment variables
export GOOGLE_CLOUD_PROJECT=YOUR_PROJECT_ID
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# Check current configuration
gcloud config list
gcloud auth list

# Test GCS access
schema-diff config --gcs-info gs://your-bucket/test-file.json
```

**Common GCS Errors:**

- `Project was not passed and could not be determined from the environment` → Set default project with `gcloud config set project YOUR_PROJECT_ID`
- `Google Cloud Storage support requires 'google-cloud-storage'` → Install with `pip install -e ".[gcs]"`
- `403 Forbidden` → Check bucket permissions and authentication
- `404 Not Found` → Verify bucket name and file path

**Missing Dependencies:**

```bash
# Install BigQuery support
pip install -e ".[bigquery]"

# Install validation support
pip install -e ".[validation]"
```

**Complex Nested Structures:**

```bash
# Focus on specific fields
schema-diff compare complex1.json complex2.json --fields user.profile.name user.settings

# Show samples to understand structure
schema-diff compare complex1.json complex2.json --show-samples -k 3
```

### Performance Tips

1. **Use Sampling:** Default sampling is usually sufficient for schema comparison
2. **Field Filtering:** Use `--fields` to focus on relevant parts of large schemas
3. **Specific Records:** Use `--first-record` for quick checks
4. **Compression:** `.gz` files are handled efficiently
5. **Streaming:** Large NDJSON files are processed in streaming mode

---

## 🤝 Contributing

### Development Setup

```bash
# Clone and install in development mode
git clone <repository>
cd schema-diff
pip install -e ".[dev,bigquery,validation]"

# Run tests
pytest tests/

# Run specific test categories
pytest tests/test_compare.py
pytest tests/test_bigquery_ddl.py
```

### Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src/schema_diff

# Run specific test file
pytest tests/test_json_schema_parser.py -v
```

---

## 📄 License

[Add your license information here]

---

## 🔗 Related Tools

- **BigQuery CLI:** `bq` command-line tool
- **dbt:** Data transformation tool
- **JSON Schema:** Schema validation standard
- **Apache Spark:** Big data processing framework
