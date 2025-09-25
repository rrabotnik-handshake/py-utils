# schema-diff

**Compare schemas across JSON/NDJSON data, JSON Schema, Spark/Databricks schemas, SQL DDL, dbt models, BigQuery live tables, and Protobuf definitions.**

Works with large files (streams arrays/NDJSON, gz OK), infers types from samples, aligns optionality vs. presence constraints, and prints clean diffs. Also includes BigQuery DDL generation, schema extraction, and migration analysis capabilities.

---

## 🚀 Quick Start

```bash
# Install (editable)
pip install -e .

# Install with optional features
pip install -e ".[bigquery]"      # BigQuery DDL generation + live table access
pip install -e ".[validation]"    # Schema validation for generated schemas
pip install -e ".[dev]"          # Development tools (pytest)

# Install multiple features
pip install -e ".[bigquery,validation]"  # BigQuery + validation
pip install -e ".[bigquery,validation,dev]"  # Everything
```

### Basic Schema Comparison

```bash
# Basic data vs data (sample 1000 records by default)
schema-diff file1.ndjson.gz file2.json.gz

# First record only
schema-diff file1.json file2.json --first-record

# Show the sampled records
schema-diff file1.json file2.json -k 5 --show-samples

# Process ALL records (comprehensive analysis)
schema-diff file1.json file2.json --all-records

# Compare only specific fields
schema-diff file1.json file2.json --fields headline full_name industry

# Combine all-records with field filtering
schema-diff file1.json file2.json --all-records --fields headline member_id
```

### Cross-Format Comparisons

```bash
# Data vs JSON Schema
schema-diff data.ndjson schema.json --right jsonschema --first-record

# Data vs SQL (choose table)
schema-diff data.json schema.sql --right sql --right-table my_table --first-record

# JSON Schema vs SQL (multi-table file)
schema-diff schema.json db.sql --left jsonschema --right sql --right-table users

# Spark schema vs data
schema-diff data.json spark_schema.txt --left data --right spark

# Protobuf schema vs data
schema-diff data.json demo.proto --right proto --right-message User

# BigQuery live table vs data
schema-diff data.json my-project:dataset.table --right bigquery

# Data vs BigQuery live table (auto-detected)
schema-diff data.json handshake-production:coresignal.users
```

### Schema Generation

```bash
# Generate JSON Schema from data
schema-diff generate data.json --format json_schema

# Generate BigQuery DDL
schema-diff generate data.json --format bigquery_ddl --table-name my_table

# Generate with validation
schema-diff generate data.json --format sql_ddl --validate

# Save to file
schema-diff generate data.json --format spark --output

# Mark specific fields as required
schema-diff generate data.json --format json_schema --required-fields user_id email
```

### BigQuery DDL Generation

```bash
# Single table DDL
schema-diff ddl my-project:dataset.table

# Batch DDL generation
schema-diff ddl-batch my-project:dataset table1 table2 table3

# Entire dataset DDL
schema-diff ddl-dataset my-project:dataset

# Save DDL to files
schema-diff ddl my-project:dataset.table --out table.sql
schema-diff ddl-dataset my-project:dataset --out-dir ./ddl --combined-out all_tables.sql
```

### Migration Analysis

```bash
# Generate migration analysis report (markdown by default)
schema-diff old_data.json new_data.json --output

# Different output formats
schema-diff old_data.json new_data.json --output --output-format json
schema-diff old_data.json new_data.json --output --output-format text
```

---

## 📋 Supported Input Formats

### Data Files

- **JSON:** `.json` files with single objects or arrays
- **NDJSON/JSONL:** `.ndjson`, `.jsonl` files with one JSON object per line
- **Compressed:** All formats support `.gz` compression
- **Large Files:** Streaming support for files of any size

### Schema Formats

- **JSON Schema:** Draft-07 compatible schemas with `$schema`, `type`, `properties`, `required`, `oneOf/anyOf/allOf`, `enum`
- **Spark Schema:** Output from `df.printSchema()` with deep nested `array<struct<...>>` parsing
- **SQL DDL:** PostgreSQL and BigQuery DDL including `ARRAY<...>`, `STRUCT<...>`, backticked identifiers
- **dbt:** `manifest.json`, `schema.yml`, and model `.sql` files
- **BigQuery Live:** Direct table access via `project:dataset.table` references
- **Protobuf:** `.proto` files with explicit message selection

---

## 🎯 Intelligent Auto-Detection

`schema-diff` automatically detects file types and comparison modes:

### Extension-Based Detection

- `.sql` → Content analysis (SQL DDL vs dbt model)
- `.yml/.yaml` → dbt schema files
- `.txt` → Spark schema dumps
- `.proto` → Protobuf schemas
- `.json/.gz` → Content analysis (see below)

### Content-Based Detection for JSON Files

- **dbt manifest:** `nodes`, `sources`, `child_map`, or `metadata.dbt_version`
- **JSON Schema:** `$schema`, `type: "object"`, or schema keywords (`oneOf`, `properties`, etc.)
- **NDJSON:** Multiple lines starting with `{`
- **Data:** Everything else

### Content-Based Detection for SQL Files

- **dbt model:** `SELECT`, Jinja (`{{}}`), or dbt functions (`ref()`, `source()`, etc.)
- **SQL DDL:** `CREATE TABLE`, `ALTER TABLE`, etc.

### Automatic Mode Selection

- **General mode:** Different file types or any schema sources detected
- **Classic data-to-data:** Both files detected as data with same type
- **Legacy modes:** Explicit `--json-schema`, `--spark-schema`, `--sql-schema` arguments

No `--left`/`--right` arguments needed for most comparisons!

---

## 📦 Optional Dependencies

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

- `ModuleNotFoundError: No module named 'google.cloud'` → Install `[bigquery]`
- `ImportError: cannot import name 'bigquery'` → Install `[bigquery]`
- Schema validation warnings → Install `[validation]` for better validation

---

## 🔧 Command Reference

### Main Command: `schema-diff`

#### Positional Arguments

- `file1` - Left input (data or schema)
- `file2` - Right input (data or schema) [optional]

#### Record Selection

- `--first-record` - Compare only the first record from each DATA file
- `--record N` - Compare the N-th record from each DATA file (1-based)
- `--record1 N` - N-th record for file1 (overrides --record)
- `--record2 N` - N-th record for file2 (overrides --record)
- `--both-modes` - Run two comparisons: chosen record(s) AND random sampled records

#### Sampling

- `-k, --samples N` - Records to sample per DATA file (default: 1000)
- `--all-records` - Process ALL records instead of sampling (may be memory intensive)
- `--seed SEED` - Random seed for reproducible sampling
- `--show-samples` - Print the chosen/sampled records

#### Output Control

- `--no-color` - Disable ANSI colors
- `--force-color` - Force ANSI colors even if stdout is not a TTY
- `--no-presence` - Suppress 'Missing / optional (presence)' section
- `--show-common` - Print the sorted list of fields that appear in both sides
- `--fields FIELD [FIELD ...]` - Compare only specific fields

#### Export Options

- `--json-out PATH` - Write diff JSON to this path
- `--dump-schemas PATH` - Write normalized left/right schemas to this path
- `--output, -o` - Save comparison results and migration analysis to ./output directory
- `--output-format {markdown,text,json}` - Format for migration analysis report (default: markdown)

#### Schema Type Selection

- `--left {auto,data,jsonschema,spark,sql,dbt-manifest,dbt-yml,dbt-model,bigquery}` - Kind of file1
- `--right {auto,data,jsonschema,spark,sql,dbt-manifest,dbt-yml,dbt-model,bigquery}` - Kind of file2
- `--left-table LEFT_TABLE` - Table to select for file1 when using SQL
- `--right-table RIGHT_TABLE` - Table to select for file2 when using SQL
- `--left-model LEFT_MODEL` - Model name for file1 when using dbt
- `--right-model RIGHT_MODEL` - Model name for file2 when using dbt
- `--left-message LEFT_MESSAGE` - Protobuf message to use for file1
- `--right-message RIGHT_MESSAGE` - Protobuf message to use for file2

#### Legacy Options (Deprecated)

- `--json-schema JSON_SCHEMA.json` - Compare DATA file1 vs a JSON Schema file
- `--spark-schema SPARK_SCHEMA.txt` - Compare DATA file1 vs a Spark-style schema text
- `--sql-schema SCHEMA.sql` - Compare DATA file1 vs a SQL schema
- `--sql-table TABLE` - Table name to select if --sql-schema has multiple tables

#### Inference Options

- `--infer-datetimes` - Treat ISO-like strings as timestamp/date/time on the DATA side

### Subcommand: `generate`

Generate schema in various formats from a data file.

```bash
schema-diff generate [OPTIONS] data_file
```

#### Options

- `--format, -f {json_schema,sql_ddl,bigquery_ddl,spark,bigquery_json,openapi}` - Output schema format
- `--output, -o` - Save schema to ./output directory with auto-generated filename
- `--table-name, -t TABLE_NAME` - Table name for SQL DDL formats
- `--samples, -k SAMPLES` - Number of records to sample (default: adaptive)
- `--all-records` - Process all records for comprehensive schema
- `--first-record` - Use only the first record
- `--required-fields [REQUIRED_FIELDS ...]` - Field paths that should be marked as required/NOT NULL
- `--show-samples` - Show the data samples being analyzed
- `--validate` - Validate generated schema syntax (default: enabled)
- `--no-validate` - Skip schema validation

### Subcommand: `ddl`

Generate pretty, formatted DDL for a single BigQuery table.

```bash
schema-diff ddl [OPTIONS] table_re
```

#### Arguments

- `table_re` - BigQuery table reference (project:dataset.table or dataset.table)

#### Options

- `--color {auto,always,never}` - Colorize SQL output (respects NO_COLOR)
- `--no-constraints` - Skip primary key and foreign key constraints
- `--out PATH` - Write DDL to file (uncolored)

### Subcommand: `ddl-batch`

Generate DDL for multiple tables in a dataset with optimized batch queries.

```bash
schema-diff ddl-batch [OPTIONS] dataset_re tables [tables ...]
```

#### Arguments

- `dataset_re` - BigQuery dataset reference (project:dataset or dataset)
- `tables` - Table names to generate DDL for

#### Options

- `--color {auto,always,never}` - Colorize SQL output
- `--no-constraints` - Skip constraints
- `--out-dir DIR` - Write each table's DDL to separate files in directory
- `--combined-out PATH` - Write all DDLs to a single file

### Subcommand: `ddl-dataset`

Generate DDL for all tables in a BigQuery dataset.

```bash
schema-diff ddl-dataset [OPTIONS] dataset_re
```

#### Arguments

- `dataset_re` - BigQuery dataset reference (project:dataset or dataset)

#### Options

- `--color {auto,always,never}` - Colorize SQL output
- `--no-constraints` - Skip constraints
- `--exclude [EXCLUDE ...]` - Table names to exclude
- `--include [INCLUDE ...]` - Only include these table names
- `--out-dir DIR` - Write each table's DDL to separate files in directory
- `--combined-out PATH` - Write all DDLs to a single file
- `--manifest PATH` - Write table list and metadata to JSON manifest file

### Subcommand: `formats`

Show all supported schema output formats with descriptions.

```bash
schema-diff formats
```

### Subcommand: `config-show`

Display current configuration values from files and environment.

```bash
schema-diff config-show [OPTIONS]
```

#### Options

- `--config PATH` - Path to config file (default: auto-discover)

### Subcommand: `config-init`

Create a new configuration file with default values.

```bash
schema-diff config-init [OPTIONS] [config_path]
```

#### Arguments

- `config_path` - Path for new config file (default: schema-diff.yml)

#### Options

- `--project PROJECT` - Default BigQuery project
- `--dataset DATASET` - Default BigQuery dataset
- `--force` - Overwrite existing config file

---

## 📊 Output Sections

### Common Fields

Fields that appear in both schemas with their types and any differences.

### Only in Source / Only in Target

Fields that appear in only one of the compared schemas.

### Type Mismatches

Fields with the same name but different types between schemas.

### Path Changes

Fields that appear in different locations or structures between schemas.

### Missing Data / NULL-ability

Differences in field optionality and presence constraints.

---

## 🔄 Migration Analysis

The migration analysis feature generates comprehensive reports for schema migrations based purely on the schema comparison results:

### Report Sections

- **📊 Summary Statistics** - Field counts, compatibility metrics
- **🎯 Compatibility Assessment** - Overall migration complexity rating
- **❌ Breaking Changes** - Issues requiring immediate attention
- **⚠️ Warnings** - Changes that may impact your system
- **💡 Recommendations** - Actionable steps for migration
- **📊 Complete Schema Comparison** - Full technical comparison details

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
schema-diff large1.json.gz large2.json.gz -k 5000

# Process specific records only
schema-diff large1.json.gz large2.json.gz --first-record
```

**BigQuery Authentication:**

```bash
# Set up application default credentials
gcloud auth application-default login

# Or use service account key
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

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
schema-diff complex1.json complex2.json --fields user.profile.name user.settings

# Show samples to understand structure
schema-diff complex1.json complex2.json --show-samples -k 3
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
