# py-utils

Utilities and tooling for Python data workflows.  
Currently includes the **`schema_diff`** package for comparing schemas across multiple formats including JSON/NDJSON data, JSON Schema, Spark schemas, SQL DDL (including BigQuery), dbt models, live BigQuery tables, and Protobuf definitions. Also includes BigQuery DDL generation and schema extraction capabilities.

## ✨ What's inside

- `schema_diff`: Compare schemas across data sources and schema definitions with comprehensive analysis capabilities, nested field support, human-readable reports, BigQuery DDL generation, and live table schema extraction.
- `tests/`: Pytest suites for all included packages.

## 🚀 Quick Start

```bash
# Install
cd coresignal
pip install -e .

# Basic comparison
schema-diff file1.json file2.json

# Comprehensive analysis (all records)
schema-diff dataset1.json.gz dataset2.json.gz --all-records

# Compare specific fields
schema-diff data.json schema.sql --right sql --fields user_id email profile.name

# Data vs schema formats
schema-diff data.ndjson schema.json --right jsonschema
schema-diff data.json spark_schema.txt --right spark
```

See `src/schema_diff/README.md` for complete documentation.

