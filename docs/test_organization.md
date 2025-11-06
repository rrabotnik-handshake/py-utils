# Test Organization

## Overview

This document describes the complete test organization for the schema-diff project, covering all 387 tests across 33 test files.

## Test Files by Category

### **Parser Tests**

| File                                | Tests | Purpose                   |
| ----------------------------------- | ----- | ------------------------- |
| `test_json_schema_parser.py`        | ~15   | JSON Schema parsing       |
| `test_sql_schema_parser.py`         | ~12   | SQL DDL parsing           |
| `test_spark_schema_parser.py`       | ~10   | Spark schema parsing      |
| `test_sqlglot_parser.py`            | ~18   | SQLGlot-based SQL parsing |
| `test_dbt_parsers.py`               | ~20   | dbt manifest/yml parsing  |
| `test_dbt_model_parser.py`          | ~8    | dbt model parsing         |
| `test_protobuf_schema_parser.py`    | ~12   | Protobuf schema parsing   |
| `test_bigquery_api_json_support.py` | 15    | BigQuery API JSON parsing |

### **Type Analysis & Nullability**

| File                                  | Tests | Purpose                             |
| ------------------------------------- | ----- | ----------------------------------- |
| `test_type_analysis.py`               | 27    | Low-level type analysis functions   |
| `test_type_nullability_separation.py` | 9     | Report building integration         |
| `test_nullability_all_formats.py`     | 11    | Cross-format nullability comparison |

### **BigQuery Support**

| File                                | Tests | Purpose                            |
| ----------------------------------- | ----- | ---------------------------------- |
| `test_bigquery_support.py`          | 13    | BigQuery table reference detection |
| `test_bigquery_ddl.py`              | 7     | DDL generation from live tables    |
| `test_bigquery_api_json_support.py` | 15    | BigQuery API JSON format           |
| `test_cli_bigquery_integration.py`  | 2     | BigQuery CLI integration           |

### **CLI & Integration**

| File                               | Tests | Purpose                             |
| ---------------------------------- | ----- | ----------------------------------- |
| `test_cli.py`                      | 3     | CLI functionality (sampling, modes) |
| `test_cli_functional.py`           | 11    | CLI parameter validation            |
| `test_cli_format_integration.py`   | 4     | Format detection integration        |
| `test_cli_bigquery_integration.py` | 2     | BigQuery CLI help text              |

### **Core Functionality**

| File                       | Tests | Purpose                    |
| -------------------------- | ----- | -------------------------- |
| `test_compare.py`          | ~25   | Schema comparison logic    |
| `test_report.py`           | ~18   | Report generation          |
| `test_schema_from_data.py` | ~20   | Schema inference from data |
| `test_auto_detection.py`   | ~15   | Format auto-detection      |
| `test_parser_factory.py`   | ~12   | Parser factory pattern     |
| `test_format_resolver.py`  | ~10   | Format string resolution   |

### **Enhanced Features**

| File                                 | Tests | Purpose                       |
| ------------------------------------ | ----- | ----------------------------- |
| `test_field_filtering.py`            | ~8    | Field filtering functionality |
| `test_migration_analysis.py`         | ~12   | Migration analysis features   |
| `test_enhanced_functionality.py`     | ~15   | Enhanced comparison features  |
| `test_schema_generation_enhanced.py` | ~10   | Schema generation             |
| `test_analyze_command.py`            | ~8    | Analyze CLI command           |

### **Utilities & Infrastructure**

| File                                | Tests | Purpose                          |
| ----------------------------------- | ----- | -------------------------------- |
| `test_io_utils.py`                  | ~10   | I/O utilities                    |
| `test_gcs_integration.py`           | ~15   | Google Cloud Storage integration |
| `test_decorators.py`                | ~12   | Decorator patterns               |
| `test_ddl_integration.py`           | ~15   | DDL integration testing          |
| `test_architectural_integration.py` | ~20   | Architectural integration        |

---

## Test Coverage

**Total**: 387 tests passing

**Nullability Tests**: 62 tests

- Type analysis: 27
- Report integration: 9
- BigQuery API JSON: 15
- Cross-format: 11

**Principles**:

- Single responsibility per test file
- No duplication
- Format-specific + cross-format coverage
- Regression prevention

---

## Adding New Formats

When adding a schema format:

1. Create format-specific tests
2. Add to `test_nullability_all_formats.py`
3. Add to `SCHEMA_SOURCES` in `src/schema_diff/compare.py`
4. Tests will verify presence injection

---

## Running Tests

```bash
# All tests
pytest tests/schema-diff/ -v

# Nullability tests only
pytest tests/schema-diff/test_*nullability*.py tests/schema-diff/test_bigquery_api_json*.py -v

# Specific file
pytest tests/schema-diff/test_type_analysis.py -v
```
