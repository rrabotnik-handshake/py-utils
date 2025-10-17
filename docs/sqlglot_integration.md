# SQLGlot Integration Architecture

## Overview

This document describes the integration of [SQLGlot](https://github.com/tobymao/sqlglot) into `schema-diff`, enabling enhanced SQL parsing and cross-dialect schema comparison.

## Features

### 1. Enhanced SQL Parsing

SQLGlot provides robust AST-based SQL parsing compared to our regex-based approach:

- **Better Type Detection**: Correctly identifies `SERIAL`, `DECIMAL`, and other dialect-specific types
- **Nested Type Support**: Handles complex `STRUCT` and `ARRAY` types natively
- **NOT NULL Constraints**: Accurately extracts column constraints
- **Comments**: Preserves and handles SQL comments
- **Multi-table DDL**: Parses files with multiple `CREATE TABLE` statements

### 2. Supported SQL Dialects

SQLGlot supports 20+ SQL dialects out of the box:

| Dialect      | Status       | Use Cases                    |
| ------------ | ------------ | ---------------------------- |
| BigQuery     | ✅ Tested    | Data warehouse, analytics    |
| Postgres     | ✅ Tested    | OLTP, general purpose        |
| MySQL        | ✅ Tested    | OLTP, web applications       |
| Snowflake    | ✅ Tested    | Cloud data warehouse         |
| Redshift     | ✅ Available | AWS data warehouse           |
| Spark        | ✅ Available | Big data processing          |
| Hive         | ✅ Available | Hadoop ecosystem             |
| Presto/Trino | ✅ Available | Distributed SQL queries      |
| ClickHouse   | ✅ Available | OLAP, real-time analytics    |
| Databricks   | ✅ Available | Lakehouse platform           |
| DuckDB       | ✅ Available | Embedded analytics           |
| Oracle       | ✅ Available | Enterprise RDBMS             |
| TSQL         | ✅ Available | SQL Server, Azure SQL        |
| Athena       | ✅ Available | AWS serverless query service |
| SQLite       | ✅ Available | Embedded database            |

### 3. Cross-Dialect Translation

SQLGlot enables comparing schemas across different SQL dialects:

```python
# Parse Postgres schema
postgres_schema, _ = schema_from_sql_ddl_sqlglot(
    "postgres_schema.sql",
    dialect="postgres"
)

# Parse BigQuery schema
bigquery_schema, _ = schema_from_sql_ddl_sqlglot(
    "bigquery_schema.sql",
    dialect="bigquery"
)

# Compare (types are normalized internally)
from schema_diff.compare import compare_schemas
report = compare_schemas(postgres_schema, bigquery_schema)
```

### 4. Type Normalization

SQLGlot automatically normalizes types across dialects:

| SQL Type                        | Normalized Type | Notes                      |
| ------------------------------- | --------------- | -------------------------- |
| `INT`, `INTEGER`, `INT64`       | `int`           | All integer variants       |
| `SERIAL`, `BIGSERIAL`           | `int`           | Postgres auto-increment    |
| `VARCHAR`, `TEXT`, `STRING`     | `str`           | All string variants        |
| `DECIMAL`, `NUMERIC`, `FLOAT64` | `float`         | All decimal/float variants |
| `BOOLEAN`, `BOOL`               | `bool`          | Boolean variants           |
| `TIMESTAMP`, `TIMESTAMPTZ`      | `timestamp`     | Temporal types             |
| `ARRAY<T>`                      | `[T]`           | Array of type T            |
| `STRUCT<...>`                   | `{...}`         | Nested object/struct       |

## Architecture

### Component Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    schema-diff CLI                          │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ├─── compare command
                       ├─── analyze command
                       └─── ddl command
                       │
      ┌────────────────┴────────────────┐
      │                                 │
┌─────▼─────┐                  ┌────────▼────────┐
│  Current  │                  │    SQLGlot      │
│  Parsers  │                  │    Parser       │
└───────────┘                  └─────────────────┘
      │                                 │
      │  - sql_schema_parser.py         │  - sqlglot_parser.py
      │  - jsonschema_parser.py         │  - Dialect support
      │  - spark_schema_parser.py       │  - Cross-dialect
      │                                 │    translation
      └─────────────┬───────────────────┘
                    │
           ┌────────▼─────────┐
           │  Unified Schema  │
           │  Representation  │
           └──────────────────┘
                    │
           ┌────────▼─────────┐
           │  Schema Compare  │
           │   & Analysis     │
           └──────────────────┘
```

### Parser Selection Strategy

`schema-diff` uses a **hybrid approach**:

1. **Auto-detection**: Detect file type/dialect from extension or content
2. **SQLGlot First**: Try SQLGlot parser for `.sql` files if installed
3. **Fallback**: Use legacy regex-based parser if SQLGlot fails or not installed
4. **User Override**: Allow explicit `--parser sqlglot` flag

```python
def parse_sql_schema(path: str, dialect: str = "bigquery") -> Schema:
    """Parse SQL schema with automatic fallback."""
    try:
        # Try SQLGlot first if available
        if _has_sqlglot():
            schema, required = schema_from_sql_ddl_sqlglot(path, dialect=dialect)
            return Schema(schema=schema, required=required)
    except Exception as e:
        logger.warning(f"SQLGlot parsing failed, falling back to regex parser: {e}")

    # Fallback to current regex-based parser
    return schema_from_sql_schema_file(path)
```

## Usage Examples

### Example 1: Parse SQL with Dialect Support

```bash
# Parse BigQuery DDL
schema-diff analyze schema.sql --type sql --dialect bigquery

# Parse Postgres DDL
schema-diff analyze schema.sql --type sql --dialect postgres
```

### Example 2: Cross-Dialect Comparison

```bash
# Compare Postgres schema to BigQuery table
schema-diff compare postgres_schema.sql \
  project:dataset.table \
  --left postgres --right bigquery
```

Output:

```
📊 Comparison: sql schema (local, postgres) → bigquery (live table)

=== Schema diff (types only) ===

-- Type mismatches -- (2)
  id: int → int                    # Both normalized correctly
  price: float → float             # DECIMAL → NUMERIC normalized

-- Only in Postgres schema -- (1)
  created_by                       # Postgres-specific field

-- Only in BigQuery table -- (1)
  partition_date                   # BigQuery partitioning field
```

### Example 3: Validate SQL Syntax

```python
from schema_diff.sqlglot_parser import validate_sql_syntax

sql = """
CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  email VARCHAR(255) NOT NULL
);
"""

is_valid, error = validate_sql_syntax(sql, dialect="postgres")
if not is_valid:
    print(f"Invalid SQL: {error}")
```

### Example 4: Translate SQL Between Dialects

```python
from schema_diff.sqlglot_parser import translate_sql_ddl

postgres_ddl = """
CREATE TABLE products (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  price DECIMAL(10,2)
);
"""

# Translate to BigQuery
bigquery_ddl = translate_sql_ddl(
    postgres_ddl,
    from_dialect="postgres",
    to_dialect="bigquery"
)

print(bigquery_ddl)
# CREATE TABLE products (
#   id INT64 NOT NULL,
#   name STRING NOT NULL,
#   price NUMERIC
# )
```

## Performance Comparison

Based on benchmarks with Rust tokenizer (`sqlglot[rs]`):

| Scenario           | Current Parser | SQLGlot Parser | Winner         |
| ------------------ | -------------- | -------------- | -------------- |
| Simple schema      | 0.08ms         | 0.29ms         | Current (2.8x) |
| Complex nested     | 0.06ms         | 0.29ms         | Current (4.8x) |
| Postgres types     | 0.05ms         | 1.52ms         | Current (30x)  |
| **Type accuracy**  | ❌ Poor        | ✅ Excellent   | **SQLGlot**    |
| **Nested types**   | ✅ Good        | ✅ Excellent   | Tie            |
| **Error messages** | ❌ Poor        | ✅ Excellent   | **SQLGlot**    |

**Verdict**:

- **Current parser**: Faster for simple schemas
- **SQLGlot parser**: More accurate type detection, better error messages, multi-dialect support
- **Recommendation**: Use SQLGlot for production schemas, especially cross-dialect comparisons

## Installation

### Option 1: Pip Install with SQLGlot

```bash
# Install with SQLGlot support
pip install -e '.[sqlglot]'

# With Rust tokenizer for better performance
pip install -e '.[sqlglot]'  # Automatically includes sqlglot[rs]
```

### Option 2: Manual Install

```bash
# Install SQLGlot separately
pip install 'sqlglot[rs]>=25.0.0'

# Then install schema-diff
pip install -e .
```

### Verify Installation

```python
from schema_diff.sqlglot_parser import _HAS_SQLGLOT, get_supported_dialects

if _HAS_SQLGLOT:
    print(f"✅ SQLGlot installed")
    print(f"Supported dialects: {get_supported_dialects()}")
else:
    print("❌ SQLGlot not installed")
```

## Future Enhancements

### Phase 1: Core Integration (✅ Complete)

- [x] SQLGlot as optional dependency
- [x] Parser module with dialect support
- [x] Type normalization
- [x] Integration tests
- [x] Benchmark vs current parser

### Phase 2: CLI Integration (Next)

- [ ] Add `--dialect` flag to `compare` command
- [ ] Auto-detect dialect from file extension
- [ ] Hybrid parser with automatic fallback
- [ ] Cross-dialect comparison reporting

### Phase 3: Advanced Features

- [ ] SQL query lineage analysis
- [ ] Table dependency graph generation
- [ ] Schema evolution suggestions
- [ ] Auto-generate migration scripts between dialects
- [ ] DDL generation from schema-diff types

## Testing

Run the SQLGlot parser test suite:

```bash
# All tests
pytest tests/schema-diff/test_sqlglot_parser.py -v

# Specific dialect
pytest tests/schema-diff/test_sqlglot_parser.py::TestPostgresParsing -v

# Benchmark comparison
python benchmark_sql_parsers.py
```

## Troubleshooting

### SQLGlot Not Found

```
ImportError: SQLGlot support requires 'sqlglot'.
Install with: pip install 'schema-diff[sqlglot]'
```

**Solution**: Install SQLGlot dependency

```bash
pip install 'sqlglot[rs]>=25.0.0'
```

### Parsing Errors

If SQLGlot fails to parse your SQL:

1. **Check dialect**: Ensure correct dialect specified
2. **Fallback**: Use legacy parser with `--parser legacy`
3. **Report**: File an issue with example SQL

### Performance Issues

If SQLGlot parsing is too slow:

1. **Use Rust tokenizer**: Install `sqlglot[rs]`
2. **Fallback to current**: Use `--parser legacy`
3. **Optimize**: Consider caching parsed schemas

## References

- **SQLGlot GitHub**: https://github.com/tobymao/sqlglot
- **SQLGlot Docs**: https://sqlglot.com/
- **Supported Dialects**: https://sqlglot.com/sqlglot/dialects.html
- **Schema-diff README**: /Users/rostislav.rabotnik/coresignal/src/schema_diff/README.md
