# CLI Format Redesign: `<family>:<representation>`

## Overview

Replace ambiguous single-token format specifiers with explicit two-part `<family>:<representation>` syntax.

## Format Specification

### Families & Representations

```
spark:       json (StructType JSON), ddl (toDDL), tree (printSchema)
bq:          table (live table), json (BigQuery schema JSON), ddl
sql:         ddl (local SQL DDL)
dbt:         manifest (manifest.json), yml (schema.yml), model (compiled SQL)
jsonschema:  json
proto:       sdl (.proto files)
data:        parquet, orc, csv, jsonl (implies inference)
```

### Argument Structure

```bash
schema-diff compare <file1> <file2> [--left <format>] [--right <format>]

# Positional arguments (auto-detected)
<file1>         First file/table/path
<file2>         Second file/table/path

# Optional format overrides
--left <family>:<representation>   # Override left format
--right <family>:<representation>  # Override right format
```

## Examples

### 1. Auto-detect (most common)

```bash
# Both formats auto-detected
schema-diff compare old.json new.json

# BigQuery table auto-detected by ":" in name
schema-diff compare data.json project:dataset.table
```

### 2. Spark tree vs JSON Schema (explicit)

```bash
schema-diff compare \
  base_employees.spark-structtype.tree.txt schema.json \
  --left spark:tree --right jsonschema:json
```

### 3. Local SQL DDL vs live BigQuery table

```bash
schema-diff compare \
  model.sql project:dataset.table \
  --left sql:ddl --right bq:table
```

### 4. dbt manifest vs BigQuery

```bash
schema-diff compare \
  target/manifest.json project:dataset.table \
  --left dbt:manifest --right bq:table
```

### 5. Data on GCS vs Spark schema JSON

```bash
schema-diff compare \
  gs://bucket/path/ df.schema.json \
  --left data:parquet --right spark:json
```

### 6. Two Spark formats (same family, different representations)

```bash
# Without explicit format, would be ambiguous
schema-diff compare schema.ddl printschema.txt \
  --left spark:ddl --right spark:tree
```

## Aliases (Backward Compatibility)

### Case-insensitive aliases map to canonical format:

```python
ALIASES = {
    # Legacy single-token → new format
    "spark": "spark:json",
    "spark-tree": "spark:tree",
    "spark-json": "spark:json",
    "spark-ddl": "spark:ddl",

    "bigquery": "bq:table",
    "bq": "bq:table",
    "bq-table": "bq:table",
    "bq-json": "bq:json",
    "bq-ddl": "bq:ddl",

    "sql": "sql:ddl",
    "sql-ddl": "sql:ddl",

    "jsonschema": "jsonschema:json",
    "json_schema": "jsonschema:json",

    "proto": "proto:sdl",
    "protobuf": "proto:sdl",

    "dbt-manifest": "dbt:manifest",
    "dbt-yml": "dbt:yml",
    "dbt-model": "dbt:model",

    "data": "data:json",  # Default to JSON data
    "json": "data:json",
    "ndjson": "data:jsonl",
    "jsonl": "data:jsonl",
    "parquet": "data:parquet",
    "orc": "data:orc",
    "csv": "data:csv",
}
```

## Defaults (Shorthand Support)

When representation is omitted, apply intelligent defaults:

```python
FAMILY_DEFAULTS = {
    "spark": "json",        # spark → spark:json (canonical)
    "bq": "table",          # bq → bq:table (with --*-ref)
    "sql": "ddl",           # sql → sql:ddl
    "dbt": "manifest",      # dbt → dbt:manifest
    "jsonschema": "json",   # jsonschema → jsonschema:json
    "proto": "sdl",         # proto → proto:sdl
    "data": "json",         # data → data:json
}
```

## Implementation Plan

### Phase 1: Parser Changes

1. **Update argument parser** (`cli/compare.py`):
   - Keep positional `file1` and `file2` arguments (unchanged)
   - Update `--left` / `--right` to accept `<family>:<representation>` format
   - Remove hardcoded `choices` list, validate dynamically instead

2. **Create format resolver** (`format_resolver.py`):
   - Parse `<family>:<representation>` strings
   - Apply aliases and defaults
   - Validate family/representation combinations

3. **Update type detection** (`io_utils.py`):
   - Modify `_guess_kind()` to recognize new formats
   - Handle both legacy single-token and new two-part formats
   - Return normalized format for internal use

### Phase 2: Backward Compatibility

1. **Positional arguments unchanged**:

   ```bash
   # Auto-detection still works (preferred for common cases)
   schema-diff compare file1.json file2.sql
   ```

2. **Legacy `--left` / `--right` values**:
   - `--left spark` → resolve to `spark:json` via alias
   - `--right bigquery` → resolve to `bq:table` via alias
   - All existing workflows continue to work

### Phase 3: Migration Path

1. **Deprecation warnings** (optional):

   ```
   Warning: Using legacy format 'spark'.
   Prefer explicit format: --left spark:tree
   ```

2. **Documentation updates**:
   - Update README with new format examples
   - Update help text
   - Add migration guide

## Benefits

### 1. **Explicitness**

- No ambiguity: `spark:tree` vs `spark:json` vs `spark:ddl`
- Clear what representation is being used

### 2. **Extensibility**

- Easy to add new representations: `spark:avro`, `bq:yaml`
- Family grouping makes related formats discoverable

### 3. **Consistency**

- All formats follow same `<family>:<representation>` pattern
- Reduces mental overhead

### 4. **Backward Compatibility**

- Aliases preserve existing workflows
- Positional arguments still work for common cases

## Implementation Code Sketch

```python
# format_resolver.py

from typing import Tuple

FAMILY_REPRESENTATIONS = {
    "spark": ["json", "ddl", "tree"],
    "bq": ["table", "json", "ddl"],
    "sql": ["ddl"],
    "dbt": ["manifest", "yml", "model"],
    "jsonschema": ["json"],
    "proto": ["sdl"],
    "data": ["json", "jsonl", "parquet", "orc", "csv"],
}

FAMILY_DEFAULTS = {
    "spark": "json",
    "bq": "table",
    "sql": "ddl",
    "dbt": "manifest",
    "jsonschema": "json",
    "proto": "sdl",
    "data": "json",
}

ALIASES = {
    # ... (as defined above)
}

def parse_format(format_string: str) -> Tuple[str, str]:
    """Parse format string into (family, representation).

    Args:
        format_string: Format like "spark:tree" or legacy "spark"

    Returns:
        Tuple of (family, representation)

    Raises:
        ValueError: If format is invalid
    """
    # Normalize to lowercase
    format_string = format_string.lower()

    # Check aliases first
    if format_string in ALIASES:
        format_string = ALIASES[format_string]

    # Parse family:representation
    if ":" in format_string:
        family, representation = format_string.split(":", 1)
    else:
        # No colon, assume it's just family
        family = format_string
        representation = FAMILY_DEFAULTS.get(family)
        if not representation:
            raise ValueError(f"Unknown family '{family}' and no default representation")

    # Validate
    if family not in FAMILY_REPRESENTATIONS:
        raise ValueError(f"Unknown family '{family}'")

    if representation not in FAMILY_REPRESENTATIONS[family]:
        valid = ", ".join(FAMILY_REPRESENTATIONS[family])
        raise ValueError(
            f"Invalid representation '{representation}' for family '{family}'. "
            f"Valid options: {valid}"
        )

    return family, representation


def format_to_internal_kind(family: str, representation: str) -> str:
    """Convert format to internal 'kind' used by existing code.

    This bridges new format system to existing implementation.
    """
    mapping = {
        ("spark", "json"): "spark",
        ("spark", "tree"): "spark-tree",
        ("spark", "ddl"): "spark-ddl",
        ("bq", "table"): "bigquery",
        ("bq", "json"): "bigquery-json",
        ("bq", "ddl"): "bigquery-ddl",
        ("sql", "ddl"): "sql",
        ("dbt", "manifest"): "dbt-manifest",
        ("dbt", "yml"): "dbt-yml",
        ("dbt", "model"): "dbt-model",
        ("jsonschema", "json"): "jsonschema",
        ("proto", "sdl"): "protobuf",
        ("data", "json"): "data",
        ("data", "jsonl"): "data",
        ("data", "parquet"): "data-parquet",
        ("data", "orc"): "data-orc",
        ("data", "csv"): "data-csv",
    }

    key = (family, representation)
    if key not in mapping:
        raise ValueError(f"No internal kind mapping for {family}:{representation}")

    return mapping[key]
```

## Testing

```python
# Test format parsing
assert parse_format("spark:tree") == ("spark", "tree")
assert parse_format("spark") == ("spark", "json")  # Default
assert parse_format("spark-tree") == ("spark", "tree")  # Alias
assert parse_format("bigquery") == ("bq", "table")  # Alias + default

# Test validation
with pytest.raises(ValueError):
    parse_format("spark:invalid")  # Invalid representation

with pytest.raises(ValueError):
    parse_format("invalid:foo")  # Invalid family
```

## Migration Timeline

### v1 (Current) - Add New Format

- Implement new format alongside existing
- Add deprecation warnings for old format
- Update documentation

### v2 (Next Release) - Prefer New Format

- Make new format the documented standard
- Old format still works but generates warnings

### v3 (Future) - Remove Old Format (Optional)

- Only new format accepted
- Clean up legacy code
