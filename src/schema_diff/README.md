# schema-diff

Compare schemas across **JSON/NDJSON data**, **JSON Schema**, **Spark/Databricks schemas**, **SQL DDL**, **dbt models**, and **Protobuf (.proto)**.
Works with large files (streams arrays/NDJSON, gz OK), infers types from samples, aligns optionality vs. presence constraints, and prints clean diffs.

---

## Quick start

```bash
# Install (editable)
pip install -e .

# Basic data vs data (sample 3 records by default)
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
```

### Supported inputs
- **Data:** `.json`, `.ndjson`, `.jsonl`, and their `.gz` variants
- **JSON Schema:** `.json` (draft formats supported for `type`, `format`, `properties`, `required`, `oneOf/anyOf/allOf`, `enum`)
- **Spark:** output of `df.printSchema()` (or similar text) — now with deep nested `array<struct<...>>` parsing
- **SQL:** Postgres-like DDL and **BigQuery DDL** (incl. `ARRAY<...>`, `STRUCT<...>`, backticked names)
- **dbt:** `manifest.json` or `schema.yml`
- **Protobuf:** `.proto` files with explicit message selection (`--left-message` / `--right-message`)

---

## CLI usage

```bash
schema-diff <left_path> <right_path> [options]
```

### Record selection (for data inputs)
- `--first-record` — compare only record #1 (or use `--record N`)
- `--record N` / `--record1 N` / `--record2 N`
- Sampling:
  - `-k, --samples N` — sample N records (default 3)
  - `--all-records` — process ALL records instead of sampling (comprehensive but memory-intensive)
  - `--seed SEED` — reproducible sampling
  - `--show-samples` — print the chosen/sampled records

### Input kinds (auto or explicit)
Left/right can be auto-detected from extension or forced:

- `--left/--right`: one of
  - `data` | `jsonschema` | `spark` | `sql` | `dbt-manifest` | `dbt-yml` | `proto` | `auto` (default)

Extra selectors:
- `--left-table NAME` / `--right-table NAME` — choose table from SQL DDL when file has multiple tables
- `--left-model NAME` / `--right-model NAME` — choose dbt model when needed
- `--left-message NAME` / `--right-message NAME` — choose Protobuf message to diff

### Output control
- `--no-color`, `--force-color`
- `--no-presence` — hide the "Missing Data / NULL-ABILITY" section
- `--show-common` — print fields present in both schemas with matching types (includes nested array fields with `[]` notation like `experience[].title`)
- `--fields FIELD [FIELD ...]` — compare only specific fields (supports nested paths like `experience.title` and array elements like `experience[].title`)
- `--json-out PATH` — save diff JSON
- `--dump-schemas PATH` — save the two normalized schemas to a file

### Timestamp inference (for data)
- `--infer-datetimes` — treat ISO-like strings as `timestamp`/`date`/`time` when inferring from data

---

## How comparisons work

1. **Each side is loaded to a _type tree_** (pure types: `int|float|bool|str|date|time|timestamp|object|array`).
   Sources:
   - **Data** → inferred by sampling; fields seen absent are *not* treated as missing here.
   - **JSON Schema** → converted from schema (`format: date-time` → `timestamp`, etc.).
   - **Spark** → parsed & mapped to internal types, now with recursive `array<struct<...>>` parsing.
   - **SQL** → parsed & mapped to internal types; BigQuery `ARRAY<...>` → `[elem_type]`, `STRUCT<...>` recursively parsed with full nested support.
   - **dbt** → built from manifest/schema.yml (columns + tests).
   - **Protobuf** → parsed from `.proto` files, expanding nested messages, handling `repeated`, `map<K,V>`, `enum`, and `oneof`.

2. **Presence constraints** are collected separately as a set of **`required_paths`**:
   - JSON Schema → `required` arrays
   - SQL → `NOT NULL` columns
   - dbt → `not_null` tests
   - Protobuf → `required` fields (proto2)
   - Data → *(no presence info)*

3. **Normalization** makes both sides comparable:
   - Collapses empties like `empty_array → array`, `empty_object → object`, `empty_string → str`
   - Flattens and sorts unions (`union(str|missing)`), deduplicates `"any"`
   - Arrays remain `[elem_type]` if known, otherwise `"array"`

4. **DeepDiff** compares the two normalized type trees.
   Output sections:
   - "Only in left/right" — keys present on one side only
   - "Missing Data / NULL-ABILITY" — data presence vs schema nullability differences with source-aware terminology:
     * Data sources: show base types (`str`, `int`) or `missing data`
     * Schema sources: show `nullable type` format
     * Filters out identical formatted types to show only meaningful differences
   - "Common" — fields present in both sides with matching types (`--show-common`) using `[]` notation for arrays
   - "Type mismatches" — real type conflicts (e.g., `int` vs `str`), filtered to exclude sampling artifacts
   - "Path changes" — same field names in different locations with 3-section structure:
     * Shared field locations and/or field paths (common to both sides)
     * Only in [left]: paths unique to left side
     * Only in [right]: paths unique to right side

---

## Advanced Features

### Comprehensive Analysis with `--all-records`

By default, schema-diff samples a small number of records (3) for performance. Use `--all-records` to process every record for comprehensive field discovery:

```bash
# Standard sampling (fast, may miss rare fields)
schema-diff large_dataset1.json.gz large_dataset2.json.gz -k 10

# Comprehensive analysis (slower, finds all fields)
schema-diff large_dataset1.json.gz large_dataset2.json.gz --all-records
```

**When to use `--all-records`:**
- Fields appear infrequently in your data
- You need to ensure complete field coverage
- Data quality validation requires comprehensive analysis
- You're comparing schemas where sampling might miss important differences

**Safety features:**
- Built-in 1M record limit to prevent memory issues
- Progress indication for large datasets

### Focused Comparison with `--fields`

Compare only specific fields instead of analyzing the entire schema:

```bash
# Compare only headline and full_name fields
schema-diff users1.json users2.json --fields headline full_name

# Support for nested fields with dot notation
schema-diff profiles1.json profiles2.json --fields experience.title education.institution

# Support for array element paths with [] notation (clean array semantics)
schema-diff profiles1.json profiles2.json --fields 'experience[].title' 'education[].institution'

# Implicit array notation (automatically handles array elements)
schema-diff profiles1.json profiles2.json --fields experience.title education.institution

# Comma-separated or space-separated field lists
schema-diff data1.json data2.json --fields "headline,full_name,industry"
```

**Use cases:**
- Focus on specific fields of interest
- Reduce noise from unrelated schema differences
- Performance optimization for large schemas
- Field-specific data quality checks
- Compare nested array elements (e.g., `experience[].title` vs `experience[].company`)

**Array Support:**
- **Explicit notation**: `experience[].title` - targets array element fields using clean array semantics
- **Implicit notation**: `experience.title` - automatically handles array elements
- **Mixed usage**: Can combine both notations in the same command
- **Legacy notation**: `experience[0].title` automatically normalized to `experience[].title` for consistency

### Combining Features

Use both features together for powerful targeted analysis:

```bash
# Comprehensive analysis of specific fields
schema-diff dataset1.json.gz dataset2.json.gz --all-records --fields headline member_id industry

# Field-focused comparison with comprehensive coverage
schema-diff events1.json.gz events2.json.gz --all-records --fields event_type timestamp user_id --show-common
```

---

## Any↔Any mode

You can mix any two inputs:

```bash
# Data vs SQL
schema-diff data.ndjson schema.sql --left data --right sql --right-table users

# JSON Schema vs Spark
schema-diff schema.json spark.txt --left jsonschema --right spark

# dbt model vs SQL
schema-diff manifest.json warehouse.sql   --left dbt-manifest --left-model analytics.users   --right sql --right-table users

# Protobuf vs JSON Schema
schema-diff demo.proto schema.json --left proto --left-message User --right jsonschema
```

KIND auto-detection by extension:
- `.ndjson`, `.jsonl`, `.json(.gz)` → `data`
- `.sql` → `sql`
- `.yml` / `.yaml` → `dbt-yml`
- `.proto` → `proto`
- `.json` may also be `jsonschema` or `dbt-manifest` — use explicit `--left/--right` to be precise.

---

## Examples

```bash
# Compare Protobuf schema vs Data
schema-diff data.json demo.proto --right proto --right-message User

# Compare Protobuf vs SQL table
schema-diff demo.proto model.sql --left proto --left-message User --right sql --right-table users
```

### Spark deep struct/array parsing

The parser supports **recursive parsing of nested arrays and structs**, including BigQuery-like types:

```bash
# Spark schema with nested array<struct<...>>
cat > spark_nested.txt <<'EOF'
root
 |-- id: long (nullable = false)
 |-- tags: array<string> (nullable = true)
 |-- events: array<struct<
 |    ts: timestamp,
 |    meta: struct<
 |      key: string,
 |      value: string
 |    >
 |  >> (nullable = true)
EOF

# Compare Spark schema to data
schema-diff data.json spark_nested.txt --left data --right spark --show-common
```

**Example output:**

```
=== Schema diff (types only, data.json → spark_nested.txt) ===

-- Only in spark_nested.txt --
  events[].ts
  events[].meta.key
  events[].meta.value

-- Missing Data / NULL-ABILITY -- (2)
  id: int → nullable int
  tags: [str] → nullable array

-- Common fields (types agree) -- (0)

-- Type mismatches -- (0)

-- Path changes (same field name in different locations) -- (1)
  user_id:
    Shared field locations and/or field paths:
      • user_id
    Only in spark_nested.txt:
      • events[].user_id
```

---

## JSON output

```bash
schema-diff data.json schema.sql --right sql --right-table p --json-out diff.json
```

`diff.json` contains the same sections printed to the console plus metadata. Use this for CI and auditing.

---

## 🧪 Tests

We use `pytest`:

```bash
pip install -e .[dev]
pytest -q
```

Tests include:
- **Parsers:** SQL (Postgres & BigQuery), Spark (with deep nested types), JSON Schema, Protobuf
- **Data I/O:** NDJSON/JSON/arrays, gz handling
- **Integration:** CLI flows (`data↔data`, `data↔jsonschema`, `jsonschema↔sql`, `proto↔data`, etc.)
- **Determinism:** seeded sampling produces stable results

---

## 🧭 Philosophy & behavior

- **Presence vs Type:** We do **not** bake nullability into types. Optionality is tracked in `required_paths` and shown in the *Missing Data / NULL-ABILITY* section with source-aware terminology that distinguishes data presence ("missing data") from schema nullability ("nullable").
- **Sampling:** Inference is by merging k sampled records (or all records with `--all-records`):
  - If a field is present in some records but not others, its type becomes a union (`union(int|missing)` after presence normalization in comparisons).
  - If an array has mixed element types, the element becomes a union (`["union(int|str)"]` → normalized per rules).
  - Use `--all-records` for comprehensive field discovery when sampling might miss infrequent fields.
- **Normalization:** Consistent, comparable trees regardless of source:
  - Empty → base type (`empty_array`→`array`, `empty_object`→`object`, `empty_string`→`str`)
  - Unions deduplicated/sorted, `"any"` removed when other specific types exist
  - Arrays stay `[elem_type]` if known, else `"array"`

---

## ⚠️ Limitations

- **Sampling-based inference:** Types are inferred from sampled records; noisy data can under/over-report unions or presence. Use `--all-records` for comprehensive analysis when sampling is insufficient.
- **Union explosion:** If many distinct types appear (e.g., 20 unique shapes in 20 samples), unions can grow.
- **SQL dialects:** Postgres + BigQuery DDL covered; other dialects may need regex additions (PRs welcome).
- **Complex unions:** Union explosion can occur with very diverse data; filtered type mismatches exclude common sampling artifacts.
- **dbt support:** Requires `manifest.json` or `schema.yml`; we currently honor `not_null` tests for presence, not full assertion semantics.
- **Protobuf support:** Only `proto2`/`proto3` message/enum/map/oneof basics are supported. Complex options and extensions are ignored.

---

## Troubleshooting

- **Wrong kind detection for `.json`:** Use `--left/--right` to force `jsonschema` or `dbt-manifest`.
- **"Table not found" for SQL:** Provide `--*-table` when the DDL file defines multiple tables.
- **Data presence noise:** Increase `-k/--samples` and consider `--infer-datetimes` if timestamps are formatted strings. The "Missing Data / NULL-ABILITY" section uses source-aware terminology: "missing data" for data sources, "nullable" for schema sources.
- **Missing fields in comparison:** Fields that appear infrequently may not be sampled. Use `--all-records` for comprehensive field discovery.
- **Too much output noise:** Use `--fields` to focus on specific fields of interest. Path changes section shows field location differences.
- **Memory issues with large files:** The `--all-records` option has a built-in 1M record safety limit. For larger datasets, use sampling with higher `-k` values.
- **Array notation confusion:** Legacy `[0]` notation is automatically normalized to `[]` for cleaner output and consistency.
