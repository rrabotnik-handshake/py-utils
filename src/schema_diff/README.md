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
  - `--seed SEED` — reproducible sampling
  - `--show-samples` — print the sampled records

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
- `--no-presence` — hide the presence/optionality section
- `--show-common` — print fields present in both schemas with matching types
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
   - **SQL** → parsed & mapped to internal types; arrays become `[elem_type]`, `STRUCT<...>` → `object`.
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
   - “Only in left/right” — keys present on one side only
   - “Missing / optional (presence)” — when type is the same but optionality differs
   - “Common” — fields present in both sides with matching types (`--show-common`)
   - “True schema mismatches” — real type conflicts (e.g., `int` vs `str`)

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
  events[0].ts
  events[0].meta.key
  events[0].meta.value

-- Common fields (types agree) --
  id: int
  tags: [str]
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

- **Presence vs Type:** We do **not** bake nullability into types. Optionality is tracked in `required_paths` and only shown in the *Presence* section when types are otherwise equal.
- **Sampling:** Inference is by merging k sampled records:
  - If a field is present in some records but not others, its type becomes a union (`union(int|missing)` after presence normalization in comparisons).
  - If an array has mixed element types, the element becomes a union (`["union(int|str)"]` → normalized per rules).
- **Normalization:** Consistent, comparable trees regardless of source:
  - Empty → base type (`empty_array`→`array`, `empty_object`→`object`, `empty_string`→`str`)
  - Unions deduplicated/sorted, `"any"` removed when other specific types exist
  - Arrays stay `[elem_type]` if known, else `"array"`

---

## ⚠️ Limitations

- **Sampling-based inference:** Types are inferred from sampled records; noisy data can under/over-report unions or presence.
- **Union explosion:** If many distinct types appear (e.g., 20 unique shapes in 20 samples), unions can grow.
- **SQL dialects:** Postgres + BigQuery DDL covered; other dialects may need regex additions (PRs welcome).
- **Arrays/structs:** Arrays map to `[elem_type]`; BigQuery `STRUCT<...>` maps to `"object"` only (no deep parsing yet).
- **dbt support:** Requires `manifest.json` or `schema.yml`; we currently honor `not_null` tests for presence, not full assertion semantics.
- **Protobuf support:** Only `proto2`/`proto3` message/enum/map/oneof basics are supported. Complex options and extensions are ignored.

---

## Troubleshooting

- **Wrong kind detection for `.json`:** Use `--left/--right` to force `jsonschema` or `dbt-manifest`.
- **“Table not found” for SQL:** Provide `--*-table` when the DDL file defines multiple tables.
- **Presence noise from data:** Increase `-k/--samples` and consider `--infer-datetimes` if timestamps are formatted strings.
