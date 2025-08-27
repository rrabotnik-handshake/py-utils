# schema-diff

Compare schemas across **JSON/NDJSON data**, **JSON Schema**, **Spark/Databricks schemas**, **SQL DDL**, and **dbt models**.  
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
schema-diff data.ndjson schema.json --right-kind jsonschema --first-record

# Data vs SQL (choose table)
schema-diff data.json schema.sql --right-kind sql --right-sql-table my_table --first-record

# JSON Schema vs SQL (multi-table file)
schema-diff schema.json db.sql --left-kind jsonschema --right-kind sql --right-sql-table users

# Spark schema vs data
schema-diff data.json spark_schema.txt --left-kind data --right-kind spark
```

### Supported inputs
- **Data:** `.json`, `.ndjson`, `.jsonl`, and their `.gz` variants
- **JSON Schema:** `.json` (draft formats supported for `type`, `format`, `properties`, `required`, `oneOf/anyOf/allOf`, `enum`)
- **Spark:** output of `df.printSchema()` (or similar text)
- **SQL:** Postgres-like DDL and **BigQuery DDL** (incl. `ARRAY<...>`, `STRUCT<...>`, backticked names)
- **dbt:** `manifest.json` or `schema.yml`

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

- `--left-kind/--right-kind`: one of
  - `data` | `jsonschema` | `spark` | `sql` | `dbt-manifest` | `dbt-yml` | `auto` (default)

Extra selectors:
- `--right-sql-table NAME` — choose table from SQL DDL when file has multiple tables
- `--right-dbt-model NAME` (or `--left-dbt-model`) — choose dbt model when needed

### Output control
- `--no-color`, `--force-color`
- `--no-presence` — hide the presence/optionality section
- `--json-out PATH` — save diff JSON
- `--dump-schemas PATH` — save the two normalized schemas to a file

### Timestamp inference (for data)
- `--infer-datetimes` — treat ISO-like strings as `timestamp`/`date`/`time` when inferring from data

---

## How comparisons work

1. **Each side is loaded to a _type tree_** (pure types: `int|float|bool|str|date|time|timestamp|object|array`).
   - **Data** → inferred by sampling; fields seen absent are *not* treated as missing here.
   - **JSON Schema** → converted from schema (`format: date-time` → `timestamp`, etc.).
   - **Spark** → parsed & mapped to internal types.
   - **SQL** → parsed & mapped to internal types; arrays become `[elem_type]`, `STRUCT<...>` → `object`.
   - **dbt** → built from manifest/schema.yml (columns + tests).

2. **Presence constraints** are collected separately as a set of **`required_paths`**:
   - JSON Schema → `required` arrays
   - SQL → `NOT NULL` columns
   - dbt → `not_null` tests
   - Data → *(no presence info)*

3. **Normalization** makes both sides comparable:
   - Collapses empties like `empty_array → array`, `empty_string → str`
   - Flattens and sorts unions (`union(str|missing)`), deduplicates `"any"`
   - Arrays remain `[elem_type]` if known, otherwise `"array"`

4. **DeepDiff** compares the two normalized type trees.
   - “Only in left/right” — keys present on one side only
   - “Missing / optional (presence)” — _only_ when the type is the same but optionality differs
   - “True schema mismatches” — real type conflicts (e.g., `int` vs `str`)

---

## Any↔Any mode

You can mix any two inputs:

```bash
# Data vs SQL
schema-diff data.ndjson schema.sql --left-kind data --right-kind sql --right-sql-table users

# JSON Schema vs Spark
schema-diff schema.json spark.txt --left-kind jsonschema --right-kind spark

# dbt model vs SQL
schema-diff manifest.json warehouse.sql \
  --left-kind dbt-manifest --left-dbt-model analytics.users \
  --right-kind sql --right-sql-table users
```

KIND auto-detection by extension:
- `.ndjson`, `.jsonl`, `.json(.gz)` → `data`
- `.sql` → `sql`
- `.yml` / `.yaml` → `dbt-yml`
- `.json` may also be `jsonschema` or `dbt-manifest` — use explicit `--*-kind` to be precise.

---

## Examples

```bash
# Compare records (sample 5 from each), with samples shown:
schema-diff a.ndjson.gz b.ndjson.gz -k 5 --seed 42 --show-samples

# Compare to JSON Schema
schema-diff data.ndjson schema.json --right-kind jsonschema --first-record

# Compare to SQL (table chosen)
schema-diff users.ndjson model.sql --right-kind sql --right-sql-table users --first-record

# Compare two external schemas (no data involved)
schema-diff schema.json spark.txt --left-kind jsonschema --right-kind spark
```

---

## JSON output

```bash
schema-diff data.json schema.sql --right-kind sql --right-sql-table p --json-out diff.json
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
- **Parsers:** SQL (Postgres & BigQuery), Spark, JSON Schema
- **Data I/O:** NDJSON/JSON/arrays, gz handling
- **Integration:** CLI flows (`data↔data`, `data↔jsonschema`, `jsonschema↔sql`, etc.)
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

## 🛠 Internals

- **`schema_from_data`** – infers schema by merging *k* sample records (reservoir sampling, optional ISO datetime inference).
- **`normalize`** – converts types into a comparable normalized tree (collapses empties, tames unions, handles arrays).
- **`compare`** – computes diffs via `DeepDiff` across normalized trees, separating presence-only vs real type mismatches.
- **`report`** – prints human-readable diffs (Only in left/right, Presence, True schema mismatches) and supports color/JSON export.
- **`loader`** – resolves file type → `(type_tree, required_paths, label)` by delegating to the appropriate parser/inference path.
- **`required_paths`**
  - **JSON Schema:** from `"required"` arrays (recursively for nested objects).
  - **SQL:** from `NOT NULL` columns.
  - **dbt:** from `not_null` tests in manifest/schema.yml.
  - **Data:** *no required info* (presence is inferred only by sampling, not enforced).

---

## ⚠️ Limitations

- **Sampling-based inference:** Types are inferred from sampled records; noisy data can under/over-report unions or presence.
- **Union explosion:** If many distinct types appear (e.g., 20 unique shapes in 20 samples), unions can grow.
- **SQL dialects:** Postgres + BigQuery DDL covered; other dialects may need regex additions (PRs welcome).
- **Arrays/structs:** Arrays map to `[elem_type]`; BigQuery `STRUCT<...>` currently maps to `"object"` (no deep field parsing yet).
- **dbt support:** Requires `manifest.json` or `schema.yml`; we currently honor `not_null` tests for presence, not full assertion semantics.

---

## Troubleshooting

- **Wrong kind detection for `.json`:** Use `--left-kind/--right-kind` to force `jsonschema` or `dbt-manifest`.
- **“Table not found” for SQL:** Provide `--*-sql-table` when the DDL file defines multiple tables.
- **Presence noise from data:** Increase `-k/--samples` and consider `--infer-datetimes` if timestamps are formatted strings.
