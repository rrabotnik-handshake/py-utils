#!/usr/bin/env python3
"""Benchmark and compare current SQL parser vs SQLGlot parser.

This script compares:
1. Parsing accuracy (do both produce the same schema?)
2. Performance (which is faster?)
3. Error handling (which gives better error messages?)
4. Feature support (which handles more SQL constructs?)
"""
import time
from pathlib import Path

print("=" * 80)
print("SQL PARSER COMPARISON: Current vs SQLGlot")
print("=" * 80)
print()

# Check if SQLGlot is available
try:
    from src.schema_diff.sqlglot_parser import _HAS_SQLGLOT, schema_from_sql_ddl_sqlglot

    if not _HAS_SQLGLOT:
        print("❌ SQLGlot not installed.")
        print("Install with: pip install 'sqlglot>=25.0.0'")
        print()
        print("To install schema-diff with SQLGlot support:")
        print("  pip install -e '.[sqlglot]'")
        exit(1)

    print("✅ SQLGlot is installed")
except ImportError as e:
    print(f"❌ Failed to import sqlglot_parser: {e}")
    exit(1)

# Import current SQL parser
try:
    from src.schema_diff.sql_schema_parser import schema_from_sql_schema_file

    print("✅ Current SQL parser imported")
except ImportError as e:
    print(f"❌ Failed to import current SQL parser: {e}")
    exit(1)

print()

# Test SQL schemas
TEST_SCHEMAS = {
    "simple_bigquery": """
CREATE TABLE users (
  id INT64 NOT NULL,
  name STRING,
  email STRING NOT NULL,
  created_at TIMESTAMP
);
""",
    "complex_bigquery": """
CREATE TABLE orders (
  order_id INT64 NOT NULL,
  user_id INT64 NOT NULL,
  items ARRAY<STRUCT<
    product_id INT64,
    quantity INT64,
    price NUMERIC
  >>,
  metadata STRUCT<
    source STRING,
    campaign_id STRING
  >,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP
);
""",
    "postgres": """
CREATE TABLE products (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  price DECIMAL(10,2),
  in_stock BOOLEAN DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL
);
""",
}


def benchmark_parser(name, parse_func, sql_content, **kwargs):
    """Benchmark a parser function."""
    print(f"\n{'='*80}")
    print(f"Testing: {name}")
    print(f"{'='*80}")

    try:
        start = time.time()
        schema, required = parse_func(sql_content, **kwargs)
        elapsed = time.time() - start

        print(f"✅ Success in {elapsed*1000:.2f}ms")
        print(f"\nSchema fields: {len(schema)}")
        print(f"Required fields: {len(required)}")

        print("\nParsed Schema:")
        for field, ftype in sorted(schema.items()):
            req_marker = " (NOT NULL)" if field in required else ""
            print(f"  {field}: {ftype}{req_marker}")

        return {
            "success": True,
            "schema": schema,
            "required": required,
            "time_ms": elapsed * 1000,
            "error": None,
        }

    except Exception as e:
        print(f"❌ Failed: {e}")
        return {
            "success": False,
            "schema": {},
            "required": set(),
            "time_ms": 0,
            "error": str(e),
        }


def compare_schemas(schema1, schema2, name1="Schema 1", name2="Schema 2"):
    """Compare two parsed schemas."""
    print(f"\n{'='*80}")
    print(f"Comparison: {name1} vs {name2}")
    print(f"{'='*80}")

    fields1 = set(schema1.keys())
    fields2 = set(schema2.keys())

    only_in_1 = fields1 - fields2
    only_in_2 = fields2 - fields1
    common = fields1 & fields2

    if only_in_1:
        print(f"\n🔴 Only in {name1}: {only_in_1}")

    if only_in_2:
        print(f"\n🔴 Only in {name2}: {only_in_2}")

    if common:
        print(f"\n✅ Common fields: {len(common)}")

        # Check for type differences
        type_diffs = []
        for field in common:
            type1 = str(schema1[field])
            type2 = str(schema2[field])
            if type1 != type2:
                type_diffs.append((field, type1, type2))

        if type_diffs:
            print(f"\n⚠️  Type differences:")
            for field, type1, type2 in type_diffs:
                print(f"  {field}: {type1} vs {type2}")
        else:
            print("  All types match! ✅")

    if not only_in_1 and not only_in_2 and not type_diffs:
        print("\n🎉 Schemas are identical!")
        return True
    else:
        print("\n⚠️  Schemas differ")
        return False


# Run benchmarks
print("\n" + "=" * 80)
print("BENCHMARKING SQL PARSERS")
print("=" * 80)

for test_name, sql_content in TEST_SCHEMAS.items():
    print(f"\n\n{'#'*80}")
    print(f"# Test Case: {test_name}")
    print(f"{'#'*80}")

    # Write SQL to temp file for current parser
    temp_sql_file = f"/tmp/test_{test_name}.sql"
    Path(temp_sql_file).write_text(sql_content)

    # Test current parser
    current_result = benchmark_parser(
        "Current Parser",
        lambda path: schema_from_sql_schema_file(path),
        temp_sql_file,
    )

    # Test SQLGlot parser
    dialect = "postgres" if test_name == "postgres" else "bigquery"
    sqlglot_result = benchmark_parser(
        "SQLGlot Parser",
        lambda path: schema_from_sql_ddl_sqlglot(path, dialect=dialect),
        temp_sql_file,
    )

    # Compare results
    if current_result["success"] and sqlglot_result["success"]:
        compare_schemas(
            current_result["schema"],
            sqlglot_result["schema"],
            "Current",
            "SQLGlot",
        )

        # Performance comparison
        print(f"\nPerformance:")
        print(f"  Current: {current_result['time_ms']:.2f}ms")
        print(f"  SQLGlot: {sqlglot_result['time_ms']:.2f}ms")

        if sqlglot_result["time_ms"] < current_result["time_ms"]:
            speedup = current_result["time_ms"] / sqlglot_result["time_ms"]
            print(f"  SQLGlot is {speedup:.1f}x faster ⚡")
        else:
            slowdown = sqlglot_result["time_ms"] / current_result["time_ms"]
            print(f"  SQLGlot is {slowdown:.1f}x slower 🐌")

    # Cleanup
    Path(temp_sql_file).unlink(missing_ok=True)


print("\n\n" + "=" * 80)
print("SUMMARY & RECOMMENDATIONS")
print("=" * 80)
print(
    """
Based on this benchmark:

1. **Accuracy**: Compare schema outputs above
2. **Performance**: See timing comparison for each test
3. **Features**: SQLGlot supports 20+ SQL dialects
4. **Error Messages**: Compare error outputs when tests fail

Next Steps:
- If schemas match and performance is good → Use SQLGlot as default
- If current parser is better for specific cases → Keep as fallback
- Consider hybrid approach: Try SQLGlot first, fall back to current

To use SQLGlot parser in schema-diff:
  schema-diff compare schema1.sql schema2.sql --parser sqlglot
"""
)

print("=" * 80)
