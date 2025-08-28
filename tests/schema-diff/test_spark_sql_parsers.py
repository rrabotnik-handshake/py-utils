# tests/test_spark_sql_parsers.py
from schema_diff.sql_schema_parser import schema_from_sql_schema_file
from schema_diff.normalize import walk_normalize
from schema_diff.spark_schema_parser import schema_from_spark_schema_file


def test_sql_schema_parser(tmp_path):
    text = """
CREATE TABLE public.people (
  id BIGINT NOT NULL,
  full_name TEXT NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE,
  tags TEXT[]
);
"""
    p = tmp_path / "schema.sql"
    p.write_text(text, encoding="utf-8")

    # Parse SQL → internal schema tree, then normalize unions/arrays/etc.
    schema_tree, required = schema_from_sql_schema_file(str(p), table="people")
    n = walk_normalize(schema_tree)

    # Types
    assert n["id"] == "int"                    # NOT NULL integer → "int"
    assert n["full_name"] == "str"             # NOT NULL text → "str"
    
    # nullable timestamp → union(timestamp|missing)
    assert n["created_at"] == "timestamp"      # type only, no presence in type
    assert required == {"id", "full_name"}     # NOT NULL columns only  
    # array element stays specific if known
    assert n["tags"] in ("array", ["str"])

    # Presence constraints are separate:
    assert required == {"id", "full_name"}

def test_sql_schema_parser_not_null_array(tmp_path):
    text = """
CREATE TABLE p (
  labels TEXT[] NOT NULL
);
"""
    p = tmp_path / "schema.sql"
    p.write_text(text, encoding="utf-8")

    schema_tree, required = schema_from_sql_schema_file(str(p), table="p")
    n = walk_normalize(schema_tree)

    # Non-nullable array column keeps element specificity after normalize
    # (TEXT → "str"), so overall becomes ["str"].
    assert n["labels"] == ["str"]
    assert required == {"labels"}
    

def test_sql_int_aliases_map():
    from schema_diff.sql_schema_parser import _sql_dtype_to_internal
    for t in ["BIGINT", "bigint", "INT8", "INTEGER", "int", "int4"]:
        assert _sql_dtype_to_internal(t) == "int"
        

def test_spark_parser_presence(tmp_path):
    spark = """root
 |-- id: long (nullable = false)
 |-- ts: timestamp (nullable = true)
 |-- tags: array<string> (nullable = true)
"""
    p = tmp_path / "spark.txt"
    p.write_text(spark, encoding="utf-8")
    tree, required = schema_from_spark_schema_file(str(p))
    n = walk_normalize(tree)
    assert n["id"] == "int"
    assert n["ts"] == "timestamp"              # pure type
    assert n["tags"] in (["str"], "array")
    assert required == {"id"}


def test_bq_simple_types(tmp_path):
    sql = """\
CREATE TABLE `myproj.myds.people` (
  id INT64 NOT NULL,
  full_name STRING NOT NULL,
  created_at TIMESTAMP,
  payload JSON,
  shape GEOGRAPHY,
  sz BYTES
);
"""
    p = tmp_path / "bq.sql"
    p.write_text(sql, encoding="utf-8")

    tree, required = schema_from_sql_schema_file(str(p), table="people")
    n = walk_normalize(tree)

    assert n["id"] == "int"
    assert n["full_name"] == "str"
    # created_at is nullable -> presence is separate; type stays 'timestamp'
    assert n["created_at"] == "timestamp"
    assert n["payload"] == "str"
    assert n["shape"] == "str"
    assert n["sz"] == "str"
    assert required == {"id", "full_name"}


def test_bq_array_and_struct(tmp_path):
    sql = """\
CREATE TABLE myds.events (
  tags ARRAY<STRING> NOT NULL,
  metrics ARRAY<NUMERIC>,
  meta STRUCT<created DATETIME, tz STRING>,
  nested ARRAY<STRUCT<a INT64, b STRING>>  -- we treat struct as object
);
"""
    p = tmp_path / "bq2.sql"
    p.write_text(sql, encoding="utf-8")

    tree, required = schema_from_sql_schema_file(str(p), table="events")
    n = walk_normalize(tree)

    # NOT NULL ARRAY<STRING> → element type preserved as ["str"] after normalization
    assert n["tags"] in (["str"], "array")
    # nullable ARRAY<NUMERIC>
    assert n["metrics"] in (["float"], "array")
    # STRUCT -> object
    assert n["meta"] == "object"
    # ARRAY<STRUCT<...>> -> ["object"] or "array"
    assert n["nested"] in (["object"], "array")
    # presence: only 'tags' is NOT NULL
    assert required == {"tags"}


def test_bq_options_ignored(tmp_path):
    sql = """\
CREATE TABLE myds.files (
  name STRING NOT NULL OPTIONS(description="file name"),
  data BYTES OPTIONS(description="raw bytes")
);
"""
    p = tmp_path / "bq3.sql"
    p.write_text(sql, encoding="utf-8")

    tree, required = schema_from_sql_schema_file(str(p), table="files")
    n = walk_normalize(tree)

    assert n["name"] == "str"
    assert n["data"] == "str"
    assert required == {"name"}
# presence only
