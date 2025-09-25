from schema_diff.spark_schema_parser import schema_from_spark_schema_file
from schema_diff.normalize import walk_normalize


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
    assert n["ts"] == "timestamp"  # pure type
    assert n["tags"] in (["str"], "array")
    assert required == {"id"}


def test_spark_nested_array_struct(tmp_path):
    spark = """root
 |-- id: long (nullable = false)
 |-- items: array<struct<qty:int,meta:struct<tag:string,ts:timestamp>>> (nullable = true)
"""
    p = tmp_path / "spark.txt"
    p.write_text(spark, encoding="utf-8")

    tree, required = schema_from_spark_schema_file(str(p))
    # items should be [object], with nested shape preserved inside object
    assert isinstance(tree["items"], list)
    elem = tree["items"][0]
    assert isinstance(elem, dict)
    assert elem["qty"] == "int"
    assert isinstance(elem["meta"], dict)
    assert elem["meta"]["tag"] == "str"
    assert elem["meta"]["ts"] == "timestamp"
    assert "id" in required and "items" not in required
