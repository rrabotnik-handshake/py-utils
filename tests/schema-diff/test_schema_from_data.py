# tests/test_schema_from_data.py
from schema_diff.schema_from_data import to_schema, merged_schema_from_samples
from schema_diff.config import Config

CFG = Config(infer_datetimes=False, color_enabled=False, show_presence=True)


def test_to_schema_primitives():
    assert to_schema(1, CFG) == "int"
    assert to_schema(1.5, CFG) == "float"
    assert to_schema(True, CFG) == "bool"
    assert to_schema(None, CFG) == "missing"
    assert to_schema("", CFG) == "empty_string"
    assert to_schema("x", CFG) == "str"


def test_to_schema_containers():
    assert to_schema([], CFG) == "empty_array"
    assert to_schema([1], CFG) == ["int"]
    assert to_schema({}, CFG) == "empty_object"
    assert to_schema({"a": 1}, CFG) == {"a": "int"}


def test_merged_schema_basic():
    recs = [{"a": 1}, {"a": "x"}, {"b": True}]
    sch = merged_schema_from_samples(recs, CFG)
    assert isinstance(sch, dict)
    assert "a" in sch and "b" in sch
