import json
from schema_diff.json_schema_parser import schema_from_json_schema_file
from schema_diff.normalize import walk_normalize


def test_json_schema_simple(tmp_path):
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "ts": {"type": "string", "format": "date-time"},
            "tags": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["id", "name"],
    }
    p = tmp_path / "schema.json"
    p.write_text(json.dumps(schema), encoding="utf-8")

    tree, required = schema_from_json_schema_file(str(p))
    n = walk_normalize(tree)

    # Pure types (no presence mixed in)
    assert n["id"] == "int"
    assert n["name"] == "str"

    # type-level only; presence is separate
    assert n["ts"] == "timestamp"
    assert n["tags"] in (["str"], "array")  # depending on normalization policy

    # Presence/requiredness is asserted via required set
    assert required == {"id", "name"}


def test_json_schema_presence(tmp_path):
    sch = {
        "type": "object",
        "required": ["id"],
        "properties": {
            "id": {"type": "integer"},
            # allows JSON null as a value (not absence)
            "ts": {"type": ["string", "null"], "format": "date-time"},
        },
    }
    p = tmp_path / "schema.json"
    p.write_text(json.dumps(sch), encoding="utf-8")

    tree, required = schema_from_json_schema_file(str(p))
    n = walk_normalize(tree)

    assert n["id"] == "int"

    # Because "null" is allowed at the value level, our type carries "missing" to represent null.
    # (Presence is still governed by `required`, and 'ts' is NOT required here.)
    assert "missing" in n["ts"] and "timestamp" in n["ts"]

    # Only 'id' is presence-required (since only it is in the "required" list)
    assert required == {"id"}


def test_nested_required(tmp_path):
    js = {
        "type": "object",
        "properties": {
            "user": {
                "type": "object",
                "required": ["id"],
                "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
            }
        },
    }
    p = tmp_path / "sch.json"
    p.write_text(json.dumps(js), encoding="utf-8")
    tree, required = schema_from_json_schema_file(str(p))
    n = walk_normalize(tree)
    assert n["user"]["id"] == "int"
    assert "user.id" in required and "user.name" not in required
