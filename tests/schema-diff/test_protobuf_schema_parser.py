from schema_diff.protobuf_schema_parser import (
    list_protobuf_messages,
    schema_from_protobuf_file,
)
from pathlib import Path
import textwrap
import json


def test_proto_basic(tmp_path):
    proto = textwrap.dedent("""
      syntax = "proto3";
      package demo;

      message User {
        int64 id = 1;
        string name = 2;
        repeated string tags = 3;
        google.protobuf.Timestamp created_at = 4;
        Address addr = 5;
        map<string, int32> counts = 6;
      }

      message Address {
        string city = 1;
        string country = 2;
      }
    """)
    p = tmp_path / "demo.proto"
    p.write_text(proto, encoding="utf-8")

    tree, required, chosen = schema_from_protobuf_file(str(p))
    assert chosen == "demo.User"
    # types
    assert tree["id"] == "int"
    assert tree["name"] == "str"
    assert tree["tags"] == ["str"]
    assert tree["created_at"] == "timestamp"
    assert isinstance(tree["addr"], dict)
    assert tree["counts"] == "object"
    assert required == set()


def test_proto_required_proto2(tmp_path):
    proto = textwrap.dedent("""
      syntax = "proto2";
      message Person {
        required int32 id = 1;
        optional string name = 2;
        message P {
          required string code = 1;
        }
        required P p = 3;
      }
    """)
    p = tmp_path / "p.proto"
    p.write_text(proto, encoding="utf-8")
    tree, required, chosen = schema_from_protobuf_file(str(p))
    assert ".id" in {"." + r for r in required}  # sanity
    # exact required paths:
    assert required == {"id", "p", "p.code"}


PROTO = textwrap.dedent(
    r"""
    // top comments
    package foo.bar;

    import "google/protobuf/timestamp.proto"; // ignored by parser

    message Outer {
      required int32 id = 1;

      // Nested message
      message Inner {
        optional string name = 1;
      }

      // repeated message reference
      repeated Inner items = 2;

      // local enum
      enum Kind {
        KIND_UNSPECIFIED = 0;
        KIND_ONE = 1;
      }

      optional Kind kind = 3;

      // maps collapse to generic object
      map<string, int32> counts = 4;

      // fully-qualified absolute type reference
      message Child {
        required .foo.bar.Outer.Inner inner = 1;
      }

      // oneof group should create optional fields
      oneof pick_one {
        string a = 10;
        int32 b = 11;
      }
    }

    message Other {
      optional string x = 1;
    }
    """
)


def write_proto(tmp_path: Path, name="sample.proto", content=PROTO) -> Path:
    p = tmp_path / name
    p.write_text(content, encoding="utf-8")
    return p


def test_list_protobuf_messages_returns_fqns(tmp_path: Path):
    p = write_proto(tmp_path)

    msgs = list_protobuf_messages(str(p))

    # Order matters only loosely, but our simple walker will hit top-level
    # then nested in-order. Assert membership and a plausible order prefix.
    assert "foo.bar.Outer" in msgs
    assert "foo.bar.Outer.Inner" in msgs
    assert "foo.bar.Outer.Child" in msgs
    assert "foo.bar.Other" in msgs

    # Basic sanity: top-level first
    first_two = msgs[:2]
    assert first_two[0] == "foo.bar.Outer"
    # Second should be a nested of Outer
    assert first_two[1].startswith("foo.bar.Outer.")


def test_schema_for_outer_tree_and_requireds(tmp_path: Path):
    p = write_proto(tmp_path)

    tree, required, chosen = schema_from_protobuf_file(str(p), message="Outer")
    assert chosen == "foo.bar.Outer"

    # Types we expect:
    # id: int (required)
    # items: [ { name: str } ]
    # kind: str (enums → str)
    # counts: object (maps → object)
    # Child: { inner: { name: str } } (inner required)
    # a: str (from oneof)
    # b: int (from oneof)
    assert isinstance(tree, dict)
    assert tree["id"] == "int"
    assert isinstance(tree["items"], list) and isinstance(
        tree["items"][0], dict)
    assert tree["items"][0]["name"] == "str"
    assert tree["kind"] == "str"
    assert tree["counts"] == "object"
    assert isinstance(tree["Child"], dict)
    assert isinstance(tree["Child"]["inner"], dict)
    assert tree["Child"]["inner"]["name"] == "str"

    # oneof members present as optional
    assert tree["a"] == "str"
    assert tree["b"] == "int"

    # Required set: proto2 "required" only
    # - Outer.id (required)
    # - Outer.Child.inner (required)
    # - Inner.name is optional, not included
    assert "id" in required
    assert "Child.inner" in required
    assert "items" not in required
    assert "Child.inner.name" not in required


def test_choose_message_by_unique_suffix(tmp_path: Path):
    p = write_proto(tmp_path)

    # Ask for Outer.Inner (unique suffix)
    tree, required, chosen = schema_from_protobuf_file(
        str(p), message="Outer.Inner")
    assert chosen == "foo.bar.Outer.Inner"
    assert tree == {"name": "str"}  # from the nested message definition
    assert required == set()         # 'name' was optional


def test_absolute_leading_dot_resolution(tmp_path: Path):
    p = write_proto(tmp_path)

    # Build Outer.Child explicitly; ensure the absolute .foo.bar.Outer.Inner was resolved
    tree, required, chosen = schema_from_protobuf_file(
        str(p), message="foo.bar.Outer.Child")
    assert chosen == "foo.bar.Outer.Child"
    assert "inner" in tree and isinstance(tree["inner"], dict)
    assert tree["inner"]["name"] == "str"
    assert "inner" in required  # Child.inner marked required in the proto


def test_enums_and_maps_shapes(tmp_path: Path):
    p = write_proto(tmp_path)

    tree, _, _ = schema_from_protobuf_file(str(p), message="Outer")

    # enums → str; maps → object
    assert tree["kind"] == "str"
    assert tree["counts"] == "object"


def test_top_level_other_message(tmp_path: Path):
    p = write_proto(tmp_path)

    tree, required, chosen = schema_from_protobuf_file(str(p), message="Other")
    assert chosen == "foo.bar.Other"
    assert tree == {"x": "str"}
    assert required == set()


def test_dbt_manifest_array_angle(tmp_path):
    man = {"nodes": {"model.x.y": {"resource_type": "model",
                                   "name": "y", "columns": {"tags": {"data_type": "ARRAY<STRING>"}}}}}
    p = tmp_path / "m.json"
    p.write_text(json.dumps(man), encoding="utf-8")
    from schema_diff.dbt_schema_parser import schema_from_dbt_manifest
    tree, req = schema_from_dbt_manifest(str(p), model="y")
    assert tree["tags"] in (["str"], "array")
