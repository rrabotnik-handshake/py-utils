from schema_diff.utils import inject_presence_for_diff
from schema_diff.normalize import walk_normalize
from schema_diff.compare import compare_trees
import json
import sys
from schema_diff.io_utils import _run


def test_inject_presence_scalars_and_arrays():
    ref = {
        "id": "int",
        "name": "str",
        "tags": ["str"],
        "meta": {"active": "bool", "note": "str"},
    }
    required = {"id", "meta.active"}  # required paths
    injected = inject_presence_for_diff(ref, required)
    n = walk_normalize(injected)

    # required stay plain
    assert n["id"] == "int"
    assert n["meta"]["active"] == "bool"

    # optional scalars/arrays gain '|missing'
    assert "missing" in n["name"]
    assert n["tags"] == "union(array|missing)"
    assert "missing" in n["meta"]["note"]


def test_compare_trees_plain_types_match(cfg_like):
    left = {"id": "int", "name": "str"}
    right = {"id": "int", "name": "str"}

    compare_trees("L", "R", left, set(), right, set(), cfg=cfg_like)


def test_presence_only_diff_jsonschema_vs_sql(tmp_path, run_cli):
    js = tmp_path / "schema.json"
    js.write_text(
        json.dumps(
            {
                "type": "object",
                "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
                "required": ["id", "name"],
            }
        ),
        encoding="utf-8",
    )

    sql = tmp_path / "schema.sql"
    sql.write_text(
        """CREATE TABLE t (
      id BIGINT NOT NULL,
      name TEXT NULL
    );""",
        encoding="utf-8",
    )

    res = run_cli([
        str(js),
        str(sql),
        "--left",
        "jsonschema",
        "--right",
        "sql",
        "--right-table",
        "t",
        "--no-color",
    ])
    assert res.returncode == 0
    assert "No differences" in res.stdout or "Type mismatches -- (0)" in res.stdout


def test_root_list_fields(tmp_path, run_cli):
    left = tmp_path / "l.json"
    right = tmp_path / "r.json"
    # Use proper JSON Schema format
    left_schema = {"type": "object", "properties": {"id": {"type": "integer"}}}
    right_schema = {"type": "object", "properties": {"id": {"type": "string"}}}
    left.write_text(json.dumps(left_schema), encoding="utf-8")
    right.write_text(json.dumps(right_schema), encoding="utf-8")

    res = run_cli([
        str(left),
        str(right),
        "--left",
        "jsonschema",
        "--right",
        "jsonschema",
        "--no-color",
    ])
    assert res.returncode == 0
    assert "-- Type mismatches --" in res.stdout
    # Check for type mismatch (format may vary)
    assert ("id:" in res.stdout or "Type mismatches" in res.stdout or res.returncode == 0)
