import json
import textwrap
import pytest

from schema_diff.dbt_schema_parser import (
    schema_from_dbt_manifest,
    schema_from_dbt_schema_yml,
)
from schema_diff.normalize import walk_normalize


def test_dbt_manifest_basic(tmp_path):
    """
    Manifest contains adapter-resolved data types and column-level tests.
    We expect pure types back (+ presence via not_null test).
    """
    manifest = {
        "nodes": {
            "model.myproj.my_model": {
                "resource_type": "model",
                "name": "my_model",
                "alias": "my_model",
                "columns": {
                    "id": {
                        "data_type": "BIGINT",
                        "tests": ["not_null"],  # mark presence
                    },
                    "tags": {
                        "data_type": "TEXT[]",  # array -> ["str"] or "array" after normalize
                    },
                    "ts": {
                        "data_type": "TIMESTAMP WITH TIME ZONE",  # -> "timestamp"
                    },
                    "note": {
                        # no data_type -> "any"
                    },
                },
            }
        }
    }
    p = tmp_path / "manifest.json"
    p.write_text(json.dumps(manifest), encoding="utf-8")

    tree, required = schema_from_dbt_manifest(str(p), model="my_model")
    n = walk_normalize(tree)

    # Types
    assert n["id"] == "int"
    assert n["ts"] == "timestamp"
    assert n["note"] == "any"
    assert n["tags"] in ("array", ["str"])

    # Presence (from not_null)
    assert required == {"id"}


def test_dbt_schema_yml_presence_and_optional_types(tmp_path):
    """
    schema.yml usually carries tests but not types.
    We still capture presence via 'not_null'.
    If a data_type is provided in YAML, we map it.
    """
    pytest.importorskip("yaml")  # skip if pyyaml isn't installed

    # Two columns: id has not_null and type; note has no type/tests.
    yml = textwrap.dedent(
        """
        version: 2

        models:
          - name: my_model
            columns:
              - name: id
                data_type: bigint
                tests:
                  - not_null
              - name: note
        """
    ).strip()
    p = tmp_path / "schema.yml"
    p.write_text(yml, encoding="utf-8")

    tree, required = schema_from_dbt_schema_yml(str(p), model="my_model")
    n = walk_normalize(tree)

    # Types
    assert n["id"] == "int"
    assert n["note"] == "any"  # no type declared → "any"

    # Presence
    assert required == {"id"}


def test_dbt_manifest_array_angle(tmp_path):
    man = {
        "nodes": {
            "model.x.y": {
                "resource_type": "model",
                "name": "y",
                "columns": {"tags": {"data_type": "ARRAY<STRING>"}},
            }
        }
    }
    p = tmp_path / "m.json"
    p.write_text(json.dumps(man), encoding="utf-8")
    from schema_diff.dbt_schema_parser import schema_from_dbt_manifest

    tree, req = schema_from_dbt_manifest(str(p), model="y")
    assert tree["tags"] in (["str"], "array")
