"""
dbt schema parsers.

This module exposes two entry points:

- `schema_from_dbt_manifest(path, model=None) -> (schema_tree, required_paths)`
    Parses a dbt `target/manifest.json` and extracts per-column *types* from
    `data_type` (or `type`) and *presence* from not-null tests/constraints.

- `schema_from_dbt_schema_yml(path, model=None) -> (schema_tree, required_paths)`
    Parses a dbt `schema.yml` (v2). Usually has *tests* but not concrete types,
    so types default to "any" unless `data_type` or `meta.type` is present.

Both return:
    schema_tree: Dict[str, Any] with *pure types* e.g. 'int'|'str'|... or ['str'] for arrays
    required_paths: Set[str] of columns that are presence-required (not null)
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional, Tuple, Set, List

import yaml
import re

from .io_utils import open_text


# Adapter-agnostic normalization of dbt/emitted data types to internal labels.
# Arrays are represented as [elem_type]; normalizer can later collapse ["any"] -> "array".
TYPE_MAP_DBT: Dict[str, str] = {
    # integers
    "tinyint": "int", "smallint": "int", "int2": "int", "integer": "int", "int": "int",
    "int4": "int", "bigint": "int", "int8": "int", "serial": "int", "bigserial": "int",

    # floats / decimals
    "float": "float", "float4": "float", "float8": "float",
    "double": "float", "double precision": "float",
    "real": "float", "numeric": "float", "decimal": "float",
    "bignumeric": "float",

    # boolean
    "boolean": "bool", "bool": "bool",

    # strings / text / ids / json
    "varchar": "str", "character varying": "str",
    "char": "str", "character": "str", "text": "str", "citext": "str",
    "uuid": "str", "json": "str", "jsonb": "str", "bytea": "str",
    "string": "str",
    "bytes": "str",

    # date/time
    "date": "date",
    "time": "time", "time without time zone": "time", "time with time zone": "time",
    "timestamp": "timestamp", "timestamp without time zone": "timestamp",
    "timestamp with time zone": "timestamp", "timestamptz": "timestamp",
    "datetime": "timestamp",
}

ARRAY_ANGLE_RE = re.compile(r"^array\s*<\s*([^>]+)\s*>$")


def _normalize_dtype(dtype: str) -> str | list[str]:
    """
    Map a dbt/adapter dtype string to internal type label.

    Returns:
      - scalar: 'int'|'float'|'bool'|'str'|'date'|'time'|'timestamp'|'any'
      - array:  [elem_type]  (e.g., ['str'])
    """
    s = " ".join((dtype or "").strip().lower().split())

    # 1) Postgres-like: text[] / varchar(255)[]
    is_array = s.endswith("[]")
    if is_array:
        s = s[:-2].strip()

    # 2) BigQuery/Snowflake-like: array<string> / array<varchar(255)>
    m = ARRAY_ANGLE_RE.match(s)
    if m:
        inner = m.group(1).strip()
        # strip inner precision too
        inner_base = inner.split("(", 1)[0].strip()
        mapped_inner = TYPE_MAP_DBT.get(
            inner_base, TYPE_MAP_DBT.get(inner, "any"))
        return ["any" if mapped_inner == "any" else mapped_inner]

    base = s.split("(", 1)[0].strip()
    mapped = TYPE_MAP_DBT.get(base, TYPE_MAP_DBT.get(s, "any"))
    return ["any" if mapped == "any" else mapped] if is_array else mapped


def _iter_test_names(tests_field: Any) -> List[str]:
    """
    Normalize dbt 'tests' lists that may contain strings or dicts.

    Examples:
      ["not_null", {"unique": {...}}, "accepted_values"]
      → ["not_null", "unique", "accepted_values"]
    """
    out: List[str] = []
    if not isinstance(tests_field, list):
        return out
    for tdef in tests_field:
        if isinstance(tdef, str):
            out.append(tdef)
        elif isinstance(tdef, dict):
            out.extend(tdef.keys())
    return out


# ---------------- Manifest.json (preferred: has types) --------------------


def schema_from_dbt_manifest(path: str, model: Optional[str] = None) -> Tuple[Dict[str, Any], Set[str]]:
    """
    Parse dbt target/manifest.json, pull column `data_type` for the target model.

    Args:
      path: path to manifest.json.
      model: model selector; can be simple name ('my_model'), alias, or full node id
             (e.g. 'model.project_name.my_model'). If None, picks the first model.

    Returns:
      (schema_tree, required_paths)
    """
    with open_text(path) as f:
        man = json.load(f)

    nodes: Dict[str, Any] = man.get("nodes", {}) or {}
    matches: List[tuple[str, Any]] = []
    mlc = (model or "").lower()

    for node_id, node in nodes.items():
        if node.get("resource_type") != "model":
            continue
        name = (node.get("name") or "").lower()
        alias = (node.get("alias") or "").lower()
        fq = node_id.lower()  # e.g., model.project_name.my_model
        if not model:
            matches.append((fq, node))
        else:
            if mlc in {name, alias, fq}:
                matches.append((fq, node))

    if not matches:
        raise ValueError(f"No dbt model found matching '{model}' in {path}")

    # Prefer exact name/alias/node-id match if provided; else take the first sorted
    _, node = sorted(matches, key=lambda x: (x[0] != mlc, x[0]))[0]

    cols = node.get("columns", {}) or {}
    schema: Dict[str, Any] = {}
    required: Set[str] = set()

    for col_name, col_meta in cols.items():
        # Prefer adapter-reported data_type; fallback to 'type' if present
        dtype = (col_meta.get("data_type")
                 or col_meta.get("type") or "").strip()
        mapped = _normalize_dtype(dtype) if dtype else "any"
        schema[col_name] = mapped

        # Presence via tests/constraints
        tests = _iter_test_names(col_meta.get("tests") or [])
        if any(tn.endswith(".not_null") or tn == "not_null" for tn in tests):
            required.add(col_name)

        for c in col_meta.get("constraints", []) or []:
            if isinstance(c, dict) and (c.get("type", "") or "").lower() == "not_null":
                required.add(col_name)

    return schema, required


# ---------------- schema.yml (tests; usually no types) --------------------


def schema_from_dbt_schema_yml(path: str, model: Optional[str] = None) -> Tuple[Dict[str, Any], Set[str]]:
    """
    Parse a dbt schema.yml (v2). Uses tests for presence (not_null).
    Types are often not present, so columns default to "any" unless a custom
    'data_type' (or meta.type) is provided.

    Args:
      path: path to schema.yml
      model: model selector by 'name' or 'alias'. If None, picks the first model.

    Returns:
      (schema_tree, required_paths)
    """
    if yaml is None:
        raise RuntimeError(
            "pyyaml is required to parse dbt schema.yml. Install 'pyyaml'.")

    with open_text(path) as f:
        data = yaml.safe_load(f) or {}

    models = data.get("models") or []
    matches: List[Dict[str, Any]] = []
    mlc = (model or "").lower()

    for m in models:
        name = (m.get("name") or "").lower()
        alias = (m.get("alias") or "").lower()
        if not model or mlc in {name, alias}:
            matches.append(m)

    if not matches:
        raise ValueError(f"No dbt model found matching '{model}' in {path}")

    m = matches[0]
    cols = m.get("columns") or []
    schema: Dict[str, Any] = {}
    required: Set[str] = set()

    for c in cols:
        col_name = c.get("name")
        if not col_name:
            continue

        # Optional declared type in schema.yml (non-standard; seen in some projects)
        declared = (c.get("data_type") or (
            c.get("meta") or {}).get("type") or "").strip()
        mapped = _normalize_dtype(declared) if declared else "any"
        schema[col_name] = mapped

        tests = _iter_test_names(c.get("tests") or [])
        if any(tn.endswith(".not_null") or tn == "not_null" for tn in tests):
            required.add(col_name)

    return schema, required
