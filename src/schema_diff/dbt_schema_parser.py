# schema_diff/dbt_schemaparser.py
from __future__ import annotations

import json
from typing import Any, Dict, Optional, Tuple, Set, List

from .io_utils import open_text

try:
    import yaml  # pyyaml
except Exception:  # pragma: no cover
    yaml = None

# Reuse a SQL-like mapping; dbt adapters emit database-native types.
TYPE_MAP_DBT: Dict[str, str] = {
    # integers
    "tinyint": "int", "smallint": "int", "int2": "int", "integer": "int", "int": "int",
    "int4": "int", "bigint": "int", "int8": "int", "serial": "int", "bigserial": "int",

    # floats / decimals
    "float": "float", "float4": "float", "float8": "float", "double": "float",
    "double precision": "float", "real": "float", "numeric": "float", "decimal": "float",

    # boolean
    "boolean": "bool", "bool": "bool",

    # strings / text / ids / json
    "varchar": "str", "character varying": "str",
    "char": "str", "character": "str", "text": "str", "citext": "str",
    "uuid": "str", "json": "str", "jsonb": "str", "bytea": "str",

    # date/time
    "date": "date",
    "time": "time", "time without time zone": "time", "time with time zone": "time",
    "timestamp": "timestamp", "timestamp without time zone": "timestamp",
    "timestamp with time zone": "timestamp", "timestamptz": "timestamp",
    "datetime": "timestamp",
}


def _normalize_dtype(s: str) -> str:
    s = " ".join(s.strip().lower().split())
    # strip (...) suffix (length/precision), keep array markers if any
    is_array = s.endswith("[]")
    if is_array:
        s = s[:-2].strip()
    base = s.split("(", 1)[0].strip()
    mapped = TYPE_MAP_DBT.get(base, TYPE_MAP_DBT.get(s, "any"))
    if is_array:
        # represent arrays as element list; normalizer will collapse ["any"] -> "array"
        # type: ignore[return-value]
        return ["any" if mapped == "any" else mapped]
    return mapped

# ---------------- Manifest.json (preferred: has types) --------------------


def schema_from_dbt_manifest(path: str, model: Optional[str] = None) -> Tuple[Dict[str, Any], Set[str]]:
    """
    Parse dbt target/manifest.json, pull column data_type for a given model.
    Returns (schema_tree, required_paths).
      - schema_tree uses pure types: 'int' | 'str' | ... or [elem]
      - required_paths are columns with not_null (from tests or constraints)
    'model' can be a model name ('my_model') or a fully qualified node name.
    """
    with open_text(path) as f:
        man = json.load(f)

    nodes: Dict[str, Any] = man.get("nodes", {})
    # If a "sources" section exists, ignore here (we're comparing models).
    matches: List[tuple[str, Any]] = []
    mlc = (model or "").lower()

    for node_id, node in nodes.items():
        if node.get("resource_type") != "model":
            continue
        name = node.get("name") or ""
        alias = node.get("alias") or ""
        fq = node_id  # e.g., model.project_name.my_model
        if not model:
            matches.append((fq, node))
        else:
            if mlc in {name.lower(), alias.lower(), fq.lower()}:
                matches.append((fq, node))

    if not matches:
        raise ValueError(f"No dbt model found matching '{model}' in {path}")

    # If multiple, prefer exact name match; else take the first
    fq_name, node = sorted(matches, key=lambda x: (x[0] != mlc, x[0]))[0]

    cols = node.get("columns", {}) or {}
    schema: Dict[str, Any] = {}
    required: Set[str] = set()

    for col_name, col_meta in cols.items():
        # Try adapter-reported data_type
        dtype = (col_meta.get("data_type")
                 or col_meta.get("type") or "").strip()
        mapped = _normalize_dtype(dtype) if dtype else "any"
        if isinstance(mapped, list):
            t: Any = ["any" if mapped[0] == "any" else mapped[0]]
        else:
            t = mapped
        schema[col_name] = t

        # Presence: honor 'not_null' tests or constraints
        tests = col_meta.get("tests") or []
        # tests can be list of strings or dicts; normalize to names
        test_names = []
        for tdef in tests:
            if isinstance(tdef, str):
                test_names.append(tdef)
            elif isinstance(tdef, dict):
                test_names.extend(tdef.keys())
        if any(tn.endswith(".not_null") or tn == "not_null" for tn in test_names):
            required.add(col_name)

        # Some adapters include constraints; also treat NOT NULL constraints
        for c in col_meta.get("constraints", []):
            if isinstance(c, dict) and c.get("type", "").lower() == "not_null":
                required.add(col_name)

    return schema, required

# ---------------- schema.yml (tests; usually no types) --------------------


def schema_from_dbt_schema_yml(path: str, model: Optional[str] = None) -> Tuple[Dict[str, Any], Set[str]]:
    """
    Parse a dbt schema.yml (v2). Uses tests for presence (not_null).
    Types are often not present, so columns default to "any" unless a custom
    'data_type' (or meta.type) is provided.
    Returns (schema_tree, required_paths).
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
        col = c.get("name")
        if not col:
            continue

        # Try to find a declared type in custom fields if present
        declared = (
            c.get("data_type")
            or (c.get("meta") or {}).get("type")
            or ""
        )
        mapped = _normalize_dtype(declared) if declared else "any"
        if isinstance(mapped, list):
            t: Any = ["any" if mapped[0] == "any" else mapped[0]]
        else:
            t = mapped
        schema[col] = t

        tests = c.get("tests") or []
        test_names = []
        for tdef in tests:
            if isinstance(tdef, str):
                test_names.append(tdef)
            elif isinstance(tdef, dict):
                test_names.extend(tdef.keys())
        if any(tn.endswith(".not_null") or tn == "not_null" for tn in test_names):
            required.add(col)

    return schema, required
