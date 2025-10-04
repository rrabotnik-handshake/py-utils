"""
JSON Schema → internal type tree.

- Produces a *pure* type tree (no presence injected).
- Returns (tree, required_paths) where required_paths is a dotted-path set
  computed from JSON Schema "required" (plus nested, and across
  oneOf/anyOf/allOf via union of branches).

Conventions
-----------
Scalars : "int" | "float" | "bool" | "str" | "date" | "time" | "timestamp" | "missing" | "any"
Objects : { key: subtree }
Arrays  : [ element_subtree ]         (even when the element is a union/object)
Unions  : "union(a|b|...)"            (sorted, deduped; "any" dropped if others present)
Presence: NOT encoded in the tree (handled by compare layer via required_paths)
"""

from __future__ import annotations

import json
from typing import Any

from .io_utils import open_text
from .json_data_file_parser import merge_schema
from .utils import union_str  # single source of truth for union string building

__all__ = [
    "load_json_schema",
    "schema_from_json_schema_file",
    "schema_from_json_schema_file_unified",
]

# -------- Mapping of JSON Schema scalar types to internal scalars ----------
TYPE_MAP: dict[str, str] = {
    "string": "str",
    "integer": "int",
    "number": "float|int",
    "boolean": "bool",
    "object": "object",
    "array": "array",
    "null": "missing",
}


def _map_string_with_format(node: dict[str, Any]) -> str:
    """
    Map JSON Schema {type:"string", format:...} to a specific internal scalar
    when format is date/time-like; otherwise "str".
    """
    fmt = node.get("format", "")
    fmt = fmt.lower() if isinstance(fmt, str) else ""
    if fmt in ("date-time", "datetime", "timestamp"):
        return "timestamp"
    if fmt == "date":
        return "date"
    if fmt == "time":
        return "time"
    return "str"


def _literal_tname_for_enum(v: Any) -> str:
    """
    A minimal, config-free classifier for JSON-literal enum values.
    (Do NOT use the DATA-side infer.tname here; it requires Config.)
    """
    if v is None:
        return "missing"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int):
        return "int"
    if isinstance(v, float):
        return "float"
    if isinstance(v, str):
        # enums typically carry meaningful, non-empty strings; map empty to str anyway
        return "str"
    if isinstance(v, dict):
        # enums usually don’t carry empty objects, but fine
        return "object" if v else "object"
    if isinstance(v, list):
        return "array" if v else "array"
    # Fallback: treat unknown literals as "any"
    return "any"


def _schema_from_js(node: Any, *, _optional: bool) -> Any:
    """
    Build the internal *type tree* from a JSON Schema node.

    NOTE: This function purposely does NOT inject '|missing' for optional fields.
          Presence is tracked separately via `_collect_required_paths_json`.
    """
    if not isinstance(node, dict):
        return "any"

    # allOf / anyOf / oneOf -> union the branches
    if any(k in node for k in ("oneO", "anyO", "allOf")):
        branches: list[Any] = []
        for k in ("oneO", "anyO", "allOf"):
            if k in node and isinstance(node[k], list):
                for sub in node[k]:
                    branches.append(_schema_from_js(sub, _optional=False))
        if not branches:
            return "any"
        u: Any = branches[0]
        for b in branches[1:]:
            if isinstance(u, dict) and isinstance(b, dict):
                keys = set(u) | set(b)
                u = {
                    kk: merge_schema(u.get(kk, "missing"), b.get(kk, "missing"))
                    for kk in keys
                }
            else:
                ua = (
                    u
                    if isinstance(u, str)
                    else (
                        "object"
                        if isinstance(u, dict)
                        else "array" if isinstance(u, list) else "any"
                    )
                )
                ub = (
                    b
                    if isinstance(b, str)
                    else (
                        "object"
                        if isinstance(b, dict)
                        else "array" if isinstance(b, list) else "any"
                    )
                )
                u = union_str([ua, ub])
        return u

    # enum -> union of literal types
    if "enum" in node and isinstance(node["enum"], list) and node["enum"]:
        enum_types = []
        for v in node["enum"]:
            enum_types.append(_literal_tname_for_enum(v))
        return union_str(enum_types)

    jtype = node.get("type")

    # Handle type: [...] (multi-type)
    if isinstance(jtype, list):
        mapped = []
        for t in jtype:
            if t == "string":
                mapped.append(_map_string_with_format(node))
            else:
                mapped.append(TYPE_MAP.get(t, "any"))
        return union_str(mapped)

    # objects (with/without explicit "type": "object")
    if jtype == "object" or ("properties" in node):
        props = node.get("properties", {}) or {}
        out: dict[str, Any] = {}
        for k, v in props.items():
            # no presence injection here
            out[k] = _schema_from_js(v, _optional=False)
        return out if out else "object"

    # arrays (with/without explicit "type": "array")
    if jtype == "array" or ("items" in node):
        items = node.get("items")
        if isinstance(items, dict):
            elem = _schema_from_js(items, _optional=False)
            # ALWAYS represent arrays as a one-element list
            return [elem]
        # items absent or not a dict → generic array
        return "array"

    # scalar types
    if isinstance(jtype, str):
        if jtype == "string":
            return _map_string_with_format(node)
        return TYPE_MAP.get(jtype, "any")

    return "any"


# ----- required paths collection -----


def _collect_required_paths_json(node: Any, prefix: str = "") -> set[str]:
    """
    Collect dotted paths from JSON Schema "required" lists.

    Rules
    -----
    - At each object node, add its direct 'required' property names (as dotted paths).
    - Recurse into properties to collect nested requireds.
    - For allOf/oneOf/anyOf, we take the UNION of branch requirements.
    - Arrays: we do not expand item-level required paths (presence is for the field).
    """
    req: set[str] = set()
    if not isinstance(node, dict):
        return req

    # combinators: union of branch requirements
    for key in ("allO", "oneO", "anyOf"):
        if isinstance(node.get(key), list):
            for sub in node[key]:
                req |= _collect_required_paths_json(sub, prefix)

    # object properties
    props = node.get("properties")
    if isinstance(props, dict):
        # add direct required properties at this level
        for name in node.get("required", []) or []:
            if isinstance(name, str):
                req.add(f"{prefix}.{name}" if prefix else name)
        # recurse into children to gather nested requireds (e.g., user.id)
        for name, sub in props.items():
            child_prefix = f"{prefix}.{name}" if prefix else name
            req |= _collect_required_paths_json(sub, child_prefix)

    return req


def load_json_schema(path: str) -> Any:
    """Read and parse a JSON Schema file (supports .gz via open_text)."""
    with open_text(path) as f:
        return json.load(f)


def schema_from_json_schema_file(path: str) -> tuple[Any, set[str]]:
    """
    Parse a JSON Schema file.

    Returns
    -------
    (type_tree, required_paths)
      - type_tree: pure type tree (no '|missing' injected)
      - required_paths: dotted paths that are presence-required by the schema
    """
    js = load_json_schema(path)
    tree = _schema_from_js(js, _optional=False)  # pure types
    required = _collect_required_paths_json(js)  # presence set
    return tree, required


def schema_from_json_schema_file_unified(path: str):
    """
    Parse a JSON Schema file and return unified Schema object.

    Returns
    -------
    Schema
        Unified schema representation using Pydantic models
    """
    from .models import from_legacy_tree

    tree, required = schema_from_json_schema_file(path)
    return from_legacy_tree(tree, required, source_type="json_schema")
