from __future__ import annotations

import json
from typing import Any, Dict, List, Set, Tuple

from .io_utils import open_text
from .json_data_file_parser import merge_schema
from .infer import tname

# -------- JSON Schema -> internal tree --------
TYPE_MAP = {
    "string": "str",
    "integer": "int",
    "number": "float|int",
    "boolean": "bool",
    "object": "object",
    "array": "array",
    "null": "missing",
}


def _union_str(types: List[str]) -> str:
    atoms: List[str] = []
    for t in types:
        atoms.extend(t.split("|"))
    atoms = sorted(set(atoms))
    if "any" in atoms and len(atoms) > 1:
        atoms.remove("any")
    return atoms[0] if len(atoms) == 1 else "union(" + "|".join(atoms) + ")"


def _map_string_with_format(node: Dict[str, Any]) -> str:
    fmt = node.get("format", "")
    fmt = fmt.lower() if isinstance(fmt, str) else ""
    if fmt in ("date-time", "datetime", "timestamp"):
        return "timestamp"
    if fmt == "date":
        return "date"
    if fmt == "time":
        return "time"
    return "str"


def _schema_from_js(node: Any, *, optional: bool) -> Any:
    """
    Build the internal *type tree* from a JSON Schema node.
    NOTE: This function does NOT inject 'missing' for optional fields.
          Presence is tracked separately via required_paths.
    """
    if not isinstance(node, dict):
        return "any"

    # allOf / anyOf / oneOf -> union the branches
    if any(k in node for k in ("oneOf", "anyOf", "allOf")):
        branches = []
        for k in ("oneOf", "anyOf", "allOf"):
            if k in node:
                for sub in node[k]:
                    branches.append(_schema_from_js(sub, optional=False))
        u = branches[0]
        for b in branches[1:]:
            if isinstance(u, dict) and isinstance(b, dict):
                keys = set(u) | set(b)
                u = {kk: merge_schema(u.get(kk, "missing"),
                                      b.get(kk, "missing")) for kk in keys}
            else:
                ua = u if isinstance(u, str) else tname(u)
                ub = b if isinstance(b, str) else tname(b)
                u = _union_str([ua, ub])
        return u

    # enum -> union of literal types
    if "enum" in node and isinstance(node["enum"], list) and node["enum"]:
        enum_types = []
        for v in node["enum"]:
            enum_types.append(
                tname(v)
                .replace("empty_string", "str")
                .replace("empty_object", "object")
                .replace("empty_array", "array")
            )
        return _union_str(enum_types)

    jtype = node.get("type")

    if isinstance(jtype, list):
        mapped = []
        for t in jtype:
            if t == "string":
                mapped.append(_map_string_with_format(node))
            else:
                mapped.append(TYPE_MAP.get(t, "any"))
        return _union_str(mapped)

    if jtype == "object" or ("properties" in node):
        props = node.get("properties", {}) or {}
        out: Dict[str, Any] = {}
        for k, v in props.items():
            # no presence injection here
            out[k] = _schema_from_js(v, optional=False)
        return out if out else "object"

    if jtype == "array" or ("items" in node):
        items = node.get("items")
        if isinstance(items, dict):
            elem = _schema_from_js(items, optional=False)
            # ALWAYS return a list for arrays
            return [elem]
        return "array"

    if isinstance(jtype, str):
        if jtype == "string":
            return _map_string_with_format(node)
        return TYPE_MAP.get(jtype, "any")

    return "any"


# ----- required paths collection -----

def _collect_required_paths(js: Any) -> Set[str]:
    """
    Walk a JSON Schema and collect dotted paths that are presence-required.
    """
    out: Set[str] = set()

    def walk(node: Any, prefix: str) -> None:
        if not isinstance(node, dict):
            return

        props = node.get("properties")
        if isinstance(props, dict):
            req_list = node.get("required") or []
            if isinstance(req_list, list):
                for name in req_list:
                    if isinstance(name, str):
                        out.add(
                            f"{prefix}{name}" if not prefix else f"{prefix}.{name}")

            # Recurse into children regardless; nested objects may have their own 'required'
            for name, subschema in props.items():
                child_prefix = f"{prefix}{name}" if not prefix else f"{prefix}.{name}"
                walk(subschema, child_prefix)

        # Traverse any combinators
        for key in ("oneOf", "anyOf", "allOf"):
            if key in node and isinstance(node[key], list):
                for sub in node[key]:
                    walk(sub, prefix)

    walk(js, "")
    return out


def load_json_schema(path: str) -> Any:
    with open_text(path) as f:
        return json.load(f)


def schema_from_json_schema_file(path: str) -> Tuple[Any, Set[str]]:
    """
    Returns (type_tree, required_paths_set)
    - type_tree: pure type tree (no 'missing' injected)
    - required_paths_set: dotted paths that are required by the schema
    """
    js = load_json_schema(path)
    # pure types (value-nullability allowed)
    tree = _schema_from_js(js, optional=False)
    # presence (required) as dotted set
    required = _collect_required_paths(js)
    return tree, required
