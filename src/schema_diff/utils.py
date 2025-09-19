"""
Shared schema utilities.

This module collects small, reusable helpers that operate on our internal
“type tree” representation, so they can be used across compare/report/loader
without duplication.
"""

from __future__ import annotations
from copy import deepcopy
from typing import Any, Iterable, Dict, Set


__all__ = [

    # tree coercion / presence
    "coerce_root_to_field_dict",
    "wrap_optional",
    "inject_presence_for_diff",

    # path analysis
    "flatten_paths",
    "paths_by_name",
    "compute_path_changes",

    # field filtering
    "filter_schema_by_fields",
]


# ──────────────────────────────────────────────────────────────────────────────
# Coercion & Presence helpers
# ──────────────────────────────────────────────────────────────────────────────

def coerce_root_to_field_dict(tree: Any) -> Any:
    """
    If the *root* is a list-of-fields, convert to {name: type}.
    Supports two common shapes:
      1) [{'name': 'id', 'type': ...}, ...]
      2) [{'id': 'int'}, {'name': 'str'}, ...]
    If the root is not a recognized list-of-fields, return it unchanged.
    """
    if not isinstance(tree, list) or not tree:
        return tree

    # case 1: [{'name': ..., 'type': ...}, ...]
    if all(isinstance(el, dict) and "name" in el for el in tree):
        out: Dict[str, Any] = {}
        for el in tree:
            name = str(el["name"])
            # prefer 'type', but tolerate other common keys
            t = el.get("type", el.get("dataType", el.get("dtype", "any")))
            out[name] = t
        return out

    # case 2: [{'id': 'int'}, {'name': 'str'}, ...]
    if all(isinstance(el, dict) and len(el) == 1 for el in tree):
        out: Dict[str, Any] = {}
        for el in tree:
            (name, t) = next(iter(el.items()))
            out[str(name)] = t
        return out

    return tree


def wrap_optional(t: Any) -> Any:
    """
    Wrap a scalar or array type with union(...|missing) if not already wrapped.

    - Scalars: "str" -> "union(str|missing)"
               "union(int|float)" -> "union(float|int|missing)"
    - Arrays:  ["str"] -> "union(array|missing)"
    - Objects are handled at the parent level and are not wrapped here.
    """
    if isinstance(t, str):
        if t.startswith("union(") and t.endswith(")"):
            parts = set(t[6:-1].split("|"))
            if "missing" in parts:
                return t
            parts.add("missing")
            return "union(" + "|".join(sorted(parts)) + ")"
        return f"union({t}|missing)"
    if isinstance(t, list):
        return "union(array|missing)"
    return t  # dict/object or other unusual node: unchanged


def inject_presence_for_diff(tree: Any, required_paths: Iterable[str] | None) -> Any:
    """
    Apply presence constraints to a *pure* type tree by wrapping optional leaves
    with '|missing', so it aligns with the data-derived schema.

    Parameters
    ----------
    tree : Any
        Pure type tree (dict/lists/str) with **no** presence mixed in.
    required_paths : Iterable[str] | None
        Dotted paths that are presence-required (e.g., NOT NULL / JSON Schema 'required').

    Returns
    -------
    Any
        A deep-copied tree where non-required **leaf** fields are unioned with 'missing'.
        Presence applies to the field holding an array, not the array's element type.
    """
    required: Set[str] = set(required_paths or [])
    out = deepcopy(tree)

    def walk(node: Any, prefix: str) -> None:
        if isinstance(node, dict):
            for k, v in list(node.items()):
                path = f"{prefix}.{k}" if prefix else k
                if isinstance(v, dict):
                    walk(v, path)
                else:
                    if path in required:
                        continue  # keep pure type for required
                    node[k] = wrap_optional(v)
        # arrays: presence is modeled at the parent, not inside the element

    if isinstance(out, dict):
        walk(out, "")
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Path analysis (used to detect moved/nested fields)
# ──────────────────────────────────────────────────────────────────────────────

def flatten_paths(tree: Any, prefix: str = "") -> list[str]:
    """
    Return a list of dotted paths for all *leaf* fields in the schema tree.

    We recurse into dicts (objects) and arrays (lists with dict elements).
    Arrays are represented with [0] notation for element access.
    """
    out: list[str] = []
    if isinstance(tree, dict):
        for k, v in tree.items():
            if isinstance(k, str) and k.startswith("__"):  # ignore meta-keys if any
                continue
            p = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                out.extend(flatten_paths(v, p))
            elif isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
                # Handle arrays with object elements - use [0] notation
                array_p = f"{p}[0]"
                out.extend(flatten_paths(v[0], array_p))
            else:
                out.append(p)
    elif isinstance(tree, list) and len(tree) > 0 and isinstance(tree[0], dict):
        # Handle root-level arrays
        array_p = f"{prefix}[0]" if prefix else "[0]"
        out.extend(flatten_paths(tree[0], array_p))
    else:
        if prefix:
            out.append(prefix)
    return out


def paths_by_name(paths: list[str]) -> dict[str, set[str]]:
    """
    Group full dotted paths by leaf name (final segment).
    Example: 'a.b.c' -> grouped under key 'c'.
    """
    by_name: dict[str, set[str]] = {}
    for p in paths:
        leaf = p.split(".")[-1]
        by_name.setdefault(leaf, set()).add(p)
    return by_name


def compute_path_changes(left_tree: Any, right_tree: Any) -> list[dict[str, Any]]:
    """
    Detect fields that share the same *name* but live in different *paths*
    between left and right schemas.

    Returns a list of dicts like:
      {"name": "foo", "left": ["foo"], "right": ["bar.foo"]}

    This is useful to surface “moved/nested differently” cases that aren’t
    type mismatches, but structural/nesting differences.
    """
    left_paths = flatten_paths(left_tree)
    right_paths = flatten_paths(right_tree)
    lmap = paths_by_name(left_paths)
    rmap = paths_by_name(right_paths)

    names = set(lmap) & set(rmap)
    out: list[dict[str, Any]] = []
    for name in sorted(names):
        lp, rp = lmap[name], rmap[name]
        if lp != rp:
            out.append({"name": name, "left": sorted(lp), "right": sorted(rp)})
    return out

# ---------- Generic helpers ----------


def strip_quotes_ident(s: str) -> str:
    s = s.strip()
    if s and s[0] in "'\"`[" and s[-1:] in "'\"`]":
        return s[1:-1]
    return s


def union_str(types: list[str]) -> str:
    atoms: list[str] = []
    for t in types:
        atoms.extend(t.split("|"))
    atoms = sorted(set(atoms))
    if "any" in atoms and len(atoms) > 1:
        atoms.remove("any")
    return atoms[0] if len(atoms) == 1 else "union(" + "|".join(atoms) + ")"

# ---------- DeepDiff path helpers ----------


def clean_deepdiff_path(path: str) -> str:
    s = path[4:] if path.startswith("root") else path
    s = s.replace("']['", ".").replace("['", ".").replace("']", "")
    return s.lstrip(".")


def fmt_dot_path(p: str) -> str:
    if not p:
        return p
    if p.startswith("."):
        return p
    return ("." + p) if (p[0].isalpha() or p[0] == "_") else p


# ──────────────────────────────────────────────────────────────────────────────
# Field filtering helpers
# ──────────────────────────────────────────────────────────────────────────────

def filter_schema_by_fields(schema: Any, fields: list[str]) -> Any:
    """
    Filter a schema tree to include only specific fields.

    Parameters
    ----------
    schema : Any
        The schema tree (typically a dict representing object fields)
    fields : list[str]
        List of field names to include (supports nested paths with dots and array notation)

    Returns
    -------
    Any
        Filtered schema containing only specified fields
    """
    if not fields:
        return schema

    if not isinstance(schema, dict):
        return schema

    filtered = {}

    for field in fields:
        # Handle array element paths like "experience[0].title"
        if "[0]." in field:
            array_field, rest = field.split("[0].", 1)
            if array_field in schema and isinstance(schema[array_field], list) and len(schema[array_field]) > 0:
                if array_field not in filtered:
                    filtered[array_field] = [{}]
                # Recursively filter the array element
                if isinstance(schema[array_field][0], dict):
                    nested_filtered = filter_schema_by_fields(schema[array_field][0], [rest])
                    if isinstance(filtered[array_field][0], dict):
                        filtered[array_field][0].update(nested_filtered)
                    else:
                        filtered[array_field][0] = nested_filtered
        # Handle nested field paths like "experience.title"
        elif "." in field:
            root_field, rest = field.split(".", 1)
            if root_field in schema:
                if root_field not in filtered:
                    if isinstance(schema[root_field], list):
                        filtered[root_field] = []
                    else:
                        filtered[root_field] = {}
                # Recursively filter nested fields
                if isinstance(schema[root_field], dict):
                    nested_filtered = filter_schema_by_fields(schema[root_field], [rest])
                    if isinstance(filtered[root_field], dict):
                        filtered[root_field].update(nested_filtered)
                    else:
                        filtered[root_field] = nested_filtered
                elif isinstance(schema[root_field], list) and len(schema[root_field]) > 0:
                    # Handle array field without explicit [0] notation
                    if isinstance(schema[root_field][0], dict):
                        nested_filtered = filter_schema_by_fields(schema[root_field][0], [rest])
                        filtered[root_field] = [nested_filtered]
        else:
            # Simple field name
            if field in schema:
                filtered[field] = schema[field]

    return filtered
