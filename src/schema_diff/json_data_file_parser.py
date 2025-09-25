"""
Turn JSON data (objects/arrays) into a compact *type tree* and merge multiple
samples into a single schema.

Conventions
-----------
- Scalars → "int" | "float" | "bool" | "str" | "date" | "time" | "timestamp" | "missing"
- Objects → {key: type_tree}
- Arrays  → [element_type_tree]   (empty arrays become "empty_array")
- Empty values → "empty_string" | "empty_object" | "empty_array"
  (a later normalizer can map these to base types)
- Unions are represented as: "union(a|b|...)" with sorted, deduplicated atoms.
"""

from __future__ import annotations

from typing import Any

from .config import Config
from .infer import tname

__all__ = [
    "to_schema",
    "union_types",
    "merge_schema",
    "merged_schema_from_samples",
]

# Mapping for normalizers that want a base type for empties (not used here directly)
BASE_OF_EMPTY = {
    "empty_array": "array",
    "empty_object": "object",
    "empty_string": "str",
}


def to_schema(o: Any, cfg: Config) -> Any:
    """
    Convert a single JSON value into a *type tree*.

    Notes
    -----
    - Arrays: we preserve element type shape as a single-element list.
      If the array is empty, we return "empty_array".
    - Objects: recursively convert fields; empty dict → "empty_object".
    - Strings: empty string → "empty_string"; otherwise uses `tname(...)`
      which can infer date/time/timestamp if cfg.infer_datetimes=True.
    """
    if o is None:
        return "missing"
    if isinstance(o, list):
        # For performance we keep the convention of using only the first element's shape.
        # The merge step will union across multiple samples anyway.
        return [to_schema(o[0], cfg)] if o else "empty_array"
    if isinstance(o, dict):
        return {k: to_schema(v, cfg) for k, v in o.items()} if o else "empty_object"
    if isinstance(o, str) and o == "":
        return "empty_string"
    return tname(o, cfg)


def union_types(a: str, b: str) -> str:
    """
    Union two scalar/union type strings into a normalized "union(...)" form.

    - Sorts atoms and deduplicates.
    - If 'any' is present alongside other atoms, drops 'any' (the others are more specific).
    - Collapses singletons back to the bare atom.
    """
    if a == b:
        return a

    def atoms(s: str) -> list[str]:
        return s[6:-1].split("|") if s.startswith("union(") and s.endswith(")") else [s]

    merged = sorted(set(atoms(a) + atoms(b)))
    if "any" in merged and len(merged) > 1:
        merged.remove("any")
    return merged[0] if len(merged) == 1 else "union(" + "|".join(merged) + ")"


def merge_schema(a: Any, b: Any) -> Any:
    """
    Merge two *type trees* produced by `to_schema`.

    Rules
    -----
    - Same scalar → itself.
    - Scalar vs scalar → `union_types`.
    - Dict vs dict    → fieldwise merge; missing sides treated as "missing".
    - List vs list    → merge element 0 shape where present; both empty → "empty_array".
      If only one side has an element shape, returns a union-like summary "array"
      via `union_types("array", "array")` (kept for legacy behavior).
    - Mixed kinds     → coerce each side to its scalar name via `tname(..., Config())`
      and union. (Note: this uses a default Config; DATA inference settings are applied
      earlier in `to_schema`.)
    """
    if a == b:
        return a

    if isinstance(a, str) and isinstance(b, str):
        return union_types(a, b)

    if isinstance(a, dict) and isinstance(b, dict):
        keys = set(a) | set(b)
        return {k: merge_schema(a.get(k, "missing"), b.get(k, "missing")) for k in keys}

    if isinstance(a, list) and isinstance(b, list):
        if not a and not b:
            return "empty_array"
        ae = a[0] if a else "empty_array"
        be = b[0] if b else "empty_array"
        # Preserve element shape when at least one side has structure
        if a and b:
            # Both have elements - merge them
            return [merge_schema(ae, be)]
        elif a or b:
            # One has elements, other is empty - preserve the element structure
            element_schema = ae if a else be
            return [element_schema]
        else:
            # Both empty (shouldn't reach here due to first check, but for safety)
            return "empty_array"

    # Handle mixed empty_array (string) with populated array (list)
    if (isinstance(a, str) and a == "empty_array" and isinstance(b, list)) or (
        isinstance(b, str) and b == "empty_array" and isinstance(a, list)
    ):
        # Preserve the populated array structure
        return a if isinstance(a, list) else b

    # Mixed kinds: fall back to scalar names and union them
    from .infer import tname as _t

    return union_types(
        a if isinstance(a, str) else _t(a, Config()),
        b if isinstance(b, str) else _t(b, Config()),
    )


def merged_schema_from_samples(recs: list[Any], cfg: Config) -> Any:
    """
    Merge a list of JSON records into a single schema tree.
    Returns "missing" if `recs` is empty.
    """
    sch: Any | None = None
    for r in recs:
        s = to_schema(r, cfg)
        sch = s if sch is None else merge_schema(sch, s)
    return sch if sch is not None else "missing"
