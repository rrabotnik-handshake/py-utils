"""
Tree normalization utilities.

This module converts heterogeneous, partially-specified type trees into a
stable, comparable representation used across the diff pipeline.

Conventions enforced by normalization
-------------------------------------
- Empty sentinels collapse to their base kinds:
    * "empty_array"  -> "array"
    * "empty_object" -> "object"
    * "empty_string" -> "str"
- Unions are canonicalized:
    * members are deduped and sorted
    * "any" is removed if other concrete types are present
    * singleton unions are unwrapped to just that type
- Arrays are represented as:
    * "array"                if element type is unknown/any
    * [<normalized_element>] otherwise
- Objects are normalized recursively key-by-key.
- Presence is NOT decided here. Presence-specific logic is handled elsewhere
  (e.g., via injecting `missing` or by presence sets).

Public helpers exposed
----------------------
- collapse_empty
- normalize_union
- walk_normalize
- _has_any               (internal, but used by reporter)
"""

from __future__ import annotations

from typing import Any

# Mapping of "empty_*" markers to their base kinds
BASE_OF_EMPTY = {
    "empty_array": "array",
    "empty_object": "object",
    "empty_string": "str",
}


def collapse_empty(t: str) -> str:
    """Map an 'empty_*' sentinel to its base type; otherwise return `t` unchanged."""
    return BASE_OF_EMPTY.get(t, t)


def _is_union(s: Any) -> bool:
    """Return True if `s` is a union string of the form 'union(a|b|...)'."""
    return isinstance(s, str) and s.startswith("union(") and s.endswith(")")


def _union_parts(s: str) -> list[str]:
    """Split a union string into its member list; return [s] if not a union."""
    return s[6:-1].split("|") if _is_union(s) else [s]


def normalize_union(s: Any) -> Any:
    """
    Canonicalize a union string or scalar.

    - Dedup and sort members.
    - Collapse 'empty_*' members to base kind.
    - Remove 'any' if other concrete members are present.
    - If only one member remains, return that member instead of 'union(...)'.
    """
    if _is_union(s):
        parts = sorted({collapse_empty(p) for p in _union_parts(s)})
        if "any" in parts and len(parts) > 1:
            parts.remove("any")
        return parts[0] if len(parts) == 1 else "union(" + "|".join(parts) + ")"
    if isinstance(s, str):
        return collapse_empty(s)
    return s


def walk_normalize(x: Any) -> Any:
    """
    Recursively normalize a schema/tree.

    Objects (dict):
        Normalize values recursively.

    Arrays (list):
        - Empty list stays empty.
        - Normalize the first element; if the element type is "any" (or union that
          effectively equals {"any"}), collapse the array to the scalar "array".
        - Otherwise, return a single-element list with the normalized element.

    Scalars/Unions:
        Normalize via `normalize_union`.
    """
    if isinstance(x, dict):
        return {k: walk_normalize(v) for k, v in x.items()}

    if isinstance(x, list):
        if not x:
            return x
        elem = walk_normalize(x[0])
        # If the element is unknown/any, collapse to just "array"
        if elem == "any" or (_is_union(elem) and set(_union_parts(elem)) == {"any"}):
            return "array"
        return [elem]

    return normalize_union(x)


def _has_any(x: Any) -> bool:
    """
    Return True if `x` is (or contains) the 'any' type.

    Cases:
      - "any"
      - ["any"]
      - union(...) where "any" is included
    """
    if x == "any":
        return True
    if isinstance(x, list) and len(x) == 1 and x[0] == "any":
        return True
    if _is_union(x):
        parts: set[str] = set(_union_parts(x))  # type: ignore[arg-type]
        return parts == {"any"} or "any" in parts
    return False
