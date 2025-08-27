# schema_diff/presence.py
from __future__ import annotations
from typing import Any, Set, List


def _is_union(s: Any) -> bool:
    return isinstance(s, str) and s.startswith("union(") and s.endswith(")")


def _union_parts(s: str) -> List[str]:
    return s[6:-1].split("|") if _is_union(s) else [s]


def _union_str(parts: List[str]) -> str:
    parts = sorted(set(parts))
    if "any" in parts and len(parts) > 1:
        parts.remove("any")
    return parts[0] if len(parts) == 1 else "union(" + "|".join(parts) + ")"


def _ensure_optional(t: Any) -> Any:
    """
    Add '|missing' to a scalar/array type if it's not already optional.
    For arrays expressed as ["str"], treat field presence: union(array|missing).
    """
    if isinstance(t, list):
        # We encode "field may be missing" at the field level, not inside the element type.
        return "union(array|missing)"
    if isinstance(t, str):
        if _is_union(t):
            parts = _union_parts(t)
            if "missing" in parts:
                return t
            return _union_str(parts + ["missing"])
        # scalar leaf
        return f"union({t}|missing)"
    # Fallback
    return "union(any|missing)"


def apply_presence(tree: Any, required_paths: Set[str]) -> Any:
    """
    Walk a dict-like type tree and inject '|missing' for paths NOT present in required_paths.
    required_paths are dotted (e.g., "a.b.c") for nested JSON Schemas; for flat SQL/dbt they are top-level names.
    """
    def walk(node: Any, prefix: str) -> Any:
        if isinstance(node, dict):
            out = {}
            for k, v in node.items():
                path = f"{prefix}.{k}" if prefix else k
                child = walk(v, path)
                if path not in required_paths:
                    child = _ensure_optional(child)
                out[k] = child
            return out
        else:
            # leaf at top-level position: only optionalize here if the *field* path is known by caller.
            # (In practice, this function is called starting at the object root, so leaves are under keys.)
            return node
    return walk(tree, "")
