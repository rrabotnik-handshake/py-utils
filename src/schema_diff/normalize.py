from typing import Any, List

BASE_OF_EMPTY = {"empty_array": "array",
                 "empty_object": "object", "empty_string": "str"}


def collapse_empty(t: str) -> str: return BASE_OF_EMPTY.get(t, t)


def _is_union(s: Any) -> bool: return isinstance(s,
                                                 str) and s.startswith("union(") and s.endswith(")")


def _union_parts(s: str) -> List[str]: return s[6:-
                                                1].split("|") if _is_union(s) else [s]


def normalize_union(s: Any) -> Any:
    if _is_union(s):
        parts = sorted(set(collapse_empty(p) for p in _union_parts(s)))
        if "any" in parts and len(parts) > 1:
            parts.remove("any")
        return parts[0] if len(parts) == 1 else "union(" + "|".join(parts) + ")"
    if isinstance(s, str):
        return collapse_empty(s)
    return s


def walk_normalize(x: Any) -> Any:
    if isinstance(x, dict):
        return {k: walk_normalize(v) for k, v in x.items()}
    if isinstance(x, list):
        if not x:
            return x
        elem = walk_normalize(x[0])
        if elem == "any" or (_is_union(elem) and set(_union_parts(elem)) == {"any"}):
            return "array"
        return [elem]
    return normalize_union(x)


def _has_any(x: Any) -> bool:
    if x == "any":
        return True
    if isinstance(x, list) and len(x) == 1 and x[0] == "any":
        return True
    if _is_union(x):
        parts = set(_union_parts(x))
        return parts == {"any"} or "any" in parts
    return False


def is_presence_issue(a: str, b: str) -> bool:
    def parts(s: str):
        if isinstance(s, str) and s.startswith("union(") and s.endswith(")"):
            return set(s[6:-1].split("|"))
        return {s} if isinstance(s, str) else {str(s)}
    A, B = parts(a), parts(b)
    return "missing" in A or "missing" in B
