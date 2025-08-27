# schema_diff/schema_from_data.py
from typing import Any, List, Dict
from .infer import tname
from .config import Config

BASE_OF_EMPTY = {"empty_array": "array",
                 "empty_object": "object", "empty_string": "str"}


def to_schema(o: Any, cfg: Config) -> Any:
    if o is None:
        return "missing"
    if isinstance(o, list):
        return [to_schema(o[0], cfg)] if o else "empty_array"
    if isinstance(o, dict):
        return {k: to_schema(v, cfg) for k, v in o.items()} if o else "empty_object"
    if isinstance(o, str) and o == "":
        return "empty_string"
    return tname(o, cfg)


def union_types(a: str, b: str) -> str:
    if a == b:
        return a

    def parts(s: str): return s[6:-1].split("|") if s.startswith(
        "union(") and s.endswith(")") else [s]
    merged = sorted(set(parts(a) + parts(b)))
    return merged[0] if len(merged) == 1 else "union(" + "|".join(merged) + ")"


def merge_schema(a: Any, b: Any) -> Any:
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
        return [merge_schema(ae, be)] if (a and b) else union_types("array", "array")
    from .infer import tname as _t
    return union_types(a if isinstance(a, str) else _t(a, Config()), b if isinstance(b, str) else _t(b, Config()))


def merged_schema_from_samples(recs: List[Any], cfg: Config) -> Any:
    sch = None
    for r in recs:
        s = to_schema(r, cfg)
        sch = s if sch is None else merge_schema(sch, s)
    return sch if sch is not None else "missing"
