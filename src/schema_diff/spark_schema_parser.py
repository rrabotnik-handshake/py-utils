from __future__ import annotations

import re
from typing import Any, Dict, List, Set, Tuple

# ---------- Type mapping (Spark -> internal) ----------
_SPARK_TO_INTERNAL = {
    "byte": "int",
    "short": "int",
    "int": "int",
    "integer": "int",
    "long": "int",
    "bigint": "int",

    "float": "float",
    "double": "float",
    "decimal": "float",  # no precision captured here; we treat as numeric

    "boolean": "bool",
    "bool": "bool",

    "string": "str",
    "binary": "str",     # treat as bytes-ish string
    "varchar": "str",
    "char": "str",

    "timestamp": "timestamp",
    "date": "date",

    # anything else we don't recognize:
    #  - interval, map, etc. will be handled below; unknown scalars => "any"
}

# Matches lines like:
#   |-- id: long (nullable = false)
#   |-- tags: array<string> (nullable = true)
#   |-- info: struct<foo:int,bar:array<string>> (nullable = true)
_LINE_RE = re.compile(
    r'^\s*\|\-\-\s*(?P<name>[A-Za-z0-9_]+)\s*:\s*(?P<dtype>[^\(]+?)\s*(?:\((?P<attrs>[^)]*)\))?\s*$'
)

# Extract nullable flag from "(nullable = false)" part
_NULLABLE_RE = re.compile(r'\bnullable\s*=\s*(true|false)\b', re.IGNORECASE)


# ---------- Type string parser (recursive) ----------

def _parse_scalar_type(tok: str) -> str:
    """Map a scalar/leaf Spark type token to internal label."""
    t = tok.strip().lower()
    return _SPARK_TO_INTERNAL.get(t, "any")


def _split_top_level_commas(s: str) -> List[str]:
    """
    Split by commas that are NOT inside angle brackets (for struct fields).
    Example: "a:int,b:array<string>,c:struct<x:int,y:string>" ->
             ["a:int", "b:array<string>", "c:struct<x:int,y:string>"]
    """
    parts: List[str] = []
    buf = []
    depth = 0
    for ch in s:
        if ch == '<':
            depth += 1
            buf.append(ch)
        elif ch == '>':
            depth = max(0, depth - 1)
            buf.append(ch)
        elif ch == ',' and depth == 0:
            parts.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    if buf:
        parts.append("".join(buf).strip())
    return parts


def _parse_dtype(dtype: str) -> Any:
    """
    Parse a Spark dtype string recursively into internal tree:
      - "int", "string", "timestamp" -> "int"/"str"/"timestamp"
      - "array<T>" -> [ parsed(T) ]
      - "struct<a:int,b:array<string>>" -> {"a": "int", "b": ["str"]}
      - Unsupported/unknown -> "any"
    """
    s = dtype.strip()
    s_lower = s.lower()

    # array<...>
    if s_lower.startswith("array<") and s_lower.endswith(">"):
        inner = s[6:-1].strip()
        elem = _parse_dtype(inner)
        # Represent arrays as [elem_type] (consistent with other parsers)
        if isinstance(elem, list) and elem:
            # If elem was itself an array (array<array<...>>), elem could be list
            # Keep nested lists as-is (downstream normalizer can collapse to "array" if needed)
            return [elem]
        return [elem]

    # struct<...>
    if s_lower.startswith("struct<") and s_lower.endswith(">"):
        inner = s[7:-1].strip()
        if not inner:
            return "object"
        fields = _split_top_level_commas(inner)
        obj: Dict[str, Any] = {}
        for f in fields:
            # each f like: "name:type" (Spark DDL struct syntax doesn't include nullability here)
            if ":" not in f:
                # malformed; fallback
                return "object"
            name, ft = f.split(":", 1)
            name = name.strip()
            obj[name] = _parse_dtype(ft.strip())
        return obj if obj else "object"

    # map<k,v>  -> treat as object (keyed collection)
    if s_lower.startswith("map<") and s_lower.endswith(">"):
        return "object"

    # scalar / fallback
    return _parse_scalar_type(s)


# ---------- Main parser ----------

def schema_from_spark_schema_file(path: str) -> Tuple[Any, Set[str]]:
    """
    Parse a Spark schema text dump like:

        root
         |-- id: long (nullable = false)
         |-- ts: timestamp (nullable = true)
         |-- tags: array<string> (nullable = true)
         |-- info: struct<foo:int,bar:array<string>> (nullable = false)

    Returns: (type_tree, required_paths)
      - type_tree: pure types (no '|missing' injection)
      - required_paths: dotted paths (here only top-level field names) with nullable=false
        (Spark's printed schema puts nullability only at the field declaration line)
    """
    lines = []
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.rstrip("\n")
            if not line.strip():
                continue
            # skip the first "root" line if present
            if line.strip().lower() == "root":
                continue
            lines.append(line)

    tree: Dict[str, Any] = {}
    required: Set[str] = set()

    for ln in lines:
        m = _LINE_RE.match(ln)
        if not m:
            # ignore non-matching lines (comments/blank/indent noise)
            continue

        name = m.group("name")
        dtype = (m.group("dtype") or "").strip()
        attrs = (m.group("attrs") or "")

        # nullability
        nullable = True
        nm = _NULLABLE_RE.search(attrs)
        if nm:
            nullable = (nm.group(1).lower() == "true")

        # type
        parsed = _parse_dtype(dtype)

        tree[name] = parsed
        if not nullable:
            required.add(name)

    # Empty schema? Return generic object + empty required set
    if not tree:
        return "object", set()

    return tree, required
