"""
Spark schema (pretty-printed) → internal type tree.

What this module does
---------------------
Parses a text dump of a Spark schema like:

    root
     |-- id: long (nullable = false)
     |-- ts: timestamp (nullable = true)
     |-- tags: array<string> (nullable = true)
     |-- info: struct<foo:int,bar:array<string>> (nullable = false)

…into:
  - a *pure type* tree (no presence/`|missing` injection), where
      * scalars map to: 'int' | 'float' | 'bool' | 'str' | 'date' | 'timestamp' | 'any'
      * arrays are represented as: [elem_type]
      * structs are represented as: {field: type, ...}
      * maps are simplified to: "object"
  - a set of *required* top-level field names where `(nullable = false)`.

Notes & constraints
-------------------
- Spark prints nullability only on the top-level field declaration lines;
  nested struct field nullability is not present in this text format, so
  only top-level required paths are returned.
- Unknown/unsupported tokens (including intervals and exotic types) become "any".
- Arrays of arrays (e.g., `array<array<int>>`) are preserved as nested lists.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Set, Tuple

__all__ = ["schema_from_spark_schema_file"]

# ---------- Type mapping (Spark -> internal) ----------
_SPARK_TO_INTERNAL = {
    # integers
    "byte": "int",
    "short": "int",
    "int": "int",
    "integer": "int",
    "long": "int",
    "bigint": "int",

    # floats / decimals
    "float": "float",
    "double": "float",
    "decimal": "float",  # precision/scale not tracked; treat as numeric

    # booleans
    "boolean": "bool",
    "bool": "bool",

    # strings / bytes-ish
    "string": "str",
    "binary": "str",     # treat Spark binary as base64-able string
    "varchar": "str",
    "char": "str",

    # date/time
    "timestamp": "timestamp",
    "date": "date",
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
    """
    Map a scalar/leaf Spark type token to an internal label.

    - Strips any precision/length parentheses (e.g., "decimal(10,2)" → "decimal").
    - Returns one of: 'int' | 'float' | 'bool' | 'str' | 'date' | 'timestamp' | 'any'.
    """
    t = tok.strip().lower()
    # Drop "(...)" precision/length from tokens like decimal(10,2) / varchar(255)
    base = re.split(r"\s*\(", t, maxsplit=1)[0].strip()
    return _SPARK_TO_INTERNAL.get(base, "any")


def _split_top_level_commas(s: str) -> List[str]:
    """
    Split by commas that are NOT inside angle brackets (for struct fields).

    Example
    -------
    "a:int,b:array<string>,c:struct<x:int,y:string>"
      -> ["a:int", "b:array<string>", "c:struct<x:int,y:string>"]
    """
    parts: List[str] = []
    buf: List[str] = []
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
    Parse a Spark dtype string into the internal type tree.

    Returns
    -------
    Any
        - "int" / "str" / "timestamp" / … for scalars
        - [elem_type] for arrays
        - {field: type, ...} for structs
        - "object" for maps and malformed structs
        - "any" for unknown scalars
    """
    s = dtype.strip()
    s_lower = s.lower()

    # array<...>
    if s_lower.startswith("array<") and s_lower.endswith(">"):
        inner = s[6:-1].strip()
        elem = _parse_dtype(inner)
        # Preserve nested array structure (normalizer may collapse if needed)
        if isinstance(elem, list) and elem:  # array<array<...>>
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
            # each f is "name:type"
            if ":" not in f:
                # malformed; fallback conservatively
                return "object"
            name, ft = f.split(":", 1)
            obj[name.strip()] = _parse_dtype(ft.strip())
        return obj if obj else "object"

    # map<k,v>  -> treat as object (keyed collection)
    if s_lower.startswith("map<") and s_lower.endswith(">"):
        return "object"

    # scalar / fallback
    return _parse_scalar_type(s)


# ---------- Main parser ----------

def schema_from_spark_schema_file(path: str) -> Tuple[Any, Set[str]]:
    """
    Parse a Spark schema text dump into (type_tree, required_paths).

    Parameters
    ----------
    path : str
        Path to the file containing Spark's printed schema (e.g., from `df.printSchema()`).

    Returns
    -------
    (type_tree, required_paths) : Tuple[Any, Set[str]]
        type_tree:
            Pure type tree (no presence injection):
            - scalars as strings ('int' | 'float' | 'bool' | 'str' | 'date' | 'timestamp' | 'any')
            - arrays as [elem_type]
            - structs as {field: type, ...}
            - maps as "object"
        required_paths:
            Set of top-level field names with `(nullable = false)`.

    Notes
    -----
    - The printed format only exposes nullability at the top-level field lines,
      so nested required paths are not derivable here.
    """
    # Collect meaningful lines (skip blank lines and the 'root' header)
    lines: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.rstrip("\n")
            if not line.strip():
                continue
            if line.strip().lower() == "root":
                continue
            lines.append(line)

    tree: Dict[str, Any] = {}
    required: Set[str] = set()

    for ln in lines:
        m = _LINE_RE.match(ln)
        if not m:
            # ignore non-matching lines (comments/indent noise, etc.)
            continue

        name = m.group("name")
        dtype = (m.group("dtype") or "").strip()
        attrs = (m.group("attrs") or "")

        # nullability (default: nullable)
        nullable = True
        nm = _NULLABLE_RE.search(attrs)
        if nm:
            nullable = (nm.group(1).lower() == "true")

        # type
        parsed = _parse_dtype(dtype)

        tree[name] = parsed
        if not nullable:
            required.add(name)

    # Empty schema → generic object + empty required set
    if not tree:
        return "object", set()

    return tree, required
