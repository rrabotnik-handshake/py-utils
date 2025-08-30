from __future__ import annotations
import re
from typing import Any, Dict, Optional, Tuple, Set

from .utils import strip_quotes_ident
from .io_utils import open_text

"""
SQL DDL schema parser.

This module extracts a **pure type tree** and a set of **presence-required** paths
from SQL `CREATE TABLE` statements (and from loose column lists if no `CREATE TABLE`
is present). It aims to be adapter-agnostic and tolerant to common dialect quirks.

Outputs:
    schema_tree: Dict[str, Any]
        - scalars: 'int' | 'float' | 'bool' | 'str' | 'date' | 'time' | 'timestamp'
        - arrays:  [elem_type], e.g. ['str']
        - complex types we don't explode (e.g., STRUCT) -> 'object'
    required_paths: Set[str]
        - flat set of column names marked NOT NULL (Spark-style inner-nullability is
          not represented here; only top-level column constraints are tracked)

Notes:
    - BigQuery ARRAY<...> / STRUCT<...> are supported, nested angle brackets ok.
    - Postgres-style arrays like TEXT[] are supported.
    - Precision/length in types (e.g., VARCHAR(255), NUMERIC(10,2)) is ignored for
      mapping purposes.
    - Table selection:
        * If multiple CREATE TABLE blocks exist, you can pass `table=` to select.
        * If none exist, the parser treats the file as a loose column list.
"""

# --- Regexes ---------------------------------------------------------------

# Accepts: CREATE TABLE `proj.ds.table` (...), proj.schema.table, schema.table, or table (with/without IF NOT EXISTS)
SQL_CREATE_RE_FULL = re.compile(
    r'^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<full>[^(\s]+)\s*\(',
    re.IGNORECASE
)

# Legacy: captures optional schema + table (no backticks/nesting)
SQL_CREATE_RE_ST = re.compile(
    r'^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?'
    r'(?:(?P<schema>["`\[]?[A-Za-z_][A-Za-z0-9_]*["`\]]?)\.)?'
    r'(?P<table>["`\[]?[A-Za-z_][A-Za-z0-9_]*["`\]]?)\s*\(',
    re.IGNORECASE
)

SQL_COL_RE = re.compile(
    r'^\s*'
    r'(?P<col>["`\[]?[A-Za-z_][A-Za-z0-9_]*["`\]]?)\s+'
    # dtype: stop before NULL/NOT NULL/DEFAULT/OPTIONS/... or comma/EOL
    r'(?P<dtype>.+?)(?=\s+(?:NOT\s+NULL|NULL|DEFAULT|OPTIONS|CONSTRAINT|PRIMARY|UNIQUE|FOREIGN\s+KEY|CHECK|REFERENCES|GENERATED|AS|IDENTITY|ON\s+UPDATE|ON\s+DELETE)\b|,|$)'
    r'(?:\s+(?P<null>NOT\s+NULL|NULL))?'
    # NEW: allow trailing tokens like OPTIONS(...), DEFAULT ..., etc., before comma/EOL
    r'(?:\s+(?:DEFAULT|OPTIONS|CONSTRAINT|PRIMARY|UNIQUE|FOREIGN\s+KEY|CHECK|REFERENCES|GENERATED|AS|IDENTITY|ON\s+UPDATE|ON\s+DELETE)\b[^\n,]*)*'
    r'\s*(?:,|$)',
    re.IGNORECASE
)


# Constraint-only lines we skip inside CREATE TABLE
SQL_CONSTRAINT_LINE_RE = re.compile(
    r'^\s*(PRIMARY\s+KEY|UNIQUE|FOREIGN\s+KEY|CHECK|CONSTRAINT)\b',
    re.IGNORECASE
)

NOT_NULL_RE = re.compile(r'\bNOT\s+NULL\b', re.IGNORECASE)

# Comments
SQL_BLOCK_COMMENT_RE = re.compile(r'/\*.*?\*/', re.DOTALL)
SQL_LINE_COMMENT_RE = re.compile(r'--.*?$', re.MULTILINE)

# --- Type mapping ----------------------------------------------------------

TYPE_MAP_SQL: Dict[str, str] = {
    # Postgres-ish numerics
    "tinyint": "int", "smallint": "int", "int2": "int", "integer": "int", "int": "int",
    "int4": "int", "bigint": "int", "int8": "int", "serial": "int", "bigserial": "int",
    "float": "float", "float4": "float", "float8": "float", "double": "float",
    "double precision": "float", "real": "float", "numeric": "float", "decimal": "float",

    # boolean
    "boolean": "bool", "bool": "bool",

    # strings / json / bytes
    "varchar": "str", "character varying": "str",
    "char": "str", "character": "str",
    "text": "str", "citext": "str",
    "uuid": "str", "json": "str", "jsonb": "str",
    "bytea": "str",

    # date/time
    "date": "date",
    "time": "time", "time without time zone": "time", "time with time zone": "time",
    "timestamp": "timestamp", "timestamp without time zone": "timestamp",
    "timestamp with time zone": "timestamp", "timestamptz": "timestamp",
    "datetime": "timestamp",

    # BigQuery numerics / bool / strings / geography
    "int64": "int", "float64": "float", "bignumeric": "float",
    "string": "str", "bytes": "str", "geography": "str",
}

# Trim tokens that may trail the dtype on the same line
_DTYPE_TRAILERS_RE = re.compile(
    r'\s+(DEFAULT|COLLATE|REFERENCES|GENERATED|AS|IDENTITY|ON\s+UPDATE|ON\s+DELETE|OPTIONS|NOT\s+NULL|NULL)\b',
    re.IGNORECASE
)

# --- Helpers ---------------------------------------------------------------


def _strip_sql_comments(s: str) -> str:
    """Remove /* ... */ block comments and -- line comments from SQL text."""
    s = SQL_BLOCK_COMMENT_RE.sub(" ", s)
    s = SQL_LINE_COMMENT_RE.sub("", s)
    return s


def _clean_dtype_token(dt: str) -> str:
    """Trim trailers after the dtype (DEFAULT/COLLATE/REFERENCES/etc.)."""
    dt = dt.strip()
    m = _DTYPE_TRAILERS_RE.search(dt)
    if m:
        dt = dt[:m.start()].rstrip()
    return dt


def _balanced_inner(s: str, start: int) -> tuple[str, int] | tuple[None, int]:
    """
    Return (inner, end_index_after_closing_gt) for the angle-bracket segment
    starting at `start`, handling nested angle brackets. If unbalanced, return
    (None, start).
    """
    if start >= len(s) or s[start] != '<':
        return None, start
    depth = 0
    i = start
    inner_chars: list[str] = []
    while i < len(s):
        ch = s[i]
        if ch == '<':
            depth += 1
            if depth > 1:
                inner_chars.append(ch)
        elif ch == '>':
            depth -= 1
            if depth == 0:
                return ''.join(inner_chars).strip(), i + 1
            else:
                inner_chars.append(ch)
        else:
            inner_chars.append(ch)
        i += 1
    return None, start  # unbalanced


def _sql_dtype_to_internal(dtype_raw: str) -> Any:
    """
    Normalize an SQL dtype token to the internal type label/tree.

    Returns:
        - scalar label: 'int'|'float'|'bool'|'str'|'date'|'time'|'timestamp'|'any'
        - array: [elem_type]
        - struct-like: 'object'
    """
    dt = " ".join(_clean_dtype_token(dtype_raw).split()).lower()

    # BigQuery ARRAY<...> (supports nested)
    if dt.startswith("array<"):
        inner, _ = _balanced_inner(dt, dt.find("<"))
        inner = inner if inner is not None else dt[6:-1].strip()
        inner_m = _sql_dtype_to_internal(inner)
        if isinstance(inner_m, list) and inner_m:
            inner_m = inner_m[0]
        if not isinstance(inner_m, str):
            inner_m = "any"
        return [inner_m]

    # BigQuery STRUCT<...> -> treat as opaque object
    if dt.startswith("struct<"):
        return "object"

    # Postgres-style arrays: text[] / varchar(255)[]
    is_array = dt.endswith("[]")
    if is_array:
        dt = dt[:-2].strip()

    # Strip precision/length (e.g., varchar(255) -> varchar)
    base = re.sub(r'\([^)]*\)', '', dt).strip()
    mapped = TYPE_MAP_SQL.get(base, TYPE_MAP_SQL.get(dt, "any"))

    return [mapped] if is_array else mapped


def _as_pure_type(mapped: Any) -> Any:
    """
    Convert a mapper result (scalar or [elem]) into a consistent "pure type".
    Ensures arrays normalize to ['any'] instead of ['any', ...] or [].
    """
    if isinstance(mapped, list):
        elem = mapped[0] if mapped else "any"
        return ["any" if elem == "any" else elem]
    return mapped


# --- Main API --------------------------------------------------------------

def schema_from_sql_schema_file(path: str, table: Optional[str] = None) -> Tuple[Dict[str, Any], Set[str]]:
    """
    Parse SQL schema content from `path`.

    Args:
        path: File with SQL DDL (or loose column list).
        table: Optional selector for a particular CREATE TABLE block. Case-insensitive.
               Accepts fully-qualified (e.g., project.dataset.table) or simple table name.

    Returns:
        (schema_tree, required_paths)
          - schema_tree: pure type tree (no presence 'missing' injection)
          - required_paths: set of NOT NULL column names (flat)
    """
    with open_text(path) as f:
        text = f.read()

    text = _strip_sql_comments(text)
    lines = text.splitlines()

    tables: Dict[str, Dict[str, Any]] = {}
    required_by_table: Dict[str, Set[str]] = {}
    name_lc_to_full: Dict[str, str] = {}

    current: Optional[Dict[str, Any]] = None
    current_required: Optional[Set[str]] = None
    current_name: Optional[str] = None
    paren_depth = 0
    in_block = False

    def _finish_block() -> None:
        """Close the current CREATE TABLE block and stash its results."""
        nonlocal current, current_required, current_name, in_block, paren_depth
        if current_name and current is not None:
            tables[current_name] = current
            required_by_table[current_name] = current_required or set()
        current = None
        current_required = None
        current_name = None
        in_block = False
        paren_depth = 0

    for raw in lines:
        line = raw.strip()
        if not line:
            continue

        # ---- START OF CREATE TABLE ----
        m_full = SQL_CREATE_RE_FULL.match(line)
        m_std = None if m_full else SQL_CREATE_RE_ST.match(line)

        if m_full or m_std:
            if in_block:
                _finish_block()

            if m_full:
                full_raw = m_full.group("full")
                full_name = strip_quotes_ident(full_raw)
                table_name = full_name.split(".")[-1].strip("`")
                # position of the first '(' to slice remainder
                hdr_open_idx = raw.upper().find("(")
            else:
                schema_name = strip_quotes_ident(m_std.group("schema") or "")
                table_name = strip_quotes_ident(m_std.group("table"))
                full_name = f"{schema_name}.{table_name}" if schema_name else table_name
                hdr_open_idx = raw.upper().find("(")

            current = {}
            current_required = set()
            current_name = full_name
            in_block = True

            name_lc_to_full[full_name.lower()] = full_name
            name_lc_to_full[table_name.lower()] = full_name

            # We will process the remainder of this *same* line as if we were already inside the block.
            # Start with depth=1 (we just consumed the header's '('), then adjust with the remainder.
            paren_depth = 1

            remainder = raw[hdr_open_idx + 1:] if hdr_open_idx != -1 else ""
            rem_line = remainder.strip()

            if rem_line:
                # ---- same logic as the in_block section, but applied to this remainder ----
                paren_depth += remainder.count("(") - remainder.count(")")

                # Skip table-level constraints
                if SQL_CONSTRAINT_LINE_RE.match(rem_line):
                    if paren_depth <= 0:
                        _finish_block()
                else:
                    m_col = SQL_COL_RE.match(rem_line)
                    if m_col and current is not None and current_required is not None:
                        col_raw = m_col.group("col")
                        dtype_raw = m_col.group("dtype") or ""
                        col = strip_quotes_ident(col_raw)

                        mapped = _sql_dtype_to_internal(dtype_raw)
                        current[col] = _as_pure_type(mapped)

                        # Presence: regex-captured group first; fallback to raw search
                        null_grp = (m_col.group("null") or "").upper()
                        if null_grp == "NOT NULL" or (not null_grp and NOT_NULL_RE.search(remainder)):
                            current_required.add(col)

                    if paren_depth <= 0:
                        _finish_block()

            # Done handling this header line; move to next input line
            continue
        # ---- END CREATE TABLE header handling ----


        if in_block:
            paren_depth += raw.count("(") - raw.count(")")

            # Skip table-level constraints
            if SQL_CONSTRAINT_LINE_RE.match(line):
                if paren_depth <= 0:
                    _finish_block()
                continue

            # Column definition
            m_col = SQL_COL_RE.match(line)
            if m_col and current is not None and current_required is not None:
                col_raw = m_col.group("col")
                dtype_raw = m_col.group("dtype") or ""
                col = strip_quotes_ident(col_raw)

                mapped = _sql_dtype_to_internal(dtype_raw)
                current[col] = _as_pure_type(mapped)

                # Presence: rely on the regex-captured nullability
                null_grp = (m_col.group("null") or "").upper()
                if null_grp == "NOT NULL" or (not null_grp and NOT_NULL_RE.search(raw)):
                    current_required.add(col)

            if paren_depth <= 0:
                _finish_block()
            continue

        # Outside CREATE TABLE: loose column list
        m_col = SQL_COL_RE.match(line)
        if m_col:
            full = "__loose__"
            if full not in tables:
                tables[full] = {}
                required_by_table[full] = set()
                name_lc_to_full[full] = full

            col = strip_quotes_ident(m_col.group("col"))
            dtype_raw = m_col.group("dtype") or ""
            mapped = _sql_dtype_to_internal(dtype_raw)

            tables[full][col] = _as_pure_type(mapped)

            null_grp = (m_col.group("null") or "").upper()
            if null_grp == "NOT NULL" or (not null_grp and NOT_NULL_RE.search(raw)):
                required_by_table[full].add(col)

    if in_block:
        _finish_block()

    if not tables:
        return {}, set()

    if table:
        want = table.strip().lower()
        key = name_lc_to_full.get(want)
        if key is None:
            raise ValueError(
                f"Table '{table}' not found in {path}. "
                f"Available: {', '.join(sorted(tables.keys()))}"
            )
        return tables[key], required_by_table.get(key, set())

    # Default: pick the first table by name (stable for tests)
    first = sorted(tables.keys())[0]
    return tables[first], required_by_table.get(first, set())
