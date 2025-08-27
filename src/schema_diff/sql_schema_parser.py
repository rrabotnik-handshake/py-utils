# schema_diff/sql_schema_parser.py
from __future__ import annotations
import re
from typing import Any, Dict, Optional, Tuple, Set
from .io_utils import open_text

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
    # dtype: non-greedy, stop before NULL/NOT NULL/DEFAULT/OPTIONS/CONSTRAINT/etc, or comma/end
    r'(?P<dtype>.+?)(?=\s+(?:NOT\s+NULL|NULL|DEFAULT|OPTIONS|CONSTRAINT|PRIMARY|UNIQUE|FOREIGN\s+KEY|CHECK|REFERENCES|GENERATED|AS|IDENTITY|ON\s+UPDATE|ON\s+DELETE)\b|,|$)'
    r'(?:\s+(?P<null>NOT\s+NULL|NULL))?'
    r'\s*(?:,|$)',
    re.IGNORECASE
)

# Constraint-only lines we skip inside CREATE TABLE
SQL_CONSTRAINT_LINE_RE = re.compile(
    r'^\s*(PRIMARY\s+KEY|UNIQUE|FOREIGN\s+KEY|CHECK|CONSTRAINT)\b',
    re.IGNORECASE
)

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
_DTYPE_TRAILERS = re.compile(
    r'\s+(DEFAULT|COLLATE|REFERENCES|GENERATED|AS|IDENTITY|ON\s+UPDATE|ON\s+DELETE|OPTIONS|NOT\s+NULL|NULL)\b',
    re.IGNORECASE
)

# --- Helpers ---------------------------------------------------------------


def _strip_sql_comments(s: str) -> str:
    s = SQL_BLOCK_COMMENT_RE.sub(" ", s)
    s = SQL_LINE_COMMENT_RE.sub("", s)
    return s


def _normalize_ident(s: str) -> str:
    """Strip quotes/backticks/brackets from an identifier."""
    s = s.strip()
    if s and s[0] in "'\"`[" and s[-1:] in "'\"`]":
        return s[1:-1]
    return s


def _clean_dtype_token(dt: str) -> str:
    """Trim trailers like DEFAULT/COLLATE/REFERENCES/etc."""
    dt = dt.strip()
    m = _DTYPE_TRAILERS.search(dt)
    if m:
        dt = dt[:m.start()].rstrip()
    return dt


def _balanced_inner(s: str, start: int) -> tuple[str, int] | tuple[None, int]:
    """
    Given s and index start pointing at '<', return (inner, end_index_after_closing_gt).
    Handles nested angle brackets. Returns (None, start) on failure.
    """
    if start >= len(s) or s[start] != '<':
        return None, start
    depth = 0
    i = start
    inner_chars = []
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
    dt = " ".join(_clean_dtype_token(dtype_raw).split()).lower()

    # BigQuery ARRAY<...> / STRUCT<...> with nesting
    if dt.startswith("array<"):
        inner, end = _balanced_inner(dt, dt.find("<"))
        # if we fail to balance, fall back to simple slice
        inner = inner if inner is not None else dt[6:-1].strip()
        inner_m = _sql_dtype_to_internal(inner)
        if isinstance(inner_m, list) and inner_m:
            inner_m = inner_m[0]
        if not isinstance(inner_m, str):
            inner_m = "any"
        return [inner_m]

    if dt.startswith("struct<"):
        # We treat structs as an opaque object (we don’t explode fields here)
        return "object"

    # Postgres style arrays: text[], varchar(255)[]
    is_array = dt.endswith("[]")
    if is_array:
        dt = dt[:-2].strip()

    base = re.sub(r'\([^)]*\)', '', dt).strip()
    mapped = TYPE_MAP_SQL.get(base, TYPE_MAP_SQL.get(dt, "any"))
    if is_array:
        return [mapped]
    return mapped


# --- Main API --------------------------------------------------------------


def schema_from_sql_schema_file(path: str, table: Optional[str] = None) -> Tuple[Dict[str, Any], Set[str]]:
    """
    Parse SQL schema content from `path`.

    Returns: (schema_tree, required_paths)
      - schema_tree: pure type tree (NO 'missing' injection).
        * scalars: 'int'|'float'|'bool'|'str'|'date'|'time'|'timestamp'
        * arrays: [elem_type], e.g. ['str']
      - required_paths: set of NOT NULL column names (flat paths)

    If multiple CREATE TABLE blocks exist, choose with `table` (case-insensitive).
    If none exist, parse as a loose column list.
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

    def _finish_block():
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
                full_name = _normalize_ident(full_raw)
                # use last dotted component as short table name
                table_name = full_name.split(".")[-1].strip("`")
            else:
                schema_name = _normalize_ident(m_std.group("schema") or "")
                table_name = _normalize_ident(m_std.group("table"))
                full_name = f"{schema_name}.{table_name}" if schema_name else table_name

            current = {}
            current_required = set()
            current_name = full_name
            in_block = True

            # case-insensitive lookup aliases
            name_lc_to_full[full_name.lower()] = full_name
            name_lc_to_full[table_name.lower()] = full_name

            paren_depth += raw.count("(") - raw.count(")")
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
                col = _normalize_ident(col_raw)

                mapped = _sql_dtype_to_internal(dtype_raw)

                # Build pure type (do NOT inject 'missing' here):
                if isinstance(mapped, list):
                    t: Any = ["any" if mapped[0] == "any" else mapped[0]]
                else:
                    t = mapped

                current[col] = t

                # Presence constraint (NOT NULL) tracked separately:
                if "NOT NULL" in line.upper():
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

            col = _normalize_ident(m_col.group("col"))
            dtype_raw = m_col.group("dtype") or ""
            mapped = _sql_dtype_to_internal(dtype_raw)

            if isinstance(mapped, list):
                t = ["any" if mapped[0] == "any" else mapped[0]]
            else:
                t = mapped

            tables[full][col] = t
            if "NOT NULL" in line.upper():
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

    first = sorted(tables.keys())[0]
    return tables[first], required_by_table.get(first, set())
