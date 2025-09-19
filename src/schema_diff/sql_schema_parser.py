from __future__ import annotations
import re
from typing import Any, Dict, Optional, Tuple, Set

from .utils import strip_quotes_ident
from .io_utils import open_text

"""
SQL DDL schema parser with comprehensive BigQuery support.

This module extracts a **pure type tree** and a set of **presence-required** paths
from SQL `CREATE TABLE` statements with full support for complex nested types.
Handles both Postgres-like DDL and BigQuery DDL with advanced type parsing.

Key Features:
- **BigQuery STRUCT parsing**: Recursively parses nested STRUCT<field:type, ...> definitions
- **BigQuery ARRAY support**: Handles ARRAY<type> including ARRAY<STRUCT<...>> patterns
- **Multi-line reconstruction**: Properly handles STRUCT/ARRAY definitions spanning multiple lines
- **Nullability detection**: Correctly identifies NOT NULL constraints for presence tracking
- **DDL statement filtering**: Ignores CREATE/ALTER/DROP statements in loose parsing mode
- **Array wrapper normalization**: Converts BigQuery's internal array format to standard arrays

Outputs:
    schema_tree: Dict[str, Any]
        - scalars: 'int' | 'float' | 'bool' | 'str' | 'date' | 'time' | 'timestamp'
        - arrays:  [elem_type] with full nested structure preservation
        - structs: recursively parsed nested dictionaries
        - complex types: 'object' for unsupported types
    required_paths: Set[str]
        - dotted paths for all fields marked NOT NULL (supports nested field paths)

Enhanced Capabilities:
- Handles deeply nested STRUCT<field1:type1, field2:STRUCT<...>, field3:ARRAY<STRUCT<...>>> patterns
- Preserves complex nested structures instead of flattening to 'object'
- Supports BigQuery's backticked identifiers and various SQL dialects
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
    # dtype: capture everything until comma or EOL (we'll handle STRUCT parsing separately)
    r'(?P<dtype>.+?)'
    r'(?:\s+(?P<null>NOT\s+NULL|NULL))?'
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


def _parse_struct_type(struct_type: str) -> Dict[str, Any]:
    """
    Parse BigQuery STRUCT<...> syntax into a nested type tree.

    Example:
        STRUCT<name STRING, age INTEGER, tags ARRAY<STRING>>
        -> {"name": "str", "age": "int", "tags": ["str"]}
    """
    # Extract the content inside STRUCT<...>
    inner, _ = _balanced_inner(struct_type, struct_type.find("<"))
    if inner is None:
        return "object"  # fallback for malformed STRUCT

    # Parse comma-separated field definitions
    fields = _split_struct_fields(inner)
    struct_dict: Dict[str, Any] = {}

    for field_def in fields:
        field_name, field_type = _parse_struct_field(field_def)
        if field_name:
            struct_dict[field_name] = _sql_dtype_to_internal(field_type)

    return struct_dict if struct_dict else "object"


def _split_struct_fields(fields_str: str) -> list[str]:
    """
    Split comma-separated field definitions, respecting nested angle brackets.

    Example: "name STRING, tags ARRAY<STRING>, profile STRUCT<bio STRING>"
    -> ["name STRING", "tags ARRAY<STRING>", "profile STRUCT<bio STRING>"]
    """
    fields = []
    current_field = ""
    depth = 0
    i = 0

    while i < len(fields_str):
        char = fields_str[i]

        if char == '<':
            depth += 1
            current_field += char
        elif char == '>':
            depth -= 1
            current_field += char
        elif char == ',' and depth == 0:
            # End of current field
            if current_field.strip():
                fields.append(current_field.strip())
            current_field = ""
        else:
            current_field += char

        i += 1

    # Add the last field
    if current_field.strip():
        fields.append(current_field.strip())

    return fields


def _parse_struct_field(field_def: str) -> tuple[str, str]:
    """
    Parse a single field definition like "name STRING" or "tags ARRAY<STRING>".

    Returns: (field_name, field_type)
    """
    # Handle quoted field names and complex types
    field_def = field_def.strip()

    # Look for the first space that's not inside angle brackets
    depth = 0
    split_pos = -1

    for i, char in enumerate(field_def):
        if char == '<':
            depth += 1
        elif char == '>':
            depth -= 1
        elif char == ' ' and depth == 0 and split_pos == -1:
            split_pos = i
            break

    if split_pos == -1:
        return "", ""  # malformed field definition

    field_name = field_def[:split_pos].strip()
    field_type = field_def[split_pos:].strip()

    # Remove quotes from field name if present
    field_name = field_name.strip('`"[]')

    return field_name, field_type


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
        # Preserve dict structures (from STRUCT parsing) and other non-string types
        return [inner_m]

    # BigQuery STRUCT<...> -> parse nested structure
    if dt.startswith("struct<"):
        return _parse_struct_type(dt)

    # Postgres-style arrays: text[] / varchar(255)[]
    is_array = dt.endswith("[]")
    if is_array:
        dt = dt[:-2].strip()

    # Strip precision/length (e.g., varchar(255) -> varchar)
    base = re.sub(r'\([^)]*\)', '', dt).strip()
    mapped = TYPE_MAP_SQL.get(base, TYPE_MAP_SQL.get(dt, "any"))

    return [mapped] if is_array else mapped


def _normalize_bigquery_arrays(schema: Any) -> Any:
    """
    Normalize BigQuery's list[0].element array wrapper pattern to standard arrays.

    Converts: {'list': [{'element': {...}}]}
    To: [{...}]

    This removes BigQuery's internal storage representation and produces
    structures that match the actual JSON data format.
    """
    if isinstance(schema, dict):
        # Check for BigQuery array pattern: {'list': [{'element': ...}]}
        if (len(schema) == 1 and 'list' in schema and
            isinstance(schema['list'], list) and len(schema['list']) == 1 and
            isinstance(schema['list'][0], dict) and len(schema['list'][0]) == 1 and
            'element' in schema['list'][0]):

            # Extract the element structure and make it an array
            element_structure = schema['list'][0]['element']
            # Recursively normalize the element structure
            normalized_element = _normalize_bigquery_arrays(element_structure)
            return [normalized_element]

        # Recursively normalize nested structures
        normalized = {}
        for key, value in schema.items():
            normalized[key] = _normalize_bigquery_arrays(value)
        return normalized

    elif isinstance(schema, list):
        # Recursively normalize array elements
        return [_normalize_bigquery_arrays(item) for item in schema]

    else:
        # Scalar values - return as-is
        return schema


def _as_pure_type(mapped: Any) -> Any:
    """
    Convert a mapper result (scalar or [elem]) into a consistent "pure type".
    Ensures arrays normalize to ['any'] instead of ['any', ...] or [].
    Then applies BigQuery normalization.
    """
    if isinstance(mapped, list):
        elem = mapped[0] if mapped else "any"
        result = ["any" if elem == "any" else elem]
    else:
        result = mapped

    # Apply BigQuery normalization to remove list[0].element wrappers
    return _normalize_bigquery_arrays(result)


def _reconstruct_multiline_structs(text: str) -> str:
    """
    Reconstruct multi-line BigQuery STRUCT definitions into single lines.

    Converts:
        `activity` STRUCT<
          `list` ARRAY<STRUCT<
              `element` STRUCT<
                `action` STRING,
                `activity_url` STRING
              >
            >
          >
        >,

    To:
        `activity` STRUCT<`list` ARRAY<STRUCT<`element` STRUCT<`action` STRING, `activity_url` STRING>>>>,
    """
    lines = text.splitlines()
    result_lines = []
    i = 0

    while i < len(lines):
        line = lines[i].strip()

        # Check if this line starts a column definition with STRUCT
        if line and ('STRUCT<' in line or 'ARRAY<' in line):
            # This might be a multi-line definition
            reconstructed_line = _collect_multiline_definition(lines, i)
            if reconstructed_line:
                result_lines.append(reconstructed_line)
                # Skip to the end of this definition
                i = _find_definition_end(lines, i) + 1
            else:
                result_lines.append(lines[i])
                i += 1
        else:
            result_lines.append(lines[i])
            i += 1

    return '\n'.join(result_lines)


def _collect_multiline_definition(lines: list[str], start_idx: int) -> str:
    """
    Collect a multi-line STRUCT/ARRAY definition and flatten it to one line.
    """
    if start_idx >= len(lines):
        return ""

    # Start with the first line
    result_parts = [lines[start_idx].strip()]

    # Track bracket depth to know when the definition ends
    depth = 0
    definition_started = False

    for i in range(start_idx, len(lines)):
        line = lines[i].strip()

        # Count angle brackets to track STRUCT/ARRAY nesting
        for char in line:
            if char == '<':
                depth += 1
                definition_started = True
            elif char == '>':
                depth -= 1

        # If this isn't the first line, add it to our reconstruction
        if i > start_idx and line:
            # Remove quotes and clean up the line for inline format
            clean_line = line.replace('`', '').replace('"', '')
            # Preserve commas - they separate fields
            result_parts.append(clean_line)

        # If we've closed all brackets and we started a definition, we're done
        if definition_started and depth == 0:
            break

    # Join all parts and clean up extra spaces
    result = ' '.join(result_parts)
    # Clean up extra spaces around commas and brackets
    result = re.sub(r'\s*,\s*', ', ', result)
    result = re.sub(r'\s*<\s*', '<', result)
    result = re.sub(r'\s*>\s*', '>', result)

    return result


def _find_definition_end(lines: list[str], start_idx: int) -> int:
    """
    Find the line index where a multi-line STRUCT/ARRAY definition ends.
    """
    depth = 0
    definition_started = False

    for i in range(start_idx, len(lines)):
        line = lines[i].strip()

        for char in line:
            if char == '<':
                depth += 1
                definition_started = True
            elif char == '>':
                depth -= 1

        if definition_started and depth == 0:
            return i

    return start_idx  # fallback


def _extract_complete_type(line: str) -> str:
    """
    Extract the complete type definition from a line, handling nested STRUCT/ARRAY.

    For lines like: `col` STRUCT<...nested...> NOT NULL,
    Returns: STRUCT<...nested...>
    """
    # Find column name and type start
    parts = line.strip().split(None, 2)
    if len(parts) < 2:
        return ""

    # Skip column name, get the rest
    after_col = line.strip().split(None, 1)[1] if ' ' in line.strip() else ""

    # If it's a simple type (no STRUCT/ARRAY), just return until comma or constraints
    if not ('STRUCT<' in after_col or 'ARRAY<' in after_col):
        # Simple type - return until comma or constraint keywords
        for keyword in ['NOT NULL', 'NULL', 'DEFAULT', 'OPTIONS', 'CONSTRAINT', ',']:
            if keyword in after_col:
                return after_col.split(keyword)[0].strip()
        return after_col.strip()

    # Complex type - need to balance angle brackets
    depth = 0
    result = ""
    started = False

    for char in after_col:
        if char == '<':
            depth += 1
            started = True
            result += char
        elif char == '>':
            depth -= 1
            result += char
            if started and depth == 0:
                # Found the end of the nested structure
                break
        elif started:
            result += char
        elif not started and char != ' ':
            result += char

    return result.strip()


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
    text = _reconstruct_multiline_structs(text)
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

                        # For STRUCT/ARRAY types, extract the complete type definition
                        if 'STRUCT<' in rem_line or 'ARRAY<' in rem_line:
                            dtype_raw = _extract_complete_type(rem_line)

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

                # For STRUCT types, extract the complete type definition
                # Note: ARRAY types work fine with regex-captured dtype, but STRUCT may need multi-line handling
                if 'STRUCT<' in line:
                    dtype_raw = _extract_complete_type(line)

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
        # Skip SQL DDL statements that shouldn't be treated as columns
        if line.upper().startswith(('CREATE ', 'ALTER ', 'DROP ', 'INSERT ', 'UPDATE ', 'DELETE ', 'SELECT ')):
            continue

        m_col = SQL_COL_RE.match(line)
        if m_col:
            full = "__loose__"
            if full not in tables:
                tables[full] = {}
                required_by_table[full] = set()
                name_lc_to_full[full] = full

            col = strip_quotes_ident(m_col.group("col"))
            dtype_raw = m_col.group("dtype") or ""

            # For STRUCT/ARRAY types, extract the complete type definition
            if 'STRUCT<' in line or 'ARRAY<' in line:
                dtype_raw = _extract_complete_type(line)

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
