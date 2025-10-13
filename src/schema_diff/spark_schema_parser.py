"""Spark schema (pretty-printed) → internal type tree with deep nested parsing.

What this module does
---------------------
Parses a text dump of a Spark schema with comprehensive support for nested structures:

    root
     |-- id: long (nullable = false)
     |-- ts: timestamp (nullable = true)
     |-- tags: array<string> (nullable = true)
     |-- events: array<struct<
     |    |-- action: string
     |    |-- meta: struct<
     |    |    |-- key: string (nullable = false)
     |    |    |-- value: string
     |    |  >>
     |  >> (nullable = true)

…into:
  - a *pure type* tree (no presence/`|missing` injection), where
      * scalars map to: 'int' | 'float' | 'bool' | 'str' | 'date' | 'timestamp' | 'any'
      * arrays are represented as: [elem_type] with full element structure
      * structs are represented as: {field: type, ...} recursively parsed
      * maps are simplified to: "object"
  - a set of *required* field paths (not just top-level) where `(nullable = false)`.

Key enhancements
----------------
- **Hierarchical parsing**: Handles deeply nested array<struct<...>> patterns
- **Full nullability tracking**: Captures required_paths at all nesting levels, not just top-level
- **Robust indentation handling**: Uses |    pattern counting for accurate nesting detection
- **Complex structure support**: Parses multi-line struct definitions and array elements

Notes & constraints
-------------------
- Properly tracks nullability throughout the entire nested structure using path prefixes
  only top-level required paths are returned.
- Unknown/unsupported tokens (including intervals and exotic types) become "any".
- Arrays of arrays (e.g., `array<array<int>>`) are preserved as nested lists.
"""

from __future__ import annotations

import re
from typing import Any

from .decorators import cache_expensive_operation, validate_and_time

__all__ = ["schema_from_spark_schema_file", "schema_from_spark_schema_file_unified"]

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
    "binary": "str",  # treat Spark binary as base64-able string
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
    r"^\s*\|\-\-\s*(?P<name>[A-Za-z0-9_]+)\s*:\s*(?P<dtype>[^\(]+?)\s*(?:\((?P<attrs>[^)]*)\))?\s*$"
)

# Extract nullable flag from "(nullable = false)" part
_NULLABLE_RE = re.compile(r"\bnullable\s*=\s*(true|false)\b", re.IGNORECASE)


# ---------- Type string parser (recursive) ----------


def _parse_scalar_type(tok: str) -> str:
    """Map a scalar/leaf Spark type token to an internal label.

    - Strips any precision/length parentheses (e.g., "decimal(10,2)" → "decimal").
    - Returns one of: 'int' | 'float' | 'bool' | 'str' | 'date' | 'timestamp' | 'any'.
    """
    t = tok.strip().lower()
    # Drop "(...)" precision/length from tokens like decimal(10,2) / varchar(255)
    base = re.split(r"\s*\(", t, maxsplit=1)[0].strip()
    return _SPARK_TO_INTERNAL.get(base, "any")


def _split_top_level_commas(s: str) -> list[str]:
    """Split by commas that are NOT inside angle brackets (for struct fields).

    Example
    -------
    "a:int,b:array<string>,c:struct<x:int,y:string>"
      -> ["a:int", "b:array<string>", "c:struct<x:int,y:string>"]
    """
    parts: list[str] = []
    buf: list[str] = []
    depth = 0
    for ch in s:
        if ch == "<":
            depth += 1
            buf.append(ch)
        elif ch == ">":
            depth = max(0, depth - 1)
            buf.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    if buf:
        parts.append("".join(buf).strip())
    return parts


def _parse_dtype(dtype: str) -> Any:
    """Parse a Spark dtype string into the internal type tree.

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
        obj: dict[str, Any] = {}
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


# ---------- Hierarchical structure parser ----------


def _parse_field_line(line: str) -> tuple[int, str, str, bool]:
    """Parse a single field line and extract indentation level, name, type, and.
    nullability.

    Returns
    -------
    (indent_level, field_name, field_type, is_nullable) : Tuple[int, str, str, bool]
    """
    # Count indentation level by counting "|    " patterns in the line
    # Level 0: " |-- field"
    # Level 1: " |    |-- field"
    # Level 2: " |    |    |-- field"
    indent_level = line.count("|    ")

    # Updated regex to handle any indentation level
    field_re = re.compile(
        r"\|\-\-\s*(?P<name>[A-Za-z0-9_]+)\s*:\s*(?P<dtype>[^\(]+?)\s*(?:\((?P<attrs>[^)]*)\))?\s*$"
    )
    match = field_re.search(line)

    if not match:
        return -1, "", "", True  # Invalid line

    name = match.group("name")
    dtype = (match.group("dtype") or "").strip()
    attrs = match.group("attrs") or ""

    # Parse nullability
    nullable = True
    nullable_match = _NULLABLE_RE.search(attrs)
    if nullable_match:
        nullable = nullable_match.group(1).lower() == "true"

    return indent_level, name, dtype, nullable


def _parse_hierarchical_structure(lines: list[str]) -> tuple[dict[str, Any], set[str]]:
    """Parse the hierarchical Spark schema structure into a nested type tree.

    This handles the indented structure like:
    |-- field: array
    |    |-- element: struct
    |    |    |-- nested_field: string
    """
    tree: dict[str, Any] = {}
    required: set[str] = set()

    # Stack to track current nesting context and path
    # Each item is (indent_level, current_dict, path_prefix)
    context_stack: list[tuple[int, dict[str, Any], str]] = [(0, tree, "")]

    i = 0
    while i < len(lines):
        line = lines[i]
        indent_level, field_name, field_type, is_nullable = _parse_field_line(line)

        if indent_level == -1:
            i += 1
            continue

        # Pop stack until we find the right parent level
        while len(context_stack) > 1 and context_stack[-1][0] >= indent_level:
            context_stack.pop()

        current_dict = context_stack[-1][1]
        current_path = context_stack[-1][2]

        # Build the full path for this field
        if current_path:
            if current_path.endswith("[0]"):
                # We're inside an array element
                full_path = f"{current_path}.{field_name}"
            else:
                full_path = f"{current_path}.{field_name}"
        else:
            full_path = field_name

        # Handle different field types
        if field_type.lower() == "array":
            # Look ahead to see if next line defines the element structure
            if i + 1 < len(lines):
                next_indent, next_name, _, _ = _parse_field_line(lines[i + 1])
                if next_indent == indent_level + 1 and next_name == "element":
                    # This is a structured array, parse the element definition
                    element_struct, i, nested_required = _parse_array_element_structure(
                        lines, i + 1, indent_level + 1, f"{full_path}[0]"
                    )
                    current_dict[field_name] = [element_struct]
                    # Add nested required paths
                    required.update(nested_required)
                else:
                    # Simple array without structure info
                    current_dict[field_name] = ["any"]
            else:
                current_dict[field_name] = ["any"]

        elif field_type.lower().startswith(("array<", "struct<", "map<")):
            # Parse inline type definitions (e.g., array<string>, struct<field:type>)
            parsed_type = _parse_dtype(field_type)
            current_dict[field_name] = parsed_type

        elif field_type.lower().startswith("struct"):
            # Parse nested struct fields (hierarchical format)
            struct_dict, i, nested_required = _parse_struct_fields(
                lines, i, indent_level, full_path
            )
            current_dict[field_name] = struct_dict
            # Add nested required paths
            required.update(nested_required)

        else:
            # Simple scalar field
            parsed_type = _parse_scalar_type(field_type)
            current_dict[field_name] = parsed_type

        # Track required fields at all levels
        if not is_nullable:
            required.add(full_path)

        i += 1

    return tree, required


def _parse_array_element_structure(
    lines: list[str], start_idx: int, element_indent: int, path_prefix: str
) -> tuple[Any, int, set[str]]:
    """Parse the structure of an array element starting from the 'element: struct' line.

    Returns
    -------
    (element_type, last_processed_index, required_paths) : Tuple[Any, int, Set[str]]
    """
    element_indent, _, element_type, _ = _parse_field_line(lines[start_idx])
    required_paths: set[str] = set()

    if element_type.lower() == "struct":
        # Parse the struct fields that follow
        struct_fields: dict[str, Any] = {}
        i = start_idx + 1

        while i < len(lines):
            field_indent, field_name, field_type, is_nullable = _parse_field_line(
                lines[i]
            )

            if field_indent <= element_indent:
                # We've reached the end of this struct
                break

            if field_indent == element_indent + 1:
                # Direct child field of the struct
                field_path = f"{path_prefix}.{field_name}"

                if field_type.lower() == "array":
                    # Nested array within the struct
                    if i + 1 < len(lines):
                        next_indent, next_name, _, _ = _parse_field_line(lines[i + 1])
                        if next_indent == field_indent + 1 and next_name == "element":
                            (
                                nested_element,
                                i,
                                nested_required,
                            ) = _parse_array_element_structure(
                                lines, i + 1, field_indent + 1, f"{field_path}[0]"
                            )
                            struct_fields[field_name] = [nested_element]
                            required_paths.update(nested_required)
                        else:
                            struct_fields[field_name] = ["any"]
                    else:
                        struct_fields[field_name] = ["any"]
                elif field_type.lower().startswith("struct"):
                    # Nested struct within the struct
                    nested_struct, i, nested_required = _parse_struct_fields(
                        lines, i, field_indent, field_path
                    )
                    struct_fields[field_name] = nested_struct
                    required_paths.update(nested_required)
                else:
                    # Simple field
                    struct_fields[field_name] = _parse_scalar_type(field_type)

                # Track required fields
                if not is_nullable:
                    required_paths.add(field_path)

            i += 1

        return struct_fields, i - 1, required_paths
    else:
        # Simple element type
        return _parse_scalar_type(element_type), start_idx, required_paths


def _parse_struct_fields(
    lines: list[str], start_idx: int, struct_indent: int, path_prefix: str
) -> tuple[dict[str, Any], int, set[str]]:
    """Parse struct fields starting from a struct declaration.

    Returns
    -------
    (struct_dict, last_processed_index, required_paths) : Tuple[Dict[str, Any], int, Set[str]]
    """
    struct_fields: dict[str, Any] = {}
    required_paths: set[str] = set()
    i = start_idx + 1

    while i < len(lines):
        field_indent, field_name, field_type, is_nullable = _parse_field_line(lines[i])

        if field_indent <= struct_indent:
            # We've reached the end of this struct
            break

        if field_indent == struct_indent + 1:
            # Direct child field of the struct
            field_path = f"{path_prefix}.{field_name}"

            if field_type.lower() == "array":
                # Handle nested arrays
                if i + 1 < len(lines):
                    next_indent, next_name, _, _ = _parse_field_line(lines[i + 1])
                    if next_indent == field_indent + 1 and next_name == "element":
                        (
                            nested_element,
                            i,
                            nested_required,
                        ) = _parse_array_element_structure(
                            lines, i + 1, field_indent + 1, f"{field_path}[0]"
                        )
                        struct_fields[field_name] = [nested_element]
                        required_paths.update(nested_required)
                    else:
                        struct_fields[field_name] = ["any"]
                else:
                    struct_fields[field_name] = ["any"]
            elif field_type.lower().startswith("struct"):
                # Handle nested structs
                nested_struct, i, nested_required = _parse_struct_fields(
                    lines, i, field_indent, field_path
                )
                struct_fields[field_name] = nested_struct
                required_paths.update(nested_required)
            else:
                # Simple field
                struct_fields[field_name] = _parse_scalar_type(field_type)

            # Track required fields
            if not is_nullable:
                required_paths.add(field_path)

        i += 1

    return struct_fields, i - 1, required_paths


# ---------- Main parser ----------


@cache_expensive_operation
@validate_and_time
def schema_from_spark_schema_file(path: str) -> tuple[Any, set[str]]:
    """Parse a Spark schema text dump into (type_tree, required_paths).

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
    - Now properly handles nested structures by parsing indentation levels.
    """
    # Read all lines and build hierarchical structure
    with open(path, encoding="utf-8") as f:
        lines = [
            line.rstrip("\n")
            for line in f
            if line.strip() and line.strip().lower() != "root"
        ]

    if not lines:
        return "object", set()

    # Parse the hierarchical structure
    tree, required = _parse_hierarchical_structure(lines)

    # Empty schema → generic object + empty required set
    if not tree:
        return "object", set()

    return tree, required


def schema_from_spark_schema_file_unified(path: str):
    """Parse a Spark schema file and return unified Schema object.

    Returns
    -------
    Schema
        Unified schema representation using Pydantic models
    """
    from .models import from_legacy_tree

    tree, required = schema_from_spark_schema_file(path)
    return from_legacy_tree(tree, required, source_type="spark")
