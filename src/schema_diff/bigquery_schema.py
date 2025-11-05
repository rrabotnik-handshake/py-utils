#!/usr/bin/env python3
"""BigQuery schema conversion and type rendering.

This module provides functionality for:
- Converting BigQuery schemas to internal schema-diff format
- Rendering BigQuery types for DDL generation
- Type mapping and normalization
"""
from __future__ import annotations

from typing import Any

from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField

# Import configuration
from schema_diff import analyze_config

# =============================================================================
# Type Rendering for DDL Generation
# =============================================================================


def render_scalar_type(bq_type: str) -> str:
    """Render scalar type using canonical DDL forms from config.

    Uses analyze_config.SCALAR_TYPE_MAP which normalizes type aliases to BigQuery DDL
    preferred forms (FLOAT64, INT64, BOOL).
    """
    return analyze_config.SCALAR_TYPE_MAP.get(bq_type, bq_type)


def render_col_options(field: SchemaField) -> str:
    """Render column-level OPTIONS(...): description + policy tags.

    Supports BigQuery's PolicyTagList (field.policy_tags.names) as well as older shapes
    (iterables / dicts). Applies to nested fields too.
    """

    def _extract_policy_tag_names(f: SchemaField) -> list[str]:
        pt = getattr(f, "policy_tags", None)
        if not pt:
            return []
        # Common case: PolicyTagList(names=[...])
        names = getattr(pt, "names", None)
        # Sometimes client returns a plain list/tuple/set
        if names is None and isinstance(pt, (list, tuple, set)):
            names = list(pt)
        # Rare: dict-like {"names": [...]} or {"policy_tags": [...]}
        if names is None and isinstance(pt, dict):
            names = pt.get("names") or pt.get("policy_tags")
        # Last resort: try to iterate
        if names is None:
            try:
                names = list(pt)
            except Exception:
                names = []
        # Normalize to strings, drop empties, dedupe preserving order
        seen = set()
        out: list[str] = []
        for n in names or []:
            s = str(n).strip()
            if s and s not in seen:
                seen.add(s)
                out.append(s)
        return out

    opts: list[str] = []

    # Description (safe escaping)
    if field.description:
        desc = (
            field.description.replace("\\", "\\\\")
            .replace('"', r"\"")
            .replace("\n", r"\n")
        )
        opts.append(f'description="{desc}"')

    # Policy tags
    tag_names = _extract_policy_tag_names(field)
    if tag_names:
        tags_literal = ", ".join(f'"{t}"' for t in tag_names)
        opts.append(f"policy_tags=[{tags_literal}]")

    if not opts:
        return ""
    return f" OPTIONS({', '.join(opts)})"


def render_not_null(field: SchemaField) -> str:
    """Render NOT NULL constraint based on field mode."""
    return " NOT NULL" if field.mode == "REQUIRED" else ""


def render_struct_type(fields: list[SchemaField], level: int) -> str:
    """
    Return a multi-line STRUCT type with nested indentation:
    STRUCT<
      `a` INT64,
      `b` STRUCT<
        `c` STRING
      >
    >
    """
    inner_lines: list[str] = []
    for idx, f in enumerate(fields):
        # Build the field type (may be nested)
        field_type_rendered = render_type_for_field(f, level + 1)  # may be multiline
        # Column-level details (NOT NULL / OPTIONS) apply to STRUCT member in-place
        tail = f"{render_not_null(f)}{render_col_options(f)}"
        inner_lines.append(
            f"{analyze_config.INDENT * (level + 1)}`{f.name}` {field_type_rendered}{tail}"
        )
        if idx < len(fields) - 1:
            inner_lines[-1] = inner_lines[-1] + ","
    struct_block = (
        "STRUCT<\n" + "\n".join(inner_lines) + f"\n{analyze_config.INDENT * level}>"
    )
    return struct_block


def render_array_of_struct_type(field: SchemaField, level: int) -> str:
    """ARRAY<STRUCT<...>> with multiline STRUCT payload:

    ARRAY<STRUCT<
      ...
    >>
    """
    payload = render_struct_type(field.fields, level + 1)
    return (
        "ARRAY<"
        + "\n".join(payload.splitlines())
        + f"\n{analyze_config.INDENT * level}>"
    )


def render_type_for_field(field: SchemaField, level: int) -> str:
    """Return the type string for a SchemaField, possibly multiline for nested types."""
    if field.field_type == "RECORD":
        if field.mode == "REPEATED":
            return render_array_of_struct_type(field, level)
        return render_struct_type(field.fields, level)
    else:
        base = render_scalar_type(field.field_type)
        if field.mode == "REPEATED":
            return f"ARRAY<{base}>"
        return base


def render_default(field: SchemaField, level: int) -> str:
    """Render DEFAULT expression (BigQuery supports DEFAULT on top-level non-STRUCT columns)."""
    # BigQuery supports DEFAULT on columns (not nested STRUCT members)
    if level == 1 and field.field_type != "RECORD":
        expr = getattr(field, "default_value_expression", None)
        if expr:
            return f" DEFAULT ({expr})"
    return ""


def render_column(field: SchemaField, level: int) -> str:
    """Render a single top-level column, expanding nested types cleanly.

    Example:
      `payload` STRUCT<
        `a` INT64,
        `b` ARRAY<STRUCT<
          `x` STRING
        >>
      > NOT NULL OPTIONS(...)
    """
    type_str = render_type_for_field(field, level)
    tail = f"{render_default(field, level)}{render_not_null(field)}{render_col_options(field)}"
    return f"{analyze_config.INDENT * level}`{field.name}` {type_str}{tail}"


def render_columns(schema: list[SchemaField]) -> str:
    """Render all columns for a CREATE TABLE statement."""
    lines: list[str] = []
    for i, f in enumerate(schema):
        col = render_column(f, 1)  # columns start indented once
        if i < len(schema) - 1:
            lines.append(col + ",")
        else:
            lines.append(col)
    return "\n".join(lines)


# =============================================================================
# Schema Conversion to Internal Format
# =============================================================================


def bigquery_schema_to_internal(
    schema: list[SchemaField],
) -> tuple[dict[str, Any], set[str]]:
    """Convert BigQuery schema to internal schema-diff format.

    Returns:
        (schema_tree, required_paths)
    """

    def convert_field(field: SchemaField, path_prefix: str = "") -> Any:
        field_path = f"{path_prefix}.{field.name}" if path_prefix else field.name

        if field.field_type == "RECORD":
            # STRUCT type
            struct_dict = {}
            for sub_field in field.fields:
                struct_dict[sub_field.name] = convert_field(sub_field, field_path)

            if field.mode == "REPEATED":
                return [struct_dict]  # Array of structs
            else:
                return struct_dict
        else:
            # Scalar type
            base_type = map_bq_type_to_internal(field.field_type)
            if field.mode == "REPEATED":
                return [base_type]  # Array of scalars
            else:
                return base_type

    def collect_required_paths(
        schema: list[SchemaField], path_prefix: str = ""
    ) -> set[str]:
        required = set()
        for field in schema:
            field_path = f"{path_prefix}.{field.name}" if path_prefix else field.name

            if field.mode == "REQUIRED":
                required.add(field_path)

            # Recurse into RECORD fields
            if field.field_type == "RECORD":
                if field.mode == "REPEATED":
                    # For repeated records, collect paths with array notation
                    nested_required = collect_required_paths(
                        field.fields, f"{field_path}[]"
                    )
                else:
                    nested_required = collect_required_paths(field.fields, field_path)
                required.update(nested_required)

        return required

    # Convert schema
    tree = {}
    for field in schema:
        tree[field.name] = convert_field(field)

    # Collect required paths
    required_paths = collect_required_paths(schema)

    # Normalize BigQuery array wrapper patterns
    tree = normalize_bigquery_arrays(tree)

    return tree, required_paths


def normalize_bigquery_arrays(tree: Any) -> Any:
    """Normalize BigQuery array wrapper patterns like {'list': [{'element': ...}]} to [...].

    This removes the BigQuery-specific array wrapper structure to match the cleaner
    array notation used elsewhere in schema-diff.
    """
    if isinstance(tree, dict):
        normalized = {}
        for key, value in tree.items():
            # Check for BigQuery array wrapper pattern
            if (
                isinstance(value, dict)
                and len(value) == 1
                and "list" in value
                and isinstance(value["list"], list)
                and len(value["list"]) == 1
                and isinstance(value["list"][0], dict)
                and len(value["list"][0]) == 1
                and "element" in value["list"][0]
            ):
                # Extract the element structure and normalize recursively
                element_structure = value["list"][0]["element"]
                normalized[key] = [normalize_bigquery_arrays(element_structure)]
            else:
                # Recursively normalize nested structures
                normalized[key] = normalize_bigquery_arrays(value)
        return normalized
    elif isinstance(tree, list):
        # Normalize list elements
        return [normalize_bigquery_arrays(item) for item in tree]
    else:
        # Return primitive values as-is
        return tree


def map_bq_type_to_internal(bq_type: str) -> str:
    """Map BigQuery types to internal schema-diff types."""
    type_map = {
        "STRING": "str",
        "INT64": "int",
        "INTEGER": "int",
        "FLOAT64": "float",
        "FLOAT": "float",
        "NUMERIC": "number",
        "BIGNUMERIC": "number",
        "BOOL": "bool",
        "BOOLEAN": "bool",
        "DATE": "date",
        "DATETIME": "datetime",
        "TIME": "time",
        "TIMESTAMP": "timestamp",
        "BYTES": "bytes",
        "GEOGRAPHY": "geography",
        "JSON": "json",
        "INTERVAL": "interval",
    }
    return type_map.get(bq_type, bq_type.lower())


def get_live_table_schema(
    project_id: str, dataset_id: str, table_id: str
) -> tuple[dict[str, Any], set[str]]:
    """Get live BigQuery table schema in schema-diff internal format.

    Returns:
        (schema_tree, required_paths)
    """
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    table = client.get_table(table_ref)

    return bigquery_schema_to_internal(table.schema)


# =============================================================================
# Type Canonicalization for Analysis
# =============================================================================


def canon_type(field_type: str) -> str:
    """Normalize BigQuery type aliases to canonical forms for analysis.

    Maps FROM type aliases TO canonical analysis forms (INT64, FLOAT64, BOOLEAN).
    This is the inverse direction of analyze_config.SCALAR_TYPE_MAP (used for DDL rendering).

    Why the difference?
    - DDL rendering: BOOLEAN → BOOL (BigQuery DDL prefers short form)
    - Analysis: BOOL → BOOLEAN (longer form is more explicit for comparisons)

    Args:
        field_type: BigQuery field type (e.g., "INTEGER", "INT64", "FLOAT")

    Returns:
        Canonical type name (e.g., "INT64", "FLOAT64", "BOOLEAN")
    """
    t = (field_type or "").upper()
    return {
        "INTEGER": "INT64",
        "FLOAT": "FLOAT64",
        "BOOL": "BOOLEAN",
        "BIGNUMERIC": "BIGNUMERIC",
    }.get(t, t)


__all__ = [
    # Type rendering for DDL
    "render_scalar_type",
    "render_col_options",
    "render_not_null",
    "render_struct_type",
    "render_array_of_struct_type",
    "render_type_for_field",
    "render_default",
    "render_column",
    "render_columns",
    # Schema conversion
    "bigquery_schema_to_internal",
    "normalize_bigquery_arrays",
    "map_bq_type_to_internal",
    "get_live_table_schema",
    # Type canonicalization
    "canon_type",
]
