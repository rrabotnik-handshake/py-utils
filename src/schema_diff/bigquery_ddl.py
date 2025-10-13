#!/usr/bin/env python3
"""BigQuery DDL generator with live table schema extraction.

This module provides:
- Live BigQuery table schema extraction
- Pretty DDL generation with nested types
- Constraint handling (PK/FK)
- Syntax highlighting and formatting
- Integration with schema-diff comparison workflow
"""
from __future__ import annotations

import os
import re
import sys
from datetime import datetime
from typing import Any

from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField


def get_default_project() -> str:
    """Get the default BigQuery project from environment or client."""
    # Try environment variable first
    project = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if project:
        return project

    # Try to get from BigQuery client
    try:
        client = bigquery.Client()
        return str(client.project)
    except Exception as e:
        raise ValueError("Unable to determine default BigQuery project") from e


# =========================
# Pretty printing + coloring
# =========================


def pretty_print_ddl(ddl: str) -> str:
    """Light post-formatter: add blank line before constraints and split long ALTER lines."""
    lines = ddl.splitlines()
    pretty: list[str] = []
    seen_body_close = False
    for line in lines:
        line = line.rstrip()

        if line.strip() == ")":
            seen_body_close = True

        # Separate constraints from CREATE block
        if line.startswith("ALTER TABLE") and seen_body_close:
            if pretty and pretty[-1]:
                pretty.append("")
            # Split "ALTER TABLE ... ADD ..." to two lines
            if " ADD " in line:
                table_part, constraint_part = line.split(" ADD ", 1)
                pretty.extend([table_part, f"  ADD {constraint_part}"])
                continue

        pretty.append(line)

    return "\n".join(pretty)


try:
    from pygments import highlight
    from pygments.formatters import Terminal256Formatter
    from pygments.lexers import SqlLexer

    _HAS_PYGMENTS = True
except Exception:
    _HAS_PYGMENTS = False

ANSI = {
    "reset": "\033[0m",
    "kw": "\033[1;34m",  # bold blue
    "ident": "\033[36m",  # cyan
    "string": "\033[32m",  # green
    "comment": "\033[2;37m",  # faint gray
    "type": "\033[35m",  # magenta
    "num": "\033[33m",  # yellow
}

KW_RE = re.compile(
    r"\b("
    r"CREATE|OR|REPLACE|TABLE|ALTER|ADD|PRIMARY|KEY|FOREIGN|REFERENCES|NOT|NULL|ENFORCED|"
    r"PARTITION|BY|RANGE_BUCKET|GENERATE_ARRAY|CLUSTER|OPTIONS|STRUCT|ARRAY|WITH|AS|"
    r"SELECT|FROM|WHERE|ORDER|GROUP|HAVING|AND|OR|ON|JOIN|LEFT|RIGHT|FULL|OUTER"
    r")\b",
    re.IGNORECASE,
)

TYPE_RE = re.compile(
    r"\b("
    r"INT64|NUMERIC|BIGNUMERIC|FLOAT64|BOOL|BOOLEAN|STRING|BYTES|DATE|DATETIME|TIME|TIMESTAMP|"
    r"GEOGRAPHY|JSON|INTERVAL"
    r")\b",
    re.IGNORECASE,
)

IDENT_RE = re.compile(r"`[^`]+`")
STRING_RE = re.compile(r"'([^'\\]|\\.)*'|\"([^\"\\]|\\.)*\"")
COMMENT_RE = re.compile(r"--.*?$", re.MULTILINE)
NUM_RE = re.compile(r"\b\d+\b")


def _fallback_color_sql(sql: str) -> str:
    def repl_comment(m):
        return f"{ANSI['comment']}{m.group(0)}{ANSI['reset']}"

    def repl_string(m):
        return f"{ANSI['string']}{m.group(0)}{ANSI['reset']}"

    def repl_ident(m):
        return f"{ANSI['ident']}{m.group(0)}{ANSI['reset']}"

    def repl_type(m):
        return f"{ANSI['type']}{m.group(0)}{ANSI['reset']}"

    def repl_num(m):
        return f"{ANSI['num']}{m.group(0)}{ANSI['reset']}"

    def repl_kw(m):
        return f"{ANSI['kw']}{m.group(0)}{ANSI['reset']}"

    s = COMMENT_RE.sub(repl_comment, sql)
    s = STRING_RE.sub(repl_string, s)
    s = IDENT_RE.sub(repl_ident, s)
    s = TYPE_RE.sub(repl_type, s)
    s = NUM_RE.sub(repl_num, s)
    s = KW_RE.sub(repl_kw, s)
    return s


def colorize_sql(sql: str, mode: str = "auto") -> str:
    """Colorize SQL.

    mode: 'auto' | 'always' | 'never'
    'auto' colors only when stdout is a TTY and NO_COLOR is not set.
    """
    want_color = True
    if mode == "never":
        want_color = False
    elif mode == "auto":
        if not sys.stdout.isatty() or os.getenv("NO_COLOR"):
            want_color = False

    if not want_color:
        return sql

    if _HAS_PYGMENTS:
        try:
            result = highlight(sql, SqlLexer(), Terminal256Formatter())
            return str(result)
        except Exception:
            # Fallback to non-highlighted SQL if pygments fails
            return _fallback_color_sql(sql)

    return _fallback_color_sql(sql)


# =========================
# INFORMATION_SCHEMA queries
# =========================

PK_SQL = """
SELECT
  tc.constraint_name,
  ARRAY_AGG(kcu.column_name ORDER BY kcu.ordinal_position) AS columns
FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS` tc
JOIN `{project}.{dataset}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` kcu
  ON tc.constraint_name = kcu.constraint_name
 AND tc.table_name     = kcu.table_name
 AND tc.table_schema   = kcu.table_schema
WHERE tc.table_name = @table_name
  AND tc.table_schema = @dataset_name
  AND tc.constraint_type = 'PRIMARY KEY'
GROUP BY tc.constraint_name
ORDER BY tc.constraint_name
"""

FK_SQL = """
SELECT
  tc.constraint_name,
  kcu.column_name AS column_name,
  ccu.table_schema AS referenced_dataset,
  ccu.table_name   AS referenced_table,
  ccu.column_name  AS referenced_column,
  kcu.ordinal_position
FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS` tc
JOIN `{project}.{dataset}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` kcu
  ON tc.constraint_name = kcu.constraint_name
 AND tc.table_name     = kcu.table_name
 AND tc.table_schema   = kcu.table_schema
JOIN `{project}.{dataset}.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE` ccu
  ON tc.constraint_name = ccu.constraint_name
WHERE tc.table_name   = @table_name
  AND tc.table_schema = @dataset_name
  AND tc.constraint_type = 'FOREIGN KEY'
ORDER BY tc.constraint_name, kcu.ordinal_position
"""

# Batch query for multiple tables
BATCH_CONSTRAINTS_SQL = """
WITH all_constraints AS (
  SELECT
    tc.table_name,
    tc.constraint_name,
    tc.constraint_type,
    ARRAY_AGG(kcu.column_name ORDER BY kcu.ordinal_position) AS columns,
    ARRAY_AGG(STRUCT(
      ccu.table_schema AS referenced_dataset,
      ccu.table_name AS referenced_table,
      ccu.column_name AS referenced_column
    ) ORDER BY kcu.ordinal_position) AS references
  FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS` tc
  JOIN `{project}.{dataset}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` kcu
    ON tc.constraint_name = kcu.constraint_name
   AND tc.table_name = kcu.table_name
   AND tc.table_schema = kcu.table_schema
  LEFT JOIN `{project}.{dataset}.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE` ccu
    ON tc.constraint_name = ccu.constraint_name
  WHERE tc.table_name IN UNNEST(@table_names)
    AND tc.table_schema = @dataset_name
    AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')
  GROUP BY tc.table_name, tc.constraint_name, tc.constraint_type
)
SELECT * FROM all_constraints
ORDER BY table_name, constraint_type, constraint_name
"""


def get_constraints(
    client: bigquery.Client, project_id: str, dataset_id: str, table_id: str
) -> tuple[list[str], list[dict[str, Any]]]:
    """Retrieve PK and FK constraints using INFORMATION_SCHEMA with.

    parameters.
    """
    try:
        job = client.query(
            PK_SQL.format(project=project_id, dataset=dataset_id),
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("table_name", "STRING", table_id),
                    bigquery.ScalarQueryParameter("dataset_name", "STRING", dataset_id),
                ]
            ),
        )
        pk_rows = list(job.result())
        primary_keys = pk_rows[0].columns if pk_rows else []

        job = client.query(
            FK_SQL.format(project=project_id, dataset=dataset_id),
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("table_name", "STRING", table_id),
                    bigquery.ScalarQueryParameter("dataset_name", "STRING", dataset_id),
                ]
            ),
        )
        foreign_keys: list[dict[str, Any]] = []
        for r in job.result():
            foreign_keys.append(
                {
                    "constraint_name": r["constraint_name"],
                    "column": r["column_name"],
                    "referenced_dataset": r["referenced_dataset"] or dataset_id,
                    "referenced_table": r["referenced_table"] or "UNKNOWN",
                    "referenced_column": r["referenced_column"] or "UNKNOWN",
                }
            )
        return primary_keys, foreign_keys
    except Exception as e:
        print(f"Error retrieving constraints for {dataset_id}.{table_id}: {e}")
        return [], []


def get_batch_constraints(
    client: bigquery.Client, project_id: str, dataset_id: str, table_ids: list[str]
) -> dict[str, tuple[list[str], list[dict[str, Any]]]]:
    """Retrieve constraints for multiple tables in a single query."""
    if not table_ids:
        return {}

    try:
        job = client.query(
            BATCH_CONSTRAINTS_SQL.format(project=project_id, dataset=dataset_id),
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("table_names", "STRING", table_ids),
                    bigquery.ScalarQueryParameter("dataset_name", "STRING", dataset_id),
                ]
            ),
        )

        results: dict[str, tuple[list[str], list[dict[str, Any]]]] = {}
        for table_id in table_ids:
            results[table_id] = ([], [])

        for row in job.result():
            table_name = row["table_name"]
            constraint_type = row["constraint_type"]

            if constraint_type == "PRIMARY KEY":
                results[table_name] = (row["columns"], results[table_name][1])
            elif constraint_type == "FOREIGN KEY":
                fks = results[table_name][1]
                for i, col in enumerate(row["columns"]):
                    ref = row["references"][i]
                    fks.append(
                        {
                            "constraint_name": row["constraint_name"],
                            "column": col,
                            "referenced_dataset": ref["referenced_dataset"]
                            or dataset_id,
                            "referenced_table": ref["referenced_table"] or "UNKNOWN",
                            "referenced_column": ref["referenced_column"] or "UNKNOWN",
                        }
                    )

        return results
    except Exception as e:
        print(f"Error retrieving batch constraints: {e}")
        return {table_id: ([], []) for table_id in table_ids}


# =========================
# DDL rendering helpers
# =========================

INDENT = "  "  # two spaces per level

SCALAR_TYPE_MAP = {
    "FLOAT": "FLOAT64",  # Normalize for BigQuery DDL
}


def _render_scalar_type(bq_type: str) -> str:
    return SCALAR_TYPE_MAP.get(bq_type, bq_type)


def _render_col_options(field: SchemaField) -> str:
    opts = []
    if field.description:
        desc = field.description.replace('"', r"\"")
        opts.append(f'description="{desc}"')
    # You can add policy_tags, column security, etc. here.
    if not opts:
        return ""
    return f" OPTIONS({', '.join(opts)})"


def _render_not_null(field: SchemaField) -> str:
    return " NOT NULL" if field.mode == "REQUIRED" else ""


def _render_struct_type(fields: list[SchemaField], level: int) -> str:
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
        field_type_rendered = _render_type_for_field(f, level + 1)  # may be multiline
        # Column-level details (NOT NULL / OPTIONS) apply to STRUCT member in-place
        tail = f"{_render_not_null(f)}{_render_col_options(f)}"
        inner_lines.append(
            f"{INDENT * (level + 1)}`{f.name}` {field_type_rendered}{tail}"
        )
        if idx < len(fields) - 1:
            inner_lines[-1] = inner_lines[-1] + ","
    struct_block = "STRUCT<\n" + "\n".join(inner_lines) + f"\n{INDENT * level}>"
    return struct_block


def _render_array_of_struct_type(field: SchemaField, level: int) -> str:
    """ARRAY<STRUCT<...>> with multiline STRUCT payload:

    ARRAY<STRUCT<
      ...
    >>
    """
    payload = _render_struct_type(field.fields, level + 1)
    return "ARRAY<" + "\n".join(payload.splitlines()) + f"\n{INDENT * level}>"


def _render_type_for_field(field: SchemaField, level: int) -> str:
    """Return the type string for a SchemaField, possibly multiline for nested.

    types.
    """
    if field.field_type == "RECORD":
        if field.mode == "REPEATED":
            return _render_array_of_struct_type(field, level)
        return _render_struct_type(field.fields, level)
    else:
        base = _render_scalar_type(field.field_type)
        if field.mode == "REPEATED":
            return f"ARRAY<{base}>"
        return base


def _render_column(field: SchemaField, level: int) -> str:
    """Render a single top-level column, expanding nested types cleanly.

    Example:
      `payload` STRUCT<
        `a` INT64,
        `b` ARRAY<STRUCT<
          `x` STRING
        >>
      > NOT NULL OPTIONS(...)
    """
    type_str = _render_type_for_field(field, level)
    tail = f"{_render_not_null(field)}{_render_col_options(field)}"
    return f"{INDENT * level}`{field.name}` {type_str}{tail}"


def _render_columns(schema: list[SchemaField]) -> str:
    lines: list[str] = []
    for i, f in enumerate(schema):
        col = _render_column(f, 1)  # columns start indented once
        if i < len(schema) - 1:
            lines.append(col + ",")
        else:
            lines.append(col)
    return "\n".join(lines)


def _render_partitioning(tbl: bigquery.Table) -> str | None:
    tp = tbl.time_partitioning
    rp = getattr(tbl, "range_partitioning", None)

    if rp:
        col = rp.field
        start = rp.range_.start
        end = rp.range_.end
        interval = rp.range_.interval
        return f"PARTITION BY RANGE_BUCKET(`{col}`, GENERATE_ARRAY({start}, {end}, {interval}))"

    if tp:
        if tp.field:
            field = tp.field
            try:
                col = next((c for c in tbl.schema if c.name == field), None)
                if col and col.field_type == "DATE":
                    part_expr = f"`{field}`"
                else:
                    part_expr = f"DATE(`{field}`)"
            except Exception:
                part_expr = f"DATE(`{field}`)"
            line = f"PARTITION BY {part_expr}"
        else:
            line = "PARTITION BY DATE(_PARTITIONTIME)"

        extras = []
        if tp.require_partition_filter:
            extras.append("require_partition_filter=true")
        if tp.expiration_ms is not None:
            days = int(tp.expiration_ms // 1000 // 60 // 60 // 24)
            extras.append(f"partition_expiration_days={days}")

        if extras:
            line += f"\nOPTIONS({', '.join(extras)})"
        return line

    return None


def _render_clustering(tbl: bigquery.Table) -> str | None:
    if tbl.clustering_fields:
        fields = ", ".join(f"`{f}`" for f in tbl.clustering_fields)
        return f"CLUSTER BY {fields}"
    return None


def _render_table_options(tbl: bigquery.Table) -> str | None:
    opts = []
    if tbl.description:
        desc_escaped = tbl.description.replace('"', r"\"")
        opts.append(f'description="{desc_escaped}"')
    # You can emit labels here if desired.
    return f"OPTIONS({', '.join(opts)})" if opts else None


# =========================
# DDL generator
# =========================


def generate_table_ddl(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_id: str,
    include_constraints: bool = True,
) -> str:
    """Generate DDL for a single BigQuery table."""
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    tbl: bigquery.Table = client.get_table(table_ref)

    create_header = f"CREATE OR REPLACE TABLE `{table_ref}` ("
    columns_block = _render_columns(tbl.schema)
    closing = ")"

    ddl_lines: list[str] = [create_header, columns_block, closing]

    part = _render_partitioning(tbl)
    if part:
        ddl_lines.append(part)
    clus = _render_clustering(tbl)
    if clus:
        ddl_lines.append(clus)
    topts = _render_table_options(tbl)
    if topts:
        ddl_lines.append(topts)

    if include_constraints:
        pk_cols, fks = get_constraints(client, project_id, dataset_id, table_id)
        if pk_cols:
            cols = ", ".join(f"`{c}`" for c in pk_cols)
            ddl_lines.append(
                f"ALTER TABLE `{table_ref}` ADD PRIMARY KEY ({cols}) NOT ENFORCED"
            )

        for fk in fks:
            ref = f"{project_id}.{fk['referenced_dataset']}.{fk['referenced_table']}"
            ddl_lines.append(
                f"ALTER TABLE `{table_ref}` ADD FOREIGN KEY (`{fk['column']}`) "
                f"REFERENCES `{ref}`(`{fk['referenced_column']}`) NOT ENFORCED"
            )

    ddl = (
        "\n".join(ddl_lines)
        + ";\n"
        + f"-- End of script generated at {datetime.now():%Y-%m-%d %H:%M:%S}"
    )
    return ddl


def generate_dataset_ddl(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_ids: list[str] | None = None,
    include_constraints: bool = True,
) -> dict[str, str]:
    """Generate DDL for multiple tables in a dataset."""
    if table_ids is None:
        # Get all tables in dataset
        dataset_ref = client.dataset(dataset_id, project=project_id)
        tables = list(client.list_tables(dataset_ref))
        table_ids = [table.table_id for table in tables]

    if not table_ids:
        return {}

    # Get batch constraints if needed
    constraints_map = {}
    if include_constraints:
        constraints_map = get_batch_constraints(
            client, project_id, dataset_id, table_ids
        )

    ddls = {}
    for table_id in table_ids:
        try:
            table_ref = f"{project_id}.{dataset_id}.{table_id}"
            tbl: bigquery.Table = client.get_table(table_ref)

            create_header = f"CREATE OR REPLACE TABLE `{table_ref}` ("
            columns_block = _render_columns(tbl.schema)
            closing = ")"

            ddl_lines: list[str] = [create_header, columns_block, closing]

            part = _render_partitioning(tbl)
            if part:
                ddl_lines.append(part)
            clus = _render_clustering(tbl)
            if clus:
                ddl_lines.append(clus)
            topts = _render_table_options(tbl)
            if topts:
                ddl_lines.append(topts)

            if include_constraints and table_id in constraints_map:
                pk_cols, fks = constraints_map[table_id]
                if pk_cols:
                    cols = ", ".join(f"`{c}`" for c in pk_cols)
                    ddl_lines.append(
                        f"ALTER TABLE `{table_ref}` ADD PRIMARY KEY ({cols}) NOT ENFORCED"
                    )

                for fk in fks:
                    ref = f"{project_id}.{fk['referenced_dataset']}.{fk['referenced_table']}"
                    ddl_lines.append(
                        f"ALTER TABLE `{table_ref}` ADD FOREIGN KEY (`{fk['column']}`) "
                        f"REFERENCES `{ref}`(`{fk['referenced_column']}`) NOT ENFORCED"
                    )

            ddl = (
                "\n".join(ddl_lines)
                + ";\n"
                + f"-- End of script generated at {datetime.now():%Y-%m-%d %H:%M:%S}"
            )
            ddls[table_id] = ddl

        except Exception as e:
            print(f"Error generating DDL for {table_id}: {e}")
            ddls[table_id] = f"-- Error: {e}"

    return ddls


# =========================
# Schema extraction for schema-diff integration
# =========================


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
            base_type = _map_bq_type_to_internal(field.field_type)
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
    tree = _normalize_bigquery_arrays(tree)

    return tree, required_paths


def _normalize_bigquery_arrays(tree: Any) -> Any:
    """Normalize BigQuery array wrapper patterns like {'list': [{'element': ...}]} to
    [...].

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
                normalized[key] = [_normalize_bigquery_arrays(element_structure)]
            else:
                # Recursively normalize nested structures
                normalized[key] = _normalize_bigquery_arrays(value)
        return normalized
    elif isinstance(tree, list):
        # Normalize list elements
        return [_normalize_bigquery_arrays(item) for item in tree]
    else:
        # Return primitive values as-is
        return tree


def _map_bq_type_to_internal(bq_type: str) -> str:
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
