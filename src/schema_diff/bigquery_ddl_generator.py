#!/usr/bin/env python3
"""BigQuery DDL generation with formatting and colorization.

This module provides functionality for:
- Generating CREATE TABLE DDL statements
- Batch DDL generation for entire datasets
- SQL syntax highlighting and formatting
- Table options, partitioning, and clustering
"""
from __future__ import annotations

import logging
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any

from google.cloud import bigquery

# Import configuration
from schema_diff import analyze_config

# Import from other modules
from .bigquery_queries import get_batch_constraints
from .bigquery_schema import render_columns

logger = logging.getLogger(__name__)


# =============================================================================
# DDL Pretty Printing and Colorization
# =============================================================================


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


# Check for pygments availability
try:
    from pygments import highlight
    from pygments.formatters import Terminal256Formatter
    from pygments.lexers import SqlLexer

    _HAS_PYGMENTS = True
except Exception:
    _HAS_PYGMENTS = False

# ANSI color codes for fallback colorization
ANSI = {
    "reset": "\033[0m",
    "kw": "\033[1;34m",  # bold blue
    "ident": "\033[36m",  # cyan
    "string": "\033[32m",  # green
    "comment": "\033[2;37m",  # faint gray
    "type": "\033[35m",  # magenta
    "num": "\033[33m",  # yellow
}

# Regular expressions for syntax highlighting
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
    """Fallback SQL colorizer using regex (when pygments is not available)."""

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
    """Colorize SQL DDL statements.

    Args:
        sql: SQL DDL string to colorize
        mode: Color mode - 'auto' (colors only when stdout is TTY),
              'always' (always colorize), or 'never' (never colorize)

    Returns:
        Colorized SQL string (or plain SQL if coloring is disabled)
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
            # Fallback to regex-based colorization if pygments fails
            return _fallback_color_sql(sql)

    return _fallback_color_sql(sql)


# =============================================================================
# Table Options and Clauses
# =============================================================================


def collect_table_options(tbl: bigquery.Table) -> dict[str, Any]:
    """Collect all table-level options (description, partition settings, labels, etc.)."""
    opts: dict[str, Any] = {}

    # Table description
    if tbl.description:
        opts["description"] = (
            tbl.description.replace("\\", "\\\\")
            .replace('"', r"\"")
            .replace("\n", r"\n")
        )

    # Partition options belong in the same OPTIONS(...) block
    tp = tbl.time_partitioning
    if tp:
        if tp.require_partition_filter:
            opts["require_partition_filter"] = True
        if tp.expiration_ms is not None:
            days = int(tp.expiration_ms // 1000 // 60 // 60 // 24)
            opts["partition_expiration_days"] = days

    # Labels (if present)
    if getattr(tbl, "labels", None):
        # BigQuery Table.labels is a dict[str, str]
        opts["labels"] = dict(tbl.labels)

    return opts


def render_options_line(opts: dict[str, Any]) -> str | None:
    """Render a single OPTIONS(...) block with all options (deterministic label order)."""
    if not opts:
        return None

    pairs: list[str] = []
    for k, v in opts.items():
        if k == "labels" and isinstance(v, dict):
            # labels=[("k","v"),("k2","v2")] - sorted for stable output
            # Note: BigQuery labels are restricted to [a-z0-9_-] so no escaping needed
            items = ", ".join(f'("{lk}","{lv}")' for lk, lv in sorted(v.items()))
            pairs.append(f"labels=[{items}]")
        elif isinstance(v, bool):
            pairs.append(f"{k}={'TRUE' if v else 'FALSE'}")
        elif isinstance(v, (int, float)):
            pairs.append(f"{k}={v}")
        else:
            pairs.append(f'{k}="{v}"')

    return f"OPTIONS({', '.join(pairs)})"


def render_partitioning(tbl: bigquery.Table) -> str | None:
    """Render PARTITION BY clause (without OPTIONS - those go in consolidated block)."""
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
            return f"PARTITION BY {part_expr}"
        else:
            # Ingestion-time partitioning - use _PARTITIONDATE (cleaner than DATE(_PARTITIONTIME))
            return "PARTITION BY _PARTITIONDATE"

    return None


def render_clustering(tbl: bigquery.Table) -> str | None:
    """Render CLUSTER BY clause."""
    if tbl.clustering_fields:
        fields = ", ".join(f"`{f}`" for f in tbl.clustering_fields)
        return f"CLUSTER BY {fields}"
    return None


# =============================================================================
# DDL Generation
# =============================================================================


def generate_table_ddl(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_id: str,
    include_constraints: bool = True,
    constraint_getter: Any = None,
) -> str:
    """Generate DDL for a single BigQuery table.

    Args:
        client: BigQuery client instance
        project_id: GCP project ID
        dataset_id: Dataset ID
        table_id: Table ID
        include_constraints: Whether to include PK/FK constraints
        constraint_getter: Optional function to retrieve constraints (for dependency injection)

    Returns:
        DDL string for the table
    """
    from .bigquery_queries import get_constraints as _default_constraints_getter

    if constraint_getter is None:
        constraint_getter = _default_constraints_getter

    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    tbl: bigquery.Table = client.get_table(table_ref)

    create_header = f"CREATE OR REPLACE TABLE `{table_ref}` ("
    columns_block = render_columns(tbl.schema)
    closing = ")"

    create_lines: list[str] = [create_header, columns_block, closing]

    # Optional clauses after CREATE body
    part = render_partitioning(tbl)
    clus = render_clustering(tbl)
    opts_dict = collect_table_options(tbl)
    opts_line = render_options_line(opts_dict)

    if part:
        create_lines.append(part)
    if clus:
        create_lines.append(clus)
    if opts_line:
        create_lines.append(opts_line)

    create_stmt = "\n".join(create_lines) + ";"

    alter_stmts: list[str] = []
    if include_constraints:
        pk_dict, fks = constraint_getter(client, project_id, dataset_id, table_id)

        # Render PRIMARY KEY with constraint name
        if pk_dict:
            cols = ", ".join(f"`{c}`" for c in pk_dict["columns"])
            pk_name = pk_dict["constraint_name"]
            alter_stmts.append(
                f"ALTER TABLE `{table_ref}` ADD CONSTRAINT `{pk_name}` PRIMARY KEY ({cols}) NOT ENFORCED;"
            )

        # Render FOREIGN KEYs with constraint names
        for fk in fks:
            fk_name = fk["constraint_name"]
            ref = f"{project_id}.{fk['referenced_dataset']}.{fk['referenced_table']}"
            if "columns" in fk and "referenced_columns" in fk:
                cols = ", ".join(f"`{c}`" for c in fk["columns"])
                ref_cols = ", ".join(f"`{c}`" for c in fk["referenced_columns"])
                alter_stmts.append(
                    f"ALTER TABLE `{table_ref}` ADD CONSTRAINT `{fk_name}` FOREIGN KEY ({cols}) REFERENCES `{ref}`({ref_cols}) NOT ENFORCED;"
                )
            else:
                # Back-compat (single-column shape)
                alter_stmts.append(
                    f"ALTER TABLE `{table_ref}` ADD CONSTRAINT `{fk_name}` FOREIGN KEY (`{fk['column']}`) "
                    f"REFERENCES `{ref}`(`{fk['referenced_column']}`) NOT ENFORCED;"
                )

    ddl = "\n\n".join(
        [create_stmt]
        + alter_stmts
        + [f"-- End of script generated at {datetime.now():%Y-%m-%d %H:%M:%S}"]
    )
    return ddl


def generate_dataset_ddl(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_ids: list[str] | None = None,
    include_constraints: bool = True,
    parallel: bool = True,
) -> dict[str, str]:
    """Generate DDL for multiple tables in a dataset.

    Args:
        client: BigQuery client instance
        project_id: GCP project ID
        dataset_id: Dataset ID
        table_ids: Optional list of table IDs to generate DDL for (None = all tables)
        include_constraints: Whether to include PK/FK constraints
        parallel: Whether to use parallel processing (default: True, 10-20x faster)

    Returns:
        Dictionary mapping table_id to DDL string
    """
    if table_ids is None:
        # Get all tables in dataset
        dataset_ref = client.dataset(dataset_id, project=project_id)
        tables = list(client.list_tables(dataset_ref))
        table_ids = [table.table_id for table in tables]

    if not table_ids:
        return {}

    # Get batch constraints if needed (done once for all tables)
    constraints_map: dict[str, tuple[dict[str, Any] | None, list[dict[str, Any]]]] = {}
    if include_constraints:
        constraints_map = get_batch_constraints(
            client, project_id, dataset_id, table_ids
        )

    def _one_table_ddl(table_id: str) -> tuple[str, str]:
        """Generate DDL for a single table (safe for parallel execution)."""
        try:
            table_ref = f"{project_id}.{dataset_id}.{table_id}"
            tbl: bigquery.Table = client.get_table(table_ref)

            create_header = f"CREATE OR REPLACE TABLE `{table_ref}` ("
            columns_block = render_columns(tbl.schema)
            closing = ")"

            create_lines: list[str] = [create_header, columns_block, closing]

            part = render_partitioning(tbl)
            clus = render_clustering(tbl)
            opts_dict = collect_table_options(tbl)
            opts_line = render_options_line(opts_dict)

            if part:
                create_lines.append(part)
            if clus:
                create_lines.append(clus)
            if opts_line:
                create_lines.append(opts_line)

            create_stmt = "\n".join(create_lines) + ";"

            alter_stmts: list[str] = []
            if include_constraints and table_id in constraints_map:
                pk_dict, fks = constraints_map[table_id]

                # Render PRIMARY KEY with constraint name
                if pk_dict:
                    cols = ", ".join(f"`{c}`" for c in pk_dict["columns"])
                    pk_name = pk_dict["constraint_name"]
                    alter_stmts.append(
                        f"ALTER TABLE `{table_ref}` ADD CONSTRAINT `{pk_name}` PRIMARY KEY ({cols}) NOT ENFORCED;"
                    )

                # Render FOREIGN KEYs with constraint names
                for fk in fks:
                    fk_name = fk["constraint_name"]
                    # Preserve cross-dataset/project references
                    ref_proj = project_id  # Default to same project
                    ref_ds = fk.get("referenced_dataset") or dataset_id
                    ref_tbl = fk.get("referenced_table") or "UNKNOWN"
                    ref = f"{ref_proj}.{ref_ds}.{ref_tbl}"

                    if "columns" in fk and "referenced_columns" in fk:
                        cols = ", ".join(f"`{c}`" for c in fk["columns"])
                        ref_cols = ", ".join(f"`{c}`" for c in fk["referenced_columns"])
                        alter_stmts.append(
                            f"ALTER TABLE `{table_ref}` ADD CONSTRAINT `{fk_name}` FOREIGN KEY ({cols}) REFERENCES `{ref}`({ref_cols}) NOT ENFORCED;"
                        )
                    else:
                        # Back-compat for single-column shape
                        alter_stmts.append(
                            f"ALTER TABLE `{table_ref}` ADD CONSTRAINT `{fk_name}` FOREIGN KEY (`{fk['column']}`) "
                            f"REFERENCES `{ref}`(`{fk['referenced_column']}`) NOT ENFORCED;"
                        )

            ddl = "\n\n".join(
                [create_stmt]
                + alter_stmts
                + [f"-- End of script generated at {datetime.now():%Y-%m-%d %H:%M:%S}"]
            )
            return table_id, ddl

        except Exception as e:
            logger.error(
                "Failed to generate DDL for %s.%s.%s: %s",
                project_id,
                dataset_id,
                table_id,
                e,
                exc_info=logger.isEnabledFor(logging.DEBUG),
            )
            return table_id, f"-- Error generating DDL: {e}"

    # Execute in parallel or sequential based on flag
    ddls: dict[str, str] = {}

    if parallel and len(table_ids) > 1:
        # Parallel execution with thread pool (10-20x faster for large datasets)
        max_workers = min(analyze_config.PARALLEL_WORKERS, len(table_ids))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_one_table_ddl, tid) for tid in table_ids]
            for future in as_completed(futures):
                tid, ddl = future.result()
                ddls[tid] = ddl
    else:
        # Sequential execution (for single table or when parallel is disabled)
        for table_id in table_ids:
            tid, ddl = _one_table_ddl(table_id)
            ddls[tid] = ddl

    return ddls


__all__ = [
    # Pretty printing and colorization
    "pretty_print_ddl",
    "colorize_sql",
    # Table options and clauses
    "collect_table_options",
    "render_options_line",
    "render_partitioning",
    "render_clustering",
    # DDL generation
    "generate_table_ddl",
    "generate_dataset_ddl",
]
