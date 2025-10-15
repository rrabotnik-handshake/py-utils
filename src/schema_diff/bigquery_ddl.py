#!/usr/bin/env python3
"""BigQuery DDL generator with live table schema extraction.

This module provides:
- Live BigQuery table schema extraction
- Pretty DDL generation with nested types
- Constraint handling (PK/FK)
- Syntax highlighting and formatting
- Integration with schema-diff comparison workflow
- DDL anti-pattern detection (unnecessary STRUCT wrappers)
"""
from __future__ import annotations

import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Callable, TypeVar

from google.api_core import exceptions as gcp_exceptions
from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField

# Import all configuration from centralized module
from schema_diff import analyze_config

logger = logging.getLogger(__name__)

T = TypeVar("T")


# --- Retry helper for transient BigQuery errors ------------------------------


def _retry_on_transient(
    fn: Callable[[], T],
    max_attempts: int = analyze_config.RETRY_MAX_ATTEMPTS,
    initial_delay: float = analyze_config.RETRY_INITIAL_DELAY,
    backoff_multiplier: float = analyze_config.RETRY_BACKOFF_MULTIPLIER,
    operation_name: str = "BigQuery operation",
) -> T:
    """Retry function on transient BigQuery errors (429, 500, 503).

    Args:
        fn: Function to execute (no arguments)
        max_attempts: Maximum number of attempts (default: 3)
        initial_delay: Initial delay in seconds (default: 1.0)
        backoff_multiplier: Backoff multiplier for exponential delay (default: 2.0)
        operation_name: Description for logging

    Returns:
        Result of fn()

    Raises:
        Last exception if all retries exhausted
    """
    delay = initial_delay

    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except (
            gcp_exceptions.TooManyRequests,  # 429
            gcp_exceptions.InternalServerError,  # 500
            gcp_exceptions.ServiceUnavailable,  # 503
            gcp_exceptions.DeadlineExceeded,  # Timeout
        ) as e:
            if attempt == max_attempts:
                logger.error(
                    "%s failed after %d attempts: %s",
                    operation_name,
                    max_attempts,
                    e,
                )
                raise

            logger.warning(
                "%s failed (attempt %d/%d), retrying in %.1fs: %s",
                operation_name,
                attempt,
                max_attempts,
                delay,
                e,
            )
            time.sleep(delay)
            delay *= backoff_multiplier

    # Should never reach here (all paths return or raise), but satisfy linter
    raise RuntimeError(f"{operation_name}: Exhausted retries without raising exception")


# --- Tokenization & matching helpers -----------------------------------------
# (Using PII_INDICATORS from bq_config)

_CAMEL_SPLIT_RE = re.compile(r"(?<!^)(?=[A-Z])")


def _tokenize_name(name: str) -> list[str]:
    """Split field name into lowercase tokens (handles snake & camelCase)."""
    n = name or ""
    # replace non-alnum with underscores, then split
    base = re.sub(r"[^0-9A-Za-z]+", "_", n)
    parts = []
    for p in base.split("_"):
        if not p:
            continue
        # split camelCase chunks too
        parts.extend(_CAMEL_SPLIT_RE.sub("_", p).lower().split("_"))
    # drop purely numeric tokens
    return [t for t in parts if t and not t.isdigit()]


def _indicator_tokens(indicator: str) -> list[str]:
    """Turn an indicator like 'credit_card' into tokens ['credit','card']."""
    return _tokenize_name(indicator)


def _matches_indicator(tokens: set[str], indicator: str) -> bool:
    """True if all tokens of the indicator are present in name tokens."""
    itoks = _indicator_tokens(indicator)
    return all(t in tokens for t in itoks)


def _classify_pii_by_name(field_name: str) -> dict[str, list[str]]:
    """Return {category: [matched_indicator,...]} for the field name."""
    tokens = set(_tokenize_name(field_name))
    hits: dict[str, list[str]] = {}
    for cat, indicators in analyze_config.PII_INDICATORS.items():
        matched = [i for i in indicators if _matches_indicator(tokens, i)]
        if matched:
            hits[cat] = matched
    return hits


def _policy_tag_names_on_field(f: SchemaField) -> list[str]:
    """Extract policy tag names from a BigQuery SchemaField."""
    pt = getattr(f, "policy_tags", None)
    names = getattr(pt, "names", None)
    if names is None and isinstance(pt, (list, tuple, set)):
        names = list(pt)
    if names is None and isinstance(pt, dict):
        names = pt.get("names") or pt.get("policy_tags")
    if names is None:
        try:
            names = list(pt) if pt is not None else []
        except Exception:
            names = []
    return [str(n).strip() for n in (names or []) if str(n).strip()]


# Shorter alias for internal use
_policy_tag_names = _policy_tag_names_on_field


# --- All anti-pattern detection configuration is in analyze_config.py -------------


# =============================================================================
# SQL Query Templates (BigQuery INFORMATION_SCHEMA queries)
# =============================================================================

# Primary key query
PK_SQL = """
SELECT
  tc.constraint_name,
  ARRAY_AGG(k.column_name ORDER BY k.ordinal_position) AS columns
FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS` tc
JOIN `{project}.{dataset}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` k
  ON tc.constraint_name = k.constraint_name
 AND tc.table_name = k.table_name
 AND tc.table_schema = k.table_schema
WHERE tc.table_name = @table_name
  AND tc.table_schema = @dataset_name
  AND tc.constraint_type = 'PRIMARY KEY'
GROUP BY tc.constraint_name
"""

# Foreign key query (with composite key support)
FK_SQL = """
-- Accurate FK mapping using REFERENTIAL_CONSTRAINTS to handle composite keys
WITH
  fk_cols AS (
    SELECT
      k.table_schema, k.table_name, k.constraint_name, k.column_name AS fk_column, k.ordinal_position
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` k
    WHERE k.table_name = @table_name AND k.table_schema = @dataset_name
  ),
  fk_to_pk AS (
    SELECT
      rc.constraint_schema, rc.constraint_name, rc.unique_constraint_schema AS pk_schema, rc.unique_constraint_name AS pk_constraint_name
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS` rc
    WHERE rc.constraint_schema = @dataset_name
  ),
  pk_cols AS (
    SELECT
      k.constraint_schema, k.constraint_name, k.column_name AS pk_column, k.table_schema AS pk_table_schema, k.table_name AS pk_table_name, k.ordinal_position
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` k
  )
SELECT
  f.constraint_name, f.fk_column AS column_name, p.pk_table_schema AS referenced_dataset, p.pk_table_name AS referenced_table, p.pk_column AS referenced_column, f.ordinal_position
FROM fk_cols f
JOIN fk_to_pk m ON f.constraint_name = m.constraint_name AND f.table_schema = m.constraint_schema
JOIN pk_cols p ON p.constraint_name = m.pk_constraint_name AND p.constraint_schema = m.pk_schema AND p.ordinal_position = f.ordinal_position
ORDER BY f.constraint_name, f.ordinal_position
"""

# Batch constraints query (multiple tables at once)
BATCH_CONSTRAINTS_SQL = """
WITH
  fk_cols AS (
    SELECT
      k.table_name, k.table_schema, k.constraint_name, k.column_name AS fk_column, k.ordinal_position
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` k
    WHERE k.table_name IN UNNEST(@table_names) AND k.table_schema = @dataset_name
  ),
  fk_to_pk AS (
    SELECT
      rc.constraint_schema, rc.constraint_name, rc.unique_constraint_schema AS pk_schema, rc.unique_constraint_name AS pk_constraint_name
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS` rc
    WHERE rc.constraint_schema = @dataset_name
  ),
  pk_cols AS (
    SELECT
      k.constraint_schema, k.constraint_name, k.column_name AS pk_column, k.table_schema AS pk_table_schema, k.table_name AS pk_table_name, k.ordinal_position
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` k
  ),
  resolved_fks AS (
    SELECT
      f.table_name, f.constraint_name,
      ARRAY_AGG(f.fk_column ORDER BY f.ordinal_position) AS fk_columns,
      ARRAY_AGG(STRUCT(p.pk_table_schema AS referenced_dataset, p.pk_table_name AS referenced_table, p.pk_column AS referenced_column) ORDER BY f.ordinal_position) AS refs
    FROM fk_cols f
    JOIN fk_to_pk m ON f.constraint_name = m.constraint_name AND f.table_schema = m.constraint_schema
    JOIN pk_cols p ON p.constraint_name = m.pk_constraint_name AND p.constraint_schema = m.pk_schema AND p.ordinal_position = f.ordinal_position
    GROUP BY f.table_name, f.constraint_name
  ),
  all_constraints AS (
    SELECT
      r.table_name, r.constraint_name, 'FOREIGN KEY' AS constraint_type, r.fk_columns AS columns, r.refs AS references
    FROM resolved_fks r
    UNION ALL
    SELECT
      tc.table_name, tc.constraint_name, 'PRIMARY KEY' AS constraint_type,
      ARRAY_AGG(k.column_name ORDER BY k.ordinal_position) AS columns,
      ARRAY<STRUCT<referenced_dataset STRING, referenced_table STRING, referenced_column STRING>>[] AS references
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS` tc
    JOIN `{project}.{dataset}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` k ON tc.constraint_name = k.constraint_name AND tc.table_name = k.table_name AND tc.table_schema = k.table_schema
    WHERE tc.table_name IN UNNEST(@table_names) AND tc.table_schema = @dataset_name AND tc.constraint_type = 'PRIMARY KEY'
    GROUP BY tc.table_name, tc.constraint_name
  )
SELECT * FROM all_constraints
ORDER BY table_name, constraint_type, constraint_name
"""


def get_default_project() -> str:
    """Get the default BigQuery project from environment or client."""
    # Try environment variables first (both common variants)
    project = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")
    if project:
        return project

    # Try to get from BigQuery client
    try:
        client = bigquery.Client()
        return str(client.project)
    except Exception as e:
        raise ValueError(
            "Unable to determine default BigQuery project (no GOOGLE_CLOUD_PROJECT and client has no project)"
        ) from e


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
-- Accurate FK mapping using REFERENTIAL_CONSTRAINTS to handle composite keys
WITH
  fk_cols AS (
    SELECT
      k.table_schema,
      k.table_name,
      k.constraint_name,
      k.column_name AS fk_column,
      k.ordinal_position
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` k
    WHERE k.table_name = @table_name
      AND k.table_schema = @dataset_name
  ),
  fk_to_pk AS (
    SELECT
      rc.constraint_schema,
      rc.constraint_name,
      rc.unique_constraint_schema AS pk_schema,
      rc.unique_constraint_name    AS pk_constraint_name
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS` rc
    WHERE rc.constraint_schema = @dataset_name
  ),
  pk_cols AS (
    SELECT
      k.constraint_schema,
      k.constraint_name,
      k.column_name AS pk_column,
      k.table_schema AS pk_table_schema,
      k.table_name   AS pk_table_name,
      k.ordinal_position
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` k
  )
SELECT
  f.constraint_name,
  f.fk_column               AS column_name,
  p.pk_table_schema         AS referenced_dataset,
  p.pk_table_name           AS referenced_table,
  p.pk_column               AS referenced_column,
  f.ordinal_position
FROM fk_cols f
JOIN fk_to_pk m
  ON f.constraint_name   = m.constraint_name
 AND f.table_schema      = m.constraint_schema
JOIN pk_cols p
  ON p.constraint_name   = m.pk_constraint_name
 AND p.constraint_schema = m.pk_schema
 AND p.ordinal_position  = f.ordinal_position
ORDER BY f.constraint_name, f.ordinal_position
"""

# Batch query for multiple tables - uses REFERENTIAL_CONSTRAINTS for accurate FK mapping
BATCH_CONSTRAINTS_SQL = """
WITH
  fk_cols AS (
    SELECT
      k.table_name,
      k.constraint_name,
      k.column_name AS fk_column,
      k.table_schema,
      k.ordinal_position
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` k
    WHERE k.table_name IN UNNEST(@table_names)
      AND k.table_schema = @dataset_name
  ),
  fk_to_pk AS (
    SELECT
      rc.constraint_schema,
      rc.constraint_name,
      rc.unique_constraint_schema AS pk_schema,
      rc.unique_constraint_name    AS pk_constraint_name
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS` rc
    WHERE rc.constraint_schema = @dataset_name
  ),
  pk_cols AS (
    SELECT
      k.constraint_schema,
      k.constraint_name,
      k.column_name AS pk_column,
      k.table_schema AS pk_table_schema,
      k.table_name   AS pk_table_name,
      k.ordinal_position
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` k
  ),
  resolved_fks AS (
    SELECT
      f.table_name,
      f.constraint_name,
      ARRAY_AGG(f.fk_column ORDER BY f.ordinal_position) AS fk_columns,
      ARRAY_AGG(STRUCT(
        p.pk_table_schema AS referenced_dataset,
        p.pk_table_name   AS referenced_table,
        p.pk_column       AS referenced_column
      ) ORDER BY f.ordinal_position) AS refs
    FROM fk_cols f
    JOIN fk_to_pk m
      ON f.constraint_name   = m.constraint_name
     AND f.table_schema      = m.constraint_schema
    JOIN pk_cols p
      ON p.constraint_name   = m.pk_constraint_name
     AND p.constraint_schema = m.pk_schema
     AND p.ordinal_position  = f.ordinal_position
    GROUP BY f.table_name, f.constraint_name
  ),
  all_constraints AS (
    SELECT
      r.table_name,
      r.constraint_name,
      'FOREIGN KEY' AS constraint_type,
      r.fk_columns  AS columns,
      r.refs        AS references
    FROM resolved_fks r
    UNION ALL
    SELECT
      tc.table_name,
      tc.constraint_name,
      'PRIMARY KEY' AS constraint_type,
      ARRAY_AGG(k.column_name ORDER BY k.ordinal_position) AS columns,
      ARRAY<STRUCT<referenced_dataset STRING, referenced_table STRING, referenced_column STRING>>[] AS references
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS` tc
    JOIN `{project}.{dataset}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` k
      ON tc.constraint_name = k.constraint_name
     AND tc.table_name      = k.table_name
     AND tc.table_schema    = k.table_schema
    WHERE tc.table_name IN UNNEST(@table_names)
      AND tc.table_schema = @dataset_name
      AND tc.constraint_type = 'PRIMARY KEY'
    GROUP BY tc.table_name, tc.constraint_name
  )
SELECT * FROM all_constraints
ORDER BY table_name, constraint_type, constraint_name
"""


def get_constraints(
    client: bigquery.Client, project_id: str, dataset_id: str, table_id: str
) -> tuple[list[str], list[dict[str, Any]]]:
    """Retrieve PK and FK constraints using INFORMATION_SCHEMA with parameters."""
    try:
        # Query PK with retry on transient errors
        pk_rows = _retry_on_transient(
            lambda: list(
                client.query(
                    PK_SQL.format(project=project_id, dataset=dataset_id),
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter(
                                "table_name", "STRING", table_id
                            ),
                            bigquery.ScalarQueryParameter(
                                "dataset_name", "STRING", dataset_id
                            ),
                        ]
                    ),
                ).result()
            ),
            operation_name=f"PK query for {project_id}.{dataset_id}.{table_id}",
        )
        primary_keys: list[str] = pk_rows[0].columns if pk_rows else []

        # Query FK with retry on transient errors
        fk_rows = _retry_on_transient(
            lambda: list(
                client.query(
                    FK_SQL.format(project=project_id, dataset=dataset_id),
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter(
                                "table_name", "STRING", table_id
                            ),
                            bigquery.ScalarQueryParameter(
                                "dataset_name", "STRING", dataset_id
                            ),
                        ]
                    ),
                ).result()
            ),
            operation_name=f"FK query for {project_id}.{dataset_id}.{table_id}",
        )

        # Group rows into composite FK constraints
        fk_groups: dict[tuple[str, str, str], dict[str, Any]] = {}
        for r in fk_rows:
            key = (
                r["constraint_name"],
                (r["referenced_dataset"] or dataset_id),
                (r["referenced_table"] or "UNKNOWN"),
            )
            grp = fk_groups.setdefault(
                key,
                {
                    "constraint_name": r["constraint_name"],
                    "referenced_dataset": key[1],
                    "referenced_table": key[2],
                    "columns": [],
                    "referenced_columns": [],
                },
            )
            grp["columns"].append(r["column_name"])
            grp["referenced_columns"].append(r["referenced_column"] or "UNKNOWN")

        foreign_keys = list(fk_groups.values())
        return primary_keys, foreign_keys

    except Exception as e:
        # Log with context for CI/debugging - schema may lack INFORMATION_SCHEMA access
        logger.warning(
            "Failed to retrieve constraints for %s.%s.%s: %s",
            project_id,
            dataset_id,
            table_id,
            e,
            exc_info=logger.isEnabledFor(logging.DEBUG),
        )
        return [], []


def get_batch_constraints(
    client: bigquery.Client, project_id: str, dataset_id: str, table_ids: list[str]
) -> dict[str, tuple[list[str], list[dict[str, Any]]]]:
    """Retrieve constraints for multiple tables in a single query."""
    if not table_ids:
        return {}

    try:
        # Query batch constraints with retry on transient errors
        rows = _retry_on_transient(
            lambda: list(
                client.query(
                    BATCH_CONSTRAINTS_SQL.format(
                        project=project_id, dataset=dataset_id
                    ),
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ArrayQueryParameter(
                                "table_names", "STRING", table_ids
                            ),
                            bigquery.ScalarQueryParameter(
                                "dataset_name", "STRING", dataset_id
                            ),
                        ]
                    ),
                ).result()
            ),
            operation_name=f"Batch constraints query for {project_id}.{dataset_id}",
        )

        results: dict[str, tuple[list[str], list[dict[str, Any]]]] = {}
        for table_id in table_ids:
            results[table_id] = ([], [])

        for row in rows:
            table_name = row["table_name"]
            constraint_type = row["constraint_type"]

            if constraint_type == "PRIMARY KEY":
                results[table_name] = (list(row["columns"]), results[table_name][1])
            elif constraint_type == "FOREIGN KEY":
                fks = results[table_name][1]
                refs = row["references"]
                # For composite FKs, all columns reference the same table
                # (REFERENTIAL_CONSTRAINTS is 1:1 with constraint_name).
                # Extract dataset/table from first ref - all refs share these.
                fks.append(
                    {
                        "constraint_name": row["constraint_name"],
                        "referenced_dataset": (
                            (refs[0]["referenced_dataset"] or dataset_id)
                            if refs
                            else dataset_id
                        ),
                        "referenced_table": (
                            (refs[0]["referenced_table"] or "UNKNOWN")
                            if refs
                            else "UNKNOWN"
                        ),
                        "columns": list(row["columns"]),
                        "referenced_columns": [
                            r["referenced_column"] or "UNKNOWN" for r in refs
                        ],
                    }
                )

        return results
    except Exception as e:
        # Log with context - schema may lack INFORMATION_SCHEMA access or have invalid refs
        logger.warning(
            "Failed to retrieve batch constraints for %s.%s (tables: %s): %s",
            project_id,
            dataset_id,
            ", ".join(table_ids[:5]) + ("..." if len(table_ids) > 5 else ""),
            e,
            exc_info=logger.isEnabledFor(logging.DEBUG),
        )
        return {table_id: ([], []) for table_id in table_ids}


# =========================
# DDL rendering helpers
# =========================

INDENT = "  "  # two spaces per level

SCALAR_TYPE_MAP = {
    # Normalize type aliases for BigQuery DDL rendering
    # Note: This maps TO canonical DDL forms (FLOAT64, INT64, BOOL)
    # Contrast with _canon_type() which maps FROM aliases for analysis
    "FLOAT": "FLOAT64",
    "INTEGER": "INT64",
    "BOOLEAN": "BOOL",
}


def _render_scalar_type(bq_type: str) -> str:
    return analyze_config.SCALAR_TYPE_MAP.get(bq_type, bq_type)


def _render_col_options(field: SchemaField) -> str:
    """Render column-level OPTIONS(...): description + policy tags.

    Supports BigQuery's PolicyTagList (field.policy_tags.names) as well as
    older shapes (iterables / dicts). Applies to nested fields too.
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
            f"{analyze_config.INDENT * (level + 1)}`{f.name}` {field_type_rendered}{tail}"
        )
        if idx < len(fields) - 1:
            inner_lines[-1] = inner_lines[-1] + ","
    struct_block = (
        "STRUCT<\n" + "\n".join(inner_lines) + f"\n{analyze_config.INDENT * level}>"
    )
    return struct_block


def _render_array_of_struct_type(field: SchemaField, level: int) -> str:
    """ARRAY<STRUCT<...>> with multiline STRUCT payload:

    ARRAY<STRUCT<
      ...
    >>
    """
    payload = _render_struct_type(field.fields, level + 1)
    return (
        "ARRAY<"
        + "\n".join(payload.splitlines())
        + f"\n{analyze_config.INDENT * level}>"
    )


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


def _render_default(field: SchemaField, level: int) -> str:
    """Render DEFAULT expression (BigQuery supports DEFAULT on top-level non-STRUCT columns)."""
    # BigQuery supports DEFAULT on columns (not nested STRUCT members)
    if level == 1 and field.field_type != "RECORD":
        expr = getattr(field, "default_value_expression", None)
        if expr:
            return f" DEFAULT ({expr})"
    return ""


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
    tail = f"{_render_default(field, level)}{_render_not_null(field)}{_render_col_options(field)}"
    return f"{analyze_config.INDENT * level}`{field.name}` {type_str}{tail}"


def _render_columns(schema: list[SchemaField]) -> str:
    lines: list[str] = []
    for i, f in enumerate(schema):
        col = _render_column(f, 1)  # columns start indented once
        if i < len(schema) - 1:
            lines.append(col + ",")
        else:
            lines.append(col)
    return "\n".join(lines)


def _collect_table_options(tbl: bigquery.Table) -> dict[str, Any]:
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


def _render_options_line(opts: dict[str, Any]) -> str | None:
    """Render a single OPTIONS(...) block with all options (deterministic label order)."""
    if not opts:
        return None

    pairs: list[str] = []
    for k, v in opts.items():
        if k == "labels" and isinstance(v, dict):
            # labels=[("k","v"),("k2","v2")] - sorted for stable output
            # Note: BigQuery labels are restricted to [a-z0-9_-] so no escaping needed
            # (keys: lowercase letters, digits, _ | values: lowercase letters, digits, _, -)
            items = ", ".join(f'("{lk}","{lv}")' for lk, lv in sorted(v.items()))
            pairs.append(f"labels=[{items}]")
        elif isinstance(v, bool):
            pairs.append(f"{k}={'TRUE' if v else 'FALSE'}")
        elif isinstance(v, (int, float)):
            pairs.append(f"{k}={v}")
        else:
            pairs.append(f'{k}="{v}"')

    return f"OPTIONS({', '.join(pairs)})"


def _render_partitioning(tbl: bigquery.Table) -> str | None:
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


def _render_clustering(tbl: bigquery.Table) -> str | None:
    """Render CLUSTER BY clause."""
    if tbl.clustering_fields:
        fields = ", ".join(f"`{f}`" for f in tbl.clustering_fields)
        return f"CLUSTER BY {fields}"
    return None


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

    create_lines: list[str] = [create_header, columns_block, closing]

    # Optional clauses after CREATE body
    part = _render_partitioning(tbl)
    clus = _render_clustering(tbl)
    opts_dict = _collect_table_options(tbl)
    opts_line = _render_options_line(opts_dict)

    if part:
        create_lines.append(part)
    if clus:
        create_lines.append(clus)
    if opts_line:
        create_lines.append(opts_line)

    create_stmt = "\n".join(create_lines) + ";"

    alter_stmts: list[str] = []
    if include_constraints:
        pk_cols, fks = get_constraints(client, project_id, dataset_id, table_id)

        if pk_cols:
            cols = ", ".join(f"`{c}`" for c in pk_cols)
            alter_stmts.append(
                f"ALTER TABLE `{table_ref}` ADD PRIMARY KEY ({cols}) NOT ENFORCED;"
            )

        for fk in fks:
            ref = f"{project_id}.{fk['referenced_dataset']}.{fk['referenced_table']}"
            if "columns" in fk and "referenced_columns" in fk:
                cols = ", ".join(f"`{c}`" for c in fk["columns"])
                ref_cols = ", ".join(f"`{c}`" for c in fk["referenced_columns"])
                alter_stmts.append(
                    f"ALTER TABLE `{table_ref}` ADD FOREIGN KEY ({cols}) REFERENCES `{ref}`({ref_cols}) NOT ENFORCED;"
                )
            else:
                # Back-compat (single-column shape)
                alter_stmts.append(
                    f"ALTER TABLE `{table_ref}` ADD FOREIGN KEY (`{fk['column']}`) "
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
    constraints_map: dict[str, tuple[list[str], list[dict[str, Any]]]] = {}
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
            columns_block = _render_columns(tbl.schema)
            closing = ")"

            create_lines: list[str] = [create_header, columns_block, closing]

            part = _render_partitioning(tbl)
            clus = _render_clustering(tbl)
            opts_dict = _collect_table_options(tbl)
            opts_line = _render_options_line(opts_dict)

            if part:
                create_lines.append(part)
            if clus:
                create_lines.append(clus)
            if opts_line:
                create_lines.append(opts_line)

            create_stmt = "\n".join(create_lines) + ";"

            alter_stmts: list[str] = []
            if include_constraints and table_id in constraints_map:
                pk_cols, fks = constraints_map[table_id]

                if pk_cols:
                    cols = ", ".join(f"`{c}`" for c in pk_cols)
                    alter_stmts.append(
                        f"ALTER TABLE `{table_ref}` ADD PRIMARY KEY ({cols}) NOT ENFORCED;"
                    )

                for fk in fks:
                    # Preserve cross-dataset/project references
                    ref_proj = project_id  # Default to same project
                    ref_ds = fk.get("referenced_dataset") or dataset_id
                    ref_tbl = fk.get("referenced_table") or "UNKNOWN"
                    ref = f"{ref_proj}.{ref_ds}.{ref_tbl}"

                    if "columns" in fk and "referenced_columns" in fk:
                        cols = ", ".join(f"`{c}`" for c in fk["columns"])
                        ref_cols = ", ".join(f"`{c}`" for c in fk["referenced_columns"])
                        alter_stmts.append(
                            f"ALTER TABLE `{table_ref}` ADD FOREIGN KEY ({cols}) REFERENCES `{ref}`({ref_cols}) NOT ENFORCED;"
                        )
                    else:
                        # Back-compat for single-column shape
                        alter_stmts.append(
                            f"ALTER TABLE `{table_ref}` ADD FOREIGN KEY (`{fk['column']}`) "
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


# =========================
# Schema Anti-Pattern Detection
# =========================


def _canon_type(field_type: str) -> str:
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


def detect_bigquery_antipatterns(
    schema: list[SchemaField],
) -> list[dict[str, Any]]:
    """Detect BigQuery schema anti-patterns in raw schema.

    Detects multiple anti-patterns:
    1. Unnecessary STRUCT wrappers
    2. Inconsistent naming conventions
    3. Boolean stored as INTEGER
    4. Deep nesting (>10 levels)
    5. REPEATED fields without ordering
    6. Low-cardinality strings (enum candidates)
    7. Generic field names
    8. Missing descriptions on complex types

    Returns list of issues with severity, description, and suggestions.
    """
    issues = []
    all_field_names = []
    nesting_depths = {}

    def get_nesting_depth(field: SchemaField, current_depth: int = 0) -> int:
        """Calculate max nesting depth for a field."""
        if field.field_type != "RECORD":
            return current_depth
        max_depth = current_depth
        for sub_field in field.fields:
            depth = get_nesting_depth(sub_field, current_depth + 1)
            max_depth = max(max_depth, depth)
        return max_depth

    def check_field(field: SchemaField, path: str = "", depth: int = 0) -> None:
        """Recursively check fields for anti-patterns."""
        field_path = f"{path}.{field.name}" if path else field.name
        all_field_names.append(field.name)

        # Track nesting depth
        if field.field_type == "RECORD":
            max_depth = get_nesting_depth(field, depth)
            nesting_depths[field_path] = max_depth

        # Anti-pattern 1: STRUCT with single "list" field that's REPEATED
        if field.field_type == "RECORD" and field.mode != "REPEATED":
            if len(field.fields) == 1:
                list_field = field.fields[0]
                if list_field.name == "list" and list_field.mode == "REPEATED":
                    if (
                        list_field.field_type == "RECORD"
                        and len(list_field.fields) == 1
                    ):
                        element_field = list_field.fields[0]
                        if element_field.name == "element":
                            issues.append(
                                {
                                    "field_name": field_path,
                                    "pattern": "unnecessary_struct_wrapper",
                                    "severity": "warning",
                                    "category": "schema_design",
                                }
                            )

        # Anti-pattern 2: STRUCT with single "element" inside REPEATED
        if field.field_type == "RECORD" and field.mode == "REPEATED":
            if len(field.fields) == 1:
                element_field = field.fields[0]
                if (
                    element_field.name == "element"
                    and element_field.field_type == "RECORD"
                ):
                    issues.append(
                        {
                            "field_name": field_path,
                            "pattern": "unnecessary_element_wrapper",
                            "severity": "warning",
                            "category": "schema_design",
                        }
                    )

        # Anti-pattern 3: Boolean stored as INTEGER
        ft = _canon_type(field.field_type)
        if ft == "INT64":
            name_lower = field.name.lower()
            if any(
                name_lower.startswith(prefix)
                for prefix in ["is_", "has_", "can_", "should_", "will_", "deleted"]
            ):
                issues.append(
                    {
                        "field_name": field_path,
                        "pattern": "boolean_as_integer",
                        "severity": "info",
                        "category": "type_optimization",
                        "suggestion": f"Use BOOLEAN instead of INTEGER for '{field_path}'",
                    }
                )

        # Anti-pattern 4: Deep nesting (>10 levels)
        if field.field_type == "RECORD" and depth > 10:
            issues.append(
                {
                    "field_name": field_path,
                    "pattern": "deep_nesting",
                    "severity": "warning",
                    "category": "complexity",
                    "depth": depth,
                    "suggestion": f"Consider flattening '{field_path}' (depth: {depth})",
                }
            )

        # Anti-pattern 5: REPEATED RECORD without ordering field
        if field.field_type == "RECORD" and field.mode == "REPEATED":
            has_order_field = any(
                sf.name
                in [
                    "order",
                    "order_in_profile",
                    "sequence",
                    "position",
                    "index",
                    "sort_order",
                ]
                for sf in field.fields
            )
            if not has_order_field and len(field.fields) > 1:
                issues.append(
                    {
                        "field_name": field_path,
                        "pattern": "missing_array_ordering",
                        "severity": "info",
                        "category": "data_quality",
                        "suggestion": f"Add ordering field to '{field_path}' array",
                    }
                )

        # Anti-pattern 6: Generic field names
        generic_names = ["data", "value", "info", "details", "metadata", "content"]
        if field.name.lower() in generic_names:
            issues.append(
                {
                    "field_name": field_path,
                    "pattern": "generic_field_name",
                    "severity": "info",
                    "category": "naming",
                    "suggestion": f"Use more descriptive name instead of '{field.name}'",
                }
            )

        # Anti-pattern 7: Missing description on complex types
        if field.field_type == "RECORD" and not field.description:
            if len(field.fields) > 5:  # Only flag complex structures
                issues.append(
                    {
                        "field_name": field_path,
                        "pattern": "missing_description",
                        "severity": "info",
                        "category": "documentation",
                        "suggestion": f"Add description to complex field '{field_path}'",
                    }
                )

        # Recurse into nested fields
        if field.field_type == "RECORD":
            for sub_field in field.fields:
                check_field(sub_field, field_path, depth + 1)

    # Check all top-level fields
    for field in schema:
        check_field(field)

    # Anti-pattern 8: Inconsistent naming conventions (check after collecting all names)
    snake_case_count = sum(1 for name in all_field_names if "_" in name)
    camel_case_count = sum(
        1
        for name in all_field_names
        if name != name.lower() and name != name.upper() and "_" not in name
    )

    total_names = len(all_field_names)
    if total_names > 5:  # Only check if we have enough fields
        if snake_case_count > 0 and camel_case_count > 0:
            ratio = min(snake_case_count, camel_case_count) / total_names
            if ratio > 0.2:  # More than 20% inconsistency
                issues.append(
                    {
                        "field_name": "schema",
                        "pattern": "inconsistent_naming",
                        "severity": "info",
                        "category": "naming",
                        "suggestion": f"Inconsistent naming: {snake_case_count} snake_case vs {camel_case_count} camelCase fields. Standardize on snake_case (BigQuery convention)",
                    }
                )

    # Anti-pattern 9: Deep nesting summary
    deeply_nested = [path for path, depth in nesting_depths.items() if depth > 5]
    if deeply_nested:
        max_depth = max(nesting_depths.values())
        if max_depth > 8:
            issues.append(
                {
                    "field_name": "schema",
                    "pattern": "overall_deep_nesting",
                    "severity": "warning",
                    "category": "complexity",
                    "suggestion": f"Schema has max nesting depth of {max_depth} levels. Consider flattening deeply nested structures.",
                    "affected_count": len(deeply_nested),
                }
            )

    # Anti-pattern 10: Wide tables (too many columns)
    total_top_level_fields = len(schema)
    if total_top_level_fields > 100:
        issues.append(
            {
                "field_name": "schema",
                "pattern": "wide_table",
                "severity": "warning",
                "category": "complexity",
                "suggestion": f"Table has {total_top_level_fields} columns. Consider splitting into multiple tables or using nested structures for better organization.",
                "field_count": total_top_level_fields,
            }
        )
    elif total_top_level_fields > 50:
        issues.append(
            {
                "field_name": "schema",
                "pattern": "wide_table",
                "severity": "info",
                "category": "complexity",
                "suggestion": f"Table has {total_top_level_fields} columns. This is getting wide - consider if splitting would improve maintainability.",
                "field_count": total_top_level_fields,
            }
        )

    # Anti-pattern 11: Inconsistent timestamp formats
    string_date_fields = []
    timestamp_fields = []

    def check_date_fields(field: SchemaField, path: str = "") -> None:
        """Check for date/time fields."""
        field_path = f"{path}.{field.name}" if path else field.name
        name_lower = field.name.lower()

        # Check for STRING fields with date-like names
        if field.field_type == "STRING":
            if any(
                keyword in name_lower
                for keyword in [
                    "date",
                    "time",
                    "timestamp",
                    "created",
                    "updated",
                    "modified",
                    "deleted",
                    "_at",
                    "_on",
                ]
            ):
                string_date_fields.append(field_path)

        # Track TIMESTAMP/DATE/DATETIME fields
        if field.field_type in ["TIMESTAMP", "DATE", "DATETIME"]:
            timestamp_fields.append(field_path)

        # Recurse
        if field.field_type == "RECORD":
            for sub_field in field.fields:
                check_date_fields(sub_field, field_path)

    for field in schema:
        check_date_fields(field)

    if string_date_fields and timestamp_fields:
        issues.append(
            {
                "field_name": "schema",
                "pattern": "inconsistent_timestamps",
                "severity": "warning",
                "category": "type_consistency",
                "suggestion": f"Mix of STRING date fields ({len(string_date_fields)}) and TIMESTAMP types ({len(timestamp_fields)}). Standardize on TIMESTAMP/DATE types for better performance and type safety.",
                "string_dates": string_date_fields[:5],
                "typed_dates": timestamp_fields[:5],
            }
        )

    # Collect nullable ID fields first (to exclude from FK check)
    primary_id_fields_set = set()

    def collect_primary_ids(field: SchemaField, path: str = "") -> None:
        """Collect primary ID field names."""
        field_path = f"{path}.{field.name}" if path else field.name
        name_lower = field.name.lower()

        if field.mode == "NULLABLE":
            # Top-level id
            if name_lower == "id" and not path:
                primary_id_fields_set.add(field_path)
            # Entity-specific IDs at top level (member_id, user_id, etc.)
            elif (
                name_lower.endswith("_id")
                and not name_lower.endswith("_ref_id")
                and not path
            ):
                if name_lower not in [
                    "parent_id",
                    "related_id",
                    "ref_id",
                    "reference_id",
                ]:
                    primary_id_fields_set.add(field_path)

        # Recurse
        if field.field_type == "RECORD":
            for sub_field in field.fields:
                collect_primary_ids(sub_field, field_path)

    for field in schema:
        collect_primary_ids(field)

    # Anti-pattern 12: Nullable foreign keys (excluding primary IDs)
    nullable_foreign_keys = []

    def check_foreign_keys(field: SchemaField, path: str = "") -> None:
        """Check for nullable foreign key fields."""
        field_path = f"{path}.{field.name}" if path else field.name

        # Check if field looks like a foreign key and is nullable
        if field.name.endswith("_id") and field.mode == "NULLABLE":
            # Exclude fields already flagged as primary IDs
            if field_path not in primary_id_fields_set:
                # Exclude common non-FK ids
                if field.name not in ["id", "uuid", "guid"]:
                    nullable_foreign_keys.append(field_path)

        # Recurse
        if field.field_type == "RECORD":
            for sub_field in field.fields:
                check_foreign_keys(sub_field, field_path)

    for field in schema:
        check_foreign_keys(field)

    if nullable_foreign_keys:
        issues.append(
            {
                "field_name": "schema",
                "pattern": "nullable_foreign_keys",
                "severity": "info",
                "category": "data_integrity",
                "suggestion": f"Foreign key fields are nullable ({len(nullable_foreign_keys)} fields). Consider making REQUIRED or adding explicit null handling to prevent orphaned references.",
                "affected_fields": nullable_foreign_keys,
            }
        )

    # Anti-pattern 13: Redundant/duplicate structures
    # Find RECORD fields with IDENTICAL structures at the TOP level only
    # This is conservative - only flags true duplicates that likely represent the same entity
    top_level_struct_signatures: dict[tuple, list[str]] = {}

    def collect_top_level_struct_signatures(field: SchemaField) -> None:
        """Collect signatures of top-level RECORD fields only."""
        # Only check direct children of root or arrays
        # Skip deeply nested structures as similarity there is often coincidental
        if field.field_type == "RECORD":
            # For arrays, check the element structure
            if (
                field.mode == "REPEATED"
                and len(field.fields) == 1
                and field.fields[0].name == "element"
            ):
                element_field = field.fields[0]
                if element_field.field_type == "RECORD":
                    # This is an array of structs - check its structure
                    signature = tuple(
                        sorted(
                            (sf.name, sf.field_type, sf.mode)
                            for sf in element_field.fields
                        )
                    )
                    # Only flag if substantial (>=6 fields) and at root level
                    if len(signature) >= 6:
                        if signature not in top_level_struct_signatures:
                            top_level_struct_signatures[signature] = []
                        top_level_struct_signatures[signature].append(
                            f"{field.name}.list.element"
                        )

    for field in schema:
        collect_top_level_struct_signatures(field)

    # Find duplicates - only report if 3+ instances (more likely to be a real issue)
    redundant_structs = {
        sig: paths
        for sig, paths in top_level_struct_signatures.items()
        if len(paths) >= 3
    }

    if redundant_structs:
        # Report the most duplicated one
        most_duplicated = max(
            redundant_structs.items(), key=lambda x: (len(x[1]), len(x[0]))
        )
        signature, paths = most_duplicated

        issues.append(
            {
                "field_name": "schema",
                "pattern": "redundant_structures",
                "severity": "info",
                "category": "normalization",
                "suggestion": f"Identical structure ({len(signature)} fields) appears in {len(paths)} top-level arrays. If these represent the same entity type, consider consolidating or creating a shared reference table.",
                "affected_fields": paths,
                "field_count": len(signature),
            }
        )

    # Anti-pattern 14: Cryptic/abbreviated column names
    cryptic_fields = []

    def check_cryptic_names(field: SchemaField, path: str = "") -> None:
        """Check for cryptic or overly abbreviated names."""
        field_path = f"{path}.{field.name}" if path else field.name
        name = field.name

        # Skip ISO standard codes (iso_2, iso_3, iso_*, etc.)
        name_lower = name.lower()
        if "iso" in name_lower or name_lower.startswith("iso_"):
            return

        # Check for cryptic patterns
        is_cryptic = False

        # Pattern 1: Very short names (1-2 chars) that aren't common
        if len(name) <= 2 and name_lower not in analyze_config.ACCEPTABLE_ABBREVIATIONS:
            is_cryptic = True

        # Pattern 2: Excessive abbreviation (lots of consonants, no vowels, >3 chars)
        if len(name) > 3 and len(name) <= 6:
            vowels = set("aeiouAEIOU")
            has_vowel = any(c in vowels for c in name)
            if (
                not has_vowel
                and name_lower not in analyze_config.ACCEPTABLE_ABBREVIATIONS
            ):
                is_cryptic = True

        # Pattern 3: Numbers in names (except common patterns like v1, v2, iso_2, iso_3)
        if re.search(r"\d", name) and not re.match(
            r".*(_v\d+|_\d{4}|_at|_on|iso_\d+)$", name_lower
        ):
            # Allow version numbers, years, and ISO codes
            if not re.match(
                r"^(v\d+|version_?\d+|.*iso.*\d+)$", name_lower, re.IGNORECASE
            ):
                is_cryptic = True

        # Pattern 4: Single letter prefixes (e.g., bActive, sName, iCount)
        if re.match(r"^[a-z][A-Z]", name):  # Hungarian notation
            is_cryptic = True

        if is_cryptic:
            cryptic_fields.append(field_path)

        # Recurse
        if field.field_type == "RECORD":
            for sub_field in field.fields:
                check_cryptic_names(sub_field, field_path)

    for field in schema:
        check_cryptic_names(field)

    if cryptic_fields:
        issues.append(
            {
                "field_name": "schema",
                "pattern": "cryptic_names",
                "severity": "info",
                "category": "naming",
                "suggestion": f"Cryptic or overly abbreviated field names detected ({len(cryptic_fields)} fields). Use descriptive names for better readability (e.g., 'usr' → 'user', 'cnt' → 'count').",
                "affected_fields": cryptic_fields,
            }
        )

    # Anti-pattern 15: Missing audit columns
    top_level_names = {f.name.lower() for f in schema}

    # Check for presence of audit field categories
    has_timestamp = bool(top_level_names & analyze_config.AUDIT_TIMESTAMP_FIELDS)
    has_actor = bool(top_level_names & analyze_config.AUDIT_ACTOR_FIELDS)
    has_version = bool(top_level_names & analyze_config.AUDIT_VERSION_FIELDS)
    has_soft_delete = bool(top_level_names & analyze_config.AUDIT_SOFT_DELETE_FIELDS)
    has_source = bool(top_level_names & analyze_config.AUDIT_SOURCE_FIELDS)

    # Find which specific recommended fields are present
    present_audit = top_level_names & analyze_config.ALL_AUDIT_FIELDS
    missing_recommended = analyze_config.RECOMMENDED_AUDIT_FIELDS - top_level_names

    # Check for incomplete audit pairs
    incomplete_pairs = []
    for ts_field, actor_field in analyze_config.AUDIT_FIELD_PAIRS:
        if ts_field in top_level_names and actor_field not in top_level_names:
            incomplete_pairs.append(f"{ts_field} (missing {actor_field})")

    # Build comprehensive feedback
    audit_summary = {
        "has_timestamp": has_timestamp,
        "has_actor": has_actor,
        "has_version": has_version,
        "has_soft_delete": has_soft_delete,
        "has_source": has_source,
        "present_fields": sorted(present_audit),
        "missing_recommended": sorted(missing_recommended),
        "incomplete_pairs": incomplete_pairs,
    }

    # Issue if missing recommended fields or has incomplete pairs
    if missing_recommended or incomplete_pairs:
        severity = "warning" if missing_recommended else "info"

        suggestion_parts = []
        if missing_recommended:
            suggestion_parts.append(
                f"Missing recommended audit fields: {', '.join(sorted(missing_recommended))}"
            )
        if incomplete_pairs:
            suggestion_parts.append(
                f"Incomplete audit pairs: {', '.join(incomplete_pairs)}"
            )

        # Add category-specific recommendations
        recommendations = []
        if not has_timestamp:
            recommendations.append(
                "Add timestamp fields (created_at, updated_at) for change tracking"
            )
        if not has_actor and has_timestamp:
            recommendations.append(
                "Consider adding actor fields (created_by, updated_by) to track who made changes"
            )
        if not has_version and (has_timestamp or has_actor):
            recommendations.append(
                "Consider adding version/etag for optimistic locking"
            )

        full_suggestion = ". ".join(suggestion_parts)
        if recommendations:
            full_suggestion += ". " + ". ".join(recommendations)

        issues.append(
            {
                "field_name": "schema",
                "pattern": "missing_audit_columns",
                "severity": severity,
                "category": "data_quality",
                "suggestion": full_suggestion,
                "audit_summary": audit_summary,
            }
        )

    # Anti-pattern 16: Inconsistent field casing within schema
    # Collect all field name casing patterns
    all_fields_for_casing = []

    def collect_all_names(field: SchemaField, path: str = "") -> None:
        """Collect all field names for casing analysis."""
        all_fields_for_casing.append(field.name)
        if field.field_type == "RECORD":
            for sub_field in field.fields:
                collect_all_names(
                    sub_field, f"{path}.{field.name}" if path else field.name
                )

    for field in schema:
        collect_all_names(field)

    # Analyze casing patterns
    snake_case = []  # lowercase_with_underscores
    camel_case = []  # camelCase
    pascal_case = []  # PascalCase
    upper_case = []  # ALLCAPS or UPPER_SNAKE
    mixed_case = []  # Anything else weird

    for name in all_fields_for_casing:
        if name.isupper():
            upper_case.append(name)
        elif "_" in name and name.islower():
            snake_case.append(name)
        elif "_" in name and name.isupper():
            upper_case.append(name)
        elif name[0].isupper() and "_" not in name:
            pascal_case.append(name)
        elif name[0].islower() and name != name.lower() and "_" not in name:
            camel_case.append(name)
        elif name.islower():
            snake_case.append(name)  # Lowercase, no underscores
        else:
            mixed_case.append(name)

    # Check for inconsistency
    casing_counts = {
        "snake_case": len(snake_case),
        "camelCase": len(camel_case),
        "PascalCase": len(pascal_case),
        "UPPER_CASE": len(upper_case),
        "mixed": len(mixed_case),
    }

    # Remove zero counts
    casing_counts = {k: v for k, v in casing_counts.items() if v > 0}

    # If we have more than one casing style with significant usage
    if len(casing_counts) > 1:
        # Check if minority style is > 20% of fields
        total = sum(casing_counts.values())
        sorted_styles = sorted(casing_counts.items(), key=lambda x: x[1], reverse=True)
        dominant_style, dominant_count = sorted_styles[0]

        minority_count = sum(count for _, count in sorted_styles[1:])
        if minority_count / total > 0.15:  # More than 15% inconsistency
            style_summary = ", ".join(
                f"{count} {style}" for style, count in sorted_styles
            )
            issues.append(
                {
                    "field_name": "schema",
                    "pattern": "inconsistent_casing",
                    "severity": "info",
                    "category": "naming",
                    "suggestion": f"Inconsistent field name casing: {style_summary}. BigQuery convention is snake_case. Standardize all fields to {dominant_style} or preferably snake_case.",
                    "casing_distribution": dict(casing_counts),
                }
            )

    # Anti-pattern 17: JSON/STRING blob fields
    json_blob_fields = []

    def check_json_blobs(field: SchemaField, path: str = "") -> None:
        """Check for STRING fields that should be structured."""
        field_path = f"{path}.{field.name}" if path else field.name
        name_lower = field.name.lower()

        # Check if STRING field with blob-like name
        if field.field_type == "STRING":
            blob_indicators = [
                "json",
                "metadata",
                "properties",
                "config",
                "data",
                "payload",
                "content",
                "details",
                "attributes",
                "extra",
                "raw",
                "blob",
                "params",
                "options",
                "settings",
            ]
            if name_lower in blob_indicators or any(
                name_lower.endswith(f"_{indicator}")
                or name_lower.startswith(f"{indicator}_")
                for indicator in blob_indicators
            ):
                json_blob_fields.append(field_path)

        # Recurse
        if field.field_type == "RECORD":
            for sub_field in field.fields:
                check_json_blobs(sub_field, field_path)

    for field in schema:
        check_json_blobs(field)

    if json_blob_fields:
        issues.append(
            {
                "field_name": "schema",
                "pattern": "json_string_blobs",
                "severity": "warning",
                "category": "schema_design",
                "suggestion": f"STRING fields likely containing JSON/structured data ({len(json_blob_fields)} fields). Use RECORD/STRUCT for type safety, better compression, and column-level access.",
                "affected_fields": json_blob_fields,
            }
        )

    # Anti-pattern 18: Nullable ID fields
    nullable_id_fields = []

    def check_nullable_ids(field: SchemaField, path: str = "") -> None:
        """Check for ID fields that should be REQUIRED."""
        field_path = f"{path}.{field.name}" if path else field.name
        name_lower = field.name.lower()

        # Check if this is a primary/important ID that's nullable
        if field.mode == "NULLABLE":
            # Primary key patterns
            if name_lower == "id" and not path:  # Top-level id
                nullable_id_fields.append(field_path)
            # Entity-specific IDs (user_id, customer_id, etc.)
            elif name_lower.endswith("_id") and not name_lower.endswith("_ref_id"):
                # Exclude obvious foreign keys like parent_id, related_id
                if name_lower not in [
                    "parent_id",
                    "related_id",
                    "ref_id",
                    "reference_id",
                ]:
                    # If it's at top level or in a non-repeated structure, flag it
                    if not path or "_id" in name_lower.split("_")[0]:
                        nullable_id_fields.append(field_path)

        # Recurse
        if field.field_type == "RECORD":
            for sub_field in field.fields:
                check_nullable_ids(sub_field, field_path)

    for field in schema:
        check_nullable_ids(field)

    if nullable_id_fields:
        issues.append(
            {
                "field_name": "schema",
                "pattern": "nullable_id_fields",
                "severity": "warning",
                "category": "data_integrity",
                "suggestion": f"Primary/identifier fields are nullable ({len(nullable_id_fields)} fields). IDs should typically be REQUIRED to ensure data integrity.",
                "affected_fields": nullable_id_fields,
            }
        )

    # Anti-pattern 19: Reserved keyword field names
    reserved_keywords = {
        "all",
        "and",
        "any",
        "array",
        "as",
        "asc",
        "assert_rows_modified",
        "at",
        "between",
        "by",
        "case",
        "cast",
        "collate",
        "contains",
        "create",
        "cross",
        "cube",
        "current",
        "default",
        "define",
        "desc",
        "distinct",
        "else",
        "end",
        "enum",
        "escape",
        "except",
        "exclude",
        "exists",
        "extract",
        "false",
        "fetch",
        "following",
        "for",
        "from",
        "full",
        "group",
        "grouping",
        "groups",
        "hash",
        "having",
        "if",
        "ignore",
        "in",
        "inner",
        "intersect",
        "interval",
        "into",
        "is",
        "join",
        "lateral",
        "left",
        "like",
        "limit",
        "lookup",
        "merge",
        "natural",
        "new",
        "no",
        "not",
        "null",
        "nulls",
        "of",
        "on",
        "or",
        "order",
        "outer",
        "over",
        "partition",
        "preceding",
        "proto",
        "range",
        "recursive",
        "respect",
        "right",
        "rollup",
        "rows",
        "select",
        "set",
        "some",
        "struct",
        "tablesample",
        "then",
        "to",
        "treat",
        "true",
        "unbounded",
        "union",
        "unnest",
        "using",
        "when",
        "where",
        "window",
        "with",
        "within",
    }

    keyword_fields = []

    def check_reserved_keywords(field: SchemaField, path: str = "") -> None:
        """Check for reserved keyword field names."""
        field_path = f"{path}.{field.name}" if path else field.name

        if field.name.lower() in reserved_keywords:
            keyword_fields.append(field_path)

        # Recurse
        if field.field_type == "RECORD":
            for sub_field in field.fields:
                check_reserved_keywords(sub_field, field_path)

    for field in schema:
        check_reserved_keywords(field)

    if keyword_fields:
        issues.append(
            {
                "field_name": "schema",
                "pattern": "reserved_keywords",
                "severity": "warning",
                "category": "naming",
                "suggestion": f"Field names use SQL reserved keywords ({len(keyword_fields)} fields). Requires backticks in queries. Rename to avoid: `select` → `selection`, `order` → `sort_order`, `group` → `group_name`.",
                "affected_fields": keyword_fields,
            }
        )

    # Anti-pattern 20: Overly granular timestamps (TIMESTAMP for date-only fields)
    granular_timestamp_fields = []

    def check_timestamp_granularity(field: SchemaField, path: str = "") -> None:
        """Check for TIMESTAMP fields that should be DATE."""
        field_path = f"{path}.{field.name}" if path else field.name
        name_lower = field.name.lower()

        # Check if TIMESTAMP with date-only semantics
        if field.field_type == "TIMESTAMP":
            date_only_indicators = [
                "birth_date",
                "birthdate",
                "date_of_birth",
                "dob",
                "hire_date",
                "hired_date",
                "start_date",
                "end_date",
                "expiry_date",
                "expiration_date",
                "due_date",
                "publish_date",
                "published_date",
                "release_date",
            ]
            # Also check for _date suffix (but not _datetime)
            if name_lower in date_only_indicators or (
                name_lower.endswith("_date") and not name_lower.endswith("_datetime")
            ):
                granular_timestamp_fields.append(field_path)

        # Recurse
        if field.field_type == "RECORD":
            for sub_field in field.fields:
                check_timestamp_granularity(sub_field, field_path)

    for field in schema:
        check_timestamp_granularity(field)

    if granular_timestamp_fields:
        issues.append(
            {
                "field_name": "schema",
                "pattern": "overly_granular_timestamps",
                "severity": "info",
                "category": "type_optimization",
                "suggestion": f"TIMESTAMP fields with date-only semantics ({len(granular_timestamp_fields)} fields). Use DATE type for fields like birth_date, hire_date. Saves 8 bytes per row and improves query semantics.",
                "affected_fields": granular_timestamp_fields,
            }
        )

    # Anti-pattern 21: Expensive unnest candidates (complex ARRAY<STRUCT>)
    expensive_unnest_fields = []

    def check_expensive_unnests(field: SchemaField, path: str = "") -> None:
        """Check for ARRAY<STRUCT> with many fields (expensive to unnest)."""
        field_path = f"{path}.{field.name}" if path else field.name

        # Check for REPEATED RECORD with many fields
        if field.field_type == "RECORD" and field.mode == "REPEATED":
            field_count = len(field.fields)
            if field_count > 10:
                expensive_unnest_fields.append((field_path, field_count))

        # Recurse
        if field.field_type == "RECORD":
            for sub_field in field.fields:
                check_expensive_unnests(sub_field, field_path)

    for field in schema:
        check_expensive_unnests(field)

    if expensive_unnest_fields:
        # Sort by field count descending
        expensive_unnest_fields.sort(key=lambda x: x[1], reverse=True)
        field_paths = [path for path, _ in expensive_unnest_fields]
        max_field_count = expensive_unnest_fields[0][1]

        issues.append(
            {
                "field_name": "schema",
                "pattern": "expensive_unnest",
                "severity": "info",
                "category": "performance",
                "suggestion": f"Complex ARRAY<STRUCT> fields ({len(expensive_unnest_fields)} arrays, max {max_field_count} fields). UNNEST operations on wide structs are expensive. Consider denormalizing into separate table or reducing struct width.",
                "affected_fields": field_paths,
            }
        )

    # Anti-pattern 22: Negative boolean field names
    negative_boolean_fields = []

    def check_negative_booleans(field: SchemaField, path: str = "") -> None:
        """Check for negative boolean naming (double negative logic)."""
        field_path = f"{path}.{field.name}" if path else field.name
        name_lower = field.name.lower()

        # Check for negative boolean patterns
        if field.field_type in ["BOOLEAN", "INTEGER"]:
            negative_patterns = [
                "is_not_",
                "has_no_",
                "cannot_",
                "isnt_",
                "hasnt_",
                "no_",
                "not_",
                "non_",
                "without_",
                "disabled",
                "inactive",
            ]
            # Check for explicit negative patterns
            for pattern in negative_patterns[:5]:  # is_not_, has_no_, etc.
                if name_lower.startswith(pattern):
                    negative_boolean_fields.append(field_path)
                    break
            # Check for implicit negatives (but be careful with common names)
            else:
                if any(
                    name_lower.startswith(p)
                    for p in ["no_", "not_", "non_", "without_"]
                ):
                    # Exclude common acceptable names
                    if name_lower not in [
                        "no_reply",
                        "notes",
                        "notice",
                        "notification",
                    ]:
                        negative_boolean_fields.append(field_path)

        # Recurse
        if field.field_type == "RECORD":
            for sub_field in field.fields:
                check_negative_booleans(sub_field, field_path)

    for field in schema:
        check_negative_booleans(field)

    if negative_boolean_fields:
        issues.append(
            {
                "field_name": "schema",
                "pattern": "negative_booleans",
                "severity": "info",
                "category": "naming",
                "suggestion": f"Negative boolean field names ({len(negative_boolean_fields)} fields). Creates double negatives in queries. Use positive names: `is_not_active` → `is_active`, `has_no_access` → `has_access`.",
                "affected_fields": negative_boolean_fields,
            }
        )

    # Anti-pattern 23: Overly long field names
    long_field_names = []

    def check_long_names(field: SchemaField, path: str = "") -> None:
        """Check for excessively long field names."""
        field_path = f"{path}.{field.name}" if path else field.name

        if len(field.name) > 50:
            long_field_names.append((field_path, len(field.name)))

        # Recurse
        if field.field_type == "RECORD":
            for sub_field in field.fields:
                check_long_names(sub_field, field_path)

    for field in schema:
        check_long_names(field)

    if long_field_names:
        # Sort by length descending
        long_field_names.sort(key=lambda x: x[1], reverse=True)
        field_paths = [path for path, _ in long_field_names]
        max_length = long_field_names[0][1]

        issues.append(
            {
                "field_name": "schema",
                "pattern": "overly_long_names",
                "severity": "info",
                "category": "naming",
                "suggestion": f"Overly long field names ({len(long_field_names)} fields, max {max_length} chars). Keep names concise (<50 chars). Use descriptions for details.",
                "affected_fields": field_paths,
            }
        )

    # Anti-pattern 24: Field ordering
    # REMOVED: Field ordering is subjective and doesn't affect BigQuery performance.
    # Column order has no impact on query execution, storage, or indexing in BigQuery.

    # ============================================================================
    # PHASE 1: High-Impact Anti-Patterns (10 patterns)
    # ============================================================================

    # Anti-pattern 25: String abuse (numbers/booleans as STRING)
    string_abuse_fields = []

    def check_string_abuse(field: SchemaField, path: str = "") -> None:
        """Check for STRING fields that should be numeric or boolean."""
        field_path = f"{path}.{field.name}" if path else field.name
        name_lower = field.name.lower()

        if field.field_type == "STRING":
            # Skip ISO codes, country codes, and code fields (correctly STRING)
            if any(
                pattern in name_lower
                for pattern in analyze_config.STRING_FIELD_EXCLUSIONS
            ):
                return

            # Numeric indicators
            # Be more conservative: require exact match or _keyword pattern
            # Exclude "number" and "position" as they're often identifiers/codes
            if any(
                name_lower == kw or f"_{kw}" in name_lower or f"{kw}_" in name_lower
                for kw in analyze_config.NUMERIC_FIELD_INDICATORS
            ):
                string_abuse_fields.append((field_path, "numeric"))

            # Boolean indicators (not caught by boolean_as_integer check)
            if any(
                name_lower.startswith(kw)
                for kw in analyze_config.BOOLEAN_FIELD_PREFIXES
            ) or any(kw in name_lower for kw in analyze_config.BOOLEAN_FIELD_KEYWORDS):
                string_abuse_fields.append((field_path, "boolean"))

        # Recurse
        if field.field_type == "RECORD":
            for sub_field in field.fields:
                check_string_abuse(sub_field, field_path)

    for field in schema:
        check_string_abuse(field)

    if string_abuse_fields:
        by_type: dict[str, list[str]] = {}
        for field_path, suggested_type in string_abuse_fields:
            by_type.setdefault(suggested_type, []).append(field_path)

        for suggested_type, fields in by_type.items():
            type_name = "INTEGER/NUMERIC" if suggested_type == "numeric" else "BOOLEAN"
            issues.append(
                {
                    "field_name": "schema",
                    "pattern": "string_type_abuse",
                    "severity": "warning",
                    "category": "type_optimization",
                    "suggestion": f"STRING fields that should be {type_name} ({len(fields)} fields). Use proper types for better performance, validation, and query optimization.",
                    "affected_fields": fields,
                }
            )

    # Anti-pattern 26: Type inconsistency (mixed types for similar fields)
    def check_type_consistency() -> None:
        """Check for inconsistent types across similar fields."""
        id_fields_by_type: dict[str, list[str]] = {}

        def collect_id_types(field: SchemaField, path: str = "") -> None:
            field_path = f"{path}.{field.name}" if path else field.name
            field_name_lower = field.name.lower()

            # Only check top-level IDs (internal entity IDs)
            # Exclude:
            # - Nested IDs (external references like credential_id, certificate_id)
            # - Common external IDs (transaction_id, order_id, etc. in nested contexts)
            if field_name_lower.endswith("_id") and not path:
                id_fields_by_type.setdefault(field.field_type, []).append(field_path)

            if field.field_type == "RECORD":
                for sub_field in field.fields:
                    collect_id_types(sub_field, field_path)

        for field in schema:
            collect_id_types(field)

        if len(id_fields_by_type) > 1:
            type_summary = ", ".join(
                f"{len(fields)} {ftype}" for ftype, fields in id_fields_by_type.items()
            )
            all_id_fields = [f for fields in id_fields_by_type.values() for f in fields]
            issues.append(
                {
                    "field_name": "schema",
                    "pattern": "inconsistent_id_types",
                    "severity": "warning",
                    "category": "type_consistency",
                    "suggestion": f"Internal ID fields have inconsistent types ({type_summary}). Standardize on one type (typically STRING or INTEGER) for primary entity IDs.",
                    "affected_fields": all_id_fields,
                }
            )

    check_type_consistency()

    # Anti-pattern 27: FLOAT64 for monetary values
    float_money_fields = []

    def check_float_money(field: SchemaField, path: str = "") -> None:
        """Check for FLOAT64 fields used for money."""
        field_path = f"{path}.{field.name}" if path else field.name
        name_lower = field.name.lower()
        ft = _canon_type(field.field_type)

        if ft == "FLOAT64":
            money_keywords = [
                "price",
                "cost",
                "amount",
                "balance",
                "tax",
                "fee",
                "payment",
                "salary",
                "revenue",
                "total",
            ]
            if any(keyword in name_lower for keyword in money_keywords):
                float_money_fields.append(field_path)

        if field.field_type == "RECORD":
            for sub_field in field.fields:
                check_float_money(sub_field, field_path)

    for field in schema:
        check_float_money(field)

    if float_money_fields:
        issues.append(
            {
                "field_name": "schema",
                "pattern": "float_for_money",
                "severity": "warning",
                "category": "type_optimization",
                "suggestion": f"FLOAT64 used for monetary values ({len(float_money_fields)} fields). Use NUMERIC/DECIMAL for exact decimal arithmetic to avoid rounding errors.",
                "affected_fields": float_money_fields,
            }
        )

    # Anti-pattern 28: God table (too many unrelated fields)
    total_fields = len(schema)
    if total_fields > 80:
        issues.append(
            {
                "field_name": "schema",
                "pattern": "god_table",
                "severity": "warning",
                "category": "schema_design",
                "suggestion": f"Table has {total_fields} top-level fields, suggesting multiple concerns. Consider splitting into focused tables (users + preferences, orders + line_items, etc.) for better maintainability.",
                "affected_fields": [],
            }
        )

    # Anti-pattern 29: Missing unique identifiers in arrays
    arrays_without_id = []

    def check_array_ids(field: SchemaField, path: str = "") -> None:
        """Check for ARRAY<STRUCT> without id field."""
        field_path = f"{path}.{field.name}" if path else field.name

        if field.field_type == "RECORD" and field.mode == "REPEATED":
            # Check if struct has an id field
            has_id = any(
                sf.name.lower() in ["id", "uuid", "guid", "key", "index"]
                for sf in field.fields
            )
            # Only flag if struct has multiple fields
            if not has_id and len(field.fields) > 2:
                arrays_without_id.append(field_path)

        if field.field_type == "RECORD":
            for sub_field in field.fields:
                check_array_ids(sub_field, field_path)

    for field in schema:
        check_array_ids(field)

    if arrays_without_id:
        issues.append(
            {
                "field_name": "schema",
                "pattern": "array_without_id",
                "severity": "warning",
                "category": "data_quality",
                "suggestion": f"ARRAY<STRUCT> fields without unique identifiers ({len(arrays_without_id)} arrays). Add id/uuid field to enable updates, deletes, and unambiguous references.",
                "affected_fields": arrays_without_id,
            }
        )

    # Anti-pattern 30: PII without clear marking
    pii_missing_both = []  # no policy tags AND no doc
    pii_missing_tags = []  # documented but missing policy tags

    def check_pii(field: SchemaField, path: str = "") -> None:
        """Check for PII fields and whether they are tagged/documented."""
        field_path = f"{path}.{field.name}" if path else field.name
        name_lower = field.name.lower()

        # Cut obvious false positives
        if (name_lower in analyze_config.PII_EXCLUDE_EXACT) or any(
            name_lower.endswith(suf) for suf in analyze_config.PII_EXCLUDE_SUFFIXES
        ):
            pass
        else:
            # Flatten all PII indicators into single list for substring check
            all_pii_indicators: list[str] = []
            for indicators in analyze_config.PII_INDICATORS.values():
                all_pii_indicators.extend(indicators)

            name_signals = any(ind in name_lower for ind in all_pii_indicators)

            # Weak signals (only count if STRING/BYTES)
            if not name_signals and any(
                t in name_lower for t in {"image", "photo", "picture"}
            ):
                name_signals = field.field_type in ("STRING", "BYTES")

            if name_signals:
                desc = (field.description or "").lower()
                documented = any(
                    w in desc
                    for w in [
                        "pii",
                        "sensitive",
                        "confidential",
                        "private",
                        "personal",
                    ]
                )
                tagged = bool(_policy_tag_names(field))

                if not documented and not tagged:
                    pii_missing_both.append(field_path)
                elif documented and not tagged:
                    pii_missing_tags.append(field_path)

        if field.field_type == "RECORD":
            for sub in field.fields:
                check_pii(sub, field_path)

    for field in schema:
        check_pii(field)

    if pii_missing_both:
        issues.append(
            {
                "field_name": "schema",
                "pattern": "pii_unmarked_undocumented",
                "severity": "warning",
                "category": "security",
                "suggestion": "Fields likely containing PII are neither policy-tagged nor documented. Add data catalog policy tags and docs.",
                "affected_fields": pii_missing_both,
            }
        )
    if pii_missing_tags:
        issues.append(
            {
                "field_name": "schema",
                "pattern": "pii_missing_policy_tags",
                "severity": "info",
                "category": "security",
                "suggestion": "Fields likely containing PII are documented as sensitive but missing policy tags. Attach taxonomy policy tags.",
                "affected_fields": pii_missing_tags,
            }
        )

    # Anti-pattern 31: Password/secret fields
    secret_fields_untagged = []
    secret_fields_tagged_or_doc = []

    def check_secrets(field: SchemaField, path: str = "") -> None:
        field_path = f"{path}.{field.name}" if path else field.name
        name_lower = field.name.lower()

        # Ignore obvious non-secrets (_id/_key references)
        if name_lower.endswith(("_id", "_key", "_ref")):
            pass
        else:
            is_secretish = name_lower in analyze_config.SENSITIVE_SECRETS_EXACT
            # tolerant extras: *_secret, *_token, *_private_key (but avoid false positives)
            if not is_secretish and any(
                name_lower.endswith(sfx)
                for sfx in ("_secret", "_token", "_private_key")
            ):
                is_secretish = True

            if is_secretish:
                desc = (field.description or "").lower()
                documented = any(
                    w in desc for w in ["secret", "credential", "sensitive"]
                )
                tagged = bool(_policy_tag_names(field))
                # Prefer BYTES or hashed STRING; flag plain STRING strongly
                if field.field_type == "STRING" and not (tagged or documented):
                    secret_fields_untagged.append(field_path)
                elif tagged or documented:
                    secret_fields_tagged_or_doc.append(field_path)

        if field.field_type == "RECORD":
            for sub in field.fields:
                check_secrets(sub, field_path)

    for field in schema:
        check_secrets(field)

    if secret_fields_untagged:
        issues.append(
            {
                "field_name": "schema",
                "pattern": "plaintext_secrets",
                "severity": "error",
                "category": "security",
                "suggestion": "Likely secrets stored without policy tags/docs. Never store plaintext passwords or long-lived tokens; prefer hashed values or external secret managers and add policy tags.",
                "affected_fields": secret_fields_untagged,
            }
        )
    if secret_fields_tagged_or_doc:
        issues.append(
            {
                "field_name": "schema",
                "pattern": "secrets_needing_review",
                "severity": "info",
                "category": "security",
                "suggestion": "Secret-like fields are present but already tagged/documented. Re-check storage strategy (hashing/rotation) and access policies.",
                "affected_fields": secret_fields_tagged_or_doc,
            }
        )

    # Anti-pattern 32: Unstructured address
    unstructured_address = []

    def check_address_structure(field: SchemaField, path: str = "") -> None:
        """Check for address fields that should be structured."""
        field_path = f"{path}.{field.name}" if path else field.name
        name_lower = field.name.lower()

        if field.field_type == "STRING" and "address" in name_lower:
            # Single string address field - should be structured
            unstructured_address.append(field_path)

        if field.field_type == "RECORD":
            for sub_field in field.fields:
                check_address_structure(sub_field, field_path)

    for field in schema:
        check_address_structure(field)

    if unstructured_address:
        issues.append(
            {
                "field_name": "schema",
                "pattern": "unstructured_address",
                "severity": "info",
                "category": "data_quality",
                "suggestion": f"Address fields stored as STRING ({len(unstructured_address)} fields). Use STRUCT with street, city, state, zip, country for better querying and validation.",
                "affected_fields": unstructured_address,
            }
        )

    # Anti-pattern 33: Enum fields without constraints
    enum_without_constraints = []

    def check_enum_fields(field: SchemaField, path: str = "") -> None:
        """Check for enum-like fields without documentation."""
        field_path = f"{path}.{field.name}" if path else field.name
        name_lower = field.name.lower()

        enum_indicators = [
            "status",
            "state",
            "type",
            "category",
            "level",
            "priority",
            "role",
            "kind",
        ]

        if field.field_type == "STRING" and any(
            indicator in name_lower for indicator in enum_indicators
        ):
            # Check if description mentions valid values
            desc = (field.description or "").lower()
            if not any(
                word in desc
                for word in ["valid", "values", "enum", "one of", "options", ":"]
            ):
                enum_without_constraints.append(field_path)

        if field.field_type == "RECORD":
            for sub_field in field.fields:
                check_enum_fields(sub_field, field_path)

    for field in schema:
        check_enum_fields(field)

    if enum_without_constraints:
        issues.append(
            {
                "field_name": "schema",
                "pattern": "undocumented_enum",
                "severity": "info",
                "category": "documentation",
                "suggestion": f"Enum-like fields without documented valid values ({len(enum_without_constraints)} fields). Document constraints in description (e.g., 'Valid values: active, inactive, pending').",
                "affected_fields": enum_without_constraints,
            }
        )

    # ============================================================================
    # PHASE 2: Medium Priority Anti-Patterns (12 patterns)
    # ============================================================================

    # Anti-pattern 34: Plural/singular confusion
    plural_singular_issues = []

    def check_pluralization(field: SchemaField, path: str = "") -> None:
        """Check for pluralization consistency."""
        field_path = f"{path}.{field.name}" if path else field.name
        name = field.name.lower()

        # Skip wrapper artifacts (.list, .element) - these are internal schema patterns
        if name in ["list", "element"]:
            return

        # Common plural endings
        is_plural = name.endswith(("s", "es", "ies")) and not name.endswith(
            ("ss", "us", "is")
        )

        # Check if this is a STRUCT wrapper for an array (e.g., awards.list pattern)
        is_array_wrapper = False
        if field.field_type == "RECORD" and field.mode != "REPEATED":
            # Check if it has a single "list" field that's REPEATED
            if (
                len(field.fields) == 1
                and field.fields[0].name == "list"
                and field.fields[0].mode == "REPEATED"
            ):
                is_array_wrapper = True

        if field.mode == "REPEATED":
            # Array should have plural name
            if not is_plural and len(name) > 3:
                plural_singular_issues.append((field_path, "should_be_plural"))
        elif is_plural and field.mode != "REPEATED" and not is_array_wrapper:
            # Non-array with plural name (but skip array wrappers)
            plural_singular_issues.append((field_path, "should_be_singular"))

        if field.field_type == "RECORD":
            for sub_field in field.fields:
                check_pluralization(sub_field, field_path)

    for field in schema:
        check_pluralization(field)

    if plural_singular_issues:
        by_type = {}
        for field_path, issue_type in plural_singular_issues:
            by_type.setdefault(issue_type, []).append(field_path)

        for issue_type, fields in by_type.items():
            if issue_type == "should_be_plural":
                msg = f"ARRAY fields with singular names ({len(fields)} fields). Use plural names for arrays (user → users, item → items)."
            else:
                msg = f"Non-array fields with plural names ({len(fields)} fields). Use singular names for scalar/object fields."

            issues.append(
                {
                    "field_name": "schema",
                    "pattern": "plural_singular_confusion",
                    "severity": "info",
                    "category": "naming",
                    "suggestion": msg,
                    "affected_fields": fields,
                }
            )

    # Anti-pattern 35: Type suffixes in names
    type_suffix_fields = []

    def check_type_suffixes(field: SchemaField, path: str = "") -> None:
        """Check for type information in field names."""
        field_path = f"{path}.{field.name}" if path else field.name
        name_lower = field.name.lower()

        type_suffixes = [
            "_string",
            "_str",
            "_int",
            "_integer",
            "_bool",
            "_boolean",
            "_array",
            "_list",
            "_dict",
            "_map",
        ]

        if any(name_lower.endswith(suffix) for suffix in type_suffixes):
            type_suffix_fields.append(field_path)

        if field.field_type == "RECORD":
            for sub_field in field.fields:
                check_type_suffixes(sub_field, field_path)

    for field in schema:
        check_type_suffixes(field)

    if type_suffix_fields:
        issues.append(
            {
                "field_name": "schema",
                "pattern": "type_in_name",
                "severity": "info",
                "category": "naming",
                "suggestion": f"Field names include type information ({len(type_suffix_fields)} fields). Remove type suffixes - schema already defines types (user_id_string → user_id).",
                "affected_fields": type_suffix_fields,
            }
        )

    # Anti-pattern 36: Redundant prefixes
    def check_redundant_prefixes() -> None:
        """Check for redundant field prefixes."""
        if len(schema) < 5:
            return

        # Find most common prefix
        prefixes: dict[str, int] = {}
        for field in schema:
            parts = field.name.lower().split("_")
            if len(parts) > 1:
                prefix = parts[0]
                prefixes[prefix] = prefixes.get(prefix, 0) + 1

        if prefixes:
            most_common_prefix, count = max(prefixes.items(), key=lambda x: x[1])
            if count / len(schema) > 0.5:  # >50% of fields share prefix
                affected = [
                    f.name
                    for f in schema
                    if f.name.lower().startswith(most_common_prefix + "_")
                ]
                issues.append(
                    {
                        "field_name": "schema",
                        "pattern": "redundant_prefix",
                        "severity": "info",
                        "category": "naming",
                        "suggestion": f"Redundant prefix '{most_common_prefix}_' in {count} fields. If table is named '{most_common_prefix}', remove prefix (user_name → name).",
                        "affected_fields": affected[:20],
                    }
                )

    check_redundant_prefixes()

    # Anti-pattern 37: Denormalization abuse
    def check_denormalization() -> None:
        """Check for excessive denormalization."""
        field_prefixes: dict[str, int] = {}

        for field in schema:
            parts = field.name.lower().split("_")
            if len(parts) >= 2:
                # Look for patterns like company_name, company_address, company_city
                prefix = "_".join(parts[:2]) if len(parts) > 2 else parts[0]
                field_prefixes[prefix] = field_prefixes.get(prefix, 0) + 1

        # Find prefixes with many related fields
        denorm_groups = {
            prefix: count for prefix, count in field_prefixes.items() if count >= 4
        }

        if denorm_groups:
            top_group = max(denorm_groups.items(), key=lambda x: x[1])
            prefix, count = top_group
            affected = [f.name for f in schema if f.name.lower().startswith(prefix)]

            issues.append(
                {
                    "field_name": "schema",
                    "pattern": "denormalization_abuse",
                    "severity": "info",
                    "category": "normalization",
                    "suggestion": f"Multiple related fields with '{prefix}_' prefix ({count} fields). Consider creating a separate table or nested STRUCT for better organization.",
                    "affected_fields": affected[:20],
                }
            )

    check_denormalization()

    # Anti-pattern 38: EAV (Entity-Attribute-Value) pattern
    eav_fields = []

    def check_eav(field: SchemaField, path: str = "") -> None:
        """Check for EAV anti-pattern."""
        field_path = f"{path}.{field.name}" if path else field.name
        name_lower = field.name.lower()

        # Check for generic key-value structures
        if field.field_type == "RECORD" and field.mode == "REPEATED":
            if name_lower in [
                "attributes",
                "properties",
                "metadata",
                "tags",
                "custom_fields",
            ]:
                # Check if it has key/value structure
                field_names = {sf.name.lower() for sf in field.fields}
                if "key" in field_names or "name" in field_names:
                    if "value" in field_names:
                        eav_fields.append(field_path)

        if field.field_type == "RECORD":
            for sub_field in field.fields:
                check_eav(sub_field, field_path)

    for field in schema:
        check_eav(field)

    if eav_fields:
        issues.append(
            {
                "field_name": "schema",
                "pattern": "eav_antipattern",
                "severity": "warning",
                "category": "schema_design",
                "suggestion": f"EAV (key-value) pattern detected ({len(eav_fields)} fields). Loses type safety and makes queries complex. Define explicit fields or use JSON type with schema validation.",
                "affected_fields": eav_fields,
            }
        )

    # Anti-pattern 39: STRING for binary data
    binary_as_string = []

    def check_binary_strings(field: SchemaField, path: str = "") -> None:
        """Check for STRING fields that should be BYTES."""
        field_path = f"{path}.{field.name}" if path else field.name
        name_lower = field.name.lower()

        if field.field_type == "STRING":
            # Exclude URL, URI, path fields (these are text, not binary)
            if any(
                suffix in name_lower
                for suffix in ["_url", "_uri", "_path", "url", "uri", "link", "href"]
            ):
                return

            binary_keywords = [
                "hash",
                "digest",
                "signature",
                "checksum",
                "image_data",
                "file_content",
                "binary",
                "hex",
                "base64",
            ]
            if any(keyword in name_lower for keyword in binary_keywords):
                binary_as_string.append(field_path)

        if field.field_type == "RECORD":
            for sub_field in field.fields:
                check_binary_strings(sub_field, field_path)

    for field in schema:
        check_binary_strings(field)

    if binary_as_string:
        issues.append(
            {
                "field_name": "schema",
                "pattern": "string_for_binary",
                "severity": "info",
                "category": "type_optimization",
                "suggestion": f"STRING fields likely containing binary data ({len(binary_as_string)} fields). Use BYTES type for better storage efficiency and semantic clarity.",
                "affected_fields": binary_as_string,
            }
        )

    # Anti-pattern 40: Missing soft delete flag
    has_soft_delete = any(
        f.name.lower() in ["is_deleted", "deleted", "deleted_at", "soft_deleted"]
        for f in schema
    )
    has_crud_timestamps = any(
        f.name.lower() in ["created_at", "updated_at"] for f in schema
    )

    if has_crud_timestamps and not has_soft_delete:
        issues.append(
            {
                "field_name": "schema",
                "pattern": "missing_soft_delete",
                "severity": "info",
                "category": "data_quality",
                "suggestion": "Table has audit timestamps but no soft delete mechanism. Add is_deleted or deleted_at field to preserve data history and enable recovery.",
                "affected_fields": [],
            }
        )

    # Anti-pattern 41: Inconsistent date granularity
    date_fields_by_type: dict[str, list[str]] = {}

    def collect_date_types(field: SchemaField, path: str = "") -> None:
        """Collect date fields by type."""
        field_path = f"{path}.{field.name}" if path else field.name
        name_lower = field.name.lower()

        if field.field_type in ["DATE", "TIMESTAMP", "DATETIME"]:
            if "_date" in name_lower or name_lower.endswith("date"):
                date_fields_by_type.setdefault(field.field_type, []).append(field_path)

        if field.field_type == "RECORD":
            for sub_field in field.fields:
                collect_date_types(sub_field, field_path)

    for field in schema:
        collect_date_types(field)

    if len(date_fields_by_type) > 1:
        type_summary = ", ".join(
            f"{len(fields)} {ftype}" for ftype, fields in date_fields_by_type.items()
        )
        all_date_fields = [f for fields in date_fields_by_type.values() for f in fields]

        issues.append(
            {
                "field_name": "schema",
                "pattern": "inconsistent_date_granularity",
                "severity": "info",
                "category": "type_consistency",
                "suggestion": f"Date fields have inconsistent types ({type_summary}). Standardize on appropriate granularity: DATE for dates, TIMESTAMP for events.",
                "affected_fields": all_date_fields,
            }
        )

    # Anti-pattern 42: Mixed NULL representations
    def check_null_representation() -> None:
        """Check for fields suggesting multiple null representations."""
        suspect_fields = []

        for field in schema:
            desc = (field.description or "").lower()
            # Check for descriptions mentioning alternative null representations
            null_mentions = [
                "empty string",
                "0 means",
                "blank means",
                "null string",
                '""',
                "n/a",
            ]
            if any(mention in desc for mention in null_mentions):
                suspect_fields.append(field.name)

        if suspect_fields:
            issues.append(
                {
                    "field_name": "schema",
                    "pattern": "mixed_null_representation",
                    "severity": "info",
                    "category": "data_quality",
                    "suggestion": f"Fields with alternative NULL representations ({len(suspect_fields)} fields). Use proper NULL values, not empty strings or magic numbers.",
                    "affected_fields": suspect_fields,
                }
            )

    check_null_representation()

    # Anti-pattern 43: Inconsistent boolean representation
    boolean_fields_by_type: dict[str, list[str]] = {}

    def collect_boolean_types(field: SchemaField, path: str = "") -> None:
        """Collect boolean-semantic fields by type."""
        field_path = f"{path}.{field.name}" if path else field.name
        name_lower = field.name.lower()

        # Boolean semantic names
        if any(
            name_lower.startswith(p)
            for p in ["is_", "has_", "can_", "should_", "will_"]
        ) or any(
            name_lower.endswith(p)
            for p in ["_enabled", "_active", "_visible", "_published"]
        ):
            boolean_fields_by_type.setdefault(field.field_type, []).append(field_path)

        if field.field_type == "RECORD":
            for sub_field in field.fields:
                collect_boolean_types(sub_field, field_path)

    for field in schema:
        collect_boolean_types(field)

    if len(boolean_fields_by_type) > 1:
        type_summary = ", ".join(
            f"{len(fields)} {ftype}" for ftype, fields in boolean_fields_by_type.items()
        )
        all_bool_fields = [
            f for fields in boolean_fields_by_type.values() for f in fields
        ]

        issues.append(
            {
                "field_name": "schema",
                "pattern": "inconsistent_boolean_type",
                "severity": "warning",
                "category": "type_consistency",
                "suggestion": f"Boolean-semantic fields use inconsistent types ({type_summary}). Standardize on BOOLEAN type for all true/false fields.",
                "affected_fields": all_bool_fields,
            }
        )

    # Anti-pattern 44: Nullable partition key (BigQuery)
    # This would require table metadata - checking field names that suggest partitioning
    partition_field_names = ["partition_date", "event_date", "date", "_partitiontime"]

    for field in schema:
        if field.name.lower() in partition_field_names and field.mode == "NULLABLE":
            issues.append(
                {
                    "field_name": "schema",
                    "pattern": "nullable_partition_field",
                    "severity": "warning",
                    "category": "performance",
                    "suggestion": f"Field '{field.name}' appears to be a partition key but is NULLABLE. Partition fields should be REQUIRED for optimal query performance.",
                    "affected_fields": [field.name],
                }
            )

    # Anti-pattern 45: Over-structuring (small structs that should be flat)
    small_structs = []

    def check_small_structs(field: SchemaField, path: str = "") -> None:
        """Check for over-structuring."""
        field_path = f"{path}.{field.name}" if path else field.name

        if field.field_type == "RECORD" and field.mode != "REPEATED":
            # Check for small structs (<=2 simple fields) at top level
            if not path and len(field.fields) <= 2:
                # Check if all sub-fields are simple types
                all_simple = all(
                    sf.field_type
                    in ["STRING", "INTEGER", "FLOAT", "BOOLEAN", "TIMESTAMP", "DATE"]
                    for sf in field.fields
                )
                if all_simple:
                    small_structs.append(field_path)

        if field.field_type == "RECORD":
            for sub_field in field.fields:
                check_small_structs(sub_field, field_path)

    for field in schema:
        check_small_structs(field)

    if small_structs:
        issues.append(
            {
                "field_name": "schema",
                "pattern": "over_structuring",
                "severity": "info",
                "category": "schema_design",
                "suggestion": f"Small STRUCT fields that should be flattened ({len(small_structs)} structs). Flatten commonly-used fields to top level for simpler queries.",
                "affected_fields": small_structs,
            }
        )

    return issues
