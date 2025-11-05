#!/usr/bin/env python3
"""BigQuery INFORMATION_SCHEMA queries and constraint handling.

This module provides SQL query templates and functions for retrieving
primary key and foreign key constraints from BigQuery tables via INFORMATION_SCHEMA.
"""
from __future__ import annotations

import logging
from typing import Any

from google.cloud import bigquery

logger = logging.getLogger(__name__)

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


# =============================================================================
# Dataset Location Handling
# =============================================================================


def get_dataset_location(
    client: bigquery.Client, project_id: str, dataset_id: str
) -> str:
    """Get the location (region) of a BigQuery dataset.

    This is critical for cross-region query compatibility: INFORMATION_SCHEMA queries
    must be executed in the same region as the dataset, otherwise they fail.

    Args:
        client: BigQuery client instance
        project_id: GCP project ID
        dataset_id: Dataset ID

    Returns:
        Dataset location (e.g., "US", "europe-west1")
    """
    try:
        ds = client.get_dataset(f"{project_id}.{dataset_id}")
        return str(ds.location)
    except Exception as e:
        logger.warning(
            "Failed to get location for %s.%s, using client default: %s",
            project_id,
            dataset_id,
            e,
        )
        # Fallback to US (uses client's default location)
        return "US"  # Safe default for most cases


# =============================================================================
# Constraint Retrieval
# =============================================================================


def get_constraints(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_id: str,
    retry_fn: Any = None,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """Retrieve PK and FK constraints using INFORMATION_SCHEMA with parameters.

    Args:
        client: BigQuery client instance
        project_id: GCP project ID
        dataset_id: Dataset ID
        table_id: Table ID
        retry_fn: Optional retry function wrapper (defaults to direct execution)

    Returns:
        (primary_key_dict, foreign_keys_list)
        - primary_key_dict: {"constraint_name": str, "columns": list[str]} or None
        - foreign_keys_list: list of FK dicts with constraint_name, columns, referenced table/columns
    """

    # Default to direct execution if no retry function provided
    def _default_retry(fn, **kwargs):
        return fn()

    if retry_fn is None:
        retry_fn = _default_retry

    try:
        # Get dataset location for cross-region compatibility
        location = get_dataset_location(client, project_id, dataset_id)

        # Create base job config with location
        job_config_base = bigquery.QueryJobConfig()
        job_config_base.location = location

        # Query PK with retry on transient errors
        job_config_pk = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("table_name", "STRING", table_id),
                bigquery.ScalarQueryParameter("dataset_name", "STRING", dataset_id),
            ]
        )
        job_config_pk.location = location

        pk_rows = retry_fn(
            lambda: list(
                client.query(
                    PK_SQL.format(project=project_id, dataset=dataset_id),
                    job_config=job_config_pk,
                ).result()
            ),
            operation_name=f"PK query for {project_id}.{dataset_id}.{table_id}",
        )
        # Return PK with constraint name
        primary_key: dict[str, Any] | None = (
            {
                "constraint_name": pk_rows[0].constraint_name,
                "columns": list(pk_rows[0].columns),
            }
            if pk_rows
            else None
        )

        # Query FK with retry on transient errors
        job_config_fk = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("table_name", "STRING", table_id),
                bigquery.ScalarQueryParameter("dataset_name", "STRING", dataset_id),
            ]
        )
        job_config_fk.location = location

        fk_rows = retry_fn(
            lambda: list(
                client.query(
                    FK_SQL.format(project=project_id, dataset=dataset_id),
                    job_config=job_config_fk,
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
        return primary_key, foreign_keys

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
        return None, []


def get_batch_constraints(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_ids: list[str],
    retry_fn: Any = None,
) -> dict[str, tuple[dict[str, Any] | None, list[dict[str, Any]]]]:
    """Retrieve constraints for multiple tables in a single query.

    Args:
        client: BigQuery client instance
        project_id: GCP project ID
        dataset_id: Dataset ID
        table_ids: List of table IDs to query
        retry_fn: Optional retry function wrapper (defaults to direct execution)

    Returns:
        Dictionary mapping table_id to (primary_key_dict, foreign_keys_list)
        - primary_key_dict: {"constraint_name": str, "columns": list[str]} or None
        - foreign_keys_list: list of FK dicts with constraint_name, columns, referenced table/columns
    """
    if not table_ids:
        return {}

    # Default to direct execution if no retry function provided
    def _default_retry(fn, **kwargs):
        return fn()

    if retry_fn is None:
        retry_fn = _default_retry

    try:
        # Get dataset location for cross-region compatibility
        location = get_dataset_location(client, project_id, dataset_id)

        # Query batch constraints with retry on transient errors
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("table_names", "STRING", table_ids),
                bigquery.ScalarQueryParameter("dataset_name", "STRING", dataset_id),
            ]
        )
        job_config.location = location

        rows = retry_fn(
            lambda: list(
                client.query(
                    BATCH_CONSTRAINTS_SQL.format(
                        project=project_id, dataset=dataset_id
                    ),
                    job_config=job_config,
                ).result()
            ),
            operation_name=f"Batch constraints query for {project_id}.{dataset_id}",
        )

        results: dict[str, tuple[dict[str, Any] | None, list[dict[str, Any]]]] = {}
        for table_id in table_ids:
            results[table_id] = (None, [])

        for row in rows:
            table_name = row["table_name"]
            constraint_type = row["constraint_type"]

            if constraint_type == "PRIMARY KEY":
                pk_dict = {
                    "constraint_name": row["constraint_name"],
                    "columns": list(row["columns"]),
                }
                results[table_name] = (pk_dict, results[table_name][1])
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
        return {table_id: (None, []) for table_id in table_ids}


__all__ = [
    "PK_SQL",
    "FK_SQL",
    "BATCH_CONSTRAINTS_SQL",
    "get_dataset_location",
    "get_constraints",
    "get_batch_constraints",
]
