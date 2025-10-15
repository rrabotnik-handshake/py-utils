#!/usr/bin/env python3
"""Unified schema loader that can return either legacy or unified format.

This module provides a single interface for loading schemas from any supported format
and returning them in either the legacy (tree, required_paths) format or the new unified
Schema object format.
"""
from __future__ import annotations

from .models import Schema


def load_schema_unified(
    file_path: str,
    schema_type: str,
    *,
    table: str | None = None,
    model: str | None = None,
    message: str | None = None,
) -> Schema:
    """Load a schema from any supported format in unified Schema format.

    Parameters
    ----------
    file_path : str
        Path to the schema file
    schema_type : str
        Type of schema (json_schema, spark, sql, protobuf, dbt-manifest, dbt-yml, dbt-model, bigquery)
    table : str, optional
        Table name for SQL schemas
    model : str, optional
        Model name for dbt schemas
    message : str, optional
        Message name for Protobuf schemas

    Returns
    -------
    Schema
        Unified Schema object
    """
    # Normalize schema type
    if schema_type == "jsonschema":
        schema_type = "json_schema"

    return _load_unified_schema(
        file_path, schema_type, table=table, model=model, message=message
    )


def _load_unified_schema(
    file_path: str,
    schema_type: str,
    *,
    table: str | None = None,
    model: str | None = None,
    message: str | None = None,
) -> Schema:
    """Load schema in unified format."""
    if schema_type == "json_schema":
        from .json_schema_parser import schema_from_json_schema_file_unified

        return schema_from_json_schema_file_unified(file_path)  # type: ignore[no-any-return]

    elif schema_type == "spark":
        from .spark_schema_parser import schema_from_spark_schema_file_unified

        return schema_from_spark_schema_file_unified(file_path)  # type: ignore[no-any-return]

    elif schema_type == "sql":
        from .sql_schema_parser import schema_from_sql_schema_file_unified

        return schema_from_sql_schema_file_unified(file_path, table=table)  # type: ignore[no-any-return]

    elif schema_type == "protobuf":
        from .protobuf_schema_parser import schema_from_protobuf_file_unified

        return schema_from_protobuf_file_unified(file_path, message=message)  # type: ignore[no-any-return]

    elif schema_type == "dbt-manifest":
        from .dbt_schema_parser import schema_from_dbt_manifest_unified

        return schema_from_dbt_manifest_unified(file_path, model=model)  # type: ignore[no-any-return]

    elif schema_type == "dbt-yml":
        from .dbt_schema_parser import schema_from_dbt_schema_yml_unified

        return schema_from_dbt_schema_yml_unified(file_path, model=model)  # type: ignore[no-any-return]

    elif schema_type == "dbt-model":
        from .dbt_schema_parser import schema_from_dbt_model_unified

        return schema_from_dbt_model_unified(file_path)  # type: ignore[no-any-return]

    elif schema_type == "bigquery":
        from google.cloud import bigquery

        from .bigquery_ddl import bigquery_schema_to_internal
        from .models import from_legacy_tree

        # Parse BigQuery table reference
        if ":" in file_path:
            project_part, table_part = file_path.split(":", 1)
        else:
            project_part = None
            table_part = file_path

        if "." in table_part:
            dataset_id, table_id = table_part.split(".", 1)
        else:
            raise ValueError(f"Invalid BigQuery reference: {file_path}")

        # Get project ID
        if project_part:
            project_id = project_part
        else:
            client = bigquery.Client()
            project_id = client.project

        # Get raw BigQuery schema for anti-pattern detection
        client = bigquery.Client(project=project_id)
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        bq_table = client.get_table(table_ref)
        raw_bq_schema = bq_table.schema

        # Convert to internal format (this flattens unnecessary wrappers)
        tree, required = bigquery_schema_to_internal(raw_bq_schema)

        # Create Schema object with raw schema in metadata
        schema = from_legacy_tree(tree, required, source_type="bigquery")
        schema.metadata["raw_bq_schema"] = raw_bq_schema
        schema.source_path = file_path

        return schema

    else:
        raise ValueError(f"Unsupported schema type for unified loading: {schema_type}")


__all__ = ["load_schema_unified"]
