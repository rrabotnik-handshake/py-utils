"""BigQuery client utilities for schema-diff.

This module provides centralized BigQuery client creation and project resolution,
eliminating duplication across the codebase.
"""

from __future__ import annotations

import os
from typing import Optional

from .exceptions import BigQueryError, DependencyError


def get_bigquery_client(project_id: Optional[str] = None):
    """Get a BigQuery client with optional project override.

    This function centralizes BigQuery client creation across the codebase,
    providing consistent error handling and dependency management.

    Parameters
    ----------
    project_id : str, optional
        Optional project ID. If None, uses default from environment or credentials.

    Returns
    -------
    google.cloud.bigquery.Client
        Configured BigQuery client

    Raises
    ------
    DependencyError
        If google-cloud-bigquery is not installed
    BigQueryError
        If client creation fails for any other reason

    Examples
    --------
    >>> # Get client with default project
    >>> client = get_bigquery_client()

    >>> # Get client for specific project
    >>> client = get_bigquery_client("my-project-id")
    """
    try:
        from google.cloud import bigquery  # type: ignore[import]
    except ImportError as e:
        raise DependencyError(
            "BigQuery functionality requires google-cloud-bigquery. "
            "Install with: pip install google-cloud-bigquery",
            dependency_name="google-cloud-bigquery",
            cause=e,
        ) from e

    try:
        if project_id:
            return bigquery.Client(project=project_id)
        return bigquery.Client()
    except Exception as e:
        raise BigQueryError(
            f"Failed to create BigQuery client: {e}",
            project_id=project_id,
            operation="client_init",
            cause=e,
        ) from e


def get_default_bigquery_project() -> str:
    """Get the default BigQuery project from environment or client.

    Tries environment variables first (GOOGLE_CLOUD_PROJECT, GCP_PROJECT),
    then falls back to creating a client and extracting its project.

    Returns
    -------
    str
        The default BigQuery project ID

    Raises
    ------
    BigQueryError
        If unable to determine default project

    Examples
    --------
    >>> project_id = get_default_bigquery_project()
    >>> print(f"Using project: {project_id}")
    """
    # Try environment variables first (both common variants)
    project = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")
    if project:
        return project

    # Try to get from BigQuery client
    try:
        client = get_bigquery_client()
        if client.project:
            return str(client.project)
        raise BigQueryError(
            "BigQuery client has no project configured. "
            "Set GOOGLE_CLOUD_PROJECT environment variable or specify project explicitly.",
            operation="get_default_project",
        )
    except Exception as e:
        if isinstance(e, BigQueryError):
            raise
        raise BigQueryError(
            "Unable to determine default BigQuery project. "
            "Set GOOGLE_CLOUD_PROJECT environment variable or configure gcloud auth.",
            operation="get_default_project",
            cause=e,
        ) from e


def parse_bigquery_table_ref(table_ref: str) -> tuple[str, str, str]:
    """Parse a BigQuery table reference into components.

    Supports multiple formats:
    - project:dataset.table
    - project.dataset.table
    - dataset.table (uses default project)

    Parameters
    ----------
    table_ref : str
        BigQuery table reference string

    Returns
    -------
    tuple[str, str, str]
        Tuple of (project_id, dataset_id, table_id)

    Raises
    ------
    BigQueryError
        If table reference format is invalid

    Examples
    --------
    >>> project, dataset, table = parse_bigquery_table_ref("proj:ds.tbl")
    >>> print(f"{project}.{dataset}.{table}")
    proj.ds.tbl

    >>> # Uses default project
    >>> project, dataset, table = parse_bigquery_table_ref("dataset.table")
    """
    from .exceptions import ArgumentError

    # Handle project:dataset.table format
    if ":" in table_ref:
        project_part, table_part = table_ref.split(":", 1)
        if "." not in table_part:
            raise ArgumentError(
                f"Invalid BigQuery table reference: {table_ref}. "
                "Expected format: project:dataset.table",
                argument_value=table_ref,
            )
        dataset_id, table_id = table_part.split(".", 1)
        return project_part, dataset_id, table_id

    # Handle project.dataset.table or dataset.table format
    parts = table_ref.split(".")
    if len(parts) == 3:
        # project.dataset.table
        return parts[0], parts[1], parts[2]
    elif len(parts) == 2:
        # dataset.table - need default project
        try:
            project_id = get_default_bigquery_project()
        except BigQueryError:
            raise ArgumentError(
                f"No project specified in table reference '{table_ref}' and unable to determine default project. "
                "Use format: project:dataset.table or set GOOGLE_CLOUD_PROJECT",
                argument_value=table_ref,
            ) from None
        return project_id, parts[0], parts[1]
    else:
        raise ArgumentError(
            f"Invalid BigQuery table reference: {table_ref}. "
            "Expected format: project:dataset.table, project.dataset.table, or dataset.table",
            argument_value=table_ref,
        )


def parse_bigquery_dataset_ref(dataset_ref: str) -> tuple[str, str]:
    """Parse a BigQuery dataset reference into components.

    Supports formats:
    - project:dataset
    - project.dataset
    - dataset (uses default project)

    Parameters
    ----------
    dataset_ref : str
        BigQuery dataset reference string

    Returns
    -------
    tuple[str, str]
        Tuple of (project_id, dataset_id)

    Raises
    ------
    BigQueryError
        If dataset reference format is invalid

    Examples
    --------
    >>> project, dataset = parse_bigquery_dataset_ref("myproject:mydataset")
    >>> print(f"{project}.{dataset}")
    myproject.mydataset
    """
    from .exceptions import ArgumentError

    # Handle project:dataset format
    if ":" in dataset_ref:
        parts = dataset_ref.split(":", 1)
        if len(parts) != 2:
            raise ArgumentError(
                f"Invalid BigQuery dataset reference: {dataset_ref}. "
                "Expected format: project:dataset",
                argument_value=dataset_ref,
            )
        return parts[0], parts[1]

    # Handle project.dataset or just dataset
    parts = dataset_ref.split(".")
    if len(parts) == 2:
        return parts[0], parts[1]
    elif len(parts) == 1:
        # Just dataset - need default project
        try:
            project_id = get_default_bigquery_project()
        except BigQueryError:
            raise ArgumentError(
                f"No project specified in dataset reference '{dataset_ref}' and unable to determine default project. "
                "Use format: project:dataset or set GOOGLE_CLOUD_PROJECT",
                argument_value=dataset_ref,
            ) from None
        return project_id, parts[0]
    else:
        raise ArgumentError(
            f"Invalid BigQuery dataset reference: {dataset_ref}. "
            "Expected format: project:dataset, project.dataset, or dataset",
            argument_value=dataset_ref,
        )


__all__ = [
    "get_bigquery_client",
    "get_default_bigquery_project",
    "parse_bigquery_table_ref",
    "parse_bigquery_dataset_ref",
]
