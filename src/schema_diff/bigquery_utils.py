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

    Accepts only standard BigQuery format:
    - project:dataset.table (standard format)
    - dataset.table (uses default project from GOOGLE_CLOUD_PROJECT env var)

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
    ArgumentError
        If table reference format is invalid

    Examples
    --------
    >>> project, dataset, table = parse_bigquery_table_ref("proj:ds.tbl")
    >>> print(f"{project}.{dataset}.{table}")
    proj.ds.tbl

    >>> # Uses default project from GOOGLE_CLOUD_PROJECT env var
    >>> project, dataset, table = parse_bigquery_table_ref("dataset.table")
    """
    from .exceptions import ArgumentError

    # Standard format: project:dataset.table
    if ":" in table_ref:
        parts = table_ref.split(":")

        # Reject if multiple colons (e.g., "project1:project2:dataset.table")
        if len(parts) != 2:
            raise ArgumentError(
                f"Invalid BigQuery table reference: {table_ref}. "
                "Expected format: project:dataset.table",
                argument_value=table_ref,
            )

        project_part = parts[0]
        table_part = parts[1]

        # Validate project part is not empty
        if not project_part:
            raise ArgumentError(
                f"Invalid BigQuery table reference: {table_ref}. "
                "Project name cannot be empty. Expected format: project:dataset.table",
                argument_value=table_ref,
            )

        # Validate dataset.table part
        if "." not in table_part:
            raise ArgumentError(
                f"Invalid BigQuery table reference: {table_ref}. "
                "Expected format: project:dataset.table",
                argument_value=table_ref,
            )

        dataset_table_parts = table_part.split(".", 1)
        dataset_id = dataset_table_parts[0]
        table_id = dataset_table_parts[1]

        # Validate dataset and table are not empty
        if not dataset_id or not table_id:
            raise ArgumentError(
                f"Invalid BigQuery table reference: {table_ref}. "
                "Dataset and table names cannot be empty. Expected format: project:dataset.table",
                argument_value=table_ref,
            )

        return project_part, dataset_id, table_id

    # Alternative format: dataset.table (requires default project)
    parts = table_ref.split(".")
    if len(parts) == 2:
        # dataset.table - need default project
        dataset_id = parts[0]
        table_id = parts[1]

        if not dataset_id or not table_id:
            raise ArgumentError(
                f"Invalid BigQuery table reference: {table_ref}. "
                "Dataset and table names cannot be empty.",
                argument_value=table_ref,
            )

        try:
            project_id = get_default_bigquery_project()
        except BigQueryError:
            raise ArgumentError(
                f"No project specified in table reference '{table_ref}' and unable to determine default project. "
                "Use format: project:dataset.table or set GOOGLE_CLOUD_PROJECT env var",
                argument_value=table_ref,
            ) from None
        return project_id, dataset_id, table_id
    else:
        # Reject project.dataset.table format and other invalid formats
        raise ArgumentError(
            f"Invalid BigQuery table reference: {table_ref}. "
            "Expected format: project:dataset.table or dataset.table",
            argument_value=table_ref,
        )


def parse_bigquery_dataset_ref(dataset_ref: str) -> tuple[str, str]:
    """Parse a BigQuery dataset reference into components.

    Accepts only standard BigQuery format:
    - project:dataset (standard format)
    - dataset (uses default project from GOOGLE_CLOUD_PROJECT env var)

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
    ArgumentError
        If dataset reference format is invalid

    Examples
    --------
    >>> project, dataset = parse_bigquery_dataset_ref("myproject:mydataset")
    >>> print(f"{project}.{dataset}")
    myproject.mydataset
    """
    from .exceptions import ArgumentError

    # Standard format: project:dataset
    if ":" in dataset_ref:
        parts = dataset_ref.split(":")

        # Reject if multiple colons
        if len(parts) != 2:
            raise ArgumentError(
                f"Invalid BigQuery dataset reference: {dataset_ref}. "
                "Expected format: project:dataset",
                argument_value=dataset_ref,
            )

        project_part = parts[0]
        dataset_part = parts[1]

        # Validate both parts are not empty
        if not project_part or not dataset_part:
            raise ArgumentError(
                f"Invalid BigQuery dataset reference: {dataset_ref}. "
                "Project and dataset names cannot be empty. Expected format: project:dataset",
                argument_value=dataset_ref,
            )

        return project_part, dataset_part

    # Single name format: just dataset (requires default project)
    if "." not in dataset_ref:
        dataset_id = dataset_ref.strip()

        if not dataset_id:
            raise ArgumentError(
                f"Invalid BigQuery dataset reference: {dataset_ref}. "
                "Dataset name cannot be empty.",
                argument_value=dataset_ref,
            )

        try:
            project_id = get_default_bigquery_project()
        except BigQueryError:
            raise ArgumentError(
                f"No project specified in dataset reference '{dataset_ref}' and unable to determine default project. "
                "Use format: project:dataset or set GOOGLE_CLOUD_PROJECT env var",
                argument_value=dataset_ref,
            ) from None
        return project_id, dataset_id
    else:
        # Reject project.dataset format and other invalid formats
        raise ArgumentError(
            f"Invalid BigQuery dataset reference: {dataset_ref}. "
            "Expected format: project:dataset or dataset",
            argument_value=dataset_ref,
        )


__all__ = [
    "get_bigquery_client",
    "get_default_bigquery_project",
    "parse_bigquery_table_ref",
    "parse_bigquery_dataset_ref",
]
