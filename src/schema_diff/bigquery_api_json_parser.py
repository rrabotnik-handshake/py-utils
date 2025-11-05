#!/usr/bin/env python3
"""BigQuery API JSON schema parser.

Parses BigQuery table schema JSON files in the format returned by the BigQuery API.
These files contain table metadata and a 'schema.fields' array with field definitions.

Example format:
{
  "id": "project:dataset.table",
  "kind": "bigquery#table",
  "schema": {
    "fields": [
      {
        "name": "field_name",
        "type": "STRING",
        "mode": "NULLABLE"
      },
      ...
    ]
  }
}
"""
from __future__ import annotations

import json
from typing import Any, Set, Tuple

from .bigquery_schema import bigquery_schema_to_internal
from .io_utils import open_text
from .logging_config import get_logger

logger = get_logger(__name__)

__all__ = ["schema_from_bigquery_api_json_file"]


def _field_dict_to_schema_field(field_dict: dict[str, Any]) -> Any:
    """Convert a BigQuery API field dict to a SchemaField object.

    Args:
        field_dict: Dictionary representation of a BigQuery field

    Returns:
        SchemaField object
    """
    try:
        from google.cloud.bigquery import SchemaField
    except ImportError as e:
        raise ImportError(
            "BigQuery support requires google-cloud-bigquery. "
            "Install with: pip install 'schema-diff[bigquery]'"
        ) from e

    name = field_dict.get("name")
    field_type = field_dict.get("type")
    mode = field_dict.get("mode", "NULLABLE")
    description = field_dict.get("description")
    fields = field_dict.get("fields")

    # Convert nested fields for RECORD types
    sub_fields = None
    if fields:
        sub_fields = tuple(_field_dict_to_schema_field(f) for f in fields)

    # Handle policy tags if present
    policy_tags = None
    if "policyTags" in field_dict or "policy_tags" in field_dict:
        policy_dict = field_dict.get("policyTags") or field_dict.get("policy_tags")
        if isinstance(policy_dict, dict) and "names" in policy_dict:
            # PolicyTagList format
            from google.cloud.bigquery import PolicyTagList

            policy_tags = PolicyTagList(names=policy_dict["names"])

    return SchemaField(
        name=name,
        field_type=field_type,
        mode=mode,
        description=description,
        fields=sub_fields or (),
        policy_tags=policy_tags,
    )


def schema_from_bigquery_api_json_file(path: str) -> Tuple[dict[str, Any], Set[str]]:
    """Parse a BigQuery API JSON schema file.

    This function reads BigQuery table schema JSON files in the format returned
    by the BigQuery API (e.g., from bq show --format=json or API get_table calls).

    Args:
        path: Path to BigQuery API JSON file

    Returns:
        Tuple of (schema_tree, required_paths)
        - schema_tree: Internal schema-diff type tree
        - required_paths: Set of dotted paths that are REQUIRED

    Raises:
        ValueError: If file is not in BigQuery API JSON format
        ImportError: If google-cloud-bigquery is not installed
    """
    with open_text(path) as f:
        data = json.load(f)

    # Validate that this looks like a BigQuery API response
    if not isinstance(data, dict):
        raise ValueError(
            f"Invalid BigQuery API JSON: expected dict, got {type(data).__name__}"
        )

    # Check for BigQuery API markers
    if "kind" not in data or not data.get("kind", "").startswith("bigquery#"):
        raise ValueError(
            "Invalid BigQuery API JSON: missing 'kind' field or not a BigQuery resource"
        )

    # Extract schema.fields
    schema_obj = data.get("schema")
    if not schema_obj or not isinstance(schema_obj, dict):
        raise ValueError("Invalid BigQuery API JSON: missing or invalid 'schema' field")

    fields_array = schema_obj.get("fields")
    if not isinstance(fields_array, list):
        raise ValueError("Invalid BigQuery API JSON: 'schema.fields' must be a list")

    logger.info(
        "Parsing BigQuery API JSON: %s (%d fields)",
        data.get("id", path),
        len(fields_array),
    )

    # Convert field dicts to SchemaField objects
    schema_fields = []
    for field_dict in fields_array:
        try:
            schema_field = _field_dict_to_schema_field(field_dict)
            schema_fields.append(schema_field)
        except Exception as e:
            logger.warning(
                "Error converting field '%s': %s", field_dict.get("name", "?"), e
            )
            raise

    # Use existing conversion logic
    tree, required_paths = bigquery_schema_to_internal(schema_fields)

    return tree, required_paths


def is_bigquery_api_json(path: str) -> bool:
    """Check if a file appears to be BigQuery API JSON format.

    Args:
        path: Path to file

    Returns:
        True if file appears to be BigQuery API JSON
    """
    try:
        with open_text(path) as f:
            data = json.load(f)

        if not isinstance(data, dict):
            return False

        # Check for BigQuery API markers
        if "kind" in data and data.get("kind", "").startswith("bigquery#"):
            # Also check for schema.fields
            schema_obj = data.get("schema", {})
            if isinstance(schema_obj, dict) and "fields" in schema_obj:
                return True

        return False
    except Exception:
        return False
