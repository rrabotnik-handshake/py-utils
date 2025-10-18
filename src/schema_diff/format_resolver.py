#!/usr/bin/env python3
"""Format resolver for schema-diff CLI.

Handles parsing and validation of <family>:<representation> format strings.
Provides backward compatibility through aliases.
"""
from __future__ import annotations

from typing import Tuple

# Supported families and their valid representations
FAMILY_REPRESENTATIONS = {
    "spark": ["json", "ddl", "tree"],
    "bq": ["table", "json", "ddl"],
    "sql": ["ddl"],
    "dbt": ["manifest", "yml", "model"],
    "jsonschema": ["json"],
    "proto": ["sdl"],
    "data": ["json", "jsonl", "parquet", "orc", "csv"],
}

# Default representation for each family when not specified
FAMILY_DEFAULTS = {
    "spark": "json",
    "bq": "table",
    "sql": "ddl",
    "dbt": "manifest",
    "jsonschema": "json",
    "proto": "sdl",
    "data": "json",
}

# Aliases for backward compatibility (case-insensitive)
# Maps legacy format names to new family:representation format
ALIASES = {
    # Spark formats
    "spark": "spark:json",
    "spark-tree": "spark:tree",
    "spark-json": "spark:json",
    "spark-ddl": "spark:ddl",
    # BigQuery formats
    "bigquery": "bq:table",
    "bq-table": "bq:table",
    "bq-json": "bq:json",
    "bq-ddl": "bq:ddl",
    # SQL formats
    "sql": "sql:ddl",
    "sql-ddl": "sql:ddl",
    # JSON Schema
    "jsonschema": "jsonschema:json",
    "json_schema": "jsonschema:json",
    "json-schema": "jsonschema:json",
    # Protobuf
    "protobuf": "proto:sdl",
    # dbt formats
    "dbt-manifest": "dbt:manifest",
    "dbt-yml": "dbt:yml",
    "dbt-yaml": "dbt:yml",
    "dbt-model": "dbt:model",
    # Data formats
    "data": "data:json",
    "json": "data:json",
    "ndjson": "data:jsonl",
    "jsonl": "data:jsonl",
    "parquet": "data:parquet",
    "orc": "data:orc",
    "csv": "data:csv",
}


def parse_format(format_string: str) -> Tuple[str, str]:
    """Parse format string into (family, representation).

    Args:
        format_string: Format like "spark:tree" or legacy "spark"

    Returns:
        Tuple of (family, representation)

    Raises:
        ValueError: If format is invalid

    Examples:
        >>> parse_format("spark:tree")
        ('spark', 'tree')

        >>> parse_format("spark")
        ('spark', 'json')

        >>> parse_format("bigquery")
        ('bq', 'table')
    """
    if not format_string:
        raise ValueError("Format string cannot be empty")

    # Normalize to lowercase
    format_string = format_string.lower().strip()

    # Check aliases first
    if format_string in ALIASES:
        format_string = ALIASES[format_string]

    # Parse family:representation
    if ":" in format_string:
        parts = format_string.split(":", 1)
        if len(parts) != 2:
            raise ValueError(
                f"Invalid format '{format_string}': expected 'family:representation'"
            )
        family, representation = parts
        family = family.strip()
        representation = representation.strip()
    else:
        # No colon, assume it's just family - use default representation
        family = format_string
        if family not in FAMILY_DEFAULTS:
            raise ValueError(
                f"Unknown family '{family}'. Valid families: {', '.join(FAMILY_REPRESENTATIONS.keys())}"
            )
        representation = FAMILY_DEFAULTS[family]

    # Validate family
    if family not in FAMILY_REPRESENTATIONS:
        valid_families = ", ".join(sorted(FAMILY_REPRESENTATIONS.keys()))
        raise ValueError(f"Unknown family '{family}'. Valid families: {valid_families}")

    # Validate representation
    valid_reps = FAMILY_REPRESENTATIONS[family]
    if representation not in valid_reps:
        valid_reps_str = ", ".join(valid_reps)
        raise ValueError(
            f"Invalid representation '{representation}' for family '{family}'. "
            f"Valid options: {valid_reps_str}"
        )

    return family, representation


def format_to_internal_kind(family: str, representation: str) -> str:
    """Convert (family, representation) to internal kind format.

    Internal kind is now family:representation (e.g., "spark:tree", "bq:table").

    Args:
        family: Format family (e.g., "spark", "bq")
        representation: Format representation (e.g., "tree", "table")

    Returns:
        Internal kind string in family:representation format
    """
    return f"{family}:{representation}"


def resolve_format(format_string: str | None) -> str | None:
    """Resolve format string to internal kind.

    Convenience function that combines parse_format and format_to_internal_kind.

    Args:
        format_string: Format string or None (for auto-detection)

    Returns:
        Internal kind string or None if format_string is None

    Raises:
        ValueError: If format is invalid
    """
    if format_string is None:
        return None

    family, representation = parse_format(format_string)
    return format_to_internal_kind(family, representation)


def get_family(format_string: str) -> str:
    """Extract just the family from a format string.

    Args:
        format_string: Format string (e.g., "spark:tree", "bq:table")

    Returns:
        Family name (e.g., "spark", "bq")

    Examples:
        >>> get_family("spark:tree")
        'spark'
        >>> get_family("bq:table")
        'bq'
    """
    if ":" in format_string:
        return format_string.split(":", 1)[0]
    return format_string


def get_all_valid_formats() -> list[str]:
    """Get all valid format strings (including aliases).

    Returns:
        Sorted list of all valid format strings
    """
    formats = set()

    # Add all family:representation combinations
    for family, representations in FAMILY_REPRESENTATIONS.items():
        for rep in representations:
            formats.add(f"{family}:{rep}")

    # Add all aliases
    formats.update(ALIASES.keys())

    return sorted(formats)


def get_format_help_text() -> str:
    """Generate help text describing available formats.

    Returns:
        Formatted help text string
    """
    lines = ["Available formats:"]
    lines.append("")

    for family, representations in sorted(FAMILY_REPRESENTATIONS.items()):
        reps_str = ", ".join(representations)
        default = FAMILY_DEFAULTS.get(family, "")
        if default:
            lines.append(f"  {family}: {reps_str} (default: {default})")
        else:
            lines.append(f"  {family}: {reps_str}")

    lines.append("")
    lines.append("Examples:")
    lines.append("  spark:tree      Spark printSchema() output")
    lines.append("  spark:json      Spark StructType JSON")
    lines.append("  bq:table        Live BigQuery table")
    lines.append("  sql:ddl         SQL CREATE TABLE statement")
    lines.append("  dbt:manifest    dbt manifest.json")
    lines.append("")
    lines.append("Backward-compatible aliases:")
    lines.append("  spark → spark:json")
    lines.append("  bigquery → bq:table")
    lines.append("  sql → sql:ddl")

    return "\n".join(lines)


__all__ = [
    "parse_format",
    "format_to_internal_kind",
    "resolve_format",
    "get_family",
    "get_all_valid_formats",
    "get_format_help_text",
    "FAMILY_REPRESENTATIONS",
    "FAMILY_DEFAULTS",
    "ALIASES",
]
