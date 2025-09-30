"""
Loader utilities for turning *anything* (data files or schema files)
into a unified (type_tree, required_paths, label) triple.

Supported kinds:
- data           : JSON / NDJSON (optionally gz), array-of-objects, or single JSON object
- jsonschema     : JSON Schema (draft-ish), also tolerates list-of-field dicts
- spark          : Spark-style text schema (recursively parsed)
- sql            : SQL CREATE TABLE / column list (Postgres, BigQuery-ish)
- dbt-manifest   : dbt manifest.json
- dbt-yml        : dbt schema.yml
- dbt-model      : dbt model.sql
- protobuf       : .proto files (requires selecting a message)

The goal: keep CLI and other callers simple and consistent, and keep all
kind-detection, sniffing, and per-source quirks confined to this module.

This module now uses the ParserFactory pattern for better extensibility and type safety.
"""

from __future__ import annotations

import json
from typing import Any

from .io_utils import open_text, sniff_ndjson
from .parser_factory import ParserFactory
from .protobuf_schema_parser import list_protobuf_messages

__all__ = [
    "load_left_or_right",
    "KIND_DATA",
    "KIND_JSONSCHEMA",
    "KIND_SPARK",
    "KIND_SQL",
    "KIND_DBT_MANIFEST",
    "KIND_DBT_YML",
    "KIND_AUTO",
    "KIND_PROTOBUF",
]

# ---- Kind constants -------------------------------------------------------

KIND_DATA = "data"
KIND_JSONSCHEMA = "jsonschema"
KIND_SPARK = "spark"
KIND_SQL = "sql"
KIND_DBT_MANIFEST = "dbt-manifest"
KIND_DBT_YML = "dbt-yml"
KIND_DBT_MODEL = "dbt-model"
KIND_PROTOBUF = "protobuf"
KIND_BIGQUERY = "bigquery"
KIND_AUTO = "auto"


# ---- Small helpers --------------------------------------------------------


def _sniff_json_kind(path: str) -> str | None:
    """
    Peek into a .json/.json.gz (or similar) and try to distinguish:
      - dbt manifest (nodes/sources/child_map or metadata.dbt_version)
      - JSON Schema (object with schema-like keys)
      - NDJSON
      - otherwise: data

    Returns a KIND_* string or None if the file doesn't look JSON-like.
    """
    try:
        with open_text(path) as f:
            # small peek; enough for roots but not whole file
            buf = f.read(131072)  # 128 KiB
    except Exception:
        return None

    if not buf or not buf.strip():
        return KIND_DATA  # empty → treat as data

    # NDJSON heuristic (works even if JSON object parse fails)
    if sniff_ndjson(buf):
        return KIND_DATA

    s = buf.lstrip()

    # JSON array root → treat as data (array of objects)
    if s.startswith("["):
        return KIND_DATA

    # Try JSON object root
    if s.startswith("{"):
        try:
            obj = json.loads(buf)
        except Exception:
            # Likely a very large single JSON record that was truncated in the peek
            return KIND_DATA

        if not isinstance(obj, dict):
            return KIND_DATA

        # dbt manifest signatures
        if (
            ("nodes" in obj and isinstance(obj["nodes"], dict))
            or ("sources" in obj and isinstance(obj["sources"], dict))
            or ("child_map" in obj)
            or (
                isinstance(obj.get("metadata"), dict)
                and "dbt_version" in obj["metadata"]
            )
        ):
            return KIND_DBT_MANIFEST

        # JSON Schema signatures
        if (
            obj.get("$schema")
            or (obj.get("type") == "object" and isinstance(obj.get("properties"), dict))
            or any(
                k in obj
                for k in ("oneO", "anyOf", "allO", "definitions", "$defs", "items")
            )
        ):
            return KIND_JSONSCHEMA

        # Otherwise: treat as a single JSON object data file
        return KIND_DATA

    # Not a JSON-looking root; leave undetermined
    return None


def _sniff_sql_kind(path: str) -> str:
    """
    Distinguish between SQL DDL (CREATE TABLE) and dbt model (.sql with SELECT/Jinja).

    Returns KIND_SQL for DDL or KIND_DBT_MODEL for dbt models.
    """
    try:
        with open_text(path) as f:
            content = f.read(8192).lower()  # Read first 8KB, convert to lowercase
    except Exception:
        return KIND_SQL  # Default to SQL DDL if we can't read

    # Remove comments to avoid false positives
    import re

    content = re.sub(r"--.*$", "", content, flags=re.MULTILINE)
    content = re.sub(r"/\*.*?\*/", "", content, flags=re.DOTALL)

    # Strong indicators of dbt models
    dbt_indicators = [
        "select",  # SELECT statements (most common in dbt models)
        "{{",  # Jinja templating
        "ref(",  # dbt ref() function
        "source(",  # dbt source() function
        "config(",  # dbt config() function
        "var(",  # dbt var() function
    ]

    # Strong indicators of SQL DDL
    ddl_indicators = [
        "create table",
        "create or replace table",
        "create schema",
        "alter table",
        "drop table",
    ]

    dbt_score = sum(1 for indicator in dbt_indicators if indicator in content)
    ddl_score = sum(1 for indicator in ddl_indicators if indicator in content)

    # If we found strong DDL indicators, it's likely SQL DDL
    if ddl_score > 0:
        return KIND_SQL

    # If we found dbt indicators and no DDL indicators, it's likely a dbt model
    if dbt_score > 0:
        return KIND_DBT_MODEL

    # Default to SQL DDL if unclear
    return KIND_SQL


def _guess_kind(path: str) -> str:
    """
    Best-effort kind detection from filename/contents.
    Prefers unambiguous extensions; uses JSON sniffing for .json/.gz.
    """
    p = path.lower()

    # Unambiguous
    if p.endswith(".sql"):
        return _sniff_sql_kind(path)
    if p.endswith(".yml") or p.endswith(".yaml"):
        return KIND_DBT_YML
    if p.endswith(".txt"):
        return KIND_SPARK
    if p.endswith(".proto"):
        return KIND_PROTOBUF

    # JSON / NDJSON (optionally gz)
    if any(
        p.endswith(suf)
        for suf in (
            ".json",
            ".json.gz",
            ".ndjson",
            ".ndjson.gz",
            ".jsonl",
            ".jsonl.gz",
            ".gz",
        )
    ):
        sniff = _sniff_json_kind(path)
        if sniff:
            return sniff
        # fallback when sniffing fails
        if p.endswith((".ndjson", ".ndjson.gz", ".jsonl", ".jsonl.gz")):
            return KIND_DATA
        return KIND_DATA  # default for .json/.json.gz

    # Last resort: assume data
    return KIND_DATA


# ---- Public API -----------------------------------------------------------


def load_left_or_right(
    path: str,
    *,
    kind: str | None,
    cfg,
    samples: int,
    first_record: int | None = None,
    all_records: bool = False,
    sql_table: str | None = None,
    dbt_model: str | None = None,
    proto_message: str | None = None,
) -> tuple[Any, set[str], str]:
    """
    Load `path` as either a DATA source or a SCHEMA source and return:
        (type_tree, required_paths, label)

    Parameters
    ----------
    path : str
        Path to the input file
    kind : Optional[str]
        Type of input ('data', 'jsonschema', 'spark', 'sql', 'dbt-manifest', 'dbt-yml', 'dbt-model', 'proto', 'auto')
    cfg : Config
        Configuration object
    samples : int
        Number of records to sample for DATA sources
    first_record : Optional[int]
        If specified, use only the N-th record for DATA sources (1-based)
    all_records : bool
        If True, process ALL records for DATA sources instead of sampling (memory intensive)
    sql_table : Optional[str]
        Table name to extract from multi-table SQL DDL files
    dbt_model : Optional[str]
        Model name to extract from dbt manifest/schema files
    proto_message : Optional[str]
        Message name to extract from Protobuf .proto files

    Returns
    -------
    tuple[Any, Set[str], str]
        (type_tree, required_paths, label) where:
        - type_tree: pure type tree (no '|missing' mixed in)
        - required_paths: set of dotted paths with presence constraints (NOT NULL, required, etc.)
        - label: display name for headings (includes table/model/message info)

    Notes
    -----
    - For DATA sources, all_records=True enables comprehensive field discovery
    - For SCHEMA sources, supports BigQuery DDL, nested Spark schemas, and standard JSON Schema
    - Now uses ParserFactory pattern for better extensibility and type safety
    """
    chosen = kind or KIND_AUTO
    if chosen == KIND_AUTO:
        chosen = _guess_kind(path)

    # Handle BigQuery live table extraction (special case)
    if chosen == KIND_BIGQUERY:
        return _handle_bigquery_live_table(path)

    # Handle Protobuf message selection (special case)
    if chosen == KIND_PROTOBUF:
        return _handle_protobuf_parsing(path, proto_message)

    # Use ParserFactory for standard parsing
    try:
        result = ParserFactory.parse_file(
            path=path,
            kind=chosen,
            cfg=cfg,
            samples=samples,
            all_records=all_records,
            first_record=(first_record is not None),
            record_n=first_record,
            table=sql_table,
            model=dbt_model,
            message=proto_message,
        )

        return result.schema_tree, result.required_paths, result.label

    except ValueError as e:
        if "Unknown parser kind" in str(e):
            raise ValueError(f"Unknown kind: {chosen}") from e
        raise


def _handle_bigquery_live_table(path: str) -> tuple[Any, set[str], str]:
    """Handle BigQuery live table schema extraction."""
    from .bigquery_ddl import get_live_table_schema

    # Parse BigQuery table reference from path
    # Expected format: project:dataset.table or project.dataset.table
    if ":" in path:
        project_part, table_part = path.split(":", 1)
    else:
        # Assume current project, parse as dataset.table
        project_part = None
        table_part = path

    if "." in table_part:
        dataset_id, table_id = table_part.split(".", 1)
    else:
        raise ValueError(
            f"Invalid BigQuery table reference: {path}. Expected format: project:dataset.table or dataset.table"
        )

    # Use default project if not specified
    try:
        from google.cloud import bigquery
    except ImportError as e:
        raise ImportError(
            "BigQuery functionality requires optional dependencies. "
            "Install with: pip install -e '.[bigquery]'"
        ) from e

    if project_part:
        project_id = project_part
    else:
        # Try to get default project
        try:
            client = bigquery.Client()
            project_id = client.project
        except Exception as e:
            raise ValueError(
                "No project specified and unable to determine default project. Use format: project:dataset.table"
            ) from e

    tree, required = get_live_table_schema(project_id, dataset_id, table_id)
    label = f"bigquery://{project_id}.{dataset_id}.{table_id}"
    return tree, required, label


def _handle_protobuf_parsing(
    path: str, proto_message: str | None
) -> tuple[Any, set[str], str]:
    """Handle Protobuf message selection and parsing."""
    from .protobuf_schema_parser import schema_from_protobuf_file

    # Autoselect single message; prompt if multiple
    if not proto_message:
        try:
            msgs = list_protobuf_messages(path)  # List[str]
        except Exception:
            msgs = []
        if len(msgs) == 1:
            proto_message = msgs[0]
        elif len(msgs) > 1:
            # Show a reasonable preview; avoid dumping thousands
            preview = ", ".join(msgs[:50])
            more = f" (+{len(msgs)-50} more)" if len(msgs) > 50 else ""
            raise ValueError(
                f"Multiple Protobuf messages in {path}. "
                "Choose one with --left-message/--right-message. "
                f"Available: {preview}{more}"
            )
        # else: zero → let the parser raise a clear error

    tree, required, selected = schema_from_protobuf_file(path, message=proto_message)
    label = (
        f"{path}#{selected or proto_message}" if (selected or proto_message) else path
    )
    return tree, (required or set()), label
