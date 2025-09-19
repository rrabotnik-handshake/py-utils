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
- protobuf       : .proto files (requires selecting a message)

The goal: keep CLI and other callers simple and consistent, and keep all
kind-detection, sniffing, and per-source quirks confined to this module.
"""

from __future__ import annotations
from typing import Any, Optional, Set, Tuple
import json

from .json_data_file_parser import merged_schema_from_samples
from .json_schema_parser import schema_from_json_schema_file
from .spark_schema_parser import schema_from_spark_schema_file
from .sql_schema_parser import schema_from_sql_schema_file
from .dbt_schema_parser import schema_from_dbt_manifest, schema_from_dbt_schema_yml
from .protobuf_schema_parser import schema_from_protobuf_file, list_protobuf_messages
from .io_utils import sample_records, nth_record, open_text, sniff_ndjson
from .utils import coerce_root_to_field_dict  # single canonical helper

__all__ = [
    "load_left_or_right",
    "KIND_DATA", "KIND_JSONSCHEMA", "KIND_SPARK",
    "KIND_SQL", "KIND_DBT_MANIFEST", "KIND_DBT_YML",
    "KIND_AUTO", "KIND_PROTOBUF",
]

# ---- Kind constants -------------------------------------------------------

KIND_DATA = "data"
KIND_JSONSCHEMA = "jsonschema"
KIND_SPARK = "spark"
KIND_SQL = "sql"
KIND_DBT_MANIFEST = "dbt-manifest"
KIND_DBT_YML = "dbt-yml"
KIND_PROTOBUF = "protobuf"
KIND_AUTO = "auto"


# ---- Small helpers --------------------------------------------------------

def _ensure_tree_required(x) -> tuple[Any, set[str]]:
    """
    Accept a value that might be:
      - a pure type tree (dict/list/str/...), or
      - a (tree, required_iterable) tuple.
    Always return (tree, required_set).
    """
    if isinstance(x, tuple) and len(x) == 2:
        tree, req = x
        try:
            req = set(req)
        except TypeError:
            req = set()
        return tree, req
    return x, set()


def _sniff_json_kind(path: str) -> Optional[str]:
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
            ("nodes" in obj and isinstance(obj["nodes"], dict)) or
            ("sources" in obj and isinstance(obj["sources"], dict)) or
            ("child_map" in obj) or
            (isinstance(obj.get("metadata"), dict)
             and "dbt_version" in obj["metadata"])
        ):
            return KIND_DBT_MANIFEST

        # JSON Schema signatures
        if (
            obj.get("$schema") or
            (obj.get("type") == "object" and isinstance(obj.get("properties"), dict)) or
            any(k in obj for k in ("oneOf", "anyOf",
                "allOf", "definitions", "$defs", "items"))
        ):
            return KIND_JSONSCHEMA

        # Otherwise: treat as a single JSON object data file
        return KIND_DATA

    # Not a JSON-looking root; leave undetermined
    return None


def _guess_kind(path: str) -> str:
    """
    Best-effort kind detection from filename/contents.
    Prefers unambiguous extensions; uses JSON sniffing for .json/.gz.
    """
    p = path.lower()

    # Unambiguous
    if p.endswith(".sql"):
        return KIND_SQL
    if p.endswith(".yml") or p.endswith(".yaml"):
        return KIND_DBT_YML
    if p.endswith(".txt"):
        return KIND_SPARK
    if p.endswith(".proto"):
        return KIND_PROTOBUF

    # JSON / NDJSON (optionally gz)
    if any(p.endswith(suf) for suf in (
        ".json", ".json.gz", ".ndjson", ".ndjson.gz",
        ".jsonl", ".jsonl.gz", ".gz"
    )):
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
    kind: Optional[str],
    cfg,
    samples: int,
    first_record: Optional[int] = None,
    all_records: bool = False,
    sql_table: Optional[str] = None,
    dbt_model: Optional[str] = None,
    proto_message: Optional[str] = None,
) -> Tuple[Any, Set[str], str]:
    """
    Load `path` as either a DATA source or a SCHEMA source and return:
        (type_tree, required_paths, label)

    Notes
    -----
    - `type_tree` is the *pure type* tree (no '|missing' mixed in).
    - `required_paths` is a set of dotted paths with presence constraints
      (NOT NULL, JSON Schema 'required', etc.).
    - `label` is what we print in headings (may include table/model/message).
    - All parser calls are normalized through `_ensure_tree_required`.
    """
    chosen = kind or KIND_AUTO
    if chosen == KIND_AUTO:
        chosen = _guess_kind(path)

    # DATA: sample or pick a specific record and infer a type tree
    if chosen == KIND_DATA:
        if all_records:
            from .io_utils import all_records as all_records_fn
            recs = all_records_fn(path, max_records=1000000)  # Safety limit
            title = f"; all {len(recs)} records"
        elif first_record is not None:
            recs = nth_record(path, first_record or 1)
            title = f"; record #{first_record or 1}"
        else:
            recs = sample_records(path, samples)
            title = f"; random {samples}-record samples"
        tree = merged_schema_from_samples(recs, cfg)
        return tree, set(), f"{path}{title}"

    # JSON Schema (also tolerates "list-of-fields" JSON roots)
    if chosen == KIND_JSONSCHEMA:
        try:
            with open_text(path) as f:
                raw = json.load(f)
            coerced = coerce_root_to_field_dict(raw)
            # If original root is a list and coercion produced a dict, accept it directly
            if isinstance(raw, list) and isinstance(coerced, dict) and coerced:
                return coerced, set(), path
        except Exception:
            # If we can't parse here, fall back to the strict parser
            pass

        tree, required = _ensure_tree_required(
            schema_from_json_schema_file(path)
        )
        return tree, required, path

    # Spark schema (recursively parsed)
    if chosen == KIND_SPARK:
        tree, required = _ensure_tree_required(
            schema_from_spark_schema_file(path)
        )
        return tree, required, path

    # SQL schema (CREATE TABLE / column list)
    if chosen == KIND_SQL:
        tree, required = _ensure_tree_required(
            schema_from_sql_schema_file(path, table=sql_table)
        )
        label = path if not sql_table else f"{path}#{sql_table}"
        return tree, required, label

    # dbt manifest
    if chosen == KIND_DBT_MANIFEST:
        tree, required = _ensure_tree_required(
            schema_from_dbt_manifest(path, model=dbt_model)
        )
        label = path if not dbt_model else f"{path}#{dbt_model}"
        return tree, required, label

    # dbt schema.yml
    if chosen == KIND_DBT_YML:
        tree, required = _ensure_tree_required(
            schema_from_dbt_schema_yml(path, model=dbt_model)
        )
        label = path if not dbt_model else f"{path}#{dbt_model}"
        return tree, required, label

    # Protobuf (.proto)
    if chosen == KIND_PROTOBUF:
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
                    f"Choose one with --left-message/--right-message. "
                    f"Available: {preview}{more}"
                )
            # else: zero → let the parser raise a clear error

        tree, required, selected = schema_from_protobuf_file(
            path, message=proto_message
        )
        label = f"{path}#{selected or proto_message}" if (
            selected or proto_message) else path
        return tree, (required or set()), label

    raise ValueError(f"Unknown kind: {kind}")
