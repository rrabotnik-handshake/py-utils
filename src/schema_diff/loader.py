# schema_diff/loader.py

from __future__ import annotations
from .io_utils import sample_records, nth_record
from typing import Any, Dict, Optional, Set, Tuple

import json
import os
import pathlib

from .io_utils import sample_records, nth_record, open_text
from .schema_from_data import merged_schema_from_samples
from .normalize import walk_normalize
from .json_schema_parser import schema_from_json_schema_file
from .spark_schema_parser import schema_from_spark_schema_file
from .sql_schema_parser import schema_from_sql_schema_file
from .dbt_schema_parser import schema_from_dbt_manifest, schema_from_dbt_schema_yml

KIND_DATA = "data"
KIND_JSONSCHEMA = "jsonschema"
KIND_SPARK = "spark"
KIND_SQL = "sql"
KIND_DBT_MANIFEST = "dbt-manifest"
KIND_DBT_YML = "dbt-yml"
KIND_AUTO = "auto"


def _is_probably_ndjson(sample_text: str) -> bool:
    # two non-empty lines both starting with "{"
    lines = [ln.strip() for ln in sample_text.splitlines() if ln.strip()]
    return len(lines) >= 2 and lines[0].startswith("{") and lines[1].startswith("{")


def _sniff_json_kind(path: str) -> Optional[str]:
    """
    Peek into a .json / .json.gz file and distinguish:
      - dbt manifest (nodes/sources)
      - JSON Schema (type=object + properties dict)
      - otherwise: data
    Returns KIND_* or None if not JSON-like.
    """
    try:
        with open_text(path) as f:
            buf = f.read(65536)  # small peek; enough for top-level keys
            s = buf.lstrip()
            if not s:
                return KIND_DATA  # empty -> treat as data
            if s.startswith("{"):
                # Try parsing a small JSON object
                obj = json.loads(buf)
                if isinstance(obj, dict):
                    # dbt manifest?
                    if ("nodes" in obj and isinstance(obj["nodes"], dict)) or \
                       ("sources" in obj and isinstance(obj["sources"], dict)) or \
                       ("child_map" in obj):
                        return KIND_DBT_MANIFEST
                    # JSON Schema?
                    if obj.get("type") == "object" and isinstance(obj.get("properties"), dict):
                        return KIND_JSONSCHEMA
                    # else: likely a single JSON record (data)
                    return KIND_DATA
                # non-dict root → treat as data
                return KIND_DATA
            # Not a single JSON object; could be NDJSON
            if _is_probably_ndjson(buf):
                return KIND_DATA
    except Exception:
        pass
    return None


def _guess_kind(path: str) -> str:
    p = path.lower()
    # supports .json.gz, .ndjson.gz, etc.
    ext = "".join(pathlib.Path(p).suffixes)

    # SQL & YAML are unambiguous
    if p.endswith(".sql"):
        return KIND_SQL
    if p.endswith(".yml") or p.endswith(".yaml"):
        return KIND_DBT_YML

    # Spark schemas are commonly provided as .txt
    if p.endswith(".txt"):
        return KIND_SPARK

    # JSON / NDJSON / JSON.GZ / NDJSON.GZ
    if any(p.endswith(suf) for suf in (".json", ".json.gz", ".ndjson", ".ndjson.gz", ".jsonl", ".jsonl.gz", ".gz")):
        sniff = _sniff_json_kind(path)
        if sniff:
            return sniff
        # fallback when sniffing fails
        if p.endswith((".ndjson", ".ndjson.gz", ".jsonl", ".jsonl.gz")):
            return KIND_DATA
        # default for .json/.json.gz if we couldn't parse
        return KIND_DATA

    # Last resort: assume data
    return KIND_DATA

def load_left_or_right(
    path: str,
    *,
    kind: Optional[str],
    cfg,
    samples: int,
    first_record: Optional[int] = None,
    sql_table: Optional[str] = None,
    dbt_model: Optional[str] = None,
) -> Tuple[Any, Set[str], str]:
    """
    Returns (type_tree, required_paths, label)
    - type_tree: pure types (no '|missing' injected)
    - required_paths: presence constraints (e.g., NOT NULL or JSON Schema 'required')
    - label: display label for headers
    """
    chosen = kind or KIND_AUTO
    if chosen == KIND_AUTO:
        chosen = _guess_kind(path)

    if chosen == KIND_DATA:
        if first_record is not None:
            recs = nth_record(path, first_record or 1)
            title = f"; record #{first_record or 1}"
        else:
            recs = sample_records(path, samples)
            title = f"; random {samples}-record samples"
        tree = merged_schema_from_samples(recs, cfg)
        return tree, set(), f"{path}{title}"

    if chosen == KIND_JSONSCHEMA:
        tree, required = schema_from_json_schema_file(path)
        return tree, required, path

    if chosen == KIND_SPARK:
        tree = schema_from_spark_schema_file(path)
        return tree, set(), path

    if chosen == KIND_SQL:
        tree, required = schema_from_sql_schema_file(path, table=sql_table)
        label = path if not sql_table else f"{path}#{sql_table}"
        return tree, required, label

    if chosen == KIND_DBT_MANIFEST:
        tree, required = schema_from_dbt_manifest(path, model=dbt_model)
        label = path if not dbt_model else f"{path}#{dbt_model}"
        return tree, required, label

    if chosen == KIND_DBT_YML:
        tree, required = schema_from_dbt_schema_yml(path, model=dbt_model)
        label = path if not dbt_model else f"{path}#{dbt_model}"
        return tree, required, label

    raise ValueError(f"Unknown kind: {kind}")


__all__ = [
    "load_left_or_right",
    "KIND_DATA", "KIND_JSONSCHEMA", "KIND_SPARK",
    "KIND_SQL", "KIND_DBT_MANIFEST", "KIND_DBT_YML", "KIND_AUTO",
]
