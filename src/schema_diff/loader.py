from __future__ import annotations
from typing import Any, Optional, Set, Tuple
import json

from .json_data_file_parser import merged_schema_from_samples
from .json_schema_parser import schema_from_json_schema_file
from .spark_schema_parser import schema_from_spark_schema_file
from .sql_schema_parser import schema_from_sql_schema_file
from .dbt_schema_parser import schema_from_dbt_manifest, schema_from_dbt_schema_yml
from .io_utils import sample_records, nth_record, open_text

__all__ = [
    "load_left_or_right",
    "KIND_DATA", "KIND_JSONSCHEMA", "KIND_SPARK",
    "KIND_SQL", "KIND_DBT_MANIFEST", "KIND_DBT_YML", "KIND_AUTO",
]

KIND_DATA = "data"
KIND_JSONSCHEMA = "jsonschema"
KIND_SPARK = "spark"
KIND_SQL = "sql"
KIND_DBT_MANIFEST = "dbt-manifest"
KIND_DBT_YML = "dbt-yml"
KIND_AUTO = "auto"


def _coerce_root_list_to_dict(obj):
    if not isinstance(obj, list) or not obj:
        return obj
    # case 1: [{'name': ..., 'type': ...}, ...]
    if all(isinstance(el, dict) and "name" in el for el in obj):
        out = {}
        for el in obj:
            name = str(el["name"])
            t = el.get("type", el.get("dataType", el.get("dtype", "any")))
            out[name] = t
        return out
    # case 2: [{'id': 'int'}, {'name': 'str'}, ...]
    if all(isinstance(el, dict) and len(el) == 1 for el in obj):
        out = {}
        for el in obj:
            (name, t) = next(iter(el.items()))
            out[str(name)] = t
        return out
    return obj


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


def _is_probably_ndjson(sample_text: str) -> bool:
    # two non-empty lines both starting with "{"
    lines = [ln.strip() for ln in sample_text.splitlines() if ln.strip()]
    return len(lines) >= 2 and lines[0].startswith("{") and lines[1].startswith("{")


def _sniff_json_kind(path: str) -> Optional[str]:
    """
    Peek into a .json/.json.gz and try to distinguish:
      - dbt manifest (nodes/sources/child_map or metadata.dbt_version)
      - JSON Schema (common JSON Schema signatures)
      - NDJSON
      - otherwise: data
    Returns KIND_* or None if not JSON-like.
    """
    try:
        with open_text(path) as f:
            # Small peek: enough to include most roots but not the whole file
            buf = f.read(131072)  # 128 KiB
    except Exception:
        return None

    if not buf or not buf.strip():
        return KIND_DATA  # empty → treat as data

    s = buf.lstrip()

    # Quick NDJSON heuristic first (works even if JSON object parse will fail)
    if _is_probably_ndjson(buf):
        return KIND_DATA

    # If root looks like an array, treat as data (JSON array of objects)
    if s.startswith("["):
        return KIND_DATA

    # Try to parse a *small* object to inspect top-level keys.
    # If this fails, we still might have NDJSON or non-JSON; fall back.
    if s.startswith("{"):
        try:
            obj = json.loads(buf)
        except Exception:
            # Could be a very large single JSON object that we truncated.
            # Fall back to "data" (single JSON object) rather than guessing schema.
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

        # JSON Schema signatures (beyond just type/properties)
        if (
            obj.get("$schema") or
            obj.get("$id") or
            (obj.get("type") == "object" and isinstance(obj.get("properties"), dict)) or
            any(k in obj for k in ("oneOf", "anyOf",
                "allOf", "definitions", "$defs", "items"))
        ):
            # Heuristic: prefer JSON Schema only if it's not obviously plain data.
            # If it has 'properties' or '$schema', it’s almost certainly a schema.
            if (obj.get("type") == "object" and isinstance(obj.get("properties"), dict)) or obj.get("$schema"):
                return KIND_JSONSCHEMA
            # If it has items/oneOf/etc., it’s very likely a schema too.
            if any(k in obj for k in ("oneOf", "anyOf", "allOf", "definitions", "$defs", "items")):
                return KIND_JSONSCHEMA

        # Otherwise treat it as "data" (single JSON object file)
        return KIND_DATA

    # Didn’t look like JSON object/array; maybe NDJSON or something else
    if _is_probably_ndjson(buf):
        return KIND_DATA

    return None



def _guess_kind(path: str) -> str:
    p = path.lower()

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
        # If the file is actually a list-of-fields, accept it directly.
        try:
            with open_text(path) as f:
                raw = json.load(f)
            coerced = _coerce_root_list_to_dict(raw)
            # Only short-circuit if the original root is a list and coercion produced a dict
            if isinstance(raw, list) and isinstance(coerced, dict) and coerced:
                # Treat as a plain type tree with no explicit required info
                return coerced, set(), path
        except Exception:
            # If we can't read/parse here, fall through to the schema parser which will surface errors.
            pass

        # Otherwise, parse as a proper JSON Schema
        tree, required = schema_from_json_schema_file(path)
        return tree, required, path

    if chosen == KIND_SPARK:
        tree, required = schema_from_spark_schema_file(path)
        return tree, required, path

    if chosen == KIND_SQL:
        tree, required = _ensure_tree_required(
            schema_from_sql_schema_file(path, table=sql_table))
        label = path if not sql_table else f"{path}#{sql_table}"
        return tree, required, label

    if chosen == KIND_DBT_MANIFEST:
        tree, required = _ensure_tree_required(
            schema_from_dbt_manifest(path, model=dbt_model))
        label = path if not dbt_model else f"{path}#{dbt_model}"
        return tree, required, label

    if chosen == KIND_DBT_YML:
        tree, required = _ensure_tree_required(
            schema_from_dbt_schema_yml(path, model=dbt_model))
        label = path if not dbt_model else f"{path}#{dbt_model}"
        return tree, required, label


    raise ValueError(f"Unknown kind: {kind}")
