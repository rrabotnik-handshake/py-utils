"""
dbt schema parsers.

This module exposes two entry points:

- `schema_from_dbt_manifest(path, model=None) -> (schema_tree, required_paths)`
    Parses a dbt `target/manifest.json` and extracts per-column *types* from
    `data_type` (or `type`) and *presence* from not-null tests/constraints.

- `schema_from_dbt_schema_yml(path, model=None) -> (schema_tree, required_paths)`
    Parses a dbt `schema.yml` (v2). Usually has *tests* but not concrete types,
    so types default to "any" unless `data_type` or `meta.type` is present.

Both return:
    schema_tree: Dict[str, Any] with *pure types* e.g. 'int'|'str'|... or ['str'] for arrays
    required_paths: Set[str] of columns that are presence-required (not null)
"""

from __future__ import annotations

import json
import re
from typing import Any

import yaml

from .io_utils import open_text

__all__ = [
    "schema_from_dbt_manifest",
    "schema_from_dbt_schema_yml",
    "schema_from_dbt_model",
    "schema_from_dbt_manifest_unified",
    "schema_from_dbt_schema_yml_unified",
    "schema_from_dbt_model_unified",
]

# Adapter-agnostic normalization of dbt/emitted data types to internal labels.
# Arrays are represented as [elem_type]; normalizer can later collapse ["any"] -> "array".
TYPE_MAP_DBT: dict[str, str] = {
    # integers
    "tinyint": "int",
    "smallint": "int",
    "int2": "int",
    "integer": "int",
    "int": "int",
    "int4": "int",
    "bigint": "int",
    "int8": "int",
    "serial": "int",
    "bigserial": "int",
    # floats / decimals
    "float": "float",
    "float4": "float",
    "float8": "float",
    "double": "float",
    "double precision": "float",
    "real": "float",
    "numeric": "float",
    "decimal": "float",
    "bignumeric": "float",
    # boolean
    "boolean": "bool",
    "bool": "bool",
    # strings / text / ids / json
    "varchar": "str",
    "character varying": "str",
    "char": "str",
    "character": "str",
    "text": "str",
    "citext": "str",
    "uuid": "str",
    "json": "str",
    "jsonb": "str",
    "bytea": "str",
    "string": "str",
    "bytes": "str",
    # date/time
    "date": "date",
    "time": "time",
    "time without time zone": "time",
    "time with time zone": "time",
    "timestamp": "timestamp",
    "timestamp without time zone": "timestamp",
    "timestamp with time zone": "timestamp",
    "timestamptz": "timestamp",
    "datetime": "timestamp",
}

ARRAY_ANGLE_RE = re.compile(r"^array\s*<\s*([^>]+)\s*>$")


def _normalize_dtype(dtype: str) -> str | list[str]:
    """
    Map a dbt/adapter dtype string to internal type label.

    Returns:
      - scalar: 'int'|'float'|'bool'|'str'|'date'|'time'|'timestamp'|'any'
      - array:  [elem_type]  (e.g., ['str'])
    """
    s = " ".join((dtype or "").strip().lower().split())

    # 1) Postgres-like: text[] / varchar(255)[]
    is_array = s.endswith("[]")
    if is_array:
        s = s[:-2].strip()

    # 2) BigQuery/Snowflake-like: array<string> / array<varchar(255)>
    m = ARRAY_ANGLE_RE.match(s)
    if m:
        inner = m.group(1).strip()
        # strip inner precision too
        inner_base = inner.split("(", 1)[0].strip()
        mapped_inner = TYPE_MAP_DBT.get(inner_base, TYPE_MAP_DBT.get(inner, "any"))
        return ["any" if mapped_inner == "any" else mapped_inner]

    base = s.split("(", 1)[0].strip()
    mapped = TYPE_MAP_DBT.get(base, TYPE_MAP_DBT.get(s, "any"))
    return ["any" if mapped == "any" else mapped] if is_array else mapped


def _iter_test_names(tests_field: Any) -> list[str]:
    """
    Normalize dbt 'tests' lists that may contain strings or dicts.

    Examples:
      ["not_null", {"unique": {...}}, "accepted_values"]
      → ["not_null", "unique", "accepted_values"]
    """
    out: list[str] = []
    if not isinstance(tests_field, list):
        return out
    for tdef in tests_field:
        if isinstance(tdef, str):
            out.append(tdef)
        elif isinstance(tdef, dict):
            out.extend(tdef.keys())
    return out


# ---------------- Manifest.json (preferred: has types) --------------------


def schema_from_dbt_manifest(
    path: str, model: str | None = None
) -> tuple[dict[str, Any], set[str]]:
    """
    Parse dbt target/manifest.json, pull column `data_type` for the target model.

    Args:
      path: path to manifest.json.
      model: model selector; can be simple name ('my_model'), alias, or full node id
             (e.g. 'model.project_name.my_model'). If None, picks the first model.

    Returns:
      (schema_tree, required_paths)
    """
    with open_text(path) as f:
        man = json.load(f)

    nodes: dict[str, Any] = man.get("nodes", {}) or {}
    matches: list[tuple[str, Any]] = []
    mlc = (model or "").lower()

    for node_id, node in nodes.items():
        if node.get("resource_type") != "model":
            continue
        name = (node.get("name") or "").lower()
        alias = (node.get("alias") or "").lower()
        fq = node_id.lower()  # e.g., model.project_name.my_model
        if not model:
            matches.append((fq, node))
        else:
            if mlc in {name, alias, fq}:
                matches.append((fq, node))

    if not matches:
        raise ValueError(f"No dbt model found matching '{model}' in {path}")

    # Prefer exact name/alias/node-id match if provided; else take the first sorted
    _, node = sorted(matches, key=lambda x: (x[0] != mlc, x[0]))[0]

    cols = node.get("columns", {}) or {}
    schema: dict[str, Any] = {}
    required: set[str] = set()

    for col_name, col_meta in cols.items():
        # Prefer adapter-reported data_type; fallback to 'type' if present
        dtype = (col_meta.get("data_type") or col_meta.get("type") or "").strip()
        mapped = _normalize_dtype(dtype) if dtype else "any"
        schema[col_name] = mapped

        # Presence via tests/constraints
        tests = _iter_test_names(col_meta.get("tests") or [])
        if any(tn.endswith(".not_null") or tn == "not_null" for tn in tests):
            required.add(col_name)

        for c in col_meta.get("constraints", []) or []:
            if isinstance(c, dict) and (c.get("type", "") or "").lower() == "not_null":
                required.add(col_name)

    return schema, required


# ---------------- schema.yml (tests; usually no types) --------------------


def schema_from_dbt_schema_yml(
    path: str, model: str | None = None
) -> tuple[dict[str, Any], set[str]]:
    """
    Parse a dbt schema.yml (v2). Uses tests for presence (not_null).
    Types are often not present, so columns default to "any" unless a custom
    'data_type' (or meta.type) is provided.

    Args:
      path: path to schema.yml
      model: model selector by 'name' or 'alias'. If None, picks the first model.

    Returns:
      (schema_tree, required_paths)
    """
    if yaml is None:
        raise RuntimeError(
            "pyyaml is required to parse dbt schema.yml. Install 'pyyaml'."
        )

    with open_text(path) as f:
        data = yaml.safe_load(f) or {}

    models = data.get("models") or []
    matches: list[dict[str, Any]] = []
    mlc = (model or "").lower()

    for m in models:
        name = (m.get("name") or "").lower()
        alias = (m.get("alias") or "").lower()
        if not model or mlc in {name, alias}:
            matches.append(m)

    if not matches:
        raise ValueError(f"No dbt model found matching '{model}' in {path}")

    m = matches[0]
    cols = m.get("columns") or []
    schema: dict[str, Any] = {}
    required: set[str] = set()

    for c in cols:
        col_name = c.get("name")
        if not col_name:
            continue

        # Optional declared type in schema.yml (non-standard; seen in some projects)
        declared = (
            c.get("data_type") or (c.get("meta") or {}).get("type") or ""
        ).strip()
        mapped = _normalize_dtype(declared) if declared else "any"
        schema[col_name] = mapped

        tests = _iter_test_names(c.get("tests") or [])
        if any(tn.endswith(".not_null") or tn == "not_null" for tn in tests):
            required.add(col_name)

    return schema, required


def schema_from_dbt_model(
    path: str, model: str | None = None
) -> tuple[dict[str, Any], set[str]]:
    """
    Parse a dbt model .sql file and extract field information from SELECT statements.

    Args:
        path: path to the dbt model .sql file.
        model: model name filter (currently unused for .sql files, but kept for consistency)

    Returns:
        (schema_tree, required_paths)

    Notes:
        This is a basic implementation that extracts field names from SELECT statements.
        It doesn't perform full SQL parsing, so type information is inferred as 'any'.
        Complex transformations and Jinja templating may not be fully parsed.
    """
    import re

    with open_text(path) as f:
        content = f.read()

    # Remove comments (both -- and /* */ style)
    content = re.sub(r"--.*$", "", content, flags=re.MULTILINE)
    content = re.sub(r"/\*.*?\*/", "", content, flags=re.DOTALL)

    # Find SELECT statements - look for field patterns
    # This is a simple heuristic approach
    schema: dict[str, Any] = {}
    required: set[str] = set()

    # Extract field names from SELECT clauses
    select_sections = re.findall(
        r"select\s+(.*?)(?:from|\{|\;|$)", content, re.DOTALL | re.IGNORECASE
    )

    for select_text in select_sections:
        # Clean up the select text
        select_text = select_text.strip()
        if not select_text:
            continue

        # Split by commas and extract field names
        fields = re.split(
            r",(?![^()]*\))", select_text
        )  # Split by comma but not inside parentheses

        for field in fields:
            field = field.strip()
            if not field or field == "*":
                continue

            # Extract field name using various patterns
            field_name = None

            # Case 1: field AS alias
            as_match = re.search(
                r"^\s*(?:\w+\.)?\w+\s+as\s+(\w+)\s*$", field, re.IGNORECASE
            )
            if as_match:
                field_name = as_match.group(1)

            # Case 2: function(...) AS alias
            elif re.search(r"^\s*\w+\([^)]*\)\s+as\s+(\w+)\s*$", field, re.IGNORECASE):
                func_match = re.search(r"as\s+(\w+)\s*$", field, re.IGNORECASE)
                if func_match:
                    field_name = func_match.group(1)

            # Case 3: simple field or alias.field
            elif re.match(r"^\s*(?:\w+\.)?(\w+)\s*$", field):
                simple_match = re.search(r"(?:\w+\.)?(\w+)\s*$", field)
                if simple_match:
                    field_name = simple_match.group(1)

            if field_name and field_name.isalnum() and not field_name.isdigit():
                # Map all fields to 'any' since we can't infer types from SQL
                schema[field_name] = "any"

    return schema, required


def schema_from_dbt_manifest_unified(path: str, model: str | None = None):
    """
    Parse dbt manifest and return unified Schema object.

    Returns
    -------
    Schema
        Unified schema representation using Pydantic models
    """
    from .models import from_legacy_tree

    tree, required = schema_from_dbt_manifest(path, model)
    return from_legacy_tree(tree, required, source_type="dbt-manifest")


def schema_from_dbt_schema_yml_unified(path: str, model: str | None = None):
    """
    Parse dbt schema.yml and return unified Schema object.

    Returns
    -------
    Schema
        Unified schema representation using Pydantic models
    """
    from .models import from_legacy_tree

    tree, required = schema_from_dbt_schema_yml(path, model)
    return from_legacy_tree(tree, required, source_type="dbt-yml")


def schema_from_dbt_model_unified(path: str):
    """
    Parse dbt model and return unified Schema object.

    Returns
    -------
    Schema
        Unified schema representation using Pydantic models
    """
    from .models import from_legacy_tree

    tree, required = schema_from_dbt_model(path)
    return from_legacy_tree(tree, required, source_type="dbt-model")
