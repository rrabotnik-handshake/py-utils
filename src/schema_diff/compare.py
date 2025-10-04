"""
Schema comparison helpers.

This module exposes two primary entry points:

- `compare_data_to_ref(...)`
    Compare a DATA sample (left) to a reference schema (right). The reference
    can carry *presence constraints* (e.g., NOT NULL, JSON Schema "required").
    We inject those constraints into the reference tree as `union(...|missing)`
    so it compares apples-to-apples with the DATA-derived tree.

- `compare_trees(...)`
    Compare two *type trees* (any ↔ any). Presence sets are currently not
    merged into the diff; only types are compared. (Presence-only diffs could
    be added later if needed.)

Implementation notes:
- We normalize both sides via `walk_normalize(...)`.
- If a root is a *list of field entries* (e.g., Spark/BigQuery/Protobuf-like),
  we coerce it to a `{name: type}` dict to get stable, per-path diffs instead
  of noisy list-index changes.
- We also print a “path changes” section to highlight the same field name that
  appears in different locations (e.g., moved or nested differently).
"""

from __future__ import annotations

import json
from typing import Any

from deepdiff import DeepDiff

from .normalize import walk_normalize
from .report import (
    build_report_struct,
    print_common_fields,
    print_path_changes,
    print_report_text,
)
from .utils import (
    coerce_root_to_field_dict,
    compute_path_changes,
    inject_presence_for_diff,
)


# ----------------------------------------------------------------------
# Tree ↔ tree comparison (any ↔ any)
# ----------------------------------------------------------------------
def compare_trees(
    left_label: str,
    right_label: str,
    left_tree: Any,
    left_required: set[str],
    right_tree: Any,
    right_required: set[str],
    *,
    cfg,
    dump_schemas: str | None = None,
    json_out: str | None = None,
    title_suffix: str = "",
    show_common: bool = False,
    left_source_type: str | None = None,
    right_source_type: str | None = None,
) -> dict | None:
    """
    Compare two *type trees* (schemas) and print a human-friendly diff with enhanced presence injection.

    Parameters
    ----------
    left_label, right_label : str
        Display names for the two sides being compared
    left_tree, right_tree : Any
        Type trees (pure types without presence mixed in)
    left_required, right_required : Set[str]
        Sets of dotted paths that are required/non-nullable on each side
    cfg : Config
        Configuration object for normalization and output
    dump_schemas : Optional[str]
        If provided, save the normalized schemas to this file
    json_out : Optional[str]
        If provided, save the diff report as JSON to this file
    title_suffix : str
        Optional suffix for the comparison title (e.g., "; record #1")
    show_common : bool
        If True, display fields present in both schemas with matching types
    left_source_type, right_source_type : Optional[str]
        Source types ('data', 'spark', 'sql', 'jsonschema', etc.) for proper presence injection

    Notes
    -----
    - Applies presence injection to schema sources (Spark, SQL, JSON Schema) to align
      with data-derived schemas that include 'missing' unions.
    - Data sources don't need presence injection as they already include 'missing'.
    """
    # Coerce list-of-fields roots into dicts so diffs are per-path, not list indices
    left_tree = coerce_root_to_field_dict(left_tree)
    right_tree = coerce_root_to_field_dict(right_tree)

    # Apply presence injection to schema sources (not data sources)
    # Data sources already have 'missing' unions; schema sources need them injected
    SCHEMA_SOURCES = {
        "sql",
        "spark",
        "jsonschema",
        "json_schema",  # Support both variants
        "protobuf",
        "dbt-manifest",
        "dbt-yml",
        "dbt-model",
    }

    if left_source_type in SCHEMA_SOURCES:
        # Left side is a schema source - apply presence injection
        left_tree = inject_presence_for_diff(left_tree, left_required)
    elif left_required:
        # Fallback: if we don't know the source type but it has required paths, assume schema source
        left_tree = inject_presence_for_diff(left_tree, left_required)

    if right_source_type in SCHEMA_SOURCES:
        # Right side is a schema source - apply presence injection
        right_tree = inject_presence_for_diff(right_tree, right_required)
    elif right_required:
        # Fallback: if we don't know the source type but it has required paths, assume schema source
        right_tree = inject_presence_for_diff(right_tree, right_required)

    # Normalize trees
    sch1n = walk_normalize(left_tree)
    sch2n = walk_normalize(right_tree)

    sch1n = coerce_root_to_field_dict(sch1n)
    sch2n = coerce_root_to_field_dict(sch2n)

    if show_common:
        print_common_fields(left_label, right_label, sch1n, sch2n, cfg.colors())

    diff = DeepDiff(sch1n, sch2n, ignore_order=True)

    direction = f"{left_label} -> {right_label}"
    RED, GRN, YEL, CYN, RST = cfg.colors()

    if not diff:
        print(
            f"\n{CYN}=== Schema diff (types only, {direction}{title_suffix}) ==={RST}\nNo differences."
        )
        if dump_schemas:
            with open(dump_schemas, "w", encoding="utf-8") as fh:
                json.dump(
                    {"left": sch1n, "right": sch2n}, fh, ensure_ascii=False, indent=2
                )
        if json_out:
            with open(json_out, "w", encoding="utf-8") as fh:
                json.dump(
                    {
                        "meta": {
                            "direction": direction,
                            "mode": title_suffix.strip("; ").strip(),
                        },
                        "note": "No differences",
                    },
                    fh,
                    ensure_ascii=False,
                    indent=2,
                )
        return {
            "meta": {"direction": direction, "mode": title_suffix.strip("; ").strip()},
            "note": "No differences",
        }

    report = build_report_struct(
        diff, left_label, right_label, include_presence=cfg.show_presence
    )
    report["meta"]["mode"] = title_suffix.strip("; ").strip()

    print_report_text(
        report,
        left_label,
        right_label,
        colors=cfg.colors(),
        show_presence=cfg.show_presence,
        title_suffix=title_suffix,
        left_source_type=left_source_type or "data",
        right_source_type=right_source_type or "data",
    )

    # Path changes on normalized/coerced trees
    path_changes = compute_path_changes(sch1n, sch2n)
    print_path_changes(left_label, right_label, path_changes, colors=cfg.colors())

    if dump_schemas:
        with open(dump_schemas, "w", encoding="utf-8") as fh:
            json.dump({"left": sch1n, "right": sch2n}, fh, ensure_ascii=False, indent=2)

    if json_out:
        with open(json_out, "w", encoding="utf-8") as fh:
            json.dump(report, fh, ensure_ascii=False, indent=2)

    return report


# =============================================================================
# Unified Schema Comparison Functions
# =============================================================================


def compare_schemas_unified(
    left_schema,
    right_schema,
    *,
    cfg,
    dump_schemas: str | None = None,
    json_out: str | None = None,
    title_suffix: str = "",
    show_common: bool = False,
    left_label: str | None = None,
    right_label: str | None = None,
) -> dict | None:
    """
    Compare two Schema objects (unified format) and return comparison results.

    Parameters
    ----------
    left_schema : Schema or tuple
        Either a unified Schema object or legacy (tree, required_paths) tuple
    right_schema : Schema or tuple
        Either a unified Schema object or legacy (tree, required_paths) tuple
    cfg : Config
        Configuration object
    dump_schemas : str, optional
        Path to dump schemas for debugging
    json_out : str, optional
        Path to save JSON output
    title_suffix : str
        Suffix for comparison title
    show_common : bool
        Whether to show common fields

    Returns
    -------
    dict or None
        Comparison report structure
    """
    from .models import Schema, to_legacy_tree

    # Convert unified schemas to legacy format for existing comparison logic
    if isinstance(left_schema, Schema):
        left_tree, left_required = to_legacy_tree(left_schema)
        left_source_type = left_schema.source_type or "data"
        left_display_label = left_label or left_source_type
    else:
        # Assume legacy tuple format
        left_tree, left_required = left_schema
        left_source_type = "data"
        left_display_label = left_label or "left"

    if isinstance(right_schema, Schema):
        right_tree, right_required = to_legacy_tree(right_schema)
        right_source_type = right_schema.source_type or "data"
        right_display_label = right_label or right_source_type
    else:
        # Assume legacy tuple format
        right_tree, right_required = right_schema
        right_source_type = "data"
        right_display_label = right_label or "right"

    # Use existing comparison logic
    return compare_trees(
        left_label=left_display_label,
        right_label=right_display_label,
        left_tree=left_tree,
        left_required=left_required,
        right_tree=right_tree,
        right_required=right_required,
        cfg=cfg,
        dump_schemas=dump_schemas,
        json_out=json_out,
        title_suffix=title_suffix,
        show_common=show_common,
        left_source_type=left_source_type,
        right_source_type=right_source_type,
    )


def analyze_schema_evolution(old_schema, new_schema) -> dict:
    """
    Analyze schema evolution between two Schema objects.

    Returns detailed analysis of changes, additions, removals, and migrations.
    """
    from .models import FieldConstraint, Schema

    if not isinstance(old_schema, Schema) or not isinstance(new_schema, Schema):
        raise ValueError("Both schemas must be unified Schema objects")

    # Build field maps for analysis
    old_fields = {f.path: f for f in old_schema.fields}
    new_fields = {f.path: f for f in new_schema.fields}

    old_paths = set(old_fields.keys())
    new_paths = set(new_fields.keys())

    # Analyze changes
    analysis = {
        "added_fields": sorted(new_paths - old_paths),
        "removed_fields": sorted(old_paths - new_paths),
        "common_fields": sorted(old_paths & new_paths),
        "type_changes": [],
        "constraint_changes": [],
        "breaking_changes": [],
        "safe_changes": [],
    }

    # Analyze common fields for changes
    for path in analysis["common_fields"]:
        old_field = old_fields[path]
        new_field = new_fields[path]

        # Type changes
        if str(old_field.type) != str(new_field.type):
            change = {
                "field": path,
                "old_type": str(old_field.type),
                "new_type": str(new_field.type),
            }
            analysis["type_changes"].append(change)

            # Determine if breaking
            if _is_breaking_type_change(str(old_field.type), str(new_field.type)):
                analysis["breaking_changes"].append(f"Type change: {path}")
            else:
                analysis["safe_changes"].append(f"Type change: {path}")

        # Constraint changes
        old_constraints = old_field.constraints
        new_constraints = new_field.constraints

        if old_constraints != new_constraints:
            change = {
                "field": path,
                "old_constraints": [c.value for c in old_constraints],
                "new_constraints": [c.value for c in new_constraints],
            }
            analysis["constraint_changes"].append(change)

            # Adding REQUIRED constraint is breaking
            if (
                FieldConstraint.REQUIRED in new_constraints
                and FieldConstraint.REQUIRED not in old_constraints
            ):
                analysis["breaking_changes"].append(f"Field became required: {path}")
            elif (
                FieldConstraint.REQUIRED in old_constraints
                and FieldConstraint.REQUIRED not in new_constraints
            ):
                analysis["safe_changes"].append(f"Field became optional: {path}")

    # Removed fields are always breaking
    for field in analysis["removed_fields"]:
        analysis["breaking_changes"].append(f"Field removed: {field}")

    # Added fields are usually safe (unless required)
    for field in analysis["added_fields"]:
        new_field = new_fields[field]
        if FieldConstraint.REQUIRED in new_field.constraints:
            analysis["breaking_changes"].append(f"Required field added: {field}")
        else:
            analysis["safe_changes"].append(f"Optional field added: {field}")

    return analysis


def _is_breaking_type_change(old_type: str, new_type: str) -> bool:
    """Determine if a type change is breaking (not backward compatible)."""
    # Simple heuristics - can be expanded
    safe_widening = {
        "int": ["float", "str"],
        "float": ["str"],
        "bool": ["str"],
        "date": ["str", "timestamp"],
        "time": ["str", "timestamp"],
    }

    # Extract base types (ignore array/union complexity for now)
    old_base = old_type.replace("[", "").replace("]", "").split("|")[0]
    new_base = new_type.replace("[", "").replace("]", "").split("|")[0]

    return new_base not in safe_widening.get(old_base, [old_base])
