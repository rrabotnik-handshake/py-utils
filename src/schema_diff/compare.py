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

from .json_data_file_parser import merged_schema_from_samples
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
# Data → reference (presence-aware) comparison
# ----------------------------------------------------------------------
def compare_data_to_ref(
    file1: str,
    ref_label: str,
    ref_schema: Any,
    *,
    cfg,
    s1_records,
    dump_schemas: str | None = None,
    json_out: str | None = None,
    title_suffix: str = "",
    required_paths: set[str] | None = None,
    show_common: bool = False,
    ref_source_type: str | None = None,
) -> None:
    """
    Compare a DATA sample (from `file1`) to a reference schema with source-aware presence handling.

    Parameters
    ----------
    file1 : str
        Path to the data file being compared
    ref_label : str
        Display name for the reference schema
    ref_schema : Any
        The reference schema type tree
    cfg : Config
        Configuration object for normalization and output
    s1_records : List[Any]
        Sample records from file1 for schema inference
    dump_schemas : str | None
        If provided, save the normalized schemas to this file
    json_out : str | None
        If provided, save the diff report as JSON to this file
    title_suffix : str
        Optional suffix for the comparison title
    required_paths : Optional[Set[str]]
        Set of paths that are required in the reference schema
    show_common : bool
        If True, display fields present in both schemas with matching types
    ref_source_type : Optional[str]
        Reference source type ('spark', 'sql', 'jsonschema', etc.) for proper presence terminology

    Notes
    -----
    - Infers schema from s1_records and compares to ref_schema
    - Applies presence injection to reference schema if it's a schema source
    - Uses source type information for clear presence terminology in output
    """
    # Coerce list-of-fields roots into dicts so diffs are per-path, not list indices
    ref_schema = coerce_root_to_field_dict(ref_schema)

    # Apply presence to reference to align with data-side unions that include 'missing'
    ref_for_diff = inject_presence_for_diff(ref_schema, required_paths)
    sch2n = walk_normalize(ref_for_diff)

    sch1 = merged_schema_from_samples(s1_records, cfg)
    sch1n = walk_normalize(sch1)

    sch1n = coerce_root_to_field_dict(sch1n)
    sch2n = coerce_root_to_field_dict(sch2n)

    if show_common:
        print_common_fields(file1, ref_label, sch1n, sch2n, cfg.colors())

    diff = DeepDiff(sch1n, sch2n, ignore_order=True)

    if not diff:
        RED, GRN, YEL, CYN, RST = cfg.colors()
        print(
            f"\n{CYN}=== Schema diff (types only, {file1} → {ref_label}{title_suffix}) ==={RST}\nNo differences."
        )
        if dump_schemas:
            with open(dump_schemas, "w", encoding="utf-8") as fh:
                json.dump(
                    {"file1": sch1n, "ref": sch2n}, fh, ensure_ascii=False, indent=2
                )
        if json_out:
            with open(json_out, "w", encoding="utf-8") as fh:
                json.dump(
                    {
                        "meta": {
                            "direction": f"{file1} -> {ref_label}",
                            "mode": title_suffix.strip("; ").strip(),
                        },
                        "note": "No differences",
                    },
                    fh,
                    ensure_ascii=False,
                    indent=2,
                )
        return

    report = build_report_struct(
        diff, file1, ref_label, include_presence=cfg.show_presence
    )
    report["meta"]["mode"] = title_suffix.strip("; ").strip()

    print_report_text(
        report,
        file1,
        ref_label,
        colors=cfg.colors(),
        show_presence=cfg.show_presence,
        title_suffix=title_suffix,
        left_source_type="data",
        right_source_type=ref_source_type or "data",
    )

    # Field moved/nested differently?
    path_changes = compute_path_changes(sch1n, sch2n)
    print_path_changes(file1, ref_label, path_changes, colors=cfg.colors())

    if dump_schemas:
        with open(dump_schemas, "w", encoding="utf-8") as fh:
            json.dump({"file1": sch1n, "ref": sch2n}, fh, ensure_ascii=False, indent=2)
    if json_out:
        with open(json_out, "w", encoding="utf-8") as fh:
            json.dump(report, fh, ensure_ascii=False, indent=2)


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
