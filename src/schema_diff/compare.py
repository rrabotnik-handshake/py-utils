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

from typing import Any, Optional, Set
import json

from deepdiff import DeepDiff

from .normalize import walk_normalize
from .json_data_file_parser import merged_schema_from_samples
from .report import (
    build_report_struct,
    print_report_text,
    print_common_fields,
    print_path_changes,
)
from .utils import coerce_root_to_field_dict, inject_presence_for_diff, compute_path_changes

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
    required_paths: Optional[Set[str]] = None,
    show_common: bool = False,
) -> None:
    """
    Compare a DATA sample (from `file1`) to a reference schema.

    Parameters
    ----------
    file1 : str
        Data source label (path) used in headings.
    ref_label : str
        Reference label (path/table/etc.) used in headings.
    ref_schema : Any
        Reference *type tree* (pure types) — may be list-of-fields or dict.
    cfg : Config
        Runtime configuration: colors, presence toggle, inference.
    s1_records : list[dict]
        The sample records chosen from the data file.
    dump_schemas : str | None
        If provided, write normalized left/right trees to JSON here.
    json_out : str | None
        If provided, write the diff report JSON here.
    title_suffix : str
        Extra info appended to headings (e.g., “; record #1”).
    required_paths : set[str] | None
        Presence constraints to inject on the reference side.
    show_common : bool
        If True, print common field names (intersection) before the diff.
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
        print(f"\n{CYN}=== Schema diff (types only, {file1} → {ref_label}{title_suffix}) ==={RST}\nNo differences.")
        if dump_schemas:
            with open(dump_schemas, "w", encoding="utf-8") as fh:
                json.dump({"file1": sch1n, "ref": sch2n},
                          fh, ensure_ascii=False, indent=2)
        if json_out:
            with open(json_out, "w", encoding="utf-8") as fh:
                json.dump(
                    {"meta": {"direction": f"{file1} -> {ref_label}", "mode": title_suffix.strip('; ').strip()},
                     "note": "No differences"},
                    fh, ensure_ascii=False, indent=2,
                )
        return

    report = build_report_struct(
        diff, file1, ref_label, include_presence=cfg.show_presence)
    report["meta"]["mode"] = title_suffix.strip("; ").strip()

    print_report_text(
        report, file1, ref_label, colors=cfg.colors(), show_presence=cfg.show_presence, title_suffix=title_suffix
    )

    # Field moved/nested differently?
    path_changes = compute_path_changes(sch1n, sch2n)
    print_path_changes(file1, ref_label, path_changes, colors=cfg.colors())

    if dump_schemas:
        with open(dump_schemas, "w", encoding="utf-8") as fh:
            json.dump({"file1": sch1n, "ref": sch2n},
                      fh, ensure_ascii=False, indent=2)
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
    left_required: Set[str],
    right_tree: Any,
    right_required: Set[str],
    *,
    cfg,
    dump_schemas: Optional[str] = None,
    json_out: Optional[str] = None,
    title_suffix: str = "",
    show_common: bool = False,
) -> None:
    """
    Compare two *type trees* (schemas) and print a human-friendly diff.

    Notes
    -----
    - Only the TYPE TREES are diffed (`left_tree` vs `right_tree`).
    - `left_required` / `right_required` are currently unused by the diff;
      presence sections come from the trees themselves (i.e., unions with
      'missing'). We still accept them so the signature lines up with
      higher-level callers and to allow future presence-only reporting.
    """
    # Coerce list-of-fields roots into dicts so diffs are per-path, not list indices
    left_tree = coerce_root_to_field_dict(left_tree)
    right_tree = coerce_root_to_field_dict(right_tree)

    # Normalize *trees only*
    sch1n = walk_normalize(left_tree)
    sch2n = walk_normalize(right_tree)

    sch1n = coerce_root_to_field_dict(sch1n)
    sch2n = coerce_root_to_field_dict(sch2n)

    if show_common:
        print_common_fields(left_label, right_label,
                            sch1n, sch2n, cfg.colors())

    diff = DeepDiff(sch1n, sch2n, ignore_order=True)

    direction = f"{left_label} -> {right_label}"
    RED, GRN, YEL, CYN, RST = cfg.colors()

    if not diff:
        print(
            f"\n{CYN}=== Schema diff (types only, {direction}{title_suffix}) ==={RST}\nNo differences.")
        if dump_schemas:
            with open(dump_schemas, "w", encoding="utf-8") as fh:
                json.dump({"left": sch1n, "right": sch2n},
                          fh, ensure_ascii=False, indent=2)
        if json_out:
            with open(json_out, "w", encoding="utf-8") as fh:
                json.dump(
                    {"meta": {"direction": direction, "mode": title_suffix.strip('; ').strip()},
                     "note": "No differences"},
                    fh, ensure_ascii=False, indent=2,
                )
        return

    report = build_report_struct(
        diff, left_label, right_label, include_presence=cfg.show_presence)
    report["meta"]["mode"] = title_suffix.strip("; ").strip()

    print_report_text(
        report,
        left_label,
        right_label,
        colors=cfg.colors(),
        show_presence=cfg.show_presence,
        title_suffix=title_suffix,
    )

    # Path changes on normalized/coerced trees
    path_changes = compute_path_changes(sch1n, sch2n)
    print_path_changes(left_label, right_label,
                       path_changes, colors=cfg.colors())

    if dump_schemas:
        with open(dump_schemas, "w", encoding="utf-8") as fh:
            json.dump({"left": sch1n, "right": sch2n},
                      fh, ensure_ascii=False, indent=2)

    if json_out:
        with open(json_out, "w", encoding="utf-8") as fh:
            json.dump(report, fh, ensure_ascii=False, indent=2)
