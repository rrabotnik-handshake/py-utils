"""
Human-friendly diff report helpers with enhanced output formatting.

This module turns a DeepDiff (computed elsewhere) into:
  - a stable, JSON-serializable report structure, and
  - readable, colorized text output with clear terminology and clean formatting.

Key features
------------
- Handles the common "entire root changed" DeepDiff corner case by deriving
  "only in left/right" from root dict or list-of-fields (Spark/BigQuery-like) shapes.
- Splits differences into *type mismatches* and *presence issues* with clear terminology:
  * Uses "missing" vs "nullable" based on source type (data vs schema)
  * Filters out sampling artifacts from type mismatches
  * Formats array paths with clean [] notation instead of legacy [0]
- Enhanced common field display with nested field support using dot notation
- Path change reporting for field location differences between schemas
- Source-aware presence formatting for clear data vs schema terminology
- Filters out comparisons involving 'any' to reduce noise.
- Always prints section headers with counts, which the tests assert on.
"""

from __future__ import annotations

from typing import Any
import json

from .utils import fmt_dot_path, clean_deepdiff_path
from .normalize import _has_any, is_presence_issue


def fmt_presence_type(type_repr: str, is_schema_source: bool = False) -> str:
    """
    Format type representation for presence issues with clear terminology.

    Args:
        type_repr: Raw type representation like 'str', 'missing', 'union(missing|str)'
        is_schema_source: True if this represents a schema source (use 'nullable'),
                         False for data source (show base type or 'missing data')

    Returns:
        Human-readable representation

    Notes:
        For data sources, union(type|missing) becomes just 'type' since the missing
        part represents data presence, not schema optionality.
    """
    if type_repr == "missing":
        return "missing data" if not is_schema_source else "nullable"

    if isinstance(type_repr, str) and type_repr.startswith("union(") and type_repr.endswith(")"):
        # Parse union type like "union(missing|str)" or "union(str|missing)"
        parts = type_repr[6:-1].split("|")
        if "missing" in parts:
            # Remove 'missing' and get the actual type
            non_missing_parts = [p for p in parts if p != "missing"]
            if len(non_missing_parts) == 1:
                base_type = non_missing_parts[0]
                if is_schema_source:
                    return f"nullable {base_type}"
                else:
                    # For data sources, union(type|missing) just means the type is sometimes present
                    # We show the base type since 'missing' is about data presence, not schema optionality
                    return base_type
            else:
                # Multiple non-missing types (complex union)
                return type_repr
        else:
            # Union without missing (shouldn't appear in presence issues)
            return type_repr

    # Simple type like 'str', 'int', etc.
    return type_repr


# DeepDiff sometimes represents "root" changes with different keys depending on context.
_ROOT_KEYS = {"root", "root['root']", "root['__root__']", ""}


def _root_dict_value_change_fallback(diff) -> tuple[list[str], list[str]]:
    """
    Fallback for the case where DeepDiff reports a single `values_changed` entry
    at the root and both sides are dicts. In that situation we reconstruct the
    "only in left/right" sets by comparing top-level keys.

    Parameters
    ----------
    diff : DeepDiff-like mapping
        The diff object with fields like `values_changed`, `dictionary_item_added`, etc.

    Returns
    -------
    (only_in_left, only_in_right) : tuple[list[str], list[str]]
        Lists of keys that exist only on the left/right root dicts.
        If the shape is not a dict→dict root change, returns ([], []).
    """
    vc = diff.get("values_changed") or {}
    if len(vc) != 1:
        return ([], [])
    key, ch = next(iter(vc.items()))
    if key not in _ROOT_KEYS:
        return ([], [])
    old, new = ch.get("old_value"), ch.get("new_value")
    if not isinstance(old, dict) or not isinstance(new, dict):
        return ([], [])
    only1 = sorted(old.keys() - new.keys())
    only2 = sorted(new.keys() - old.keys())
    return [str(k) for k in only1], [str(k) for k in only2]


def _root_list_value_change_fallback(diff) -> tuple[list[str], list[str]]:
    """
    Fallback for "whole-root changed" when the root is a *list of fields*, e.g.
    Spark/BigQuery/Protobuf-like shapes:

        [{'name': 'id', 'type': ...}, ...]           # case 1
      or
        [{'id': 'int'}, {'name': 'str'}, ...]        # case 2

    We derive 'only in left/right' by extracting field names from the lists.

    Parameters
    ----------
    diff : DeepDiff-like mapping

    Returns
    -------
    (only_in_left, only_in_right) : tuple[list[str], list[str]]
        Lists of field names that exist only on the left/right side.
        If the root shapes aren’t recognized list-of-fields, returns ([], []).
    """
    vc = diff.get("values_changed") or {}
    if len(vc) != 1:
        return ([], [])
    key, ch = next(iter(vc.items()))
    if key not in _ROOT_KEYS:
        return ([], [])
    old, new = ch.get("old_value"), ch.get("new_value")

    def names_from_list(lst):
        if isinstance(lst, list) and lst:
            # case 1: [{'name': 'id', 'type': ...}, ...]
            if all(isinstance(el, dict) and "name" in el for el in lst):
                return {str(el["name"]) for el in lst}
            # case 2: [{'id': 'int'}, {'name': 'str'}, ...]
            if all(isinstance(el, dict) and len(el) == 1 for el in lst):
                return {str(next(iter(el.keys()))) for el in lst}
        return None

    ks_old = names_from_list(old)
    ks_new = names_from_list(new)
    if ks_old is None or ks_new is None:
        return ([], [])
    only1 = sorted(ks_old - ks_new)
    only2 = sorted(ks_new - ks_old)
    return only1, only2


def build_report_struct(diff, f1: str, f2: str, include_presence: bool) -> dict[str, Any]:
    """
    Convert a DeepDiff into a stable, JSON-serializable report structure.

    The structure contains:
      - meta.direction
      - only_in_file1: list[str]
      - only_in_file2: list[str]
      - schema_mismatches: list[{"path","file1","file2"}]
      - (optional) presence_issues: list[{"path","file1","file2"}] if include_presence

    Notes
    -----
    - If DeepDiff only reports a single `values_changed` at root, we attempt
      to compute `only_in_*` via dict/list-of-fields fallbacks.
    - Entries involving `'any'` are filtered out for signal.
    - Presence issues are differences where either side includes `'missing'`.

    Parameters
    ----------
    diff : DeepDiff-like mapping
    f1, f2 : str
        Labels for the two compared inputs (used in meta.direction).
    include_presence : bool
        Whether to include presence-only differences in the output.

    Returns
    -------
    dict[str, Any]
        The report payload described above.
    """
    # Try dictionary root fallback first.
    fb1, fb2 = _root_dict_value_change_fallback(diff)

    # If not a dict root, try the list-of-fields fallback.
    if not fb1 and not fb2:
        lb1, lb2 = _root_list_value_change_fallback(diff)
        if lb1 or lb2:
            fb1, fb2 = lb1, lb2

    only_in_2 = sorted(clean_deepdiff_path(p)
                       for p in diff.get("dictionary_item_added", []))
    only_in_1 = sorted(clean_deepdiff_path(p)
                       for p in diff.get("dictionary_item_removed", []))
    if not only_in_1 and not only_in_2 and (fb1 or fb2):
        only_in_1, only_in_2 = fb1, fb2

    presence: list[dict[str, Any]] = []
    schema: list[dict[str, Any]] = []

    for p, ch in (diff.get("values_changed") or {}).items():
        # Ignore dict→dict whole-root change here; fallbacks already handled it.
        if p in _ROOT_KEYS:
            ov, nv = ch.get("old_value"), ch.get("new_value")
            if isinstance(ov, dict) and isinstance(nv, dict):
                continue

        old, new = ch.get("old_value"), ch.get("new_value")
        if _has_any(old) or _has_any(new):
            continue
        entry = {"path": clean_deepdiff_path(p), "file1": old, "file2": new}
        (presence if is_presence_issue(old, new) else schema).append(entry)

    # Handle type_changes (when DeepDiff detects fundamental type changes)
    for p, ch in (diff.get("type_changes") or {}).items():
        # Ignore root-level type changes; fallbacks already handled them
        if p in _ROOT_KEYS:
            continue

        old, new = ch.get("old_value"), ch.get("new_value")
        if _has_any(old) or _has_any(new):
            continue

        # For type_changes, we want to show meaningful schema differences
        def schema_repr(value):
            if isinstance(value, str):
                # Handle special schema strings
                if value == "array":
                    return "array (unstructured)"
                elif value == "empty_array":
                    return "array (unstructured)"
                return value  # Already a schema string like "str", "int", etc.
            elif isinstance(value, list) and len(value) == 1 and isinstance(value[0], dict):
                # Structured array - show that it contains objects
                return "array of objects"
            elif isinstance(value, list):
                return "array"
            elif isinstance(value, dict):
                return "object"
            else:
                # Fallback to basic type inference
                from .infer import tname
                from .config import Config
                return tname(value, Config())

        old_repr = schema_repr(old)
        new_repr = schema_repr(new)

        # Filter out likely sampling artifacts (different array representations)
        is_sampling_artifact = (
            (old_repr == "array (unstructured)" and new_repr == "array of objects") or
            (old_repr == "array of objects" and new_repr == "array (unstructured)") or
            (old_repr == "array (unstructured)" and new_repr == "array") or
            (old_repr == "array" and new_repr == "array (unstructured)")
        )

        # Extract nested fields from type changes (unstructured -> structured arrays)
        base_path = clean_deepdiff_path(p)

        def extract_nested_fields(schema_tree, path_prefix=""):
            """Extract all field paths from a schema tree."""
            paths = []
            if isinstance(schema_tree, dict):
                for key, value in schema_tree.items():
                    field_path = f"{path_prefix}.{key}" if path_prefix else key
                    paths.append(field_path)
                    paths.extend(extract_nested_fields(value, field_path))
            elif isinstance(schema_tree, list) and len(schema_tree) == 1:
                # Structured array: extract fields from the element
                element_paths = extract_nested_fields(schema_tree[0], f"{path_prefix}[]")
                paths.extend(element_paths)
            return paths

        # Check if this is a transition from unstructured to structured array
        if (isinstance(old, str) and old in ("array", "empty_array") and
            isinstance(new, list) and len(new) == 1 and isinstance(new[0], dict)):
            # Extract nested fields from the new structured array
            nested_fields = extract_nested_fields(new[0], f"{base_path}[]")
            only_in_2.extend(nested_fields)

        # Check if this is a transition from structured to unstructured array
        elif (isinstance(new, str) and new in ("array", "empty_array") and
              isinstance(old, list) and len(old) == 1 and isinstance(old[0], dict)):
            # Extract nested fields from the old structured array
            nested_fields = extract_nested_fields(old[0], f"{base_path}[]")
            only_in_1.extend(nested_fields)

        # Only add if there's actually a meaningful difference and not a sampling artifact
        if old_repr != new_repr and not is_sampling_artifact:
            entry = {"path": clean_deepdiff_path(p), "file1": old_repr, "file2": new_repr}
            (presence if is_presence_issue(old_repr, new_repr) else schema).append(entry)

    # Stable order for deterministic output
    schema.sort(key=lambda x: x["path"])
    presence.sort(key=lambda x: x["path"])
    only_in_1.sort()
    only_in_2.sort()

    out: dict[str, Any] = {
        "meta": {"direction": f"{f1} -> {f2}"},
        "only_in_file1": only_in_1,
        "only_in_file2": only_in_2,
        "schema_mismatches": schema,
    }
    if include_presence:
        out["presence_issues"] = presence
    return out


def print_report_text(
    report: dict[str, Any],
    f1: str,
    f2: str,
    *,
    colors: tuple[str, str, str, str, str],
    show_presence: bool,
    title_suffix: str = "",
    left_source_type: str = "data",
    right_source_type: str = "data",
) -> None:
    """
    Pretty-print a report structure returned by `build_report_struct`.

    Parameters
    ----------
    report : dict[str, Any]
        The payload from `build_report_struct`.
    f1, f2 : str
        Left/right labels for headings.
    colors : tuple[str, str, str, str, str]
        (RED, GRN, YEL, CYN, RST) color codes; pass empty strings to disable.
    show_presence : bool
        If True, prints the presence section (if present in the report).
    title_suffix : str
        Optional extra detail shown in the header (e.g., “; record #1”).
    """
    RED, GRN, YEL, CYN, RST = colors
    print(f"\n{CYN}=== Schema diff (types only, {f1} → {f2}{title_suffix}) ==={RST}")

    only1, only2 = report["only_in_file1"], report["only_in_file2"]
    print(f"\n{YEL}-- Only in {f1} -- ({len(only1)}){RST}")
    for p in only1:
        print(f"  {RED}{fmt_dot_path(p)}{RST}")
    print(f"\n{YEL}-- Only in {f2} -- ({len(only2)}){RST}")
    for p in only2:
        print(f"  {GRN}{fmt_dot_path(p)}{RST}")

    if show_presence and "presence_issues" in report:
        pres = report["presence_issues"]

        # Determine which sides are schema sources for proper terminology
        SCHEMA_SOURCES = {'sql', 'spark', 'jsonschema', 'protobuf', 'dbt-manifest', 'dbt-schema'}
        left_is_schema = left_source_type in SCHEMA_SOURCES
        right_is_schema = right_source_type in SCHEMA_SOURCES

        # Filter out items where both sides are identical after formatting
        actual_presence_issues = []
        for e in pres:
            left_formatted = fmt_presence_type(e['file1'], is_schema_source=left_is_schema)
            right_formatted = fmt_presence_type(e['file2'], is_schema_source=right_is_schema)

            # Only include if there's an actual difference after formatting
            if left_formatted != right_formatted:
                actual_presence_issues.append((e, left_formatted, right_formatted))

        print(f"\n{YEL}-- Missing Data / NULL-ABILITY -- ({len(actual_presence_issues)}){RST}")

        for e, left_formatted, right_formatted in actual_presence_issues:
            print(
                f"  {CYN}{fmt_dot_path(e['path'])}{RST}: {left_formatted} → {right_formatted}")

    mism = report["schema_mismatches"]
    print(f"\n{YEL}-- Type mismatches -- ({len(mism)}){RST}")
    for e in mism:
        print(
            f"  {CYN}{fmt_dot_path(e['path'])}{RST}: "
            f"{RED}{e['file1']}{RST} → {GRN}{e['file2']}{RST}"
        )


def print_samples(
    tag: str,
    recs,
    *,
    colors: tuple[str, str, str, str, str] = ("", "", "", "", ""),
    max_chars: int = 2000,
) -> None:
    """
    Pretty-print sampled records, with optional truncation to keep output tidy.

    Parameters
    ----------
    tag : str
        Label printed in the section header and sample labels.
    recs : Iterable[Any]
        The sequence of objects to print as JSON.
    colors : tuple[str, str, str, str, str]
        (RED, GRN, YEL, CYN, RST) color codes; pass empty strings to disable.
    max_chars : int
        If > 0, each sample’s JSON is truncated to this length and “…” appended.
        If 0 or negative, full JSON is printed.
    """
    RED, GRN, YEL, CYN, RST = colors
    print(f"\n{CYN}=== Samples: {tag} ({len(recs)}) ==={RST}")
    for i, r in enumerate(recs, 1):
        js = json.dumps(r, ensure_ascii=False, indent=2)
        js_to_show = js[:max_chars] + \
            "..." if (max_chars and max_chars >
                      0 and len(js) > max_chars) else js
        print(f"{YEL}-- {tag} sample {i}{RST}\n{js_to_show}")


def print_common_fields(
    left_label: str,
    right_label: str,
    sch1n: Any,
    sch2n: Any,
    colors: tuple[str, str, str, str, str],
) -> None:
    """
    Print the intersection of field paths (including nested fields) when both roots are objects.

    Parameters
    ----------
    left_label, right_label : str
        Labels for headings.
    sch1n, sch2n : Any
        Normalized/coerced trees. Only dict roots contribute fields.
    colors : tuple[str, str, str, str, str]
        (RED, GRN, YEL, CYN, RST) color codes; pass empty strings to disable.
    """
    RED, GRN, YEL, CYN, RST = colors

    def _as_field_dict(x: Any) -> dict:
        """Return a dict of fields if root is a dict; otherwise empty (for non-object roots)."""
        return x if isinstance(x, dict) else {}

    d1 = _as_field_dict(sch1n)
    d2 = _as_field_dict(sch2n)

    # Use flatten_paths to get all nested field paths
    from .utils import flatten_paths
    paths1 = flatten_paths(d1)
    paths2 = flatten_paths(d2)
    common_paths = sorted(set(paths1) & set(paths2))

    print(
        f"\n{YEL}-- Common fields in {left_label} ∩ {right_label} -- ({len(common_paths)}){RST}")
    for path in common_paths:
        clean_path = fmt_dot_path(path)
        print(f"  {CYN}{clean_path}{RST}")


def print_path_changes(
    left_label: str,
    right_label: str,
    changes: list[dict[str, Any]],
    *,
    colors: tuple[str, str, str, str, str],
) -> None:
    """
    Print a list of "path changes": same field name appearing at different paths
    on the left vs right tree.

    Uses a clear 3-section structure for each field:
    1. "Shared field locations and/or field paths:" - paths common to both sides
    2. "Only in [left_label]:" - paths unique to left side
    3. "Only in [right_label]:" - paths unique to right side

    This eliminates any ambiguity about what's shared vs unique.

    Parameters
    ----------
    left_label, right_label : str
        Labels for headings.
    changes : list[dict[str, Any]]
        Each entry must contain: {"name": <field_name>, "shared": [common_paths],
        "left": [left_only_paths], "right": [right_only_paths]}.
    colors : tuple[str, str, str, str, str]
        (RED, GRN, YEL, CYN, RST) color codes; pass empty strings to disable.
    """
    RED, GRN, YEL, CYN, RST = colors
    print(f"\n{YEL}-- Path changes (same field name in different locations) -- ({len(changes)}){RST}")
    for ch in changes:
        name = ch["name"]
        shared_paths = ch["shared"]
        left_paths = ch["left"]
        right_paths = ch["right"]

        print(f"  {CYN}{name}{RST}:")

        # Shared paths
        if shared_paths:
            print("    Shared field locations and/or field paths:")
            for path in shared_paths:
                clean_path = fmt_dot_path(path)
                print(f"      • {clean_path}")

        # Left side only
        if left_paths:
            print(f"    Only in {RED}{left_label}{RST}:")
            for path in left_paths:
                clean_path = fmt_dot_path(path)
                print(f"      • {clean_path}")

        # Right side only
        if right_paths:
            print(f"    Only in {GRN}{right_label}{RST}:")
            for path in right_paths:
                clean_path = fmt_dot_path(path)
                print(f"      • {clean_path}")

        print()  # Empty line between field names for better separation
