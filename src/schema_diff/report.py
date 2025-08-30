"""
Human-friendly diff report helpers.

This module turns a DeepDiff (computed elsewhere) into:
  - a stable, JSON-serializable report structure, and
  - readable, colorized text output.

Key features
------------
- Handles the common "entire root changed" DeepDiff corner case by deriving
  "only in left/right" from root dict or list-of-fields (Spark/BigQuery-like) shapes.
- Splits differences into *type mismatches* and *presence issues* (i.e., changes that
  are just about `missing` in unions), with optional presence printing.
- Filters out comparisons involving 'any' to reduce noise.
- Always prints section headers with counts, which the tests assert on.
"""

from typing import Any
import json

from .utils import fmt_dot_path, clean_deepdiff_path
from .normalize import _has_any, is_presence_issue


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

    # Stable order for deterministic output
    schema.sort(key=lambda x: x["path"])
    presence.sort(key=lambda x: x["path"])

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
        print(f"  {RED}{p}{RST}")
    print(f"\n{YEL}-- Only in {f2} -- ({len(only2)}){RST}")
    for p in only2:
        print(f"  {GRN}{p}{RST}")

    if show_presence and "presence_issues" in report:
        pres = report["presence_issues"]
        print(f"\n{YEL}-- Missing / optional (presence) -- ({len(pres)}){RST}")
        for e in pres:
            print(
                f"  {CYN}{fmt_dot_path(e['path'])}{RST}: {e['file1']} → {e['file2']}")

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
    Print the intersection of top-level field names when both roots are objects.

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
    common = sorted(set(d1.keys()) & set(d2.keys()))
    print(
        f"\n{YEL}-- Common fields in {left_label} ∩ {right_label} -- ({len(common)}){RST}")
    for k in common:
        print(f"  {CYN}{k}{RST}")


def print_path_changes(
    left_label: str,
    right_label: str,
    changes: list[dict[str, Any]],
    *,
    colors: tuple[str, str, str, str, str],
) -> None:
    """
    Print a list of “path changes”: same field name appearing at different paths
    on the left vs right tree.

    Parameters
    ----------
    left_label, right_label : str
        Labels for headings.
    changes : list[dict[str, Any]]
        Each entry must contain: {"name": <field_name>, "left": [paths], "right": [paths]}.
    colors : tuple[str, str, str, str, str]
        (RED, GRN, YEL, CYN, RST) color codes; pass empty strings to disable.
    """
    RED, GRN, YEL, CYN, RST = colors
    print(f"\n{YEL}-- Path changes (same field name in different locations) -- ({len(changes)}){RST}")
    for ch in changes:
        name = ch["name"]
        lp = ", ".join(ch["left"])
        rp = ", ".join(ch["right"])
        print(
            f"  {CYN}{name}{RST}:\n"
            f"    {RED}{left_label}{RST}: {lp}\n"
            f"    {GRN}{right_label}{RST}: {rp}"
        )
