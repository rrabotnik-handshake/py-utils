# schema_diff/compare.py
from copy import deepcopy
from typing import Any, Iterable, Optional, Set
from .normalize import walk_normalize
from .schema_from_data import merged_schema_from_samples
from .report import build_report_struct, print_report_text
from deepdiff import DeepDiff
import json


def _coerce_root_list_to_dict(tree: Any) -> Any:
    """
    If the root is a list of field entries, convert it to a {name: type} dict.
    Supports two common shapes:
      1) [{'name': 'id', 'type': ...}, ...]
      2) [{'id': 'int'}, {'name': 'str'}, ...]
    If the list doesn't look like fields, return it unchanged.
    """

    if not isinstance(tree, list) or not tree:
        return tree

    # case 1: [{'name': ..., 'type': ...}, ...]
    if all(isinstance(el, dict) and "name" in el for el in tree):
        out = {}
        for el in tree:
            name = str(el["name"])
            # prefer 'type', but tolerate other common keys
            t = el.get("type", el.get("dataType", el.get("dtype", "any")))
            out[name] = t
        return out

    # case 2: [{'id': 'int'}, {'name': 'str'}, ...]
    if all(isinstance(el, dict) and len(el) == 1 for el in tree):
        out = {}
        for el in tree:
            (name, t) = next(iter(el.items()))
            out[str(name)] = t
        return out

    return tree

def _wrap_optional(t: Any) -> Any:
    """Wrap a scalar or array type with union(...|missing) if not already wrapped."""
    if isinstance(t, str):
        if t.startswith("union(") and t.endswith(")"):
            parts = set(t[6:-1].split("|"))
            if "missing" in parts:
                return t
            parts.add("missing")
            return "union(" + "|".join(sorted(parts)) + ")"
        # array type is represented as list in tree, so keep scalar here
        return f"union({t}|missing)"
    if isinstance(t, list):
        # arrays: treat as union(array|missing)
        return "union(array|missing)"
    # objects are handled by recursion at parent
    return t


def _inject_presence_for_diff(tree: Any, required: Optional[Iterable[str]]) -> Any:
    """
    Given a pure type tree (no presence mixed in) and a set of required dotted-paths,
    return a new tree where non-required leaf/array fields are wrapped with '|missing'
    so it aligns with how the data-derived schema encodes presence.
    """
    required = set(required or [])
    out = deepcopy(tree)

    def walk(node: Any, prefix: str):
        if isinstance(node, dict):
            for k, v in list(node.items()):
                path = f"{prefix}.{k}" if prefix else k
                # Recurse first to handle nested objects
                if isinstance(v, dict):
                    walk(v, path)
                else:
                    if path in required:
                        # required -> leave type as-is
                        continue
                    # optional -> wrap in union(...|missing)
                    node[k] = _wrap_optional(v)
        elif isinstance(node, list):
            # arrays: presence applies to the field holding the array, not elements
            pass
        else:
            # scalar at root is unusual; presence is for object members
            pass

    if isinstance(out, dict):
        walk(out, "")
    return out


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
):

    # Coerce root list-of-fields into dicts so diffs are per-path, not list indices
    ref_schema = _coerce_root_list_to_dict(ref_schema)

    # Apply presence to reference to align with data-side unions that include 'missing'
    ref_for_diff = _inject_presence_for_diff(ref_schema, required_paths)
    sch2n = walk_normalize(ref_for_diff)

    sch1 = merged_schema_from_samples(s1_records, cfg)
    sch1n = walk_normalize(sch1)

    sch1n = _coerce_root_list_to_dict(sch1n)
    sch2n = _coerce_root_list_to_dict(sch2n)

    # Optionally show common fields (works for any parser, after normalization)
    if show_common:
        _print_common_fields(file1, ref_label, sch1n, sch2n, cfg.colors())

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
                    fh, ensure_ascii=False, indent=2
                )
        return

    report = build_report_struct(
        diff, file1, ref_label, include_presence=cfg.show_presence)
    report["meta"]["mode"] = title_suffix.strip("; ").strip()
    print_report_text(report, file1, ref_label, colors=cfg.colors(
    ), show_presence=cfg.show_presence, title_suffix=title_suffix)

    if dump_schemas:
        with open(dump_schemas, "w", encoding="utf-8") as fh:
            json.dump({"file1": sch1n, "ref": sch2n},
                      fh, ensure_ascii=False, indent=2)
    if json_out:
        with open(json_out, "w", encoding="utf-8") as fh:
            json.dump(report, fh, ensure_ascii=False, indent=2)


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
    Compare two *type trees* (schemas) and print a diff.

    Notes:
      - Only the TYPE TREES are diffed (left_tree vs right_tree).
      - `left_required` / `right_required` are currently not merged into the diff;
        they’re available if you later want to add a presence-only section based
        on external constraints. For now we keep behavior consistent with other modes:
        presence section comes from the trees (i.e., unions with 'missing'), not required sets.
    """
    # Coerce root list-of-fields into dicts on both sides
    left_tree = _coerce_root_list_to_dict(left_tree)
    right_tree = _coerce_root_list_to_dict(right_tree)

    # Normalize *trees only*
    sch1n = walk_normalize(left_tree)
    sch2n = walk_normalize(right_tree)

    sch1n = _coerce_root_list_to_dict(sch1n)
    sch2n = _coerce_root_list_to_dict(sch2n)

    if show_common:
        _print_common_fields(left_label, right_label,
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
                    {"meta": {"direction": direction, "mode": title_suffix.strip("; ").strip()},
                     "note": "No differences"},
                    fh, ensure_ascii=False, indent=2,
                )
        return

    # Build and print human-friendly report
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

    if dump_schemas:
        with open(dump_schemas, "w", encoding="utf-8") as fh:
            json.dump({"left": sch1n, "right": sch2n},
                      fh, ensure_ascii=False, indent=2)

    if json_out:
        with open(json_out, "w", encoding="utf-8") as fh:
            json.dump(report, fh, ensure_ascii=False, indent=2)


def _as_field_dict(x: Any) -> dict:
    """Return a dict of fields if root is a dict; otherwise empty (for non-object roots)."""
    return x if isinstance(x, dict) else {}


def _print_common_fields(left_label: str, right_label: str, sch1n: Any, sch2n: Any, colors: tuple[str, str, str, str, str]) -> None:
    RED, GRN, YEL, CYN, RST = colors
    d1 = _as_field_dict(sch1n)
    d2 = _as_field_dict(sch2n)
    common = sorted(set(d1.keys()) & set(d2.keys()))
    print(
        f"\n{YEL}-- Common fields in {left_label} ∩ {right_label} -- ({len(common)}){RST}")
    for k in common:
        print(f"  {CYN}{k}{RST}")