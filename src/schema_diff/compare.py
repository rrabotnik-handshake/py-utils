# schema_diff/compare.py
from copy import deepcopy
from typing import Any, Iterable, Optional, Set
from .normalize import walk_normalize
from .schema_from_data import merged_schema_from_samples
from .report import build_report_struct, print_report_text
from deepdiff import DeepDiff
from .presence import apply_presence
import json


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
):
    # Apply presence to reference to align with data-side unions that include 'missing'
    ref_for_diff = _inject_presence_for_diff(ref_schema, required_paths)
    sch2n = walk_normalize(ref_for_diff)

    sch1 = merged_schema_from_samples(s1_records, cfg)
    sch1n = walk_normalize(sch1)

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
    dump_schemas: str | None = None,
    json_out: str | None = None,
    title_suffix: str = "",
):
    """
    Pure schema-vs-schema comparison that respects presence:
    - Inject '|missing' for fields NOT in required sets
    - Normalize both sides
    - DeepDiff + report
    """
    # Inject presence expectations
    lt = apply_presence(left_tree, left_required)
    rt = apply_presence(right_tree, right_required)

    ln = walk_normalize(lt)
    rn = walk_normalize(rt)

    diff = DeepDiff(ln, rn, ignore_order=True)
    RED, GRN, YEL, CYN, RST = cfg.colors()

    direction = f"{left_label} -> {right_label}"
    if not diff:
        print(
            f"\n{CYN}=== Schema diff (types only, {direction}{title_suffix}) ==={RST}\nNo differences.")
        if dump_schemas:
            with open(dump_schemas, "w", encoding="utf-8") as fh:
                json.dump({"left": ln, "right": rn}, fh,
                          ensure_ascii=False, indent=2)
        if json_out:
            with open(json_out, "w", encoding="utf-8") as fh:
                json.dump({"meta": {"direction": direction, "mode": title_suffix.strip('; ').strip()},
                           "note": "No differences"}, fh, ensure_ascii=False, indent=2)
        return

    report = build_report_struct(
        diff, left_label, right_label, include_presence=cfg.show_presence)
    report["meta"]["mode"] = title_suffix.strip("; ").strip()
    print_report_text(report, left_label, right_label, colors=cfg.colors(
    ), show_presence=cfg.show_presence, title_suffix=title_suffix)

    if dump_schemas:
        with open(dump_schemas, "w", encoding="utf-8") as fh:
            json.dump({"left": ln, "right": rn}, fh,
                      ensure_ascii=False, indent=2)
    if json_out:
        with open(json_out, "w", encoding="utf-8") as fh:
            json.dump(report, fh, ensure_ascii=False, indent=2)


def compare_data_to_ref(file1: str, ref_label: str, ref_schema: Any, *, cfg,
                        s1_records, dump_schemas: str | None = None,
                        json_out: str | None = None, title_suffix: str = "",
                        required_paths=None):
    # Data left:
    from .schema_from_data import merged_schema_from_samples
    left_tree = merged_schema_from_samples(s1_records, cfg)
    left_required: Set[str] = set()

    # Ref right:
    right_tree = ref_schema
    right_required: Set[str] = required_paths or set()

    compare_trees(
        file1, ref_label, left_tree, left_required, right_tree, right_required,
        cfg=cfg, dump_schemas=dump_schemas, json_out=json_out, title_suffix=title_suffix
    )
