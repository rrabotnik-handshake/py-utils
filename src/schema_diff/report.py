# schema_diff/report.py
from typing import Any, Dict, Tuple
from .normalize import _has_any, is_presence_issue
import json


def _clean(path: str) -> str:
    s = path[4:] if path.startswith("root") else path
    s = s.replace("']['", ".").replace("['", ".").replace("']", "")
    return s.lstrip(".")


def _root_dict_value_change_fallback(diff) -> tuple[list[str], list[str]]:
    """
    If the entire root dict changed (DeepDiff reports a single values_changed on root),
    derive 'only in left/right' by comparing keys of old/new dicts.
    """
    vc = diff.get("values_changed") or {}
    if len(vc) != 1:
        return ([], [])
    key, ch = next(iter(vc.items()))
    if key not in ("root", "root['root']", "root['__root__']", ""):
        return ([], [])
    old, new = ch.get("old_value"), ch.get("new_value")
    if not isinstance(old, dict) or not isinstance(new, dict):
        return ([], [])
    only1 = sorted(old.keys() - new.keys())
    only2 = sorted(new.keys() - old.keys())
    return [str(k) for k in only1], [str(k) for k in only2]


def _root_list_value_change_fallback(diff) -> tuple[list[str], list[str]]:
    """
    Similar fallback when the root is a list of field entries (e.g., Spark/BigQuery),
    like [{'name': 'id', 'type': ...}, ...] or [{'id': 'int'}, {'name': 'str'}, ...].
    """
    vc = diff.get("values_changed") or {}
    if len(vc) != 1:
        return ([], [])
    key, ch = next(iter(vc.items()))
    if key not in ("root", "root['root']", "root['__root__']", ""):
        return ([], [])
    old, new = ch.get("old_value"), ch.get("new_value")

    def names_from_list(lst):
        if isinstance(lst, list) and lst:
            # case 1: [{'name': 'id', 'type': ...}, ...]
            if all(isinstance(el, dict) and "name" in el for el in lst):
                return {str(el["name"]) for el in lst}
            # case 2: [{'id': 'int'}, {'name': 'str'}, ...]
            if all(isinstance(el, dict) and len(el) == 1 for el in lst):
                out = set()
                for el in lst:
                    for k in el.keys():
                        out.add(str(k))
                return out
        return None

    ks_old = names_from_list(old)
    ks_new = names_from_list(new)
    if ks_old is None or ks_new is None:
        return ([], [])
    only1 = sorted(ks_old - ks_new)
    only2 = sorted(ks_new - ks_old)
    return only1, only2

def build_report_struct(diff, f1: str, f2: str, include_presence: bool) -> Dict[str, Any]:
    fb1, fb2 = _root_dict_value_change_fallback(diff)

    if not fb1 and not fb2:
        lb1, lb2 = _root_list_value_change_fallback(diff)
        if lb1 or lb2:
            fb1, fb2 = lb1, lb2

    only_in_2 = sorted(_clean(p)
                       for p in diff.get("dictionary_item_added", []))
    only_in_1 = sorted(_clean(p)
                       for p in diff.get("dictionary_item_removed", []))
    if not only_in_1 and not only_in_2 and (fb1 or fb2):
        only_in_1, only_in_2 = fb1, fb2

    presence, schema = [], []
    for p, ch in (diff.get("values_changed") or {}).items():
        if p in ("root", "root['root']", "root['__root__']", ""):
            ov, nv = ch.get("old_value"), ch.get("new_value")
            if isinstance(ov, dict) and isinstance(nv, dict):
                continue
        old, new = ch.get("old_value"), ch.get("new_value")
        if _has_any(old) or _has_any(new):
            continue
        entry = {"path": _clean(p), "file1": old, "file2": new}
        (presence if is_presence_issue(old, new) else schema).append(entry)
    schema.sort(key=lambda x: x["path"])

    out = {
        "meta": {"direction": f"{f1} -> {f2}"},
        "only_in_file1": only_in_1,
        "only_in_file2": only_in_2,
        "schema_mismatches": schema,
    }
    if include_presence:
        out["presence_issues"] = presence
    return out


def print_report_text(
    report: Dict[str, Any],
    f1: str,
    f2: str,
    *,
    colors: Tuple[str, str, str, str, str],
    show_presence: bool,
    title_suffix: str = "",
):
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
            print(f"  {CYN}{e['path']}{RST}: {e['file1']} → {e['file2']}")

    mism = report["schema_mismatches"]
    print(f"\n{YEL}-- True schema mismatches -- ({len(mism)}){RST}")
    for e in mism:
        print(
            f"  {CYN}{e['path']}{RST}: {RED}{e['file1']}{RST} → {GRN}{e['file2']}{RST}")


# ---- Samples printing (color-safe) ----

def print_samples(tag: str, recs, *, colors: tuple[str, str, str, str, str] = ("", "", "", "", ""), max_chars: int = 2000):
    RED, GRN, YEL, CYN, RST = colors
    print(f"\n{CYN}=== Samples: {tag} ({len(recs)}) ==={RST}")
    for i, r in enumerate(recs, 1):
        js = json.dumps(r, ensure_ascii=False, indent=2)
        if max_chars and max_chars > 0 and len(js) > max_chars:
            js_to_show = js[:max_chars] + "..."
        else:
            js_to_show = js
        print(f"{YEL}-- {tag} sample {i}{RST}\n{js_to_show}")
