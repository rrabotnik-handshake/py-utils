import re
import json
import sys
from schema_diff.io_utils import _run
from schema_diff.report import build_report_struct


def test_report_presence_vs_schema():
    diff = {
        "dictionary_item_removed": ["root['only_in_2']"],
        "dictionary_item_added":   ["root['only_in_1']"],
        "values_changed": {
            # schema mismatch
            "root['a']": {"old_value": "str", "new_value": "int"},
            # presence issue
            "root['b']": {"old_value": "union(str|missing)", "new_value": "str"},
        },
    }
    r = build_report_struct(diff, "f1", "f2", include_presence=True)
    schema = [e["path"] for e in r["schema_mismatches"]]
    pres = [e["path"] for e in r["presence_issues"]]
    assert "a" in schema
    assert "b" in pres
    assert "only_in_1" in r["only_in_file2"]
    assert "only_in_2" in r["only_in_file1"]


def _extract_common_keys(stdout: str) -> set[str]:
    keys, in_block = set(), False
    for ln in stdout.splitlines():
        if ln.strip().startswith("-- Common fields"):
            in_block = True
            continue
        if in_block:
            # next section starts
            if ln.strip().startswith("-- "):
                break
            # bullet lines: two spaces then the path
            m = re.match(r"^\s{2}(.+?)\s*$", ln)
            if m:
                keys.add(m.group(1))
    return keys


def test_show_common_keys(tmp_path):
    left = tmp_path / "l.json"
    right = tmp_path / "r.json"
    left.write_text(json.dumps({"a": 1, "b": 2}), encoding="utf-8")
    right.write_text(json.dumps({"b": "x", "c": 3}), encoding="utf-8")

    exe = [sys.executable, "-m", "schema_diff.cli"]
    res = _run(exe + [str(left), str(right), "--left", "data", "--right", "data", "--first-record", "--show-common", "--no-color"])
    assert res.returncode == 0
    assert "Common fields" in res.stdout

    keys = _extract_common_keys(res.stdout)
    assert keys == {"b"}
