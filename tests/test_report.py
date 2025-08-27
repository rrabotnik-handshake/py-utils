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
