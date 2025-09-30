import json
import sys


def test_autodetect_jsonschema_vs_force_data(tmp_path, run_cli):
    sch = {
        "type": "object",
        "properties": {"id": {"type": "integer"}},
        "required": ["id"],
    }
    p = tmp_path / "maybe.json"
    p.write_text(json.dumps(sch), encoding="utf-8")

    # Auto → JSON Schema
    r1 = run_cli([str(p), str(p), "--no-color"])
    assert r1.returncode == 0

    # Force DATA on left, JSON Schema on right — should still work
    r2 = run_cli([
        str(p),
        str(p),
        "--left",
        "data",
        "--right",
        "jsonschema",
        "--first-record",
        "--no-color",
    ])
    assert r2.returncode == 0
