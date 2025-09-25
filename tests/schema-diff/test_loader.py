import json
import sys
from schema_diff.io_utils import _run


def test_autodetect_jsonschema_vs_force_data(tmp_path):
    sch = {
        "type": "object",
        "properties": {"id": {"type": "integer"}},
        "required": ["id"],
    }
    p = tmp_path / "maybe.json"
    p.write_text(json.dumps(sch), encoding="utf-8")

    exe = [sys.executable, "-m", "schema_diff.cli"]

    # Auto → JSON Schema
    r1 = _run(exe + [str(p), str(p), "--no-color"])
    assert r1.returncode == 0

    # Force DATA on left, JSON Schema on right — should still work
    r2 = _run(
        exe
        + [
            str(p),
            str(p),
            "--left",
            "data",
            "--right",
            "jsonschema",
            "--first-record",
            "--no-color",
        ]
    )
    assert r2.returncode == 0
