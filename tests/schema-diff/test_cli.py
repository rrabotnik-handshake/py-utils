import json
import gzip
import sys

from schema_diff.io_utils import _run


def _w(tmp_path, name, text, gz=False) -> str:
    """Write a tiny JSON/NDJSON file (optionally gzipped) and return its path as str."""
    p = tmp_path / name
    if gz:
        with gzip.open(p, "wt", encoding="utf-8") as f:
            f.write(text)
    else:
        p.write_text(text, encoding="utf-8")
    return str(p)

def test_cli_first_record_show_samples(tmp_path):
    f1 = _w(tmp_path, "a.ndjson.gz",
            '{"id":1,"name":"A"}\n{"id":2,"name":"B"}\n', gz=True)
    f2 = _w(tmp_path, "b.ndjson.gz",
            '{"id":"1","name":"A"}\n{"id":"2","name":"B"}\n', gz=True)
    exe = [sys.executable, "-m", "schema_diff.cli"]
    res = _run(exe + [f1, f2, "--first-record",
               "--show-samples", "--no-color"])
    assert res.returncode == 0, res.stderr
    assert res.stderr == ""
    # Samples should appear
    assert "=== Samples:" in res.stdout
    # And a mismatch (id: int vs str)
    assert "True schema mismatches" in res.stdout


def test_cli_seeded_sampling_json_out(tmp_path):
    f1 = _w(tmp_path, "a.ndjson", "\n".join(
        json.dumps({"i": i}) for i in range(10)) + "\n")
    f2 = _w(tmp_path, "b.ndjson", "\n".join(
        json.dumps({"i": str(i)}) for i in range(10)) + "\n")
    out1 = tmp_path / "diff1.json"
    out2 = tmp_path / "diff2.json"
    exe = [sys.executable, "-m", "schema_diff.cli"]
    r1 = _run(exe + [f1, f2, "-k", "3", "--seed", "42",
              "--json-out", str(out1), "--no-color"])
    r2 = _run(exe + [f1, f2, "-k", "3", "--seed", "42",
              "--json-out", str(out2), "--no-color"])
    assert r1.returncode == 0 and r2.returncode == 0
    assert r1.stderr == "" and r2.stderr == ""
    # Same seed => identical snapshot
    assert out1.read_text() == out2.read_text()


def test_cli_vs_json_schema(tmp_path):
    # Data
    f1 = _w(tmp_path, "a.ndjson", '{"id":1,"ts":"2025-01-01T00:00:00Z"}\n')
    # JSON Schema
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "ts": {"type": "string", "format": "date-time"}
        },
        "required": ["id"]
    }
    sch = tmp_path / "schema.json"
    sch.write_text(json.dumps(schema), encoding="utf-8")
    exe = [sys.executable, "-m", "schema_diff.cli"]
    res = _run(exe + [f1, "--json-schema", str(sch),
               "--first-record", "--show-samples", "--no-color"])
    assert res.returncode == 0, res.stderr
    assert res.stderr == ""
    assert "=== Samples:" in res.stdout


def test_cli_vs_spark_schema(tmp_path):
    f1 = _w(tmp_path, "a.ndjson", '{"id":1,"full_name":"A"}\n')
    spark = """root
 |-- id: long (nullable = false)
 |-- full_name: string (nullable = false)
"""
    sp = tmp_path / "spark.txt"
    sp.write_text(spark, encoding="utf-8")
    exe = [sys.executable, "-m", "schema_diff.cli"]
    res = _run(exe + [f1, "--spark-schema", str(sp),
               "--first-record", "--show-samples", "--no-color"])
    assert res.returncode == 0, res.stderr
    assert res.stderr == ""
    assert "=== Samples:" in res.stdout


def test_cli_vs_sql_schema(tmp_path):
    f1 = _w(tmp_path, "a.ndjson", '{"id":1,"full_name":"A"}\n')
    sql = """CREATE TABLE p (
      id BIGINT NOT NULL,
      full_name TEXT NOT NULL
    );"""
    sp = tmp_path / "schema.sql"
    sp.write_text(sql, encoding="utf-8")
    exe = [sys.executable, "-m", "schema_diff.cli"]
    res = _run(exe + [f1, "--sql-schema", str(sp), "--sql-table",
               "p", "--first-record", "--show-samples", "--no-color"])
    assert res.returncode == 0, res.stderr
    assert res.stderr == ""
    assert "=== Samples:" in res.stdout


def test_cli_both_modes_with_record(tmp_path):
    """Verify --both-modes prints ordinal AND sampled sections when a record is specified."""
    f1 = _w(tmp_path, "a.ndjson", '\n'.join(
        ['{"x":1}', '{"x":2}', '{"x":3}']) + "\n")
    f2 = _w(tmp_path, "b.ndjson", '\n'.join(
        ['{"x":"1"}', '{"x":"2"}', '{"x":"3"}']) + "\n")
    exe = [sys.executable, "-m", "schema_diff.cli"]
    res = _run(exe + [f1, f2, "--record", "1", "--both-modes",
               "-k", "2", "--show-samples", "--seed", "7", "--no-color"])
    assert res.returncode == 0, res.stderr
    assert res.stderr == ""
    # We expect *two* samples sections: one for file1 and one for file2 in the sampled run
    assert res.stdout.count("=== Samples: ") >= 2
