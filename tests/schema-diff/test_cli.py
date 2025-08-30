import json
import sys

from schema_diff.io_utils import _run


def test_cli_seeded_sampling_json_out(tmp_path, write_file):
    f1 = write_file( "a.ndjson", "\n".join(
        json.dumps({"i": i}) for i in range(10)) + "\n")
    f2 = write_file( "b.ndjson", "\n".join(
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

def test_cli_both_modes_with_record(tmp_path, write_file):
    """Verify --both-modes prints ordinal AND sampled sections when a record is specified."""
    f1 = write_file( "a.ndjson", '\n'.join(
        ['{"x":1}', '{"x":2}', '{"x":3}']) + "\n")
    f2 = write_file( "b.ndjson", '\n'.join(
        ['{"x":"1"}', '{"x":"2"}', '{"x":"3"}']) + "\n")
    exe = [sys.executable, "-m", "schema_diff.cli"]
    res = _run(exe + [f1, f2, "--record", "1", "--both-modes",
               "-k", "2", "--show-samples", "--seed", "7", "--no-color"])
    assert res.returncode == 0, res.stderr
    assert res.stderr == ""
    # We expect *two* samples sections: one for file1 and one for file2 in the sampled run
    assert res.stdout.count("=== Samples: ") >= 2

    def test_cli_samples_sections(tmp_path, write_file, run_cli):
        # data with int vs str mismatch to force samples
        f1 = write_file("a.ndjson", '{"id":1}\n')
        sch = tmp_path / "schema.json"
        sch.write_text(json.dumps({"type": "object", "properties": {
                    "id": {"type": "string"}}}), encoding="utf-8")

        spark = tmp_path / "spark.txt"
        spark.write_text(
            "root\n |-- id: string (nullable = false)\n", encoding="utf-8")

        sql = tmp_path / "schema.sql"
        sql.write_text("CREATE TABLE p (id TEXT NOT NULL);", encoding="utf-8")

        matrix = [
            ([f1, "--json-schema", str(sch), "--first-record",
            "--show-samples", "--no-color"]),
            ([f1, "--spark-schema", str(spark),
            "--first-record", "--show-samples", "--no-color"]),
            ([f1, "--sql-schema", str(sql), "--sql-table", "p",
            "--first-record", "--show-samples", "--no-color"]),
        ]
        for args in matrix:
            res = run_cli(args)
            assert res.returncode == 0, res.stderr
            assert "=== Samples:" in res.stdout
