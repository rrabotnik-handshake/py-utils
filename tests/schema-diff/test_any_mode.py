import json
import sys
from schema_diff.io_utils import _run


def test_any_mode_matrix(tmp_path, write_file, run_cli):
    # Arrange inputs
    data = write_file(
        "data.ndjson.gz", '{"id":1,"name":"A"}\n{"id":2,"name":"B"}\n', gz=True
    )
    js_path = tmp_path / "schema.json"
    js_path.write_text(
        json.dumps(
            {
                "type": "object",
                "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
                "required": ["id", "name"],
            }
        ),
        encoding="utf-8",
    )
    sql_path = tmp_path / "schema.sql"
    sql_path.write_text(
        "CREATE TABLE p (id BIGINT NOT NULL, full_name TEXT NOT NULL);",
        encoding="utf-8",
    )

    cases = [
        # data vs jsonschema
        (
            [
                data,
                str(js_path),
                "--left",
                "data",
                "--right",
                "jsonschema",
                "--first-record",
                "--no-color",
            ],
            True,
        ),
        # jsonschema vs sql (explicit right-table)
        (
            [
                str(js_path),
                str(sql_path),
                "--left",
                "jsonschema",
                "--right",
                "sql",
                "--right-table",
                "p",
                "--no-color",
            ],
            True,
        ),
        # auto-detect jsonschema vs sql
        ([str(js_path), str(sql_path), "--right-table", "p", "--no-color"], True),
    ]
    for args, expect_no_diffs in cases:
        res = run_cli(args)
        assert res.returncode == 0, res.stderr
        assert "Schema diff" in res.stdout
        if expect_no_diffs:
            assert (
                "No differences" in res.stdout or "Type mismatches -- (0)" in res.stdout
            )


def test_any_any_sql_multi_table_select(tmp_path, run_cli):
    # Two tables in one SQL file – we must select explicitly
    sql = """\
CREATE TABLE a (
  id BIGINT NOT NULL
);

CREATE TABLE b (
  id TEXT NOT NULL
);
"""
    sqlp = tmp_path / "multi.sql"
    sqlp.write_text(sql, encoding="utf-8")

    # JSON Schema for integer id (matches table a, not b)
    schema = {
        "type": "object",
        "properties": {"id": {"type": "integer"}},
        "required": ["id"],
    }
    js = tmp_path / "schema.json"
    js.write_text(json.dumps(schema), encoding="utf-8")

    # Compare against table a -> should match (no diffs)
    res_ok = run_cli([
        str(js),
        str(sqlp),
        "--left",
        "jsonschema",
        "--right",
        "sql",
        "--right-table",
        "a",
        "--no-color",
    ])
    assert res_ok.returncode == 0, res_ok.stderr
    assert "Schema diff" in res_ok.stdout
    assert (
        "No differences" in res_ok.stdout or "Type mismatches -- (0)" in res_ok.stdout
    )

    # Compare against table b -> should mismatch (int vs str)
    res_bad = run_cli([
        str(js),
        str(sqlp),
        "--left",
        "jsonschema",
        "--right",
        "sql",
        "--right-table",
        "b",
        "--no-color",
    ])
    assert res_bad.returncode == 0, res_bad.stderr
    assert "Schema diff" in res_bad.stdout
    assert "Type mismatches" in res_bad.stdout
    # A helpful sanity check that the path 'id' is mentioned somewhere
    assert ".id" in res_bad.stdout or " id" in res_bad.stdout


def test_any_any_mismatch_data_vs_jsonschema(tmp_path, write_file, run_cli):
    # Data has id as string, schema expects integer
    data = write_file("bad.ndjson", '{"id":"1"}\n', gz=False)
    schema = {
        "type": "object",
        "properties": {"id": {"type": "integer"}},
        "required": ["id"],
    }
    js = tmp_path / "schema.json"
    js.write_text(json.dumps(schema), encoding="utf-8")

    res = run_cli([
        data,
        str(js),
        "--left",
        "data",
        "--right",
        "jsonschema",
        "--first-record",
        "--no-color",
    ])
    assert res.returncode == 0, res.stderr
    assert "Type mismatches" in res.stdout
    # Presence section optional depending on your --no-presence default, so we just check mismatch exists.


def test_any_any_data_vs_sql(tmp_path, write_file, run_cli):
    # Data: id int, full_name str
    data = write_file("p.ndjson.gz", '{"id":1,"full_name":"A"}\n', gz=True)
    # SQL: matching columns
    sql = """CREATE TABLE p (
      id BIGINT NOT NULL,
      full_name TEXT NOT NULL
    );"""
    sqlp = tmp_path / "schema.sql"
    sqlp.write_text(sql, encoding="utf-8")

    res = run_cli([
        data,
        str(sqlp),
        "--left",
        "data",
        "--right",
        "sql",
        "--right-table",
        "p",
        "--first-record",
        "--no-color",
    ])
    assert res.returncode == 0, res.stderr
    assert "Schema diff" in res.stdout
    assert "No differences" in res.stdout or "Type mismatches -- (0)" in res.stdout
