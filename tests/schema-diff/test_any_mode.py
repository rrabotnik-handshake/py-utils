# tests/test_any_any_mode.py
import json
import gzip
import subprocess
import sys


def _w(tmp_path, name, text, gz=False):
    p = tmp_path / name
    if gz:
        with gzip.open(p, "wt", encoding="utf-8") as f:
            f.write(text)
    else:
        p.write_text(text, encoding="utf-8")
    return str(p)


class CommandError(Exception):
    """Raised when _run fails with non-zero exit code."""

    def __init__(self, cmd, returncode, stdout, stderr):
        self.cmd = cmd
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        super().__init__(
            f"Command {cmd} failed with {returncode}: {stderr.strip()}")

def _run(args, cwd=None, env=None, check_untrusted=True):
    """
    Run a subprocess safely.

    - args: list of strings (no shell=True)
    - Raises CommandError if exit != 0
    - Optionally sanity-check args if `check_untrusted=True`
    """
    # validate args
    if not isinstance(args, (list, tuple)) or not args:
        raise ValueError("args must be a non-empty list of strings")

    for a in args:
        if not isinstance(a, str):
            raise ValueError(f"Non-string arg: {a!r}")
        if check_untrusted:
            # crude guard against control chars, semicolons, pipes, etc.
            if any(c in a for c in [';', '|', '&', '\n', '\r']):
                raise ValueError(f"Suspicious characters in arg: {a!r}")

    # run
    res = subprocess.run(
        args, cwd=cwd, capture_output=True, text=True, env=env)

    if res.returncode != 0:
        raise CommandError(args, res.returncode, res.stdout, res.stderr)

    return res



def test_any_any_data_vs_jsonschema(tmp_path):
    # NDJSON data (id int, name str)
    data = _w(tmp_path, "data.ndjson.gz",
              '{"id":1,"name":"A"}\n{"id":2,"name":"B"}\n', gz=True)

    # JSON Schema (id integer, name string)
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"}
        },
        "required": ["id", "name"]
    }
    js = tmp_path / "schema.json"
    js.write_text(json.dumps(schema), encoding="utf-8")

    exe = [sys.executable, "-m", "schema_diff.cli"]
    # explicit kinds: left=data, right=jsonschema
    res = _run(exe + [
        data, str(js),
        "--left", "data",
        "--right", "jsonschema",
        "--first-record",
        "--no-color",
    ])
    assert res.returncode == 0, res.stderr
    assert "Schema diff" in res.stdout
    # Likely no differences:
    assert "No differences" in res.stdout or "True schema mismatches -- (0)" in res.stdout


def test_any_any_jsonschema_vs_sql(tmp_path):
    # JSON Schema (id integer, full_name string)
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "full_name": {"type": "string"}
        },
        "required": ["id", "full_name"]
    }
    js = tmp_path / "schema.json"
    js.write_text(json.dumps(schema), encoding="utf-8")

    # SQL (compatible with JSON schema)
    sql = """CREATE TABLE p (
      id BIGINT NOT NULL,
      full_name TEXT NOT NULL
    );"""
    sqlp = tmp_path / "schema.sql"
    sqlp.write_text(sql, encoding="utf-8")

    exe = [sys.executable, "-m", "schema_diff.cli"]
    res = _run(exe + [
        str(js), str(sqlp),
        "--left", "jsonschema",
        "--right", "sql",
        "--right-table", "p",
        "--no-color",
    ])
    assert res.returncode == 0, res.stderr
    assert "Schema diff" in res.stdout
    # Should be compatible (ints/strings), so expect no mismatches:
    assert "No differences" in res.stdout or "True schema mismatches -- (0)" in res.stdout


def test_any_any_auto_detect_jsonschema_vs_sql(tmp_path):
    # Same as above but use auto-detect kinds (by extension)
    schema = {
        "type": "object",
        "properties": {"id": {"type": "integer"}},
        "required": ["id"]
    }
    js = tmp_path / "schema.json"
    js.write_text(json.dumps(schema), encoding="utf-8")

    sql = """CREATE TABLE users (
      id BIGINT NOT NULL
    );"""
    sqlp = tmp_path / "schema.sql"
    sqlp.write_text(sql, encoding="utf-8")

    exe = [sys.executable, "-m", "schema_diff.cli"]
    res = _run(exe + [
        str(js), str(sqlp),
        "--right-table", "users",   # table still needed
        "--no-color",
    ])
    assert res.returncode == 0, res.stderr
    assert "Schema diff" in res.stdout
    assert "No differences" in res.stdout or "True schema mismatches -- (0)" in res.stdout


def test_any_any_sql_multi_table_select(tmp_path):
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
    schema = {"type": "object", "properties": {
        "id": {"type": "integer"}}, "required": ["id"]}
    js = tmp_path / "schema.json"
    js.write_text(json.dumps(schema), encoding="utf-8")

    exe = [sys.executable, "-m", "schema_diff.cli"]

    # Compare against table a -> should match (no diffs)
    res_ok = _run(exe + [
        str(js), str(sqlp),
        "--left", "jsonschema",
        "--right", "sql",
        "--right-table", "a",
        "--no-color",
    ])
    assert res_ok.returncode == 0, res_ok.stderr
    assert "Schema diff" in res_ok.stdout
    assert "No differences" in res_ok.stdout or "True schema mismatches -- (0)" in res_ok.stdout

    # Compare against table b -> should mismatch (int vs str)
    res_bad = _run(exe + [
        str(js), str(sqlp),
        "--left", "jsonschema",
        "--right", "sql",
        "--right-table", "b",
        "--no-color",
    ])
    assert res_bad.returncode == 0, res_bad.stderr
    assert "Schema diff" in res_bad.stdout
    assert "True schema mismatches" in res_bad.stdout
    # A helpful sanity check that the path 'id' is mentioned somewhere
    assert ".id" in res_bad.stdout or " id" in res_bad.stdout


def test_any_any_mismatch_data_vs_jsonschema(tmp_path):
    # Data has id as string, schema expects integer
    data = _w(tmp_path, "bad.ndjson", '{"id":"1"}\n', gz=False)
    schema = {"type": "object", "properties": {
        "id": {"type": "integer"}}, "required": ["id"]}
    js = tmp_path / "schema.json"
    js.write_text(json.dumps(schema), encoding="utf-8")

    exe = [sys.executable, "-m", "schema_diff.cli"]
    res = _run(exe + [
        data, str(js),
        "--left", "data",
        "--right", "jsonschema",
        "--first-record",
        "--no-color",
    ])
    assert res.returncode == 0, res.stderr
    assert "True schema mismatches" in res.stdout
    # Presence section optional depending on your --no-presence default, so we just check mismatch exists.


def test_any_any_data_vs_sql(tmp_path):
    # Data: id int, full_name str
    data = _w(tmp_path, "p.ndjson.gz", '{"id":1,"full_name":"A"}\n', gz=True)
    # SQL: matching columns
    sql = """CREATE TABLE p (
      id BIGINT NOT NULL,
      full_name TEXT NOT NULL
    );"""
    sqlp = tmp_path / "schema.sql"
    sqlp.write_text(sql, encoding="utf-8")

    exe = [sys.executable, "-m", "schema_diff.cli"]
    res = _run(exe + [
        data, str(sqlp),
        "--left", "data",
        "--right", "sql",
        "--right-table", "p",
        "--first-record",
        "--no-color",
    ])
    assert res.returncode == 0, res.stderr
    assert "Schema diff" in res.stdout
    assert "No differences" in res.stdout or "True schema mismatches -- (0)" in res.stdout
