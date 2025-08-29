import gzip
import json
import sys
import pytest
from schema_diff.io_utils import CommandError, iter_records, sample_records, nth_record, _run


def _write(tmp_path, name, text, gz=False):
    p = tmp_path / name
    if gz:
        with gzip.open(p, "wt", encoding="utf-8") as f:
            f.write(text)
    else:
        p.write_text(text, encoding="utf-8")
    return p


def test_iter_records_ndjson(tmp_path):
    p = _write(tmp_path, "a.ndjson", '{"a":1}\n{"a":2}\n')
    assert list(iter_records(str(p))) == [{"a": 1}, {"a": 2}]


def test_iter_records_json_array(tmp_path):
    p = _write(tmp_path, "a.json", '[{"a":1},{"a":2}]')
    assert list(iter_records(str(p))) == [{"a": 1}, {"a": 2}]


def test_iter_records_single_object(tmp_path):
    p = _write(tmp_path, "a.json", '{"a":1}')
    assert list(iter_records(str(p))) == [{"a": 1}]


def test_iter_records_gz_ndjson(tmp_path):
    p = _write(tmp_path, "a.ndjson.gz", '{"a":1}\n{"a":2}\n', gz=True)
    assert list(iter_records(str(p))) == [{"a": 1}, {"a": 2}]


def test_nth_record(tmp_path):
    p = _write(tmp_path, "a.ndjson", '{"a":1}\n{"a":2}\n{"a":3}\n')
    assert nth_record(str(p), 2) == [{"a": 2}]


def test_sample_records_seeded(tmp_path, monkeypatch):
    p = _write(tmp_path, "a.ndjson", "\n".join(
        json.dumps({"i": i}) for i in range(1, 11))+"\n")
    import random
    random.seed(123)
    s1 = sample_records(str(p), 3)
    random.seed(123)
    s2 = sample_records(str(p), 3)
    assert s1 == s2 and len(s1) == 3


def test_unreadable_file(tmp_path):
    p = tmp_path / "bad.json"
    p.write_text("{", encoding="utf-8")
    exe = [sys.executable, "-m", "schema_diff.cli"]
    with pytest.raises(CommandError):
        _run(exe + [str(p), str(p), "--left", "data", "--right", "data", "--first-record", "--no-color"])

def test_sql_table_missing(tmp_path):
    p = tmp_path / "ddl.sql"
    p.write_text("CREATE TABLE x (id INT);", encoding="utf-8")
    exe = [sys.executable, "-m", "schema_diff.cli"]
    with pytest.raises(CommandError):
        _run(exe + [str(p), str(p), "--left", "sql",
              "--right", "sql", "--right-table", "y", "--no-color"])

def test_exports(tmp_path):
    left = tmp_path / "l.json"
    right = tmp_path / "r.json"
    left.write_text(json.dumps({"id": 1}), encoding="utf-8")
    right.write_text(json.dumps({"id": "x"}), encoding="utf-8")
    dump = tmp_path / "dump.json"
    jout = tmp_path / "diff.json"
    exe = [sys.executable, "-m", "schema_diff.cli"]
    res = _run(exe + [str(left), str(right), "--left", "data", "--right", "data", "--first-record",
                                "--dump-schemas", str(dump), "--json-out", str(jout), "--no-color"])
    assert res.returncode == 0
    dj = json.loads(dump.read_text())
    assert "left" in dj and "right" in dj
    jj = json.loads(jout.read_text())
    assert "meta" in jj


def test_bom_gz_ndjson(tmp_path):
    p = tmp_path / "data.json.gz"
    content = "\ufeff" + '{"a":1}' + "\n" + '{"a":2}' + "\n"
    with gzip.open(p, "wb") as g:
        g.write(content.encode("utf-8"))
    # Should not crash
    from schema_diff.io_utils import sample_records
    recs = sample_records(str(p), 2)
    assert len(recs) == 2 and recs[0]["a"] == 1
