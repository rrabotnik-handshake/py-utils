import gzip
import json
from schema_diff.io_utils import iter_records, sample_records, nth_record


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