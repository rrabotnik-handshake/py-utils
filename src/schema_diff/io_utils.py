from __future__ import annotations
import io
import gzip
import json
from typing import Any, List, Iterator
import ijson

def open_text(path: str):
    """
    Return a text stream for either plain UTF-8 or gzip-compressed files,
    regardless of file extension.
    """
    f = open(path, "rb")
    magic = f.read(2)
    f.seek(0)
    if magic == b"\x1f\x8b":
        return io.TextIOWrapper(gzip.GzipFile(fileobj=f), encoding="utf-8-sig", errors="strict")
    else:
        return io.TextIOWrapper(f, encoding="utf-8-sig", errors="strict")


def open_binary(path: str):
    """
    Open a path as a *binary* stream, auto-detecting gzip via magic bytes.
    Use this for ijson, which prefers bytes.
    """
    f = open(path, "rb")
    head = f.read(2)
    f.seek(0)
    if head == b"\x1f\x8b":  # gzip magic
        return gzip.GzipFile(fileobj=f)  # binary file-like
    return f  # binary file-like


def sniff_ndjson(sample: str) -> bool:
    """
    Heuristic: if the first two non-empty lines start with '{', assume NDJSON.
    """
    lines = [ln.strip() for ln in sample.splitlines() if ln.strip()]
    return len(lines) >= 2 and lines[0].startswith("{") and lines[1].startswith("{")


def iter_records(path: str) -> Iterator[Any]:
    """
    Yield records from a JSON file that could be:
      - NDJSON (one JSON object per line)
      - A single JSON object
      - A JSON array of objects (streamed with ijson)
    Includes a fallback for NDJSON where the first record is larger than
    the initial sniff buffer (so json.load() raises JSONDecodeError).
    """
    with open_text(path) as f:
        buf = f.read(8192)
        f.seek(0)

        # Empty file
        if not buf.strip():
            return
        s = buf.lstrip()

        # 1) Typical NDJSON case (short lines)
        if sniff_ndjson(buf):
            for line in f:
                line = line.strip()
                if line:
                    yield json.loads(line)
            return

        # 2) Single object OR NDJSON with a very long first line
        if s.startswith("{"):
            try:
                # Try to parse as a single JSON object
                yield json.load(f)
                return
            except json.JSONDecodeError:
                # Fallback: it's actually NDJSON (first newline beyond sniff window)
                f.seek(0)
                for line in f:
                    line = line.strip()
                    if line:
                        yield json.loads(line)
                return

        # 3) JSON array
        if s.startswith("["):
            # Reopen as binary for ijson to avoid the warning
            with open_binary(path) as fb:
                for item in ijson.items(fb, "item"):
                    yield item
            return

        # 4) Last resort: line-by-line NDJSON-ish
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def sample_records(path: str, k: int) -> List[Any]:
    """
    Reservoir-sample k records from the file without loading everything.
    """
    import random
    reservoir: List[Any] = []
    n = 0
    for rec in iter_records(path):
        n += 1
        if len(reservoir) < k:
            reservoir.append(rec)
        else:
            j = random.randint(1, n)
            if j <= k:
                reservoir[j - 1] = rec
    return reservoir


def nth_record(path: str, n: int) -> List[Any]:
    """
    Return the 1-based Nth record (as a single-item list), or [] if missing.
    """
    if n <= 0:
        return []
    for i, rec in enumerate(iter_records(path), 1):
        if i == n:
            return [rec]
    return []
