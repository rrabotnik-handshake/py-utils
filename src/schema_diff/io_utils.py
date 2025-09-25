from __future__ import annotations

import gzip
import io
import json
import subprocess
from collections.abc import Iterator, Sequence
from typing import Any

import ijson

# Constants
MAX_RECORD_SAFETY_LIMIT = 1_000_000  # Safety limit for --all-records


__all__ = [
    "CommandError",
    "_run",
    "open_text",
    "open_binary",
    "sniff_ndjson",
    "iter_records",
    "sample_records",
    "nth_record",
    "all_records",
]


class CommandError(Exception):
    """Raised when `_run` fails with a non-zero exit code."""

    def __init__(self, cmd: Sequence[str], returncode: int, stdout: str, stderr: str):
        self.cmd = list(cmd)
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        super().__init__(f"Command {cmd} failed with {returncode}: {stderr.strip()}")


def _run(args, cwd=None, env=None, check_untrusted=True):
    """
    Run a subprocess safely (no shell). Returns CompletedProcess or raises CommandError.

    Parameters
    ----------
    args : list[str]
        Command and arguments; must be non-empty.
    cwd : str | None
        Working directory for the child process.
    env : mapping | None
        Environment variables for the child process.
    check_untrusted : bool
        If True, reject args containing control/shell metacharacters.

    Raises
    ------
    ValueError
        If args is empty or contains non-strings (or suspicious chars when enabled).
    CommandError
        If the command exits with non-zero status.
    """
    if not isinstance(args, (list, tuple)) or not args:
        raise ValueError("args must be a non-empty list of strings")

    for a in args:
        if not isinstance(a, str):
            raise ValueError(f"Non-string arg: {a!r}")
        if check_untrusted and any(c in a for c in [";", "|", "&", "\n", "\r"]):
            raise ValueError(f"Suspicious characters in arg: {a!r}")

    res = subprocess.run(args, cwd=cwd, capture_output=True, text=True, env=env)

    if res.returncode != 0:
        raise CommandError(args, res.returncode, res.stdout, res.stderr)

    return res


def open_text(path: str) -> io.TextIOWrapper:
    """
    Open a path as text, auto-detecting gzip via magic bytes.

    - Uses UTF-8 with BOM support (`utf-8-sig`)
    - Raises UnicodeDecodeError on invalid sequences (`errors='strict'`)
    """
    f = open(path, "rb")
    magic = f.read(2)
    f.seek(0)
    if magic == b"\x1f\x8b":
        # Type ignore: gzip.GzipFile is compatible with IO[bytes] but MyPy doesn't recognize it
        return io.TextIOWrapper(
            gzip.GzipFile(fileobj=f),  # type: ignore
            encoding="utf-8-sig",
            errors="strict",
        )
    return io.TextIOWrapper(f, encoding="utf-8-sig", errors="strict")


def open_binary(path: str):
    """
    Open a path as *binary*, auto-detecting gzip via magic bytes.
    Useful for `ijson`, which prefers bytes streams.
    """
    f = open(path, "rb")
    head = f.read(2)
    f.seek(0)
    if head == b"\x1f\x8b":  # gzip magic
        return gzip.GzipFile(fileobj=f)  # binary file-like
    return f  # binary file-like


def sniff_ndjson(sample: str) -> bool:
    """
    Heuristic: if the first two non-empty lines both start with '{', treat as NDJSON.
    """
    lines = [ln.strip() for ln in sample.splitlines() if ln.strip()]
    return len(lines) >= 2 and lines[0].startswith("{") and lines[1].startswith("{")


def iter_records(path: str) -> Iterator[Any]:
    """
    Yield records from a JSON-ish file that could be:
      1) NDJSON (one JSON object per line)
      2) A single JSON object
      3) A JSON array of objects (streamed with ijson)

    Includes a fallback for NDJSON where the first record is longer than the sniff buffer.
    """
    with open_text(path) as f:
        buf = f.read(8192)
        f.seek(0)

        # Empty file
        if not buf.strip():
            return
        s = buf.lstrip()

        # 1) Typical NDJSON (short lines)
        if sniff_ndjson(buf):
            for line in f:
                line = line.strip()
                if line:
                    yield json.loads(line)
            return

        # 2) Single object OR NDJSON with a very long first line
        if s.startswith("{"):
            try:
                yield json.load(f)  # single JSON object
                return
            except json.JSONDecodeError:
                # Fallback: actually NDJSON (first newline beyond sniff window)
                f.seek(0)
                for line in f:
                    line = line.strip()
                    if line:
                        yield json.loads(line)
                return

        # 3) Top-level JSON array
        if s.startswith("["):
            # Reopen as binary for ijson
            with open_binary(path) as fb:
                yield from ijson.items(fb, "item")
            return

        # 4) Last resort: line-by-line JSON-ish
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def sample_records(path: str, k: int) -> list[Any]:
    """
    Reservoir-sample `k` records from the file without loading everything.
    """
    import random

    reservoir: list[Any] = []
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


def nth_record(path: str, n: int) -> list[Any]:
    """
    Return the 1-based Nth record (as a single-item list), or [] if missing.
    """
    if n <= 0:
        return []
    for i, rec in enumerate(iter_records(path), 1):
        if i == n:
            return [rec]
    return []


def all_records(path: str, max_records: int | None = None) -> list[Any]:
    """
    Read ALL records from a file. Use with caution for large files.

    Parameters
    ----------
    path : str
        Path to the data file
    max_records : int, optional
        Maximum number of records to read (safety limit). If None, reads all.

    Returns
    -------
    List[Any]
        List of all records
    """
    records = []
    for i, rec in enumerate(iter_records(path)):
        if max_records is not None and i >= max_records:
            print(f"Warning: Stopped at {max_records} records (safety limit)")
            break
        records.append(rec)
    return records
