"""Lightweight value → type-name inference used when deriving schemas from.DATA.

Returned labels are the *atoms* expected by the rest of the pipeline:
- Scalars: "int" | "float" | "bool" | "str" | "date" | "time" | "timestamp" | "missing"
- Containers: "object" | "array"
- Empty sentinels: "empty_string" | "empty_object" | "empty_array"

Datetime inference is gated by Config.infer_datetimes; when enabled, we detect:
- date:      YYYY-MM-DD
- time:      HH:MM[:SS[.ffffff]]
- timestamp: YYYY-MM-DD[ T]HH:MM[:SS[.ffffff]][Z|±HH:MM]
"""

from __future__ import annotations

import re
from typing import Any

from .config import Config

# Precompiled ISO-ish patterns (simple and fast)
ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
ISO_TIME_RE = re.compile(r"^\d{2}:\d{2}(?::\d{2}(?:\.\d{1,9})?)?$")
ISO_TS_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}"
    r"(?::\d{2}(?:\.\d{1,9})?)?"
    r"(?:[Zz]|[+-]\d{2}:\d{2})?$"
)


def tname(v: Any, cfg: Config) -> str:
    """Map a Python value to the internal atomic type label.

    Notes
    -----
    - `bool` must be checked before `int` (since `bool` is a subclass of `int`).
    - Empty containers/strings return special sentinel labels so the
      normalizer can distinguish optionality vs emptiness.
    - When `cfg.infer_datetimes` is True, ISO-like date/time/timestamp strings
      are recognized; otherwise they remain "str".
    """
    # Order matters: bool before int
    if isinstance(v, bool):
        return "bool"

    if v is None:
        return "missing"

    if isinstance(v, int):
        return "int"

    if isinstance(v, float):
        return "float"

    if isinstance(v, str):
        # Preserve empty-string signal
        if v == "":
            return "empty_string"

        if cfg.infer_datetimes:
            s = v.strip()  # be tolerant of incidental whitespace
            if ISO_TS_RE.match(s):
                return "timestamp"
            if ISO_DATE_RE.match(s):
                return "date"
            if ISO_TIME_RE.match(s):
                return "time"
        return "str"

    if isinstance(v, dict):
        # Keep an explicit empty-object sentinel
        return "object" if v else "empty_object"

    if isinstance(v, list):
        # Keep an explicit empty-array sentinel
        return "array" if v else "empty_array"

    # Fallback for uncommon types (bytes, decimal, numpy scalars, etc.)
    return type(v).__name__
