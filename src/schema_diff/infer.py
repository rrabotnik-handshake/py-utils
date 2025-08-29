import re
from typing import Any
from .config import Config

ISO_DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')
ISO_TIME_RE = re.compile(r'^\d{2}:\d{2}(:\d{2}(\.\d{1,9})?)?$')
ISO_TS_RE = re.compile(
    r'^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2}(\.\d{1,9})?)?([Zz]|[+-]\d{2}:\d{2})?$')


def tname(v: Any, cfg: Config) -> str:
    if isinstance(v, bool):
        return "bool"
    if v is None:
        return "missing"
    if isinstance(v, int):
        return "int"
    if isinstance(v, float):
        return "float"
    if isinstance(v, str):
        if v == "":
            return "empty_string"
        if cfg.infer_datetimes:
            if ISO_TS_RE.match(v):
                return "timestamp"
            if ISO_DATE_RE.match(v):
                return "date"
            if ISO_TIME_RE.match(v):
                return "time"
        return "str"
    if isinstance(v, dict):
        return "object" if v else "empty_object"
    if isinstance(v, list):
        return "array" if v else "empty_array"
    return type(v).__name__
