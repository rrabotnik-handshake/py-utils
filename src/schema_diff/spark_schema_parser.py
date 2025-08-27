import re
from typing import Any
from .io_utils import open_text

# -------- Spark schema parsing --------
SPARK_LINE_RE = re.compile(
    r'^\s*\|\s*--\s*([A-Za-z0-9_]+)\s*:\s*([^(\s]+).*?\(nullable\s*=\s*(true|false)\)\s*$', re.IGNORECASE
)

# Spark/Databricks dtype -> our internal labels
TYPE_MAP_SPARK = {
    # integral
    "byte": "int", "short": "int", "integer": "int", "int": "int", "long": "int",
    # floating
    "float": "float", "double": "float", "real": "float",
    "decimal": "float", "numeric": "float",
    # boolean
    "boolean": "bool", "bool": "bool",
    # strings/binary
    "string": "str", "varchar": "str", "char": "str", "binary": "str",
    # temporal (distinct timestamp/date)
    "date": "date",
    "timestamp": "timestamp", "timestamp_ntz": "timestamp",
    "timestamp_ltz": "timestamp", "timestamp_tz": "timestamp",
}


def _spark_type_to_internal(dtype: str) -> Any:
    dt = dtype.strip().lower()
    # array<...>
    if dt.startswith("array<") and dt.endswith(">"):
        inner = dt[6:-1].strip()
        elem = _spark_type_to_internal(inner)
        return [elem] if isinstance(elem, (str, dict, list)) else [str(elem)]
    # map/struct -> treat as object
    if dt.startswith("map<") or dt.startswith("struct<"):
        return "object"
    # primitive
    return TYPE_MAP_SPARK.get(dt, "any")


def schema_from_spark_schema_file(path: str) -> tuple[dict[str, Any], set[str]]:
    props = {}
    required: set[str] = set()
    with open_text(path) as f:
        for raw in f:
            m = SPARK_LINE_RE.match(raw.rstrip("\n"))
            if not m:
                continue
            name, dtype, nullable = m.group(1), m.group(
                2), (m.group(3).lower() == "true")
            # "int"/"str"/timestamp or ["str"]
            t = _spark_type_to_internal(dtype)
            props[name] = t
            if not nullable:
                required.add(name)                          # presence only
    return (props if props else "object"), required
