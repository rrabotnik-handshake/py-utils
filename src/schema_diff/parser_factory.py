#!/usr/bin/env python3
"""
Parser Factory for schema-diff.

Provides an explicit factory pattern for creating schema parsers,
improving extensibility, type safety, and error handling.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Protocol, Set, Tuple, Type

from .constants import DEFAULT_SAMPLE_SIZE
from .dbt_schema_parser import (
    schema_from_dbt_manifest,
    schema_from_dbt_model,
    schema_from_dbt_schema_yml,
)
from .io_utils import nth_record, sample_records, sniff_ndjson
from .json_data_file_parser import merged_schema_from_samples
from .json_schema_parser import schema_from_json_schema_file
from .logging_config import get_logger
from .protobuf_schema_parser import schema_from_protobuf_file
from .spark_schema_parser import schema_from_spark_schema_file
from .sql_schema_parser import schema_from_sql_schema_file
from .utils import coerce_root_to_field_dict

logger = get_logger(__name__)


class ParseResult:
    """Standardized result from any parser."""

    def __init__(
        self,
        schema_tree: Any,
        required_paths: Set[str],
        label: str,
        source_type: Optional[str] = None,
    ):
        self.schema_tree = schema_tree
        self.required_paths = required_paths
        self.label = label
        self.source_type = source_type


class Parser(Protocol):
    """Protocol defining the interface for all parsers."""

    def parse(self, path: str, **kwargs) -> ParseResult:
        """Parse a file and return standardized result."""
        ...

    def can_handle(self, path: str) -> bool:
        """Check if this parser can handle the given file."""
        ...


class DataParser:
    """Parser for data files (JSON/NDJSON)."""

    def parse(
        self,
        path: str,
        cfg=None,
        samples: int = DEFAULT_SAMPLE_SIZE,
        all_records: bool = False,
        first_record: bool = False,
        record_n: Optional[int] = None,
        **kwargs,
    ) -> ParseResult:
        """Parse data file and infer schema."""
        from .config import Config

        if cfg is None:
            cfg = Config()

        # Determine sampling strategy
        if first_record:
            records = nth_record(
                path, 1
            )  # nth_record uses 1-based indexing and returns a list
        elif record_n is not None:
            records = nth_record(path, record_n)  # nth_record already returns a list
        elif all_records:
            from .io_utils import all_records as all_records_fn

            records = all_records_fn(path)  # All records
        else:
            records = sample_records(path, samples)

        # Infer schema from samples
        schema_tree = merged_schema_from_samples(records, cfg)
        schema_tree = coerce_root_to_field_dict(schema_tree)

        # Data files don't have explicit required fields
        required_paths: Set[str] = set()

        # Generate label
        record_count = len(records) if records else 0
        if all_records:
            label = f"{path} (all {record_count} records)"
        elif first_record:
            label = f"{path} (first record)"
        elif record_n is not None:
            label = f"{path} (record {record_n})"
        else:
            label = f"{path} ({record_count} samples)"

        return ParseResult(schema_tree, required_paths, label, "data")

    def can_handle(self, path: str) -> bool:
        """Check if file appears to be data (JSON/NDJSON)."""
        try:
            return sniff_ndjson(path) or path.lower().endswith(
                (".json", ".jsonl", ".ndjson")
            )
        except Exception:
            return False


class JsonSchemaParser:
    """Parser for JSON Schema files."""

    def parse(self, path: str, **kwargs) -> ParseResult:
        """Parse JSON Schema file."""
        schema_tree, required_paths = schema_from_json_schema_file(path)
        schema_tree = coerce_root_to_field_dict(schema_tree)
        label = f"{path} (JSON Schema)"
        return ParseResult(schema_tree, required_paths, label, "jsonschema")

    def can_handle(self, path: str) -> bool:
        """Check if file appears to be JSON Schema."""
        try:
            import json

            with open(path, "r") as f:
                data = json.load(f)
            return isinstance(data, dict) and "$schema" in data
        except Exception:
            return False


class SparkSchemaParser:
    """Parser for Spark schema files."""

    def parse(self, path: str, **kwargs) -> ParseResult:
        """Parse Spark schema file."""
        schema_tree, required_paths = schema_from_spark_schema_file(path)
        schema_tree = coerce_root_to_field_dict(schema_tree)
        label = f"{path} (Spark schema)"
        return ParseResult(schema_tree, required_paths, label, "spark")

    def can_handle(self, path: str) -> bool:
        """Check if file appears to be Spark schema."""
        try:
            with open(path, "r") as f:
                content = f.read().strip()
            # Look for Spark schema patterns
            return "root" in content.lower() and (
                "|--" in content or "nullable = " in content
            )
        except Exception:
            return False


class SqlSchemaParser:
    """Parser for SQL DDL files."""

    def parse(self, path: str, table: Optional[str] = None, **kwargs) -> ParseResult:
        """Parse SQL DDL file."""
        schema_tree, required_paths = schema_from_sql_schema_file(path, table)
        schema_tree = coerce_root_to_field_dict(schema_tree)

        label = f"{path} (SQL DDL)"
        if table:
            label += f" table={table}"

        return ParseResult(schema_tree, required_paths, label, "sql")

    def can_handle(self, path: str) -> bool:
        """Check if file appears to be SQL DDL."""
        try:
            with open(path, "r") as f:
                content = f.read().lower()
            return (
                "create table" in content
                or "create or replace table" in content
                or path.lower().endswith(".sql")
            )
        except Exception:
            return False


class DbtManifestParser:
    """Parser for dbt manifest.json files."""

    def parse(self, path: str, model: Optional[str] = None, **kwargs) -> ParseResult:
        """Parse dbt manifest file."""
        schema_tree, required_paths = schema_from_dbt_manifest(path, model)
        schema_tree = coerce_root_to_field_dict(schema_tree)

        label = f"{path} (dbt manifest)"
        if model:
            label += f" model={model}"

        return ParseResult(schema_tree, required_paths, label, "dbt-manifest")

    def can_handle(self, path: str) -> bool:
        """Check if file appears to be dbt manifest."""
        return path.lower().endswith("manifest.json")


class DbtYmlParser:
    """Parser for dbt schema.yml files."""

    def parse(self, path: str, model: Optional[str] = None, **kwargs) -> ParseResult:
        """Parse dbt schema.yml file."""
        schema_tree, required_paths = schema_from_dbt_schema_yml(path, model)
        schema_tree = coerce_root_to_field_dict(schema_tree)

        label = f"{path} (dbt schema.yml)"
        if model:
            label += f" model={model}"

        return ParseResult(schema_tree, required_paths, label, "dbt-yml")

    def can_handle(self, path: str) -> bool:
        """Check if file appears to be dbt schema.yml."""
        return (
            path.lower().endswith(".yml") or path.lower().endswith(".yaml")
        ) and "schema" in path.lower()


class DbtModelParser:
    """Parser for dbt model.sql files."""

    def parse(self, path: str, **kwargs) -> ParseResult:
        """Parse dbt model file."""
        schema_tree, required_paths = schema_from_dbt_model(path)
        schema_tree = coerce_root_to_field_dict(schema_tree)
        label = f"{path} (dbt model)"
        return ParseResult(schema_tree, required_paths, label, "dbt-model")

    def can_handle(self, path: str) -> bool:
        """Check if file appears to be dbt model."""
        if not path.lower().endswith(".sql"):
            return False
        try:
            with open(path, "r") as f:
                content = f.read()
            # Look for dbt-specific patterns
            return ("{{" in content and "}}" in content) or "ref(" in content
        except Exception:
            return False


class ProtobufParser:
    """Parser for Protobuf .proto files."""

    def parse(self, path: str, message: Optional[str] = None, **kwargs) -> ParseResult:
        """Parse Protobuf file."""
        schema_tree, required_paths, selected_message = schema_from_protobuf_file(
            path, message
        )
        schema_tree = coerce_root_to_field_dict(schema_tree)

        label = f"{path} (protobuf)"
        if selected_message:
            label += f" message={selected_message}"

        return ParseResult(schema_tree, required_paths, label, "protobuf")

    def can_handle(self, path: str) -> bool:
        """Check if file appears to be Protobuf."""
        return path.lower().endswith(".proto")


class ParserFactory:
    """Factory for creating appropriate parsers based on file type and kind."""

    # Registry of available parsers
    _parsers: Dict[str, Type[Parser]] = {
        "data": DataParser,
        "jsonschema": JsonSchemaParser,
        "spark": SparkSchemaParser,
        "sql": SqlSchemaParser,
        "dbt-manifest": DbtManifestParser,
        "dbt-yml": DbtYmlParser,
        "dbt-model": DbtModelParser,
        "protobuf": ProtobufParser,
    }

    # Auto-detection order (most specific first)
    _auto_detection_order = [
        "dbt-manifest",
        "dbt-yml",
        "dbt-model",
        "protobuf",
        "jsonschema",
        "spark",
        "sql",
        "data",  # Most permissive, try last
    ]

    @classmethod
    def create_parser(cls, kind: str) -> Parser:
        """Create a parser for the specified kind."""
        if kind not in cls._parsers:
            available = ", ".join(cls._parsers.keys())
            raise ValueError(f"Unknown parser kind '{kind}'. Available: {available}")

        parser_class = cls._parsers[kind]
        return parser_class()

    @classmethod
    def auto_detect_parser(cls, path: str) -> Tuple[Parser, str]:
        """Auto-detect the appropriate parser for a file."""
        for kind in cls._auto_detection_order:
            parser = cls.create_parser(kind)
            if parser.can_handle(path):
                return parser, kind

        # Fallback to data parser
        return cls.create_parser("data"), "data"

    @classmethod
    def parse_file(cls, path: str, kind: Optional[str] = None, **kwargs) -> ParseResult:
        """Parse a file using the appropriate parser."""
        if kind == "auto" or kind is None:
            parser, detected_kind = cls.auto_detect_parser(path)
            if kind is None:
                logger.info("Auto-detected format for %s: %s", path, detected_kind)
                print(f"🔍 Auto-detected format: {detected_kind}")
        else:
            parser = cls.create_parser(kind)

        return parser.parse(path, **kwargs)

    @classmethod
    def register_parser(cls, kind: str, parser_class: Type[Parser]) -> None:
        """Register a new parser type (for extensibility)."""
        cls._parsers[kind] = parser_class

    @classmethod
    def list_supported_kinds(cls) -> list[str]:
        """List all supported parser kinds."""
        return list(cls._parsers.keys())


# Convenience function for backward compatibility
def create_parser(kind: str) -> Parser:
    """Create a parser instance for the given kind."""
    return ParserFactory.create_parser(kind)
