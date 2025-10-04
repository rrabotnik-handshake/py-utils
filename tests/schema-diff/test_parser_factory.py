#!/usr/bin/env python3
"""
Tests for the ParserFactory design pattern.
"""
import pytest
import tempfile
import json
from pathlib import Path

from src.schema_diff.parser_factory import (
    ParserFactory,
    ParseResult,
    DataParser,
    JsonSchemaParser,
    SparkSchemaParser,
    SqlSchemaParser,
)


class TestParserFactory:
    """Test the ParserFactory pattern."""

    def test_create_parser_valid_kinds(self):
        """Test creating parsers for all valid kinds."""
        valid_kinds = [
            'data',
            'jsonschema', 
            'spark',
            'sql',
            'dbt-manifest',
            'dbt-yml',
            'dbt-model',
            'protobuf'
        ]
        
        for kind in valid_kinds:
            parser = ParserFactory.create_parser(kind)
            assert parser is not None
            assert hasattr(parser, 'parse')
            assert hasattr(parser, 'can_handle')

    def test_create_parser_invalid_kind(self):
        """Test creating parser with invalid kind raises ValueError."""
        with pytest.raises(ValueError, match="Unknown parser kind 'invalid'"):
            ParserFactory.create_parser('invalid')

    def test_list_supported_kinds(self):
        """Test listing supported parser kinds."""
        kinds = ParserFactory.list_supported_kinds()
        expected_kinds = [
            'data', 'jsonschema', 'spark', 'sql', 
            'dbt-manifest', 'dbt-yml', 'dbt-model', 'protobuf'
        ]
        
        for kind in expected_kinds:
            assert kind in kinds

    def test_register_parser(self):
        """Test registering a custom parser."""
        class CustomParser:
            def parse(self, path: str, **kwargs):
                return ParseResult({}, set(), "custom")
            
            def can_handle(self, path: str) -> bool:
                return path.endswith('.custom')
        
        # Register custom parser
        ParserFactory.register_parser('custom', CustomParser)
        
        # Verify it's registered
        assert 'custom' in ParserFactory.list_supported_kinds()
        
        # Verify we can create it
        parser = ParserFactory.create_parser('custom')
        assert isinstance(parser, CustomParser)

    def test_auto_detect_integration(self):
        """Test that factory auto-detection integrates with underlying detection logic."""
        # This test focuses on the factory pattern integration rather than 
        # duplicating the detailed auto-detection logic tested in test_auto_detection.py
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            # Create a clear JSON Schema file
            json.dump({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {"name": {"type": "string"}}
            }, f)
            f.flush()
            
            # Test that factory correctly delegates to auto-detection and returns right parser
            parser, kind = ParserFactory.auto_detect_parser(f.name)
            assert kind == 'jsonschema'
            assert isinstance(parser, JsonSchemaParser)
            
            Path(f.name).unlink()

    def test_parse_file_with_explicit_kind(self):
        """Test parsing file with explicit kind specification."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({"id": 1, "name": "test"}, f)
            f.flush()
            
            from src.schema_diff.config import Config
            cfg = Config()
            
            result = ParserFactory.parse_file(f.name, kind='data', cfg=cfg, samples=1)
            
            assert isinstance(result, ParseResult)
            assert result.schema_tree is not None
            assert isinstance(result.required_paths, set)
            assert result.label is not None
            assert result.source_type == 'data'
            
            Path(f.name).unlink()

    def test_parse_file_with_auto_detection(self):
        """Test parsing file with auto-detection."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {"name": {"type": "string"}}
            }, f)
            f.flush()
            
            result = ParserFactory.parse_file(f.name, kind='auto')
            
            assert isinstance(result, ParseResult)
            assert result.source_type == 'jsonschema'
            
            Path(f.name).unlink()


class TestParseResult:
    """Test the ParseResult class."""

    def test_parse_result_creation(self):
        """Test creating ParseResult instances."""
        schema_tree = {"name": "str", "age": "int"}
        required_paths = {"name"}
        label = "test.json"
        source_type = "data"
        
        result = ParseResult(schema_tree, required_paths, label, source_type)
        
        assert result.schema_tree == schema_tree
        assert result.required_paths == required_paths
        assert result.label == label
        assert result.source_type == source_type

    def test_parse_result_optional_source_type(self):
        """Test ParseResult with optional source_type."""
        result = ParseResult({"name": "str"}, set(), "test.json")
        
        assert result.source_type is None


class TestIndividualParsers:
    """Test individual parser implementations."""

    def test_data_parser_can_handle(self):
        """Test DataParser can_handle method."""
        parser = DataParser()
        
        assert parser.can_handle("data.json")
        assert parser.can_handle("data.jsonl")
        assert parser.can_handle("data.ndjson")
        assert not parser.can_handle("schema.proto")

    def test_json_schema_parser_can_handle(self):
        """Test JsonSchemaParser can_handle method."""
        parser = JsonSchemaParser()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({"$schema": "http://json-schema.org/draft-07/schema#"}, f)
            f.flush()
            
            assert parser.can_handle(f.name)
            Path(f.name).unlink()

    def test_spark_parser_can_handle(self):
        """Test SparkSchemaParser can_handle method."""
        parser = SparkSchemaParser()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("root\n |-- field: string (nullable = true)")
            f.flush()
            
            assert parser.can_handle(f.name)
            Path(f.name).unlink()

    def test_sql_parser_can_handle(self):
        """Test SqlSchemaParser can_handle method."""
        parser = SqlSchemaParser()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write("CREATE TABLE test (id INT);")
            f.flush()
            
            assert parser.can_handle(f.name)
            Path(f.name).unlink()
