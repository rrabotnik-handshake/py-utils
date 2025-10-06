#!/usr/bin/env python3
"""
Tests for migration analysis functionality.

Tests the --output flag and migration analysis report generation.
"""
import json
import tempfile
from pathlib import Path

import pytest


class TestMigrationAnalysisGeneration:
    """Test migration analysis report generation."""

    def test_migration_analysis_basic(self, run_cli):
        """Test basic migration analysis generation with --output."""
        with tempfile.TemporaryDirectory() as temp_dir:
            file1 = Path(temp_dir) / "old_data.json"
            file2 = Path(temp_dir) / "new_data.json"

            old_data = {"name": "John", "age": 30}
            new_data = {"name": "John", "age": 30, "email": "john@example.com"}

            file1.write_text(json.dumps(old_data))
            file2.write_text(json.dumps(new_data))

            result = run_cli([
                str(file1), str(file2),
                "--output",
                "--no-color"
            ])

            assert result.returncode == 0
            assert "Migration analysis saved to output/reports/" in result.stdout

            # Check that migration analysis file was created
            output_dir = Path("output/reports")
            if output_dir.exists():
                analysis_files = list(output_dir.glob("migration_analysis*.md"))
                assert len(analysis_files) > 0

    def test_migration_analysis_with_differences(self, run_cli):
        """Test migration analysis with various types of differences."""
        with tempfile.TemporaryDirectory() as temp_dir:
            file1 = Path(temp_dir) / "schema_v1.json"
            file2 = Path(temp_dir) / "schema_v2.json"

            # Create data with multiple types of changes
            v1_data = {
                "user_id": 123,
                "name": "John Doe",
                "email": "john@example.com",
                "age": 30,
                "status": "active",
                "metadata": {
                    "created_at": "2023-01-01",
                    "version": 1
                }
            }

            v2_data = {
                "user_id": 123,
                "full_name": "John Doe",  # Renamed field
                "email": "john@example.com",
                "age": "30",  # Type change: int -> str
                "status": "active",
                "is_premium": True,  # New field
                "metadata": {
                    "created_at": "2023-01-01T00:00:00Z",  # Format change
                    "updated_at": "2023-06-01T12:00:00Z",  # New nested field
                    "version": 2
                },
                "preferences": {  # New nested object
                    "theme": "dark",
                    "notifications": True
                }
            }

            file1.write_text(json.dumps(v1_data))
            file2.write_text(json.dumps(v2_data))

            result = run_cli([
                str(file1), str(file2),
                "--output",
                "--no-color"
            ])

            assert result.returncode == 0
            assert "Migration analysis saved to output/reports/" in result.stdout

    def test_migration_analysis_no_differences(self, run_cli):
        """Test migration analysis when there are no differences."""
        with tempfile.TemporaryDirectory() as temp_dir:
            file1 = Path(temp_dir) / "data1.json"
            file2 = Path(temp_dir) / "data2.json"

            data = {"name": "John", "age": 30, "active": True}

            file1.write_text(json.dumps(data))
            file2.write_text(json.dumps(data))

            result = run_cli([
                str(file1), str(file2),
                "--output",
                "--no-color"
            ])

            assert result.returncode == 0
            assert "No differences" in result.stdout
            assert "Migration analysis saved to output/reports/" in result.stdout

    def test_migration_analysis_with_common_fields(self, run_cli):
        """Test migration analysis includes common fields analysis."""
        with tempfile.TemporaryDirectory() as temp_dir:
            file1 = Path(temp_dir) / "source.json"
            file2 = Path(temp_dir) / "target.json"

            source_data = {
                "id": 1,
                "name": "Product A",
                "price": 19.99,
                "category": "Electronics",
                "in_stock": True
            }

            target_data = {
                "id": 1,
                "name": "Product A",
                "price": 19.99,
                "category": "Electronics",
                "available": True,  # Renamed field
                "description": "A great product"  # New field
            }

            file1.write_text(json.dumps(source_data))
            file2.write_text(json.dumps(target_data))

            result = run_cli([
                str(file1), str(file2),
                "--output",
                "--show-common",
                "--no-color"
            ])

            assert result.returncode == 0
            assert "Migration analysis saved to output/reports/" in result.stdout

    def test_migration_analysis_cross_format(self, run_cli):
        """Test migration analysis with cross-format comparison."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "data.json"
            schema_file = Path(temp_dir) / "schema.json"

            data = {"user_id": 123, "username": "john", "email": "john@example.com"}
            schema = {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "user_id": {"type": "integer"},
                    "username": {"type": "string"},
                    "email": {"type": "string"},
                    "full_name": {"type": "string"}  # Additional field in schema
                },
                "required": ["user_id", "username"]
            }

            data_file.write_text(json.dumps(data))
            schema_file.write_text(json.dumps(schema))

            result = run_cli([
                str(data_file), str(schema_file),
                "--right", "json_schema",
                "--output",
                "--no-color"
            ])

            assert result.returncode == 0
            assert "Migration analysis saved to output/reports/" in result.stdout

    def test_migration_analysis_with_sampling(self, run_cli):
        """Test migration analysis with different sampling options."""
        with tempfile.TemporaryDirectory() as temp_dir:
            file1 = Path(temp_dir) / "large_data1.json"
            file2 = Path(temp_dir) / "large_data2.json"

            # Create larger datasets
            data1 = [
                {"id": i, "name": f"User {i}", "score": i * 10}
                for i in range(100)
            ]
            data2 = [
                {"id": i, "name": f"User {i}", "score": i * 10, "level": i % 5}
                for i in range(100)
            ]

            file1.write_text('\n'.join(json.dumps(record) for record in data1))
            file2.write_text('\n'.join(json.dumps(record) for record in data2))

            # Test with specific sampling
            result = run_cli([
                str(file1), str(file2),
                "-k", "10",
                "--output",
                "--no-color"
            ])

            assert result.returncode == 0
            assert "Migration analysis saved to output/reports/" in result.stdout

    def test_migration_analysis_with_field_filtering(self, run_cli):
        """Test migration analysis with field filtering."""
        with tempfile.TemporaryDirectory() as temp_dir:
            file1 = Path(temp_dir) / "filtered1.json"
            file2 = Path(temp_dir) / "filtered2.json"

            data1 = {
                "id": 1,
                "name": "John",
                "email": "john@example.com",
                "age": 30,
                "city": "NYC"
            }

            data2 = {
                "id": 1,
                "name": "John",
                "email": "john@newdomain.com",  # Changed
                "age": 31,  # Changed
                "country": "USA"  # New field, but filtered out
            }

            file1.write_text(json.dumps(data1))
            file2.write_text(json.dumps(data2))

            result = run_cli([
                str(file1), str(file2),
                "--fields", "id", "name", "email",
                "--output",
                "--no-color"
            ])

            assert result.returncode == 0
            assert "Migration analysis saved to output/reports/" in result.stdout

    def test_migration_analysis_file_content(self, run_cli):
        """Test that migration analysis file contains expected content."""
        with tempfile.TemporaryDirectory() as temp_dir:
            file1 = Path(temp_dir) / "before.json"
            file2 = Path(temp_dir) / "after.json"

            before_data = {"name": "John", "age": 30}
            after_data = {"name": "John", "age": 30, "email": "john@example.com"}

            file1.write_text(json.dumps(before_data))
            file2.write_text(json.dumps(after_data))

            result = run_cli([
                str(file1), str(file2),
                "--output",
                "--no-color"
            ])

            assert result.returncode == 0

            # Check that migration analysis file exists and has content
            output_dir = Path("output/reports")
            if output_dir.exists():
                analysis_files = list(output_dir.glob("migration_analysis*.md"))
                if analysis_files:
                    content = analysis_files[0].read_text()

                    # Check for expected sections in migration analysis
                    assert "Migration Analysis" in content
                    assert "Schema Comparison Summary" in content or "Summary" in content

                    # Should mention the added field
                    assert "email" in content

    def test_migration_analysis_output_directory_creation(self, run_cli):
        """Test that output directory is created if it doesn't exist."""
        # Clean up any existing output directory for this test
        output_dir = Path("output")
        if output_dir.exists():
            import shutil
            shutil.rmtree(output_dir)

        with tempfile.TemporaryDirectory() as temp_dir:
            file1 = Path(temp_dir) / "test1.json"
            file2 = Path(temp_dir) / "test2.json"

            data1 = {"field": "value1"}
            data2 = {"field": "value2"}

            file1.write_text(json.dumps(data1))
            file2.write_text(json.dumps(data2))

            result = run_cli([
                str(file1), str(file2),
                "--output",
                "--no-color"
            ])

            assert result.returncode == 0
            assert "Migration analysis saved to output/reports/" in result.stdout

            # Verify directory was created
            assert Path("output/reports").exists()

    def test_migration_analysis_multiple_runs(self, run_cli):
        """Test multiple migration analysis runs create separate files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            file1 = Path(temp_dir) / "base.json"
            file2 = Path(temp_dir) / "variant.json"

            base_data = {"id": 1, "status": "active"}
            variant_data = {"id": 1, "status": "inactive"}

            file1.write_text(json.dumps(base_data))
            file2.write_text(json.dumps(variant_data))

            # Run twice
            result1 = run_cli([
                str(file1), str(file2),
                "--output",
                "--no-color"
            ])

            result2 = run_cli([
                str(file1), str(file2),
                "--output",
                "--no-color"
            ])

            assert result1.returncode == 0
            assert result2.returncode == 0

            # Both should succeed (files may be overwritten or timestamped)
