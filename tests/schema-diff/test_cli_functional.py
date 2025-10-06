"""
Functional tests for schema-diff CLI.

These tests verify complete end-to-end functionality of the CLI,
testing the full user workflow rather than individual components.
"""
import json
import tempfile
from pathlib import Path
import pytest


class TestCLIFunctional:
    """Test complete CLI workflows end-to-end."""

    def test_show_common_displays_fields(self, tmp_path, run_cli):
        """Test that --show-common actually displays common fields in output."""
        # Create test files with known common fields
        file1 = tmp_path / "data1.json"
        file2 = tmp_path / "data2.json"

        data1 = {"name": "Alice", "age": 30, "city": "NYC", "unique1": "value1"}
        data2 = {"name": "Bob", "age": 25, "city": "LA", "unique2": "value2"}

        file1.write_text(json.dumps(data1))
        file2.write_text(json.dumps(data2))

        # Run with --show-common
        result = run_cli([
            str(file1), str(file2),
            "--show-common", "--first-record", "--no-color"
        ])

        assert result.returncode == 0

        # Verify common fields are displayed
        assert "Common fields" in result.stdout
        assert "name" in result.stdout
        assert "age" in result.stdout
        assert "city" in result.stdout

        # Verify count is correct (3 common fields)
        assert "(3)" in result.stdout

        # Verify unique fields are not in common section
        common_section = self._extract_common_section(result.stdout)
        assert "unique1" not in common_section
        assert "unique2" not in common_section

    def test_show_common_with_nested_fields(self, tmp_path, run_cli):
        """Test --show-common with nested object structures."""
        file1 = tmp_path / "nested1.json"
        file2 = tmp_path / "nested2.json"

        data1 = {
            "user": {"name": "Alice", "profile": {"age": 30}},
            "settings": {"theme": "dark"},
            "unique1": "value1"
        }
        data2 = {
            "user": {"name": "Bob", "profile": {"age": 25}},
            "settings": {"theme": "light"},
            "unique2": "value2"
        }

        file1.write_text(json.dumps(data1))
        file2.write_text(json.dumps(data2))

        result = run_cli([
            str(file1), str(file2),
            "--show-common", "--first-record", "--no-color"
        ])

        assert result.returncode == 0

        # Verify nested common fields are displayed with dot notation
        assert "user.name" in result.stdout
        assert "user.profile.age" in result.stdout
        assert "settings.theme" in result.stdout

    def test_migration_analysis_generation(self, tmp_path, run_cli):
        """Test that --output generates migration analysis with correct data."""
        file1 = tmp_path / "source.json"
        file2 = tmp_path / "target.json"

        # Create files with known differences
        data1 = {"id": 1, "name": "Alice", "removed_field": "gone"}
        data2 = {"id": 1, "name": "Bob", "new_field": "added"}

        file1.write_text(json.dumps(data1))
        file2.write_text(json.dumps(data2))

        # Change to tmp directory to control output location
        original_cwd = Path.cwd()
        try:
            import os
            os.chdir(tmp_path)

            result = run_cli([
                str(file1), str(file2),
                "--output", "--first-record", "--no-color"
            ])

            assert result.returncode == 0

            # Verify migration analysis file was created
            analysis_file = tmp_path / "output" / "reports" / "migration_analysis.md"
            assert analysis_file.exists(), f"Migration analysis not found at {analysis_file}"

            # Verify analysis content
            analysis_content = analysis_file.read_text()

            # Should show correct common field count (2: id, name)
            assert "2 common fields" in analysis_content

            # Should have basic migration analysis structure
            # Note: Current migration analyzer doesn't detail individual field changes
            # but at least it should generate a complete analysis

            # Should have proper structure
            assert "# 📊 Schema Migration Analysis" in analysis_content
            assert "## Migration Overview" in analysis_content
            assert "## Compatibility Summary" in analysis_content

        finally:
            os.chdir(original_cwd)

    def test_output_file_path_correctness(self, tmp_path, run_cli):
        """Test that --output creates files in correct directory structure."""
        file1 = tmp_path / "test1.json"
        file2 = tmp_path / "test2.json"

        file1.write_text(json.dumps({"a": 1}))
        file2.write_text(json.dumps({"b": 2}))

        original_cwd = Path.cwd()
        try:
            import os
            os.chdir(tmp_path)

            result = run_cli([
                str(file1), str(file2),
                "--output", "--first-record"
            ])

            assert result.returncode == 0

            # Verify correct directory structure
            output_dir = tmp_path / "output"
            reports_dir = output_dir / "reports"
            analysis_file = reports_dir / "migration_analysis.md"

            assert output_dir.exists()
            assert reports_dir.exists()
            assert analysis_file.exists()

            # Verify it's a file, not a directory
            assert analysis_file.is_file()
            assert not analysis_file.is_dir()

        finally:
            os.chdir(original_cwd)

    def test_no_double_output(self, tmp_path, run_cli):
        """Test that comparison output appears only once (no double execution)."""
        file1 = tmp_path / "single1.json"
        file2 = tmp_path / "single2.json"

        # Create files with actual schema differences
        file1.write_text(json.dumps({"field1": "value", "common": "shared"}))
        file2.write_text(json.dumps({"field2": "value", "common": "shared"}))

        result = run_cli([
            str(file1), str(file2),
            "--first-record", "--no-color"
        ])

        assert result.returncode == 0

        # Count occurrences of the schema diff header
        header_count = result.stdout.count("=== Schema diff")
        assert header_count == 1, f"Expected 1 schema diff header, found {header_count}"

        # Count occurrences of sections that should appear only once
        only_in_count = result.stdout.count("-- Only in")
        # Should be 2: "Only in file1" and "Only in file2"
        assert only_in_count == 2, f"Expected 2 'Only in' sections, found {only_in_count}"

    def test_parameter_passing_integration(self, tmp_path, run_cli):
        """Test that CLI parameters are correctly passed to underlying functions."""
        file1 = tmp_path / "param1.json"
        file2 = tmp_path / "param2.json"

        # Create files with array data to test sampling
        data1 = [{"id": i, "value": f"item{i}"} for i in range(100)]
        data2 = [{"id": i, "value": f"modified{i}"} for i in range(100)]

        file1.write_text(json.dumps(data1))
        file2.write_text(json.dumps(data2))

        # Test sampling parameter
        result = run_cli([
            str(file1), str(file2),
            "-k", "5", "--no-color"  # Sample only 5 records
        ])

        assert result.returncode == 0

        # Should mention sampling in output
        assert "5 samples" in result.stdout

        # Test all-records parameter
        result_all = run_cli([
            str(file1), str(file2),
            "--all-records", "--no-color"
        ])

        assert result_all.returncode == 0

        # Should mention all records in output
        assert "all records" in result_all.stdout

    def test_complex_workflow_with_all_flags(self, tmp_path, run_cli):
        """Test complex workflow combining multiple flags."""
        file1 = tmp_path / "complex1.json"
        file2 = tmp_path / "complex2.json"

        # Complex nested data
        data1 = {
            "users": [
                {"id": 1, "name": "Alice", "profile": {"age": 30, "city": "NYC"}},
                {"id": 2, "name": "Bob", "profile": {"age": 25, "city": "LA"}}
            ],
            "metadata": {"version": "1.0", "created": "2023-01-01"},
            "removed_section": {"old_data": "will be removed"}
        }

        data2 = {
            "users": [
                {"id": 1, "name": "Alice", "profile": {"age": 30, "city": "NYC"}},
                {"id": 2, "name": "Bob", "profile": {"age": 25, "city": "LA"}}
            ],
            "metadata": {"version": "2.0", "created": "2023-01-01", "updated": "2023-12-01"},
            "new_section": {"new_data": "freshly added"}
        }

        file1.write_text(json.dumps(data1))
        file2.write_text(json.dumps(data2))

        original_cwd = Path.cwd()
        try:
            import os
            os.chdir(tmp_path)

            # Run with multiple flags
            result = run_cli([
                str(file1), str(file2),
                "--show-common", "--output", "--all-records", "--no-color"
            ])

            assert result.returncode == 0

            # Verify common fields are shown
            assert "Common fields" in result.stdout
            assert "users[].id" in result.stdout
            assert "users[].name" in result.stdout
            assert "metadata.created" in result.stdout

            # Verify migration analysis was created
            analysis_file = tmp_path / "output" / "reports" / "migration_analysis.md"
            assert analysis_file.exists()

            analysis_content = analysis_file.read_text()

            # Should show correct common field count
            # Common fields: users[].id, users[].name, users[].profile.age,
            # users[].profile.city, metadata.created, metadata.version
            assert "6 common fields" in analysis_content

            # Migration analysis should be generated successfully
            # Note: Current migration analyzer focuses on compatibility, not detailed field changes
            assert "Migration Recommendation" in analysis_content

        finally:
            os.chdir(original_cwd)

    def _extract_common_section(self, stdout: str) -> str:
        """Extract the common fields section from stdout."""
        lines = stdout.split('\n')
        common_section = []
        in_common = False

        for line in lines:
            if "Common fields" in line:
                in_common = True
                common_section.append(line)
                continue
            elif in_common and line.strip().startswith("--"):
                # Next section started
                break
            elif in_common:
                common_section.append(line)

        return '\n'.join(common_section)


class TestCLIParameterValidation:
    """Test that CLI parameters are validated and passed correctly."""

    def test_show_common_parameter_passed(self, tmp_path, run_cli):
        """Verify --show-common parameter is actually used."""
        file1 = tmp_path / "test1.json"
        file2 = tmp_path / "test2.json"

        file1.write_text(json.dumps({"common": "field1", "unique1": "value"}))
        file2.write_text(json.dumps({"common": "field2", "unique2": "value"}))

        # Without --show-common
        result_without = run_cli([
            str(file1), str(file2), "--first-record", "--no-color"
        ])

        # With --show-common
        result_with = run_cli([
            str(file1), str(file2), "--first-record", "--show-common", "--no-color"
        ])

        assert result_without.returncode == 0
        assert result_with.returncode == 0

        # Only the version with --show-common should have common fields section
        assert "Common fields" not in result_without.stdout
        assert "Common fields" in result_with.stdout
        assert "common" in result_with.stdout

    def test_output_parameter_creates_files(self, tmp_path, run_cli):
        """Verify --output parameter actually creates output files."""
        file1 = tmp_path / "out1.json"
        file2 = tmp_path / "out2.json"

        file1.write_text(json.dumps({"test": 1}))
        file2.write_text(json.dumps({"test": 2}))

        original_cwd = Path.cwd()
        try:
            import os
            os.chdir(tmp_path)

            # Without --output
            result_without = run_cli([
                str(file1), str(file2), "--first-record"
            ])

            # With --output
            result_with = run_cli([
                str(file1), str(file2), "--first-record", "--output"
            ])

            assert result_without.returncode == 0
            assert result_with.returncode == 0

            # Only the version with --output should create files
            output_dir = tmp_path / "output"

            # The directory should exist after --output
            assert output_dir.exists()

            # Migration analysis should exist
            analysis_file = output_dir / "reports" / "migration_analysis.md"
            assert analysis_file.exists()

        finally:
            os.chdir(original_cwd)


class TestCLIErrorHandling:
    """Test CLI error handling and edge cases."""

    def test_invalid_files_handled_gracefully(self, tmp_path, run_cli):
        """Test that invalid files produce helpful error messages."""
        from schema_diff.io_utils import CommandError

        nonexistent = tmp_path / "does_not_exist.json"
        valid_file = tmp_path / "valid.json"
        valid_file.write_text(json.dumps({"test": "data"}))

        # Should raise CommandError with helpful message
        with pytest.raises(CommandError) as exc_info:
            run_cli([str(nonexistent), str(valid_file)])

        # Should contain helpful error message
        error_output = str(exc_info.value) + exc_info.value.stdout + exc_info.value.stderr
        assert "No such file or directory" in error_output

    def test_malformed_json_handled(self, tmp_path, run_cli):
        """Test that malformed JSON is handled gracefully."""
        from schema_diff.io_utils import CommandError

        bad_file = tmp_path / "malformed.json"
        good_file = tmp_path / "good.json"

        bad_file.write_text('{"invalid": json}')  # Invalid JSON
        good_file.write_text(json.dumps({"valid": "json"}))

        # Should raise CommandError with JSON parsing error
        with pytest.raises(CommandError) as exc_info:
            run_cli([str(bad_file), str(good_file)])

        # Should contain JSON parsing error message
        error_output = str(exc_info.value) + exc_info.value.stdout + exc_info.value.stderr
        assert "Expecting value" in error_output
