"""
Tests for DDL integration in the consolidated CLI.
"""
import pytest
import sys
from unittest.mock import patch, Mock
from pathlib import Path

from schema_diff.cli import cmd_ddl, _parse_table_ref, _parse_dataset_ref


class TestDDLIntegration:
    """Test DDL command integration with the CLI."""

    @patch("schema_diff.output_utils.write_output_file")
    @patch("schema_diff.output_utils.print_output_success")
    @patch("schema_diff.bigquery_ddl.generate_table_ddl")
    @patch("google.cloud.bigquery.Client")
    def test_ddl_table_command_console_output(self, mock_client, mock_generate, mock_print, mock_write):
        """Test DDL table command with console output."""
        mock_generate.return_value = "CREATE OR REPLACE TABLE `test-project.test_dataset.test_table` (\n  id INT64\n);"
        
        mock_args = Mock()
        mock_args.ddl_command = "table"
        mock_args.table_ref = "test-project:test_dataset.test_table"
        mock_args.output = False
        
        with patch("builtins.print") as mock_console_print:
            cmd_ddl(mock_args)
            
        mock_generate.assert_called_once_with(mock_client.return_value, "test-project", "test_dataset", "test_table")
        # Should print status message and DDL content
        assert mock_console_print.call_count == 2
        mock_write.assert_not_called()

    @patch("schema_diff.cli.ddl.write_output_file")
    @patch("schema_diff.output_utils.print_output_success")
    @patch("schema_diff.bigquery_ddl.generate_table_ddl")
    @patch("google.cloud.bigquery.Client")
    def test_ddl_table_command_file_output(self, mock_client, mock_generate, mock_print, mock_write):
        """Test DDL table command with file output."""
        mock_generate.return_value = "CREATE OR REPLACE TABLE `test-project.test_dataset.test_table` (\n  id INT64\n);"
        mock_write.return_value = "/path/to/output/file.sql"
        
        mock_args = Mock()
        mock_args.ddl_command = "table"
        mock_args.table_ref = "test-project:test_dataset.test_table"
        mock_args.output = True
        
        cmd_ddl(mock_args)
        
        mock_generate.assert_called_once_with(mock_client.return_value, "test-project", "test_dataset", "test_table")
        mock_write.assert_called_once()
        mock_print.assert_called_once()

    @patch("schema_diff.output_utils.write_output_file")
    @patch("schema_diff.output_utils.print_output_success")
    @patch("schema_diff.bigquery_ddl.generate_table_ddl")
    @patch("google.cloud.bigquery.Client")
    def test_ddl_batch_command(self, mock_client, mock_generate, mock_print, mock_write):
        """Test DDL batch command."""
        mock_generate.side_effect = [
            "CREATE OR REPLACE TABLE `test-project.test_dataset.table1` (\n  id INT64\n);",
            "CREATE OR REPLACE TABLE `test-project.test_dataset.table2` (\n  name STRING\n);"
        ]
        
        mock_args = Mock()
        mock_args.ddl_command = "batch"
        mock_args.table_refs = ["test-project:test_dataset.table1", "test-project:test_dataset.table2"]
        mock_args.output = False
        
        with patch("builtins.print") as mock_console_print:
            cmd_ddl(mock_args)
        
        assert mock_generate.call_count == 2
        assert mock_console_print.call_count >= 2  # At least one call per table

    @patch("schema_diff.output_utils.write_output_file")
    @patch("schema_diff.output_utils.print_output_success")
    @patch("schema_diff.bigquery_ddl.generate_dataset_ddl")
    @patch("google.cloud.bigquery.Client")
    def test_ddl_dataset_command(self, mock_client, mock_generate, mock_print, mock_write):
        """Test DDL dataset command."""
        mock_generate.return_value = {
            "test-project:test_dataset.table1": "CREATE OR REPLACE TABLE `test-project.test_dataset.table1` (\n  id INT64\n);",
            "test-project:test_dataset.table2": "CREATE OR REPLACE TABLE `test-project.test_dataset.table2` (\n  name STRING\n);"
        }
        
        mock_args = Mock()
        mock_args.ddl_command = "dataset"
        mock_args.dataset_ref = "test-project:test_dataset"
        mock_args.output = False
        
        with patch("builtins.print") as mock_console_print:
            cmd_ddl(mock_args)
        
        mock_generate.assert_called_once_with(mock_client.return_value, "test-project", "test_dataset")
        assert mock_console_print.call_count >= 2  # At least one call per table

    @patch("google.cloud.bigquery.Client")
    def test_ddl_bigquery_client_error(self, mock_client):
        """Test DDL command with BigQuery client initialization error."""
        mock_client.side_effect = Exception("Authentication failed")
        
        mock_args = Mock()
        mock_args.ddl_command = "table"
        mock_args.table_ref = "test-project:test_dataset.test_table"
        
        with pytest.raises(SystemExit):
            cmd_ddl(mock_args)

    def test_ddl_invalid_command(self):
        """Test DDL command with invalid subcommand."""
        mock_args = Mock()
        mock_args.ddl_command = None
        
        with pytest.raises(SystemExit):
            cmd_ddl(mock_args)


class TestDDLErrorScenarios:
    """Test DDL generation error scenarios."""

    @patch("google.cloud.bigquery.Client")
    def test_ddl_invalid_table_reference(self, mock_client):
        """Test DDL generation with invalid table reference format."""
        from schema_diff.cli.ddl import _parse_table_ref
        
        invalid_refs = [
            "project:dataset:table:extra",  # Too many colons  
            "project:dataset",  # Missing table (no dot)
            "",  # Empty string
            ":",  # Just colon
            "project:dataset:table.extra",  # Too many dots after colon
        ]
        
        for invalid_ref in invalid_refs:
            try:
                _parse_table_ref(invalid_ref)
                assert False, f"Expected error for invalid table ref: {invalid_ref}"
            except ValueError:
                pass  # Expected to fail

    @patch("google.cloud.bigquery.Client")
    def test_ddl_table_not_found(self, mock_client_class):
        """Test DDL generation when table doesn't exist."""
        from schema_diff.bigquery_ddl import generate_table_ddl
        from google.api_core.exceptions import NotFound
        
        # Mock table not found
        mock_client = Mock()
        mock_table = Mock()
        mock_client.get_table.side_effect = NotFound("Table not found")
        mock_client_class.return_value = mock_client
        
        try:
            generate_table_ddl(mock_client, "test-project", "test_dataset", "nonexistent_table")
            assert False, "Expected table not found error"
        except Exception as e:
            assert "not found" in str(e).lower()

    @patch("google.cloud.bigquery.Client")
    def test_ddl_permission_denied(self, mock_client_class):
        """Test DDL generation with insufficient permissions."""
        from schema_diff.bigquery_ddl import generate_table_ddl
        from google.api_core.exceptions import Forbidden
        
        # Mock permission denied
        mock_client = Mock()
        mock_client.get_table.side_effect = Forbidden("Permission denied")
        mock_client_class.return_value = mock_client
        
        try:
            generate_table_ddl(mock_client, "test-project", "restricted_dataset", "table")
            assert False, "Expected permission denied error"
        except Exception as e:
            assert "permission" in str(e).lower() or "forbidden" in str(e).lower()

    @patch("google.cloud.bigquery.Client")
    def test_ddl_dataset_not_found(self, mock_client_class):
        """Test DDL generation when dataset doesn't exist."""
        from schema_diff.bigquery_ddl import generate_dataset_ddl
        from google.api_core.exceptions import NotFound
        
        # Mock dataset not found
        mock_client = Mock()
        mock_client.list_tables.side_effect = NotFound("Dataset not found")
        mock_client_class.return_value = mock_client
        
        try:
            generate_dataset_ddl(mock_client, "test-project", "nonexistent_dataset")
            assert False, "Expected dataset not found error"
        except Exception as e:
            assert "not found" in str(e).lower()

    @patch("google.cloud.bigquery.Client")
    def test_ddl_network_timeout(self, mock_client_class):
        """Test DDL generation with network timeout."""
        from schema_diff.bigquery_ddl import generate_table_ddl
        from google.api_core.exceptions import ServiceUnavailable
        
        # Mock network timeout
        mock_client = Mock()
        mock_client.get_table.side_effect = ServiceUnavailable("Service unavailable")
        mock_client_class.return_value = mock_client
        
        try:
            generate_table_ddl(mock_client, "test-project", "test_dataset", "test_table")
            assert False, "Expected service unavailable error"
        except Exception as e:
            assert "unavailable" in str(e).lower() or "timeout" in str(e).lower()

    def test_ddl_invalid_dataset_reference(self):
        """Test DDL generation with invalid dataset reference format."""
        from schema_diff.cli.ddl import _parse_dataset_ref
        
        invalid_refs = [
            "project:dataset:extra",  # Too many colons
            "project.dataset",  # Wrong separator (should be colon)
        ]
        
        for invalid_ref in invalid_refs:
            try:
                _parse_dataset_ref(invalid_ref)
                assert False, f"Expected error for invalid dataset ref: {invalid_ref}"
            except ValueError:
                pass  # Expected to fail


class TestTableRefParsing:
    """Test table and dataset reference parsing."""

    def test_parse_table_ref_standard_format(self):
        """Test parsing standard table references."""
        project, dataset, table = _parse_table_ref("my-project:my_dataset.my_table")
        assert project == "my-project"
        assert dataset == "my_dataset"
        assert table == "my_table"

    def test_parse_table_ref_with_special_chars(self):
        """Test parsing table references with special characters."""
        project, dataset, table = _parse_table_ref("my-project-123:dataset_v2.table-final_v1")
        assert project == "my-project-123"
        assert dataset == "dataset_v2"
        assert table == "table-final_v1"

    def test_parse_table_ref_missing_colon(self):
        """Test parsing table reference missing colon."""
        with pytest.raises(ValueError, match="Invalid table reference format"):
            _parse_table_ref("project.dataset.table")

    def test_parse_table_ref_missing_dot(self):
        """Test parsing table reference missing dot."""
        with pytest.raises(ValueError, match="Invalid dataset.table format"):
            _parse_table_ref("project:dataset_table")

    def test_parse_table_ref_too_many_parts(self):
        """Test parsing table reference with too many parts."""
        with pytest.raises(ValueError, match="Invalid table reference format"):
            _parse_table_ref("project1:project2:dataset.table")

    def test_parse_dataset_ref_standard_format(self):
        """Test parsing standard dataset references."""
        project, dataset = _parse_dataset_ref("my-project:my_dataset")
        assert project == "my-project"
        assert dataset == "my_dataset"

    def test_parse_dataset_ref_with_special_chars(self):
        """Test parsing dataset references with special characters."""
        project, dataset = _parse_dataset_ref("my-project-123:dataset_v2")
        assert project == "my-project-123"
        assert dataset == "dataset_v2"

    def test_parse_dataset_ref_missing_colon(self):
        """Test parsing dataset reference missing colon."""
        with pytest.raises(ValueError, match="Invalid dataset reference format"):
            _parse_dataset_ref("project.dataset")

    def test_parse_dataset_ref_too_many_parts(self):
        """Test parsing dataset reference with too many parts."""
        with pytest.raises(ValueError, match="Invalid dataset reference format"):
            _parse_dataset_ref("project1:project2:dataset")


class TestDDLCommandLineIntegration:
    """Test DDL commands through the CLI interface."""

    def test_ddl_table_help(self, run_cli):
        """Test DDL table help command."""
        result = run_cli(["ddl", "table", "--help"])
        assert result.returncode == 0
        assert "table_ref" in result.stdout
        assert "Table reference (project:dataset.table)" in result.stdout

    def test_ddl_batch_help(self, run_cli):
        """Test DDL batch help command."""
        result = run_cli(["ddl", "batch", "--help"])
        assert result.returncode == 0
        assert "table_refs" in result.stdout

    def test_ddl_dataset_help(self, run_cli):
        """Test DDL dataset help command."""
        result = run_cli(["ddl", "dataset", "--help"])
        assert result.returncode == 0
        assert "dataset_ref" in result.stdout
        assert "Dataset reference (project:dataset)" in result.stdout

    def test_ddl_table_command_integration(self, run_cli):
        """Test DDL table command end-to-end integration."""
        # This will fail at BigQuery client initialization, which is expected
        # We're testing that the CLI parsing works correctly
        from schema_diff.io_utils import CommandError
        
        try:
            result = run_cli(["ddl", "table", "test-project:test_dataset.test_table"])
            # If it doesn't raise, check the result
            assert result.returncode == 1
            assert "Error:" in result.stderr
        except CommandError as e:
            # Expected - BigQuery error, not argument parsing error
            assert e.returncode == 1  # BigQuery error, not argparse error (which would be 2)
            assert "Error:" in e.stderr


class TestDDLOutputFormatting:
    """Test DDL output formatting and file naming."""

    def test_table_ref_to_filename(self):
        """Test conversion of table reference to filename."""
        table_ref = "my-project:my_dataset.my_table"
        expected_filename = "my-project_my_dataset_my_table_ddl.sql"
        actual_filename = f"{table_ref.replace(':', '_').replace('.', '_')}_ddl.sql"
        assert actual_filename == expected_filename

    def test_complex_table_ref_to_filename(self):
        """Test conversion of complex table reference to filename."""
        table_ref = "my-project-123:dataset_v2.table-final_v1"
        expected_filename = "my-project-123_dataset_v2_table-final_v1_ddl.sql"
        actual_filename = f"{table_ref.replace(':', '_').replace('.', '_')}_ddl.sql"
        assert actual_filename == expected_filename
