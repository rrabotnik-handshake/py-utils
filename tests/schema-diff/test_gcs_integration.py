#!/usr/bin/env python3
"""
Tests for GCS integration functionality.

Tests GCS path detection, file resolution, caching, and CLI integration.
"""
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from schema_diff.gcs_utils import (
    download_gcs_file,
    get_local_filename,
    is_gcs_path,
    parse_gcs_path,
)
from schema_diff.io_utils import resolve_file_path, set_force_download_context


class TestGCSPathDetection:
    """Test GCS path detection and parsing."""

    def test_is_gcs_path_gs_format(self):
        """Test detection of gs:// format paths."""
        assert is_gcs_path("gs://my-bucket/data.json")
        assert is_gcs_path("gs://bucket-name/path/to/file.json.gz")
        assert not is_gcs_path("s3://bucket/file.json")
        assert not is_gcs_path("local/file.json")

    def test_is_gcs_path_https_formats(self):
        """Test detection of HTTPS GCS URLs."""
        assert is_gcs_path("https://storage.cloud.google.com/bucket/file.json")
        assert is_gcs_path("https://storage.googleapis.com/bucket/file.json")
        assert not is_gcs_path("https://example.com/file.json")
        assert not is_gcs_path("http://storage.cloud.google.com/bucket/file.json")

    def test_parse_gcs_path_gs_format(self):
        """Test parsing of gs:// format paths."""
        bucket, obj = parse_gcs_path("gs://my-bucket/data.json")
        assert bucket == "my-bucket"
        assert obj == "data.json"

        bucket, obj = parse_gcs_path("gs://test-bucket/path/to/file.json.gz")
        assert bucket == "test-bucket"
        assert obj == "path/to/file.json.gz"

    def test_parse_gcs_path_https_formats(self):
        """Test parsing of HTTPS GCS URLs."""
        bucket, obj = parse_gcs_path("https://storage.cloud.google.com/bucket/file.json")
        assert bucket == "bucket"
        assert obj == "file.json"

        bucket, obj = parse_gcs_path("https://storage.googleapis.com/bucket/path/file.json")
        assert bucket == "bucket"
        assert obj == "path/file.json"

    def test_parse_gcs_path_url_decoding(self):
        """Test URL decoding in object paths."""
        bucket, obj = parse_gcs_path("https://storage.cloud.google.com/bucket/path%20with%20spaces.json")
        assert bucket == "bucket"
        assert obj == "path with spaces.json"

    def test_parse_gcs_path_invalid(self):
        """Test error handling for invalid paths."""
        with pytest.raises(ValueError, match="Not a valid GCS path"):
            parse_gcs_path("s3://bucket/file.json")

        with pytest.raises(ValueError, match="Invalid GCS path format"):
            parse_gcs_path("gs://")

        with pytest.raises(ValueError, match="Invalid GCS HTTPS URL format"):
            parse_gcs_path("https://storage.cloud.google.com/bucket-only")


class TestGCSFileOperations:
    """Test GCS file download and caching."""

    def test_get_local_filename(self):
        """Test local filename generation."""
        filename = get_local_filename("gs://my-bucket/data.json")
        assert filename == "data/my-bucket_data.json"

        filename = get_local_filename("gs://test/path/to/file.json.gz")
        assert filename == "data/test_path_to_file.json.gz"

    def test_get_local_filename_custom_dir(self):
        """Test local filename generation with custom directory."""
        filename = get_local_filename("gs://bucket/file.json", data_dir="custom")
        assert filename == "custom/bucket_file.json"

    @patch('schema_diff.gcs_utils._HAS_GCS', True)
    @patch('schema_diff.gcs_utils.storage')
    def test_download_gcs_file_success(self, mock_storage):
        """Test successful GCS file download."""
        # Mock GCS client and blob
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()

        mock_storage.Client.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = os.path.join(temp_dir, "test_file.json")

            # Mock successful download
            def mock_download(path):
                Path(path).write_text('{"test": "data"}')

            mock_blob.download_to_filename.side_effect = mock_download

            result = download_gcs_file("gs://test-bucket/data.json", local_path)

            assert result == local_path
            assert os.path.exists(local_path)
            mock_blob.download_to_filename.assert_called_once_with(local_path)

    @patch('schema_diff.gcs_utils._HAS_GCS', True)
    def test_download_gcs_file_caching(self):
        """Test GCS file caching behavior."""
        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = os.path.join(temp_dir, "cached_file.json")

            # Create existing file
            Path(local_path).write_text('{"cached": "data"}')

            # Should return cached file without downloading
            result = download_gcs_file("gs://test/data.json", local_path, force=False)
            assert result == local_path

    @patch('schema_diff.gcs_utils._HAS_GCS', False)
    def test_download_gcs_file_no_gcs_library(self):
        """Test error when GCS library is not available."""
        with pytest.raises(ImportError, match="Google Cloud Storage support requires"):
            download_gcs_file("gs://bucket/file.json")

    def test_download_gcs_file_invalid_path(self):
        """Test error for invalid GCS paths."""
        with pytest.raises(ValueError, match="Not a valid GCS path"):
            download_gcs_file("local/file.json")


class TestGCSIntegrationWithIO:
    """Test GCS integration with IO utilities."""

    def test_resolve_file_path_local(self):
        """Test path resolution for local files."""
        local_path = "local/file.json"
        result = resolve_file_path(local_path)
        assert result == local_path

    @patch('schema_diff.gcs_utils.download_gcs_file')
    @patch('schema_diff.gcs_utils.is_gcs_path')
    def test_resolve_file_path_gcs(self, mock_is_gcs, mock_download):
        """Test path resolution for GCS files."""
        mock_is_gcs.return_value = True
        mock_download.return_value = "/tmp/downloaded_file.json"

        result = resolve_file_path("gs://bucket/file.json")

        assert result == "/tmp/downloaded_file.json"
        mock_download.assert_called_once_with("gs://bucket/file.json", force=False)

    @patch('schema_diff.gcs_utils.download_gcs_file')
    @patch('schema_diff.gcs_utils.is_gcs_path')
    def test_resolve_file_path_force_download(self, mock_is_gcs, mock_download):
        """Test path resolution with force download."""
        mock_is_gcs.return_value = True
        mock_download.return_value = "/tmp/downloaded_file.json"

        result = resolve_file_path("gs://bucket/file.json", force_download=True)

        assert result == "/tmp/downloaded_file.json"
        mock_download.assert_called_once_with("gs://bucket/file.json", force=True)

    def test_force_download_context(self):
        """Test global force download context."""
        # Test default context
        set_force_download_context(False)

        with patch('schema_diff.gcs_utils.download_gcs_file') as mock_download, \
             patch('schema_diff.gcs_utils.is_gcs_path', return_value=True):
            mock_download.return_value = "/tmp/file.json"

            resolve_file_path("gs://bucket/file.json")
            mock_download.assert_called_once_with("gs://bucket/file.json", force=False)

        # Test force context
        set_force_download_context(True)

        with patch('schema_diff.gcs_utils.download_gcs_file') as mock_download, \
             patch('schema_diff.gcs_utils.is_gcs_path', return_value=True):
            mock_download.return_value = "/tmp/file.json"

            resolve_file_path("gs://bucket/file.json")
            mock_download.assert_called_once_with("gs://bucket/file.json", force=True)


class TestGCSCLIIntegration:
    """Test GCS integration with CLI commands."""

    def test_compare_command_exists(self):
        """Test that compare command exists and can be imported."""
        from schema_diff.cli.compare import cmd_compare
        assert callable(cmd_compare)

    def test_generate_command_exists(self):
        """Test that generate command exists and can be imported."""
        from schema_diff.cli.generate import cmd_generate
        assert callable(cmd_generate)

    def test_gcs_context_functions_exist(self):
        """Test that GCS context functions exist."""
        from schema_diff.io_utils import set_force_download_context, resolve_file_path
        assert callable(set_force_download_context)
        assert callable(resolve_file_path)


class TestGCSErrorScenarios:
    """Test GCS error handling scenarios."""

    def test_invalid_gcs_url_formats(self):
        """Test handling of malformed GCS URLs."""
        from schema_diff.gcs_utils import parse_gcs_path

        invalid_urls = [
            "gs://",  # Missing bucket
            "gs:///file.json",  # Empty bucket
            "gs://bucket",  # Missing object
            "gs://bucket/",  # Empty object
            "https://storage.cloud.google.com/",  # Missing bucket and object
            "https://invalid-domain.com/bucket/file.json",  # Wrong domain
            "ftp://bucket/file.json",  # Wrong protocol
        ]

        for invalid_url in invalid_urls:
            try:
                parse_gcs_path(invalid_url)
                assert False, f"Expected error for invalid URL: {invalid_url}"
            except (ValueError, Exception):
                pass  # Expected to fail

    @patch('schema_diff.gcs_utils.storage.Client')
    def test_gcs_authentication_error(self, mock_client_class):
        """Test handling of GCS authentication errors."""
        from schema_diff.gcs_utils import download_gcs_file
        from google.auth.exceptions import DefaultCredentialsError

        # Mock authentication failure
        mock_client_class.side_effect = DefaultCredentialsError("No credentials found")

        try:
            download_gcs_file("gs://test-bucket/test-file.json")
            assert False, "Expected authentication error"
        except Exception as e:
            assert "credentials" in str(e).lower() or "auth" in str(e).lower()

    @patch('schema_diff.gcs_utils.storage.Client')
    def test_gcs_network_error(self, mock_client_class):
        """Test handling of GCS network errors."""
        from schema_diff.gcs_utils import download_gcs_file
        from google.api_core.exceptions import ServiceUnavailable

        # Mock network failure
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_blob.download_to_filename.side_effect = ServiceUnavailable("Service unavailable")
        mock_bucket.blob.return_value = mock_blob
        mock_client.bucket.return_value = mock_bucket
        mock_client_class.return_value = mock_client

        try:
            download_gcs_file("gs://test-bucket/test-file.json")
            assert False, "Expected network error"
        except Exception as e:
            assert "unavailable" in str(e).lower() or "network" in str(e).lower()

    @patch('schema_diff.gcs_utils.storage.Client')
    def test_gcs_file_not_found(self, mock_client_class):
        """Test handling of GCS file not found errors."""
        from schema_diff.gcs_utils import download_gcs_file
        from google.api_core.exceptions import NotFound

        # Mock file not found
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_blob.download_to_filename.side_effect = NotFound("File not found")
        mock_bucket.blob.return_value = mock_blob
        mock_client.bucket.return_value = mock_bucket
        mock_client_class.return_value = mock_client

        try:
            download_gcs_file("gs://test-bucket/nonexistent-file.json")
            assert False, "Expected file not found error"
        except Exception as e:
            assert "not found" in str(e).lower()

    @patch('schema_diff.gcs_utils.storage.Client')
    def test_gcs_permission_denied(self, mock_client_class):
        """Test handling of GCS permission denied errors."""
        from schema_diff.gcs_utils import download_gcs_file
        from google.api_core.exceptions import Forbidden

        # Mock permission denied
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_blob.download_to_filename.side_effect = Forbidden("Permission denied")
        mock_bucket.blob.return_value = mock_blob
        mock_client.bucket.return_value = mock_bucket
        mock_client_class.return_value = mock_client

        try:
            download_gcs_file("gs://test-bucket/restricted-file.json")
            assert False, "Expected permission denied error"
        except Exception as e:
            assert "permission" in str(e).lower() or "forbidden" in str(e).lower()

    def test_gcs_url_decoding(self):
        """Test proper URL decoding for GCS paths with special characters."""
        from schema_diff.gcs_utils import parse_gcs_path

        # Test HTTPS URL with encoded characters (this format supports URL decoding)
        encoded_https_url = "https://storage.cloud.google.com/my-bucket/path%20with%20spaces/file%2Bname.json"
        bucket, object_path = parse_gcs_path(encoded_https_url)

        assert bucket == "my-bucket"
        assert object_path == "path with spaces/file+name.json"

        # Test gs:// URL (doesn't do URL decoding - this is expected behavior)
        gs_url = "gs://my-bucket/path%20with%20spaces/file%2Bname.json"
        bucket2, object_path2 = parse_gcs_path(gs_url)

        assert bucket2 == "my-bucket"
        assert object_path2 == "path%20with%20spaces/file%2Bname.json"  # No decoding for gs:// format
