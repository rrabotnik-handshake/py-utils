"""Google Cloud Storage utilities for schema-diff.

This module provides functionality to detect, download, and cache files from GCS for use
with schema-diff operations.

Enhanced with retry decorators for robust GCS operations.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urlparse

from .decorators import retry_gcs_operation
from .exceptions import ArgumentError, DependencyError, FileOperationError, GCSError

try:
    from google.cloud import storage  # type: ignore

    _HAS_GCS = True
except ImportError:
    _HAS_GCS = False


def is_gcs_path(path: str) -> bool:
    """Check if a path is a GCS URI (gs://) or HTTPS URL to GCS."""
    return (
        path.startswith("gs://")
        or path.startswith("https://storage.cloud.google.com/")
        or path.startswith("https://storage.googleapis.com/")
    )


def parse_gcs_path(gcs_path: str) -> tuple[str, str]:
    """Parse a GCS path into bucket and object components.

    Args:
        gcs_path: GCS URI like 'gs://bucket-name/path/to/file.json' or
                 HTTPS URL like 'https://storage.cloud.google.com/bucket-name/path/to/file.json'

    Returns:
        Tuple of (bucket_name, object_path)

    Raises:
        ArgumentError: If the path is not a valid GCS URI
    """
    if not is_gcs_path(gcs_path):
        raise ArgumentError(
            f"Not a valid GCS path: {gcs_path}",
            argument_name="gcs_path",
            argument_value=gcs_path,
        )

    if gcs_path.startswith("gs://"):
        # Handle gs:// format
        parsed = urlparse(gcs_path)
        bucket_name = parsed.netloc
        object_path = parsed.path.lstrip("/")
    elif gcs_path.startswith("https://storage.cloud.google.com/"):
        # Handle https://storage.cloud.google.com/bucket/object format
        path_part = gcs_path[len("https://storage.cloud.google.com/") :]
        if "/" not in path_part:
            raise ArgumentError(
                f"Invalid GCS HTTPS URL format: {gcs_path}",
                argument_name="gcs_path",
                argument_value=gcs_path,
            )
        bucket_name, object_path = path_part.split("/", 1)
        # URL decode the object path to handle encoded characters
        object_path = unquote(object_path)
    elif gcs_path.startswith("https://storage.googleapis.com/"):
        # Handle https://storage.googleapis.com/bucket/object format
        path_part = gcs_path[len("https://storage.googleapis.com/") :]
        if "/" not in path_part:
            raise ArgumentError(
                f"Invalid GCS HTTPS URL format: {gcs_path}",
                argument_name="gcs_path",
                argument_value=gcs_path,
            )
        bucket_name, object_path = path_part.split("/", 1)
        # URL decode the object path to handle encoded characters
        object_path = unquote(object_path)
    else:
        raise ArgumentError(
            f"Unsupported GCS path format: {gcs_path}",
            argument_name="gcs_path",
            argument_value=gcs_path,
        )

    if not bucket_name or not object_path:
        raise ArgumentError(
            f"Invalid GCS path format: {gcs_path}",
            argument_name="gcs_path",
            argument_value=gcs_path,
        )

    return bucket_name, object_path


def get_local_filename(gcs_path: str, data_dir: str = "data") -> str:
    """Generate a local filename for a GCS object.

    Args:
        gcs_path: GCS URI
        data_dir: Local directory to store the file

    Returns:
        Local file path where the GCS object should be stored
    """
    bucket_name, object_path = parse_gcs_path(gcs_path)

    # Create a safe filename from the GCS path
    # Replace path separators with underscores to flatten the structure
    safe_object_path = object_path.replace("/", "_")
    filename = f"{bucket_name}_{safe_object_path}"

    # Ensure data directory exists
    Path(data_dir).mkdir(exist_ok=True)

    return os.path.join(data_dir, filename)


# Track which files we've already notified about to avoid duplicate messages
_notified_cached_files = set()


@retry_gcs_operation
def download_gcs_file(
    gcs_path: str, local_path: Optional[str] = None, force: bool = False
) -> str:
    """Download a file from GCS to local storage.

    Args:
        gcs_path: GCS URI to download
        local_path: Local path to save to (auto-generated if None)
        force: If True, re-download even if file exists locally

    Returns:
        Path to the downloaded local file

    Raises:
        DependencyError: If google-cloud-storage is not installed
        GCSError: If download fails
        ArgumentError: If the path is not a valid GCS URI
    """
    if not _HAS_GCS:
        raise DependencyError(
            "Google Cloud Storage support requires 'google-cloud-storage'. "
            "Install with: pip install google-cloud-storage",
            dependency_name="google-cloud-storage",
        )

    if not is_gcs_path(gcs_path):
        raise ArgumentError(
            f"Not a valid GCS path: {gcs_path}",
            argument_name="gcs_path",
            argument_value=gcs_path,
        )

    # Determine local path
    if local_path is None:
        local_path = get_local_filename(gcs_path)

    # Check if file already exists and force is not set
    if os.path.exists(local_path) and not force:
        # Only print the message once per file per session
        if local_path not in _notified_cached_files:
            from .cli.colors import GREEN, RESET

            print(f"{GREEN}📁 Using cached file: {local_path}{RESET}")
            _notified_cached_files.add(local_path)
        return local_path

    # Parse GCS path
    bucket_name, object_path = parse_gcs_path(gcs_path)

    try:
        # Initialize GCS client
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_path)

        # Ensure local directory exists
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        # Download the file
        from .cli.colors import GREEN, RESET

        print(f"{GREEN}☁️  Downloading {gcs_path} → {local_path}{RESET}")
        blob.download_to_filename(local_path)

        # Verify download
        if not os.path.exists(local_path):
            raise FileOperationError(
                f"Download failed: {local_path} was not created",
                file_path=local_path,
                operation="download_verify",
            )

        file_size = os.path.getsize(local_path)
        print(f"{GREEN}✅ Downloaded {file_size:,} bytes{RESET}")

        # Mark as notified to avoid "Using cached file" message on next access
        _notified_cached_files.add(local_path)

        return local_path

    except (GCSError, ArgumentError, DependencyError, FileOperationError):
        # Re-raise schema-diff exceptions as-is
        raise
    except Exception as e:
        # Clean up partial download
        if os.path.exists(local_path):
            try:
                os.remove(local_path)
            except Exception:
                pass  # Best effort cleanup

        error_msg = str(e).lower()
        if "project" in error_msg and "environment" in error_msg:
            raise GCSError(
                f"Failed to download from GCS: {str(e)}\n"
                "💡 Fix: Set up GCS authentication:\n"
                "   gcloud config set project YOUR_PROJECT_ID\n"
                "   gcloud auth application-default login\n"
                "   OR export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json",
                bucket_name=bucket_name,
                object_name=object_path,
                operation="download",
                cause=e,
            ) from e
        elif "credentials" in error_msg or "authentication" in error_msg:
            raise GCSError(
                f"Failed to download from GCS: {str(e)}\n"
                "💡 Fix: Authenticate with GCS:\n"
                "   gcloud auth application-default login\n"
                "   OR export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json",
                bucket_name=bucket_name,
                object_name=object_path,
                operation="download",
                cause=e,
            ) from e
        else:
            raise GCSError(
                f"Failed to download from GCS: {str(e)}",
                bucket_name=bucket_name,
                object_name=object_path,
                operation="download",
                cause=e,
            ) from e


# Convenience functions for common operations


def get_gcs_status() -> str:
    """Get a status message about GCS availability."""
    if _HAS_GCS:
        try:
            # Try to create a client to check authentication
            storage.Client()
            return "✅ GCS support available and authenticated"
        except Exception as e:
            return f"⚠️  GCS support available but authentication failed: {e}"
    else:
        return "❌ GCS support not available (install google-cloud-storage)"
