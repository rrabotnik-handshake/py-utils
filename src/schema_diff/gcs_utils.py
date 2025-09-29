"""
Google Cloud Storage utilities for schema-diff.

This module provides functionality to detect, download, and cache files from GCS
for use with schema-diff operations.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urlparse

try:
    from google.cloud import storage

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
    """
    Parse a GCS path into bucket and object components.

    Args:
        gcs_path: GCS URI like 'gs://bucket-name/path/to/file.json' or
                 HTTPS URL like 'https://storage.cloud.google.com/bucket-name/path/to/file.json'

    Returns:
        Tuple of (bucket_name, object_path)

    Raises:
        ValueError: If the path is not a valid GCS URI
    """
    if not is_gcs_path(gcs_path):
        raise ValueError(f"Not a valid GCS path: {gcs_path}")

    if gcs_path.startswith("gs://"):
        # Handle gs:// format
        parsed = urlparse(gcs_path)
        bucket_name = parsed.netloc
        object_path = parsed.path.lstrip("/")
    elif gcs_path.startswith("https://storage.cloud.google.com/"):
        # Handle https://storage.cloud.google.com/bucket/object format
        path_part = gcs_path[len("https://storage.cloud.google.com/") :]
        if "/" not in path_part:
            raise ValueError(f"Invalid GCS HTTPS URL format: {gcs_path}")
        bucket_name, object_path = path_part.split("/", 1)
        # URL decode the object path to handle encoded characters
        object_path = unquote(object_path)
    elif gcs_path.startswith("https://storage.googleapis.com/"):
        # Handle https://storage.googleapis.com/bucket/object format
        path_part = gcs_path[len("https://storage.googleapis.com/") :]
        if "/" not in path_part:
            raise ValueError(f"Invalid GCS HTTPS URL format: {gcs_path}")
        bucket_name, object_path = path_part.split("/", 1)
        # URL decode the object path to handle encoded characters
        object_path = unquote(object_path)
    else:
        raise ValueError(f"Unsupported GCS path format: {gcs_path}")

    if not bucket_name or not object_path:
        raise ValueError(f"Invalid GCS path format: {gcs_path}")

    return bucket_name, object_path


def get_local_filename(gcs_path: str, data_dir: str = "data") -> str:
    """
    Generate a local filename for a GCS object.

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


def download_gcs_file(
    gcs_path: str, local_path: Optional[str] = None, force: bool = False
) -> str:
    """
    Download a file from GCS to local storage.

    Args:
        gcs_path: GCS URI to download
        local_path: Local path to save to (auto-generated if None)
        force: If True, re-download even if file exists locally

    Returns:
        Path to the downloaded local file

    Raises:
        ImportError: If google-cloud-storage is not installed
        Exception: If download fails
    """
    if not _HAS_GCS:
        raise ImportError(
            "Google Cloud Storage support requires 'google-cloud-storage'. "
            "Install with: pip install google-cloud-storage"
        )

    if not is_gcs_path(gcs_path):
        raise ValueError(f"Not a valid GCS path: {gcs_path}")

    # Determine local path
    if local_path is None:
        local_path = get_local_filename(gcs_path)

    # Check if file already exists and force is not set
    if os.path.exists(local_path) and not force:
        print(f"📁 Using cached file: {local_path}")
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
        print(f"☁️  Downloading {gcs_path} → {local_path}")
        blob.download_to_filename(local_path)

        # Verify download
        if not os.path.exists(local_path):
            raise Exception(f"Download failed: {local_path} was not created")

        file_size = os.path.getsize(local_path)
        print(f"✅ Downloaded {file_size:,} bytes")

        return local_path

    except Exception as e:
        # Clean up partial download
        if os.path.exists(local_path):
            os.remove(local_path)

        error_msg = str(e).lower()
        if "project" in error_msg and "environment" in error_msg:
            raise Exception(
                f"Failed to download {gcs_path}: {str(e)}\n"
                "💡 Fix: Set up GCS authentication:\n"
                "   gcloud config set project YOUR_PROJECT_ID\n"
                "   gcloud auth application-default login\n"
                "   OR export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json"
            ) from e
        elif "credentials" in error_msg or "authentication" in error_msg:
            raise Exception(
                f"Failed to download {gcs_path}: {str(e)}\n"
                "💡 Fix: Authenticate with GCS:\n"
                "   gcloud auth application-default login\n"
                "   OR export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json"
            ) from e
        else:
            raise Exception(f"Failed to download {gcs_path}: {str(e)}") from e


def resolve_path(path: str, force_download: bool = False) -> str:
    """
    Resolve a path that might be local or GCS.

    If it's a GCS path, download it to local storage.
    If it's a local path, return as-is.

    Args:
        path: Local file path or GCS URI
        force_download: If True, re-download GCS files even if cached

    Returns:
        Local file path (either original or downloaded)
    """
    if is_gcs_path(path):
        return download_gcs_file(path, force=force_download)
    else:
        return path


def cleanup_temp_files(paths: list[str]) -> None:
    """
    Clean up temporary downloaded files.

    Args:
        paths: List of file paths to potentially clean up
    """
    for path in paths:
        if path and os.path.exists(path) and path.startswith(tempfile.gettempdir()):
            try:
                os.remove(path)
                print(f"🗑️  Cleaned up temporary file: {path}")
            except Exception as e:
                print(f"⚠️  Could not clean up {path}: {e}")


def get_gcs_info(gcs_path: str) -> dict[str, any]:
    """
    Get metadata information about a GCS object.

    Args:
        gcs_path: GCS URI

    Returns:
        Dictionary with object metadata

    Raises:
        ImportError: If google-cloud-storage is not installed
    """
    if not _HAS_GCS:
        raise ImportError(
            "Google Cloud Storage support requires 'google-cloud-storage'. "
            "Install with: pip install google-cloud-storage"
        )

    bucket_name, object_path = parse_gcs_path(gcs_path)

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_path)

        # Reload to get fresh metadata
        blob.reload()

        return {
            "name": blob.name,
            "bucket": bucket_name,
            "size": blob.size,
            "updated": blob.updated,
            "content_type": blob.content_type,
            "md5_hash": blob.md5_hash,
            "etag": blob.etag,
        }

    except Exception as e:
        error_msg = str(e).lower()
        if "project" in error_msg and "environment" in error_msg:
            raise Exception(
                f"Failed to get info for {gcs_path}: {str(e)}\n"
                "💡 Fix: Set up GCS authentication:\n"
                "   gcloud config set project YOUR_PROJECT_ID\n"
                "   gcloud auth application-default login\n"
                "   OR export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json"
            ) from e
        elif "credentials" in error_msg or "authentication" in error_msg:
            raise Exception(
                f"Failed to get info for {gcs_path}: {str(e)}\n"
                "💡 Fix: Authenticate with GCS:\n"
                "   gcloud auth application-default login\n"
                "   OR export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json"
            ) from e
        else:
            raise Exception(f"Failed to get info for {gcs_path}: {str(e)}") from e


# Convenience functions for common operations


def download_and_compare(
    left_path: str, right_path: str, force_download: bool = False
) -> tuple[str, str]:
    """
    Download GCS files if needed and return local paths for comparison.

    Args:
        left_path: First file (local or GCS)
        right_path: Second file (local or GCS)
        force_download: Re-download GCS files even if cached

    Returns:
        Tuple of (local_left_path, local_right_path)
    """
    local_left = resolve_path(left_path, force_download)
    local_right = resolve_path(right_path, force_download)

    return local_left, local_right


def is_gcs_available() -> bool:
    """Check if GCS functionality is available."""
    return _HAS_GCS


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
