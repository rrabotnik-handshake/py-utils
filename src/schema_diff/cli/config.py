#!/usr/bin/env python3
"""
Config command implementation for schema-diff CLI.

Handles configuration management and system information display.
"""
from __future__ import annotations

import sys
from pathlib import Path


def add_config_subcommand(subparsers) -> None:
    """Add config subcommand to the parser."""
    config_parser = subparsers.add_parser(
        "config",
        help="Manage configuration",
        description="Manage configuration and show system information",
    )

    config_parser.add_argument(
        "--show-info",
        action="store_true",
        help="Show system and dependency information",
    )
    config_parser.add_argument(
        "--check-deps", action="store_true", help="Check optional dependencies"
    )
    config_parser.add_argument(
        "--version", action="store_true", help="Show version information"
    )
    config_parser.add_argument(
        "--gcs-info", help="Show GCS file information for the specified path"
    )


def cmd_config(args) -> None:
    """Execute the config command."""
    if args.gcs_info:
        _show_gcs_info(args.gcs_info)
    elif args.version:
        _show_version()
    elif args.show_info:
        _show_system_info()
    elif args.check_deps:
        _check_dependencies()
    else:
        # Default: show all information
        _show_version()
        print()
        _show_system_info()
        print()
        _check_dependencies()


def _show_version() -> None:
    """Show version information."""
    try:
        import schema_diff

        version = getattr(schema_diff, "__version__", "unknown")
    except Exception:
        version = "unknown"

    print(f"📦 schema-diff version: {version}")


def _show_system_info() -> None:
    """Show system information."""
    print("🖥️  System Information:")
    print(f"   Python: {sys.version.split()[0]}")
    print(f"   Platform: {sys.platform}")

    # Show current working directory
    print(f"   Working directory: {Path.cwd()}")

    # Show Python path
    print(f"   Python executable: {sys.executable}")


def _check_dependencies() -> None:
    """Check optional dependencies."""
    print("📋 Dependency Status:")

    dependencies = {
        "Core Dependencies": [
            ("ijson", "JSON streaming"),
            ("deepdiff", "Deep comparison"),
            ("pyyaml", "YAML parsing"),
            ("protobuf", "Protobuf parsing"),
        ],
        "Optional Dependencies": [
            ("google-cloud-bigquery", "BigQuery integration"),
            ("google-cloud-storage", "GCS file support"),
            ("pygments", "SQL syntax highlighting"),
            ("sqlparse", "SQL DDL validation"),
            ("jsonschema", "JSON Schema validation"),
            ("typer", "Modern CLI framework"),
            ("rich", "Rich terminal output"),
            ("pydantic", "Data validation"),
        ],
    }

    for category, deps in dependencies.items():
        print(f"\n   {category}:")
        for dep_name, description in deps:
            try:
                __import__(dep_name.replace("-", "_"))
                status = "✅ Available"
            except ImportError:
                status = "❌ Not installed"

            print(f"     {dep_name:<25} {description:<25} {status}")

    # Check for BigQuery authentication
    print("\n   BigQuery Authentication:")
    try:
        from google.cloud import bigquery

        client = bigquery.Client()
        project = client.project
        print(f"     Project: {project} ✅")
    except Exception as e:
        print(f"     Status: ❌ Not configured ({str(e)[:50]}...)")

    # Check for GCS authentication
    print("\n   GCS Authentication:")
    try:
        from google.cloud import storage  # type: ignore[attr-defined]

        client = storage.Client()
        project = client.project
        print(f"     Project: {project} ✅")
    except Exception as e:
        print(f"     Status: ❌ Not configured ({str(e)[:50]}...)")


def _show_gcs_info(gcs_path: str) -> None:
    """Show information about a GCS file."""
    from ..gcs_utils import is_gcs_path, parse_gcs_path

    if not is_gcs_path(gcs_path):
        print(f"❌ Not a valid GCS path: {gcs_path}")
        print("Valid formats:")
        print("  - gs://bucket-name/path/to/file")
        print("  - https://storage.cloud.google.com/bucket-name/path/to/file")
        print("  - https://storage.googleapis.com/bucket-name/path/to/file")
        return

    try:
        bucket_name, object_path = parse_gcs_path(gcs_path)
        print("📁 GCS File Information:")
        print(f"   Path: {gcs_path}")
        print(f"   Bucket: {bucket_name}")
        print(f"   Object: {object_path}")

        # Try to get file metadata
        try:
            from google.cloud import storage  # type: ignore[attr-defined]

            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(object_path)

            if blob.exists():
                print(f"   Size: {blob.size:,} bytes")
                print(f"   Content Type: {blob.content_type}")
                print(f"   Updated: {blob.updated}")
                print("   Status: ✅ File exists and is accessible")
            else:
                print("   Status: ❌ File does not exist")

        except Exception as e:
            print(f"   Status: ❌ Cannot access file ({str(e)})")

    except Exception as e:
        print(f"❌ Error processing GCS path: {str(e)}")
