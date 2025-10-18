#!/usr/bin/env python3
"""Config command implementation for schema-diff CLI.

Handles configuration management and system information display.
"""
from __future__ import annotations

import sys
from pathlib import Path


def add_config_subcommand(subparsers) -> None:
    """Add config subcommand to the parser."""
    from ..helpfmt import ColorDefaultsFormatter
    from .colors import BOLD, CYAN, GREEN, RESET, YELLOW

    config_parser = subparsers.add_parser(
        "config",
        help="Manage configuration",
        formatter_class=ColorDefaultsFormatter,
        description=f"""
Show system information, check dependencies, and inspect GCS files.

{BOLD}{YELLOW}WHAT IT SHOWS:{RESET}
  • Python version and platform
  • Installed packages
  • Optional dependencies status
  • GCS file metadata

{BOLD}{CYAN}EXAMPLES:{RESET}
  {GREEN}# Show all information (default){RESET}
  schema-diff config

  {GREEN}# Show version only{RESET}
  schema-diff config --version

  {GREEN}# Check which optional features are installed{RESET}
  schema-diff config --check-deps

  {GREEN}# Inspect GCS file{RESET}
  schema-diff config --gcs-info gs://bucket/file.json
        """,
    )

    config_parser.add_argument(
        "--show-info",
        action="store_true",
        help="Show system information (Python version, platform, installed packages)",
    )
    config_parser.add_argument(
        "--check-deps",
        action="store_true",
        help="Check which optional dependencies are installed (bigquery, gcs, validation)",
    )
    config_parser.add_argument(
        "--version",
        action="store_true",
        help="Show schema-diff version",
    )
    config_parser.add_argument(
        "--gcs-info",
        help="Show GCS file metadata (size, content-type, last modified) for given path",
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

    from .colors import GREEN, RESET

    print(f"{GREEN}📦 schema-diff version: {version}{RESET}")


def _show_system_info() -> None:
    """Show system information."""
    from .colors import GREEN, RESET

    print(f"{GREEN}🖥️  System Information:{RESET}")
    print(f"   Python: {sys.version.split()[0]}")
    print(f"   Platform: {sys.platform}")

    # Show current working directory
    print(f"   Working directory: {Path.cwd()}")

    # Show Python path
    print(f"   Python executable: {sys.executable}")


def _check_dependencies() -> None:
    """Check optional dependencies."""
    from .colors import GREEN, RESET

    print(f"{GREEN}📋 Dependency Status:{RESET}")

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
                from .colors import GREEN, RESET

                status = f"{GREEN}✅ Available{RESET}"
            except ImportError:
                from .colors import RED, RESET

                status = f"{RED}❌ Not installed{RESET}"

            print(f"     {dep_name:<25} {description:<25} {status}")

    # Check for BigQuery authentication
    from .colors import GREEN, RESET

    print(f"\n   {GREEN}BigQuery Authentication:{RESET}")
    try:
        from google.cloud import bigquery

        from .colors import GREEN, RESET

        client = bigquery.Client()
        project = client.project
        print(f"     Project: {project} {GREEN}✅{RESET}")
    except Exception as e:
        from .colors import RED, RESET

        print(f"     Status: {RED}❌ Not configured ({str(e)[:50]}...){RESET}")

    # Check for GCS authentication
    from .colors import GREEN, RESET

    print(f"\n   {GREEN}GCS Authentication:{RESET}")
    try:
        from google.cloud import storage  # type: ignore[attr-defined]

        from .colors import GREEN, RESET

        client = storage.Client()
        project = client.project
        print(f"     Project: {project} {GREEN}✅{RESET}")
    except Exception as e:
        from .colors import RED, RESET

        print(f"     Status: {RED}❌ Not configured ({str(e)[:50]}...){RESET}")


def _show_gcs_info(gcs_path: str) -> None:
    """Show information about a GCS file."""
    from ..gcs_utils import is_gcs_path, parse_gcs_path

    if not is_gcs_path(gcs_path):
        from ..cli.colors import RED, RESET

        print(f"{RED}❌ Not a valid GCS path: {gcs_path}{RESET}")
        print("Valid formats:")
        print("  - gs://bucket-name/path/to/file")
        print("  - https://storage.cloud.google.com/bucket-name/path/to/file")
        print("  - https://storage.googleapis.com/bucket-name/path/to/file")
        return

    try:
        bucket_name, object_path = parse_gcs_path(gcs_path)
        from ..cli.colors import GREEN, RESET

        print(f"{GREEN}📁 GCS File Information:{RESET}")
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
                from ..cli.colors import GREEN, RESET

                print(f"   Size: {blob.size:,} bytes")
                print(f"   Content Type: {blob.content_type}")
                print(f"   Updated: {blob.updated}")
                print(f"   Status: {GREEN}✅ File exists and is accessible{RESET}")
            else:
                from ..cli.colors import RED, RESET

                print(f"   Status: {RED}❌ File does not exist{RESET}")

        except Exception as e:
            from ..cli.colors import RED, RESET

            print(f"   Status: {RED}❌ Cannot access file ({str(e)}){RESET}")

    except Exception as e:
        from ..cli.colors import RED, RESET

        print(f"{RED}❌ Error processing GCS path: {str(e)}{RESET}")
