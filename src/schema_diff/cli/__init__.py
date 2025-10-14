#!/usr/bin/env python3
"""Modular CLI for schema-diff with proper separation of responsibilities.

This package contains the command-line interface implementation split into separate
modules for better maintainability and separation of concerns.
"""
from __future__ import annotations

from .analyze import cmd_analyze
from .compare import cmd_compare
from .config import cmd_config
from .ddl import _parse_dataset_ref, _parse_table_ref, cmd_ddl
from .generate import cmd_generate


# Main entry point
def main() -> int:
    """Main entry point for modular CLI."""
    import argparse
    import warnings

    from ..helpfmt import ColorDefaultsFormatter

    # Suppress Google Cloud SDK authentication warnings
    # These are informational warnings about quota projects that don't affect functionality
    warnings.filterwarnings(
        "ignore",
        message="Your application has authenticated using end user credentials.*",
        category=UserWarning,
        module="google.auth._default",
    )

    parser = argparse.ArgumentParser(
        prog="schema-diff",
        description="Compare schemas across multiple formats and generate schema documentation",
        formatter_class=ColorDefaultsFormatter,
    )

    subparsers = parser.add_subparsers(
        dest="command",
        help="Available commands",
        metavar="{compare,generate,ddl,config,analyze}",
    )

    # Add subcommands by importing their setup functions
    from .analyze import add_analyze_subcommand
    from .compare import add_compare_subcommand
    from .config import add_config_subcommand
    from .ddl import add_ddl_subcommand
    from .generate import add_generate_subcommand

    add_compare_subcommand(subparsers)
    add_generate_subcommand(subparsers)
    add_ddl_subcommand(subparsers)
    add_config_subcommand(subparsers)
    add_analyze_subcommand(subparsers)

    # Parse arguments
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    # Dispatch to appropriate handler
    try:
        if args.command == "compare":
            cmd_compare(args)
        elif args.command == "generate":
            cmd_generate(args)
        elif args.command == "ddl":
            cmd_ddl(args)
        elif args.command == "config":
            cmd_config(args)
        elif args.command == "analyze":
            cmd_analyze(args)
        else:
            parser.print_help()
            return 1

        return 0
    except KeyboardInterrupt:
        print("\n⚠️ Operation cancelled by user")
        return 130
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1


__all__ = [
    "main",
    "cmd_analyze",
    "cmd_compare",
    "cmd_generate",
    "cmd_ddl",
    "cmd_config",
    "_parse_table_ref",
    "_parse_dataset_ref",
]
