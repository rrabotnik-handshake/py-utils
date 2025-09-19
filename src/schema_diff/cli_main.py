#!/usr/bin/env python3
"""
Main CLI entry point for schema-diff with subcommands.

Provides:
- schema-diff (compare) - Original comparison functionality
- schema-diff ddl - BigQuery DDL generation
- schema-diff config - Configuration management
"""
from __future__ import annotations

import sys
import argparse
from .helpfmt import ColorDefaultsFormatter


def main() -> None:
    """Main entry point with subcommand support."""
    # Check if the first argument is a subcommand
    ddl_commands = {'ddl', 'ddl-batch', 'ddl-dataset', 'config-show', 'config-init'}

    if len(sys.argv) > 1 and sys.argv[1] in ddl_commands:
        # Handle DDL and config subcommands
        parser = argparse.ArgumentParser(
            formatter_class=ColorDefaultsFormatter,
            description="BigQuery DDL generation and configuration for schema-diff"
        )

        subparsers = parser.add_subparsers(dest="command", help="Available commands")

        # Add DDL generation subcommands
        from .cli_ddl import add_ddl_subcommands
        add_ddl_subcommands(subparsers)

        # Add configuration subcommands
        from .cli_config import add_config_subcommands
        add_config_subcommands(subparsers)

        args = parser.parse_args()

        if hasattr(args, 'func'):
            args.func(args)
        else:
            parser.print_help()
    else:
        # Default to original comparison behavior
        from .cli import main as compare_main
        compare_main()



if __name__ == "__main__":
    main()
