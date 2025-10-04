#!/usr/bin/env python3
"""
Main CLI entry point for schema-diff.

This module provides the main entry point and delegates to the modular CLI structure.
"""
from __future__ import annotations


def main() -> int:
    """Main entry point that delegates to modular CLI."""
    from .cli import main as cli_main

    return cli_main()
