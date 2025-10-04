#!/usr/bin/env python3
"""
Main entry point for running schema-diff CLI as a module.

This allows running: python -m schema_diff.cli
"""
from __future__ import annotations

import sys

from . import main

if __name__ == "__main__":
    sys.exit(main())
