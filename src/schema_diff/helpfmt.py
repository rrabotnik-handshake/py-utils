"""
Custom argparse help formatter.

This combines:
- ArgumentDefaultsHelpFormatter → automatically appends default values to help text.
- RawTextHelpFormatter → preserves newlines and indentation in help strings.

We also adjust layout defaults for better readability in wide terminals.
"""

from __future__ import annotations

import argparse


class ColorDefaultsFormatter(
    argparse.ArgumentDefaultsHelpFormatter,
    argparse.RawTextHelpFormatter,
):
    """
    Argparse help formatter with color-friendly alignment.

    - Shows default values for options.
    - Preserves line breaks in help descriptions.
    - Sets `max_help_position` (option column width) and `width` (wrap length).
    """

    def __init__(self, *a, **k):
        k.setdefault("max_help_position", 40)
        k.setdefault("width", 100)
        super().__init__(*a, **k)
