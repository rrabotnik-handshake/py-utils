"""Custom argparse help formatter.

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
    """Argparse help formatter with color-friendly alignment and colors.

    - Shows default values for options.
    - Preserves line breaks in help descriptions.
    - Sets `max_help_position` (option column width) and `width` (wrap length).
    - Adds ANSI colors to option flags and section headers.
    """

    def __init__(self, *a, **k):
        k.setdefault("max_help_position", 30)
        k.setdefault("width", 120)
        super().__init__(*a, **k)

    def _format_action_invocation(self, action):
        """Format option invocation without colors to avoid alignment issues."""
        # Don't add colors here - argparse calculates width before rendering
        # and ANSI codes break the alignment calculation
        return super()._format_action_invocation(action)

    def start_section(self, heading):
        """Add color to section headers."""
        from .cli.colors import BOLD, CYAN, RESET

        if heading:
            heading = f"{BOLD}{CYAN}{heading}{RESET}"
        return super().start_section(heading)
