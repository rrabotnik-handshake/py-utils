"""ANSI color codes for CLI help text."""

import os
import sys


def supports_color() -> bool:
    """Check if the terminal supports ANSI colors."""
    # Check if stdout is a TTY and NO_COLOR is not set
    return (
        hasattr(sys.stdout, "isatty")
        and sys.stdout.isatty()
        and not sys.platform.startswith("win")
    ) and "NO_COLOR" not in os.environ


# ANSI color codes
if supports_color():
    RED = "\033[91m"  # Red for errors/critical
    YELLOW = "\033[93m"  # Yellow for warnings/subsection headers
    GREEN = "\033[92m"  # Green for examples and positive items
    BLUE = "\033[94m"  # Blue for info/format names
    CYAN = "\033[96m"  # Bright cyan for section headers
    BOLD = "\033[1m"  # Bold for emphasis
    DIM = "\033[2m"  # Dim for secondary text
    RESET = "\033[0m"  # Reset to default
else:
    RED = YELLOW = GREEN = BLUE = CYAN = BOLD = DIM = RESET = ""


def section_header(text: str) -> str:
    """Format a section header with color."""
    return f"{BOLD}{CYAN}{text}{RESET}"


def subsection_header(text: str) -> str:
    """Format a subsection header with color."""
    return f"{BOLD}{YELLOW}{text}{RESET}"


def example(text: str) -> str:
    """Format example text with color."""
    return f"{GREEN}{text}{RESET}"


def format_name(text: str) -> str:
    """Format a format/option name with color."""
    return f"{BLUE}{text}{RESET}"


def dim_text(text: str) -> str:
    """Format secondary text with dim color."""
    return f"{DIM}{text}{RESET}"
