"""Shared utilities for consistent file output across schema-diff tools."""

from __future__ import annotations

from pathlib import Path
from typing import Optional


def ensure_output_dir(subdir: Optional[str] = None) -> Path:
    """Ensure the output directory exists and return the path.

    Args:
        subdir: Optional subdirectory within ./output

    Returns:
        Path to the output directory
    """
    if subdir:
        output_dir = Path("./output") / subdir
    else:
        output_dir = Path("./output")

    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def write_output_file(
    content: str, filename: str, subdir: Optional[str] = None
) -> Path:
    """Write content to a file in the output directory.

    Args:
        content: Content to write
        filename: Name of the file
        subdir: Optional subdirectory within ./output

    Returns:
        Path to the written file
    """
    output_dir = ensure_output_dir(subdir)
    output_path = output_dir / filename

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)

    return output_path


def print_output_success(output_path: Path, description: str = "File") -> None:
    """Print a standardized success message for file output.

    Args:
        output_path: Path to the written file
        description: Description of what was written
    """
    from .cli.colors import GREEN, RESET

    print(f"{GREEN}✅ {description} written to: {output_path}{RESET}")
    print(f"{GREEN}📁 Output directory: {output_path.parent.absolute()}{RESET}")
